use std::{collections::HashMap, io::Write, path::{Path, PathBuf}, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use reqwest::{Client, Method, Url};
use tokio::sync::{Mutex, Semaphore, oneshot};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    config::{AppConfig, MatrixConfig},
    domain::{ActorId, ChannelId, ClientConversationId, ClientKind, EventId, GuildId, MessageId, NewSession, ThreadId},
    omp::{ApprovalRequest, OmpTransport, UploadRequest},
    runner::{ThreadMessenger, ThreadQueueRunner},
    store::{CreateSessionResult, EnqueueResult, Store, TurnSource},
};

const SYNC_TIMEOUT_MS: u64 = 30_000;
const PROMPT_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Clone)]
struct MatrixHttp {
    client: Client,
    homeserver: Url,
    token: Arc<str>,
}

impl MatrixHttp {
    fn new(config: &MatrixConfig) -> Result<Self> {
        Ok(Self {
            client: Client::builder().timeout(Duration::from_secs(45)).build()?,
            homeserver: config.homeserver_url.clone(),
            token: Arc::from(config.load_access_token()?),
        })
    }

    fn endpoint(&self, segments: &[&str]) -> Result<Url> {
        let mut url = self.homeserver.clone();
        url.set_query(None);
        url.set_fragment(None);
        let mut path = url.path_segments_mut().map_err(|_| anyhow::anyhow!("Matrix homeserver URL cannot be a base URL"))?;
        path.clear();
        path.extend(segments);
        drop(path);
        Ok(url)
    }

    async fn request(&self, method: Method, url: Url, body: Option<&Value>) -> Result<Value> {
        let mut request = self.client.request(method, url).bearer_auth(&*self.token);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await.context("Matrix request failed")?;
        let status = response.status();
        let bytes = response.bytes().await.context("failed to read Matrix response")?;
        if !status.is_success() {
            let detail = serde_json::from_slice::<Value>(&bytes).ok()
                .and_then(|value| value.get("error").and_then(Value::as_str).map(str::to_owned))
                .unwrap_or_else(|| "Matrix server returned no error detail".to_owned());
            bail!("Matrix request returned {status}: {detail}");
        }
        if bytes.is_empty() { return Ok(Value::Null); }
        serde_json::from_slice(&bytes).context("Matrix response was not valid JSON")
    }

    async fn send_event(&self, room_id: &str, event_type: &str, content: &Value) -> Result<String> {
        let transaction = Uuid::new_v4().to_string();
        let url = self.endpoint(&["_matrix", "client", "v3", "rooms", room_id, "send", event_type, &transaction])?;
        let response = self.request(Method::PUT, url, Some(content)).await?;
        response.get("event_id").and_then(Value::as_str).map(str::to_owned)
            .context("Matrix send response omitted event_id")
    }

    async fn send_text(&self, room_id: &str, body: &str) -> Result<String> {
        self.send_event(room_id, "m.room.message", &json!({"msgtype":"m.text", "body":body})).await
    }

    async fn download_media(&self, uri: &str, limit: u64) -> Result<Vec<u8>> {
        let uri = Url::parse(uri).context("Matrix attachment URL is invalid")?;
        if uri.scheme() != "mxc" {
            bail!("Matrix attachment URL must use mxc");
        }
        let server = uri.host_str().context("Matrix attachment URL omitted server")?;
        let media_id = uri.path().trim_start_matches('/');
        if media_id.is_empty() || media_id.contains('/') {
            bail!("Matrix attachment URL has an invalid media ID");
        }
        let url = self.endpoint(&["_matrix", "client", "v1", "media", "download", server, media_id])?;
        let response = self.client.get(url).bearer_auth(&*self.token).send().await?;
        let status = response.status();
        if !status.is_success() {
            bail!("Matrix media download returned {status}");
        }
        if response.content_length().is_some_and(|length| length > limit) {
            bail!("Matrix attachment exceeds {limit} bytes");
        }
        let bytes = response.bytes().await?;
        if u64::try_from(bytes.len())? > limit {
            bail!("Matrix attachment exceeds {limit} bytes");
        }
        Ok(bytes.to_vec())
    }
}

#[derive(Default)]
struct ReplyBroker {
    pending: Mutex<HashMap<String, oneshot::Sender<String>>>,
}

impl ReplyBroker {
    async fn wait(&self, event_id: String) -> Option<String> {
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(event_id.clone(), sender);
        let result = tokio::time::timeout(PROMPT_TIMEOUT, receiver).await.ok().and_then(Result::ok);
        self.pending.lock().await.remove(&event_id);
        result
    }

    async fn resolve(&self, event_id: &str, body: String) -> bool {
        let sender = self.pending.lock().await.remove(event_id);
        sender.is_some_and(|sender| sender.send(body).is_ok())
    }
}

struct MatrixMessenger {
    http: MatrixHttp,
    store: Store,
    rooms: HashMap<ThreadId, String>,
    replies: Arc<ReplyBroker>,
}

impl MatrixMessenger {
    fn room(&self, thread_id: ThreadId) -> Result<&str> {
        self.rooms.get(&thread_id).map(String::as_str).context("Matrix conversation is not configured")
    }
}

#[async_trait]
impl ThreadMessenger for MatrixMessenger {
    async fn send(&self, thread_id: ThreadId, content: &str) -> Result<()> {
        self.http.send_text(self.room(thread_id)?, content).await?; Ok(())
    }
    async fn send_assistant(&self, thread_id: ThreadId, content: &str) -> Result<()> { self.send(thread_id, content).await }
    async fn send_image(&self, thread_id: ThreadId, data: Vec<u8>, filename: String) -> Result<()> {
        let mut url = self.http.endpoint(&["_matrix", "media", "v3", "upload"])?;
        url.query_pairs_mut().append_pair("filename", &filename);
        let response = self.http.client.post(url).bearer_auth(&*self.http.token).header("content-type", "image/png").body(data).send().await?;
        let status = response.status();
        let value: Value = response.json().await?;
        if !status.is_success() { bail!("Matrix media upload returned {status}"); }
        let uri = value.get("content_uri").and_then(Value::as_str).context("Matrix media upload omitted content_uri")?;
        self.http.send_event(self.room(thread_id)?, "m.room.message", &json!({"msgtype":"m.image","body":filename,"url":uri})).await?;
        Ok(())
    }
    async fn create_activity(&self, thread_id: ThreadId, content: &str, _finished: bool) -> Result<MessageId> {
        let event = self.http.send_text(self.room(thread_id)?, content).await?;
        self.store.ensure_event(&EventId::scoped(ClientKind::Matrix, event)?).await
    }
    async fn edit_activity(&self, thread_id: ThreadId, message_id: MessageId, content: &str, _finished: bool) -> Result<MessageId> {
        let target = self
            .store
            .event_native_id(ClientKind::Matrix, message_id)
            .await?
            .context("Matrix activity event mapping is missing")?;
        self.http
            .send_event(
                self.room(thread_id)?,
                "m.room.message",
                &json!({
                    "msgtype": "m.text",
                    "body": format!("* {content}"),
                    "m.new_content": {"msgtype": "m.text", "body": content},
                    "m.relates_to": {"rel_type": "m.replace", "event_id": target},
                }),
            )
            .await?;
        Ok(message_id)
    }
    async fn confirm(&self, thread_id: ThreadId, request: ApprovalRequest) -> bool {
        let text = format!("Approval required: {request:?}\nReply to this message with approve or deny.");
        let Ok(event) = self.http.send_text(self.room(thread_id).unwrap_or(""), &text).await else { return false; };
        self.replies.wait(event).await.is_some_and(|reply| matches!(reply.trim().to_ascii_lowercase().as_str(), "approve" | "approved" | "yes"))
    }
    async fn prompt(&self, thread_id: ThreadId, request: Value) -> Value {
        let text = format!("Input required:\n{request}\nReply to this message.");
        let Ok(event) = self.http.send_text(self.room(thread_id).unwrap_or(""), &text).await else { return Value::Null; };
        self.replies.wait(event).await.map(Value::String).unwrap_or(Value::Null)
    }
    async fn notify(&self, thread_id: ThreadId, message: &str) { let _ = self.send(thread_id, message).await; }
    async fn channel_history(&self, thread_id: ThreadId, days: u16) -> Result<String> { self.thread_history(thread_id, thread_id, days).await }
    async fn thread_history(&self, _session_thread_id: ThreadId, target_thread_id: ThreadId, _days: u16) -> Result<String> {
        let room = self.room(target_thread_id)?;
        let mut url = self.http.endpoint(&["_matrix", "client", "v3", "rooms", room, "messages"])?;
        url.query_pairs_mut().append_pair("dir", "b").append_pair("limit", "100");
        let value = self.http.request(Method::GET, url, None).await?;
        let lines = value.get("chunk").and_then(Value::as_array).into_iter().flatten().filter_map(|event| {
            let sender = event.get("sender")?.as_str()?;
            let body = event.pointer("/content/body")?.as_str()?;
            Some(format!("{sender}: {body}"))
        }).collect::<Vec<_>>();
        Ok(lines.into_iter().rev().collect::<Vec<_>>().join("\n"))
    }
    async fn thread_participants(&self, thread_id: ThreadId) -> Result<String> {
        let url = self.http.endpoint(&["_matrix", "client", "v3", "rooms", self.room(thread_id)?, "joined_members"])?;
        let value = self.http.request(Method::GET, url, None).await?;
        Ok(value.get("joined").and_then(Value::as_object).map(|joined| joined.keys().cloned().collect::<Vec<_>>().join("\n")).unwrap_or_default())
    }
    async fn upload_file(&self, thread_id: ThreadId, request: UploadRequest) -> Result<String> {
        let data = tokio::fs::read(&request.path).await?;
        let filename = request.path.file_name().and_then(|name| name.to_str()).unwrap_or("upload").to_owned();
        let mut url = self.http.endpoint(&["_matrix", "media", "v3", "upload"])?;
        url.query_pairs_mut().append_pair("filename", &filename);
        let response = self.http.client.post(url).bearer_auth(&*self.http.token).header("content-type", "application/octet-stream").body(data).send().await?;
        let status = response.status(); let value: Value = response.json().await?;
        if !status.is_success() { bail!("Matrix media upload returned {status}"); }
        let uri = value.get("content_uri").and_then(Value::as_str).context("Matrix media upload omitted content_uri")?;
        self.http.send_event(self.room(thread_id)?, "m.room.message", &json!({"msgtype":"m.file","body":filename,"url":uri})).await?;
        Ok(uri.to_owned())
    }
    async fn thread_status(&self, thread_id: ThreadId, status: &str) -> Result<()> {
        let url = self.http.endpoint(&["_matrix", "client", "v3", "rooms", self.room(thread_id)?, "state", "io.omp.bridge.status", ""])?;
        self.http.request(Method::PUT, url, Some(&json!({"status":status}))).await?; Ok(())
    }
}

pub async fn run(config: Arc<AppConfig>, store: Store, permits: Arc<Semaphore>) -> Result<()> {
    let matrix = config.matrix.as_ref().context("Matrix is not configured")?;
    let http = MatrixHttp::new(matrix)?;
    let whoami = http.request(Method::GET, http.endpoint(&["_matrix", "client", "v3", "account", "whoami"])?, None).await?;
    let own_user = whoami.get("user_id").and_then(Value::as_str).context("Matrix whoami omitted user_id")?.to_owned();
    let replies = Arc::new(ReplyBroker::default());
    let mut rooms = HashMap::new();
    for policy in &matrix.rooms {
        let key = ClientConversationId::scoped(ClientKind::Matrix, policy.id.clone())?;
        let conversation = store.ensure_conversation(&key).await?;
        rooms.insert(ThreadId::new(u64::try_from(conversation.get())?), policy.id.clone());
    }
    let messenger = Arc::new(MatrixMessenger { http: http.clone(), store: store.clone(), rooms, replies: Arc::clone(&replies) });
    let transport = Arc::new(OmpTransport::new(config.omp.clone(), Duration::from_secs(config.runtime.turn_timeout_seconds), config.runtime.max_response_bytes, config.runtime.discord_history_max_days));
    let runner = ThreadQueueRunner::new_with_permits(store.clone(), transport, messenger, config.runtime.discord_message_limit, permits);
    for policy in &matrix.rooms {
        let key = ClientConversationId::scoped(ClientKind::Matrix, policy.id.clone())?;
        if let Some(conversation) = store.conversation_id(&key).await? {
            if let Some(session) = store.session_by_conversation(conversation).await? {
                runner.poke(session.thread_id).await;
            }
        }
    }
    sync_loop(&config, matrix, &http, &store, &runner, &replies, &own_user).await
}

async fn sync_loop(config: &AppConfig, matrix: &MatrixConfig, http: &MatrixHttp, store: &Store, runner: &Arc<ThreadQueueRunner>, replies: &ReplyBroker, own_user: &str) -> Result<()> {
    let mut since = load_since(&matrix.session_store_path).await?;
    loop {
        let mut url = http.endpoint(&["_matrix", "client", "v3", "sync"])?;
        { let mut query = url.query_pairs_mut(); query.append_pair("timeout", &SYNC_TIMEOUT_MS.to_string()); if let Some(token) = &since { query.append_pair("since", token); } }
        let response = http.request(Method::GET, url, None).await?;
        let next = response.get("next_batch").and_then(Value::as_str).context("Matrix sync omitted next_batch")?.to_owned();
        if since.is_some() { process_sync(config, http, store, runner, replies, own_user, &response).await; }
        save_since(&matrix.session_store_path, &next).await?;
        since = Some(next);
    }
}

async fn process_sync(config: &AppConfig, http: &MatrixHttp, store: &Store, runner: &Arc<ThreadQueueRunner>, replies: &ReplyBroker, own_user: &str, response: &Value) {
    let Some(joined) = response.pointer("/rooms/join").and_then(Value::as_object) else { return; };
    for (room_id, room) in joined {
        let Some(policy) = config.matrix_room_policy(room_id) else { continue; };
        let Some(events) = room.pointer("/timeline/events").and_then(Value::as_array) else { continue; };
        for event in events {
            if let Err(error) = process_event(config, http, store, runner, replies, policy, room_id, own_user, event).await {
                tracing::warn!(%room_id, %error, "Matrix event rejected");
            }
        }
    }
}

async fn process_event(config: &AppConfig, http: &MatrixHttp, store: &Store, runner: &Arc<ThreadQueueRunner>, replies: &ReplyBroker, policy: &crate::config::MatrixRoomPolicy, room_id: &str, own_user: &str, event: &Value) -> Result<()> {
    if event.get("type").and_then(Value::as_str) != Some("m.room.message") { return Ok(()); }
    let sender = event.get("sender").and_then(Value::as_str).context("Matrix event omitted sender")?;
    if sender == own_user || !policy.permits(sender) { return Ok(()); }
    let event_id = event.get("event_id").and_then(Value::as_str).context("Matrix event omitted event_id")?;
    let mut body = event.pointer("/content/body").and_then(Value::as_str).context("Matrix message omitted body")?.trim().to_owned();
    if let Some(reply_to) = event.pointer("/content/m.relates_to/m.in_reply_to/event_id").and_then(Value::as_str) {
        if replies.resolve(reply_to, body.clone()).await { return Ok(()); }
    }
    if event.pointer("/content/url").and_then(Value::as_str).is_some()
        && event.pointer("/content/msgtype").and_then(Value::as_str) != Some("m.text")
    {
        let uri = event.pointer("/content/url").and_then(Value::as_str).unwrap();
        let data = http.download_media(uri, config.runtime.max_attachment_bytes).await?;
        let directory = config.omp.session_dir.join("matrix-media");
        tokio::fs::create_dir_all(&directory).await?;
        let safe_event = event_id.chars().filter(|character| character.is_ascii_alphanumeric()).take(80).collect::<String>();
        let supplied_name = Path::new(&body).file_name().and_then(|name| name.to_str()).unwrap_or("attachment");
        let safe_name = supplied_name.chars().map(|character| if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') { character } else { '_' }).take(120).collect::<String>();
        let path = directory.join(format!("{safe_event}-{safe_name}"));
        let writer_path = path.clone();
        tokio::task::spawn_blocking(move || write_private_file(&writer_path, &data)).await.context("Matrix attachment writer panicked")??;
        body.push_str("\n\nMatrix attachment saved at: ");
        body.push_str(&path.to_string_lossy());
    }
    let conversation_key = ClientConversationId::scoped(ClientKind::Matrix, room_id.to_owned())?;
    let conversation = store.ensure_conversation(&conversation_key).await?;
    let thread_id = ThreadId::new(u64::try_from(conversation.get())?);
    let message_id = store.ensure_event(&EventId::scoped(ClientKind::Matrix, event_id.to_owned())?).await?;
    let actor_id = store.ensure_actor(&ActorId::scoped(ClientKind::Matrix, sender.to_owned())?).await?;
    if store.session(thread_id).await?.is_none() {
        let input = NewSession { thread_id, guild_id: GuildId::new(thread_id.get()), parent_channel_id: ChannelId::new(thread_id.get()), trigger_message_id: message_id, cwd: policy.cwd.clone(), created_by_user_id: actor_id };
        match store.create_session_for_conversation(conversation, &input).await? { CreateSessionResult::Created(_) | CreateSessionResult::Existing(_) => {} }
    }
    match body.as_str() {
        "!stop" => { runner.stop(thread_id).await?; }
        "!new" => { runner.new_session(thread_id).await?; }
        "!status" => { let status = runner.status(thread_id).await?; runner.notify(thread_id, &status).await; }
        "!models" => { let models = runner.models(thread_id).await?; runner.notify(thread_id, &models).await; }
        _ if body.starts_with("!model ") => { let result = runner.set_model(thread_id, body[7..].trim()).await?; runner.notify(thread_id, &result).await; }
        _ => {
            if matches!(store.enqueue_turn(thread_id, message_id, actor_id, &body, TurnSource::Human).await?, EnqueueResult::Enqueued(_)) { runner.poke(thread_id).await; }
        }
    }
    Ok(())
}

async fn load_since(path: &Path) -> Result<Option<String>> {
    match tokio::fs::read_to_string(path).await { Ok(value) => Ok(Some(value.trim().to_owned())), Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None), Err(error) => Err(error.into()) }
}

async fn save_since(path: &Path, token: &str) -> Result<()> {
    if let Some(parent) = path.parent() { tokio::fs::create_dir_all(parent).await?; }
    let temporary = path.with_extension("tmp");
    let temporary_writer = temporary.clone();
    let token = token.to_owned();
    tokio::task::spawn_blocking(move || write_private_file(&temporary_writer, token.as_bytes()))
        .await
        .context("Matrix sync-token writer panicked")??;
    tokio::fs::rename(temporary, path).await?;
    Ok(())
}

fn write_private_file(path: &PathBuf, contents: &[u8]) -> Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(contents)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn replies_resolve_only_the_referenced_prompt() {
        let broker = Arc::new(ReplyBroker::default());
        let waiter = {
            let broker = Arc::clone(&broker);
            tokio::spawn(async move { broker.wait("$prompt".to_owned()).await })
        };
        tokio::task::yield_now().await;

        assert!(!broker.resolve("$other", "ignored".to_owned()).await);
        assert!(broker.resolve("$prompt", "approved".to_owned()).await);
        assert_eq!(waiter.await.unwrap().as_deref(), Some("approved"));
    }

    #[tokio::test]
    async fn sync_token_is_saved_and_loaded_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("matrix-sync");

        assert_eq!(load_since(&path).await.unwrap(), None);
        save_since(&path, "next-token").await.unwrap();
        assert_eq!(load_since(&path).await.unwrap().as_deref(), Some("next-token"));
        assert!(!path.with_extension("tmp").exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(std::fs::metadata(path).unwrap().permissions().mode() & 0o077, 0);
        }
    }

    #[tokio::test]
    async fn matrix_http_sends_authenticated_room_events() {
        use std::{io::Read, net::TcpListener, thread};

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            let mut request = Vec::new();
            let mut chunk = [0_u8; 4096];
            loop {
                let read = stream.read(&mut chunk).unwrap();
                request.extend_from_slice(&chunk[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n")
                    && request.ends_with(b"}")
                {
                    break;
                }
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("PUT /_matrix/client/v3/rooms/"));
            assert!(request.contains("/send/m.room.message/"));
            assert!(request.to_ascii_lowercase().contains("authorization: bearer test-token"));
            assert!(request.contains("\"body\":\"hello\""));
            stream.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{\"event_id\":\"$response\"}").unwrap();
        });
        let http = MatrixHttp {
            client: Client::builder().no_proxy().build().unwrap(),
            homeserver: Url::parse(&format!("http://{address}")).unwrap(),
            token: Arc::from("test-token"),
        };

        assert_eq!(http.send_text("!room:example.org", "hello").await.unwrap(), "$response");
        server.join().unwrap();
    }
}

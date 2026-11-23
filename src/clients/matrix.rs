use std::{collections::HashMap, io::Write, path::{Path, PathBuf}, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use reqwest::{Certificate, Client, Method, Url};
use tokio::sync::{Mutex, RwLock, Semaphore, oneshot};
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
        let mut builder = Client::builder().timeout(Duration::from_secs(45));
        if let Some(ca_file) = &config.ca_file {
            let pem = std::fs::read(ca_file)
                .with_context(|| format!("failed to read matrix.ca_file {}", ca_file.display()))?;
            let certs = Certificate::from_pem_bundle(&pem)
                .with_context(|| format!("matrix.ca_file {} is not a valid PEM certificate bundle", ca_file.display()))?;
            if certs.is_empty() {
                bail!("matrix.ca_file {} contains no certificates", ca_file.display());
            }
            for cert in certs {
                builder = builder.add_root_certificate(cert);
            }
        }
        Ok(Self {
            client: builder.build()?,
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

#[derive(Clone)]
struct MatrixTarget {
    room_id: String,
    root_event_id: Option<String>,
}

fn thread_relation(root_event_id: &str) -> Value {
    json!({
        "rel_type": "m.thread",
        "event_id": root_event_id,
        "is_falling_back": true,
        "m.in_reply_to": {"event_id": root_event_id},
    })
}

struct MatrixMessenger {
    http: MatrixHttp,
    store: Store,
    targets: Arc<RwLock<HashMap<ThreadId, MatrixTarget>>>,
    replies: Arc<ReplyBroker>,
}

impl MatrixMessenger {
    async fn target(&self, thread_id: ThreadId) -> Result<MatrixTarget> {
        self.targets
            .read()
            .await
            .get(&thread_id)
            .cloned()
            .context("Matrix conversation is not configured")
    }

    async fn send_content(&self, thread_id: ThreadId, mut content: Value) -> Result<String> {
        let target = self.target(thread_id).await?;
        if let Some(root) = target.root_event_id {
            content["m.relates_to"] = thread_relation(&root);
        }
        self.http.send_event(&target.room_id, "m.room.message", &content).await
    }

    async fn send_text(&self, thread_id: ThreadId, content: &str) -> Result<String> {
        self.send_content(thread_id, json!({"msgtype": "m.text", "body": content})).await
    }
}

#[async_trait]
impl ThreadMessenger for MatrixMessenger {
    async fn send(&self, thread_id: ThreadId, content: &str) -> Result<()> {
        self.send_text(thread_id, content).await?;
        Ok(())
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
        self.send_content(thread_id, json!({"msgtype":"m.image","body":filename,"url":uri})).await?;
        Ok(())
    }
    async fn create_activity(&self, thread_id: ThreadId, content: &str, _finished: bool) -> Result<MessageId> {
        let event = self.send_text(thread_id, content).await?;
        self.store.ensure_event(&EventId::scoped(ClientKind::Matrix, event)?).await
    }
    async fn edit_activity(&self, thread_id: ThreadId, message_id: MessageId, content: &str, _finished: bool) -> Result<MessageId> {
        let activity_event = self
            .store
            .event_native_id(ClientKind::Matrix, message_id)
            .await?
            .context("Matrix activity event mapping is missing")?;
        let target = self.target(thread_id).await?;
        self.http
            .send_event(
                &target.room_id,
                "m.room.message",
                &json!({
                    "msgtype": "m.text",
                    "body": format!("* {content}"),
                    "m.new_content": {"msgtype": "m.text", "body": content},
                    "m.relates_to": {"rel_type": "m.replace", "event_id": activity_event},
                }),
            )
            .await?;
        Ok(message_id)
    }
    async fn confirm(&self, thread_id: ThreadId, request: ApprovalRequest) -> bool {
        let text = format!("Approval required: {request:?}\nReply to this message with approve or deny.");
        let Ok(event) = self.send_text(thread_id, &text).await else { return false; };
        self.replies.wait(event).await.is_some_and(|reply| matches!(reply.trim().to_ascii_lowercase().as_str(), "approve" | "approved" | "yes"))
    }
    async fn prompt(&self, thread_id: ThreadId, request: Value) -> Value {
        let text = format!("Input required:\n{request}\nReply to this message.");
        let Ok(event) = self.send_text(thread_id, &text).await else { return Value::Null; };
        self.replies.wait(event).await.map(Value::String).unwrap_or(Value::Null)
    }
    async fn notify(&self, thread_id: ThreadId, message: &str) { let _ = self.send(thread_id, message).await; }
    async fn channel_history(&self, thread_id: ThreadId, days: u16) -> Result<String> { self.thread_history(thread_id, thread_id, days).await }
    async fn thread_history(&self, _session_thread_id: ThreadId, target_thread_id: ThreadId, _days: u16) -> Result<String> {
        let target = self.target(target_thread_id).await?;
        let mut url = self.http.endpoint(&["_matrix", "client", "v3", "rooms", &target.room_id, "messages"])?;
        url.query_pairs_mut().append_pair("dir", "b").append_pair("limit", "100");
        let value = self.http.request(Method::GET, url, None).await?;
        let lines = value.get("chunk").and_then(Value::as_array).into_iter().flatten().filter_map(|event| {
            if let Some(root) = &target.root_event_id {
                let event_root = event.pointer("/content/m.relates_to/event_id").and_then(Value::as_str);
                let event_id = event.get("event_id").and_then(Value::as_str);
                if event_root != Some(root.as_str()) && event_id != Some(root.as_str()) {
                    return None;
                }
            }
            let sender = event.get("sender")?.as_str()?;
            let body = event.pointer("/content/body")?.as_str()?;
            Some(format!("{sender}: {body}"))
        }).collect::<Vec<_>>();
        Ok(lines.into_iter().rev().collect::<Vec<_>>().join("\n"))
    }
    async fn thread_participants(&self, thread_id: ThreadId) -> Result<String> {
        let target = self.target(thread_id).await?;
        let url = self.http.endpoint(&["_matrix", "client", "v3", "rooms", &target.room_id, "joined_members"])?;
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
        self.send_content(thread_id, json!({"msgtype":"m.file","body":filename,"url":uri})).await?;
        Ok(uri.to_owned())
    }
    async fn thread_status(&self, thread_id: ThreadId, status: &str) -> Result<()> {
        let target = self.target(thread_id).await?;
        let state_key = target.root_event_id.as_deref().unwrap_or("");
        let url = self.http.endpoint(&["_matrix", "client", "v3", "rooms", &target.room_id, "state", "io.omp.bridge.status", state_key])?;
        self.http.request(Method::PUT, url, Some(&json!({"status":status}))).await?;
        Ok(())
    }
}

fn conversation_native_id(room_id: &str, root_event_id: Option<&str>) -> String {
    root_event_id.map_or_else(|| room_id.to_owned(), |root| format!("{room_id}\n{root}"))
}

fn target_from_native_id(native_id: &str) -> MatrixTarget {
    let (room_id, root_event_id) = native_id
        .split_once('\n')
        .map_or((native_id, None), |(room, root)| (room, Some(root.to_owned())));
    MatrixTarget { room_id: room_id.to_owned(), root_event_id }
}

fn mentions_user(event: &Value, user_id: &str) -> bool {
    event
        .pointer("/content/m.mentions/user_ids")
        .and_then(Value::as_array)
        .is_some_and(|users| users.iter().any(|user| user.as_str() == Some(user_id)))
}

pub async fn run(config: Arc<AppConfig>, store: Store, permits: Arc<Semaphore>) -> Result<()> {
    let matrix = config.matrix.as_ref().context("Matrix is not configured")?;
    let http = MatrixHttp::new(matrix)?;
    let whoami = http.request(Method::GET, http.endpoint(&["_matrix", "client", "v3", "account", "whoami"])?, None).await?;
    let own_user = whoami.get("user_id").and_then(Value::as_str).context("Matrix whoami omitted user_id")?.to_owned();
    let replies = Arc::new(ReplyBroker::default());
    let mut loaded_targets = HashMap::new();
    for (conversation, native_id) in store.client_conversations(ClientKind::Matrix).await? {
        let target = target_from_native_id(&native_id);
        if config.matrix_room_policy(&target.room_id).is_some() {
            loaded_targets.insert(ThreadId::new(u64::try_from(conversation.get())?), target);
        }
    }
    for policy in matrix.rooms.iter().filter(|policy| !policy.require_mention) {
        let key = ClientConversationId::scoped(ClientKind::Matrix, policy.id.clone())?;
        let conversation = store.ensure_conversation(&key).await?;
        loaded_targets.entry(ThreadId::new(u64::try_from(conversation.get())?)).or_insert_with(|| MatrixTarget {
            room_id: policy.id.clone(),
            root_event_id: None,
        });
    }
    let targets = Arc::new(RwLock::new(loaded_targets));
    let messenger = Arc::new(MatrixMessenger { http: http.clone(), store: store.clone(), targets: Arc::clone(&targets), replies: Arc::clone(&replies) });
    let transport = Arc::new(OmpTransport::new(config.omp.clone(), Duration::from_secs(config.runtime.turn_timeout_seconds), config.runtime.max_response_bytes, config.runtime.discord_history_max_days));
    let runner = ThreadQueueRunner::new_with_permits(store.clone(), transport, messenger, config.runtime.discord_message_limit, permits);
    for thread_id in targets.read().await.keys().copied().collect::<Vec<_>>() {
        if store.session(thread_id).await?.is_some() {
            runner.poke(thread_id).await;
        }
    }
    sync_loop(&config, matrix, &http, &store, &runner, &replies, &targets, &own_user).await
}

async fn sync_loop(config: &AppConfig, matrix: &MatrixConfig, http: &MatrixHttp, store: &Store, runner: &Arc<ThreadQueueRunner>, replies: &ReplyBroker, targets: &Arc<RwLock<HashMap<ThreadId, MatrixTarget>>>, own_user: &str) -> Result<()> {
    let mut since = load_since(&matrix.session_store_path).await?;
    loop {
        let mut url = http.endpoint(&["_matrix", "client", "v3", "sync"])?;
        { let mut query = url.query_pairs_mut(); query.append_pair("timeout", &SYNC_TIMEOUT_MS.to_string()); if let Some(token) = &since { query.append_pair("since", token); } }
        let response = http.request(Method::GET, url, None).await?;
        let next = response.get("next_batch").and_then(Value::as_str).context("Matrix sync omitted next_batch")?.to_owned();
        if since.is_some() { process_sync(config, http, store, runner, replies, targets, own_user, &response).await; }
        save_since(&matrix.session_store_path, &next).await?;
        since = Some(next);
    }
}

async fn process_sync(config: &AppConfig, http: &MatrixHttp, store: &Store, runner: &Arc<ThreadQueueRunner>, replies: &ReplyBroker, targets: &Arc<RwLock<HashMap<ThreadId, MatrixTarget>>>, own_user: &str, response: &Value) {
    let Some(joined) = response.pointer("/rooms/join").and_then(Value::as_object) else { return; };
    for (room_id, room) in joined {
        let Some(policy) = config.matrix_room_policy(room_id) else { continue; };
        let Some(events) = room.pointer("/timeline/events").and_then(Value::as_array) else { continue; };
        for event in events {
            if let Err(error) = process_event(config, http, store, runner, replies, targets, policy, room_id, own_user, event).await {
                tracing::warn!(%room_id, %error, "Matrix event rejected");
            }
        }
    }
}

async fn process_event(config: &AppConfig, http: &MatrixHttp, store: &Store, runner: &Arc<ThreadQueueRunner>, replies: &ReplyBroker, targets: &Arc<RwLock<HashMap<ThreadId, MatrixTarget>>>, policy: &crate::config::MatrixRoomPolicy, room_id: &str, own_user: &str, event: &Value) -> Result<()> {
    if event.get("type").and_then(Value::as_str) != Some("m.room.message") { return Ok(()); }
    let sender = event.get("sender").and_then(Value::as_str).context("Matrix event omitted sender")?;
    if sender == own_user || !policy.permits(sender) { return Ok(()); }
    let event_id = event.get("event_id").and_then(Value::as_str).context("Matrix event omitted event_id")?;
    let mut body = event.pointer("/content/body").and_then(Value::as_str).context("Matrix message omitted body")?.trim().to_owned();
    if let Some(reply_to) = event.pointer("/content/m.relates_to/m.in_reply_to/event_id").and_then(Value::as_str) {
        if replies.resolve(reply_to, body.clone()).await { return Ok(()); }
    }
    let thread_root = event
        .pointer("/content/m.relates_to")
        .filter(|relation| relation.get("rel_type").and_then(Value::as_str) == Some("m.thread"))
        .and_then(|relation| relation.get("event_id"))
        .and_then(Value::as_str);
    if thread_root.is_none() && policy.require_mention && !mentions_user(event, own_user) {
        return Ok(());
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
    let root_event_id = thread_root.map(str::to_owned).or_else(|| policy.require_mention.then(|| event_id.to_owned()));
    let conversation_key = ClientConversationId::scoped(ClientKind::Matrix, conversation_native_id(room_id, root_event_id.as_deref()))?;
    let conversation = store.ensure_conversation(&conversation_key).await?;
    let thread_id = ThreadId::new(u64::try_from(conversation.get())?);
    targets.write().await.insert(thread_id, MatrixTarget { room_id: room_id.to_owned(), root_event_id });
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

    #[test]
    fn native_thread_identity_and_relation_preserve_the_root_event() {
        let native = conversation_native_id("!room:example.org", Some("$root"));
        let target = target_from_native_id(&native);
        assert_eq!(target.room_id, "!room:example.org");
        assert_eq!(target.root_event_id.as_deref(), Some("$root"));
        assert_eq!(
            thread_relation("$root"),
            json!({
                "rel_type": "m.thread",
                "event_id": "$root",
                "is_falling_back": true,
                "m.in_reply_to": {"event_id": "$root"},
            })
        );
    }

    #[test]
    fn mention_admission_uses_matrix_mentions_metadata() {
        let event = json!({"content": {"m.mentions": {"user_ids": ["@bot:example.org"]}}});
        assert!(mentions_user(&event, "@bot:example.org"));
        assert!(!mentions_user(&event, "@other:example.org"));
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

        assert_eq!(
            http.send_event(
                "!room:example.org",
                "m.room.message",
                &json!({"msgtype": "m.text", "body": "hello"}),
            )
            .await
            .unwrap(),
            "$response"
        );
        server.join().unwrap();
    }

    #[test]
    fn http_client_builds_with_a_valid_ca_file_and_rejects_garbage() {
        use std::os::unix::fs::PermissionsExt;

        const TEST_CA_PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIDBTCCAe2gAwIBAgIUDmDWBXuEwtYwCA6GjA33t8u4NZswDQYJKoZIhvcNAQEL\nBQAwEjEQMA4GA1UEAwwHdGVzdC1jYTAeFw0yNjExMjMyMTI5NDdaFw0yNjExMjQy\nMTI5NDdaMBIxEDAOBgNVBAMMB3Rlc3QtY2EwggEiMA0GCSqGSIb3DQEBAQUAA4IB\nDwAwggEKAoIBAQDkugkvKLyuKPvHVnkf9/s45wzjm2cDjBvzzfcj47cebzD6zkBZ\ncO63L9udAEZpOt4VEilTCPEnmypT38ayZPYGFbE4PIEMGe9TWfCGmGVQiU54XTtI\naB5nCNCUOeTQI4n0Kxgcp0EthmDG3hrS5vIiSfzhejJKk28Kcry8g2ZE8+A0igtP\nPBfyLdRwT9fn8U/YhtZZOP48S77JNEdm6mlr7smHZK2CGc7/zJL4SBPJuREsiXzM\nfhUPjKkRIPTgTD8+kh2OPOfacFpV97aZLL21ZBrkbS29wwyo/ZyXDqjHRr2uuzso\n+r1wVZTdjq5F6WBoOm8tjX1vE16A++JbeQQVAgMBAAGjUzBRMB0GA1UdDgQWBBSv\n/dwlao/IY3F3lGBTQ3Ar9LchJTAfBgNVHSMEGDAWgBSv/dwlao/IY3F3lGBTQ3Ar\n9LchJTAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQAWWAe7Ichn\ntQ803tD01wO2okGYr62JgdHWLn+jZJMJazpNjqx5Wl9WrFgxuoai4QGAiQrrQW73\n+Rxjc3w8W8ucyjOVDaLz3VxGgTUxcpjeGNi59a0YqlSximnv1AW3ANhKHi8VjBSY\nHCk1GZY5qzxYvEe2eeQRT0406Qv4iXOYP3VoUuaG2x8c+0jKnui8pYUsuEkeyKFQ\n6uY4TnMLQF30IFCeCQgOQKtRgtKzJq04K6b6qFNUACPO+jl5J74nqd+ANcW+smi+\nIpMOiegO4JntUgzkpj/aQ/zGCeaK8b7I72xl8jPXKSxoeAA2uS/Fl4PqxIrhqz8L\nHbjgyJmO5/0w\n-----END CERTIFICATE-----\n";

        let root = tempfile::tempdir().unwrap();
        let token = root.path().join("token");
        std::fs::write(&token, "test-token").unwrap();
        #[cfg(unix)]
        std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();

        let ca_file = root.path().join("ca.pem");
        std::fs::write(&ca_file, TEST_CA_PEM).unwrap();
        let config = MatrixConfig {
            homeserver_url: Url::parse("https://matrix.example.org").unwrap(),
            access_token_file: token.clone(),
            ca_file: Some(ca_file.clone()),
            session_store_path: root.path().join("store"),
            rooms: vec![],
        };
        MatrixHttp::new(&config).unwrap();

        let garbage = root.path().join("garbage.pem");
        std::fs::write(&garbage, "not a certificate").unwrap();
        let bad = MatrixConfig {
            ca_file: Some(garbage),
            ..config.clone()
        };
        assert!(MatrixHttp::new(&bad).is_err(), "garbage PEM must fail client construction");
    }
}

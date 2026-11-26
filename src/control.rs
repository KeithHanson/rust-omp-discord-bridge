//! Private local control socket for external triggers that start a new OMP session.
//!
//! The daemon binds this socket (mode 0600 in a mode-0700 directory owned by the
//! same user) and accepts one-line JSON requests from processes of the same user.
//! The trigger flow mirrors the gateway's session-creation path: headline message,
//! anchor thread, a stored session, an enqueued turn, and a synchronous wait for
//! the turn outcome.
use std::{
    fs, io,
    os::unix::{
        fs::{FileTypeExt, MetadataExt, PermissionsExt},
        net::UnixStream as StdUnixStream,
    },
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
};

/// The per-connection handler shape: takes the accepted Unix stream and returns a
/// spawned-style boxed future.
pub type Handler = std::sync::Arc<
    dyn Fn(UnixStream) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

/// One request frame and response frame may not exceed 16 KiB.
pub const MAX_FRAME_BYTES: usize = 16 * 1024;
/// Prompt is bounded well inside the frame limit; the mention flow uses no explicit cap.
pub const MAX_PROMPT_BYTES: usize = 8 * 1024;
/// Default and maximum synchronous wait for a turn, in seconds.
pub const DEFAULT_TIMEOUT_SECONDS: u64 = 1_800;
pub const MAX_TIMEOUT_SECONDS: u64 = 7_200;
/// Default socket path; override with `[control] socket = "path"` in config.toml,
/// or disable by setting it to "".
pub const DEFAULT_SOCKET_PATH: &str = "~/.local/state/omp-discord-bridge/control.sock";
/// Bound on concurrently admitted connections.
pub const MAX_CLIENTS: usize = 8;
/// Time given to a connection to deliver its complete request frame.
pub const READ_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TriggerRequest {
    pub prompt: String,
    pub channel_id: u64,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// When true, the trigger starts an unattended session that drops no new
    /// thread, headline, or prompt post: the session just does the work and
    /// the AI posts results through its send-message host tools.
    #[serde(default)]
    pub silent: bool,
}

fn default_timeout_seconds() -> u64 {
    DEFAULT_TIMEOUT_SECONDS
}

impl TriggerRequest {
    /// Validates the request and returns the normalized prompt to use.
    pub fn validate(&self) -> Result<&str> {
        ensure!(self.channel_id != 0, "channel_id must be nonzero");
        ensure!(
            self.timeout_seconds > 0 && self.timeout_seconds <= MAX_TIMEOUT_SECONDS,
            "timeout_seconds must be between 1 and {MAX_TIMEOUT_SECONDS}"
        );
        let prompt = self.prompt.trim();
        ensure!(!prompt.is_empty(), "prompt must not be empty");
        ensure!(
            prompt.len() <= MAX_PROMPT_BYTES,
            "prompt exceeds the {MAX_PROMPT_BYTES} byte limit"
        );
        Ok(prompt)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TriggerResponse {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guild_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_channel_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl TriggerResponse {
    pub fn failure(error: impl Into<String>) -> Self {
        Self {
            ok: false,
            thread_id: None,
            guild_id: None,
            parent_channel_id: None,
            message_id: None,
            turn_id: None,
            response: None,
            error: Some(error.into()),
        }
    }
}
/// A unique synthetic snowflake for a silent session. There is no headline or
/// thread to take identity from, so the id is derived here and never used
/// against the Discord API; the top bit marks it as synthetic and keeps it
/// clear of real Discord snowflakes.
pub fn synthetic_id(request: &TriggerRequest) -> u64 {
    use sha2::{Digest, Sha256};
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let digest = Sha256::digest(
        format!(
            "silent-trigger:{nanos}:{}:{}",
            request.channel_id, request.prompt
        )
        .as_bytes(),
    );
    u64::from_be_bytes(digest[..8].try_into().expect("truncated hash")) | (1u64 << 63)
}

/// Reads one bounded, newline-terminated JSON frame from a stream.
pub async fn read_frame<T: for<'de> Deserialize<'de>>(stream: &mut UnixStream) -> Result<T> {
    let max_bytes = MAX_FRAME_BYTES + 1;
    let mut bytes = Vec::new();
    BufReader::new(stream.take(max_bytes as u64))
        .read_until(b'\n', &mut bytes)
        .await?;
    ensure!(
        bytes.len() <= MAX_FRAME_BYTES && bytes.last() == Some(&b'\n'),
        "invalid or oversized control frame"
    );
    serde_json::from_slice(&bytes).context("invalid control protocol JSON")
}

/// Writes one JSON frame as a single newline-delimited line.
pub async fn write_frame<T: Serialize>(stream: &mut UnixStream, value: &T) -> Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    ensure!(
        bytes.len() <= MAX_FRAME_BYTES,
        "control frame exceeds limit"
    );
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(())
}

/// Opens a new connection to the control socket.
pub async fn connect(path: &std::path::Path) -> Result<UnixStream> {
    let stream = UnixStream::connect(path)
        .await
        .with_context(|| format!("cannot connect to control socket {}", path.display()))?;
    Ok(stream)
}

/// A protected Unix-socket listener. Ownership is checked against the caller's uid.
pub struct ControlSocket {
    listener: UnixListener,
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl ControlSocket {
    pub fn bind(path: PathBuf) -> Result<Self> {
        ensure!(path.is_absolute(), "control socket path must be absolute");
        let parent = path
            .parent()
            .context("control socket requires a parent directory")?;
        ensure!(
            parent.is_dir(),
            "control socket parent {} does not exist; create it as a mode-0700 directory",
            parent.display()
        );
        let directory = fs::symlink_metadata(parent)?;
        ensure!(
            directory.is_dir() && directory.mode() & 0o7777 == 0o700,
            "control socket parent must be a preexisting mode-0700 directory"
        );
        // Never unlink an occupied path. A stale socket with no one listening may be removed.
        let listener = match UnixListener::bind(&path) {
            Ok(listener) => listener,
            Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
                let metadata = fs::symlink_metadata(&path)
                    .context("control socket path was replaced or disappeared")?;
                ensure!(
                    metadata.file_type().is_socket(),
                    "control socket path exists and is not a socket"
                );
                if StdUnixStream::connect(&path).is_ok() {
                    bail!(
                        "control socket {} is already in use by another daemon",
                        path.display()
                    );
                }
                fs::remove_file(&path).context("cannot remove stale control socket")?;
                UnixListener::bind(&path)
                    .context("cannot bind control socket after stale removal")?
            }
            Err(error) => return Err(error).context("cannot bind control socket"),
        };
        let metadata = fs::symlink_metadata(&path)?;
        let socket = Self {
            listener,
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        fs::set_permissions(&socket.path, fs::Permissions::from_mode(0o600))?;
        Ok(socket)
    }

    pub fn unlink_owned(&self) -> Result<()> {
        match fs::symlink_metadata(&self.path) {
            Ok(metadata)
                if metadata.file_type().is_socket()
                    && metadata.dev() == self.device
                    && metadata.ino() == self.inode =>
            {
                fs::remove_file(&self.path)?;
                Ok(())
            }
            Ok(_) => bail!("control socket path was replaced; refusing removal"),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// Accepts connections and hands each to `handler`. Bounded concurrency keeps a
    /// flood from exhausting the event loop; new connections wait in the backlog
    /// instead of admitting a second turn-away computation at once. A panicking
    /// handler is isolated to its task and cannot take down the listener.
    pub async fn serve(&self, handler: Handler) -> Result<()> {
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CLIENTS));
        loop {
            let (stream, _) = self
                .listener
                .accept()
                .await
                .context("control socket accept failed")?;
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .context("control concurrency semaphore closed")?;
            let handler = std::sync::Arc::clone(&handler);
            tokio::spawn(async move {
                let _permit = permit;
                handler(stream).await;
            });
        }
    }
}

impl Drop for ControlSocket {
    fn drop(&mut self) {
        if self.unlink_owned().is_err() {
            tracing::warn!("could not remove owned control socket");
        }
    }
}

/// Waits for the outcome of `turn_id` in `thread_id`, polling every 250 ms.
/// Returns `None` when the deadline elapses (the turn keeps running).
pub async fn wait_for_turn(
    store: &crate::store::Store,
    thread_id: crate::domain::ThreadId,
    turn_id: i64,
    timeout: Duration,
) -> Result<Option<crate::store::TurnOutcome>> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(outcome) = store.next_outcome(thread_id, turn_id - 1).await? {
            return Ok(Some(outcome));
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(None);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The default socket path: `~/.local/state/omp-discord-bridge/control.sock`.
pub(crate) fn default_socket_path() -> Result<PathBuf> {
    let text = DEFAULT_SOCKET_PATH;
    if text == "~" || text.starts_with("~/") {
        let home = std::env::var_os("HOME")
            .context("HOME is not set; cannot expand default socket path")?;
        let mut expanded = PathBuf::from(home);
        if text.len() > 2 {
            expanded.push(&text[2..]);
        }
        Ok(expanded)
    } else {
        Ok(PathBuf::from(text))
    }
}

/// The control socket path from loaded configuration, or `None` when disabled.
pub fn socket_path(config: &crate::config::AppConfig) -> Option<PathBuf> {
    config
        .control
        .as_ref()
        .and_then(|control| control.socket.clone())
}

/// Renders the headline attached to a session created by a trigger. Keeps the
/// first line and truncates with an ellipsis so the attention post stays short.
pub fn headline_preview(prompt: &str) -> String {
    let first_line = prompt.lines().next().unwrap_or(prompt).trim();
    let mut preview = first_line.chars().take(160).collect::<String>();
    if first_line.chars().count() > 160 {
        preview.push('…');
    }
    preview
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_validation_requires_prompt_channel_and_bounded_timeout() {
        let valid = TriggerRequest {
            prompt: "  summarize the repo  ".into(),
            channel_id: 99,
            timeout_seconds: 10,
            silent: false,
        };
        assert_eq!(valid.validate().unwrap(), "summarize the repo");

        let mut empty = valid.clone();
        empty.prompt = "   ".into();
        assert!(empty.validate().is_err());

        let mut oversized = valid.clone();
        oversized.prompt = "x".repeat(MAX_PROMPT_BYTES + 1);
        assert!(oversized.validate().is_err());

        let mut zero_channel = valid.clone();
        zero_channel.channel_id = 0;
        assert!(zero_channel.validate().is_err());

        for timeout in [0, MAX_TIMEOUT_SECONDS + 1] {
            let mut request = valid.clone();
            request.timeout_seconds = timeout;
            assert!(request.validate().is_err());
        }
    }

    #[test]
    fn request_rejects_unknown_fields_and_malformed_json() {
        let valid = b"{\"prompt\":\"go\",\"channel_id\":42,\"timeout_seconds\":7}\n";
        let request: TriggerRequest = serde_json::from_slice(valid).unwrap();
        assert_eq!(request.prompt, "go");
        assert_eq!(request.channel_id, 42);

        let extra = b"{\"prompt\":\"go\",\"channel_id\":42,\"admin\":true}\n";
        assert!(serde_json::from_slice::<TriggerRequest>(extra).is_err());

        // A default timeout applies when omitted.
        let omitted = b"{\"prompt\":\"go\",\"channel_id\":42}\n";
        assert_eq!(
            serde_json::from_slice::<TriggerRequest>(omitted)
                .unwrap()
                .timeout_seconds,
            DEFAULT_TIMEOUT_SECONDS
        );

        // Malformed JSON is rejected.
        for bytes in [
            b"not json\n".as_slice(),
            b"{\"prompt\":\"go\",\"channel_id\":}".as_slice(),
        ] {
            assert!(serde_json::from_slice::<TriggerRequest>(bytes).is_err());
        }
    }

    #[test]
    fn response_serializes_without_secret_bearing_fields() {
        let response = TriggerResponse {
            ok: true,
            thread_id: Some(11),
            guild_id: Some(7),
            parent_channel_id: Some(9),
            message_id: Some(5),
            turn_id: Some(1),
            response: Some("done".into()),
            error: None,
        };
        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["ok"], true);
        assert_eq!(json["thread_id"], 11);
        assert!(json.get("error").is_none());
        assert!(json.get("token").is_none());
    }

    #[test]
    fn headline_preview_keeps_first_line_and_truncates() {
        let prompt = "first line\nsecond line\n";
        assert_eq!(headline_preview(prompt), "first line");
        let long = "y".repeat(300);
        let preview = headline_preview(&long);
        assert!(preview.chars().count() <= 161);
        assert!(preview.ends_with('…'));
    }

    #[test]
    fn socket_bind_rejects_relative_path_and_stale_occupied_path() {
        assert!(ControlSocket::bind(PathBuf::from("relative/control.sock")).is_err());

        // A preexisting occupied socket from another daemon must not be unlinked.
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().join("parent");
        std::fs::create_dir(&parent).unwrap();
        std::fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
        let path = parent.join("control.sock");
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let error = ControlSocket::bind(path).err().expect("bind should fail");
        drop(listener);
        assert!(
            error.to_string().contains("already in use") || error.to_string().contains("exists")
        );
    }
}

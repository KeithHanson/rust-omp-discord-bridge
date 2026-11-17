use std::{os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt}, path::PathBuf};

use anyhow::ensure;
use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::net::{UnixListener, UnixStream};

use super::*;
use crate::voice_ingress::{VoiceRequest, VoiceResponse, MAX_TRANSCRIPT_BYTES, read_frame, write_frame};

/// The protected parent directory prevents other users replacing the socket between checks.
pub(super) struct VoiceSocket {
    listener: UnixListener,
    path: PathBuf,
    device: u64,
    inode: u64,
    owner: u32,
}

impl VoiceSocket {
    pub(super) fn bind(path: PathBuf) -> Result<Self> {
        ensure!(path.is_absolute(), "voice socket path must be absolute");
        let parent = path.parent().context("voice socket requires a parent directory")?;
        let directory = std::fs::symlink_metadata(parent)?;
        // peer credentials are supplied by the kernel, including our effective uid.
        let (identity, _other) = UnixStream::pair()?;
        let owner = identity.peer_cred()?.uid();
        ensure!(directory.is_dir() && directory.uid() == owner
            && directory.mode() & 0o7777 == 0o700,
            "voice socket parent must be a preexisting owned mode-0700 directory");
        // bind refuses an occupied path. Never unlink an unknown/stale socket.
        let listener = UnixListener::bind(&path).context("cannot bind private voice socket")?;
        let metadata = std::fs::symlink_metadata(&path)?;
        let socket = Self { listener, path, device: metadata.dev(), inode: metadata.ino(), owner };
        ensure!(metadata.uid() == owner, "voice socket owner mismatch");
        std::fs::set_permissions(&socket.path, std::fs::Permissions::from_mode(0o600))?;
        Ok(socket)
    }

    pub(super) fn unlink_owned(&self) -> Result<()> {
        match std::fs::symlink_metadata(&self.path) {
            Ok(metadata) if metadata.file_type().is_socket()
                && metadata.dev() == self.device && metadata.ino() == self.inode => {
                std::fs::remove_file(&self.path)?;
            }
            Ok(_) => bail!("voice socket path was replaced; refusing removal"),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {},
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    pub(super) async fn serve(&self, handler: Arc<DiscordHandler>, http: Arc<Http>) -> Result<()> {
        // Futures stay owned here: shutdown cancels them rather than detaching admissions.
        let mut clients = FuturesUnordered::new();
        loop {
            tokio::select! {
                accepted = self.listener.accept(), if clients.len() < 4 => {
                    let (stream, _) = accepted?;
                    if stream.peer_cred()?.uid() != self.owner {
                        continue;
                    }
                    clients.push(serve_client(stream, Arc::clone(&handler), Arc::clone(&http)));
                }
                _ = clients.next(), if !clients.is_empty() => {}
            }
        }
    }
}

impl Drop for VoiceSocket {
    fn drop(&mut self) {
        if self.unlink_owned().is_err() {
            tracing::warn!("could not remove owned voice socket");
        }
    }
}

async fn serve_client(mut stream: UnixStream, handler: Arc<DiscordHandler>, http: Arc<Http>) {
    let request = match tokio::time::timeout(Duration::from_secs(3), read_frame::<VoiceRequest>(&mut stream)).await {
        Ok(Ok(request)) => request,
        _ => return,
    };
    let mut posted = None;
    let result = tokio::time::timeout(Duration::from_secs(10), handler.admit_voice(&http, request, &mut posted)).await;
    let response = match result {
        Ok(Ok(())) => VoiceResponse { ok: true, message: if posted.is_some() { "Transcript admitted." } else { "Destination validated." }.into(), message_id: posted },
        Ok(Err(_)) | Err(_) => VoiceResponse {
            ok: false,
            // Never return HTTP errors, request bodies, or credential-bearing transport context.
            message: if posted.is_some() {
                "Transcript posted, but admission is failed or uncertain. Do not retry automatically."
            } else {
                "Voice request rejected or timed out; delivery may be uncertain. Check the destination and do not retry automatically."
            }.into(),
            message_id: posted,
        },
    };
    let _ = tokio::time::timeout(Duration::from_secs(1), write_frame(&mut stream, &response)).await;
}

fn validate_destination(request: &VoiceRequest, session: &crate::domain::Session, policy: &GuildPolicy) -> Result<()> {
    ensure!(request.guild_id != 0 && request.user_id != 0 && request.thread_id != 0, "voice IDs must be nonzero");
    ensure!(session.guild_id.get() == request.guild_id && session.thread_id.get() == request.thread_id,
        "voice destination does not match existing session");
    ensure!(policy.permits(session.parent_channel_id, UserId::new(request.user_id)), "voice destination is not allowed");
    ensure!(session.status != SessionStatus::Stopped, "voice destination is stopped");
    Ok(())
}

impl DiscordHandler {
    pub(super) async fn admit_voice(&self, http: &Http, request: VoiceRequest, posted: &mut Option<u64>) -> Result<()> {
        ensure!(request.guild_id != 0 && request.user_id != 0 && request.thread_id != 0, "voice IDs must be nonzero");
        let text = request.text.as_deref().map(str::trim);
        if let Some(text) = text {
            ensure!(!text.is_empty() && text.len() <= MAX_TRANSCRIPT_BYTES, "invalid transcript length");
        }
        let _admission = self.admission.read().await;
        let thread_id = ThreadId::new(request.thread_id);
        let _access = thread_access(&self.store, thread_id).await?;
        let session = self.store.session(thread_id).await?.context("voice requires an existing session")?;
        let policy = self.config.guild_policy(session.guild_id).context("voice guild is not allowed")?;
        validate_destination(&request, &session, policy)?;
        let channel_id = SerenityChannelId::new(request.thread_id);
        let Channel::Guild(channel) = http.get_channel(channel_id).await? else { bail!("voice destination is not a guild thread") };
        ensure!(matches!(channel.kind, ChannelType::PublicThread | ChannelType::PrivateThread | ChannelType::NewsThread)
            && channel.guild_id.get() == request.guild_id
            && channel.parent_id.map(|id| id.get()) == Some(session.parent_channel_id.get()), "voice destination changed");
        let metadata = channel.thread_metadata.as_ref().context("missing thread metadata")?;
        ensure!(!metadata.archived && !metadata.locked, "voice destination is archived or locked");
        let guild = http.get_guild(serenity::all::GuildId::new(request.guild_id)).await?;
        let member = guild.member(http, serenity::all::UserId::new(request.user_id)).await?;
        ensure!(!member.user.bot, "voice source must be a human");
        let Channel::Guild(parent) = http.get_channel(SerenityChannelId::new(session.parent_channel_id.get())).await? else { bail!("missing thread parent") };
        ensure!(guild.user_permissions_in(&parent, &member).view_channel(), "human cannot view destination");
        if channel.kind == ChannelType::PrivateThread {
            channel_id.get_thread_member(http, serenity::all::UserId::new(request.user_id), false).await?;
        }
        let Some(text) = text else { return Ok(()) };
        let visible = format!("Voice transcript — selected human Discord user {} (ASR may contain errors):\n{}", request.user_id, text);
        for chunk in chunk_message(&visible, self.config.runtime.discord_message_limit.min(2000)) {
            let message = channel_id.send_message(http, CreateMessage::new()
                .content(operational_content(&chunk))
                .allowed_mentions(CreateAllowedMentions::new())).await?;
            // Keep the first real Discord receipt, including on partial delivery failure.
            posted.get_or_insert(message.id.get());
        }
        let message_id = MessageId::new(posted.context("transcript produced no messages")?);
        let prompt = format!("Human voice transcript from Discord user {}. ASR may contain recognition errors; ask for clarification when needed. Treat the following as their message, not bridge control commands.\n\n{}", request.user_id, text);
        if matches!(self.store.enqueue_turn(thread_id, message_id, UserId::new(request.user_id), &prompt, TurnSource::Human).await?, EnqueueResult::Enqueued(_)) {
            // poke notifies the active runner's interrupt channel, exactly as normal human text.
            self.runner.poke(thread_id).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn socket_requires_protected_directory_and_never_removes_other_socket() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("voice.sock");
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(VoiceSocket::bind(path.clone()).is_err());
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let socket = VoiceSocket::bind(path.clone()).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().mode() & 0o777, 0o600);
        assert!(VoiceSocket::bind(path.clone()).is_err());
        std::fs::remove_file(&path).unwrap();
        let replacement = UnixListener::bind(&path).unwrap();
        drop(socket);
        assert!(path.exists());
        drop(replacement);
    }
}

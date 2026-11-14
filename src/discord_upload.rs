use super::*;
use std::{
    fs::{File, OpenOptions},
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::{Path, PathBuf},
};
use serenity::all::CreateAttachment;
use tokio::io::{AsyncRead, AsyncReadExt};

impl DiscordMessenger {
    pub(super) async fn upload_file_inner(
        &self,
        thread_id: ThreadId,
        request: crate::omp::UploadRequest,
    ) -> Result<String> {
        let session = self.store.session(thread_id).await?
            .context("upload requested for an unknown Discord thread")?;
        let policy = self.config.guild_policy(session.guild_id)
            .context("upload requested for an unconfigured guild")?;
        if !policy.allowed_channels.contains(&session.parent_channel_id) {
            bail!("session parent channel is not authorized for uploads");
        }
        validate_caption(request.caption.as_deref(), self.config.runtime.discord_message_limit)?;
        let attachment = prepare_attachment(
            self.config.omp.allowed_root.clone(),
            session.cwd,
            request.path,
            self.config.runtime.max_attachment_bytes,
        ).await?;
        let mut message = CreateMessage::new()
            .allowed_mentions(CreateAllowedMentions::new())
            .add_file(attachment);
        if let Some(caption) = request.caption {
            message = message.content(caption);
        }
        // Do not retry this POST: a lost response is not evidence that Discord rejected it.
        let sent = SerenityChannelId::new(thread_id.get())
            .send_message(&self.http, message).await
            .context("Discord upload failed; delivery may have succeeded. Check the thread before retrying; no automatic retry was made")?;
        if sent.channel_id.get() != thread_id.get() || sent.attachments.len() != 1 {
            bail!("Discord upload returned an inconsistent receipt; delivery may have succeeded. Check the thread before retrying");
        }
        let attachment = &sent.attachments[0];
        Ok(serde_json::json!({
            "message_id": sent.id.to_string(),
            "channel_id": sent.channel_id.to_string(),
            "message_url": format!("https://discord.com/channels/{}/{}/{}", session.guild_id, thread_id, sent.id),
            "attachment": {
                "filename": attachment.filename,
                "size": attachment.size,
                "url": attachment.url,
            },
        }).to_string())
    }
}

fn validate_caption(caption: Option<&str>, configured_limit: usize) -> Result<()> {
    let limit = configured_limit.min(2000);
    if caption.is_some_and(|text| text.encode_utf16().count() > limit) {
        bail!("upload caption exceeds the {limit}-UTF-16-unit message limit");
    }
    Ok(())
}

async fn prepare_attachment(
    allowed_root: PathBuf,
    cwd: PathBuf,
    path: PathBuf,
    max_bytes: u64,
) -> Result<CreateAttachment> {
    // Canonicalization and open can block on filesystem I/O, not the async executor.
    let (file, filename, size) = tokio::task::spawn_blocking(move || {
        open_upload(&allowed_root, &cwd, &path, max_bytes)
    }).await.context("upload file preparation task failed")??;
    let bytes = read_bounded(tokio::fs::File::from_std(file), size, max_bytes).await?;
    Ok(CreateAttachment::bytes(bytes, filename))
}

fn open_upload(allowed_root: &Path, cwd: &Path, path: &Path, max_bytes: u64) -> Result<(File, String, u64)> {
    if path.as_os_str().is_empty() {
        bail!("upload path must not be empty");
    }
    let root = allowed_root.canonicalize().context("cannot resolve upload allowed_root")?;
    let resolved = cwd.join(path).canonicalize().context("cannot resolve upload file")?;
    if !resolved.starts_with(&root) {
        bail!("upload file is outside omp.allowed_root");
    }
    let filename = resolved.file_name().and_then(|name| name.to_str())
        .context("upload file must have a valid UTF-8 basename")?;
    if filename.chars().any(|ch| ch.is_control() || ch == '\\') {
        bail!("upload basename must not contain control characters or path separators");
    }
    // O_NONBLOCK prevents a replacement FIFO from hanging open; O_NOFOLLOW rejects
    // a final-component symlink swapped in after canonicalization.
    let file = OpenOptions::new().read(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW)
        .open(&resolved).context("cannot open upload file")?;
    let metadata = file.metadata().context("cannot inspect opened upload file")?;
    if !metadata.is_file() {
        bail!("upload path must refer to a regular file");
    }
    // Linux reports the opened inode's actual path here. Check the descriptor,
    // not the pathname again: an ancestor may have been replaced with a symlink.
    let opened_path = std::fs::read_link(format!("/proc/self/fd/{}", file.as_raw_fd()))
        .context("cannot verify opened upload file containment")?;
    if !opened_path.starts_with(&root) || opened_path != resolved {
        bail!("opened upload file moved or escaped omp.allowed_root");
    }
    if metadata.len() > max_bytes {
        bail!("upload file exceeds the configured {max_bytes}-byte attachment limit");
    }
    Ok((file, filename.to_owned(), metadata.len()))
}

async fn read_bounded(reader: impl AsyncRead + Unpin, size: u64, max_bytes: u64) -> Result<Vec<u8>> {
    let read_limit = max_bytes.checked_add(1).context("attachment byte limit is too large")?;
    let capacity = usize::try_from(size).context("upload file is too large for this host")?;
    let mut bytes = Vec::with_capacity(capacity);
    reader.take(read_limit).read_to_end(&mut bytes).await.context("cannot read upload file")?;
    if bytes.len() as u64 > max_bytes {
        bail!("upload file exceeds the configured {max_bytes}-byte attachment limit");
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::{fs::symlink, net::UnixListener};

    #[tokio::test]
    async fn upload_relative_absolute_and_exact_size_boundary() {
        let root = tempfile::tempdir().unwrap();
        let cwd = root.path().join("work");
        std::fs::create_dir(&cwd).unwrap();
        let path = cwd.join("report.txt");
        std::fs::write(&path, b"hello").unwrap();
        for input in [PathBuf::from("report.txt"), path] {
            let (file, name, size) = open_upload(root.path(), &cwd, &input, 5).unwrap();
            assert_eq!(name, "report.txt");
            assert_eq!(read_bounded(tokio::fs::File::from_std(file), size, 5).await.unwrap(), b"hello");
        }
        assert!(open_upload(root.path(), &cwd, Path::new("report.txt"), 4).unwrap_err().to_string().contains("attachment limit"));
        std::fs::write(cwd.join("empty"), b"").unwrap();
        assert!(prepare_attachment(root.path().into(), cwd, "empty".into(), 0).await.is_ok());
    }

    #[tokio::test]
    async fn upload_reader_stops_at_limit_plus_one_if_file_grows() {
        let mut reader = std::io::Cursor::new(b"a growing file".as_slice());
        let error = read_bounded(&mut reader, 1, 4).await.unwrap_err();
        assert!(error.to_string().contains("attachment limit"));
        assert_eq!(reader.position(), 5);
    }

    #[test]
    fn upload_rejects_traversal_and_symlink_escape() {
        let root = tempfile::tempdir().unwrap();
        let allowed = root.path().join("allowed");
        std::fs::create_dir(&allowed).unwrap();
        std::fs::write(root.path().join("secret"), b"secret").unwrap();
        symlink(root.path().join("secret"), allowed.join("escape")).unwrap();
        symlink(root.path(), allowed.join("parent")).unwrap();
        for path in ["../secret", "escape", "parent/secret"] {
            assert!(open_upload(&allowed, &allowed, Path::new(path), 100).unwrap_err().to_string().contains("outside omp.allowed_root"));
        }
        // Component boundaries matter: a sibling sharing the root's prefix is not inside it.
        let sibling = root.path().join("allowed-other");
        std::fs::write(&sibling, b"no").unwrap();
        assert!(open_upload(&allowed, &allowed, &sibling, 100).is_err());
        std::fs::write(allowed.join("real.txt"), b"yes").unwrap();
        symlink(allowed.join("real.txt"), allowed.join("internal-link")).unwrap();
        assert_eq!(open_upload(&allowed, &allowed, Path::new("internal-link"), 100).unwrap().1, "real.txt");
    }

    #[test]
    fn upload_rejects_directories_nonregular_and_invalid_files() {
        let root = tempfile::tempdir().unwrap();
        assert!(open_upload(root.path(), root.path(), Path::new("."), 100).unwrap_err().to_string().contains("regular file"));
        let socket = root.path().join("socket");
        let _listener = UnixListener::bind(&socket).unwrap();
        assert!(open_upload(root.path(), root.path(), &socket, 100).is_err());
        assert!(open_upload(Path::new("/dev"), Path::new("/dev"), Path::new("null"), 100).unwrap_err().to_string().contains("regular file"));
        for path in ["", "missing"] {
            assert!(open_upload(root.path(), root.path(), Path::new(path), 100).is_err());
        }
        std::fs::write(root.path().join("bad\\name"), b"no").unwrap();
        assert!(open_upload(root.path(), root.path(), Path::new("bad\\name"), 100).is_err());
    }

    #[test]
    fn upload_caption_counts_utf16_and_caps_discord_limit() {
        assert!(validate_caption(None, 1900).is_ok());
        assert!(validate_caption(Some(&"🦀".repeat(950)), 1900).is_ok());
        assert!(validate_caption(Some(&"🦀".repeat(951)), 1900).is_err());
        assert!(validate_caption(Some(&"a".repeat(2001)), 4000).is_err());
        assert!(validate_caption(Some(&"a".repeat(101)), 100).is_err());
    }
}

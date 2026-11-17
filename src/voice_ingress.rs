//! Private same-user Unix-socket ingress for the opt-in voice companion.
use std::{path::Path, time::Duration};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader}, net::UnixStream};

pub const MAX_FRAME_BYTES: usize = 16_384;
pub const MAX_TRANSCRIPT_BYTES: usize = 6_000;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VoiceRequest {
    pub guild_id: u64,
    pub user_id: u64,
    pub thread_id: u64,
    /// None validates a destination without posting or starting a turn.
    pub text: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VoiceResponse {
    pub ok: bool,
    pub message: String,
    pub message_id: Option<u64>,
}

pub async fn read_frame<T: DeserializeOwned>(reader: impl AsyncRead + Unpin) -> Result<T> {
    let mut bytes = Vec::new();
    BufReader::new(reader.take((MAX_FRAME_BYTES + 1) as u64)).read_until(b'\n', &mut bytes).await?;
    ensure!(bytes.len() <= MAX_FRAME_BYTES && bytes.last() == Some(&b'\n'), "invalid or oversized voice frame");
    serde_json::from_slice(&bytes).context("invalid voice protocol JSON")
}

pub async fn write_frame<T: Serialize>(mut writer: impl AsyncWrite + Unpin, value: &T) -> Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    ensure!(bytes.len() <= MAX_FRAME_BYTES, "voice frame exceeds limit");
    writer.write_all(&bytes).await?;
    Ok(())
}

/// No retries: a failed acknowledgement may follow a successfully admitted turn.
pub async fn request(path: &Path, value: &VoiceRequest) -> Result<VoiceResponse> {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut stream = UnixStream::connect(path).await.context("voice bridge socket unavailable")?;
        write_frame(&mut stream, value).await?;
        read_frame(&mut stream).await
    }).await.context("voice bridge acknowledgement timed out; delivery may have occurred")?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn malformed_and_oversized_input_never_becomes_a_request() {
        let invalid = [
            b"{\"guild_id\":1,\"user_id\":2,\"thread_id\":3,\"text\":null,\"admin\":true}\n".as_slice(),
            b"{\"guild_id\":1,\"user_id\":2,\"thread_id\":3,\"text\":null}".as_slice(),
            b"null\n".as_slice(),
        ];
        for bytes in invalid {
            assert!(read_frame::<VoiceRequest>(bytes).await.is_err());
        }
        let oversized = vec![b' '; MAX_FRAME_BYTES + 1000];
        assert!(read_frame::<VoiceRequest>(oversized.as_slice()).await.is_err());
    }
}

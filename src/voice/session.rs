//! OMP-backed live voice session.
//!
//! One spawned OMP process hosts the voice session for the lifetime of a
//! voice channel session: `live_start` brings the session up, then the frames
//! `live_transcript` / `live_phase` / `live_end` carry the conversation out.
//! The audio itself never flows through this module — OMP captures the
//! default source and plays the default sink (the virtual sinks routed in
//! `relay`). This file only starts and stops the live session and watches
//! its frames.

use std::{path::Path, process::Stdio, time::Duration};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    process::{Child, Command},
    sync::oneshot,
    time::timeout,
};

use crate::{
    config::OmpConfig,
    omp::{expect_ready, next_frame, take_frames},
};

/// How long to wait for the live session to come up.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
/// How long to wait for a `live_stop` response.
const STOP_TIMEOUT: Duration = Duration::from_secs(10);

pub struct OmpVoiceSession {
    inner: Option<Inner>,
}

struct Inner {
    child: Child,
    stop_tx: tokio::sync::watch::Sender<bool>,
    frame_task: tokio::task::JoinHandle<()>,
}

/// Outcome reported by the background frame loop once the session is live.
#[derive(Debug, Clone)]
pub struct VoiceStarted {
    /// The voice omp selected for the session.
    pub voice: String,
}

/// Start the live session. Blocks until OMP reports the session live or fails.
pub async fn start(
    config: &OmpConfig,
    cwd: &Path,
    instructions: &str,
) -> Result<(OmpVoiceSession, VoiceStarted)> {
    let mut command = Command::new(&config.executable);
    command
        .arg("--mode")
        .arg("rpc-ui")
        .arg("--session-dir")
        .arg(&config.session_dir)
        .arg("--cwd")
        .arg(cwd)
        .arg("--no-title")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(model) = config.model.as_deref() {
        command.arg("--model").arg(model);
    }
    if let Some(level) = config.thinking_level.as_deref() {
        command.arg("--thinking").arg(level);
    }

    tokio::fs::create_dir_all(&config.session_dir)
        .await
        .context("failed to create OMP voice session directory")?;

    let mut child = command
        .spawn()
        .with_context(|| format!("failed to start OMP: {}", config.executable.display()))?;

    if let Some(mut stderr) = child.stderr.take() {
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            let mut buffer = Vec::new();
            let _ = stderr.read_to_end(&mut buffer).await;
        });
    }

    let stdin = child
        .stdin
        .take()
        .context("OMP voice stdin unavailable")?;
    let mut frames = take_frames(&mut child)
        .map_err(|error| anyhow::anyhow!("failed to read OMP voice frames: {error}"))?;

    let instructions_for_task = instructions.to_owned();
    let (ready_tx, ready_rx) = oneshot::channel::<Result<VoiceStarted, String>>();
    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    let mut input = BufWriter::new(stdin);

    let frame_task = tokio::spawn(async move {
        // Startup: expect the ready frame, send live_start, await its response.
        let startup = async {
            expect_ready(&mut frames).await?;
            crate::omp::send_frame(
                &mut input,
                &json!({
                    "id": "voice-start",
                    "type": "live_start",
                    "instructions": instructions_for_task,
                }),
            )
            .await?;
            loop {
                let frame = next_frame(frames.next().await, false)
                    .map_err(|e| anyhow::anyhow!("OMP voice startup failed: {e}"))?;
                if frame.get("type").and_then(Value::as_str) == Some("response")
                    && frame.get("id").and_then(Value::as_str) == Some("voice-start")
                {
                    if frame.get("success").and_then(Value::as_bool) != Some(true) {
                        let error = frame
                            .get("error")
                            .and_then(Value::as_str)
                            .unwrap_or("OMP rejected live_start")
                            .to_owned();
                        return Err(anyhow::anyhow!(error));
                    }
                    let voice = frame
                        .pointer("/data/voice")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned();
                    return Ok(VoiceStarted { voice });
                }
                // A live frame arriving before the start response: log and keep waiting.
                log_frame(&frame);
            }
        }
        .await;

        let started = match startup {
            Ok(started) => {
                let _ = ready_tx.send(Ok(started.clone()));
                started
            }
            Err(error) => {
                let _ = ready_tx.send(Err(format!("{error:#}")));
                let _ = input.shutdown().await;
                return;
            }
        };
        let _ = started;

        // Main loop: forward conversation frames until the session ends.
        loop {
            tokio::select! {
                _ = stop_rx.changed() => {
                    let _ = crate::omp::send_frame(
                        &mut input,
                        &json!({
                            "id": "voice-stop",
                            "type": "live_stop",
                        }),
                    )
                    .await;
                    let wait = async {
                        loop {
                            let frame = next_frame(frames.next().await, false);
                            match frame {
                                Ok(frame) => {
                                    if frame.get("type").and_then(Value::as_str) == Some("response")
                                        && frame.get("id").and_then(Value::as_str) == Some("voice-stop")
                                    {
                                        break;
                                    }
                                    log_frame(&frame);
                                }
                                Err(_) => break,
                            }
                        }
                    };
                    let _ = timeout(STOP_TIMEOUT, wait).await;
                    break;
                }
                frame = frames.next() => {
                    let Some(frame) = frame else { break };
                    match next_frame(Some(frame), false) {
                        Ok(parsed) => {
                            if parsed.get("type").and_then(Value::as_str) == Some("live_end") {
                                break;
                            }
                            log_frame(&parsed);
                        }
                        Err(error) => {
                            tracing::warn!(error = %error, "omp voice session read error");
                            break;
                        }
                    }
                }
            }
        }

        let _ = input.shutdown().await;
    });

    let ready = timeout(STARTUP_TIMEOUT, ready_rx)
        .await
        .context("timed out waiting for the OMP live session")?
        .context("OMP live session task ended before reporting")?;

    let started = match ready {
        Ok(started) => started,
        Err(error) => {
            // Half-started session: abort the frame task and kill the child
            // so it does not linger holding the stdout.
            frame_task.abort();
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Err(anyhow::anyhow!("OMP live session failed: {error}"));
        }
    };

    let session = OmpVoiceSession {
        inner: Some(Inner {
            child,
            stop_tx,
            frame_task,
        }),
    };

    Ok((session, started))
}

fn log_frame(frame: &Value) {
    match frame.get("type").and_then(Value::as_str) {
        Some("live_transcript") if frame.get("final").and_then(Value::as_bool) == Some(true) => {
            let role = frame.get("role").and_then(Value::as_str).unwrap_or("?");
            let text = frame.get("text").and_then(Value::as_str).unwrap_or("");
            tracing::info!(role, "voice transcript: {}", text);
        }
        Some("live_phase") => {
            if let Some(phase) = frame.get("phase").and_then(Value::as_str) {
                tracing::info!(phase, "voice phase");
            }
        }
        Some("live_end") => {
            let error = frame.get("error").and_then(Value::as_str);
            tracing::info!(error = ?error, "voice session ended");
        }
        _ => {}
    }
}

impl OmpVoiceSession {
    /// Stop the session and release the backend. Idempotent.
    pub async fn stop(&mut self) {
        let Some(mut inner) = self.inner.take() else { return };
        let _ = inner.stop_tx.send(true);
        // Give the frame task a moment to deliver the live_stop response.
        let _ = timeout(STOP_TIMEOUT + Duration::from_secs(5), inner.frame_task).await;
        let _ = inner.child.start_kill();
        let _ = inner.child.wait().await;
    }

    /// Re-check that the session task is still running (it ends on session end).
    pub fn is_running(&mut self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|inner| !inner.frame_task.is_finished())
    }
}

impl Drop for OmpVoiceSession {
    fn drop(&mut self) {
        if let Some(mut inner) = self.inner.take() {
            // Signal the frame task (it exits immediately on the next frame or
            // channel tick) and kill the child; kill_on_drop also applies.
            let _ = inner.stop_tx.send(true);
            let _ = inner.child.start_kill();
        }
    }
}

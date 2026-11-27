//! Virtual audio routing for the OMP live session.
//!
//! OMP's native live session captures the *default* source and plays the
//! *default* sink. There is no device-selection setting anywhere in the live
//! path, so this module remaps those two defaults around the session: the
//! default source becomes a null sink's monitor (which carries the operator's
//! Discord speech) and the default sink becomes a null sink (whose monitor
//! carries OMP's reply audio). Everything else — transcripts, delegations,
//! signaling — stays exactly OMP's own, and the defaults are restored when
//! the session ends.
//!
//! Why two sinks instead of one: the out-monitor tap and the mic leg both read
//! a monitor, but a shared sink would let the bridge hear its own playback in
//! the out-monitor. Keeping the two directions apart avoids that feedback loop.

use std::process::Stdio;

use anyhow::{Context, Result, bail};
use tokio::process::{Child, Command};

/// Name of the null sink fed from Discord audio (the mic leg).
pub const MIC_SINK: &str = "omp-voice-mic";
/// Name of the null sink OMP plays into; the bridge reads its monitor.
pub const OUT_SINK: &str = "omp-voice-out";
/// `pactl module-null-sink` var ids recorded so cleanup is exact.
struct RelayResources {
    mic_module: String,
    out_module: String,
    old_default_source: String,
    old_default_sink: String,
}

// `Child` is deliberately `!Send`-ish by construction; tokio handles it.

/// Run a single argv and return trimmed stdout, erroring on failure.
async fn run_quiet(program: &str, args: &[&str]) -> Result<String> {
    let output = tokio::process::Command::new(program)
        .args(args)
        .output()
        .await
        .with_context(|| format!("failed to run `{program}`"))?;
    if !output.status.success() {
        bail!(
            "`{program} {}` failed ({}): {}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim(),
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Run a single argv and tolerate a failure (used for restoring defaults).
async fn run_best_effort(program: &str, args: &[&str]) {
    let _ = tokio::process::Command::new(program).args(args).output().await;
}

async fn load_null_sink(name: &str) -> Result<String> {
    // pactl prints the newly created module's var id on stdout.
    run_quiet(
        "pactl",
        &[
            "load-module",
            "module-null-sink",
            &format!("sink_name={name}"),
            "sink_properties=device.description=OMP Voice Null Sink",
        ],
    )
    .await
    .with_context(|| format!("could not load virtual sink {name}"))
}

/// Virtual-device routing state for one live voice session.
pub struct AudioRelay {
    resources: RelayResources,
}

impl AudioRelay {
    /// Create both null sinks and remap the default source/sink onto them.
    ///
    /// Cleanup on failure restores whatever was already in place.
    pub async fn start() -> Result<AudioRelay> {
        let old_default_source = run_quiet("pactl", &["get-default-source"])
            .await
            .context("could not read the current default source")?;
        let old_default_sink = run_quiet("pactl", &["get-default-sink"])
            .await
            .context("could not read the current default sink")?;

        let mic_module = match load_null_sink(MIC_SINK).await {
            Ok(module) => module,
            Err(error) => {
                return Err(error).context("could not create mic virtual sink");
            }
        };
        let out_module = match load_null_sink(OUT_SINK).await {
            Ok(module) => module,
            Err(error) => {
                let _ = run_quiet("pactl", &["unload-module", &mic_module]).await;
                return Err(error).context("could not create out virtual sink");
            }
        };

        let mic_monitor = format!("{MIC_SINK}.monitor");
        if let Err(error) = run_quiet("pactl", &["set-default-source", &mic_monitor]).await {
            let _ = run_quiet("pactl", &["unload-module", &out_module]).await;
            let _ = run_quiet("pactl", &["unload-module", &mic_module]).await;
            return Err(error).context("could not remap default source to virtual sink monitor");
        }
        if let Err(error) = run_quiet("pactl", &["set-default-sink", OUT_SINK]).await {
            let _ = run_best_effort("pactl", &["set-default-source", &old_default_source]).await;
            let _ = run_quiet("pactl", &["unload-module", &out_module]).await;
            let _ = run_quiet("pactl", &["unload-module", &mic_module]).await;
            return Err(error).context("could not remap default sink to virtual sink");
        }

        Ok(AudioRelay {
            resources: RelayResources {
                mic_module,
                out_module,
                old_default_source,
                old_default_sink,
            },
        })
    }

    /// Restore the original defaults and unload the virtual sinks.
    ///
    /// Best effort by design: the caller's voice session must not be pinned
    /// by a failing unload, and the defaults are restored first so the
    /// desktop audio path is usable even if a sink unload fails.
    pub async fn stop(self) {
        let _ = run_quiet("pactl", &["set-default-source", &self.resources.old_default_source]).await;
        let _ = run_quiet("pactl", &["set-default-sink", &self.resources.old_default_sink]).await;
        let _ = run_best_effort("pactl", &["unload-module", &self.resources.mic_module]).await;
        let _ = run_best_effort("pactl", &["unload-module", &self.resources.out_module]).await;
    }
}

impl Drop for AudioRelay {
    /// Best-effort restore on the abort path. The explicit `stop()` call in
    /// teardown is the norm; this path keeps the desktop audio path usable if
    /// the session task is cancelled before teardown runs.
    fn drop(&mut self) {
        let resources = std::mem::replace(
            &mut self.resources,
            RelayResources {
                mic_module: String::new(),
                out_module: String::new(),
                old_default_source: String::new(),
                old_default_sink: String::new(),
            },
        );
        if std::env::var_os("OMP_VOICE_RELAY_SUPPRESS_DROP").is_some() {
            return;
        }
        tokio::spawn(async move {
            let _ = run_quiet("pactl", &["set-default-source", &resources.old_default_source]).await;
            let _ = run_quiet("pactl", &["set-default-sink", &resources.old_default_sink]).await;
            let _ = run_best_effort("pactl", &["unload-module", &resources.mic_module]).await;
            let _ = run_best_effort("pactl", &["unload-module", &resources.out_module]).await;
        });
    }
}

impl std::fmt::Debug for AudioRelay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AudioRelay")
            .field("mic_module", &self.resources.mic_module)
            .field("out_module", &self.resources.out_module)
            .finish()
    }
}

/// Mic-leg writer: streams Discord audio into the virtual mic sink.
///
/// Spawns `pw-cat --playback` reading raw f32le mono 16 kHz (the format omp's
/// native capture requests); the sink resamples to its native rate for the
/// monitor. Dropping this struct kills the child.
pub struct MicWriter {
    inner: Option<Child>,
    stdin: Option<tokio::process::ChildStdin>,
}

impl MicWriter {
    pub fn spawn() -> Result<MicWriter> {
        let mut child = Command::new("pw-cat")
            .args([
                "--playback",
                "--raw",
                "--target",
                MIC_SINK,
                "--rate",
                "16000",
                "--channels",
                "1",
                "--format",
                "f32",
                "-",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context("failed to start pw-cat for the mic relay")?;

        let stdin = child.stdin.take().context("pw-cat stdin unavailable")?;
        Ok(MicWriter {
            inner: Some(child),
            stdin: Some(stdin),
        })
    }

    /// Write raw 16 kHz mono f32le bytes. Fails after the child exits.
    pub async fn write(&mut self, bytes: &[u8]) -> Result<()> {
        let stdin = self.stdin.as_mut().context("pw-cat stdin already closed")?;
        use tokio::io::AsyncWriteExt;
        stdin
            .write_all(bytes)
            .await
            .context("failed to write Discord audio to the virtual mic sink")?;
        stdin.flush().await.context("failed to flush Discord audio to the virtual mic sink")?;
        Ok(())
    }

    /// True if the child has exited (caller should tear down the session).
    pub fn is_dead(&mut self) -> bool {
        match self.inner.as_mut() {
            Some(child) => matches!(child.try_wait(), Ok(Some(_)) | Err(_)),
            None => true,
        }
    }

    pub fn kill(&mut self) {
        if let Some(mut child) = self.inner.take() {
            let _ = child.start_kill();
        }
        self.stdin.take();
    }
}

impl Drop for MicWriter {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Out-leg reader: streams the out sink's monitor (OMP's reply audio).
pub struct OutReader {
    inner: Option<Child>,
    stdout: Option<tokio::process::ChildStdout>,
}

impl OutReader {
    /// Spawn `pw-record` on the out sink's monitor. OMP's own playback stays
    /// at its negotiated rate; the child emits whatever the monitor delivers
    /// (default 48 kHz stereo f32le).
    pub fn spawn() -> Result<OutReader> {
        let mut child = Command::new("pw-record")
            .args([
                "--target",
                &format!("{OUT_SINK}.monitor"),
                "--rate",
                "48000",
                "--channels",
                "2",
                "--format",
                "f32",
                "-",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context("failed to start pw-record for the out relay")?;

        let stdout = child.stdout.take().context("pw-record stdout unavailable")?;
        Ok(OutReader {
            inner: Some(child),
            stdout: Some(stdout),
        })
    }

    /// Read raw bytes (f32le stereo 48 kHz). Returns an empty Vec on EOF.
    pub async fn read(&mut self, max_bytes: usize) -> Result<Vec<u8>> {
        use tokio::io::AsyncReadExt;
        let stdout = self.stdout.as_mut().context("pw-record stdout already closed")?;
        let mut buffer = vec![0u8; max_bytes];
        let mut filled = 0usize;
        while filled < max_bytes {
            let chunk = stdout
                .read(&mut buffer[filled..])
                .await
                .context("failed to read the out-sink monitor")?;
            if chunk == 0 {
                break;
            }
            filled += chunk;
        }
        buffer.truncate(filled);
        Ok(buffer)
    }

    pub fn is_dead(&mut self) -> bool {
        match self.inner.as_mut() {
            Some(child) => matches!(child.try_wait(), Ok(Some(_)) | Err(_)),
            None => true,
        }
    }

    pub fn kill(&mut self) {
        if let Some(mut child) = self.inner.take() {
            let _ = child.start_kill();
        }
        self.stdout.take();
    }
}

impl Drop for OutReader {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Convert mono 16 kHz i16 capture samples into mono 16 kHz f32le bytes.
pub fn mono_i16_to_f32le(samples: &[i16], out: &mut Vec<u8>) {
    // Reserve 4 bytes per sample.
    let start = out.len();
    out.resize(start + samples.len() * 4, 0);
    for (index, sample) in samples.iter().enumerate() {
        let f32 = *sample as f32 / 32768.0;
        let offset = start + index * 4;
        out[offset..offset + 4].copy_from_slice(&f32.to_le_bytes());
    }
}

/// Downmix 48 kHz stereo f32le (out leg, stereo pairs) to mono f32.
pub fn stereo_f32le_to_mono_f32(bytes: &[u8], out: &mut Vec<f32>) {
    let pairs = bytes.len() / 8;
    for index in 0..pairs {
        let offset = index * 8;
        let left = f32::from_le_bytes([bytes[offset], bytes[offset + 1], bytes[offset + 2], bytes[offset + 3]]);
        let right = f32::from_le_bytes([bytes[offset + 4], bytes[offset + 5], bytes[offset + 6], bytes[offset + 7]]);
        out.push((left + right) * 0.5);
    }
}


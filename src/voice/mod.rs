//! Voice bridge: joins the operator's voice channel when they enter it and
//! pipes the operator's speech to OMP's live session and OMP's reply audio
//! back into the channel.
//!
//! The feature is entirely config-gated: when the `[voice]` section is absent
//! the bridge is never constructed and every event hook is a no-op.
//!
//! Pipeline:
//! 1. `voice_state_update` reports an operator entering/leaving a voice channel.
//! 2. On entry the bridge joins the channel, routes audio through two virtual
//!    PipeWire sinks (see `relay`), spawns one OMP live session for the
//!    lifetime of the channel session, and spawns the capture loop.
//! 3. Discord speech (16 kHz mono i16 from the channel) is forwarded to the
//!    virtual mic sink; OMP captures it and streams it to the remote live
//!    server through its native WebRTC path.
//! 4. OMP's reply audio plays into the virtual out sink; the bridge reads its
//!    monitor and plays it back into the channel.
//! 5. On exit or on session end the session is torn down (legs killed,
//!    defaults restored, sinks unloaded) and the bridge leaves the channel.

use std::{
    collections::{HashMap, HashSet},
    io::Cursor,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use serenity::all::{ChannelId, GuildId, VoiceState};
use songbird::{CoreEvent, Event, EventContext, Songbird, tracks::Track};
use tokio::sync::watch;

use crate::config::{OmpConfig, VoiceConfig};
use crate::voice::relay::{AudioRelay, MicWriter, OutReader, mono_i16_to_f32le};
use crate::voice::session::OmpVoiceSession;

pub mod relay;
pub mod session;

/// Audio in the out leg (OMP's reply) is played at 48 kHz stereo f32le.
const OUT_SAMPLE_RATE: u32 = 48_000;
const OUT_CHANNELS: u32 = 2;
/// Bounded wait for a session task to tear down before falling back to
/// abort. Set above the longest expected teardown (live_stop 10 s + 5 s
/// grace + pactl waits).
const SESSION_TEARDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Target size of one playback chunk (~150 ms of 48 kHz stereo f32le audio).
const PLAY_CHUNK_BYTES: usize = OUT_SAMPLE_RATE as usize * 2 * 4 * 150 / 1000;
/// Read granularity of the out-monitor pipe.
const OUT_READ_CHUNK_BYTES: usize = 4096;

/// The bridge that joins voice channels on behalf of the operator.
pub struct VoiceBridge {
    config: VoiceConfig,
    omp: OmpConfig,
    manager: Arc<Songbird>,
    bot_name: Arc<RwLock<String>>,
    /// guild id → working directory for the OMP session.
    guild_cwds: Arc<HashMap<u64, PathBuf>>,
    /// guild id → live session, fenced by generation.
    sessions: Arc<Mutex<HashMap<u64, SessionState>>>,
}

struct SessionState {
    generation: u64,
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl VoiceBridge {
    pub fn new(
        config: VoiceConfig,
        omp: OmpConfig,
        guild_cwds: HashMap<u64, PathBuf>,
        manager: Arc<Songbird>,
    ) -> Self {
        Self {
            config,
            omp,
            manager,
            bot_name: Arc::new(RwLock::new("Voice Agent".to_owned())),
            guild_cwds: Arc::new(guild_cwds),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set_bot_name(&self, name: String) {
        *self.bot_name.write().expect("bot name lock") = name;
    }

    pub fn bot_name(&self) -> String {
        self.bot_name.read().expect("bot name lock").clone()
    }

    pub fn allowed_guild(&self, guild_id: u64) -> bool {
        self.guild_cwds.contains_key(&guild_id)
    }

    /// Handle a `voice_state_update`. Only the configured operator's own state
    /// changes bridge membership.
    pub async fn on_voice_state_update(&self, new: &VoiceState) -> Result<()> {
        let operator_id = self.config.operator_user_id;
        let Some(guild_id) = new.guild_id else {
            return Ok(());
        };
        let guild_id = guild_id.get();
        if !self.allowed_guild(guild_id) {
            return Ok(());
        }
        if new.user_id.get() != operator_id {
            return Ok(());
        }
        match new.channel_id {
            Some(channel) => self.join(guild_id, channel.get()).await?,
            None => self.leave(guild_id).await?,
        }
        Ok(())
    }

    /// Join the operator's channel and spawn the session task.
    pub async fn join(&self, guild_id: u64, channel_id: u64) -> Result<()> {
        let Some(cwd) = self.guild_cwds.get(&guild_id).cloned() else {
            bail!("no configured cwd for guild {guild_id}");
        };
        // Tear down an existing session for this guild first. Await its
        // bounded teardown (the task restores defaults, kills omp, unloads
        // the virtual sinks) rather than aborting blindly; abort after the
        // timeout as a last resort.
        let generation = {
            let previous = {
                let mut sessions = self.sessions.lock().expect("sessions lock");
                sessions.remove(&guild_id)
            };
            let generation = match previous {
                Some(previous) => {
                    let _ = previous.stop.send(true);
                    let mut task = previous.task;
                    let _ = tokio::time::timeout(SESSION_TEARDOWN_TIMEOUT, &mut task).await;
                    if !task.is_finished() {
                        task.abort();
                    }
                    previous.generation + 1
                }
                None => 1,
            };
            generation
        };

        let call = self.manager.get_or_insert(GuildId::new(guild_id));
        {
            let mut call = call.lock().await;
            call.mute(false).await?;
            call.deafen(false).await?;
        }
        if let Err(error) = self
            .manager
            .join(GuildId::new(guild_id), ChannelId::new(channel_id))
            .await
        {
            // The session is already torn down; make sure no half-open
            // voice state lingers for this guild.
            let _ = self.manager.leave(GuildId::new(guild_id)).await;
            return Err(error).context("failed to join voice channel");
        }
        tracing::info!(
            guild_id,
            channel_id,
            generation,
            "voice bridge joined channel"
        );

        let (stop, stop_rx) = watch::channel(false);
        let bot_name = self.bot_name();
        let config = self.config.clone();
        let omp = self.omp.clone();
        let manager = Arc::clone(&self.manager);
        let sessions = Arc::clone(&self.sessions);
        let task = tokio::spawn(async move {
            let session = VoiceSession {
                guild_id,
                generation,
                bot_name,
                cwd,
                config,
                omp,
                manager,
                sessions,
            };
            session.run(stop_rx).await;
        });
        {
            let mut sessions = self.sessions.lock().expect("sessions lock");
            sessions.insert(
                guild_id,
                SessionState {
                    generation,
                    stop,
                    task,
                },
            );
        }
        Ok(())
    }

    /// Leave the operator's channel and tear down the session.
    pub async fn leave(&self, guild_id: u64) -> Result<()> {
        {
            let state = {
                let mut sessions = self.sessions.lock().expect("sessions lock");
                sessions.remove(&guild_id)
            };
            if let Some(state) = state {
                let _ = state.stop.send(true);
                let mut task = state.task;
                let _ = tokio::time::timeout(SESSION_TEARDOWN_TIMEOUT, &mut task).await;
                if !task.is_finished() {
                    task.abort();
                }
            }
        }
        if self.manager.get(GuildId::new(guild_id)).is_some() {
            let _ = self.manager.leave(GuildId::new(guild_id)).await;
            tracing::info!(guild_id, "voice bridge left channel");
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The per-guild session
// ---------------------------------------------------------------------------

struct VoiceSession {
    guild_id: u64,
    generation: u64,
    bot_name: String,
    cwd: PathBuf,
    config: VoiceConfig,
    omp: OmpConfig,
    manager: Arc<Songbird>,
    sessions: Arc<Mutex<HashMap<u64, SessionState>>>,
}

impl VoiceSession {
    async fn run(&self, mut stop: watch::Receiver<bool>) {
        if let Err(error) = self.run_inner(&mut stop).await {
            tracing::warn!(guild_id = self.guild_id, error = %error, "voice session ended with error");
        }
        // Tidy up our own session state only if we are still the current generation.
        let mut sessions = self.sessions.lock().expect("sessions lock");
        if sessions
            .get(&self.guild_id)
            .is_some_and(|s| s.generation == self.generation)
        {
            sessions.remove(&self.guild_id);
        }
    }

    /// Run the session until the operator leaves or the session ends.
    async fn run_inner(&self, stop: &mut watch::Receiver<bool>) -> Result<()> {
        let mut relay: Option<AudioRelay>;
        let mut session: Option<OmpVoiceSession> = None;

        // Remap the default source/sink onto the virtual sinks before OMP
        // spawns, so its native capture/playback flows through the
        // Discord-channel legs without touching the real devices.
        relay = Some(AudioRelay::start().await.context("starting virtual audio relay")?);

        // Spawn the live session.
        let instructions = self.config.render_system_prompt(&self.bot_name);
        match crate::voice::session::start(&self.omp, &self.cwd, &instructions).await {
            Ok((session_inst, started)) => {
                tracing::info!(
                    guild_id = self.guild_id,
                    voice = %started.voice,
                    "OMP live session started"
                );
                session = Some(session_inst);
            }
            Err(error) => {
                teardown(session.take(), relay.take()).await;
                return Err(error).context("starting OMP live session");
            }
        }

        // Spawn the two media legs. Both failures tear down cleanly.
        let spawn_legs = async {
            let mic = MicWriter::spawn();
            let out = OutReader::spawn();
            match (mic, out) {
                (Ok(mic), Ok(out)) => Ok((mic, out)),
                (mic_result, out_result) => {
                    // Kill whichever legs started; their Drop impls would
                    // kill too, but do it now so failure is synchronous.
                    if let Ok(mut writer) = mic_result {
                        writer.kill();
                    }
                    if let Ok(mut reader) = out_result {
                        reader.kill();
                    }
                    Err(anyhow!("media leg startup failed"))
                }
            }
        };
        let (mut mic_writer, mut out_reader) = match spawn_legs.await {
            Ok(legs) => legs,
            Err(error) => {
                teardown(session.take(), relay.take()).await;
                return Err(error).context("starting audio legs");
            }
        };

        // Attach the capture event to the voice call.
        let (pcm_tx, mut pcm_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<i16>>();
        if let Err(error) = self.attach_capture(pcm_tx).await {
            drop(mic_writer);
            drop(out_reader);
            teardown(session.take(), relay.take()).await;
            return Err(error).context("attaching voice channel capture");
        }

        // Mic leg: channel PCM → virtual mic sink (omp's default source).
        // Polled in place rather than spawned, so stopping this session
        // cannot orphan a writer task holding a live pw-cat child.
        // Dropping the pinned future drops the MicWriter/OutReader, which
        // kill the pw-cat/pw-record children.
        let mut mic_leg = Box::pin(async move {
            let mut buffer = Vec::new();
            while let Some(pcm) = pcm_rx.recv().await {
                buffer.clear();
                mono_i16_to_f32le(&pcm, &mut buffer);
                if mic_writer.write(&buffer).await.is_err() {
                    tracing::warn!("mic relay writer failed; stopping mic leg");
                    return;
                }
            }
        });

        // Out leg: virtual out sink monitor → channel playback.
        let manager = Arc::clone(&self.manager);
        let guild_id = self.guild_id;
        let mut out_leg = Box::pin(async move {
            let mut buffer: Vec<u8> = Vec::new();
            loop {
                let bytes = match out_reader.read(OUT_READ_CHUNK_BYTES).await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        tracing::warn!(error = %error, "out relay read failed");
                        break;
                    }
                };
                if bytes.is_empty() {
                    // EOF: the out sink or pw-record went away.
                    break;
                }
                buffer.extend_from_slice(&bytes);
                if buffer.len() >= PLAY_CHUNK_BYTES {
                    play_chunk(&manager, guild_id, &buffer).await;
                    buffer.clear();
                }
            }
            if !buffer.is_empty() {
                play_chunk(&manager, guild_id, &buffer).await;
            }
        });

        // Main wait loop: stop requested, either media leg dies, or omp ends.
        let result = loop {
            tokio::select! {
                _ = stop.changed() => break Ok(()),
                _ = &mut mic_leg => {
                    tracing::warn!("mic leg ended");
                    break Ok(());
                }
                _ = &mut out_leg => {
                    tracing::warn!("out leg ended");
                    break Ok(());
                }
                _ = tokio::time::sleep(Duration::from_millis(250)) => {
                    let running = session
                        .as_mut()
                        .is_some_and(|s| s.is_running());
                    if !running {
                        tracing::warn!("OMP live session ended");
                        break Ok(());
                    }
                }
            }
        };

        // Explicitly drop the legs before the relay retires: the drop kills
        // the pw-cat/pw-record children so the virtual sinks are not read
        // after teardown.
        drop(mic_leg);
        drop(out_leg);

        teardown(session.take(), relay.take()).await;
        result
    }

    /// Attach a global capture event to the voice call.
    async fn attach_capture(
        &self,
        pcm_tx: tokio::sync::mpsc::UnboundedSender<Vec<i16>>,
    ) -> Result<()> {
        let call = self.manager.get_or_insert(GuildId::new(self.guild_id));
        let mut call = call.lock().await;
        call.remove_all_global_events();
        let receiver = CaptureReceiver {
            operator_id: self.config.operator_user_id,
            operator_ssrcs: Arc::new(Mutex::new(HashSet::new())),
            pcm_tx,
        };
        for event in [
            CoreEvent::SpeakingStateUpdate,
            CoreEvent::VoiceTick,
            CoreEvent::ClientDisconnect,
            CoreEvent::DriverConnect,
            CoreEvent::DriverReconnect,
            CoreEvent::DriverDisconnect,
        ] {
            call.add_global_event(event.into(), receiver.clone());
        }
        Ok(())
    }
}

/// Tear down every resource we created, in dependency order.
async fn teardown(mut session: Option<OmpVoiceSession>, mut relay: Option<AudioRelay>) {
    if let Some(mut session_) = session.take() {
        session_.stop().await;
    }
    if let Some(relay_) = relay.take() {
        relay_.stop().await;
    }
}

/// Play one chunk of out-monitor PCM back into the channel.
async fn play_chunk(manager: &Arc<Songbird>, guild_id: u64, bytes: &[u8]) {
    let input: songbird::input::Input = songbird::input::RawAdapter::new(
        Cursor::new(bytes.to_vec()),
        OUT_SAMPLE_RATE,
        OUT_CHANNELS,
    )
    .into();
    let Some(call) = manager.get(GuildId::new(guild_id)) else {
        tracing::warn!(guild_id, "voice call unavailable; dropping out-chunk");
        return;
    };
    let mut call = call.lock().await;
    let _track = call.play_only(Track::from(input).volume(0.9));
}

// ---------------------------------------------------------------------------
// Capture receiver
// ---------------------------------------------------------------------------

/// Event handler that feeds operator PCM into the session.
///
/// SSRCs are mapped to users via `SpeakingStateUpdate` events and only the
/// configured operator's SSRCs are captured.
#[derive(Clone)]
struct CaptureReceiver {
    pcm_tx: tokio::sync::mpsc::UnboundedSender<Vec<i16>>,
    operator_id: u64,
    operator_ssrcs: Arc<Mutex<HashSet<u32>>>,
}

#[async_trait::async_trait]
impl songbird::EventHandler for CaptureReceiver {
    async fn act(&self, event: &EventContext<'_>) -> Option<Event> {
        match event {
            EventContext::SpeakingStateUpdate(update) => {
                let mut ssrcs = self.operator_ssrcs.lock().expect("ssrc lock");
                match update.user_id {
                    Some(user_id) if user_id.0 == self.operator_id => {
                        if update.speaking.microphone() {
                            ssrcs.insert(update.ssrc);
                        } else {
                            ssrcs.remove(&update.ssrc);
                        }
                    }
                    Some(_) => {
                        ssrcs.remove(&update.ssrc);
                    }
                    None => {
                        // Unknown user: do not widen capture to unknown speakers.
                    }
                }
            }
            EventContext::VoiceTick(tick) => {
                let ssrcs = self.operator_ssrcs.lock().expect("ssrc lock");
                for (ssrc, voice) in &tick.speaking {
                    if !ssrcs.contains(ssrc) {
                        continue;
                    }
                    if let Some(pcm) = voice.decoded_voice.as_deref()
                        && !pcm.is_empty()
                    {
                        self.pcm_tx.send(pcm.to_vec()).ok();
                    }
                }
            }
            _ => {}
        }
        None
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn config_validation_surfaces_default_prompt() {
        let config = crate::config::VoiceConfig {
            operator_user_id: 345678901234567890,
            system_prompt: crate::config::DEFAULT_VOICE_SYSTEM_PROMPT.to_owned(),
        };
        assert!(config.system_prompt.contains("{bot_name}"));
        assert!(config.render_system_prompt("Aria").contains("Aria"));
    }
}

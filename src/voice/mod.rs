//! Voice bridge: joins the operator's voice channel when they enter it,
//! detects speech, hands the speech to the voice model, and pipes the model's
//! audio back into the channel.
//!
//! The feature is entirely config-gated: when the `[voice]` section is absent
//! the bridge is never constructed and every event hook is a no-op.
//!
//! Pipeline:
//! 1. `voice_state_update` reports an operator entering/leaving a voice channel.
//! 2. On entry the bridge joins the channel and spawns one session task.
//! 3. Captured operator speech is resampled to 16 kHz PCM16 and appended to the
//!    realtime input buffer, committed, and a response is created.
//! 4. The voice model's `output_audio.delta` events are buffered, then played
//!    into the channel. `hand_off` tool calls are executed (the configured HTTP
//!    endpoint) and the result is injected back into the session as text for
//!    the voice model to say.
//! 5. On exit the session is torn down and the bridge leaves the channel.

use std::{
    collections::{HashMap, HashSet},
    io::Cursor,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use serenity::all::{ChannelId, GuildId, VoiceState};
use songbird::{CoreEvent, Event, EventContext, Songbird, tracks::Track};
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};

use crate::config::VoiceConfig;

/// PCM16 sample rate sent to the voice model.
const MODEL_SAMPLE_RATE: usize = 16_000;
/// Sample rate the model speaks back at.
const PLAYBACK_SAMPLE_RATE: usize = 24_000;
/// The voice model speaks mono.
const PLAYBACK_CHANNELS: u32 = 1;
/// Pre-roll audio kept when speech begins.
const PRE_ROLL_SAMPLES: usize = 1_600;
/// Max utterance length in 16 kHz PCM16 samples (30 s) before forced send.
const MAX_UTTERANCE_SAMPLES: usize = 16_000 * 30;
/// Minimum speech length before we bother sending.
const MIN_SPEECH_SECONDS: f32 = 0.5;
/// 80 ms of silence (5 x 16 ms frames) before an utterance ends.
const SILENCE_FRAMES: usize = 5;
/// 16 ms at 16 kHz mono.
const INPUT_FRAME_SAMPLES: usize = 256;

/// The single tool offered to the voice model.
pub const HAND_OFF_TOOL: &str = "hand_off";

/// Audio collected from the voice model, ready to play back.
#[derive(Debug, Default)]
pub struct AudioSegment {
    /// Raw PCM16 LE bytes at `PLAYBACK_SAMPLE_RATE`.
    pub pcm16: Vec<u8>,
}

impl AudioSegment {
    fn to_f32(&self) -> Vec<f32> {
        self.pcm16
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| i16::from_le_bytes([pair[0], pair[1]]) as f32 / 32768.0)
            .collect()
    }
}

/// The bridge that joins voice channels on behalf of the operator.
pub struct VoiceBridge {
    config: VoiceConfig,
    manager: Arc<Songbird>,
    bot_name: Arc<RwLock<String>>,
    allowed_guilds: Arc<HashSet<u64>>,
    /// guild id → live session, fenced by generation.
    sessions: Arc<Mutex<HashMap<u64, SessionState>>>,
}

struct SessionState {
    generation: u64,
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl VoiceBridge {
    pub fn new(config: VoiceConfig, allowed_guilds: HashSet<u64>, manager: Arc<Songbird>) -> Self {
        Self {
            config,
            manager,
            bot_name: Arc::new(RwLock::new("Voice Agent".to_owned())),
            allowed_guilds: Arc::new(allowed_guilds),
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
        self.allowed_guilds.contains(&guild_id)
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
        let generation = {
            let mut sessions = self.sessions.lock().expect("sessions lock");
            // Tear down an existing session for this guild first.
            let previous = sessions.remove(&guild_id);
            if let Some(previous) = &previous {
                let _ = previous.stop.send(true);
                previous.task.abort();
            }
            previous.map(|s| s.generation + 1).unwrap_or(1)
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
        let manager = Arc::clone(&self.manager);
        let sessions = Arc::clone(&self.sessions);
        let task = tokio::spawn(async move {
            let session = VoiceSession {
                guild_id,
                channel_id,
                generation,
                bot_name,
                config,
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
            let mut sessions = self.sessions.lock().expect("sessions lock");
            if let Some(state) = sessions.remove(&guild_id) {
                let _ = state.stop.send(true);
                state.task.abort();
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
    channel_id: u64,
    generation: u64,
    bot_name: String,
    config: VoiceConfig,
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

    /// Connect, then stream operator speech to the model and back.
    async fn run_inner(&self, stop: &mut watch::Receiver<bool>) -> Result<()> {
        // Receive operator PCM from the voice channel.
        let (pcm_tx, mut pcm_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<i16>>();
        self.attach_capture(pcm_tx).await?;

        // Connect to the voice model.
        let mut socket = connect_async(self.config.model_url.as_str())
            .await
            .context("connecting to voice model")?
            .0;

        // Handshake: instructions + the single hand_off tool.
        let instructions = self.config.render_system_prompt(&self.bot_name);
        let tools = vec![json!({
            "type": "function",
            "name": HAND_OFF_TOOL,
            "description": "Ask the hand_off model what to say next. Reply with the exact words the voice assistant should speak.",
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "The operator's request or question for the hand_off model",
                    }
                },
                "required": ["question"],
            }
        })];
        socket
            .send(WsMessage::text(
                json!({
                    "type": "session.update",
                    "session": { "instructions": instructions, "tools": tools }
                })
                .to_string(),
            ))
            .await
            .context("sending session.update")?;

        // The capture loop runs alongside the WS loop.
        let mut capture = Capture::new(PRE_ROLL_SAMPLES, MAX_UTTERANCE_SAMPLES);

        loop {
            tokio::select! {
                _ = stop.changed() => return Ok(()),
                maybe_pcm = pcm_rx.recv() => {
                    let Some(pcm) = maybe_pcm else { return Ok(()); };
                    let Some(utterance) = capture.feed(&pcm) else { continue };
                    // One utterance completed (ended by silence or by the cap).
                    self.handle_turn(&mut socket, utterance).await?;
                }
            }
        }
    }

    /// Play a turn: send the utterance, wait for the response, play the audio.
    async fn handle_turn(
        &self,
        socket: &mut (
                 impl futures_util::Sink<WsMessage, Error = tokio_tungstenite::tungstenite::Error>
                 + futures_util::Stream<
            Item = Result<WsMessage, tokio_tungstenite::tungstenite::Error>,
        > + Unpin
             ),
        pcm: Vec<i16>,
    ) -> Result<()> {
        if pcm.len() < (MIN_SPEECH_SECONDS * MODEL_SAMPLE_RATE as f32) as usize {
            tracing::debug!(samples = pcm.len(), "utterance too short; ignored");
            return Ok(());
        }
        let audio = pcm_to_base64(&pcm);
        socket
            .send(WsMessage::text(
                json!({ "type": "input_audio_buffer.append", "audio": audio }).to_string(),
            ))
            .await
            .context("appending audio")?;
        socket
            .send(WsMessage::text(
                json!({ "type": "input_audio_buffer.commit" }).to_string(),
            ))
            .await
            .context("committing audio")?;
        socket
            .send(WsMessage::text(
                json!({
                    "type": "response.create",
                    "response": { "output_modalities": ["text", "audio"] }
                })
                .to_string(),
            ))
            .await
            .context("creating response")?;

        // Collect audio + tool-call responses.
        let mut collected = AudioSegment::default();
        let mut text_acc = String::new();
        let mut tools: Vec<(String, Value, String)> = Vec::new(); // (name, args, call_id)
        let mut tool_buffers: HashMap<String, (String, String)> = HashMap::new(); // item_id → (name, args)

        loop {
            let Some(message) = socket.next().await else {
                break;
            };
            let message = message.context("voice model connection closed")?;
            let WsMessage::Text(text) = message else {
                continue;
            };
            let Ok(event) = serde_json::from_str::<Value>(&text) else {
                continue;
            };
            let event_type = event.get("type").and_then(Value::as_str).unwrap_or("");
            match event_type {
                "response.function_call_arguments.delta" => {
                    let item_id = event
                        .get("item_id")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned();
                    let delta = event
                        .get("delta")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned();
                    tool_buffers.entry(item_id).or_default().1.push_str(&delta);
                }
                "response.function_call_arguments.done" => {
                    let item_id = event
                        .get("item_id")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned();
                    let name = event
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned();
                    let args_text = tool_buffers
                        .get(&item_id)
                        .map(|(_, a)| a.clone())
                        .unwrap_or_else(|| {
                            event
                                .get("arguments")
                                .and_then(Value::as_str)
                                .unwrap_or("{}")
                                .to_owned()
                        });
                    let call_id = event
                        .get("call_id")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned();
                    let args = serde_json::from_str::<Value>(&args_text).unwrap_or(Value::Null);
                    tools.push((name, args, call_id));
                }
                "response.output_audio.delta" => {
                    if let Some(delta) = event.get("delta").and_then(Value::as_str)
                        && let Ok(bytes) = BASE64.decode(delta)
                    {
                        collected.pcm16.extend_from_slice(&bytes);
                    }
                }
                "response.output_text.delta" => {
                    if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                        text_acc.push_str(delta);
                    }
                }
                "response.output_audio.done" | "response.done" => {
                    break;
                }
                "error" => {
                    tracing::warn!(?event, "voice model error during turn");
                    break;
                }
                _ => {}
            }
        }

        // Execute any hand_off calls and inject results back into the session.
        let mut outputs: Vec<(String, String)> = Vec::new();
        for (name, args, call_id) in tools {
            if name != HAND_OFF_TOOL {
                tracing::warn!(name, "unexpected tool call from voice model");
                continue;
            }
            let question = args
                .get("question")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_owned();
            let reply = if question.is_empty() {
                "Sorry, I did not get a question to ask.".to_owned()
            } else {
                self.ask_hand_off(&question).await
            };
            outputs.push((call_id, reply));
        }
        if !outputs.is_empty() {
            // Batch all outputs, then drive one follow-up response — the same
            // flow the voice-demo uses.
            for (call_id, reply) in outputs {
                socket
                    .send(WsMessage::text(json!({
                        "type": "conversation.item.create",
                        "item": { "type": "function_call_output", "call_id": call_id, "output": reply }
                    }).to_string()))
                    .await
                    .context("injecting hand_off result")?;
            }
            socket
                .send(WsMessage::text(
                    json!({
                        "type": "response.create",
                        "response": { "output_modalities": ["text", "audio"] }
                    })
                    .to_string(),
                ))
                .await
                .context("creating follow-up response")?;
            // Collect the follow-up response audio.
            loop {
                let Some(message) = socket.next().await else {
                    break;
                };
                let message = message.context("voice model connection closed")?;
                let WsMessage::Text(text) = message else {
                    continue;
                };
                let Ok(event) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                let event_type = event.get("type").and_then(Value::as_str).unwrap_or("");
                match event_type {
                    "response.output_audio.delta" => {
                        if let Some(delta) = event.get("delta").and_then(Value::as_str)
                            && let Ok(bytes) = BASE64.decode(delta)
                        {
                            collected.pcm16.extend_from_slice(&bytes);
                        }
                    }
                    "response.output_audio.done" | "response.done" => break,
                    "error" => break,
                    _ => {}
                }
            }
        }

        // Play the collected audio.
        if !collected.pcm16.is_empty() {
            let pcm_bytes = collected.pcm16.len();
            self.play(collected).await?;
            tracing::info!(
                guild_id = self.guild_id,
                text = %text_acc.trim(),
                pcm_bytes,
                "voice turn complete"
            );
        }
        Ok(())
    }

    /// Call the configured hand_off endpoint with the operator's request.
    async fn ask_hand_off(&self, question: &str) -> String {
        let url = &self.config.hand_off_url;
        let client = match reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(90))
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                tracing::warn!(%error, "hand_off client failed");
                return "I could not reach the hand_off service.".to_owned();
            }
        };
        let result = client
            .post(url)
            .json(&json!({
                "question": question,
                "context": format!("guild={} channel={}", self.guild_id, self.channel_id),
            }))
            .send()
            .await;
        match result {
            Ok(response) => {
                let status = response.status();
                match response.json::<Value>().await {
                    Ok(body) => {
                        if status.is_success()
                            && body.get("ok").and_then(Value::as_bool) == Some(true)
                            && let Some(reply) = body.get("reply").and_then(Value::as_str)
                        {
                            let reply = reply.trim().to_owned();
                            if !reply.is_empty() {
                                return reply;
                            }
                        }
                        "The hand_off service did not return a reply.".to_owned()
                    }
                    Err(_) => "The hand_off service returned an unreadable response.".to_owned(),
                }
            }
            Err(error) => {
                tracing::warn!(%error, "hand_off request failed");
                "I could not reach the hand_off service.".to_owned()
            }
        }
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

    /// Play a collected audio segment via a songbird track.
    async fn play(&self, segment: AudioSegment) -> Result<()> {
        let call = self
            .manager
            .get(GuildId::new(self.guild_id))
            .context("voice call unavailable")?;
        let f32s = segment.to_f32();
        let mut float_bytes = Vec::with_capacity(f32s.len() * 4);
        for sample in &f32s {
            float_bytes.extend_from_slice(&sample.to_le_bytes());
        }
        let input: songbird::input::Input = songbird::input::RawAdapter::new(
            Cursor::new(float_bytes),
            PLAYBACK_SAMPLE_RATE as u32,
            PLAYBACK_CHANNELS,
        )
        .into();
        let mut call = call.lock().await;
        let _track = call.play_only(Track::from(input).volume(0.99));
        // Give playback a small beat to start; mirrors the reference branch.
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Speech capture (VAD)
// ---------------------------------------------------------------------------

/// Speech detector + utterance buffer.
///
/// A mono 16 kHz PCM stream is fed in; completed utterances are returned once,
/// by either the end-silence rule or the max length.
struct Capture {
    detector: earshot::Detector,
    /// Non-complete-frame leftover samples.
    carry: Vec<i16>,
    /// Frames kept while not speaking (pre-roll), newest last.
    pre_roll: std::collections::VecDeque<Vec<i16>>,
    pre_roll_samples: usize,
    /// Samples collected since speech started (incl. pre-roll).
    active: Vec<i16>,
    /// Frames seen without speech since the last speech frame.
    silence_frames: usize,
    max_samples: usize,
}

impl Capture {
    fn new(pre_roll_samples: usize, max_samples: usize) -> Self {
        Self {
            detector: earshot::Detector::default(),
            carry: Vec::new(),
            pre_roll: std::collections::VecDeque::new(),
            pre_roll_samples,
            active: Vec::new(),
            silence_frames: 0,
            max_samples,
        }
    }

    /// Feed mono 16 kHz samples. Returns `Some(utterance)` when one completes.
    fn feed(&mut self, mono: &[i16]) -> Option<Vec<i16>> {
        let mut utterance: Option<Vec<i16>> = None;
        self.carry.extend_from_slice(mono);
        // Process every complete 16 ms frame.
        while self.carry.len() >= INPUT_FRAME_SAMPLES {
            let frame: Vec<i16> = self.carry.drain(..INPUT_FRAME_SAMPLES).collect();
            let score = self.detector.predict_i16(&frame);
            let speech = score >= 0.5;
            if speech {
                if self.active.is_empty() {
                    // Start of an utterance: seed with pre-roll audio.
                    self.active = self.pre_roll.drain(..).flatten().collect();
                    self.silence_frames = 0;
                }
                self.active.extend_from_slice(&frame);
                self.silence_frames = 0;
                if self.active.len() >= self.max_samples {
                    utterance = Some(std::mem::take(&mut self.active));
                    self.pre_roll.clear();
                    self.silence_frames = 0;
                    self.carry.clear();
                    return utterance;
                }
            } else if !self.active.is_empty() {
                // In an utterance: count trailing silence frames.
                self.silence_frames += 1;
                if self.silence_frames >= SILENCE_FRAMES {
                    utterance = Some(std::mem::take(&mut self.active));
                    self.pre_roll.clear();
                    self.silence_frames = 0;
                    self.carry.clear();
                    return utterance;
                }
            } else {
                // Not speaking: keep a capped pre-roll.
                self.pre_roll.push_back(frame);
                let mut kept: usize = self.pre_roll.iter().map(Vec::len).sum();
                while kept > self.pre_roll_samples {
                    if let Some(oldest) = self.pre_roll.pop_front() {
                        kept -= oldest.len();
                    } else {
                        break;
                    }
                }
            }
        }
        utterance
    }
}

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

fn pcm_to_base64(pcm: &[i16]) -> String {
    let mut bytes = Vec::with_capacity(pcm.len() * 2);
    for sample in pcm {
        bytes.extend_from_slice(&sample.to_le_bytes());
    }
    BASE64.encode(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_prompt_contains_placeholder() {
        let config = crate::config::VoiceConfig {
            model_url: "wss://host/v1/realtime".to_owned(),
            hand_off_url: "https://host/delegate".to_owned(),
            operator_user_id: 345678901234567890,
            system_prompt: crate::config::DEFAULT_VOICE_SYSTEM_PROMPT.to_owned(),
        };
        assert!(config.system_prompt.contains("{bot_name}"));
        assert!(config.render_system_prompt("Athena").contains("Athena"));
    }

    #[test]
    fn pcm16_roundtrip_uses_little_endian_samples() {
        let pcm = vec![1_i16, -2_i16, 3_i16];
        let encoded = BASE64.encode(&{
            let mut bytes = Vec::new();
            for sample in &pcm {
                bytes.extend_from_slice(&sample.to_le_bytes());
            }
            bytes
        });
        let decoded = BASE64.decode(&encoded).expect("decoded base64");
        assert_eq!(decoded, {
            let mut bytes = Vec::new();
            for sample in &pcm {
                bytes.extend_from_slice(&sample.to_le_bytes());
            }
            bytes
        });
    }
}

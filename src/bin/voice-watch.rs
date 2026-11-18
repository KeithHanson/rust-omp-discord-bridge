//! Opt-in local voice receiver with optional private routing and bounded review capture.
use std::{collections::VecDeque, fs, io::{self, Cursor, SeekFrom}, num::NonZeroU64,
    os::unix::fs::{OpenOptionsExt, PermissionsExt}, path::{Path, PathBuf}, pin::Pin,
    sync::{Arc, Mutex}, task::{Context as TaskContext, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH}};

use anyhow::{Context as _, Result, ensure};
use futures_util::{SinkExt, StreamExt};
use ogg::writing::{PacketWriteEndInfo, PacketWriter};
use opus2::{Application, Channels as OpusChannels, Encoder as OpusEncoder};
use clap::Parser;
use omp_discord_bridge::{config::DiscordConfig, voice_ingress::{self, VoiceRequest}};
use serenity::{all::{Channel, ChannelId, ChannelType, CommandDataOption, CommandDataOptionValue,
    CommandInteraction, CommandOptionType, CommandType, Context, CreateAllowedMentions,
    CreateCommand, CreateCommandOption, CreateInteractionResponse, CreateInteractionResponseMessage,
    CreateMessage, EditInteractionResponse, EventHandler, GatewayIntents, Guild, GuildId, Http,
    Interaction, Message, Ready, UnavailableGuild, UserId, VoiceState},
    gateway::{ConnectionStage, ShardStageUpdateEvent}, async_trait, Client};
use songbird::{driver::{Channels, DecodeConfig, DecodeMode, SampleRate},
    input::{codecs::{get_codec_registry, get_probe}, AsyncAdapterStream, AsyncMediaSource,
        AudioStream, Input, LiveInput, RawAdapter},
    tracks::{LoopState, Track}, CoreEvent, Event, EventContext, SerenityInit, Songbird};
use tokio::{fs::File, io::{AsyncRead, AsyncSeek, AsyncWriteExt, DuplexStream, ReadBuf},
    sync::{mpsc, watch}};
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};

const SAMPLE_RATE: usize = 16_000;
const CHANNELS: usize = 2;
const BUFFER_SAMPLES: usize = SAMPLE_RATE * CHANNELS * 20;
const VAD_SAMPLES: usize = 256 * CHANNELS; // Earshot: 16 ms at 16 kHz.
const PRE_ROLL_SAMPLES: usize = SAMPLE_RATE * CHANNELS * 640 / 1000;
const END_SILENCE_FRAMES: usize = 320 / 16;
const MIN_SPEECH_FRAMES: usize = 256 / 16;
const MAX_QUEUED_UTTERANCES: usize = 2;
const PERSONAPLEX_FRAME_SAMPLES: usize = 480; // 20 ms at 24 kHz mono.
const PERSONAPLEX_QUEUE_FRAMES: usize = 50; // Bound latency to one second.
const REVIEW_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Parser)]
#[command(about = "Private auto-join voice receiver with optional transcription or review capture")]
struct Args {
    #[arg(long)]
    token_file: PathBuf,
    /// Refuse to connect if the token belongs to another bot.
    #[arg(long)]
    bot_id: NonZeroU64,
    #[arg(long)]
    guild_id: NonZeroU64,
    #[arg(long)]
    channel_id: NonZeroU64,
    #[arg(long)]
    user_id: NonZeroU64,
    /// Private ASR base URL, e.g. http://192.168.1.173:18086.
    #[arg(long, requires = "stt_key_file")]
    stt_url: Option<reqwest::Url>,
    #[arg(long, requires = "stt_url")]
    stt_key_file: Option<PathBuf>,
    /// Enable /voice on|off routing to existing bridge threads; otherwise only log transcripts.
    #[arg(long, requires = "stt_url")]
    bridge_socket: Option<PathBuf>,
    /// Pocket TTS server base URL. Speaks the next assistant reply in the armed thread.
    #[arg(long, requires = "bridge_socket")]
    tts_url: Option<reqwest::Url>,
    /// Native full-duplex PersonaPlex base URL. Mutually exclusive with STT/TTS routing.
    #[arg(long, conflicts_with_all = ["stt_url", "stt_key_file", "bridge_socket", "tts_url"])]
    personaplex_url: Option<reqwest::Url>,
    /// Private directory for 24-hour PersonaPlex quality-review artifacts.
    #[arg(long, requires = "personaplex_url")]
    review_dir: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Desired {
    generation: u64,
    present: bool,
}

/// Fixed allocation, chronological PCM retained across wrap.
struct AudioBuffer {
    samples: Box<[i16]>,
    next: usize,
    len: usize,
}

impl AudioBuffer {
    fn new(capacity: usize) -> Self {
        Self { samples: vec![0; capacity].into_boxed_slice(), next: 0, len: 0 }
    }

    fn push(&mut self, pcm: &[i16]) {
        let capacity = self.samples.len();
        let pcm = &pcm[pcm.len().saturating_sub(capacity)..];
        let first = pcm.len().min(capacity - self.next);
        self.samples[self.next..self.next + first].copy_from_slice(&pcm[..first]);
        self.samples[..pcm.len() - first].copy_from_slice(&pcm[first..]);
        self.next = (self.next + pcm.len()) % capacity;
        self.len = (self.len + pcm.len()).min(capacity);
    }

    fn clear(&mut self) {
        let start = (self.next + self.samples.len() - self.len) % self.samples.len();
        let first = self.len.min(self.samples.len() - start);
        self.samples[start..start + first].fill(0);
        self.samples[..self.len - first].fill(0);
        self.next = 0;
        self.len = 0;
    }

    fn slices(&self) -> (&[i16], &[i16]) {
        let start = (self.next + self.samples.len() - self.len) % self.samples.len();
        let first = self.len.min(self.samples.len() - start);
        (&self.samples[start..start + first], &self.samples[..self.len - first])
    }

    fn wav(&self) -> Vec<u8> {
        let bytes = (self.len * 2) as u32;
        let mut wav = Vec::with_capacity(44 + bytes as usize);
        wav.extend_from_slice(b"RIFF");
        wav.extend_from_slice(&(36 + bytes).to_le_bytes());
        wav.extend_from_slice(b"WAVEfmt ");
        wav.extend_from_slice(&16_u32.to_le_bytes());
        wav.extend_from_slice(&1_u16.to_le_bytes());
        wav.extend_from_slice(&(CHANNELS as u16).to_le_bytes());
        wav.extend_from_slice(&(SAMPLE_RATE as u32).to_le_bytes());
        wav.extend_from_slice(&((SAMPLE_RATE * CHANNELS * 2) as u32).to_le_bytes());
        wav.extend_from_slice(&((CHANNELS * 2) as u16).to_le_bytes());
        wav.extend_from_slice(&16_u16.to_le_bytes());
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&bytes.to_le_bytes());
        let (first, second) = self.slices();
        for sample in first.iter().chain(second) {
            wav.extend_from_slice(&sample.to_le_bytes());
        }
        wav
    }
}

struct Utterance {
    wav: Vec<u8>,
    sequence: u64,
    ended: Instant,
    epoch: u64,
    destination: Option<ChannelId>,
}

struct Capture {
    desired: Desired,
    // Also invalidates in-flight results on transport/SSRC resets within one membership.
    epoch: u64,
    routing: bool,
    binding: Option<ChannelId>,
    command_revision: u64,
    awaiting_reply: Option<ChannelId>,
    feedback_generation: u64,
    ssrc: Option<u32>,
    audio: AudioBuffer,
    pre_roll: AudioBuffer,
    detector: earshot::Detector,
    frame: [i16; VAD_SAMPLES],
    frame_len: usize,
    active: bool,
    speech_frames: usize,
    quiet_frames: usize,
    ready: VecDeque<Utterance>,
    sequence: u64,
    frames: u64,
    concealed_frames: u64,
    silent_ticks: u64,
    nonzero_samples: u64,
}

impl Capture {
    fn new(capacity: usize) -> Self {
        Self {
            desired: Desired { generation: 0, present: false },
            epoch: 0,
            routing: false,
            binding: None,
            command_revision: 0,
            awaiting_reply: None,
            feedback_generation: 0,
            ssrc: None,
            audio: AudioBuffer::new(capacity),
            pre_roll: AudioBuffer::new(PRE_ROLL_SAMPLES),
            detector: earshot::Detector::default(),
            frame: [0; VAD_SAMPLES],
            frame_len: 0,
            active: false,
            speech_frames: 0,
            quiet_frames: 0,
            ready: VecDeque::with_capacity(MAX_QUEUED_UTTERANCES),
            sequence: 0,
            frames: 0,
            concealed_frames: 0,
            silent_ticks: 0,
            nonzero_samples: 0,
        }
    }

    fn reset(&mut self) {
        self.ssrc = None;
        self.reset_audio();
    }

    fn reset_audio(&mut self) {
        if self.frames > 0 {
            tracing::info!(frames = self.frames, concealed_frames = self.concealed_frames,
                silent_ticks = self.silent_ticks, nonzero_samples = self.nonzero_samples,
                buffered_samples = self.audio.len, "Voice capture ended; clearing PCM");
        }
        self.epoch += 1;
        self.awaiting_reply = None;
        self.feedback_generation += 1;
        self.audio.clear();
        self.pre_roll.clear();
        self.detector.reset();
        self.frame.fill(0);
        self.frame_len = 0;
        self.active = false;
        self.speech_frames = 0;
        self.quiet_frames = 0;
        for utterance in &mut self.ready {
            utterance.wav.fill(0);
        }
        self.ready.clear();
        self.frames = 0;
        self.concealed_frames = 0;
        self.silent_ticks = 0;
        self.nonzero_samples = 0;
    }

    fn bind(&mut self, destination: ChannelId) -> Option<ChannelId> {
        let previous = self.binding.replace(destination);
        self.reset_audio();
        previous
    }

    fn bind_if_current(&mut self, revision: u64, destination: ChannelId) -> Option<(Option<ChannelId>, bool)> {
        if self.command_revision != revision {
            return None;
        }
        Some((self.bind(destination), self.desired.present))
    }

    fn begin_command(&mut self) -> u64 {
        self.command_revision += 1;
        self.command_revision
    }

    fn disarm(&mut self) -> Option<ChannelId> {
        self.begin_command();
        let previous = self.binding.take();
        self.reset_audio();
        previous
    }

    fn reset_transport(&mut self) -> Option<ChannelId> {
        self.ssrc = None;
        self.disarm()
    }

    fn accepts(&self, epoch: u64, destination: Option<ChannelId>) -> bool {
        self.epoch == epoch && self.desired.present
            && (!self.routing || destination.is_some() && destination == self.binding)
    }

    fn begin_feedback(&mut self, destination: ChannelId) -> u64 {
        self.awaiting_reply = Some(destination);
        self.feedback_generation += 1;
        self.feedback_generation
    }

    fn take_reply(&mut self, destination: ChannelId) -> bool {
        if self.awaiting_reply != Some(destination) {
            return false;
        }
        self.awaiting_reply = None;
        self.feedback_generation += 1;
        true
    }

    fn feedback_current(&self, generation: u64) -> bool {
        self.feedback_generation == generation && self.awaiting_reply.is_some()
    }

    fn identify(&mut self, generation: u64, ssrc: u32, is_target: bool) -> Option<ChannelId> {
        if generation != self.desired.generation || !self.desired.present {
            return None;
        }
        let mut disarmed = None;
        if is_target && self.ssrc != Some(ssrc) {
            if self.ssrc.is_some() {
                disarmed = self.reset_transport();
            } else {
                // Initial identity after joining must preserve a binding armed before joining.
                self.reset();
            }
            self.ssrc = Some(ssrc);
            tracing::info!(ssrc, epoch = self.epoch, "Operator speaker identity established");
        } else if !is_target && self.ssrc == Some(ssrc) {
            // SSRC reuse must not join audio or return results from different speakers.
            disarmed = self.reset_transport();
        }
        disarmed
    }

    fn receive(&mut self, generation: u64, ssrc: u32, pcm: &[i16], concealed: bool) {
        if generation != self.desired.generation || !self.desired.present
            || self.ssrc != Some(ssrc) || pcm.is_empty() || self.routing && self.binding.is_none()
        {
            return;
        }
        self.frames += 1;
        self.concealed_frames += u64::from(concealed);
        self.nonzero_samples += pcm.iter().filter(|sample| **sample != 0).count() as u64;
        self.feed(pcm);
        if self.frames == 1 || self.frames.is_multiple_of(250) {
            tracing::info!(frames = self.frames, concealed_frames = self.concealed_frames,
                silent_ticks = self.silent_ticks, nonzero_samples = self.nonzero_samples,
                buffered_samples = self.audio.len, sample_rate = SAMPLE_RATE, channels = CHANNELS,
                "Operator PCM received (memory only)");
        }
    }

    fn voice_tick(&mut self, generation: u64, pcm: Option<&[i16]>, packet_received: bool) {
        if generation != self.desired.generation || !self.desired.present
            || self.routing && self.binding.is_none()
        {
            return;
        }
        let Some(ssrc) = self.ssrc else { return };
        if let Some(pcm) = pcm.filter(|pcm| !pcm.is_empty()) {
            // Packet=None can still contain real decoder/Opus concealment output.
            self.receive(generation, ssrc, pcm, !packet_received);
        } else {
            // Preserve pauses on the playout clock, including DTX with no packets.
            self.silent_ticks += 1;
            self.feed(&[0; SAMPLE_RATE * CHANNELS / 50]);
        }
    }

    fn feed(&mut self, mut pcm: &[i16]) {
        while !pcm.is_empty() {
            let take = pcm.len().min(VAD_SAMPLES - self.frame_len);
            self.frame[self.frame_len..self.frame_len + take].copy_from_slice(&pcm[..take]);
            self.frame_len += take;
            pcm = &pcm[take..];
            if self.frame_len == VAD_SAMPLES {
                let mut mono = [0_i16; VAD_SAMPLES / CHANNELS];
                for (out, stereo) in mono.iter_mut().zip(self.frame.chunks_exact(CHANNELS)) {
                    *out = ((i32::from(stereo[0]) + i32::from(stereo[1])) / 2) as i16;
                }
                let speech = self.detector.predict_i16(&mono) >= 0.5;
                self.accept_frame(speech);
                self.frame_len = 0;
            }
        }
    }

    fn accept_frame(&mut self, speech: bool) {
        if !self.active {
            if !speech {
                self.pre_roll.push(&self.frame);
                return;
            }
            self.active = true;
            let (first, second) = self.pre_roll.slices();
            self.audio.push(first);
            self.audio.push(second);
        }
        self.audio.push(&self.frame);
        self.pre_roll.push(&self.frame);
        self.speech_frames += usize::from(speech);
        self.quiet_frames = if speech { 0 } else { self.quiet_frames + 1 };
        let reason = if self.quiet_frames >= END_SILENCE_FRAMES {
            Some("speech_end")
        } else if self.audio.len == self.audio.samples.len() {
            Some("duration_limit_with_overlap")
        } else { None };
        if let Some(reason) = reason {
            self.finish(reason);
        }
    }

    fn finish(&mut self, reason: &'static str) {
        self.sequence += 1;
        let accepted = self.speech_frames >= MIN_SPEECH_FRAMES;
        let (first, second) = self.audio.slices();
        let square_sum: f64 = first.iter().chain(second).map(|&s| f64::from(s).powi(2)).sum();
        let rms = (square_sum / self.audio.len.max(1) as f64).sqrt();
        tracing::info!(sequence = self.sequence, reason, accepted,
            audio_ms = self.audio.len * 1000 / (SAMPLE_RATE * CHANNELS),
            speech_ms = self.speech_frames * 16, rms,
            received_frames = self.frames, concealed_frames = self.concealed_frames,
            silent_ticks = self.silent_ticks, "Voice chunk boundary (stream counters cumulative)");
        if accepted {
            if self.ready.len() < MAX_QUEUED_UTTERANCES {
                self.ready.push_back(Utterance {
                    wav: self.audio.wav(), sequence: self.sequence, ended: Instant::now(),
                    epoch: self.epoch, destination: self.binding,
                });
            } else {
                tracing::warn!(sequence = self.sequence, "ASR backlog full; dropping new utterance");
            }
        }
        self.audio.clear();
        self.active = false;
        self.speech_frames = 0;
        self.quiet_frames = 0;
        // Retain pre-roll, including 640ms overlap when the duration cap cuts active speech.
    }
}

struct BridgeRoute {
    socket: PathBuf,
    guild: GuildId,
    user: UserId,
    http: Arc<Http>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VoiceAction {
    On,
    Off,
}

fn voice_action(options: &[CommandDataOption]) -> Option<VoiceAction> {
    let [option] = options else { return None };
    let CommandDataOptionValue::SubCommand(arguments) = &option.value else { return None };
    if !arguments.is_empty() { return None }
    match option.name.as_str() {
        "on" => Some(VoiceAction::On),
        "off" => Some(VoiceAction::Off),
        _ => None,
    }
}

fn permitted_operator(guild: Option<GuildId>, user: UserId, bot: bool, expected_guild: GuildId, expected_user: UserId) -> bool {
    guild == Some(expected_guild) && user == expected_user && !bot
}

fn is_thread(kind: ChannelType) -> bool {
    matches!(kind, ChannelType::PublicThread | ChannelType::PrivateThread | ChannelType::NewsThread)
}

async fn notify_thread(http: &Http, thread: ChannelId, message: &str) {
    if thread.send_message(http, CreateMessage::new().content(message)
        .allowed_mentions(CreateAllowedMentions::new())).await.is_err()
    {
        tracing::warn!(thread_id = thread.get(), "Could not post voice status; no retry");
    }
}

async fn command_reply(http: &Http, command: &CommandInteraction, message: &str) -> bool {
    if command.edit_response(http, EditInteractionResponse::new().content(message)
        .allowed_mentions(CreateAllowedMentions::new())).await.is_err()
    {
        tracing::warn!("Could not finish voice command response; no retry");
        false
    } else {
        true
    }
}

struct Watcher {
    guild: GuildId,
    channel: ChannelId,
    user: UserId,
    desired: watch::Sender<Desired>,
    capture: Arc<Mutex<Capture>>,
    bridge: Option<Arc<BridgeRoute>>,
    bot: UserId,
    manager: Arc<Songbird>,
    tts_url: Option<reqwest::Url>,
}

impl Watcher {
    fn update(&self, present: bool, fresh_snapshot: bool) -> Option<ChannelId> {
        let mut capture = self.capture.lock().unwrap();
        if capture.desired.present == present && !fresh_snapshot {
            return None;
        }
        let previous = if !present || fresh_snapshot {
            capture.reset_transport()
        } else {
            // The operator may arm in a thread before joining General.
            capture.reset();
            None
        };
        capture.desired = Desired { generation: capture.desired.generation + 1, present };
        self.desired.send_replace(capture.desired);
        tracing::info!(present, "Operator General voice state updated");
        previous
    }

    fn voice_state(&self, guild: Option<GuildId>, user: UserId, channel: Option<ChannelId>) -> Option<ChannelId> {
        if guild == Some(self.guild) && user == self.user {
            self.update(channel == Some(self.channel), false)
        } else {
            None
        }
    }

    async fn command(&self, ctx: &Context, command: &CommandInteraction) {
        let rejection = if !permitted_operator(command.guild_id, command.user.id, command.user.bot, self.guild, self.user) {
            Some("Voice control is only available to the selected human in the configured server.")
        } else if self.bridge.is_none() {
            Some("Thread routing is disabled; this voice watcher is in log-only mode.")
        } else if command.data.kind != CommandType::ChatInput || voice_action(&command.data.options).is_none() {
            Some("Use /voice on or /voice off without additional arguments.")
        } else if command.channel.as_ref().is_some_and(|channel| channel.id != command.channel_id || !is_thread(channel.kind)) {
            Some("Run /voice inside an existing bridge thread, not a channel or DM.")
        } else {
            None
        };
        if let Some(message) = rejection {
            if command.create_response(&ctx.http, CreateInteractionResponse::Message(
                CreateInteractionResponseMessage::new().content(message).ephemeral(true)
                    .allowed_mentions(CreateAllowedMentions::new()))).await.is_err()
            {
                tracing::warn!("Could not reject voice command; no retry");
            }
            return;
        }
        let action = voice_action(&command.data.options).unwrap();
        // Reserve before the first await so a slow earlier validation cannot undo a later off/on.
        let revision = self.capture.lock().unwrap().begin_command();
        if command.defer(&ctx.http).await.is_err() {
            tracing::warn!("Could not defer voice command; no routing change");
            return;
        }
        // Use the invoking Discord ID, never a channel name or caller-supplied destination.
        let thread = if command.channel.is_some() {
            command.channel_id // The authenticated Discord payload already passed the type/ID guard.
        } else {
            match command.channel_id.to_channel(&ctx.http).await {
                Ok(Channel::Guild(channel)) if channel.guild_id == self.guild && is_thread(channel.kind) => channel.id,
                _ => {
                    command_reply(&ctx.http, command, "Cannot verify this destination. Run /voice in an existing bridge thread.").await;
                    return;
                }
            }
        };
        if action == VoiceAction::Off {
            let previous = {
                let mut capture = self.capture.lock().unwrap();
                (capture.command_revision == revision).then(|| capture.disarm())
            };
            let Some(previous) = previous else {
                command_reply(&ctx.http, command, "This command was superseded by a newer voice command or connection change.").await;
                return;
            };
            command_reply(&ctx.http, command,
                "Voice is off. Pending audio and recognition were discarded; already-dispatched or admitted turns are not canceled.").await;
            if let Some(previous) = previous.filter(|previous| *previous != thread) {
                notify_thread(&ctx.http, previous, "Voice is off; the operator disabled routing from another thread.").await;
            }
            return;
        }
        let bridge = self.bridge.as_ref().unwrap();
        let validation = voice_ingress::request(&bridge.socket, &VoiceRequest {
            guild_id: self.guild.get(), user_id: self.user.get(), thread_id: thread.get(), text: None,
        }).await;
        match validation {
            Ok(response) if response.ok => {}
            Ok(_) => {
                command_reply(&ctx.http, command, "Cannot arm voice: the bridge rejected this destination. Use an existing permitted bridge thread.").await;
                return;
            }
            Err(_) => {
                command_reply(&ctx.http, command, "Cannot arm voice: the private bridge did not acknowledge destination validation. No routing change was made.").await;
                return;
            }
        }
        let binding = self.capture.lock().unwrap().bind_if_current(revision, thread);
        let Some((previous, present)) = binding else {
            command_reply(&ctx.http, command, "Voice was not armed: a newer command, departure, or connection change superseded this request.").await;
            return;
        };
        let message = match (previous.filter(|previous| *previous != thread), present) {
            (Some(_), true) => "Voice moved to this thread; the previous destination is off. Only this thread receives new speech from General. Replies are text only.",
            (Some(_), false) => "Voice moved to this thread; the previous destination is off. Join General to send speech here. Replies are text only.",
            (None, true) => "Voice is on for this thread only while you are in General. Pending audio was cleared. Replies are text only; leaving General turns voice off.",
            (None, false) => "Voice is armed for this thread only. Join General to send speech here. Replies are text only; leaving General turns voice off.",
        };
        if !command_reply(&ctx.http, command, message).await {
            let mut capture = self.capture.lock().unwrap();
            if capture.command_revision == revision {
                capture.disarm();
            }
        }
        if let Some(previous) = previous.filter(|previous| *previous != thread) {
            notify_thread(&ctx.http, previous, "Voice is off for this thread; the operator selected another destination.").await;
        }
    }
}


#[derive(Clone)]
struct FeedbackAudio {
    acknowledgement: Arc<[u8]>,
    acknowledgement_duration: Duration,
    thinking: Arc<[u8]>,
}

fn wav_f32_pcm(wav: &[u8]) -> Result<Vec<u8>> {
    ensure!(wav.len() >= 44 && &wav[..4] == b"RIFF" && &wav[8..12] == b"WAVE",
        "TTS acknowledgement is not RIFF/WAVE");
    ensure!(u16::from_le_bytes(wav[20..22].try_into()?) == 1
        && u16::from_le_bytes(wav[22..24].try_into()?) == 1
        && u32::from_le_bytes(wav[24..28].try_into()?) == 24_000
        && u16::from_le_bytes(wav[34..36].try_into()?) == 16,
        "TTS acknowledgement format changed");
    let data = wav.windows(4).position(|window| window == b"data")
        .context("TTS acknowledgement has no data chunk")? + 8;
    ensure!(data <= wav.len() && (wav.len() - data).is_multiple_of(2),
        "TTS acknowledgement PCM is truncated");
    let mut pcm = Vec::with_capacity((wav.len() - data) * 2);
    for sample in wav[data..].chunks_exact(2) {
        let value = f32::from(i16::from_le_bytes(sample.try_into()?)) / 32_768.0;
        pcm.extend_from_slice(&value.to_le_bytes());
    }
    Ok(pcm)
}

fn thinking_pcm() -> Arc<[u8]> {
    let mut pcm = Vec::with_capacity(24_000 * 2 * 4);
    for sample in 0..24_000 * 2 {
        let seconds = sample as f32 / 24_000.0;
        let pulse = |start: f32, frequency: f32| {
            let local = seconds - start;
            if (0.0..0.09).contains(&local) {
                let envelope = (std::f32::consts::PI * local / 0.09).sin();
                (std::f32::consts::TAU * frequency * local).sin() * envelope * 0.035
            } else {
                0.0
            }
        };
        let value = pulse(0.25, 440.0) + pulse(0.48, 554.37);
        pcm.extend_from_slice(&value.to_le_bytes());
    }
    pcm.into()
}

async fn load_feedback_audio(url: &reqwest::Url) -> Result<FeedbackAudio> {
    let wav = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(30))
        .build()?
        .post(url.join("/tts")?)
        .form(&[("text", "Got it. Let me think about that.")])
        .send().await?.error_for_status()?.bytes().await?;
    let acknowledgement = wav_f32_pcm(&wav)?;
    let acknowledgement_duration =
        Duration::from_secs_f64(acknowledgement.len() as f64 / (24_000.0 * 4.0));
    Ok(FeedbackAudio {
        acknowledgement: acknowledgement.into(),
        acknowledgement_duration,
        thinking: thinking_pcm(),
    })
}

async fn play_waiting_feedback(
    manager: Arc<Songbird>,
    guild: GuildId,
    capture: Arc<Mutex<Capture>>,
    audio: Arc<FeedbackAudio>,
    generation: u64,
) {
    let result = async {
        let call = manager.get(guild).context("voice call unavailable")?;
        {
            let mut call = call.lock().await;
            let input: Input = RawAdapter::new(
                Cursor::new(Arc::clone(&audio.acknowledgement)), 24_000, 1).into();
            call.play_only(Track::from(input).volume(0.99));
        }
        tracing::info!(generation, "Voice acknowledgement started");
        tokio::time::sleep(audio.acknowledgement_duration + Duration::from_millis(100)).await;
        if !capture.lock().unwrap().feedback_current(generation) {
            return Result::<()>::Ok(());
        }
        let mut call = call.lock().await;
        let input: Input = RawAdapter::new(
            Cursor::new(Arc::clone(&audio.thinking)), 24_000, 1).into();
        call.play_only(Track::from(input).loops(LoopState::Infinite).volume(0.99));
        tracing::info!(generation, "Voice thinking cue started");
        Result::<()>::Ok(())
    }.await;
    if let Err(error) = result {
        tracing::warn!(%error, "Voice wait feedback failed");
    }
}

async fn speak_reply(manager: Arc<Songbird>, guild: GuildId, url: reqwest::Url, text: String) {
    let started = Instant::now();
    let result = async {
        let endpoint = url.join("/tts")?;
        let response = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(30))
            .build()?
            .post(endpoint)
            .form(&[("text", text.as_str())])
            .send()
            .await?
            .error_for_status()?;
        let (reader, mut writer) = tokio::io::duplex(256 * 1024);
        let playback = tokio::spawn(play_audio_stream(
            manager, guild, reader, "Pocket TTS", started));
        let mut chunks = response.bytes_stream();
        let mut bytes = 0_u64;
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            bytes += chunk.len() as u64;
            writer.write_all(&chunk).await?;
        }
        drop(writer);
        playback.await.context("joining TTS playback task")??;
        tracing::info!(bytes, elapsed_ms = started.elapsed().as_millis(),
            "TTS response stream completed");
        Result::<()>::Ok(())
    }
    .await;
    if let Err(error) = result {
        tracing::warn!(%error, elapsed_ms = started.elapsed().as_millis(), "TTS reply failed");
    }
}

#[async_trait]
impl EventHandler for Watcher {
    async fn ready(&self, ctx: Context, ready: Ready) {
        tracing::info!(bot_id = ready.user.id.get(), "Voice watcher gateway connected");
        if self.bridge.is_some() {
            // Create/upsert only our guild command, preserving every existing command.
            let command = CreateCommand::new("voice").kind(CommandType::ChatInput)
                .description("Control private General voice routing to this bridge thread")
                .add_option(CreateCommandOption::new(CommandOptionType::SubCommand, "on", "Send your General speech to this thread"))
                .add_option(CreateCommandOption::new(CommandOptionType::SubCommand, "off", "Stop routing your speech to any thread"));
            if self.guild.create_command(&ctx.http, command).await.is_err() {
                tracing::error!("Could not register /voice; thread routing cannot be enabled through this connection");
            }
        }
    }

    async fn guild_create(&self, ctx: Context, guild: Guild, _is_new: Option<bool>) {
        if guild.id == self.guild {
            let present = guild.voice_states.get(&self.user)
                .is_some_and(|state| state.channel_id == Some(self.channel));
            if let Some(thread) = self.update(present, true) {
                notify_thread(&ctx.http, thread, "Voice is off after a fresh server connection. Use /voice on to arm again.").await;
            }
            tracing::info!(present, "Voice watcher initial membership synchronized");
        }
    }

    async fn guild_delete(&self, ctx: Context, guild: UnavailableGuild, _full: Option<Guild>) {
        if guild.id == self.guild {
            if let Some(thread) = self.update(false, true) {
                notify_thread(&ctx.http, thread, "Voice is off because the server became unavailable.").await;
            }
        }
    }

    async fn voice_state_update(&self, ctx: Context, _old: Option<VoiceState>, new: VoiceState) {
        if new.user_id == self.bot {
            tracing::info!(channel = ?new.channel_id, self_mute = new.self_mute,
                self_deaf = new.self_deaf, suppress = new.suppress, "Bot voice state updated");
        }
        if let Some(thread) = self.voice_state(new.guild_id, new.user_id, new.channel_id) {
            notify_thread(&ctx.http, thread, "Voice is off because you left General. Use /voice on to arm again.").await;
        }
    }

    async fn shard_stage_update(&self, ctx: Context, event: ShardStageUpdateEvent) {
        if event.old == ConnectionStage::Connected && event.new != ConnectionStage::Connected {
            let previous = self.capture.lock().unwrap().reset_transport();
            if let Some(thread) = previous {
                notify_thread(&ctx.http, thread, "Voice is off because the Discord gateway connection changed. Use /voice on to arm again.").await;
            }
        }
    }

    async fn resume(&self, ctx: Context, _event: serenity::all::ResumedEvent) {
        let previous = self.capture.lock().unwrap().reset_transport();
        if let Some(thread) = previous {
            notify_thread(&ctx.http, thread, "Voice is off after Discord resumed the gateway connection. Use /voice on to arm again.").await;
        }
    }

    async fn interaction_create(&self, ctx: Context, interaction: Interaction) {
        if let Interaction::Command(command) = interaction {
            if command.data.name == "voice" {
                self.command(&ctx, &command).await;
            }
        }
    }

    async fn message(&self, _ctx: Context, message: Message) {
        let Some(tts_url) = self.tts_url.clone() else { return };
        if message.author.id != self.bot || message.content.trim().is_empty() {
            return;
        }
        let ignored = message.content.starts_with("Human voice transcript from Discord user ")
            || message.content == "Message accepted. Active work in this thread will be interrupted."
            || message.content.starts_with("OMP session created.");
        if ignored {
            return;
        }
        let should_speak = {
            self.capture.lock().unwrap().take_reply(message.channel_id)
        };
        if should_speak {
            let text: String = message.content.chars().take(1500).collect();
            tokio::spawn(speak_reply(
                Arc::clone(&self.manager),
                self.guild,
                tts_url,
                text,
            ));
        }
    }
}

fn discord_to_personaplex(pcm: Option<&[i16]>) -> [i16; PERSONAPLEX_FRAME_SAMPLES] {
    let mut output = [0; PERSONAPLEX_FRAME_SAMPLES];
    let Some(pcm) = pcm else { return output };
    for (sample, frames) in output.iter_mut().zip(pcm.chunks_exact(4)) {
        let sum = frames.iter().map(|value| i32::from(*value)).sum::<i32>();
        *sample = (sum / 4) as i16;
    }
    output
}

#[derive(Clone)]
struct Receiver {
    user: u64,
    generation: u64,
    capture: Arc<Mutex<Capture>>,
    http: Arc<Http>,
    personaplex: Option<mpsc::Sender<[i16; PERSONAPLEX_FRAME_SAMPLES]>>,
}

#[async_trait]
impl songbird::EventHandler for Receiver {
    async fn act(&self, event: &EventContext<'_>) -> Option<Event> {
        let previous = {
            let mut capture = self.capture.lock().unwrap();
            match event {
                EventContext::SpeakingStateUpdate(speaking) => speaking.user_id
                    .and_then(|user| capture.identify(self.generation, speaking.ssrc, user.0 == self.user)),
                EventContext::VoiceTick(tick) => {
                    let voice = capture.ssrc.and_then(|ssrc| tick.speaking.get(&ssrc));
                    if let Some(personaplex) = &self.personaplex {
                        let frame = discord_to_personaplex(
                            voice.and_then(|voice| voice.decoded_voice.as_deref()));
                        _ = personaplex.try_send(frame);
                    } else {
                        capture.voice_tick(self.generation,
                            voice.and_then(|voice| voice.decoded_voice.as_deref()),
                            voice.is_some_and(|voice| voice.packet.is_some()));
                    }
                    None
                }
                EventContext::ClientDisconnect(client) if client.user_id.0 == self.user => {
                    if capture.desired.generation == self.generation {
                        capture.reset_transport()
                    } else { None }
                }
                EventContext::DriverConnect(_) => {
                    tracing::info!("Voice transport connected; awaiting operator PCM");
                    None
                }
                EventContext::DriverReconnect(_) | EventContext::DriverDisconnect(_) => {
                    tracing::warn!("Voice transport changed; requiring fresh speaker identity and /voice on");
                    if capture.desired.generation == self.generation {
                        capture.reset_transport()
                    } else { None }
                }
                _ => None,
            }
        };
        if let Some(thread) = previous {
            let http = Arc::clone(&self.http);
            tokio::spawn(async move {
                notify_thread(&http, thread,
                    "Voice is off because the voice connection or speaker identity changed. Use /voice on to arm again.").await;
            });
        }
        None
    }
}

async fn leave(manager: &Songbird, guild: GuildId) -> Result<()> {
    if manager.get(guild).is_some() {
        manager.remove(guild).await.context("leaving voice channel")?;
        tracing::info!("Left voice channel");
    }
    Ok(())
}

async fn follow(
    manager: Arc<Songbird>,
    guild: GuildId,
    channel: ChannelId,
    user: UserId,
    capture: Arc<Mutex<Capture>>,
    personaplex: Option<mpsc::Sender<[i16; PERSONAPLEX_FRAME_SAMPLES]>>,
    mut desired: watch::Receiver<Desired>,
    http: Arc<Http>,
) -> Result<()> {
    loop {
        desired.changed().await.context("voice membership watcher stopped")?;
        let state = *desired.borrow_and_update();
        leave(&manager, guild).await?;
        if !state.present {
            continue;
        }
        let call = manager.get_or_insert(guild);
        {
            let mut call = call.lock().await;
            call.mute(false).await?;
            call.deafen(false).await?;
            let receiver = Receiver { user: user.get(), generation: state.generation,
                capture: Arc::clone(&capture), http: Arc::clone(&http),
                personaplex: personaplex.clone() };
            for event in [CoreEvent::SpeakingStateUpdate, CoreEvent::VoiceTick,
                CoreEvent::ClientDisconnect, CoreEvent::DriverConnect,
                CoreEvent::DriverReconnect, CoreEvent::DriverDisconnect]
            {
                call.add_global_event(event.into(), receiver.clone());
            }
        }
        // Leaving during a handshake cancels it; the next iteration removes its partial call.
        tokio::select! {
            biased;
            result = desired.changed() => {
                result.context("voice membership watcher stopped")?;
                desired.mark_changed();
            }
            result = manager.join(guild, channel) => {
                result.context("joining configured voice channel")?;
                tracing::info!("Joined General; receive-only, no recordings");
            }
        }
    }
}
struct Unseekable(DuplexStream);

impl AsyncRead for Unseekable {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>) -> Poll<io::Result<()>>
    {
        Pin::new(&mut self.0).poll_read(cx, buffer)
    }
}

impl AsyncSeek for Unseekable {
    fn start_seek(self: Pin<&mut Self>, _position: SeekFrom) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "live PersonaPlex stream"))
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>)
        -> Poll<io::Result<u64>>
    {
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::Unsupported, "live PersonaPlex stream")))
    }
}

#[async_trait]
impl AsyncMediaSource for Unseekable {
    fn is_seekable(&self) -> bool { false }
    async fn byte_len(&self) -> Option<u64> { None }
}

struct PersonaOpus {
    encoder: OpusEncoder,
    writer: PacketWriter<'static, Cursor<Vec<u8>>>,
    emitted: usize,
    granule: u64,
}

impl PersonaOpus {
    fn new() -> Result<Self> {
        let mut encoder = OpusEncoder::new(
            24_000, OpusChannels::Mono, Application::LowDelay)?;
        encoder.set_complexity(0)?;
        let pre_skip = u16::try_from(encoder.get_lookahead()? * 2)
            .context("Opus lookahead exceeds Ogg header")?;
        let mut writer = PacketWriter::new(Cursor::new(Vec::with_capacity(64 * 1024)));
        let mut head = Vec::with_capacity(19);
        head.extend_from_slice(b"OpusHead");
        head.push(1);
        head.push(1);
        head.extend_from_slice(&pre_skip.to_le_bytes());
        head.extend_from_slice(&24_000_u32.to_le_bytes());
        head.extend_from_slice(&0_i16.to_le_bytes());
        head.push(0);
        writer.write_packet(head, 1, PacketWriteEndInfo::EndPage, 0)?;
        let vendor = b"omp-discord-bridge";
        let mut tags = Vec::with_capacity(16 + vendor.len());
        tags.extend_from_slice(b"OpusTags");
        tags.extend_from_slice(&(vendor.len() as u32).to_le_bytes());
        tags.extend_from_slice(vendor);
        tags.extend_from_slice(&0_u32.to_le_bytes());
        writer.write_packet(tags, 1, PacketWriteEndInfo::EndPage, 0)?;
        Ok(Self { encoder, writer, emitted: 0, granule: 0 })
    }

    fn headers(&mut self) -> Vec<u8> {
        self.take_new_bytes()
    }

    fn encode(&mut self, pcm: &[i16; PERSONAPLEX_FRAME_SAMPLES]) -> Result<Vec<u8>> {
        let mut encoded = [0_u8; 1275];
        let len = self.encoder.encode(pcm, &mut encoded)?;
        self.granule += 960; // Ogg Opus granules are always measured at 48 kHz.
        self.writer.write_packet(encoded[..len].to_vec(), 1,
            PacketWriteEndInfo::EndPage, self.granule)?;
        Ok(self.take_new_bytes())
    }

    fn take_new_bytes(&mut self) -> Vec<u8> {
        let bytes = self.writer.inner().get_ref();
        let output = bytes[self.emitted..].to_vec();
        self.emitted = bytes.len();
        output
    }
}

async fn play_audio_stream(
    manager: Arc<Songbird>,
    guild: GuildId,
    reader: DuplexStream,
    source: &'static str,
    started: Instant,
) -> Result<()> {
    let stream = AsyncAdapterStream::new(Box::new(Unseekable(reader)), 256 * 1024);
    let input = Input::Live(LiveInput::Raw(AudioStream { input: Box::new(stream) }), None)
        .make_playable_async(get_codec_registry(), get_probe()).await
        .with_context(|| format!("parsing {source} audio stream"))?;
    let call = manager.get(guild).context("voice call unavailable")?;
    let mut call = call.lock().await;
    call.play_only(Track::from(input).volume(0.99));
    tracing::info!(source, elapsed_ms = started.elapsed().as_millis(),
        "Voice output track started");
    Ok(())
}

fn personaplex_endpoint(mut url: reqwest::Url) -> Result<reqwest::Url> {
    match url.scheme() {
        "http" => { url.set_scheme("ws").map_err(|_| anyhow::anyhow!("invalid PersonaPlex URL"))?; }
        "https" => { url.set_scheme("wss").map_err(|_| anyhow::anyhow!("invalid PersonaPlex URL"))?; }
        "ws" | "wss" => {}
        _ => anyhow::bail!("PersonaPlex URL must use http, https, ws, or wss"),
    }
    url.set_path("/api/chat");
    url.set_query(None);
    url.query_pairs_mut()
        .append_pair("voice_prompt", "NATF2.pt")
        .append_pair("text_prompt",
            "You are Hephaestus, a concise, friendly technical assistant. Speak naturally.");
    Ok(url)
}

struct ReviewCapture {
    directory: PathBuf,
    incoming: File,
    outgoing: File,
    model_text: File,
    timing: File,
    started: Instant,
    incoming_frames: u64,
    outgoing_bytes: u64,
    text_bytes: u64,
}

fn private_file(path: &Path) -> Result<File> {
    let file = fs::OpenOptions::new().create_new(true).write(true).mode(0o600)
        .open(path).with_context(|| format!("creating {}", path.display()))?;
    Ok(File::from_std(file))
}

fn prepare_review_root(root: &Path) -> Result<()> {
    fs::create_dir_all(root).with_context(|| format!("creating {}", root.display()))?;
    fs::set_permissions(root, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("securing {}", root.display()))?;
    let now = SystemTime::now();
    for entry in fs::read_dir(root).with_context(|| format!("reading {}", root.display()))? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let modified = entry.metadata()?.modified()?;
        if now.duration_since(modified).unwrap_or_default() >= REVIEW_RETENTION {
            fs::remove_dir_all(entry.path())
                .with_context(|| format!("removing expired {}", entry.path().display()))?;
        }
    }
    Ok(())
}

impl ReviewCapture {
    async fn new(root: &Path) -> Result<Self> {
        prepare_review_root(root)?;
        let elapsed = SystemTime::now().duration_since(UNIX_EPOCH)
            .context("system clock predates Unix epoch")?;
        let directory = root.join(format!("{}-{:03}", elapsed.as_secs(), elapsed.subsec_millis()));
        fs::create_dir(&directory)
            .with_context(|| format!("creating {}", directory.display()))?;
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
        let mut metadata = private_file(&directory.join("session.json"))?;
        metadata.write_all(br#"{
  "retention_hours": 24,
  "incoming": {"file": "incoming.pcm", "format": "s16le", "channels": 1, "sample_rate": 24000},
  "outgoing": {"file": "outgoing.ogg", "format": "ogg_opus"},
  "model_text": "model-text.txt",
  "timing": "timing.jsonl"
}
"#).await?;
        let mut capture = Self {
            incoming: private_file(&directory.join("incoming.pcm"))?,
            outgoing: private_file(&directory.join("outgoing.ogg"))?,
            model_text: private_file(&directory.join("model-text.txt"))?,
            timing: private_file(&directory.join("timing.jsonl"))?,
            directory,
            started: Instant::now(),
            incoming_frames: 0,
            outgoing_bytes: 0,
            text_bytes: 0,
        };
        capture.event("session_started").await?;
        Ok(capture)
    }

    async fn event(&mut self, event: &str) -> Result<()> {
        let line = serde_json::json!({
            "elapsed_ms": self.started.elapsed().as_millis(),
            "event": event,
        });
        self.timing.write_all(line.to_string().as_bytes()).await?;
        self.timing.write_all(b"\n").await?;
        Ok(())
    }

    async fn input(&mut self, frame: &[i16; PERSONAPLEX_FRAME_SAMPLES]) -> Result<()> {
        if self.incoming_frames == 0 {
            self.event("first_input_frame").await?;
        }
        let mut bytes = [0_u8; PERSONAPLEX_FRAME_SAMPLES * 2];
        for (encoded, sample) in bytes.chunks_exact_mut(2).zip(frame) {
            encoded.copy_from_slice(&sample.to_le_bytes());
        }
        self.incoming.write_all(&bytes).await?;
        self.incoming_frames += 1;
        Ok(())
    }

    async fn output(&mut self, bytes: &[u8]) -> Result<()> {
        if self.outgoing_bytes == 0 {
            self.event("first_output_bytes").await?;
        }
        self.outgoing.write_all(bytes).await?;
        self.outgoing_bytes += bytes.len() as u64;
        Ok(())
    }

    async fn text(&mut self, bytes: &[u8]) -> Result<()> {
        if self.text_bytes == 0 {
            self.event("first_model_text").await?;
        }
        self.model_text.write_all(bytes).await?;
        self.text_bytes += bytes.len() as u64;
        Ok(())
    }

    async fn finish(mut self) -> Result<()> {
        let summary = serde_json::json!({
            "elapsed_ms": self.started.elapsed().as_millis(),
            "event": "session_finished",
            "incoming_frames": self.incoming_frames,
            "outgoing_bytes": self.outgoing_bytes,
            "model_text_bytes": self.text_bytes,
        });
        self.timing.write_all(summary.to_string().as_bytes()).await?;
        self.timing.write_all(b"\n").await?;
        self.incoming.flush().await?;
        self.outgoing.flush().await?;
        self.model_text.flush().await?;
        self.timing.flush().await?;
        tracing::info!(directory = %self.directory.display(), "PersonaPlex review capture saved");
        Ok(())
    }
}

async fn run_personaplex_session(
    manager: Arc<Songbird>,
    guild: GuildId,
    endpoint: &reqwest::Url,
    review_root: Option<&Path>,
    frames: &mut mpsc::Receiver<[i16; PERSONAPLEX_FRAME_SAMPLES]>,
    desired: &mut watch::Receiver<Desired>,
) -> Result<()> {
    let (mut socket, _) = connect_async(endpoint.as_str()).await
        .context("connecting to PersonaPlex")?;
    loop {
        tokio::select! {
            result = desired.changed() => {
                result.context("voice membership watcher stopped")?;
                return Ok(());
            }
            message = socket.next() => {
                let message = message.context("PersonaPlex closed before handshake")??;
                if let WsMessage::Binary(data) = &message {
                    if data.as_ref() == [0] { break; }
                }
            }
        }
    }
    while frames.try_recv().is_ok() {}
    let mut opus = PersonaOpus::new()?;
    let mut first = Vec::with_capacity(1 + opus.writer.inner().get_ref().len());
    first.push(1);
    first.extend_from_slice(&opus.headers());
    socket.send(WsMessage::Binary(first.into())).await?;
    let mut review = match review_root {
        Some(root) => Some(ReviewCapture::new(root).await?),
        None => None,
    };
    let (reader, mut writer) = tokio::io::duplex(256 * 1024);
    tokio::spawn(async move {
        if let Err(error) = play_audio_stream(
            manager, guild, reader, "PersonaPlex", Instant::now()).await
        {
            tracing::warn!(%error, "PersonaPlex output failed");
        }
    });
    tracing::info!("PersonaPlex full-duplex session connected");
    loop {
        tokio::select! {
            result = desired.changed() => {
                result.context("voice membership watcher stopped")?;
                if let Some(review) = review.take() {
                    review.finish().await?;
                }
                return Ok(());
            }
            frame = frames.recv() => {
                let frame = frame.context("PersonaPlex input channel closed")?;
                if let Some(review) = &mut review {
                    review.input(&frame).await?;
                }
                let page = opus.encode(&frame)?;
                let mut message = Vec::with_capacity(1 + page.len());
                message.push(1);
                message.extend_from_slice(&page);
                socket.send(WsMessage::Binary(message.into())).await?;
            }
            message = socket.next() => {
                let message = message.context("PersonaPlex connection closed")??;
                match message {
                    WsMessage::Binary(data) if data.first() == Some(&1) => {
                        if let Some(review) = &mut review {
                            review.output(&data[1..]).await?;
                        }
                        writer.write_all(&data[1..]).await?;
                    }
                    WsMessage::Binary(data) if data.first() == Some(&2) => {
                        if let Some(review) = &mut review {
                            review.text(&data[1..]).await?;
                        }
                        if let Ok(text) = std::str::from_utf8(&data[1..]) {
                            tracing::debug!(text, "PersonaPlex inner speech");
                        }
                    }
                    WsMessage::Close(_) => anyhow::bail!("PersonaPlex closed the session"),
                    _ => {}
                }
            }
        }
    }
}

async fn personaplex(
    manager: Arc<Songbird>,
    guild: GuildId,
    url: Option<reqwest::Url>,
    review_root: Option<PathBuf>,
    mut frames: mpsc::Receiver<[i16; PERSONAPLEX_FRAME_SAMPLES]>,
    mut desired: watch::Receiver<Desired>,
) -> Result<()> {
    let Some(url) = url else { return std::future::pending().await };
    let endpoint = personaplex_endpoint(url)?;
    loop {
        while !desired.borrow().present {
            desired.changed().await.context("voice membership watcher stopped")?;
        }
        while frames.try_recv().is_ok() {}
        if let Err(error) = run_personaplex_session(Arc::clone(&manager), guild, &endpoint,
            review_root.as_deref(), &mut frames, &mut desired).await
        {
            tracing::warn!(%error, "PersonaPlex session ended");
        }
        if desired.borrow().present {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}


async fn transcribe(
    capture: Arc<Mutex<Capture>>,
    manager: Arc<Songbird>,
    guild: GuildId,
    feedback: Option<Arc<FeedbackAudio>>,
    url: Option<reqwest::Url>,
    key: Option<String>,
    bridge: Option<Arc<BridgeRoute>>,
) -> Result<()> {
    let Some(url) = url else { return std::future::pending().await };
    let endpoint = url.join("/v1/audio/transcriptions")?;
    let client = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(3)).timeout(Duration::from_secs(15)).build()?;
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    loop {
        interval.tick().await;
        let audio = capture.lock().unwrap().ready.pop_front();
        let Some(Utterance { wav, sequence, ended, epoch, destination }) = audio else { continue };
        if !capture.lock().unwrap().accepts(epoch, destination) { continue }
        let started = Instant::now();
        tracing::info!(sequence, queue_ms = ended.elapsed().as_millis(), "Submitting speech chunk");
        let form = reqwest::multipart::Form::new().text("model", "voice-asr")
            .text("language", "en")
            .text("max_completion_tokens", "256")
            .part("file", reqwest::multipart::Part::bytes(wav).file_name("speech.wav").mime_str("audio/wav")?);
        let result = async {
            let response = client.post(endpoint.clone()).bearer_auth(key.as_deref().unwrap())
                .multipart(form).send().await?.error_for_status()?;
            let value: serde_json::Value = response.json().await?;
            value.get("text").and_then(|text| text.as_str()).map(str::to_owned)
                .context("ASR response missing text")
        }.await;
        if !capture.lock().unwrap().accepts(epoch, destination) { continue }
        match result {
            Ok(text) if !text.trim().is_empty() => {
                tracing::info!(text, sequence, elapsed_ms = started.elapsed().as_millis(), "Operator transcript");
                if let (Some(bridge), Some(thread)) = (&bridge, destination) {
                    if text.len() > voice_ingress::MAX_TRANSCRIPT_BYTES {
                        notify_thread(&bridge.http, thread, "Voice transcript was too long to submit; it was discarded without retry.").await;
                        continue;
                    }
                    let request = VoiceRequest { guild_id: bridge.guild.get(), user_id: bridge.user.get(),
                        thread_id: thread.get(), text: Some(text) };
                    if !capture.lock().unwrap().accepts(epoch, destination) { continue }
                    // No retry or cancellation after dispatch: an ambiguous acknowledgement may
                    // already have admitted a Human turn. Off only fences results not yet sent.
                    match voice_ingress::request(&bridge.socket, &request).await {
                        Ok(response) if response.ok => {
                            let generation = capture.lock().unwrap().begin_feedback(thread);
                            if let Some(feedback) = &feedback {
                                tokio::spawn(play_waiting_feedback(
                                    Arc::clone(&manager), guild, Arc::clone(&capture),
                                    Arc::clone(feedback), generation));
                            }
                        }
                        Ok(_) => notify_thread(&bridge.http, thread,
                            "Voice submission failed or is uncertain; it may already have been admitted. No retry was made. Check this thread before repeating it.").await,
                        Err(_) => notify_thread(&bridge.http, thread,
                            "Voice submission was not acknowledged; it may already have been admitted. No retry was made. Check this thread before repeating it.").await,
                    }
                }
            }
            Ok(_) => {}
            Err(_) => {
                tracing::warn!("ASR request failed; audio discarded without retry");
                if let (Some(bridge), Some(thread)) = (&bridge, destination) {
                    notify_thread(&bridge.http, thread,
                        "Speech recognition failed; no text was sent. Audio was discarded without retry.").await;
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Keep dependency payloads and credentials out of verbose protocol logs.
    tracing_subscriber::fmt()
        .with_env_filter("warn,voice_watch=info")
        .with_ansi(false)
        .init();
    let args = Args::parse();
    let stt_key = args.stt_key_file.map(|token_file| DiscordConfig {
        token_file, thread_auto_archive_minutes: 1440, guilds: Vec::new(),
    }.load_token()).transpose()?;
    let token = DiscordConfig {
        token_file: args.token_file,
        thread_auto_archive_minutes: 1440,
        guilds: Vec::new(),
    }.load_token()?;
    let guild = GuildId::new(args.guild_id.get());
    let channel = ChannelId::new(args.channel_id.get());
    let user = UserId::new(args.user_id.get());
    let http = Arc::new(Http::new(&token));
    let me = http.get_current_user().await?;
    ensure!(me.id.get() == args.bot_id.get(), "token does not belong to the selected test bot");
    ensure!(!guild.member(&http, user).await?.user.bot, "target must be a human guild member");
    let Channel::Guild(target) = channel.to_channel(&http).await? else {
        anyhow::bail!("target is not a guild channel");
    };
    ensure!(target.guild_id == guild && target.kind == ChannelType::Voice,
        "target must be a voice channel in the configured guild");

    let native_voice = args.personaplex_url.is_some();
    let decode_rate = if native_voice { SampleRate::Hz48000 } else { SampleRate::Hz16000 };
    let manager = Songbird::serenity_from_config(
        songbird::Config::default().decode_mode(DecodeMode::Decode(
            DecodeConfig::new(Channels::Stereo, decode_rate))),
    );
    let capture = Arc::new(Mutex::new(Capture::new(BUFFER_SAMPLES)));
    capture.lock().unwrap().routing = args.bridge_socket.is_some();
    let bridge = args.bridge_socket.map(|socket| Arc::new(BridgeRoute {
        socket, guild, user, http: Arc::clone(&http),
    }));
    let (desired, changes) = watch::channel(capture.lock().unwrap().desired);
    let (personaplex_tx, personaplex_rx) = mpsc::channel(PERSONAPLEX_QUEUE_FRAMES);
    let personaplex_sender = native_voice.then_some(personaplex_tx);
    let personaplex_url = args.personaplex_url.clone();
    let review_dir = args.review_dir.clone();
    let persona_changes = changes.clone();
    let tts_url = args.tts_url.clone();
    let feedback = match &tts_url {
        Some(url) => Some(Arc::new(load_feedback_audio(url).await
            .context("preloading voice acknowledgement")?)),
        None => None,
    };
    let handler = Watcher {
        guild,
        channel,
        user,
        desired,
        capture: Arc::clone(&capture),
        bridge: bridge.clone(),
        bot: me.id,
        manager: Arc::clone(&manager),
        tts_url,
    };
    let mut client = Client::builder(
        &token,
        GatewayIntents::GUILDS
            | GatewayIntents::GUILD_VOICE_STATES
            | GatewayIntents::GUILD_MESSAGES,
    )
        .event_handler(handler)
        .register_songbird_with(Arc::clone(&manager))
        .await?;
    let shard_manager = Arc::clone(&client.shard_manager);
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let result = tokio::select! {
        result = client.start() => result.context("voice gateway stopped"),
        result = follow(Arc::clone(&manager), guild, channel, user, Arc::clone(&capture),
            personaplex_sender, changes, Arc::clone(&http)) => result,
        result = transcribe(Arc::clone(&capture), Arc::clone(&manager), guild, feedback,
            args.stt_url, stt_key, bridge) => result,
        result = personaplex(Arc::clone(&manager), guild, personaplex_url, review_dir,
            personaplex_rx, persona_changes) => result,
        result = tokio::signal::ctrl_c() => result.context("waiting for Ctrl-C"),
        _ = terminate.recv() => Ok(()),
    };
    {
        let mut capture = capture.lock().unwrap();
        capture.desired.present = false;
        capture.reset_transport();
    }
    let cleanup = leave(&manager, guild).await;
    shard_manager.shutdown_all().await;
    result.and(cleanup)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chronological(buffer: &AudioBuffer) -> Vec<i16> {
        let start = (buffer.next + buffer.samples.len() - buffer.len) % buffer.samples.len();
        (0..buffer.len).map(|i| buffer.samples[(start + i) % buffer.samples.len()]).collect()
    }


    #[test]
    fn pcm_ring_keeps_newest_samples_across_wrap_and_oversize_frames() {
        let mut buffer = AudioBuffer::new(5);
        buffer.push(&[1, 2, 3]);
        buffer.push(&[4, 5, 6, 7]);
        assert_eq!(chronological(&buffer), [3, 4, 5, 6, 7]);
        buffer.push(&[8, 9, 10, 11, 12, 13]);
        assert_eq!(chronological(&buffer), [9, 10, 11, 12, 13]);
        let wav = buffer.wav();
        assert_eq!(&wav[..4], b"RIFF");
        assert_eq!(u32::from_le_bytes(wav[24..28].try_into().unwrap()), 16_000);
        assert_eq!(u16::from_le_bytes(wav[22..24].try_into().unwrap()), 2);
        assert_eq!(u32::from_le_bytes(wav[40..44].try_into().unwrap()), 10);
        let samples: Vec<i16> = wav[44..].chunks_exact(2)
            .map(|bytes| i16::from_le_bytes(bytes.try_into().unwrap())).collect();
        assert_eq!(samples, [9, 10, 11, 12, 13]);
        buffer.clear();
        assert_eq!(buffer.len, 0);
        assert!(buffer.samples.iter().all(|sample| *sample == 0));
    }

    #[test]
    fn capture_rejects_unknown_other_and_stale_speakers() {
        let mut capture = Capture::new(10);
        capture.desired = Desired { generation: 1, present: true };
        capture.receive(1, 42, &[99], false);
        assert_eq!(capture.frame_len, 0);
        capture.identify(1, 42, true);
        capture.receive(1, 43, &[99], false);
        capture.receive(0, 42, &[99], false);
        capture.receive(1, 42, &[1, -2, 0, 0], false);
        assert_eq!(&capture.frame[..capture.frame_len], &[1, -2, 0, 0]);
        assert_eq!(capture.nonzero_samples, 2);
        let epoch = capture.epoch;
        capture.identify(1, 42, false);
        capture.receive(1, 42, &[99], false);
        assert_eq!(capture.frame_len, 0);
        assert!(capture.epoch > epoch);
    }

    #[test]
    fn only_target_in_selected_guild_and_channel_changes_membership() {
        let capture = Arc::new(Mutex::new(Capture::new(10)));
        let (desired, changes) = watch::channel(capture.lock().unwrap().desired);
        let watcher = Watcher {
            guild: GuildId::new(1), channel: ChannelId::new(2),
            user: UserId::new(3), desired, capture: Arc::clone(&capture), bridge: None,
            bot: UserId::new(9), manager: Songbird::serenity(), tts_url: None,
        };
        watcher.voice_state(Some(GuildId::new(4)), watcher.user, Some(watcher.channel));
        watcher.voice_state(Some(watcher.guild), UserId::new(4), Some(watcher.channel));
        assert!(!changes.borrow().present);
        watcher.voice_state(Some(watcher.guild), watcher.user, Some(watcher.channel));
        let joined = *changes.borrow();
        assert!(joined.present);
        {
            let mut capture = capture.lock().unwrap();
            capture.identify(joined.generation, 42, true);
            capture.receive(joined.generation, 42, &[1, 2], false);
        }
        // Mute/deafen updates in the same channel must not reconnect or erase audio.
        watcher.voice_state(Some(watcher.guild), watcher.user, Some(watcher.channel));
        assert_eq!(*changes.borrow(), joined);
        assert_eq!(capture.lock().unwrap().frame_len, 2);
        watcher.voice_state(Some(watcher.guild), watcher.user, Some(ChannelId::new(9)));
        let mut capture = capture.lock().unwrap();
        assert!(!capture.desired.present);
        assert_eq!(capture.audio.len, 0);
        capture.identify(joined.generation, 42, true);
        capture.receive(joined.generation, 42, &[99], false);
        assert_eq!(capture.audio.len, 0);
    }

    #[test]
    fn fresh_startup_snapshot_joins_existing_member_and_invalidates_old_audio() {
        let capture = Arc::new(Mutex::new(Capture::new(10)));
        let (desired, changes) = watch::channel(capture.lock().unwrap().desired);
        let watcher = Watcher {
            guild: GuildId::new(1), channel: ChannelId::new(2),
            user: UserId::new(3), desired, capture: Arc::clone(&capture), bridge: None,
            bot: UserId::new(9), manager: Songbird::serenity(), tts_url: None,
        };
        watcher.update(true, true);
        let old = *changes.borrow();
        watcher.update(true, true);
        let current = *changes.borrow();
        assert!(current.present);
        assert_ne!(current.generation, old.generation);
        let mut capture = capture.lock().unwrap();
        capture.identify(old.generation, 42, true);
        capture.receive(old.generation, 42, &[99], false);
        assert_eq!(capture.audio.len, 0);
    }

    fn wav_pcm(utterance: &Utterance) -> Vec<i16> {
        utterance.wav[44..].chunks_exact(2)
            .map(|bytes| i16::from_le_bytes(bytes.try_into().unwrap())).collect()
    }

    #[test]
    fn packetless_decoder_audio_is_preserved_and_silence_keeps_playout_time() {
        let mut capture = Capture::new(BUFFER_SAMPLES);
        capture.desired = Desired { generation: 1, present: true };
        capture.identify(1, 42, true);
        // A partial decoder frame must survive the next packet and must not become zeros.
        let pcm = [123; VAD_SAMPLES / 2];
        capture.voice_tick(1, Some(&pcm), false);
        capture.voice_tick(1, Some(&pcm), true);
        assert_eq!(chronological(&capture.pre_roll), vec![123; VAD_SAMPLES]);
        assert_eq!(capture.concealed_frames, 1);
        capture.voice_tick(1, None, false);
        assert_eq!(capture.silent_ticks, 1);
        assert_eq!(capture.frame_len, SAMPLE_RATE * CHANNELS / 50 - VAD_SAMPLES);
        let before = capture.pre_roll.len;
        capture.voice_tick(0, Some(&pcm), false);
        assert_eq!(capture.pre_roll.len, before);
    }

    #[test]
    fn speech_endpoint_preserves_preroll_and_short_pause_but_rejects_brief_noise() {
        let mut capture = Capture::new(BUFFER_SAMPLES);
        capture.frame.fill(1);
        for _ in 0..20 { capture.accept_frame(false); }
        capture.frame.fill(2);
        for _ in 0..MIN_SPEECH_FRAMES { capture.accept_frame(true); }
        capture.frame.fill(0);
        for _ in 0..END_SILENCE_FRAMES - 1 { capture.accept_frame(false); }
        assert!(capture.ready.is_empty());
        capture.frame.fill(3);
        capture.accept_frame(true);
        capture.frame.fill(0);
        for _ in 0..END_SILENCE_FRAMES { capture.accept_frame(false); }
        let pcm = wav_pcm(&capture.ready.pop_front().unwrap());
        assert_eq!(&pcm[..20 * VAD_SAMPLES], vec![1; 20 * VAD_SAMPLES]);
        assert_eq!(&pcm[20 * VAD_SAMPLES..(20 + MIN_SPEECH_FRAMES) * VAD_SAMPLES],
            vec![2; MIN_SPEECH_FRAMES * VAD_SAMPLES]);
        let marker = 20 + MIN_SPEECH_FRAMES + END_SILENCE_FRAMES - 1;
        assert_eq!(&pcm[marker * VAD_SAMPLES..(marker + 1) * VAD_SAMPLES],
            vec![3; VAD_SAMPLES]);
        assert_eq!(pcm.len(), (marker + 1 + END_SILENCE_FRAMES) * VAD_SAMPLES);
        capture.frame.fill(100);
        for _ in 0..MIN_SPEECH_FRAMES - 1 { capture.accept_frame(true); }
        capture.frame.fill(0);
        for _ in 0..END_SILENCE_FRAMES { capture.accept_frame(false); }
        assert!(capture.ready.is_empty());
    }

    #[test]
    fn duration_limit_keeps_overlap_and_queue_is_bounded_then_erased_on_reset() {
        let mut capture = Capture::new(BUFFER_SAMPLES);
        for i in 0..BUFFER_SAMPLES / VAD_SAMPLES {
            capture.frame.fill(i as i16);
            capture.accept_frame(true);
        }
        let first = wav_pcm(&capture.ready[0]);
        for _ in 0..MIN_SPEECH_FRAMES {
            capture.frame.fill(2000);
            capture.accept_frame(true);
        }
        capture.frame.fill(0);
        for _ in 0..END_SILENCE_FRAMES { capture.accept_frame(false); }
        let second = wav_pcm(&capture.ready[1]);
        let overlap = PRE_ROLL_SAMPLES;
        assert_eq!(&first[first.len() - overlap..], &second[..overlap]);
        for _ in 0..MIN_SPEECH_FRAMES { capture.accept_frame(true); }
        for _ in 0..END_SILENCE_FRAMES { capture.accept_frame(false); }
        assert_eq!(capture.ready.len(), MAX_QUEUED_UTTERANCES);
        let epoch = capture.epoch;
        capture.reset();
        assert!(capture.ready.is_empty());
        assert_eq!(capture.pre_roll.len, 0);
        assert!(capture.frame.iter().all(|&sample| sample == 0));
        assert!(capture.epoch > epoch);
    }

    #[test]
    fn actual_detector_does_not_submit_silence_and_framing_ignores_packet_boundaries() {
        let mut capture = Capture::new(BUFFER_SAMPLES);
        for _ in 0..100 { capture.feed(&[0; SAMPLE_RATE * CHANNELS / 50]); }
        assert!(capture.ready.is_empty());
        assert!(!capture.active);
        let samples: Vec<i16> = (0..4096).map(|i| (i % 97) as i16 - 48).collect();
        let mut whole = Capture::new(BUFFER_SAMPLES);
        let mut split = Capture::new(BUFFER_SAMPLES);
        whole.feed(&samples);
        for packet in samples.chunks(126) { split.feed(packet); }
        assert_eq!(chronological(&whole.pre_roll), chronological(&split.pre_roll));
        assert_eq!(chronological(&whole.audio), chronological(&split.audio));
        assert_eq!(whole.frame_len, split.frame_len);
        assert_eq!(whole.speech_frames, split.speech_frames);
    }

    fn routing_watcher() -> Watcher {
        let mut state = Capture::new(BUFFER_SAMPLES);
        state.routing = true;
        let capture = Arc::new(Mutex::new(state));
        let (desired, _) = watch::channel(capture.lock().unwrap().desired);
        Watcher {
            guild: GuildId::new(1), channel: ChannelId::new(2), user: UserId::new(3),
            desired, capture, bridge: None, bot: UserId::new(9),
            manager: Songbird::serenity(), tts_url: None,
        }
    }

    fn queue_speech(capture: &mut Capture) {
        capture.frame.fill(123);
        for _ in 0..MIN_SPEECH_FRAMES { capture.accept_frame(true); }
        capture.frame.fill(0);
        for _ in 0..END_SILENCE_FRAMES { capture.accept_frame(false); }
    }

    #[test]
    fn arm_before_join_survives_membership_and_initial_speaker_assignment() {
        let watcher = routing_watcher();
        let thread = ChannelId::new(10);
        let revision = {
            let mut capture = watcher.capture.lock().unwrap();
            let revision = capture.begin_command();
            assert_eq!(capture.bind_if_current(revision, thread), Some((None, false)));
            revision
        };
        assert_eq!(watcher.update(true, false), None);
        let mut capture = watcher.capture.lock().unwrap();
        let generation = capture.desired.generation;
        assert_eq!(capture.identify(generation, 42, true), None);
        assert_eq!(capture.binding, Some(thread));
        assert_eq!(capture.command_revision, revision);
        capture.receive(generation, 42, &[1, 2], false);
        assert_eq!(capture.frame_len, 2);
        let epoch = capture.epoch;
        assert_eq!(capture.identify(generation, 42, true), None);
        assert_eq!(capture.epoch, epoch);
        assert_eq!(capture.frame_len, 2);
    }

    #[test]
    fn unarmed_audio_is_ignored_and_rearming_clears_every_pending_buffer() {
        let mut capture = Capture::new(BUFFER_SAMPLES);
        capture.routing = true;
        capture.desired = Desired { generation: 1, present: true };
        assert_eq!(capture.identify(1, 42, true), None);
        capture.receive(1, 42, &[123; VAD_SAMPLES], false);
        capture.voice_tick(1, None, false);
        assert_eq!(capture.frames, 0);
        assert_eq!(capture.silent_ticks, 0);
        assert_eq!(capture.pre_roll.len, 0);
        assert_eq!(capture.frame_len, 0);
        let thread = ChannelId::new(10);
        assert_eq!(capture.bind(thread), None);
        queue_speech(&mut capture);
        capture.receive(1, 42, &[1, 2], false);
        assert!(!capture.ready.is_empty());
        assert!(capture.pre_roll.len > 0);
        assert_eq!(capture.bind(thread), Some(thread));
        assert!(capture.ready.is_empty());
        assert_eq!(capture.audio.len, 0);
        assert_eq!(capture.pre_roll.len, 0);
        assert_eq!(capture.frame_len, 0);
        assert!(capture.frame.iter().all(|&sample| sample == 0));
        assert_eq!(capture.ssrc, Some(42));
        capture.receive(1, 42, &[3, 4], false);
        assert_eq!(&capture.frame[..capture.frame_len], &[3, 4]);
    }

    #[test]
    fn recognized_results_keep_their_capture_binding_and_never_revive_after_off() {
        let mut capture = Capture::new(BUFFER_SAMPLES);
        capture.routing = true;
        capture.desired = Desired { generation: 1, present: true };
        let first = ChannelId::new(10);
        let second = ChannelId::new(11);
        assert_eq!(capture.bind(first), None);
        queue_speech(&mut capture);
        let in_flight = capture.ready.pop_front().unwrap();
        assert_eq!(in_flight.destination, Some(first));
        assert!(capture.accepts(in_flight.epoch, in_flight.destination));
        assert_eq!(capture.bind(second), Some(first));
        assert!(!capture.accepts(in_flight.epoch, in_flight.destination));
        queue_speech(&mut capture);
        let rebound = capture.ready.pop_front().unwrap();
        assert_eq!(rebound.destination, Some(second));
        assert!(capture.accepts(rebound.epoch, rebound.destination));
        queue_speech(&mut capture);
        assert_eq!(capture.disarm(), Some(second));
        assert!(capture.ready.is_empty());
        assert!(!capture.accepts(rebound.epoch, rebound.destination));
        assert_eq!(capture.bind(second), None);
        assert!(!capture.accepts(rebound.epoch, rebound.destination));
        assert!(!capture.accepts(in_flight.epoch, in_flight.destination));
    }

    #[test]
    fn pending_on_validation_cannot_undo_off_or_a_newer_destination() {
        let mut capture = Capture::new(BUFFER_SAMPLES);
        capture.routing = true;
        let first = ChannelId::new(10);
        let second = ChannelId::new(11);
        let delayed_on = capture.begin_command();
        assert_eq!(capture.disarm(), None);
        assert_eq!(capture.bind_if_current(delayed_on, first), None);
        assert_eq!(capture.binding, None);
        let delayed_on = capture.begin_command();
        let newer_on = capture.begin_command();
        assert_eq!(capture.bind_if_current(newer_on, second), Some((None, false)));
        assert_eq!(capture.bind_if_current(delayed_on, first), None);
        assert_eq!(capture.binding, Some(second));
    }

    #[test]
    fn departure_and_fresh_snapshots_fence_pending_validation_and_recognition() {
        let watcher = routing_watcher();
        let thread = ChannelId::new(10);
        let pending = watcher.capture.lock().unwrap().begin_command();
        assert_eq!(watcher.update(true, false), None);
        let in_flight = {
            let mut capture = watcher.capture.lock().unwrap();
            let generation = capture.desired.generation;
            assert_eq!(capture.identify(generation, 42, true), None);
            assert_eq!(capture.bind_if_current(pending, thread), Some((None, true)));
            queue_speech(&mut capture);
            capture.ready.pop_front().unwrap()
        };
        let pending = watcher.capture.lock().unwrap().begin_command();
        assert_eq!(watcher.update(false, false), Some(thread));
        {
            let mut capture = watcher.capture.lock().unwrap();
            assert_eq!(capture.bind_if_current(pending, thread), None);
            assert!(!capture.accepts(in_flight.epoch, in_flight.destination));
            assert_eq!(capture.binding, None);
            assert_eq!(capture.ssrc, None);
            assert_eq!(capture.bind(thread), None);
        }
        let pending = watcher.capture.lock().unwrap().begin_command();
        assert_eq!(watcher.update(true, true), Some(thread));
        let mut capture = watcher.capture.lock().unwrap();
        assert_eq!(capture.bind_if_current(pending, thread), None);
        assert!(!capture.accepts(in_flight.epoch, in_flight.destination));
    }

    #[test]
    fn changed_transport_or_reassigned_ssrc_disarms_and_fences_pending_on() {
        for replacement_is_target in [true, false] {
            let mut capture = Capture::new(BUFFER_SAMPLES);
            capture.routing = true;
            capture.desired = Desired { generation: 1, present: true };
            let thread = ChannelId::new(10);
            assert_eq!(capture.bind(thread), None);
            assert_eq!(capture.identify(1, 42, true), None);
            queue_speech(&mut capture);
            let in_flight = capture.ready.pop_front().unwrap();
            let pending = capture.begin_command();
            let ssrc = if replacement_is_target { 43 } else { 42 };
            assert_eq!(capture.identify(1, ssrc, replacement_is_target), Some(thread));
            assert_eq!(capture.binding, None);
            assert!(!capture.accepts(in_flight.epoch, in_flight.destination));
            assert_eq!(capture.bind_if_current(pending, thread), None);
            assert_eq!(capture.bind(thread), None);
            let pending = capture.begin_command();
            assert_eq!(capture.reset_transport(), Some(thread));
            assert_eq!(capture.ssrc, None);
            assert_eq!(capture.bind_if_current(pending, thread), None);
        }
    }

    #[test]
    fn voice_commands_require_the_selected_human_and_actual_guild_thread() {
        let guild = GuildId::new(1);
        let user = UserId::new(2);
        assert!(permitted_operator(Some(guild), user, false, guild, user));
        assert!(!permitted_operator(None, user, false, guild, user));
        assert!(!permitted_operator(Some(GuildId::new(3)), user, false, guild, user));
        assert!(!permitted_operator(Some(guild), UserId::new(3), false, guild, user));
        assert!(!permitted_operator(Some(guild), user, true, guild, user));
        for kind in [ChannelType::PublicThread, ChannelType::PrivateThread, ChannelType::NewsThread] {
            assert!(is_thread(kind));
        }
        for kind in [ChannelType::Text, ChannelType::Private, ChannelType::Voice, ChannelType::Forum] {
            assert!(!is_thread(kind));
        }
        let options = |value| serde_json::from_value::<Vec<CommandDataOption>>(value).unwrap();
        assert_eq!(voice_action(&options(serde_json::json!([{"name":"on","type":1,"options":[]}]))), Some(VoiceAction::On));
        assert_eq!(voice_action(&options(serde_json::json!([{"name":"off","type":1,"options":[]}]))), Some(VoiceAction::Off));
        assert_eq!(voice_action(&options(serde_json::json!([{"name":"elsewhere","type":1,"options":[]}]))), None);
        assert_eq!(voice_action(&options(serde_json::json!([{"name":"on","type":3,"value":"off"}]))), None);
        assert_eq!(voice_action(&options(serde_json::json!([{"name":"on","type":1,"options":[{"name":"thread","type":3,"value":"other"}]}]))), None);
        assert_eq!(voice_action(&options(serde_json::json!([{"name":"on","type":1,"options":[]},{"name":"off","type":1,"options":[]}]))), None);
        assert_eq!(voice_action(&[]), None);
    }
    #[test]
    fn personaplex_input_downmixes_stereo_and_halves_48_khz_rate() {
        let mut input = vec![0_i16; PERSONAPLEX_FRAME_SAMPLES * 4];
        input[0..8].copy_from_slice(&[100, 300, 500, 700, -100, -300, -500, -700]);
        let output = discord_to_personaplex(Some(&input));
        assert_eq!(output[0], 400);
        assert_eq!(output[1], -400);
        assert!(discord_to_personaplex(None).iter().all(|sample| *sample == 0));
    }

    #[test]
    fn personaplex_endpoint_and_opus_pages_match_server_protocol() {
        let endpoint = personaplex_endpoint("http://192.0.2.1:18998/old".parse().unwrap()).unwrap();
        assert_eq!(endpoint.scheme(), "ws");
        assert_eq!(endpoint.path(), "/api/chat");
        let query = endpoint.query_pairs().collect::<std::collections::HashMap<_, _>>();
        assert_eq!(query.get("voice_prompt").map(|value| value.as_ref()), Some("NATF2.pt"));
        assert!(query.contains_key("text_prompt"));

        let mut opus = PersonaOpus::new().unwrap();
        let headers = opus.headers();
        assert!(headers.starts_with(b"OggS"));
        assert!(headers.windows(8).any(|window| window == b"OpusHead"));
        assert!(headers.windows(8).any(|window| window == b"OpusTags"));
        let page = opus.encode(&[0; PERSONAPLEX_FRAME_SAMPLES]).unwrap();
        assert!(page.starts_with(b"OggS"));
    }

    #[tokio::test]
    async fn review_capture_is_private_complete_and_removes_expired_sessions() {
        use std::os::unix::fs::MetadataExt;

        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("review");
        fs::create_dir_all(&root).unwrap();
        let stale = root.join("stale");
        fs::create_dir(&stale).unwrap();
        let stale_handle = fs::File::open(&stale).unwrap();
        stale_handle.set_times(fs::FileTimes::new().set_modified(
            SystemTime::now() - REVIEW_RETENTION - Duration::from_secs(1))).unwrap();

        let mut review = ReviewCapture::new(&root).await.unwrap();
        let directory = review.directory.clone();
        review.input(&[7; PERSONAPLEX_FRAME_SAMPLES]).await.unwrap();
        review.output(b"OggSoutput").await.unwrap();
        review.text(b"hello").await.unwrap();
        review.finish().await.unwrap();

        assert!(!stale.exists());
        assert_eq!(fs::metadata(&root).unwrap().mode() & 0o777, 0o700);
        assert_eq!(fs::metadata(&directory).unwrap().mode() & 0o777, 0o700);
        assert_eq!(fs::metadata(directory.join("incoming.pcm")).unwrap().len(),
            (PERSONAPLEX_FRAME_SAMPLES * 2) as u64);
        assert_eq!(fs::read(directory.join("outgoing.ogg")).unwrap(), b"OggSoutput");
        assert_eq!(fs::read_to_string(directory.join("model-text.txt")).unwrap(), "hello");
        let timing = fs::read_to_string(directory.join("timing.jsonl")).unwrap();
        assert!(timing.contains("\"event\":\"session_finished\""));
        for file in ["incoming.pcm", "outgoing.ogg", "model-text.txt", "timing.jsonl",
            "session.json"]
        {
            assert_eq!(fs::metadata(directory.join(file)).unwrap().mode() & 0o777, 0o600);
        }
    }

    #[test]
    fn wait_feedback_is_generation_fenced_and_audio_is_playable() {
        let mut capture = Capture::new(BUFFER_SAMPLES);
        let thread = ChannelId::new(7);
        let first = capture.begin_feedback(thread);
        assert!(capture.feedback_current(first));
        assert!(!capture.take_reply(ChannelId::new(8)));
        assert!(capture.take_reply(thread));
        assert!(!capture.feedback_current(first));
        let second = capture.begin_feedback(thread);
        capture.reset_audio();
        assert!(!capture.feedback_current(second));

        let mut wav = b"RIFF\x28\0\0\0WAVEfmt ".to_vec();
        wav.extend_from_slice(&16_u32.to_le_bytes());
        wav.extend_from_slice(&1_u16.to_le_bytes());
        wav.extend_from_slice(&1_u16.to_le_bytes());
        wav.extend_from_slice(&24_000_u32.to_le_bytes());
        wav.extend_from_slice(&48_000_u32.to_le_bytes());
        wav.extend_from_slice(&2_u16.to_le_bytes());
        wav.extend_from_slice(&16_u16.to_le_bytes());
        wav.extend_from_slice(b"data\x04\0\0\0\0\0\0\x40");
        let pcm = wav_f32_pcm(&wav).unwrap();
        assert_eq!(f32::from_le_bytes(pcm[..4].try_into().unwrap()), 0.0);
        assert_eq!(f32::from_le_bytes(pcm[4..].try_into().unwrap()), 0.5);
        let thinking = thinking_pcm();
        assert_eq!(thinking.len(), 24_000 * 2 * 4);
        assert!(thinking.iter().any(|byte| *byte != 0));
    }

}

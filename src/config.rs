use std::{
    collections::HashSet,
    env, fs,
    io::Read,
    path::{Path, PathBuf},
    str::FromStr,
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::domain::{ChannelId, GuildId, UserId};

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub discord: DiscordConfig,
    pub matrix: Option<MatrixConfig>,
    pub omp: OmpConfig,
    pub runtime: RuntimeConfig,
    pub threadwatch: Option<ThreadwatchConfig>,
}

#[derive(Debug, Clone)]
pub struct DiscordConfig {
    pub token_file: PathBuf,
    pub thread_auto_archive_minutes: u16,
    pub guilds: Vec<GuildPolicy>,
}

#[derive(Debug, Clone)]
pub struct GuildPolicy {
    pub id: GuildId,
    pub allowed_channels: HashSet<ChannelId>,
    pub allowed_users: HashSet<UserId>,
    pub cwd: PathBuf,
}

#[derive(Debug, Clone)]
pub struct MatrixConfig {
    pub homeserver_url: reqwest::Url,
    pub access_token_file: PathBuf,
    pub session_store_path: PathBuf,
    pub rooms: Vec<MatrixRoomPolicy>,
}

#[derive(Debug, Clone)]
pub struct MatrixRoomPolicy {
    pub id: String,
    pub allowed_users: HashSet<String>,
    pub cwd: PathBuf,
    pub require_mention: bool,
}


#[derive(Debug, Clone)]
pub struct OmpConfig {
    pub executable: PathBuf,
    pub session_dir: PathBuf,
    pub allowed_root: PathBuf,
    pub model: Option<String>,
    pub title_model: Option<String>,
    pub thinking_level: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ThreadwatchConfig {
    pub server_url: String,
    pub ingest_token_file: PathBuf,
    pub bot_id: String,
    pub bot_name: String,
    pub data_dir: PathBuf,
    pub collect_interval_ms: u64,
    pub heartbeat_interval_ms: u64,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub database_path: PathBuf,
    pub max_concurrency: usize,
    pub discord_message_limit: usize,
    pub max_attachment_bytes: u64,
    pub max_attachments: usize,
    pub max_response_bytes: usize,
    pub discord_history_max_days: u16,
    pub discord_history_max_messages: usize,
    pub discord_history_max_bytes: usize,
    pub discord_history_timeout_seconds: u64,
    pub turn_timeout_seconds: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    discord: RawDiscordConfig,
    matrix: Option<RawMatrixConfig>,
    omp: RawOmpConfig,
    #[serde(default)]
    runtime: RawRuntimeConfig,
    threadwatch: Option<RawThreadwatchConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDiscordConfig {
    token_file: PathBuf,
    #[serde(default = "default_auto_archive_minutes")]
    thread_auto_archive_minutes: u16,
    guilds: Vec<RawGuildPolicy>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGuildPolicy {
    id: String,
    allowed_channels: Vec<String>,
    allowed_users: Vec<String>,
    cwd: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMatrixConfig {
    homeserver_url: String,
    access_token_file: PathBuf,
    session_store_path: PathBuf,
    rooms: Vec<RawMatrixRoomPolicy>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMatrixRoomPolicy {
    id: String,
    allowed_users: Vec<String>,
    #[serde(default)]
    require_mention: bool,
    cwd: PathBuf,
}


#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOmpConfig {
    #[serde(default = "default_omp_executable")]
    executable: PathBuf,
    session_dir: PathBuf,
    allowed_root: PathBuf,
    model: Option<String>,
    title_model: Option<String>,
    thinking_level: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawThreadwatchConfig {
    server_url: String,
    ingest_token_file: PathBuf,
    bot_id: String,
    bot_name: String,
    data_dir: PathBuf,
    #[serde(default = "default_threadwatch_collect_interval_ms")]
    collect_interval_ms: u64,
    #[serde(default = "default_threadwatch_heartbeat_interval_ms")]
    heartbeat_interval_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawRuntimeConfig {
    database_path: PathBuf,
    max_concurrency: usize,
    discord_message_limit: usize,
    max_attachment_bytes: u64,
    max_attachments: usize,
    max_response_bytes: usize,
    discord_history_max_days: u16,
    discord_history_max_messages: usize,
    discord_history_max_bytes: usize,
    discord_history_timeout_seconds: u64,
    turn_timeout_seconds: u64,
}

impl Default for RawRuntimeConfig {
    fn default() -> Self {
        Self {
            database_path: PathBuf::from("~/.local/state/omp-client-bridge/bridge.sqlite"),
            max_concurrency: 2,
            discord_message_limit: 1_900,
            max_attachment_bytes: 25_000_000,
            max_attachments: 10,
            max_response_bytes: 1_048_576,
            discord_history_max_days: 30,
            discord_history_max_messages: 5_000,
            discord_history_max_bytes: 131_072,
            discord_history_timeout_seconds: 30,
            turn_timeout_seconds: 3_600,
        }
    }
}

const fn default_auto_archive_minutes() -> u16 {
    1_440
}

fn default_omp_executable() -> PathBuf {
    PathBuf::from("omp")
}

const fn default_threadwatch_collect_interval_ms() -> u64 {
    2_000
}

const fn default_threadwatch_heartbeat_interval_ms() -> u64 {
    10_000
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let source = fs::read_to_string(path)
            .with_context(|| format!("failed to read configuration {}", path.display()))?;
        let raw: RawConfig = toml::from_str(&source)
            .with_context(|| format!("failed to parse configuration {}", path.display()))?;
        Self::from_raw(raw)
    }
    /// Replace the `omp.model` key in the file in place, preserving
    /// surrounding comments and formatting. The file must already exist;
    /// the change is re-validated with [`AppConfig::load`] before it is
    /// reported as applied.
    pub fn write_default_model(path: &Path, model: &str) -> Result<()> {
        if model.is_empty()
            || model.contains('"')
            || model.contains('\\')
            || model.chars().any(|ch| ch.is_whitespace() || ch.is_control())
        {
            bail!("invalid omp.model selection {model:?}");
        }
        let source = fs::read_to_string(path)
            .with_context(|| format!("failed to read configuration {}", path.display()))?;
        let mut lines: Vec<String> = source.lines().map(str::to_owned).collect();
        let heading = lines
            .iter()
            .position(|line| line.trim() == "[omp]")
            .context("configuration has no [omp] section")?;
        let section_end = lines[heading + 1..]
            .iter()
            .position(|line| line.trim_start().starts_with('['))
            .map(|offset| heading + 1 + offset)
            .unwrap_or(lines.len());
        let replacement = format!("model = {:?}", model);
        let mut replaced = false;
        for index in (heading + 1)..section_end {
            let trimmed = lines[index].trim();
            if let Some(rest) = trimmed.strip_prefix("model") {
                if !trimmed.starts_with("title_model")
                    && rest.trim_start().starts_with('=')
                {
                    lines[index] = replacement.clone();
                    replaced = true;
                    break;
                }
            }
        }
        if !replaced {
            lines.insert(heading + 1, replacement);
        }
        let mut updated = lines.join("\n");
        if source.ends_with('\n') {
            updated.push('\n');
        }
        fs::write(path, updated)
            .with_context(|| format!("failed to write configuration {}", path.display()))?;
        AppConfig::load(path)?;
        Ok(())
    }

    fn from_raw(raw: RawConfig) -> Result<Self> {
        if raw.discord.guilds.is_empty() {
            bail!("discord.guilds must contain at least one policy");
        }
        if !matches!(
            raw.discord.thread_auto_archive_minutes,
            60 | 1_440 | 4_320 | 10_080
        ) {
            bail!("discord.thread_auto_archive_minutes must be 60, 1440, 4320, or 10080");
        }

        let allowed_root = canonical_directory(&raw.omp.allowed_root, "omp.allowed_root")?;
        let session_dir = expand_home(&raw.omp.session_dir)?;
        let database_path = expand_home(&raw.runtime.database_path)?;

        let mut guild_ids = HashSet::new();
        let mut guilds = Vec::with_capacity(raw.discord.guilds.len());
        for policy in raw.discord.guilds {
            let id = GuildId::from_str(&policy.id)
                .with_context(|| format!("invalid Discord guild id `{}`", policy.id))?;
            if !guild_ids.insert(id) {
                bail!("discord guild {id} is configured more than once");
            }
            let allowed_channels = parse_nonempty_ids(
                policy.allowed_channels,
                "allowed_channels",
                ChannelId::from_str,
            )?;
            let allowed_users =
                parse_nonempty_ids(policy.allowed_users, "allowed_users", UserId::from_str)?;
            let cwd = canonical_directory(&policy.cwd, "discord.guilds[].cwd")?;
            if !cwd.starts_with(&allowed_root) {
                bail!(
                    "guild {id} cwd {} is outside omp.allowed_root {}",
                    cwd.display(),
                    allowed_root.display()
                );
            }
            guilds.push(GuildPolicy {
                id,
                allowed_channels,
                allowed_users,
                cwd,
            });
        }
        let matrix = raw
            .matrix
            .map(|matrix| -> Result<MatrixConfig> {
                let homeserver_url = reqwest::Url::parse(&matrix.homeserver_url)
                    .context("matrix.homeserver_url must be an HTTP(S) origin")?;
                if !matches!(homeserver_url.scheme(), "http" | "https")
                    || !homeserver_url.username().is_empty()
                    || homeserver_url.password().is_some()
                    || homeserver_url.host_str().is_none()
                    || homeserver_url.query().is_some()
                    || homeserver_url.fragment().is_some()
                {
                    bail!("matrix.homeserver_url must be an HTTP(S) origin");
                }
                if matrix.rooms.is_empty() {
                    bail!("matrix.rooms must contain at least one policy");
                }
                let mut room_ids = HashSet::new();
                let mut rooms = Vec::with_capacity(matrix.rooms.len());
                for policy in matrix.rooms {
                    if !policy.id.strip_prefix('!').is_some_and(|id| {
                        !id.is_empty()
                            && !id.chars().any(|character| character.is_whitespace() || character.is_control())
                    }) {
                        bail!("invalid Matrix room id `{}`", policy.id);
                    }
                    if !room_ids.insert(policy.id.clone()) {
                        bail!("Matrix room {} is configured more than once", policy.id);
                    }
                    let allowed_users = parse_nonempty_matrix_users(policy.allowed_users)?;
                    let cwd = canonical_directory(&policy.cwd, "matrix.rooms[].cwd")?;
                    if !cwd.starts_with(&allowed_root) {
                        bail!(
                            "Matrix room {} cwd {} is outside omp.allowed_root {}",
                            policy.id,
                            cwd.display(),
                            allowed_root.display()
                        );
                    }
                    rooms.push(MatrixRoomPolicy {
                        id: policy.id,
                        allowed_users,
                        require_mention: policy.require_mention,
                        cwd,
                    });
                }
                Ok(MatrixConfig {
                    homeserver_url,
                    access_token_file: expand_home(&matrix.access_token_file)?,
                    session_store_path: expand_home(&matrix.session_store_path)?,
                    rooms,
                })
            })
            .transpose()?;


        if raw.runtime.max_concurrency == 0 {
            bail!("runtime.max_concurrency must be at least 1");
        }
        if !(100..=2_000).contains(&raw.runtime.discord_message_limit) {
            bail!("runtime.discord_message_limit must be between 100 and 2000");
        }
        if raw.runtime.max_attachments == 0 || raw.runtime.max_attachments > 10 {
            bail!("runtime.max_attachments must be between 1 and 10");
        }
        if raw.runtime.max_response_bytes == 0 || raw.runtime.max_response_bytes > 67_108_864 {
            bail!("runtime.max_response_bytes must be between 1 and 67108864");
        }
        if raw.runtime.discord_history_max_days == 0 {
            bail!("runtime.discord_history_max_days must be at least 1");
        }
        if raw.runtime.discord_history_max_messages == 0 {
            bail!("runtime.discord_history_max_messages must be at least 1");
        }
        if !(1..=131_072).contains(&raw.runtime.discord_history_max_bytes) {
            bail!("runtime.discord_history_max_bytes must be between 1 and 131072");
        }
        if raw.runtime.discord_history_timeout_seconds == 0 {
            bail!("runtime.discord_history_timeout_seconds must be at least 1");
        }
        if raw.runtime.turn_timeout_seconds == 0 {
            bail!("runtime.turn_timeout_seconds must be at least 1");
        }
        let threadwatch = raw
            .threadwatch
            .map(|config| -> Result<ThreadwatchConfig> {
                let url = reqwest::Url::parse(&config.server_url)
                    .context("threadwatch.server_url must be a plain HTTP origin")?;
                if url.scheme() != "http"
                    || !url.username().is_empty()
                    || url.password().is_some()
                    || url.path() != "/"
                    || url.query().is_some()
                    || url.fragment().is_some()
                    || url.host_str().is_none()
                {
                    bail!("threadwatch.server_url must be a plain HTTP origin");
                }
                if config.bot_id.is_empty()
                    || config.bot_id.len() > 100
                    || !config
                        .bot_id
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
                {
                    bail!("threadwatch.bot_id is invalid");
                }
                if config.bot_name.trim().is_empty() {
                    bail!("threadwatch.bot_name must not be empty");
                }
                if !(100..=2_147_483_647).contains(&config.collect_interval_ms) {
                    bail!("threadwatch.collect_interval_ms is invalid");
                }
                if !(100..=30_000).contains(&config.heartbeat_interval_ms) {
                    bail!("threadwatch.heartbeat_interval_ms is invalid");
                }
                Ok(ThreadwatchConfig {
                    server_url: url.origin().ascii_serialization(),
                    ingest_token_file: expand_home(&config.ingest_token_file)?,
                    bot_id: config.bot_id,
                    bot_name: config.bot_name,
                    data_dir: expand_home(&config.data_dir)?,
                    collect_interval_ms: config.collect_interval_ms,
                    heartbeat_interval_ms: config.heartbeat_interval_ms,
                })
            })
            .transpose()?;


        Ok(Self {
            discord: DiscordConfig {
                token_file: expand_home(&raw.discord.token_file)?,
                thread_auto_archive_minutes: raw.discord.thread_auto_archive_minutes,
                guilds,
            },
            matrix,
            omp: OmpConfig {
                executable: expand_home(&raw.omp.executable)?,
                session_dir,
                allowed_root,
                model: nonempty_optional(raw.omp.model, "omp.model")?,
                title_model: nonempty_optional(raw.omp.title_model, "omp.title_model")?,
                thinking_level: nonempty_optional(raw.omp.thinking_level, "omp.thinking_level")?,
            },
            runtime: RuntimeConfig {
                database_path,
                max_concurrency: raw.runtime.max_concurrency,
                discord_message_limit: raw.runtime.discord_message_limit,
                max_attachment_bytes: raw.runtime.max_attachment_bytes,
                max_attachments: raw.runtime.max_attachments,
                max_response_bytes: raw.runtime.max_response_bytes,
                discord_history_max_days: raw.runtime.discord_history_max_days,
                discord_history_max_messages: raw.runtime.discord_history_max_messages,
                discord_history_max_bytes: raw.runtime.discord_history_max_bytes,
                discord_history_timeout_seconds: raw.runtime.discord_history_timeout_seconds,
                turn_timeout_seconds: raw.runtime.turn_timeout_seconds,
            },
            threadwatch,
        })
    }

    pub fn guild_policy(&self, guild_id: GuildId) -> Option<&GuildPolicy> {
        self.discord
            .guilds
            .iter()
            .find(|policy| policy.id == guild_id)
    }

    pub fn matrix_room_policy(&self, room_id: &str) -> Option<&MatrixRoomPolicy> {
        self.matrix
            .as_ref()?
            .rooms
            .iter()
            .find(|policy| policy.id == room_id)
    }
}

impl DiscordConfig {
    pub fn load_token(&self) -> Result<String> {
        load_protected_secret(&self.token_file, "Discord token")
    }
}

impl MatrixConfig {
    pub fn load_access_token(&self) -> Result<String> {
        load_protected_secret(&self.access_token_file, "Matrix access token")
    }
}


fn load_protected_secret(path: &Path, label: &str) -> Result<String> {
    const MAX_SECRET_BYTES: usize = 4_096;

    let file = fs::File::open(path)
        .with_context(|| format!("failed to open {label} file {}", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect {label} file {}", path.display()))?;
    if !metadata.is_file() {
        bail!("{label} path {} is not a regular file", path.display());
    }
    #[cfg(unix)]
    if metadata.permissions().mode() & 0o077 != 0 {
        bail!(
            "{label} file {} grants group or other access; run `chmod 600 {}`",
            path.display(),
            path.display()
        );
    }

    let mut secret = String::new();
    file.take((MAX_SECRET_BYTES + 1) as u64)
        .read_to_string(&mut secret)
        .with_context(|| format!("failed to read {label} file {}", path.display()))?;
    if secret.len() > MAX_SECRET_BYTES {
        bail!("{label} file exceeds {MAX_SECRET_BYTES} bytes");
    }
    while matches!(secret.as_bytes().last(), Some(b'\r' | b'\n')) {
        secret.pop();
    }
    if secret.is_empty() {
        bail!("{label} file is empty");
    }
    if secret.bytes().any(|byte| byte.is_ascii_whitespace()) {
        bail!("{label} contains whitespace");
    }
    Ok(secret)
}

impl GuildPolicy {
    pub fn permits(&self, channel_id: ChannelId, user_id: UserId) -> bool {
        self.allowed_channels.contains(&channel_id) && self.allowed_users.contains(&user_id)
    }
}

impl MatrixRoomPolicy {
    pub fn permits(&self, user_id: &str) -> bool {
        self.allowed_users.contains(user_id)
    }
}

fn parse_nonempty_matrix_users(values: Vec<String>) -> Result<HashSet<String>> {
    if values.is_empty() {
        bail!("matrix.rooms[].allowed_users must not be empty (access is default-deny)");
    }
    let mut parsed = HashSet::with_capacity(values.len());
    for value in values {
        if !value.starts_with('@') || !value.contains(':') {
            bail!("invalid Matrix user id `{value}`");
        }
        if !parsed.insert(value.clone()) {
            bail!("Matrix user {value} is configured more than once");
        }
    }
    Ok(parsed)
}

fn parse_nonempty_ids<T, E>(
    values: Vec<String>,
    field: &str,
    parse: impl Fn(&str) -> Result<T, E>,
) -> Result<HashSet<T>>
where
    T: Eq + std::hash::Hash,
    E: std::error::Error + Send + Sync + 'static,
{
    if values.is_empty() {
        bail!("discord.guilds[].{field} must not be empty (access is default-deny)");
    }
    values
        .into_iter()
        .map(|value| parse(&value).with_context(|| format!("invalid {field} id `{value}`")))
        .collect()
}

fn canonical_directory(path: &Path, field: &str) -> Result<PathBuf> {
    let expanded = expand_home(path)?;
    let canonical = expanded
        .canonicalize()
        .with_context(|| format!("{field} directory {} does not exist", expanded.display()))?;
    if !canonical.is_dir() {
        bail!("{field} {} is not a directory", canonical.display());
    }
    Ok(canonical)
}

fn expand_home(path: &Path) -> Result<PathBuf> {
    let text = path.to_string_lossy();
    if text == "~" || text.starts_with("~/") {
        let home = env::var_os("HOME").context("HOME is not set; cannot expand `~`")?;
        let mut expanded = PathBuf::from(home);
        if text.len() > 2 {
            expanded.push(&text[2..]);
        }
        Ok(expanded)
    } else {
        Ok(path.to_owned())
    }
}

fn nonempty_optional(value: Option<String>, field: &str) -> Result<Option<String>> {
    match value {
        Some(value) if value.trim().is_empty() => bail!("{field} must not be empty"),
        value => Ok(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_open_ended_access_lists() {
        let root = tempfile::tempdir().expect("tempdir");
        let raw = RawConfig {
            discord: RawDiscordConfig {
                token_file: root.path().join("discord-token"),
                thread_auto_archive_minutes: default_auto_archive_minutes(),
                guilds: vec![RawGuildPolicy {
                    id: "1".to_owned(),
                    allowed_channels: Vec::new(),
                    allowed_users: vec!["2".to_owned()],
                    cwd: root.path().to_owned(),
                }],
            },
            matrix: None,
            omp: RawOmpConfig {
                executable: default_omp_executable(),
                session_dir: root.path().join("sessions"),
                allowed_root: root.path().to_owned(),
                model: None,
                title_model: None,
                thinking_level: None,
            },
            runtime: RawRuntimeConfig::default(),
            threadwatch: None,
        };

        let error = AppConfig::from_raw(raw).expect_err("empty channel allowlist must fail");
        assert!(
            error
                .to_string()
                .contains("allowed_channels must not be empty")
        );
    }

    #[test]
    fn rejects_cwd_outside_allowed_root() {
        let allowed = tempfile::tempdir().expect("allowed tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let raw = RawConfig {
            discord: RawDiscordConfig {
                token_file: allowed.path().join("discord-token"),
                thread_auto_archive_minutes: default_auto_archive_minutes(),
                guilds: vec![RawGuildPolicy {
                    id: "1".to_owned(),
                    allowed_channels: vec!["2".to_owned()],
                    allowed_users: vec!["3".to_owned()],
                    cwd: outside.path().to_owned(),
                }],
            },
            matrix: None,
            omp: RawOmpConfig {
                executable: default_omp_executable(),
                session_dir: allowed.path().join("sessions"),
                allowed_root: allowed.path().to_owned(),
                model: None,
                title_model: None,
                thinking_level: None,
            },
            runtime: RawRuntimeConfig::default(),
            threadwatch: None,
        };

        let error = AppConfig::from_raw(raw).expect_err("cwd escape must fail");
        assert!(error.to_string().contains("outside omp.allowed_root"));
    }

    #[test]
    fn authorization_requires_matching_channel_and_user() {
        let root = tempfile::tempdir().expect("tempdir");
        let config = AppConfig::from_raw(RawConfig {
            discord: RawDiscordConfig {
                token_file: root.path().join("discord-token"),
                thread_auto_archive_minutes: default_auto_archive_minutes(),
                guilds: vec![RawGuildPolicy {
                    id: "1".to_owned(),
                    allowed_channels: vec!["2".to_owned()],
                    allowed_users: vec!["3".to_owned()],
                    cwd: root.path().to_owned(),
                }],
            },
            matrix: None,
            omp: RawOmpConfig {
                executable: default_omp_executable(),
                session_dir: root.path().join("sessions"),
                allowed_root: root.path().to_owned(),
                model: None,
                title_model: None,
                thinking_level: None,
            },
            runtime: RawRuntimeConfig::default(),
            threadwatch: None,
        })
        .expect("valid config");
        let policy = config.guild_policy(GuildId::new(1)).expect("guild policy");

        assert!(policy.permits(ChannelId::new(2), UserId::new(3)));
        assert!(!policy.permits(ChannelId::new(9), UserId::new(3)));
        assert!(!policy.permits(ChannelId::new(2), UserId::new(9)));
    }

    #[test]
    fn matrix_policy_is_client_scoped_and_default_deny() {
        let root = tempfile::tempdir().expect("tempdir");
        let mut raw = raw_with_runtime(root.path(), RawRuntimeConfig::default());
        raw.matrix = Some(RawMatrixConfig {
            homeserver_url: "http://matrix.example:8008".to_owned(),
            access_token_file: root.path().join("matrix-token"),
            session_store_path: root.path().join("matrix-store"),
            rooms: vec![RawMatrixRoomPolicy {
                id: "!opaque-room-id".to_owned(),
                allowed_users: vec!["@operator:matrix.example".to_owned()],
                require_mention: true,
                cwd: root.path().to_owned(),
            }],
        });

        let config = AppConfig::from_raw(raw).expect("valid Matrix config");
        let matrix = config.matrix.expect("Matrix config");
        assert_eq!(matrix.rooms.len(), 1);
        assert!(matrix.rooms[0].require_mention);
        assert!(matrix.rooms[0].permits("@operator:matrix.example"));
        assert!(!matrix.rooms[0].permits("@intruder:matrix.example"));
    }


    fn raw_with_runtime(root: &Path, runtime: RawRuntimeConfig) -> RawConfig {
        RawConfig {
            discord: RawDiscordConfig {
                token_file: root.join("discord-token"),
                thread_auto_archive_minutes: default_auto_archive_minutes(),
                guilds: vec![RawGuildPolicy {
                    id: "1".to_owned(),
                    allowed_channels: vec!["2".to_owned()],
                    allowed_users: vec!["3".to_owned()],
                    cwd: root.to_owned(),
                }],
            },
            matrix: None,
            omp: RawOmpConfig {
                executable: default_omp_executable(),
                session_dir: root.join("sessions"),
                allowed_root: root.to_owned(),
                model: None,
                title_model: None,
                thinking_level: None,
            },
            runtime,
            threadwatch: None,
        }
    }

    fn assert_invalid_history_runtime(
        root: &Path,
        expected: &str,
        mutate: impl FnOnce(&mut RawRuntimeConfig),
    ) {
        let mut runtime = RawRuntimeConfig::default();
        mutate(&mut runtime);
        let error = AppConfig::from_raw(raw_with_runtime(root, runtime))
            .expect_err("invalid history limit must fail");
        assert!(error.to_string().contains(expected));
    }

    #[test]
    fn validates_discord_history_resource_boundaries() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path();

        let at_byte_limit = RawRuntimeConfig {
            discord_history_max_bytes: 131_072,
            ..RawRuntimeConfig::default()
        };
        AppConfig::from_raw(raw_with_runtime(path, at_byte_limit))
            .expect("maximum history output size");

        assert_invalid_history_runtime(path, "max_days", |runtime| {
            runtime.discord_history_max_days = 0;
        });
        assert_invalid_history_runtime(path, "max_messages", |runtime| {
            runtime.discord_history_max_messages = 0;
        });
        assert_invalid_history_runtime(path, "between 1 and 131072", |runtime| {
            runtime.discord_history_max_bytes = 131_073;
        });
        assert_invalid_history_runtime(path, "timeout_seconds", |runtime| {
            runtime.discord_history_timeout_seconds = 0;
        });
    }

    fn valid_config_toml(root: &Path) -> String {
        format!(
            r#"[discord]
token_file = "{}"
thread_auto_archive_minutes = 1440

[[discord.guilds]]
id = "123456789012345678"
allowed_channels = ["234567890123456789"]
allowed_users = ["345678901234567890"]
cwd = "{}"

[omp]
executable = "omp"
session_dir = "{}"
allowed_root = "{}"
# model = "openai-codex/old"
# title_model = "openai-codex/title"
"#,
            root.join("token").display(),
            root.display(),
            root.join("sessions").display(),
            root.display(),
        )
    }

    #[test]
    fn write_default_model_inserts_and_replaces_model_line() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("config.toml");
        fs::write(&path, valid_config_toml(root.path())).expect("write config");

        AppConfig::write_default_model(&path, "openai-codex/new").expect("write model");
        let source = fs::read_to_string(&path).expect("read back");
        assert!(source.contains("model = \"openai-codex/new\""), "{source}");
        assert!(source.contains("# model = \"openai-codex/old\""), "{source}");
        assert!(source.contains("# title_model = \"openai-codex/title\""), "{source}");
        let reloaded = AppConfig::load(&path).expect("reload");
        assert_eq!(reloaded.omp.model.as_deref(), Some("openai-codex/new"));

        // A real model line is replaced in place, keeping comments intact.
        let path = root.path().join("config.toml");
        fs::write(
            &path,
            valid_config_toml(root.path()).replace(
                "# model = \"openai-codex/old\"",
                "model = \"openai-codex/old\"",
            ),
        )
        .expect("write config with real model line");
        AppConfig::write_default_model(&path, "openai-codex/changed").expect("replace model");
        let source = fs::read_to_string(&path).expect("read back");
        assert!(source.contains("model = \"openai-codex/changed\""), "{source}");
        assert!(!source.lines().any(|line| line.trim() == "model = \"openai-codex/old\""), "{source}");
        assert!(source.contains("# title_model = \"openai-codex/title\""), "{source}");
        assert_eq!(
            AppConfig::load(&path).expect("reload").omp.model.as_deref(),
            Some("openai-codex/changed")
        );

        assert!(AppConfig::write_default_model(&path, "bad value").is_err());
        assert!(AppConfig::write_default_model(&path, "").is_err());
    }

    #[test]
    fn title_model_is_optional_and_validated() {
        let raw: RawOmpConfig = toml::from_str(
            "session_dir = '/tmp/sessions'\nallowed_root = '/tmp'\nmodel = 'primary'",
        )
        .expect("existing configuration remains valid");
        assert!(raw.title_model.is_none());
        assert_eq!(raw.model.as_deref(), Some("primary"));

        let root = tempfile::tempdir().expect("tempdir");
        let mut raw = raw_with_runtime(root.path(), RawRuntimeConfig::default());
        raw.omp.title_model = Some("reviewer".to_owned());
        let config = AppConfig::from_raw(raw).expect("title model");
        assert_eq!(config.omp.title_model.as_deref(), Some("reviewer"));

        let mut raw = raw_with_runtime(root.path(), RawRuntimeConfig::default());
        raw.omp.title_model = Some("  ".to_owned());
        assert!(AppConfig::from_raw(raw).unwrap_err().to_string().contains("omp.title_model"));
    }
    #[cfg(unix)]
    #[test]
    fn loads_token_from_private_file() {
        let directory = tempfile::tempdir().expect("tempdir");
        let token_file = directory.path().join("discord-token");
        fs::write(&token_file, "secret-token\n").expect("write token");
        fs::set_permissions(&token_file, fs::Permissions::from_mode(0o600)).expect("protect token");
        let config = DiscordConfig {
            token_file,
            thread_auto_archive_minutes: default_auto_archive_minutes(),
            guilds: Vec::new(),
        };

        assert_eq!(config.load_token().expect("load token"), "secret-token");
    }

    #[cfg(unix)]
    #[test]
    fn rejects_token_file_readable_by_other_users() {
        let directory = tempfile::tempdir().expect("tempdir");
        let token_file = directory.path().join("discord-token");
        fs::write(&token_file, "secret-token\n").expect("write token");
        fs::set_permissions(&token_file, fs::Permissions::from_mode(0o644))
            .expect("set unsafe permissions");
        let config = DiscordConfig {
            token_file,
            thread_auto_archive_minutes: default_auto_archive_minutes(),
            guilds: Vec::new(),
        };

        let error = config
            .load_token()
            .expect_err("unsafe permissions must fail");
        assert!(error.to_string().contains("chmod 600"));
    }
}

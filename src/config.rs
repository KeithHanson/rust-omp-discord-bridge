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
    pub omp: OmpConfig,
    pub runtime: RuntimeConfig,
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
pub struct OmpConfig {
    pub executable: PathBuf,
    pub session_dir: PathBuf,
    pub allowed_root: PathBuf,
    pub model: Option<String>,
    pub title_model: Option<String>,
    pub thinking_level: Option<String>,
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
    omp: RawOmpConfig,
    #[serde(default)]
    runtime: RawRuntimeConfig,
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
            database_path: PathBuf::from("~/.local/state/omp-discord-bridge/bridge.sqlite"),
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

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let source = fs::read_to_string(path)
            .with_context(|| format!("failed to read configuration {}", path.display()))?;
        let raw: RawConfig = toml::from_str(&source)
            .with_context(|| format!("failed to parse configuration {}", path.display()))?;
        Self::from_raw(raw)
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

        Ok(Self {
            discord: DiscordConfig {
                token_file: expand_home(&raw.discord.token_file)?,
                thread_auto_archive_minutes: raw.discord.thread_auto_archive_minutes,
                guilds,
            },
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
        })
    }

    pub fn guild_policy(&self, guild_id: GuildId) -> Option<&GuildPolicy> {
        self.discord
            .guilds
            .iter()
            .find(|policy| policy.id == guild_id)
    }
}

impl DiscordConfig {
    pub fn load_token(&self) -> Result<String> {
        const MAX_TOKEN_BYTES: usize = 4_096;

        let file = fs::File::open(&self.token_file).with_context(|| {
            format!(
                "failed to open Discord token file {}",
                self.token_file.display()
            )
        })?;
        let metadata = file.metadata().with_context(|| {
            format!(
                "failed to inspect Discord token file {}",
                self.token_file.display()
            )
        })?;
        if !metadata.is_file() {
            bail!(
                "Discord token path {} is not a regular file",
                self.token_file.display()
            );
        }
        #[cfg(unix)]
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!(
                "Discord token file {} grants group or other access; run `chmod 600 {}`",
                self.token_file.display(),
                self.token_file.display()
            );
        }

        let mut token = String::new();
        file.take(4_097)
            .read_to_string(&mut token)
            .with_context(|| {
                format!(
                    "failed to read Discord token file {}",
                    self.token_file.display()
                )
            })?;
        if token.len() > MAX_TOKEN_BYTES {
            bail!("Discord token file exceeds {MAX_TOKEN_BYTES} bytes");
        }
        while matches!(token.as_bytes().last(), Some(b'\r' | b'\n')) {
            token.pop();
        }
        if token.is_empty() {
            bail!("Discord token file is empty");
        }
        if token.bytes().any(|byte| byte.is_ascii_whitespace()) {
            bail!("Discord token contains whitespace");
        }
        Ok(token)
    }
}

impl GuildPolicy {
    pub fn permits(&self, channel_id: ChannelId, user_id: UserId) -> bool {
        self.allowed_channels.contains(&channel_id) && self.allowed_users.contains(&user_id)
    }
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
            omp: RawOmpConfig {
                executable: default_omp_executable(),
                session_dir: root.path().join("sessions"),
                allowed_root: root.path().to_owned(),
                model: None,
                title_model: None,
                thinking_level: None,
            },
            runtime: RawRuntimeConfig::default(),
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
            omp: RawOmpConfig {
                executable: default_omp_executable(),
                session_dir: allowed.path().join("sessions"),
                allowed_root: allowed.path().to_owned(),
                model: None,
                title_model: None,
                thinking_level: None,
            },
            runtime: RawRuntimeConfig::default(),
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
            omp: RawOmpConfig {
                executable: default_omp_executable(),
                session_dir: root.path().join("sessions"),
                allowed_root: root.path().to_owned(),
                model: None,
                title_model: None,
                thinking_level: None,
            },
            runtime: RawRuntimeConfig::default(),
        })
        .expect("valid config");
        let policy = config.guild_policy(GuildId::new(1)).expect("guild policy");

        assert!(policy.permits(ChannelId::new(2), UserId::new(3)));
        assert!(!policy.permits(ChannelId::new(9), UserId::new(3)));
        assert!(!policy.permits(ChannelId::new(2), UserId::new(9)));
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
            omp: RawOmpConfig {
                executable: default_omp_executable(),
                session_dir: root.join("sessions"),
                allowed_root: root.to_owned(),
                model: None,
                title_model: None,
                thinking_level: None,
            },
            runtime,
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

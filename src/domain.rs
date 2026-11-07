use std::{fmt, path::PathBuf, str::FromStr};

macro_rules! snowflake {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(u64);

        impl $name {
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl FromStr for $name {
            type Err = ParseSnowflakeError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let parsed = value
                    .parse::<u64>()
                    .map_err(|_| ParseSnowflakeError(value.to_owned()))?;
                if parsed == 0 {
                    return Err(ParseSnowflakeError(value.to_owned()));
                }
                Ok(Self(parsed))
            }
        }
    };
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("`{0}` is not a valid Discord snowflake")]
pub struct ParseSnowflakeError(String);

snowflake!(GuildId);
snowflake!(ChannelId);
snowflake!(ThreadId);
snowflake!(UserId);
snowflake!(MessageId);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionStatus {
    Active,
    Stopped,
}

impl SessionStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Stopped => "stopped",
        }
    }

    pub fn from_database(value: &str) -> Result<Self, InvalidDatabaseState> {
        match value {
            "active" => Ok(Self::Active),
            "stopped" => Ok(Self::Stopped),
            _ => Err(InvalidDatabaseState(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnStatus {
    Queued,
    Running,
    Done,
    Failed,
    Aborted,
    Uncertain,
}

impl TurnStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Aborted => "aborted",
            Self::Uncertain => "uncertain",
        }
    }

    pub fn from_database(value: &str) -> Result<Self, InvalidDatabaseState> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "done" => Ok(Self::Done),
            "failed" => Ok(Self::Failed),
            "aborted" => Ok(Self::Aborted),
            "uncertain" => Ok(Self::Uncertain),
            _ => Err(InvalidDatabaseState(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("database contains unsupported state `{0}`")]
pub struct InvalidDatabaseState(String);

#[derive(Debug, Clone)]
pub struct Session {
    pub thread_id: ThreadId,
    pub guild_id: GuildId,
    pub parent_channel_id: ChannelId,
    pub trigger_message_id: MessageId,
    pub omp_session_path: Option<PathBuf>,
    pub cwd: PathBuf,
    pub created_by_user_id: UserId,
    pub status: SessionStatus,
}

#[derive(Debug, Clone)]
pub struct NewSession {
    pub thread_id: ThreadId,
    pub guild_id: GuildId,
    pub parent_channel_id: ChannelId,
    pub trigger_message_id: MessageId,
    pub cwd: PathBuf,
    pub created_by_user_id: UserId,
}

#[derive(Debug, Clone)]
pub struct Turn {
    pub id: i64,
    pub thread_id: ThreadId,
    pub discord_message_id: MessageId,
    pub author_id: UserId,
    pub content: String,
    pub status: TurnStatus,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TurnCounts {
    pub queued: u64,
    pub running: u64,
    pub uncertain: u64,
}

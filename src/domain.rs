use std::{fmt, path::PathBuf, str::FromStr};

use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ClientKind {
    Discord,
    Matrix,
}

impl ClientKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Discord => "discord",
            Self::Matrix => "matrix",
        }
    }
}

impl fmt::Display for ClientKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for ClientKind {
    type Err = ParseClientKindError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "discord" => Ok(Self::Discord),
            "matrix" => Ok(Self::Matrix),
            _ => Err(ParseClientKindError(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("`{0}` is not a supported client kind")]
pub struct ParseClientKindError(String);

macro_rules! client_scoped_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name {
            client: ClientKind,
            native: Arc<str>,
        }

        impl $name {
            pub fn scoped(
                client: ClientKind,
                native: impl Into<Arc<str>>,
            ) -> Result<Self, InvalidClientId> {
                let native = native.into();
                if native.is_empty() {
                    return Err(InvalidClientId);
                }
                Ok(Self { client, native })
            }

            pub fn discord(native: u64) -> Result<Self, InvalidClientId> {
                if native == 0 {
                    return Err(InvalidClientId);
                }
                Self::scoped(ClientKind::Discord, native.to_string())
            }

            pub const fn client(&self) -> ClientKind {
                self.client
            }

            pub fn native(&self) -> &str {
                &self.native
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{}:{}", self.client, self.native)
            }
        }
    };
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("client-native identifiers must not be empty")]
pub struct InvalidClientId;

client_scoped_id!(ClientConversationId);
client_scoped_id!(EventId);
client_scoped_id!(ActorId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConversationId(i64);

impl ConversationId {
    pub const fn new(value: i64) -> Result<Self, InvalidConversationId> {
        if value <= 0 {
            return Err(InvalidConversationId(value));
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("conversation database IDs must be positive, got {0}")]
pub struct InvalidConversationId(i64);

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
    pub conversation_id: ConversationId,
    pub thread_id: ThreadId,
    pub guild_id: GuildId,
    pub parent_channel_id: ChannelId,
    pub trigger_message_id: MessageId,
    pub omp_session_path: Option<PathBuf>,
    pub model: Option<String>,
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
    pub conversation_id: ConversationId,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_scoped_ids_do_not_collide() {
        let discord = ClientConversationId::scoped(ClientKind::Discord, "same").unwrap();
        let matrix = ClientConversationId::scoped(ClientKind::Matrix, "same").unwrap();

        assert_ne!(discord, matrix);
        assert_eq!(discord.to_string(), "discord:same");
        assert_eq!(matrix.to_string(), "matrix:same");
    }

    #[test]
    fn client_scoped_ids_reject_empty_native_ids() {
        assert!(EventId::scoped(ClientKind::Matrix, "").is_err());
        assert!(ActorId::scoped(ClientKind::Discord, "").is_err());
    }

    #[test]
    fn conversation_database_ids_are_positive() {
        assert!(ConversationId::new(0).is_err());
        assert_eq!(ConversationId::new(1).unwrap().get(), 1);
    }
}

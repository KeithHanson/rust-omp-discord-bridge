use std::{path::Path, str::FromStr, time::SystemTime};

use anyhow::{Context, Result, bail};
use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};

use crate::domain::{
    MessageId, NewSession, Session, SessionStatus, ThreadId, Turn, TurnCounts, TurnStatus, UserId,
};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS sessions (
    thread_id TEXT PRIMARY KEY NOT NULL,
    guild_id TEXT NOT NULL,
    parent_channel_id TEXT NOT NULL,
    trigger_message_id TEXT NOT NULL UNIQUE,
    omp_session_path TEXT,
    cwd TEXT NOT NULL,
    created_by_user_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'stopped')),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS turns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    thread_id TEXT NOT NULL REFERENCES sessions(thread_id) ON DELETE CASCADE,
    discord_message_id TEXT NOT NULL UNIQUE,
    author_id TEXT NOT NULL,
    content TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'done', 'failed', 'aborted', 'uncertain')),
    response TEXT,
    error TEXT,
    created_at_ms INTEGER NOT NULL,
    started_at_ms INTEGER,
    finished_at_ms INTEGER
);

CREATE INDEX IF NOT EXISTS turns_thread_status_id_idx ON turns(thread_id, status, id);
"#;

#[derive(Debug, Clone)]
pub struct Store {
    pool: SqlitePool,
}

#[derive(Debug, Clone)]
pub enum CreateSessionResult {
    Created(Session),
    Existing(Session),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueResult {
    Enqueued(i64),
    Duplicate,
}

impl Store {
    pub async fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!("failed to create database directory {}", parent.display())
            })?;
        }
        let options = sqlx::sqlite::SqliteConnectOptions::from_str(&format!(
            "sqlite://{}",
            path.to_string_lossy()
        ))?
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .with_context(|| format!("failed to open database {}", path.display()))?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn create_session(&self, input: &NewSession) -> Result<CreateSessionResult> {
        let now = now_ms()?;
        let result = sqlx::query(
            "INSERT INTO sessions (thread_id, guild_id, parent_channel_id, trigger_message_id, cwd, created_by_user_id, status, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?) ON CONFLICT(trigger_message_id) DO NOTHING",
        )
        .bind(input.thread_id.to_string())
        .bind(input.guild_id.to_string())
        .bind(input.parent_channel_id.to_string())
        .bind(input.trigger_message_id.to_string())
        .bind(input.cwd.to_string_lossy().into_owned())
        .bind(input.created_by_user_id.to_string())
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 1 {
            let session = self
                .session(input.thread_id)
                .await?
                .context("newly inserted session was not found")?;
            return Ok(CreateSessionResult::Created(session));
        }

        let session = self
            .session_by_trigger(input.trigger_message_id)
            .await?
            .context("conflicting trigger message has no session")?;
        Ok(CreateSessionResult::Existing(session))
    }

    pub async fn session(&self, thread_id: ThreadId) -> Result<Option<Session>> {
        let row = sqlx::query("SELECT * FROM sessions WHERE thread_id = ?")
            .bind(thread_id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| session_from_row(&row)).transpose()
    }

    pub async fn session_by_trigger(&self, message_id: MessageId) -> Result<Option<Session>> {
        let row = sqlx::query("SELECT * FROM sessions WHERE trigger_message_id = ?")
            .bind(message_id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| session_from_row(&row)).transpose()
    }

    pub async fn set_session_path(&self, thread_id: ThreadId, path: &Path) -> Result<()> {
        let result = sqlx::query(
            "UPDATE sessions SET omp_session_path = ?, updated_at_ms = ? WHERE thread_id = ?",
        )
        .bind(path.to_string_lossy().into_owned())
        .bind(now_ms()?)
        .bind(thread_id.to_string())
        .execute(&self.pool)
        .await?;
        require_one_row(result.rows_affected(), "session path update")
    }

    pub async fn reset_session(&self, thread_id: ThreadId) -> Result<u64> {
        let mut transaction = self.pool.begin().await?;
        let running: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM turns WHERE thread_id = ? AND status = 'running'",
        )
        .bind(thread_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        if running != 0 {
            bail!("cannot reset a session while a turn is running");
        }
        let now = now_ms()?;
        let discarded = sqlx::query(
            "UPDATE turns SET status = 'aborted', error = 'discarded when a new OMP session was requested', finished_at_ms = ? WHERE thread_id = ? AND status = 'queued'",
        )
        .bind(now)
        .bind(thread_id.to_string())
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let result = sqlx::query(
            "UPDATE sessions SET omp_session_path = NULL, status = 'active', updated_at_ms = ? WHERE thread_id = ?",
        )
        .bind(now)
        .bind(thread_id.to_string())
        .execute(&mut *transaction)
        .await?;
        require_one_row(result.rows_affected(), "session reset")?;
        transaction.commit().await?;
        Ok(discarded)
    }

    pub async fn requeue_claimed_turn(&self, turn_id: i64) -> Result<()> {
        let result = sqlx::query(
            "UPDATE turns SET status = 'queued', started_at_ms = NULL WHERE id = ? AND status = 'running'",
        )
        .bind(turn_id)
        .execute(&self.pool)
        .await?;
        require_one_row(result.rows_affected(), "turn requeue")
    }

    pub async fn set_session_status(
        &self,
        thread_id: ThreadId,
        status: SessionStatus,
    ) -> Result<()> {
        let result =
            sqlx::query("UPDATE sessions SET status = ?, updated_at_ms = ? WHERE thread_id = ?")
                .bind(status.as_str())
                .bind(now_ms()?)
                .bind(thread_id.to_string())
                .execute(&self.pool)
                .await?;
        require_one_row(result.rows_affected(), "session status update")
    }

    pub async fn enqueue_turn(
        &self,
        thread_id: ThreadId,
        message_id: MessageId,
        author_id: UserId,
        content: &str,
    ) -> Result<EnqueueResult> {
        let result = sqlx::query(
            "INSERT INTO turns (thread_id, discord_message_id, author_id, content, status, created_at_ms) VALUES (?, ?, ?, ?, 'queued', ?) ON CONFLICT(discord_message_id) DO NOTHING",
        )
        .bind(thread_id.to_string())
        .bind(message_id.to_string())
        .bind(author_id.to_string())
        .bind(content)
        .bind(now_ms()?)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            Ok(EnqueueResult::Duplicate)
        } else {
            Ok(EnqueueResult::Enqueued(result.last_insert_rowid()))
        }
    }

    pub async fn claim_next(&self, thread_id: ThreadId) -> Result<Option<Turn>> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT * FROM turns WHERE thread_id = ? AND status = 'queued' ORDER BY id LIMIT 1",
        )
        .bind(thread_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            transaction.commit().await?;
            return Ok(None);
        };
        let turn = turn_from_row(&row)?;
        let result = sqlx::query(
            "UPDATE turns SET status = 'running', started_at_ms = ? WHERE id = ? AND status = 'queued'",
        )
        .bind(now_ms()?)
        .bind(turn.id)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            transaction.rollback().await?;
            return Ok(None);
        }
        transaction.commit().await?;
        Ok(Some(Turn {
            status: TurnStatus::Running,
            ..turn
        }))
    }

    pub async fn finish_turn(
        &self,
        turn_id: i64,
        status: TurnStatus,
        response: Option<&str>,
        error: Option<&str>,
    ) -> Result<()> {
        if !matches!(
            status,
            TurnStatus::Done | TurnStatus::Failed | TurnStatus::Aborted | TurnStatus::Uncertain
        ) {
            bail!("turn cannot finish in state {}", status.as_str());
        }
        let result = sqlx::query(
            "UPDATE turns SET status = ?, response = ?, error = ?, finished_at_ms = ? WHERE id = ? AND status = 'running'",
        )
        .bind(status.as_str())
        .bind(response)
        .bind(error)
        .bind(now_ms()?)
        .bind(turn_id)
        .execute(&self.pool)
        .await?;
        require_one_row(result.rows_affected(), "turn completion")
    }

    pub async fn recover_running(&self) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE turns SET status = 'uncertain', error = 'daemon stopped while OMP outcome was unresolved', finished_at_ms = ? WHERE status = 'running'",
        )
        .bind(now_ms()?)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn counts(&self, thread_id: ThreadId) -> Result<TurnCounts> {
        let row = sqlx::query(
            "SELECT COUNT(*) FILTER (WHERE status = 'queued') AS queued, COUNT(*) FILTER (WHERE status = 'running') AS running, COUNT(*) FILTER (WHERE status = 'uncertain') AS uncertain FROM turns WHERE thread_id = ?",
        )
        .bind(thread_id.to_string())
        .fetch_one(&self.pool)
        .await?;
        Ok(TurnCounts {
            queued: row.try_get::<i64, _>("queued")?.try_into()?,
            running: row.try_get::<i64, _>("running")?.try_into()?,
            uncertain: row.try_get::<i64, _>("uncertain")?.try_into()?,
        })
    }

    pub async fn queued_thread_ids(&self) -> Result<Vec<ThreadId>> {
        let rows = sqlx::query(
            "SELECT DISTINCT thread_id FROM turns WHERE status = 'queued' ORDER BY thread_id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| parse_id(row.try_get::<String, _>("thread_id")?, "thread_id"))
            .collect()
    }
}

fn session_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Session> {
    Ok(Session {
        thread_id: parse_id(row.try_get("thread_id")?, "thread_id")?,
        guild_id: parse_id(row.try_get("guild_id")?, "guild_id")?,
        parent_channel_id: parse_id(row.try_get("parent_channel_id")?, "parent_channel_id")?,
        trigger_message_id: parse_id(row.try_get("trigger_message_id")?, "trigger_message_id")?,
        omp_session_path: row
            .try_get::<Option<String>, _>("omp_session_path")?
            .map(Into::into),
        cwd: row.try_get::<String, _>("cwd")?.into(),
        created_by_user_id: parse_id(row.try_get("created_by_user_id")?, "created_by_user_id")?,
        status: SessionStatus::from_database(row.try_get("status")?)?,
    })
}

fn turn_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Turn> {
    Ok(Turn {
        id: row.try_get("id")?,
        thread_id: parse_id(row.try_get("thread_id")?, "thread_id")?,
        discord_message_id: parse_id(row.try_get("discord_message_id")?, "discord_message_id")?,
        author_id: parse_id(row.try_get("author_id")?, "author_id")?,
        content: row.try_get("content")?,
        status: TurnStatus::from_database(row.try_get("status")?)?,
    })
}

fn parse_id<T>(value: String, column: &str) -> Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    value
        .parse()
        .with_context(|| format!("database column {column} has invalid Discord id `{value}`"))
}

fn require_one_row(rows: u64, operation: &str) -> Result<()> {
    if rows == 1 {
        Ok(())
    } else {
        bail!("{operation} affected {rows} rows instead of 1")
    }
}

fn now_ms() -> Result<i64> {
    let millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis();
    millis
        .try_into()
        .context("system time exceeds SQLite range")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ChannelId, GuildId, MessageId, NewSession, ThreadId, UserId};

    async fn test_store() -> (tempfile::TempDir, Store) {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = Store::open(&directory.path().join("bridge.sqlite"))
            .await
            .expect("open store");
        (directory, store)
    }

    fn new_session(thread: u64, trigger: u64) -> NewSession {
        NewSession {
            thread_id: ThreadId::new(thread),
            guild_id: GuildId::new(1),
            parent_channel_id: ChannelId::new(2),
            trigger_message_id: MessageId::new(trigger),
            cwd: std::env::temp_dir(),
            created_by_user_id: UserId::new(3),
        }
    }

    #[tokio::test]
    async fn duplicate_discord_message_is_not_enqueued_twice() {
        let (_directory, store) = test_store().await;
        store
            .create_session(&new_session(10, 20))
            .await
            .expect("create session");

        let first = store
            .enqueue_turn(
                ThreadId::new(10),
                MessageId::new(30),
                UserId::new(3),
                "first",
            )
            .await
            .expect("enqueue first");
        let duplicate = store
            .enqueue_turn(
                ThreadId::new(10),
                MessageId::new(30),
                UserId::new(3),
                "duplicate",
            )
            .await
            .expect("enqueue duplicate");

        assert!(matches!(first, EnqueueResult::Enqueued(_)));
        assert_eq!(duplicate, EnqueueResult::Duplicate);
        assert_eq!(
            store
                .counts(ThreadId::new(10))
                .await
                .expect("counts")
                .queued,
            1
        );
    }

    #[tokio::test]
    async fn restart_marks_running_turn_uncertain_without_requeueing() {
        let (_directory, store) = test_store().await;
        store
            .create_session(&new_session(10, 20))
            .await
            .expect("create session");
        store
            .enqueue_turn(ThreadId::new(10), MessageId::new(30), UserId::new(3), "run")
            .await
            .expect("enqueue");
        let claimed = store
            .claim_next(ThreadId::new(10))
            .await
            .expect("claim")
            .expect("queued turn");
        assert_eq!(claimed.status, TurnStatus::Running);

        assert_eq!(store.recover_running().await.expect("recover"), 1);
        let counts = store.counts(ThreadId::new(10)).await.expect("counts");
        assert_eq!(counts.queued, 0);
        assert_eq!(counts.running, 0);
        assert_eq!(counts.uncertain, 1);
    }

    #[tokio::test]
    async fn session_mapping_and_omp_path_survive_reopen() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("bridge.sqlite");
        let session_path = directory.path().join("omp-session.jsonl");
        let store = Store::open(&database).await.expect("open store");
        store
            .create_session(&new_session(10, 20))
            .await
            .expect("create session");
        store
            .set_session_path(ThreadId::new(10), &session_path)
            .await
            .expect("set OMP path");
        store.pool.close().await;

        let reopened = Store::open(&database).await.expect("reopen store");
        let session = reopened
            .session(ThreadId::new(10))
            .await
            .expect("read session")
            .expect("persisted session");
        assert_eq!(
            session.omp_session_path.as_deref(),
            Some(session_path.as_path())
        );
        assert_eq!(session.cwd, std::env::temp_dir());
    }
}

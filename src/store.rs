use std::{path::Path, str::FromStr, time::SystemTime};

use anyhow::{Context, Result, bail};
use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};

use crate::domain::{
    MessageId, NewSession, Session, SessionStatus, ThreadId, Turn, TurnCounts, TurnStatus, UserId,
};

const MAX_PENDING_TURNS_PER_THREAD: i64 = 64;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnOutcome {
    pub id: i64,
    pub status: TurnStatus,
    pub response: Option<String>,
    pub error: Option<String>,
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
        let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::raw_sql(SCHEMA).execute(&mut *transaction).await?;
        let interruption_column: Option<String> = sqlx::query_scalar(
            "SELECT name FROM pragma_table_info('turns') WHERE name = 'interrupted_by_turn_id'",
        )
        .fetch_optional(&mut *transaction)
        .await?;
        if interruption_column.is_none() {
            sqlx::query(
                "ALTER TABLE turns ADD COLUMN interrupted_by_turn_id INTEGER REFERENCES turns(id)",
            )
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
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
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let duplicate: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM turns WHERE discord_message_id = ?)")
                .bind(message_id.to_string())
                .fetch_one(&mut *transaction)
                .await?;
        if duplicate {
            transaction.commit().await?;
            return Ok(EnqueueResult::Duplicate);
        }
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM sessions WHERE thread_id = ?")
                .bind(thread_id.to_string())
                .fetch_optional(&mut *transaction)
                .await?;
        match status.as_deref() {
            Some("active") => {}
            Some(_) => bail!("cannot enqueue a turn in a stopped session"),
            None => bail!("cannot enqueue a turn without a session"),
        }
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM turns WHERE thread_id = ? AND status IN ('queued', 'running')",
        )
        .bind(thread_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        if pending >= MAX_PENDING_TURNS_PER_THREAD {
            bail!("session pending turn limit ({MAX_PENDING_TURNS_PER_THREAD}) reached");
        }
        let result = sqlx::query(
            "INSERT INTO turns (thread_id, discord_message_id, author_id, content, status, created_at_ms) VALUES (?, ?, ?, ?, 'queued', ?) ON CONFLICT(discord_message_id) DO NOTHING",
        )
        .bind(thread_id.to_string())
        .bind(message_id.to_string())
        .bind(author_id.to_string())
        .bind(content)
        .bind(now_ms()?)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        if result.rows_affected() == 0 {
            Ok(EnqueueResult::Duplicate)
        } else {
            Ok(EnqueueResult::Enqueued(result.last_insert_rowid()))
        }
    }

    pub async fn claim_next(&self, thread_id: ThreadId) -> Result<Option<Turn>> {
        self.claim_next_pending(thread_id, false).await
    }

    pub async fn claim_next_interrupting(&self, thread_id: ThreadId) -> Result<Option<Turn>> {
        self.claim_next_pending(thread_id, true).await
    }

    async fn claim_next_pending(
        &self,
        thread_id: ThreadId,
        interrupting: bool,
    ) -> Result<Option<Turn>> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let row = sqlx::query(
            "SELECT * FROM turns WHERE thread_id = ? AND status = 'queued' AND EXISTS (SELECT 1 FROM sessions WHERE sessions.thread_id = turns.thread_id AND sessions.status = 'active') ORDER BY id LIMIT 1",
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
        if interrupting {
            sqlx::query(
                "UPDATE turns SET interrupted_by_turn_id = ? WHERE thread_id = ? AND status = 'running' AND id < ? AND interrupted_by_turn_id IS NULL",
            )
            .bind(turn.id)
            .bind(thread_id.to_string())
            .bind(turn.id)
            .execute(&mut *transaction)
            .await?;
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

    pub async fn finish_running_uncertain(&self, thread_id: ThreadId, error: &str) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE turns SET status = 'uncertain', error = ?, finished_at_ms = ? WHERE thread_id = ? AND status = 'running'",
        )
        .bind(error)
        .bind(now_ms()?)
        .bind(thread_id.to_string())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn next_outcome(
        &self,
        thread_id: ThreadId,
        after_id: i64,
    ) -> Result<Option<TurnOutcome>> {
        let row = sqlx::query(
            "SELECT id, status, response, error FROM turns WHERE thread_id = ? AND id > ? AND status IN ('done', 'failed', 'aborted', 'uncertain') ORDER BY id LIMIT 1",
        )
        .bind(thread_id.to_string())
        .bind(after_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            Ok(TurnOutcome {
                id: row.try_get("id")?,
                status: TurnStatus::from_database(row.try_get("status")?)?,
                response: row.try_get("response")?,
                error: row.try_get("error")?,
            })
        })
        .transpose()
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

    async fn enqueue(store: &Store, thread: u64, message: u64) -> i64 {
        match store
            .enqueue_turn(
                ThreadId::new(thread),
                MessageId::new(message),
                UserId::new(3),
                "prompt",
            )
            .await
            .expect("enqueue")
        {
            EnqueueResult::Enqueued(id) => id,
            EnqueueResult::Duplicate => panic!("expected a new turn"),
        }
    }

    async fn interruption_link(store: &Store, turn_id: i64) -> Option<i64> {
        sqlx::query_scalar("SELECT interrupted_by_turn_id FROM turns WHERE id = ?")
            .bind(turn_id)
            .fetch_one(&store.pool)
            .await
            .expect("read interruption link")
    }

    #[tokio::test]
    async fn rapid_interruptions_preserve_each_predecessor_and_partial_result() {
        let (_directory, store) = test_store().await;
        let thread = ThreadId::new(10);
        store
            .create_session(&new_session(10, 20))
            .await
            .expect("create session");
        let first = enqueue(&store, 10, 30).await;
        let second = enqueue(&store, 10, 31).await;
        let third = enqueue(&store, 10, 32).await;
        assert_eq!(
            store.claim_next(thread).await.expect("claim").unwrap().id,
            first
        );
        for expected in [second, third] {
            let claimed = store
                .claim_next_interrupting(thread)
                .await
                .expect("interrupt")
                .expect("queued turn");
            assert_eq!(claimed.id, expected);
            assert_eq!(claimed.status, TurnStatus::Running);
        }
        assert_eq!(interruption_link(&store, first).await, Some(second));
        assert_eq!(interruption_link(&store, second).await, Some(third));
        assert_eq!(interruption_link(&store, third).await, None);
        assert!(
            store
                .claim_next_interrupting(thread)
                .await
                .expect("empty queue")
                .is_none()
        );
        store
            .finish_turn(first, TurnStatus::Aborted, Some("partial"), None)
            .await
            .expect("finish predecessor");
        assert!(
            store
                .finish_turn(first, TurnStatus::Done, Some("overwrite"), None)
                .await
                .is_err()
        );
        let outcome = store
            .next_outcome(thread, 0)
            .await
            .expect("outcome")
            .expect("aborted predecessor");
        assert_eq!(outcome.id, first);
        assert_eq!(outcome.status, TurnStatus::Aborted);
        assert_eq!(outcome.response.as_deref(), Some("partial"));
        assert_eq!(interruption_link(&store, first).await, Some(second));
        assert_eq!(interruption_link(&store, second).await, Some(third));
    }

    #[tokio::test]
    async fn restart_preserves_interrupt_intent_and_untouched_pending_turns() {
        let (directory, store) = test_store().await;
        let thread = ThreadId::new(10);
        store
            .create_session(&new_session(10, 20))
            .await
            .expect("create session");
        let first = enqueue(&store, 10, 30).await;
        let second = enqueue(&store, 10, 31).await;
        let pending = enqueue(&store, 10, 32).await;
        store.claim_next(thread).await.expect("claim first");
        store
            .claim_next_interrupting(thread)
            .await
            .expect("claim interruption");
        store.pool.close().await;

        let reopened = Store::open(&directory.path().join("bridge.sqlite"))
            .await
            .expect("reopen");
        assert_eq!(reopened.recover_running().await.expect("recover"), 2);
        assert_eq!(reopened.recover_running().await.expect("recover again"), 0);
        let counts = reopened.counts(thread).await.expect("counts");
        assert_eq!((counts.queued, counts.running, counts.uncertain), (1, 0, 2));
        assert_eq!(interruption_link(&reopened, first).await, Some(second));
        assert_eq!(interruption_link(&reopened, second).await, None);
        for (after_id, expected) in [(0, first), (first, second)] {
            let outcome = reopened
                .next_outcome(thread, after_id)
                .await
                .expect("read outcome")
                .expect("uncertain dispatched turn");
            assert_eq!(outcome.id, expected);
            assert_eq!(outcome.status, TurnStatus::Uncertain);
        }
        assert_eq!(
            reopened
                .claim_next(thread)
                .await
                .expect("claim untouched prompt")
                .expect("pending turn")
                .id,
            pending
        );
        assert!(
            reopened
                .claim_next(thread)
                .await
                .expect("no replay")
                .is_none()
        );
    }

    #[tokio::test]
    async fn pending_limit_counts_running_but_duplicates_still_succeed() {
        let (_directory, store) = test_store().await;
        let thread = ThreadId::new(10);
        store
            .create_session(&new_session(10, 20))
            .await
            .expect("create session");
        for message in 100..164 {
            enqueue(&store, 10, message).await;
        }
        let running = store
            .claim_next(thread)
            .await
            .expect("claim")
            .expect("running turn");
        let overflow = store
            .enqueue_turn(thread, MessageId::new(164), UserId::new(3), "overflow")
            .await
            .expect_err("64 pending turns must reject admission");
        assert!(overflow.to_string().contains("limit (64)"));
        for message in [100, 163] {
            assert_eq!(
                store
                    .enqueue_turn(thread, MessageId::new(message), UserId::new(3), "duplicate")
                    .await
                    .expect("duplicates bypass limit"),
                EnqueueResult::Duplicate
            );
        }
        let counts = store.counts(thread).await.expect("counts");
        assert_eq!((counts.queued, counts.running), (63, 1));
        store
            .create_session(&new_session(11, 21))
            .await
            .expect("independent session");
        enqueue(&store, 11, 200).await;
        store
            .finish_turn(running.id, TurnStatus::Done, Some("done"), None)
            .await
            .expect("free capacity");
        enqueue(&store, 10, 164).await;
        assert_eq!(store.counts(thread).await.expect("counts").queued, 64);
    }

    #[tokio::test]
    async fn stopped_sessions_reject_new_admissions_and_both_claim_paths() {
        let (_directory, store) = test_store().await;
        let thread = ThreadId::new(10);
        store
            .create_session(&new_session(10, 20))
            .await
            .expect("create session");
        let first = enqueue(&store, 10, 30).await;
        let pending = enqueue(&store, 10, 31).await;
        store.claim_next(thread).await.expect("claim");
        store
            .set_session_status(thread, SessionStatus::Stopped)
            .await
            .expect("stop");
        assert!(
            store
                .claim_next(thread)
                .await
                .expect("claim stopped")
                .is_none()
        );
        assert!(
            store
                .claim_next_interrupting(thread)
                .await
                .expect("interrupt stopped")
                .is_none()
        );
        assert_eq!(interruption_link(&store, first).await, None);
        assert_eq!(
            store
                .enqueue_turn(thread, MessageId::new(31), UserId::new(3), "duplicate")
                .await
                .expect("duplicate after stop"),
            EnqueueResult::Duplicate
        );
        let error = store
            .enqueue_turn(thread, MessageId::new(32), UserId::new(3), "stale handler")
            .await
            .expect_err("new admission after stop");
        assert!(error.to_string().contains("stopped"));
        let counts = store.counts(thread).await.expect("counts");
        assert_eq!((counts.queued, counts.running), (1, 1));
        store
            .set_session_status(thread, SessionStatus::Active)
            .await
            .expect("resume");
        assert_eq!(
            store
                .claim_next_interrupting(thread)
                .await
                .expect("claim pending")
                .expect("untouched queued turn")
                .id,
            pending
        );
        enqueue(&store, 10, 32).await;
    }

    #[tokio::test]
    async fn failure_marks_only_this_threads_remaining_running_turns_uncertain() {
        let (_directory, store) = test_store().await;
        let thread = ThreadId::new(10);
        for (thread_id, trigger) in [(10, 20), (11, 21)] {
            store
                .create_session(&new_session(thread_id, trigger))
                .await
                .expect("create session");
        }
        let finished = enqueue(&store, 10, 30).await;
        let interrupted = enqueue(&store, 10, 31).await;
        let active = enqueue(&store, 10, 32).await;
        let pending = enqueue(&store, 10, 33).await;
        enqueue(&store, 11, 40).await;
        store.claim_next(thread).await.expect("claim");
        store
            .finish_turn(finished, TurnStatus::Done, Some("retained"), None)
            .await
            .expect("finish");
        store.claim_next(thread).await.expect("claim");
        store
            .claim_next_interrupting(thread)
            .await
            .expect("interrupt");
        store
            .claim_next(ThreadId::new(11))
            .await
            .expect("other claim");
        assert_eq!(
            store
                .finish_running_uncertain(thread, "connection lost")
                .await
                .expect("record failure"),
            2
        );
        assert_eq!(
            store
                .finish_running_uncertain(thread, "later error")
                .await
                .expect("nothing remains running"),
            0
        );
        assert_eq!(interruption_link(&store, interrupted).await, Some(active));
        let done = store
            .next_outcome(thread, 0)
            .await
            .expect("outcome")
            .expect("finished outcome");
        assert_eq!(done.status, TurnStatus::Done);
        assert_eq!(done.response.as_deref(), Some("retained"));
        for (after_id, id) in [(finished, interrupted), (interrupted, active)] {
            let outcome = store
                .next_outcome(thread, after_id)
                .await
                .expect("outcome")
                .expect("uncertain outcome");
            assert_eq!(outcome.id, id);
            assert_eq!(outcome.status, TurnStatus::Uncertain);
            assert_eq!(outcome.error.as_deref(), Some("connection lost"));
        }
        assert!(
            store
                .next_outcome(thread, active)
                .await
                .expect("queued has no outcome")
                .is_none()
        );
        assert_eq!(
            store.claim_next(thread).await.expect("claim").unwrap().id,
            pending
        );
        assert_eq!(
            store
                .counts(ThreadId::new(11))
                .await
                .expect("other counts")
                .running,
            1
        );
    }

    #[tokio::test]
    async fn terminal_outcomes_are_ordered_by_acceptance_not_completion() {
        let (_directory, store) = test_store().await;
        let thread = ThreadId::new(10);
        store
            .create_session(&new_session(10, 20))
            .await
            .expect("create session");
        let mut ids = Vec::new();
        for message in 30..34 {
            ids.push(enqueue(&store, 10, message).await);
            store.claim_next_interrupting(thread).await.expect("claim");
        }
        enqueue(&store, 10, 34).await;
        let statuses = [
            TurnStatus::Aborted,
            TurnStatus::Failed,
            TurnStatus::Uncertain,
            TurnStatus::Done,
        ];
        for (&id, &status) in ids.iter().zip(&statuses).rev() {
            store
                .finish_turn(id, status, Some("response"), Some("detail"))
                .await
                .expect("finish in reverse order");
        }
        let mut cursor = 0;
        for (&id, &status) in ids.iter().zip(&statuses) {
            let outcome = store
                .next_outcome(thread, cursor)
                .await
                .expect("read outcome")
                .expect("terminal outcome");
            assert_eq!(
                outcome,
                TurnOutcome {
                    id,
                    status,
                    response: Some("response".into()),
                    error: Some("detail".into()),
                }
            );
            cursor = outcome.id;
        }
        assert!(
            store
                .next_outcome(thread, cursor)
                .await
                .expect("end")
                .is_none()
        );
        assert!(
            store
                .next_outcome(ThreadId::new(11), 0)
                .await
                .expect("other thread")
                .is_none()
        );
    }

    #[tokio::test]
    async fn restart_preserves_terminal_results_and_never_requeues_them_for_delivery() {
        let (directory, store) = test_store().await;
        let thread = ThreadId::new(10);
        store.create_session(&new_session(10, 20)).await.unwrap();
        let statuses = [
            TurnStatus::Aborted,
            TurnStatus::Failed,
            TurnStatus::Uncertain,
            TurnStatus::Done,
        ];
        let mut ids = Vec::new();
        for (index, status) in statuses.into_iter().enumerate() {
            let id = enqueue(&store, 10, 30 + u64::try_from(index).unwrap()).await;
            store.claim_next(thread).await.unwrap().unwrap();
            store
                .finish_turn(
                    id,
                    status,
                    Some("durable answer or partial"),
                    Some("detail"),
                )
                .await
                .unwrap();
            ids.push(id);
        }
        store.pool.close().await;
        let reopened = Store::open(&directory.path().join("bridge.sqlite"))
            .await
            .unwrap();
        assert_eq!(reopened.recover_running().await.unwrap(), 0);
        assert!(reopened.queued_thread_ids().await.unwrap().is_empty());
        assert!(reopened.claim_next(thread).await.unwrap().is_none());
        let mut cursor = 0;
        for (id, status) in ids.into_iter().zip(statuses) {
            let outcome = reopened
                .next_outcome(thread, cursor)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(outcome.id, id);
            assert_eq!(outcome.status, status);
            assert_eq!(
                outcome.response.as_deref(),
                Some("durable answer or partial")
            );
            assert_eq!(outcome.error.as_deref(), Some("detail"));
            assert!(
                reopened
                    .finish_turn(id, TurnStatus::Uncertain, None, Some("late error"))
                    .await
                    .is_err()
            );
            cursor = id;
        }
    }

    #[tokio::test]
    async fn prior_schema_migrates_without_losing_existing_turns_or_session() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("prior.sqlite");
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&database)
            .create_if_missing(true)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .expect("open prior database");
        sqlx::raw_sql(SCHEMA)
            .execute(&pool)
            .await
            .expect("create original schema");
        let original = Store { pool };
        original
            .create_session(&new_session(10, 20))
            .await
            .expect("create original session");
        let first = enqueue(&original, 10, 30).await;
        let pending = enqueue(&original, 10, 31).await;
        original
            .claim_next(ThreadId::new(10))
            .await
            .expect("claim original turn");
        original.pool.close().await;

        let migrated = Store::open(&database).await.expect("migrate");
        assert_eq!(interruption_link(&migrated, first).await, None);
        assert_eq!(
            migrated
                .session_by_trigger(MessageId::new(20))
                .await
                .expect("session")
                .expect("original mapping")
                .thread_id,
            ThreadId::new(10)
        );
        let claimed = migrated
            .claim_next_interrupting(ThreadId::new(10))
            .await
            .expect("interrupt original turn")
            .expect("original pending turn");
        assert_eq!(claimed.id, pending);
        assert_eq!(claimed.content, "prompt");
        assert_eq!(interruption_link(&migrated, first).await, Some(pending));
        migrated.pool.close().await;
        let reopened = Store::open(&database)
            .await
            .expect("migration is idempotent");
        assert_eq!(interruption_link(&reopened, first).await, Some(pending));
    }
}

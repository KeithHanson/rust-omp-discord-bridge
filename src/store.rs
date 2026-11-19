use std::{
    collections::HashMap,
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use anyhow::{Context, Result, bail};
use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};
use tokio::sync::RwLock;

use crate::domain::{
    ActorId, ClientConversationId, ClientKind, ConversationId, EventId, MessageId, NewSession,
    Session, SessionStatus, ThreadId, Turn, TurnCounts, TurnStatus, UserId,
};

const MAX_PENDING_TURNS_PER_THREAD: i64 = 64;
pub const MAX_BOT_EXCHANGES: usize = 8;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS client_conversations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_kind TEXT NOT NULL CHECK (client_kind IN ('discord', 'matrix')),
    native_id TEXT NOT NULL,
    UNIQUE(client_kind, native_id)
);

CREATE TABLE IF NOT EXISTS client_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_kind TEXT NOT NULL CHECK (client_kind IN ('discord', 'matrix')),
    native_id TEXT NOT NULL,
    UNIQUE(client_kind, native_id)
);

CREATE TABLE IF NOT EXISTS client_actors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_kind TEXT NOT NULL CHECK (client_kind IN ('discord', 'matrix')),
    native_id TEXT NOT NULL,
    UNIQUE(client_kind, native_id)
);

CREATE TABLE IF NOT EXISTS excluded_threads (
    thread_id TEXT PRIMARY KEY NOT NULL,
    excluded_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    thread_id TEXT PRIMARY KEY NOT NULL,
    conversation_id INTEGER REFERENCES client_conversations(id),
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
    conversation_id INTEGER REFERENCES client_conversations(id),
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
    thread_access: Arc<Mutex<HashMap<ThreadId, Arc<RwLock<()>>>>>,
}

#[derive(Debug, Clone)]
pub enum CreateSessionResult {
    Created(Session),
    Existing(Session),
}

/// The caller must authorize the author before classifying an admitted turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnSource {
    Human,
    Bot,
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

#[derive(Debug)]
pub(crate) struct TitleContextTurn {
    pub status: TurnStatus,
    pub prompt: String,
    pub response: Option<String>,
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
        for (column, migration) in [
            (
                "bot_exchange_count",
                "ALTER TABLE sessions ADD COLUMN bot_exchange_count INTEGER NOT NULL DEFAULT 0 CHECK (bot_exchange_count BETWEEN 0 AND 8)",
            ),
            (
                "latest_human_message_id",
                "ALTER TABLE sessions ADD COLUMN latest_human_message_id TEXT",
            ),
            ("model", "ALTER TABLE sessions ADD COLUMN model TEXT"),
            (
                "conversation_id",
                "ALTER TABLE sessions ADD COLUMN conversation_id INTEGER REFERENCES client_conversations(id)",
            ),
        ] {
            let existing: Option<String> =
                sqlx::query_scalar("SELECT name FROM pragma_table_info('sessions') WHERE name = ?")
                    .bind(column)
                    .fetch_optional(&mut *transaction)
                    .await?;
            if existing.is_none() {
                sqlx::query(migration).execute(&mut *transaction).await?;
            }
        }
        let turn_conversation_column: Option<String> = sqlx::query_scalar(
            "SELECT name FROM pragma_table_info('turns') WHERE name = 'conversation_id'",
        )
        .fetch_optional(&mut *transaction)
        .await?;
        if turn_conversation_column.is_none() {
            sqlx::query(
                "ALTER TABLE turns ADD COLUMN conversation_id INTEGER REFERENCES client_conversations(id)",
            )
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query(
            "INSERT INTO client_conversations (client_kind, native_id)
             SELECT 'discord', thread_id FROM sessions WHERE true
             ON CONFLICT(client_kind, native_id) DO NOTHING",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE sessions
             SET conversation_id = (
                 SELECT id FROM client_conversations
                 WHERE client_kind = 'discord' AND native_id = sessions.thread_id
             )
             WHERE conversation_id IS NULL",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE turns
             SET conversation_id = (
                 SELECT sessions.conversation_id FROM sessions
                 WHERE sessions.thread_id = turns.thread_id
             )
             WHERE conversation_id IS NULL",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::raw_sql(
            "CREATE INDEX IF NOT EXISTS sessions_conversation_id_idx ON sessions(conversation_id);
             CREATE INDEX IF NOT EXISTS turns_conversation_status_id_idx ON turns(conversation_id, status, id);",
        )
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(Self {
            pool,
            thread_access: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Serialize thread admission and outbound effects with exclusion across Store clones.
    /// Callers take a read guard for admission/output, or a write guard before exclusion.
    pub fn thread_access(&self, thread_id: ThreadId) -> Arc<RwLock<()>> {
        self.thread_access
            .lock()
            .expect("thread access registry poisoned")
            .entry(thread_id)
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    pub async fn conversation_id(
        &self,
        key: &ClientConversationId,
    ) -> Result<Option<ConversationId>> {
        let id: Option<i64> = sqlx::query_scalar(
            "SELECT id FROM client_conversations WHERE client_kind = ? AND native_id = ?",
        )
        .bind(key.client().as_str())
        .bind(key.native())
        .fetch_optional(&self.pool)
        .await?;
        id.map(ConversationId::new).transpose().map_err(Into::into)
    }

    pub async fn ensure_conversation(
        &self,
        key: &ClientConversationId,
    ) -> Result<ConversationId> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let id = Self::ensure_conversation_in(&mut transaction, key).await?;
        transaction.commit().await?;
        Ok(id)
    }

    async fn ensure_conversation_in(
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        key: &ClientConversationId,
    ) -> Result<ConversationId> {
        sqlx::query(
            "INSERT INTO client_conversations (client_kind, native_id) VALUES (?, ?)
             ON CONFLICT(client_kind, native_id) DO NOTHING",
        )
        .bind(key.client().as_str())
        .bind(key.native())
        .execute(&mut **transaction)
        .await?;
        let id: i64 = sqlx::query_scalar(
            "SELECT id FROM client_conversations WHERE client_kind = ? AND native_id = ?",
        )
        .bind(key.client().as_str())
        .bind(key.native())
        .fetch_one(&mut **transaction)
        .await?;
        Ok(ConversationId::new(id)?)
    }

    pub async fn is_thread_excluded(&self, thread_id: ThreadId) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM excluded_threads WHERE thread_id = ?)",
        )
        .bind(thread_id.to_string())
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn ensure_event(&self, key: &EventId) -> Result<MessageId> {
        sqlx::query(
            "INSERT INTO client_events (client_kind, native_id) VALUES (?, ?)
             ON CONFLICT(client_kind, native_id) DO NOTHING",
        )
        .bind(key.client().as_str())
        .bind(key.native())
        .execute(&self.pool)
        .await?;
        let id: i64 = sqlx::query_scalar(
            "SELECT id FROM client_events WHERE client_kind = ? AND native_id = ?",
        )
        .bind(key.client().as_str())
        .bind(key.native())
        .fetch_one(&self.pool)
        .await?;
        Ok(MessageId::new(u64::try_from(id)?))
    }

    pub async fn ensure_actor(&self, key: &ActorId) -> Result<UserId> {
        sqlx::query(
            "INSERT INTO client_actors (client_kind, native_id) VALUES (?, ?)
             ON CONFLICT(client_kind, native_id) DO NOTHING",
        )
        .bind(key.client().as_str())
        .bind(key.native())
        .execute(&self.pool)
        .await?;
        let id: i64 = sqlx::query_scalar(
            "SELECT id FROM client_actors WHERE client_kind = ? AND native_id = ?",
        )
        .bind(key.client().as_str())
        .bind(key.native())
        .fetch_one(&self.pool)
        .await?;
        Ok(UserId::new(u64::try_from(id)?))
    }

    /// Persist exclusion even for an unmanaged thread; preserve history and running turns.
    /// The caller holds the thread's write guard and cancels running work after committing.
    pub async fn exclude_thread(&self, thread_id: ThreadId) -> Result<()> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let now = now_ms()?;
        sqlx::query(
            "INSERT INTO excluded_threads (thread_id, excluded_at_ms) VALUES (?, ?) ON CONFLICT(thread_id) DO NOTHING",
        )
        .bind(thread_id.to_string())
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE sessions SET status = 'stopped', updated_at_ms = ? WHERE thread_id = ?",
        )
        .bind(now)
        .bind(thread_id.to_string())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE turns SET status = 'aborted', error = 'bot was kicked from this thread', finished_at_ms = ? WHERE thread_id = ? AND status = 'queued'",
        )
        .bind(now)
        .bind(thread_id.to_string())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn create_session(&self, input: &NewSession) -> Result<CreateSessionResult> {
        let key =
            ClientConversationId::discord(input.thread_id.get()).expect("valid Discord thread id");
        let conversation_id = self.ensure_conversation(&key).await?;
        self.create_session_for_conversation(conversation_id, input).await
    }

    pub async fn create_session_for_conversation(
        &self,
        conversation_id: ConversationId,
        input: &NewSession,
    ) -> Result<CreateSessionResult> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        require_thread_admitted(&mut transaction, input.thread_id).await?;
        let now = now_ms()?;
        let result = sqlx::query(
            "INSERT INTO sessions (thread_id, conversation_id, guild_id, parent_channel_id, trigger_message_id, cwd, created_by_user_id, status, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, ?) ON CONFLICT DO NOTHING",
        )
        .bind(input.thread_id.to_string())
        .bind(conversation_id.get())
        .bind(input.guild_id.to_string())
        .bind(input.parent_channel_id.to_string())
        .bind(input.trigger_message_id.to_string())
        .bind(input.cwd.to_string_lossy().into_owned())
        .bind(input.created_by_user_id.to_string())
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        let row = sqlx::query(
            "SELECT * FROM sessions WHERE trigger_message_id = ? OR thread_id = ? ORDER BY (trigger_message_id = ?) DESC LIMIT 1",
        )
        .bind(input.trigger_message_id.to_string())
        .bind(input.thread_id.to_string())
        .bind(input.trigger_message_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .context("inserted or conflicting session was not found")?;
        let session = session_from_row(&row)?;
        if session.thread_id != input.thread_id {
            require_thread_admitted(&mut transaction, session.thread_id).await?;
        }
        transaction.commit().await?;
        if result.rows_affected() == 1 {
            Ok(CreateSessionResult::Created(session))
        } else {
            Ok(CreateSessionResult::Existing(session))
        }
    }

    pub async fn session(&self, thread_id: ThreadId) -> Result<Option<Session>> {
        let row = sqlx::query("SELECT * FROM sessions WHERE thread_id = ?")
            .bind(thread_id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| session_from_row(&row)).transpose()
    }

    pub async fn session_by_conversation(
        &self,
        conversation_id: ConversationId,
    ) -> Result<Option<Session>> {
        let row = sqlx::query("SELECT * FROM sessions WHERE conversation_id = ?")
            .bind(conversation_id.get())
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

    pub async fn set_session_model(&self, thread_id: ThreadId, model: Option<&str>) -> Result<()> {
        // Admission may race the runner's idle check. Check pending work and
        // update the override in one write so no queued turn changes models.
        let result = sqlx::query(
            "UPDATE sessions SET model = ?, updated_at_ms = ? WHERE thread_id = ? AND NOT EXISTS (SELECT 1 FROM turns WHERE turns.thread_id = sessions.thread_id AND status IN ('queued', 'running'))",
        )
        .bind(model)
        .bind(now_ms()?)
        .bind(thread_id.to_string())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            bail!("model changes require a mapped session with no queued or running turns");
        }
        Ok(())
    }

    pub async fn reset_session(&self, thread_id: ThreadId) -> Result<u64> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        require_thread_admitted(&mut transaction, thread_id).await?;
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
        let result = sqlx::query(
            "UPDATE sessions SET status = ?, updated_at_ms = ? WHERE thread_id = ? AND (? = 'stopped' OR NOT EXISTS (SELECT 1 FROM excluded_threads WHERE excluded_threads.thread_id = sessions.thread_id))",
        )
        .bind(status.as_str())
        .bind(now_ms()?)
        .bind(thread_id.to_string())
        .bind(status.as_str())
        .execute(&self.pool)
        .await?;
        require_one_row(result.rows_affected(), "session status update")
    }

    /// Record an authorized human control or mention that did not enqueue a turn.
    pub async fn note_human_message(
        &self,
        thread_id: ThreadId,
        message_id: MessageId,
    ) -> Result<()> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        require_thread_admitted(&mut transaction, thread_id).await?;
        let row = sqlx::query("SELECT latest_human_message_id FROM sessions WHERE thread_id = ?")
            .bind(thread_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?
            .context("cannot record a human message without a session")?;
        if is_new_human_message(&row, message_id)? {
            sqlx::query(
                "UPDATE sessions SET bot_exchange_count = 0, latest_human_message_id = ?, updated_at_ms = ? WHERE thread_id = ?",
            )
            .bind(message_id.to_string())
            .bind(now_ms()?)
            .bind(thread_id.to_string())
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn enqueue_turn(
        &self,
        thread_id: ThreadId,
        message_id: MessageId,
        author_id: UserId,
        content: &str,
        source: TurnSource,
    ) -> Result<EnqueueResult> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        require_thread_admitted(&mut transaction, thread_id).await?;
        let duplicate: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM turns WHERE discord_message_id = ?)")
                .bind(message_id.to_string())
                .fetch_one(&mut *transaction)
                .await?;
        if duplicate {
            transaction.commit().await?;
            return Ok(EnqueueResult::Duplicate);
        }
        let session = sqlx::query(
            "SELECT status, conversation_id, bot_exchange_count, latest_human_message_id FROM sessions WHERE thread_id = ?",
        )
        .bind(thread_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .context("cannot enqueue a turn without a session")?;
        if session.try_get::<&str, _>("status")? != "active" {
            bail!("cannot enqueue a turn in a stopped session");
        }
        let conversation_id: i64 = session
            .try_get::<Option<i64>, _>("conversation_id")?
            .context("session is missing its client conversation mapping")?;
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM turns WHERE thread_id = ? AND status IN ('queued', 'running')",
        )
        .bind(thread_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        if pending >= MAX_PENDING_TURNS_PER_THREAD {
            bail!("session pending turn limit ({MAX_PENDING_TURNS_PER_THREAD}) reached");
        }
        let bot_exchanges: i64 = session.try_get("bot_exchange_count")?;
        if source == TurnSource::Bot && bot_exchanges >= MAX_BOT_EXCHANGES as i64 {
            bail!(
                "session bot exchange limit ({MAX_BOT_EXCHANGES}) reached; a new authorized human message is needed"
            );
        }
        let resets_budget =
            source == TurnSource::Human && is_new_human_message(&session, message_id)?;
        let result = sqlx::query(
            "INSERT INTO turns (thread_id, conversation_id, discord_message_id, author_id, content, status, created_at_ms) VALUES (?, ?, ?, ?, ?, 'queued', ?) ON CONFLICT(discord_message_id) DO NOTHING",
        )
        .bind(thread_id.to_string())
        .bind(conversation_id)
        .bind(message_id.to_string())
        .bind(author_id.to_string())
        .bind(content)
        .bind(now_ms()?)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            transaction.commit().await?;
            return Ok(EnqueueResult::Duplicate);
        }
        match source {
            TurnSource::Bot => {
                // Admission order defines the budget: delayed bots also consume the current
                // allowance, never a previous human-message window.
                sqlx::query(
                    "UPDATE sessions SET bot_exchange_count = bot_exchange_count + 1 WHERE thread_id = ?",
                )
                .bind(thread_id.to_string())
                .execute(&mut *transaction)
                .await?;
            }
            TurnSource::Human if resets_budget => {
                sqlx::query(
                    "UPDATE sessions SET bot_exchange_count = 0, latest_human_message_id = ?, updated_at_ms = ? WHERE thread_id = ?",
                )
                .bind(message_id.to_string())
                .bind(now_ms()?)
                .bind(thread_id.to_string())
                .execute(&mut *transaction)
                .await?;
            }
            TurnSource::Human => {}
        }
        transaction.commit().await?;
        Ok(EnqueueResult::Enqueued(result.last_insert_rowid()))
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
            "SELECT * FROM turns WHERE thread_id = ? AND status = 'queued' AND EXISTS (SELECT 1 FROM sessions WHERE sessions.thread_id = turns.thread_id AND sessions.status = 'active') AND NOT EXISTS (SELECT 1 FROM excluded_threads WHERE excluded_threads.thread_id = turns.thread_id) ORDER BY id LIMIT 1",
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

    /// Select bounded visible text in SQL, never materializing full historical turns.
    pub(crate) async fn title_context(&self, thread_id: ThreadId) -> Result<Vec<TitleContextTurn>> {
        let rows = sqlx::query(
            "SELECT status, substr(content, 1, 2048) AS prompt, substr(response, 1, 2048) AS response FROM turns WHERE thread_id = ? ORDER BY id DESC LIMIT 8",
        )
        .bind(thread_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .rev()
            .map(|row| {
                Ok(TitleContextTurn {
                    status: TurnStatus::from_database(row.try_get("status")?)?,
                    prompt: row.try_get("prompt")?,
                    response: row.try_get("response")?,
                })
            })
            .collect()
    }

    pub async fn queued_thread_ids(&self) -> Result<Vec<ThreadId>> {
        let rows = sqlx::query(
            "SELECT DISTINCT thread_id FROM turns WHERE status = 'queued' AND NOT EXISTS (SELECT 1 FROM excluded_threads WHERE excluded_threads.thread_id = turns.thread_id) ORDER BY thread_id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| parse_id(row.try_get::<String, _>("thread_id")?, "thread_id"))
            .collect()
    }

    pub async fn queued_thread_ids_for(&self, client: ClientKind) -> Result<Vec<ThreadId>> {
        let rows = sqlx::query(
            "SELECT DISTINCT turns.thread_id
             FROM turns
             JOIN client_conversations ON client_conversations.id = turns.conversation_id
             WHERE turns.status = 'queued'
               AND client_conversations.client_kind = ?
               AND NOT EXISTS (
                   SELECT 1 FROM excluded_threads
                   WHERE excluded_threads.thread_id = turns.thread_id
               )
             ORDER BY turns.thread_id",
        )
        .bind(client.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| parse_id(row.try_get::<String, _>("thread_id")?, "thread_id"))
            .collect()
    }

    pub async fn queued_conversation_ids(&self) -> Result<Vec<ConversationId>> {
        let rows = sqlx::query(
            "SELECT DISTINCT conversation_id FROM turns
             WHERE status = 'queued' AND conversation_id IS NOT NULL
             ORDER BY conversation_id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| Ok(ConversationId::new(row.try_get("conversation_id")?)?))
            .collect()
    }
}

async fn require_thread_admitted(
    connection: &mut sqlx::SqliteConnection,
    thread_id: ThreadId,
) -> Result<()> {
    let excluded: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM excluded_threads WHERE thread_id = ?)",
    )
    .bind(thread_id.to_string())
    .fetch_one(connection)
    .await?;
    if excluded {
        bail!("bot was kicked from this thread");
    }
    Ok(())
}

fn is_new_human_message(row: &sqlx::sqlite::SqliteRow, message_id: MessageId) -> Result<bool> {
    let latest = row
        .try_get::<Option<&str>, _>("latest_human_message_id")?
        .map(|value| {
            value.parse::<u64>().with_context(|| {
                format!("database column latest_human_message_id has invalid Discord id `{value}`")
            })
        })
        .transpose()?;
    Ok(latest.is_none_or(|latest| message_id.get() > latest))
}

fn session_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Session> {
    Ok(Session {
        conversation_id: ConversationId::new(row.try_get("conversation_id")?)?,
        thread_id: parse_id(row.try_get("thread_id")?, "thread_id")?,
        guild_id: parse_id(row.try_get("guild_id")?, "guild_id")?,
        parent_channel_id: parse_id(row.try_get("parent_channel_id")?, "parent_channel_id")?,
        trigger_message_id: parse_id(row.try_get("trigger_message_id")?, "trigger_message_id")?,
        omp_session_path: row
            .try_get::<Option<String>, _>("omp_session_path")?
            .map(Into::into),
        model: row.try_get("model")?,
        cwd: row.try_get::<String, _>("cwd")?.into(),
        created_by_user_id: parse_id(row.try_get("created_by_user_id")?, "created_by_user_id")?,
        status: SessionStatus::from_database(row.try_get("status")?)?,
    })
}

fn turn_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Turn> {
    Ok(Turn {
        conversation_id: ConversationId::new(row.try_get("conversation_id")?)?,
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
    use crate::domain::{
        ChannelId, ClientConversationId, ClientKind, GuildId, MessageId, NewSession, ThreadId,
        UserId,
    };

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
    async fn conversation_keys_are_client_scoped() {
        let (_directory, store) = test_store().await;
        store.create_session(&new_session(10, 20)).await.unwrap();
        let discord = ClientConversationId::discord(10).unwrap();
        let matrix =
            ClientConversationId::scoped(ClientKind::Matrix, "10").expect("Matrix room id");

        let discord_id = store
            .conversation_id(&discord)
            .await
            .unwrap()
            .expect("Discord conversation mapping");
        store
            .enqueue_turn(
                ThreadId::new(10),
                MessageId::new(30),
                UserId::new(3),
                "prompt",
                TurnSource::Human,
            )
            .await
            .unwrap();
        let turn_conversation_id: i64 =
            sqlx::query_scalar("SELECT conversation_id FROM turns WHERE discord_message_id = '30'")
                .fetch_one(&store.pool)
                .await
                .unwrap();
        assert_eq!(turn_conversation_id, discord_id.get());
        assert_eq!(
            store
                .session_by_conversation(discord_id)
                .await
                .unwrap()
                .unwrap()
                .thread_id,
            ThreadId::new(10)
        );
        assert_eq!(store.queued_conversation_ids().await.unwrap(), vec![discord_id]);
        assert!(store.conversation_id(&matrix).await.unwrap().is_none());

        let matrix_id = store.ensure_conversation(&matrix).await.unwrap();

        assert_ne!(discord_id, matrix_id);
        assert_eq!(
            store.conversation_id(&matrix).await.unwrap(),
            Some(matrix_id)
        );
        let matrix_thread = ThreadId::new(u64::try_from(matrix_id.get()).unwrap());
        let mut matrix_session = new_session(matrix_thread.get(), 40);
        matrix_session.created_by_user_id = UserId::new(4);
        store
            .create_session_for_conversation(matrix_id, &matrix_session)
            .await
            .unwrap();
        store
            .enqueue_turn(
                matrix_thread,
                MessageId::new(50),
                UserId::new(4),
                "matrix prompt",
                TurnSource::Human,
            )
            .await
            .unwrap();
        assert_eq!(
            store.queued_thread_ids_for(ClientKind::Discord).await.unwrap(),
            vec![ThreadId::new(10)]
        );
        assert_eq!(
            store.queued_thread_ids_for(ClientKind::Matrix).await.unwrap(),
            vec![matrix_thread]
        );
    }

    #[tokio::test]
    async fn client_event_and_actor_ids_are_durable_and_namespaced() {
        let (directory, store) = test_store().await;
        let matrix_event = EventId::scoped(ClientKind::Matrix, "same").unwrap();
        let discord_event = EventId::scoped(ClientKind::Discord, "same").unwrap();
        let actor = ActorId::scoped(ClientKind::Matrix, "@operator:example.org").unwrap();

        let matrix_id = store.ensure_event(&matrix_event).await.unwrap();
        assert_eq!(store.ensure_event(&matrix_event).await.unwrap(), matrix_id);
        assert_ne!(store.ensure_event(&discord_event).await.unwrap(), matrix_id);
        let actor_id = store.ensure_actor(&actor).await.unwrap();
        assert_eq!(store.ensure_actor(&actor).await.unwrap(), actor_id);
        store.pool.close().await;

        let reopened = Store::open(&directory.path().join("bridge.sqlite")).await.unwrap();
        assert_eq!(reopened.ensure_event(&matrix_event).await.unwrap(), matrix_id);
        assert_eq!(reopened.ensure_actor(&actor).await.unwrap(), actor_id);
    }

    #[tokio::test]
    async fn opening_existing_discord_state_backfills_conversation_keys() {
        let (directory, store) = test_store().await;
        store.create_session(&new_session(10, 20)).await.unwrap();
        sqlx::query("UPDATE sessions SET conversation_id = NULL")
            .execute(&store.pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM client_conversations")
            .execute(&store.pool)
            .await
            .unwrap();
        store.pool.close().await;

        let reopened = Store::open(&directory.path().join("bridge.sqlite"))
            .await
            .unwrap();
        let key = ClientConversationId::discord(10).unwrap();
        assert!(reopened.conversation_id(&key).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn unmanaged_thread_exclusion_survives_reopen_and_blocks_creation() {
        let (directory, store) = test_store().await;
        let thread = ThreadId::new(10);
        assert!(!store.is_thread_excluded(thread).await.unwrap());
        store.exclude_thread(thread).await.unwrap();
        store.exclude_thread(thread).await.unwrap();
        assert!(store.session(thread).await.unwrap().is_none());
        store.pool.close().await;

        let reopened = Store::open(&directory.path().join("bridge.sqlite"))
            .await
            .unwrap();
        assert!(reopened.is_thread_excluded(thread).await.unwrap());
        assert!(reopened.create_session(&new_session(10, 20)).await.is_err());
        assert!(reopened.session(thread).await.unwrap().is_none());
        assert!(!reopened.is_thread_excluded(ThreadId::new(11)).await.unwrap());
        reopened.create_session(&new_session(11, 21)).await.unwrap();
    }

    #[tokio::test]
    async fn exclusion_discards_only_queued_turns_and_preserves_history_on_reopen() {
        let (directory, store) = test_store().await;
        let thread = ThreadId::new(10);
        let other = ThreadId::new(11);
        let session_path = directory.path().join("omp-session.jsonl");
        store.create_session(&new_session(10, 20)).await.unwrap();
        store.create_session(&new_session(11, 21)).await.unwrap();
        store.set_session_path(thread, &session_path).await.unwrap();
        let done = enqueue(&store, 10, 30).await;
        store.claim_next(thread).await.unwrap().unwrap();
        store
            .finish_turn(done, TurnStatus::Done, Some("preserved response"), None)
            .await
            .unwrap();
        let running = enqueue(&store, 10, 31).await;
        store.claim_next(thread).await.unwrap().unwrap();
        let queued = enqueue(&store, 10, 32).await;
        let other_queued = enqueue(&store, 11, 33).await;

        store.exclude_thread(thread).await.unwrap();
        store.exclude_thread(thread).await.unwrap();
        let counts = store.counts(thread).await.unwrap();
        assert_eq!((counts.queued, counts.running, counts.uncertain), (0, 1, 0));
        let preserved = store.next_outcome(thread, 0).await.unwrap().unwrap();
        assert_eq!(preserved.id, done);
        assert_eq!(preserved.status, TurnStatus::Done);
        assert_eq!(preserved.response.as_deref(), Some("preserved response"));
        let discarded = store.next_outcome(thread, done).await.unwrap().unwrap();
        assert_eq!(discarded.id, queued);
        assert_eq!(discarded.status, TurnStatus::Aborted);
        assert_eq!(discarded.error.as_deref(), Some("bot was kicked from this thread"));
        assert_eq!(store.queued_thread_ids().await.unwrap(), vec![other]);
        store.pool.close().await;

        let reopened = Store::open(&directory.path().join("bridge.sqlite"))
            .await
            .unwrap();
        assert!(reopened.is_thread_excluded(thread).await.unwrap());
        let session = reopened.session(thread).await.unwrap().unwrap();
        assert_eq!(session.status, SessionStatus::Stopped);
        assert_eq!(session.omp_session_path.as_deref(), Some(session_path.as_path()));
        assert_eq!(reopened.next_outcome(thread, done).await.unwrap(), Some(discarded));
        assert!(reopened.claim_next(thread).await.unwrap().is_none());
        assert!(reopened.claim_next_interrupting(thread).await.unwrap().is_none());
        // The runner can still resolve an in-flight turn after exclusion.
        reopened
            .finish_turn(running, TurnStatus::Aborted, Some("partial"), None)
            .await
            .unwrap();
        assert_eq!(
            reopened.claim_next(other).await.unwrap().unwrap().id,
            other_queued
        );
        assert_eq!(
            reopened.session(other).await.unwrap().unwrap().status,
            SessionStatus::Active
        );
    }

    #[tokio::test]
    async fn exclusion_blocks_reset_reactivation_and_all_admission_paths() {
        let (_directory, store) = test_store().await;
        let thread = ThreadId::new(10);
        store.create_session(&new_session(10, 20)).await.unwrap();
        enqueue(&store, 10, 30).await;
        store.exclude_thread(thread).await.unwrap();
        assert!(store.reset_session(thread).await.is_err());
        assert!(store.set_session_status(thread, SessionStatus::Active).await.is_err());
        store.set_session_status(thread, SessionStatus::Stopped).await.unwrap();
        assert!(store.create_session(&new_session(10, 20)).await.is_err());
        assert!(store.create_session(&new_session(10, 21)).await.is_err());
        // A duplicate trigger must not return the excluded session under another thread ID.
        assert!(store.create_session(&new_session(11, 20)).await.is_err());
        assert!(store.session(ThreadId::new(11)).await.unwrap().is_none());
        assert!(store.note_human_message(thread, MessageId::new(40)).await.is_err());
        for (message, source) in [(30, TurnSource::Human), (31, TurnSource::Human), (32, TurnSource::Bot)] {
            assert!(
                store
                    .enqueue_turn(thread, MessageId::new(message), UserId::new(3), "blocked", source)
                    .await
                    .is_err()
            );
        }
        assert!(store.claim_next(thread).await.unwrap().is_none());
        assert!(store.claim_next_interrupting(thread).await.unwrap().is_none());
        assert!(store.queued_thread_ids().await.unwrap().is_empty());
        assert_eq!(store.session(thread).await.unwrap().unwrap().status, SessionStatus::Stopped);

        let other = ThreadId::new(11);
        store.create_session(&new_session(11, 21)).await.unwrap();
        store.set_session_status(other, SessionStatus::Stopped).await.unwrap();
        store.reset_session(other).await.unwrap();
        store.note_human_message(other, MessageId::new(41)).await.unwrap();
        let admitted = enqueue(&store, 11, 42).await;
        assert_eq!(store.claim_next(other).await.unwrap().unwrap().id, admitted);
    }

    #[tokio::test]
    async fn claims_and_queue_discovery_independently_honor_exclusion() {
        let (_directory, store) = test_store().await;
        let thread = ThreadId::new(10);
        store.create_session(&new_session(10, 20)).await.unwrap();
        let turn = enqueue(&store, 10, 30).await;
        store.exclude_thread(thread).await.unwrap();
        // Simulate stale state imported by an older writer: neither the active flag nor
        // a queued turn may override the independent durable tombstone.
        sqlx::query("UPDATE sessions SET status = 'active' WHERE thread_id = '10'")
            .execute(&store.pool)
            .await
            .unwrap();
        sqlx::query("UPDATE turns SET status = 'queued' WHERE id = ?")
            .bind(turn)
            .execute(&store.pool)
            .await
            .unwrap();
        assert!(store.claim_next(thread).await.unwrap().is_none());
        assert!(store.claim_next_interrupting(thread).await.unwrap().is_none());
        assert!(store.queued_thread_ids().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn thread_access_locks_are_shared_by_clones_and_isolated_by_thread() {
        let (_directory, store) = test_store().await;
        let clone = store.clone();
        let lock = store.thread_access(ThreadId::new(10));
        let same = clone.thread_access(ThreadId::new(10));
        let other = clone.thread_access(ThreadId::new(11));
        assert!(Arc::ptr_eq(&lock, &same));
        assert!(!Arc::ptr_eq(&lock, &other));
        let reader = lock.clone().read_owned().await;
        assert!(same.try_write().is_err());
        assert!(other.try_write().is_ok());
        drop(reader);
        let writer = same.clone().write_owned().await;
        assert!(lock.try_read().is_err());
        assert!(other.try_read().is_ok());
        drop(writer);
        assert!(lock.try_read().is_ok());
    }

    #[tokio::test]
    async fn independent_connections_serialize_exclusion_with_creation_and_admission() {
        let (directory, store) = test_store().await;
        let independent = Store::open(&directory.path().join("bridge.sqlite"))
            .await
            .unwrap();
        for thread_number in 10..14 {
            let thread = ThreadId::new(thread_number);
            let input = new_session(thread_number, thread_number + 100);
            let (created, excluded) = tokio::join!(
                store.create_session(&input),
                independent.exclude_thread(thread),
            );
            excluded.unwrap();
            assert!(store.is_thread_excluded(thread).await.unwrap());
            if created.is_ok() {
                assert_eq!(
                    store.session(thread).await.unwrap().unwrap().status,
                    SessionStatus::Stopped
                );
            } else {
                assert!(store.session(thread).await.unwrap().is_none());
            }
            assert!(store.create_session(&input).await.is_err());
        }

        for thread_number in 20..24 {
            let thread = ThreadId::new(thread_number);
            store.create_session(&new_session(thread_number, thread_number + 100)).await.unwrap();
            enqueue(&store, thread_number, thread_number + 200).await;
            let (_, _, _, _, excluded) = tokio::join!(
                store.reset_session(thread),
                store.set_session_status(thread, SessionStatus::Active),
                store.enqueue_turn(
                    thread,
                    MessageId::new(thread_number + 300),
                    UserId::new(3),
                    "racing admission",
                    TurnSource::Human,
                ),
                store.claim_next_interrupting(thread),
                independent.exclude_thread(thread),
            );
            excluded.unwrap();
            assert_eq!(
                store.session(thread).await.unwrap().unwrap().status,
                SessionStatus::Stopped
            );
            assert_eq!(store.counts(thread).await.unwrap().queued, 0);
            assert!(store.claim_next(thread).await.unwrap().is_none());
            assert!(store.claim_next_interrupting(thread).await.unwrap().is_none());
            assert!(!store.queued_thread_ids().await.unwrap().contains(&thread));
        }
    }

    #[tokio::test]
    async fn title_context_bounds_text_and_selects_only_recent_session_turns() {
        let (_directory, store) = test_store().await;
        store.create_session(&new_session(10, 20)).await.unwrap();
        store.create_session(&new_session(11, 21)).await.unwrap();
        let huge = "界".repeat(10_000);
        for index in 0..10 {
            sqlx::query("INSERT INTO turns (thread_id, discord_message_id, author_id, content, response, status, created_at_ms) VALUES ('10', ?, '3', ?, ?, 'done', 0)")
                .bind((100 + index).to_string())
                .bind(format!("{index}:{huge}"))
                .bind(&huge)
                .execute(&store.pool)
                .await
                .unwrap();
        }
        store.enqueue_turn(ThreadId::new(11), MessageId::new(200), UserId::new(3), "unrelated", TurnSource::Human).await.unwrap();
        let context = store.title_context(ThreadId::new(10)).await.unwrap();
        assert_eq!(context.len(), 8);
        assert!(context[0].prompt.starts_with("2:"));
        assert!(context[7].prompt.starts_with("9:"));
        for turn in context {
            assert_eq!(turn.prompt.chars().count(), 2048);
            assert_eq!(turn.response.unwrap().chars().count(), 2048);
            assert_eq!(turn.status, TurnStatus::Done);
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
                TurnSource::Human,
            )
            .await
            .expect("enqueue first");
        let duplicate = store
            .enqueue_turn(
                ThreadId::new(10),
                MessageId::new(30),
                UserId::new(3),
                "duplicate",
                TurnSource::Human,
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
            .enqueue_turn(
                ThreadId::new(10),
                MessageId::new(30),
                UserId::new(3),
                "run",
                TurnSource::Human,
            )
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
    async fn session_mapping_path_and_model_survive_reopen() {
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
        store
            .set_session_model(ThreadId::new(10), Some("openai/shared"))
            .await
            .expect("set model");
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
        assert_eq!(session.model.as_deref(), Some("openai/shared"));
        reopened
            .set_session_model(ThreadId::new(10), None)
            .await
            .expect("clear override");
        reopened.pool.close().await;
        let defaulted = Store::open(&database).await.expect("reopen default");
        assert!(
            defaulted
                .session(ThreadId::new(10))
                .await
                .unwrap()
                .unwrap()
                .model
                .is_none()
        );
    }

    async fn enqueue(store: &Store, thread: u64, message: u64) -> i64 {
        match store
            .enqueue_turn(
                ThreadId::new(thread),
                MessageId::new(message),
                UserId::new(3),
                "prompt",
                TurnSource::Human,
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
            .enqueue_turn(
                thread,
                MessageId::new(164),
                UserId::new(3),
                "overflow",
                TurnSource::Human,
            )
            .await
            .expect_err("64 pending turns must reject admission");
        assert!(overflow.to_string().contains("limit (64)"));
        for message in [100, 163] {
            assert_eq!(
                store
                    .enqueue_turn(
                        thread,
                        MessageId::new(message),
                        UserId::new(3),
                        "duplicate",
                        TurnSource::Human,
                    )
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
                .enqueue_turn(
                    thread,
                    MessageId::new(31),
                    UserId::new(3),
                    "duplicate",
                    TurnSource::Human,
                )
                .await
                .expect("duplicate after stop"),
            EnqueueResult::Duplicate
        );
        let error = store
            .enqueue_turn(
                thread,
                MessageId::new(32),
                UserId::new(3),
                "stale handler",
                TurnSource::Human,
            )
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
        let original = Store { pool, thread_access: Arc::new(Mutex::new(HashMap::new())) };
        sqlx::query(
            "INSERT INTO sessions (thread_id, guild_id, parent_channel_id, trigger_message_id, cwd, created_by_user_id, status, created_at_ms, updated_at_ms) VALUES ('10', '1', '2', '20', '/tmp', '3', 'active', 0, 0)",
        )
        .execute(&original.pool)
        .await
        .expect("create original session");
        let mut ids = Vec::new();
        for message in [30, 31] {
            let result = sqlx::query(
                "INSERT INTO turns (thread_id, discord_message_id, author_id, content, status, created_at_ms) VALUES ('10', ?, '3', 'prompt', 'queued', 0)",
            )
            .bind(message.to_string())
            .execute(&original.pool)
            .await
            .expect("insert original turn");
            ids.push(result.last_insert_rowid());
        }
        let [first, pending] = ids.as_slice() else {
            panic!("expected two original turns");
        };
        let (first, pending) = (*first, *pending);
        sqlx::query("UPDATE turns SET status = 'running', started_at_ms = 0 WHERE id = ?")
            .bind(first)
            .execute(&original.pool)
            .await
            .expect("mark original turn running");
        original.pool.close().await;

        let migrated = Store::open(&database).await.expect("migrate");
        assert_eq!(budget_state(&migrated, 10).await, (0, None));
        assert_eq!(interruption_link(&migrated, first).await, None);
        assert!(
            migrated
                .session(ThreadId::new(10))
                .await
                .unwrap()
                .unwrap()
                .model
                .is_none()
        );
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
        assert_eq!(budget_state(&reopened, 10).await, (0, None));
    }

    async fn admit(
        store: &Store,
        thread: u64,
        message: u64,
        source: TurnSource,
    ) -> Result<EnqueueResult> {
        store
            .enqueue_turn(
                ThreadId::new(thread),
                MessageId::new(message),
                UserId::new(3),
                "prompt",
                source,
            )
            .await
    }

    async fn budget_state(store: &Store, thread: u64) -> (i64, Option<String>) {
        sqlx::query_as(
            "SELECT bot_exchange_count, latest_human_message_id FROM sessions WHERE thread_id = ?",
        )
        .bind(thread.to_string())
        .fetch_one(&store.pool)
        .await
        .expect("read budget")
    }

    #[tokio::test]
    async fn bot_budget_is_per_session_and_duplicates_never_replenish_it() {
        let (_directory, store) = test_store().await;
        store.create_session(&new_session(10, 20)).await.unwrap();
        enqueue(&store, 10, 30).await;
        for message in 100..108 {
            assert!(matches!(
                admit(&store, 10, message, TurnSource::Bot).await.unwrap(),
                EnqueueResult::Enqueued(_)
            ));
        }
        assert_eq!(MAX_BOT_EXCHANGES, 8);
        assert_eq!(budget_state(&store, 10).await, (8, Some("30".into())));
        for source in [TurnSource::Bot, TurnSource::Human] {
            assert_eq!(
                admit(&store, 10, 107, source).await.unwrap(),
                EnqueueResult::Duplicate
            );
        }
        assert_eq!(budget_state(&store, 10).await, (8, Some("30".into())));
        let error = admit(&store, 10, 108, TurnSource::Bot)
            .await
            .expect_err("ninth bot message must be rejected");
        assert!(error.to_string().contains("limit (8)"));
        assert!(error.to_string().contains("human message"));
        assert_eq!(store.counts(ThreadId::new(10)).await.unwrap().queued, 9);

        store.create_session(&new_session(11, 21)).await.unwrap();
        admit(&store, 11, 200, TurnSource::Bot).await.unwrap();
        assert_eq!(budget_state(&store, 11).await, (1, None));

        for status in [TurnStatus::Done, TurnStatus::Failed, TurnStatus::Aborted] {
            let turn = store.claim_next(ThreadId::new(10)).await.unwrap().unwrap();
            store
                .finish_turn(turn.id, status, None, None)
                .await
                .unwrap();
        }
        assert_eq!(budget_state(&store, 10).await, (8, Some("30".into())));
        assert!(admit(&store, 10, 108, TurnSource::Bot).await.is_err());
    }

    #[tokio::test]
    async fn concurrent_bot_admission_and_human_watermarks_are_serialized() {
        let (directory, store) = test_store().await;
        store.create_session(&new_session(10, 20)).await.unwrap();
        let other = Store::open(&directory.path().join("bridge.sqlite"))
            .await
            .unwrap();
        let mut tasks = tokio::task::JoinSet::new();
        for message in 100..132 {
            let writer = if message % 2 == 0 {
                store.clone()
            } else {
                other.clone()
            };
            tasks.spawn(async move { admit(&writer, 10, message, TurnSource::Bot).await });
        }
        let mut accepted = 0;
        let mut rejected = 0;
        while let Some(result) = tasks.join_next().await {
            match result.unwrap() {
                Ok(EnqueueResult::Enqueued(_)) => accepted += 1,
                Ok(EnqueueResult::Duplicate) => panic!("all messages are distinct"),
                Err(error) => {
                    assert!(error.to_string().contains("human message"));
                    rejected += 1;
                }
            }
        }
        assert_eq!((accepted, rejected), (8, 24));
        assert_eq!(budget_state(&store, 10).await, (8, None));
        assert_eq!(store.counts(ThreadId::new(10)).await.unwrap().queued, 8);

        let mut notes = tokio::task::JoinSet::new();
        for message in (300..332).rev() {
            let writer = if message % 2 == 0 {
                store.clone()
            } else {
                other.clone()
            };
            notes.spawn(async move {
                writer
                    .note_human_message(ThreadId::new(10), MessageId::new(message))
                    .await
            });
        }
        while let Some(result) = notes.join_next().await {
            result.unwrap().unwrap();
        }
        assert_eq!(budget_state(&store, 10).await, (0, Some("331".into())));
        admit(&store, 10, 400, TurnSource::Bot).await.unwrap();
        for message in 300..332 {
            other
                .note_human_message(ThreadId::new(10), MessageId::new(message))
                .await
                .unwrap();
        }
        assert_eq!(budget_state(&store, 10).await, (1, Some("331".into())));
    }

    #[tokio::test]
    async fn bot_budget_and_human_watermark_survive_reopening() {
        let (directory, store) = test_store().await;
        let thread = ThreadId::new(10);
        store.create_session(&new_session(10, 20)).await.unwrap();
        enqueue(&store, 10, 100).await;
        for message in 101..108 {
            admit(&store, 10, message, TurnSource::Bot).await.unwrap();
        }
        store.pool.close().await;
        let reopened = Store::open(&directory.path().join("bridge.sqlite"))
            .await
            .unwrap();
        assert_eq!(budget_state(&reopened, 10).await, (7, Some("100".into())));
        for (message, source) in [(100, TurnSource::Human), (101, TurnSource::Bot)] {
            assert_eq!(
                admit(&reopened, 10, message, source).await.unwrap(),
                EnqueueResult::Duplicate
            );
        }
        reopened
            .note_human_message(thread, MessageId::new(100))
            .await
            .unwrap();
        assert_eq!(budget_state(&reopened, 10).await, (7, Some("100".into())));
        admit(&reopened, 10, 108, TurnSource::Bot).await.unwrap();
        assert!(admit(&reopened, 10, 109, TurnSource::Bot).await.is_err());
        assert_eq!(budget_state(&reopened, 10).await, (8, Some("100".into())));
    }

    #[tokio::test]
    async fn only_newer_human_prompts_and_controls_reset_the_budget() {
        let (_directory, store) = test_store().await;
        let thread = ThreadId::new(10);
        store.create_session(&new_session(10, 20)).await.unwrap();
        enqueue(&store, 10, 100).await;
        for message in 200..208 {
            admit(&store, 10, message, TurnSource::Bot).await.unwrap();
        }
        assert_eq!(
            admit(&store, 10, 100, TurnSource::Human).await.unwrap(),
            EnqueueResult::Duplicate
        );
        enqueue(&store, 10, 90).await;
        for message in [99, 100] {
            store
                .note_human_message(thread, MessageId::new(message))
                .await
                .unwrap();
        }
        assert_eq!(budget_state(&store, 10).await, (8, Some("100".into())));
        enqueue(&store, 10, 300).await;
        assert_eq!(budget_state(&store, 10).await, (0, Some("300".into())));
        admit(&store, 10, 301, TurnSource::Bot).await.unwrap();
        store
            .note_human_message(thread, MessageId::new(400))
            .await
            .unwrap();
        assert_eq!(budget_state(&store, 10).await, (0, Some("400".into())));

        // Delayed bot messages predate the reset, but consume the current allowance.
        for message in 150..158 {
            admit(&store, 10, message, TurnSource::Bot).await.unwrap();
        }
        assert!(admit(&store, 10, 158, TurnSource::Bot).await.is_err());
        assert_eq!(budget_state(&store, 10).await, (8, Some("400".into())));
        for message in [400, 399] {
            store
                .note_human_message(thread, MessageId::new(message))
                .await
                .unwrap();
            store.reset_session(thread).await.unwrap();
        }
        assert_eq!(budget_state(&store, 10).await, (8, Some("400".into())));
        assert!(admit(&store, 10, 401, TurnSource::Bot).await.is_err());
        store
            .note_human_message(thread, MessageId::new(500))
            .await
            .unwrap();
        admit(&store, 10, 501, TurnSource::Bot).await.unwrap();
        enqueue(&store, 10, 350).await;
        assert_eq!(budget_state(&store, 10).await, (1, Some("500".into())));
    }

    #[tokio::test]
    async fn human_watermarks_compare_full_unsigned_snowflakes_numerically() {
        let (_directory, store) = test_store().await;
        let thread = ThreadId::new(10);
        store.create_session(&new_session(10, 20)).await.unwrap();
        let messages = [9, 10, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX];
        for (index, message) in messages.into_iter().enumerate() {
            store
                .note_human_message(thread, MessageId::new(message))
                .await
                .unwrap();
            assert_eq!(
                budget_state(&store, 10).await,
                (0, Some(message.to_string()))
            );
            admit(&store, 10, 100 + index as u64, TurnSource::Bot)
                .await
                .unwrap();
            for replay in [message, message - 1] {
                store
                    .note_human_message(thread, MessageId::new(replay))
                    .await
                    .unwrap();
            }
            assert_eq!(
                budget_state(&store, 10).await,
                (1, Some(message.to_string()))
            );
        }
    }

    #[tokio::test]
    async fn queue_full_and_stopped_rejections_do_not_change_budget_or_watermark() {
        let (_directory, store) = test_store().await;
        let thread = ThreadId::new(10);
        store.create_session(&new_session(10, 20)).await.unwrap();
        for message in 100..157 {
            enqueue(&store, 10, message).await;
        }
        for message in 200..207 {
            admit(&store, 10, message, TurnSource::Bot).await.unwrap();
        }
        assert_eq!(budget_state(&store, 10).await, (7, Some("156".into())));
        for (message, source) in [(300, TurnSource::Human), (207, TurnSource::Bot)] {
            let error = admit(&store, 10, message, source).await.unwrap_err();
            assert!(error.to_string().contains("limit (64)"));
        }
        for (message, source) in [(156, TurnSource::Human), (206, TurnSource::Bot)] {
            assert_eq!(
                admit(&store, 10, message, source).await.unwrap(),
                EnqueueResult::Duplicate
            );
        }
        assert_eq!(budget_state(&store, 10).await, (7, Some("156".into())));
        store
            .set_session_status(thread, SessionStatus::Stopped)
            .await
            .unwrap();
        for (message, source) in [(300, TurnSource::Human), (207, TurnSource::Bot)] {
            let error = admit(&store, 10, message, source).await.unwrap_err();
            assert!(error.to_string().contains("stopped"));
        }
        assert_eq!(budget_state(&store, 10).await, (7, Some("156".into())));
        assert_eq!(store.reset_session(thread).await.unwrap(), 64);
        assert_eq!(budget_state(&store, 10).await, (7, Some("156".into())));
        admit(&store, 10, 207, TurnSource::Bot).await.unwrap();
        assert_eq!(budget_state(&store, 10).await, (8, Some("156".into())));
        enqueue(&store, 10, 300).await;
        assert_eq!(budget_state(&store, 10).await, (0, Some("300".into())));
    }

    #[tokio::test]
    async fn budget_update_failure_rolls_back_the_turn_and_all_budget_changes() {
        let (_directory, store) = test_store().await;
        let thread = ThreadId::new(10);
        store.create_session(&new_session(10, 20)).await.unwrap();
        enqueue(&store, 10, 100).await;
        admit(&store, 10, 101, TurnSource::Bot).await.unwrap();
        sqlx::raw_sql(
            "CREATE TRIGGER reject_budget_change BEFORE UPDATE OF bot_exchange_count ON sessions
             BEGIN SELECT RAISE(ABORT, 'test budget failure'); END;",
        )
        .execute(&store.pool)
        .await
        .unwrap();
        for (message, source) in [(200, TurnSource::Human), (102, TurnSource::Bot)] {
            let error = admit(&store, 10, message, source).await.unwrap_err();
            assert!(error.to_string().contains("test budget failure"));
            assert_eq!(budget_state(&store, 10).await, (1, Some("100".into())));
            assert_eq!(store.counts(thread).await.unwrap().queued, 2);
        }
        assert!(
            store
                .note_human_message(thread, MessageId::new(300))
                .await
                .is_err()
        );
        assert_eq!(budget_state(&store, 10).await, (1, Some("100".into())));
        sqlx::query("DROP TRIGGER reject_budget_change")
            .execute(&store.pool)
            .await
            .unwrap();
        enqueue(&store, 10, 200).await;
        admit(&store, 10, 102, TurnSource::Bot).await.unwrap();
        assert_eq!(budget_state(&store, 10).await, (1, Some("200".into())));
    }

    #[tokio::test]
    async fn concurrent_existing_thread_adoption_keeps_the_winners_metadata() {
        let (directory, store) = test_store().await;
        let other = Store::open(&directory.path().join("bridge.sqlite"))
            .await
            .unwrap();
        let thread = ThreadId::new(10);
        let first = new_session(10, 20);
        let second = NewSession {
            guild_id: GuildId::new(4),
            parent_channel_id: ChannelId::new(5),
            cwd: directory.path().join("other"),
            created_by_user_id: UserId::new(6),
            ..new_session(10, 21)
        };
        let (left, right) =
            tokio::join!(store.create_session(&first), other.create_session(&second),);
        let (created, existing, winner, loser) = match (left.unwrap(), right.unwrap()) {
            (CreateSessionResult::Created(created), CreateSessionResult::Existing(existing)) => {
                (created, existing, &first, &second)
            }
            (CreateSessionResult::Existing(existing), CreateSessionResult::Created(created)) => {
                (created, existing, &second, &first)
            }
            results => panic!("expected one winner and one existing session: {results:?}"),
        };
        for session in [&created, &existing] {
            assert_eq!(session.thread_id, thread);
            assert_eq!(session.trigger_message_id, winner.trigger_message_id);
            assert_eq!(session.guild_id, winner.guild_id);
            assert_eq!(session.parent_channel_id, winner.parent_channel_id);
            assert_eq!(session.cwd, winner.cwd);
            assert_eq!(session.created_by_user_id, winner.created_by_user_id);
        }
        assert!(
            store
                .session_by_trigger(loser.trigger_message_id)
                .await
                .unwrap()
                .is_none()
        );
        let session_path = directory.path().join("existing-session.jsonl");
        store.set_session_path(thread, &session_path).await.unwrap();
        store
            .note_human_message(thread, MessageId::new(500))
            .await
            .unwrap();
        admit(&store, 10, 501, TurnSource::Bot).await.unwrap();
        store
            .set_session_status(thread, SessionStatus::Stopped)
            .await
            .unwrap();
        let CreateSessionResult::Existing(session) = other.create_session(loser).await.unwrap()
        else {
            panic!("adopting a known thread must not replace it");
        };
        assert_eq!(session.trigger_message_id, winner.trigger_message_id);
        assert_eq!(session.omp_session_path, Some(session_path));
        assert_eq!(session.status, SessionStatus::Stopped);
        assert_eq!(budget_state(&store, 10).await, (1, Some("500".into())));
    }

    #[tokio::test]
    async fn duplicate_trigger_takes_precedence_over_an_existing_thread() {
        let (_directory, store) = test_store().await;
        store.create_session(&new_session(10, 20)).await.unwrap();
        store.create_session(&new_session(11, 21)).await.unwrap();
        for thread in [12, 11] {
            let CreateSessionResult::Existing(session) = store
                .create_session(&new_session(thread, 20))
                .await
                .unwrap()
            else {
                panic!("duplicate trigger must retain the original mapping");
            };
            assert_eq!(session.thread_id, ThreadId::new(10));
            assert_eq!(session.trigger_message_id, MessageId::new(20));
        }
        assert!(store.session(ThreadId::new(12)).await.unwrap().is_none());
        assert_eq!(
            store
                .session(ThreadId::new(11))
                .await
                .unwrap()
                .unwrap()
                .trigger_message_id,
            MessageId::new(21)
        );
    }
}

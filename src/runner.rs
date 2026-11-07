use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use tokio::sync::{Mutex, Notify, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::{
    domain::{SessionStatus, ThreadId, Turn, TurnStatus},
    omp::{AgentTransport, ApprovalRequest, OmpTurnError, OmpUi},
    render::chunk_message,
    store::Store,
};

#[async_trait]
pub trait ThreadMessenger: Send + Sync {
    async fn send(&self, thread_id: ThreadId, content: &str) -> Result<()>;
    async fn typing(&self, thread_id: ThreadId) -> Result<()>;
    async fn confirm(&self, thread_id: ThreadId, request: ApprovalRequest) -> bool;
    async fn notify(&self, thread_id: ThreadId, message: &str);
    async fn channel_history(&self, thread_id: ThreadId, days: u16) -> Result<String>;
}

pub struct ThreadQueueRunner {
    store: Store,
    transport: Arc<dyn AgentTransport>,
    messenger: Arc<dyn ThreadMessenger>,
    message_limit: usize,
    permits: Arc<Semaphore>,
    draining: Mutex<HashSet<ThreadId>>,
    active: Mutex<HashMap<ThreadId, CancellationToken>>,
    idle: Notify,
}

impl ThreadQueueRunner {
    pub fn new(
        store: Store,
        transport: Arc<dyn AgentTransport>,
        messenger: Arc<dyn ThreadMessenger>,
        message_limit: usize,
        max_concurrency: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            transport,
            messenger,
            message_limit,
            permits: Arc::new(Semaphore::new(max_concurrency)),
            draining: Mutex::new(HashSet::new()),
            active: Mutex::new(HashMap::new()),
            idle: Notify::new(),
        })
    }

    pub async fn poke(self: &Arc<Self>, thread_id: ThreadId) {
        if !self.draining.lock().await.insert(thread_id) {
            return;
        }
        let runner = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                if let Err(error) = runner.drain(thread_id).await {
                    tracing::error!(thread_id = %thread_id, error = %error, "thread queue drain failed");
                    let _ = runner
                        .messenger
                        .send(thread_id, &format!("Bridge queue failed: {error}"))
                        .await;
                }
                runner.draining.lock().await.remove(&thread_id);
                runner.idle.notify_waiters();
                match runner.store.counts(thread_id).await {
                    Ok(counts) if counts.queued > 0 => {
                        if !runner.draining.lock().await.insert(thread_id) {
                            break;
                        }
                    }
                    Ok(_) => break,
                    Err(error) => {
                        tracing::error!(thread_id = %thread_id, error = %error, "failed to inspect remaining queue");
                        break;
                    }
                }
            }
        });
    }

    pub async fn stop(&self, thread_id: ThreadId) -> Result<bool> {
        self.store
            .set_session_status(thread_id, SessionStatus::Stopped)
            .await?;
        let cancellation = self.active.lock().await.get(&thread_id).cloned();
        if let Some(cancellation) = cancellation {
            cancellation.cancel();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn new_session(&self, thread_id: ThreadId) -> Result<u64> {
        if self.active.lock().await.contains_key(&thread_id) {
            bail!(
                "stop the active OMP turn and wait for it to finish before starting a new session"
            );
        }
        self.store.reset_session(thread_id).await
    }

    pub async fn status(&self, thread_id: ThreadId) -> Result<String> {
        let session = self
            .store
            .session(thread_id)
            .await?
            .context("no OMP session is mapped to this thread")?;
        let counts = self.store.counts(thread_id).await?;
        let session_path = session.omp_session_path.as_deref().map_or_else(
            || "not created yet".to_owned(),
            |path| path.display().to_string(),
        );
        Ok(format!(
            "OMP session status: {}\nWorking directory: `{}`\nSession file: `{}`\nQueue: {} running, {} queued, {} uncertain",
            session.status.as_str(),
            session.cwd.display(),
            session_path,
            counts.running,
            counts.queued,
            counts.uncertain
        ))
    }

    pub async fn wait_idle(&self, thread_id: ThreadId) {
        loop {
            let notified = self.idle.notified();
            if !self.draining.lock().await.contains(&thread_id) {
                return;
            }
            notified.await;
        }
    }

    async fn drain(self: &Arc<Self>, thread_id: ThreadId) -> Result<()> {
        loop {
            let session = self
                .store
                .session(thread_id)
                .await?
                .context("queued thread has no session mapping")?;
            if session.status == SessionStatus::Stopped {
                return Ok(());
            }

            let _permit = Arc::clone(&self.permits)
                .acquire_owned()
                .await
                .context("global turn semaphore closed")?;
            let Some(turn) = self.store.claim_next(thread_id).await? else {
                return Ok(());
            };
            let cancellation = CancellationToken::new();
            self.active
                .lock()
                .await
                .insert(thread_id, cancellation.clone());

            let current = self
                .store
                .session(thread_id)
                .await?
                .context("claimed turn has no session mapping")?;
            if current.status == SessionStatus::Stopped {
                self.active.lock().await.remove(&thread_id);
                self.store.requeue_claimed_turn(turn.id).await?;
                return Ok(());
            }

            if let Err(error) = self.process_turn(&current, &turn, cancellation).await {
                tracing::error!(thread_id = %thread_id, turn_id = turn.id, error = %error, "turn processing failed");
            }
            self.active.lock().await.remove(&thread_id);
        }
    }

    async fn process_turn(
        &self,
        session: &crate::domain::Session,
        turn: &Turn,
        cancellation: CancellationToken,
    ) -> Result<()> {
        self.messenger.typing(turn.thread_id).await?;
        let ui = RunnerUi {
            messenger: Arc::clone(&self.messenger),
            thread_id: turn.thread_id,
        };
        match self
            .transport
            .run_turn(session, &turn.content, cancellation, &ui)
            .await
        {
            Ok(result) => {
                self.store
                    .set_session_path(turn.thread_id, &result.session_path)
                    .await?;
                if result.aborted {
                    self.store
                        .finish_turn(
                            turn.id,
                            TurnStatus::Aborted,
                            (!result.text.is_empty()).then_some(result.text.as_str()),
                            Some("turn cancelled by user"),
                        )
                        .await?;
                    self.messenger
                        .send(turn.thread_id, "OMP turn aborted.")
                        .await?;
                } else {
                    self.store
                        .finish_turn(turn.id, TurnStatus::Done, Some(&result.text), None)
                        .await?;
                    for chunk in chunk_message(&result.text, self.message_limit) {
                        self.messenger.send(turn.thread_id, &chunk).await?;
                    }
                }
            }
            Err(error) => {
                let status = if error.is_uncertain() {
                    TurnStatus::Uncertain
                } else {
                    TurnStatus::Failed
                };
                self.store
                    .finish_turn(turn.id, status, None, Some(&error.to_string()))
                    .await?;
                self.messenger
                    .send(turn.thread_id, &turn_error_message(&error))
                    .await?;
            }
        }
        Ok(())
    }
}

struct RunnerUi {
    messenger: Arc<dyn ThreadMessenger>,
    thread_id: ThreadId,
}

#[async_trait]
impl OmpUi for RunnerUi {
    async fn confirm(&self, request: ApprovalRequest) -> bool {
        self.messenger.confirm(self.thread_id, request).await
    }

    async fn notify(&self, message: &str) {
        self.messenger.notify(self.thread_id, message).await;
    }

    async fn channel_history(&self, days: u16) -> Result<String, String> {
        self.messenger
            .channel_history(self.thread_id, days)
            .await
            .map_err(|error| error.to_string())
    }
}

fn turn_error_message(error: &OmpTurnError) -> String {
    if error.is_uncertain() {
        format!(
            "OMP connection failed after the turn may have started. The turn was not replayed automatically. Inspect the working tree and session before retrying.\n\n{error}"
        )
    } else {
        format!("OMP turn failed: {error}")
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use tokio::sync::Mutex;

    use super::*;
    use crate::{
        domain::{ChannelId, GuildId, MessageId, NewSession, ThreadId, UserId},
        omp::OmpTurnResult,
        store::EnqueueResult,
    };

    #[derive(Default)]
    struct FakeTransport {
        prompts: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl AgentTransport for FakeTransport {
        async fn run_turn(
            &self,
            _session: &crate::domain::Session,
            prompt: &str,
            _cancellation: CancellationToken,
            _ui: &dyn OmpUi,
        ) -> Result<OmpTurnResult, OmpTurnError> {
            self.prompts.lock().await.push(prompt.to_owned());
            Ok(OmpTurnResult {
                text: format!("reply: {prompt}"),
                session_path: PathBuf::from("/tmp/fake-session.jsonl"),
                session_id: "fake".to_owned(),
                aborted: false,
            })
        }
    }

    #[derive(Default)]
    struct FakeMessenger {
        sent: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl ThreadMessenger for FakeMessenger {
        async fn send(&self, _thread_id: ThreadId, content: &str) -> Result<()> {
            self.sent.lock().await.push(content.to_owned());
            Ok(())
        }

        async fn typing(&self, _thread_id: ThreadId) -> Result<()> {
            Ok(())
        }

        async fn confirm(&self, _thread_id: ThreadId, _request: ApprovalRequest) -> bool {
            false
        }

        async fn notify(&self, _thread_id: ThreadId, _message: &str) {}

        async fn channel_history(&self, _thread_id: ThreadId, _days: u16) -> Result<String> {
            Ok("[]".to_owned())
        }
    }

    #[tokio::test]
    async fn turns_for_one_thread_execute_in_acceptance_order() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = Store::open(&directory.path().join("bridge.sqlite"))
            .await
            .expect("store");
        let thread_id = ThreadId::new(10);
        store
            .create_session(&NewSession {
                thread_id,
                guild_id: GuildId::new(1),
                parent_channel_id: ChannelId::new(2),
                trigger_message_id: MessageId::new(3),
                cwd: directory.path().to_owned(),
                created_by_user_id: UserId::new(4),
            })
            .await
            .expect("session");
        for (message_id, content) in [(20, "first"), (21, "second")] {
            assert!(matches!(
                store
                    .enqueue_turn(
                        thread_id,
                        MessageId::new(message_id),
                        UserId::new(4),
                        content,
                    )
                    .await
                    .expect("enqueue"),
                EnqueueResult::Enqueued(_)
            ));
        }
        let transport = Arc::new(FakeTransport::default());
        let messenger = Arc::new(FakeMessenger::default());
        let runner = ThreadQueueRunner::new(store, transport.clone(), messenger.clone(), 1_900, 1);

        runner.poke(thread_id).await;
        runner.wait_idle(thread_id).await;

        assert_eq!(
            *transport.prompts.lock().await,
            vec!["first".to_owned(), "second".to_owned()]
        );
        assert_eq!(
            *messenger.sent.lock().await,
            vec!["reply: first".to_owned(), "reply: second".to_owned()]
        );
    }
}

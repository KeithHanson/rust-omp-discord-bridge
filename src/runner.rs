use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use tokio::{
    sync::{Mutex, Notify, Semaphore, mpsc},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::{
    activity::ActivityReporter,
    domain::{MessageId, SessionStatus, ThreadId, Turn, TurnStatus},
    omp::{AgentTransport, ApprovalRequest, OmpTurnError, OmpUi, TurnControl, TurnInput},
    render::chunk_message,
    store::Store,
};

const MAX_PENDING_MESSAGES: usize = 64;
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);

struct QueuedMessage {
    text: String,
    assistant: bool,
}

struct MessageDelivery {
    sender: mpsc::Sender<QueuedMessage>,
}

impl MessageDelivery {
    fn new() -> (Self, mpsc::Receiver<QueuedMessage>) {
        let (sender, receiver) = mpsc::channel(MAX_PENDING_MESSAGES);
        (Self { sender }, receiver)
    }

    fn message(&self, text: impl Into<String>) -> Result<(), OmpTurnError> {
        self.enqueue(text, true)
    }

    fn notice(&self, text: impl Into<String>) -> Result<(), OmpTurnError> {
        self.enqueue(text, false)
    }

    fn enqueue(&self, text: impl Into<String>, assistant: bool) -> Result<(), OmpTurnError> {
        let permit = self.sender.try_reserve().map_err(|error| {
            let reason = match error {
                mpsc::error::TrySendError::Full(_) => "queue is full",
                mpsc::error::TrySendError::Closed(_) => "worker stopped",
            };
            OmpTurnError::Uncertain(format!(
                "Discord delivery {reason}; completed message was not queued"
            ))
        })?;
        permit.send(QueuedMessage {
            text: text.into(),
            assistant,
        });
        Ok(())
    }
}

async fn deliver_messages(
    messenger: &dyn ThreadMessenger,
    thread_id: ThreadId,
    mut receiver: mpsc::Receiver<QueuedMessage>,
    message_limit: usize,
    send_timeout: Duration,
) {
    while let Some(message) = receiver.recv().await {
        for chunk in chunk_message(&message.text, message_limit) {
            let result = timeout(send_timeout, async {
                if message.assistant {
                    messenger.send_assistant(thread_id, &chunk).await
                } else {
                    messenger.send(thread_id, &chunk).await
                }
            })
            .await
            .context("Discord message send timed out")
            .and_then(|result| result);
            if let Err(error) = result {
                // Discord may have accepted this chunk. Never retry it or send
                // the rest of this message, but keep later independent messages.
                tracing::error!(
                    %thread_id,
                    %error,
                    "message delivery failed; delivery not retried"
                );
                break;
            }
        }
    }
}

#[async_trait]
pub trait ThreadMessenger: Send + Sync {
    async fn send(&self, thread_id: ThreadId, content: &str) -> Result<()>;
    async fn send_assistant(&self, thread_id: ThreadId, content: &str) -> Result<()>;
    async fn create_activity(
        &self,
        thread_id: ThreadId,
        content: &str,
        finished: bool,
    ) -> Result<MessageId>;
    async fn edit_activity(
        &self,
        thread_id: ThreadId,
        message_id: MessageId,
        content: &str,
        finished: bool,
    ) -> Result<MessageId>;
    /// Change the activity phase without replacing the current thread title.
    async fn activity_phase(&self, thread_id: ThreadId, label: &str) -> Result<()>;
    async fn confirm(&self, thread_id: ThreadId, request: ApprovalRequest) -> bool;
    async fn notify(&self, thread_id: ThreadId, message: &str);
    async fn channel_history(&self, thread_id: ThreadId, days: u16) -> Result<String>;
    async fn thread_participants(&self, thread_id: ThreadId) -> Result<String>;
    async fn thread_title(&self, thread_id: ThreadId, title: &str, status: &str) -> Result<String>;
    /// Change the status prefix without replacing the current thread title.
    async fn thread_status(&self, thread_id: ThreadId, status: &str) -> Result<()>;
}

pub struct ThreadQueueRunner {
    store: Store,
    transport: Arc<dyn AgentTransport>,
    messenger: Arc<dyn ThreadMessenger>,
    message_limit: usize,
    permits: Arc<Semaphore>,
    draining: Mutex<HashSet<ThreadId>>,
    active: Mutex<HashMap<ThreadId, ActiveRun>>,
    idle: Notify,
}

#[derive(Clone)]
struct ActiveRun {
    cancellation: CancellationToken,
    interrupts: Arc<Notify>,
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
        if let Some(active) = self.active.lock().await.get(&thread_id) {
            active.interrupts.notify_one();
        }
        if !self.draining.lock().await.insert(thread_id) {
            return;
        }
        let runner = Arc::clone(self);
        tokio::spawn(async move {
            'drain: loop {
                if let Err(error) = runner.drain(thread_id).await {
                    tracing::error!(thread_id = %thread_id, error = %error, "thread queue drain failed");
                    let _ = timeout(
                        DELIVERY_TIMEOUT,
                        runner
                            .messenger
                            .send(thread_id, &format!("Bridge queue failed: {error}")),
                    )
                    .await;
                }
                let mut completed = false;
                loop {
                    let mut draining = runner.draining.lock().await;
                    let remaining = async {
                        let session = runner.store.session(thread_id).await?;
                        let counts = runner.store.counts(thread_id).await?;
                        let idle = counts.queued == 0
                            && counts.running == 0
                            && !runner.active.lock().await.contains_key(&thread_id);
                        Ok::<_, anyhow::Error>((
                            session.is_some_and(|session| session.status == SessionStatus::Active)
                                && counts.queued > 0,
                            idle,
                        ))
                    }
                    .await;
                    match remaining {
                        Ok((true, _)) => continue 'drain,
                        Ok((false, idle)) if idle != completed => {
                            // Keep ownership, but never hold the global lock over HTTP.
                            // Recheck afterward: an arrival may have been poked while
                            // this drain still owned the thread.
                            drop(draining);
                            runner
                                .update_thread_status(
                                    thread_id,
                                    if idle { "completed" } else { "working" },
                                )
                                .await;
                            completed = idle;
                            continue;
                        }
                        Ok(_) => {}
                        Err(error) => {
                            tracing::error!(thread_id = %thread_id, error = %error, "failed to inspect remaining queue");
                        }
                    }
                    draining.remove(&thread_id);
                    runner.idle.notify_waiters();
                    break 'drain;
                }
            }
        });
    }

    pub async fn stop(&self, thread_id: ThreadId) -> Result<bool> {
        self.store
            .set_session_status(thread_id, SessionStatus::Stopped)
            .await?;
        let active = self.active.lock().await.get(&thread_id).cloned();
        if let Some(active) = active {
            active.cancellation.cancel();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn new_session(self: &Arc<Self>, thread_id: ThreadId) -> Result<u64> {
        if self.active.lock().await.contains_key(&thread_id) {
            bail!(
                "stop the active OMP turn and wait for it to finish before starting a new session"
            );
        }
        let discarded = self.store.reset_session(thread_id).await?;
        self.poke(thread_id).await;
        Ok(discarded)
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
        let usage = self.transport.usage().await.unwrap_or_else(|error| {
            tracing::warn!(%error, "provider usage query failed");
            "OpenAI/Codex usage unavailable; the provider query failed or timed out.".to_owned()
        });
        Ok(format!(
            "OMP session status: {}\nWorking directory: `{}`\nSession file: `{}`\nQueue: {} running, {} queued, {} uncertain\n\n{}",
            session.status.as_str(),
            session.cwd.display(),
            session_path,
            counts.running,
            counts.queued,
            counts.uncertain,
            usage
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

    async fn update_thread_status(&self, thread_id: ThreadId, status: &str) {
        let result = timeout(
            DELIVERY_TIMEOUT,
            self.messenger.thread_status(thread_id, status),
        )
        .await
        .context("thread status update timed out")
        .and_then(|result| result);
        if let Err(error) = result {
            tracing::warn!(%thread_id, %status, %error, "thread status update failed");
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
            // Empty threads must settle even while other threads hold every permit.
            if self.store.counts(thread_id).await?.queued == 0 {
                return Ok(());
            }

            let _permit = Arc::clone(&self.permits)
                .acquire_owned()
                .await
                .context("global turn semaphore closed")?;
            let Some(turn) = self.store.claim_next(thread_id).await? else {
                return Ok(());
            };
            let active = ActiveRun {
                cancellation: CancellationToken::new(),
                interrupts: Arc::new(Notify::new()),
            };
            self.active.lock().await.insert(thread_id, active.clone());
            self.update_thread_status(thread_id, "working").await;
            let result = self.process_turn(&turn, &active).await;
            self.active.lock().await.remove(&thread_id);
            if let Err(error) = result {
                tracing::error!(thread_id = %thread_id, turn_id = turn.id, error = %error, "turn processing failed");
                self.store
                    .finish_running_uncertain(thread_id, &error.to_string())
                    .await?;
            }
        }
    }

    async fn process_turn(&self, turn: &Turn, active: &ActiveRun) -> Result<()> {
        let session = self
            .store
            .session(turn.thread_id)
            .await?
            .context("claimed turn has no session mapping")?;
        if session.status == SessionStatus::Stopped {
            active.cancellation.cancel();
        }
        let activity = ActivityReporter::start(Arc::clone(&self.messenger), turn.thread_id);
        let (delivery, receiver) = MessageDelivery::new();
        let run = async {
            let ui = RunnerUi {
                messenger: Arc::clone(&self.messenger),
                thread_id: turn.thread_id,
                activity: &activity,
                delivery: &delivery,
            };
            let control = RunnerControl {
                runner: self,
                thread_id: turn.thread_id,
                cancellation: &active.cancellation,
                activity: &activity,
                delivery: &delivery,
                notified: Mutex::new(HashSet::new()),
            };
            let result = self
                .transport
                .run_turn(
                    &session,
                    TurnInput {
                        first: turn,
                        interrupts: Arc::clone(&active.interrupts),
                        control: &control,
                    },
                    active.cancellation.clone(),
                    &ui,
                )
                .await;
            let status = match &result {
                Ok(result) if result.aborted => "Stopped",
                Ok(_) => "OMP completed",
                Err(error) if error.is_uncertain() => "Outcome uncertain; inspect before retrying",
                Err(_) => "Failed",
            };
            let finalized = async {
                let Err(error) = &result else {
                    return Ok(());
                };
                // An input may have crossed the process boundary even if its
                // acknowledgement was lost. Never make it runnable again.
                self.store
                    .finish_running_uncertain(turn.thread_id, &error.to_string())
                    .await?;
                let mut after_id = turn.id - 1;
                while let Some(outcome) = self.store.next_outcome(turn.thread_id, after_id).await? {
                    after_id = outcome.id;
                    if control.notified.lock().await.contains(&outcome.id) {
                        continue;
                    }
                    if let Some(notice) =
                        outcome_notice(outcome.id, outcome.status, outcome.error.as_deref())?
                    {
                        // The RPC run has ended. Waiting for queue capacity here
                        // preserves unresolved failure notices even after overflow.
                        delivery
                            .sender
                            .send(QueuedMessage {
                                text: notice,
                                assistant: false,
                            })
                            .await
                            .context("delivery worker stopped")?;
                    }
                }
                Ok(())
            }
            .await;
            drop(delivery);
            (status, finalized)
        };
        // These futures share a lifetime: no delivery task can outlive the run.
        // Closing the producer drains every queued message before returning.
        let ((status, result), ()) = tokio::join!(
            run,
            deliver_messages(
                self.messenger.as_ref(),
                turn.thread_id,
                receiver,
                self.message_limit,
                DELIVERY_TIMEOUT,
            )
        );
        activity.finish(status).await;
        result
    }
}

struct RunnerControl<'a> {
    runner: &'a ThreadQueueRunner,
    thread_id: ThreadId,
    cancellation: &'a CancellationToken,
    activity: &'a ActivityReporter,
    delivery: &'a MessageDelivery,
    notified: Mutex<HashSet<i64>>,
}

#[async_trait]
impl TurnControl for RunnerControl<'_> {
    async fn next_turn(&self) -> Result<Option<Turn>, OmpTurnError> {
        if self.cancellation.is_cancelled() {
            return Ok(None);
        }
        let turn = self
            .runner
            .store
            .claim_next_interrupting(self.thread_id)
            .await
            .map_err(|error| OmpTurnError::Uncertain(error.to_string()))?;
        if turn.is_some() {
            self.runner
                .update_thread_status(self.thread_id, "working")
                .await;
        }
        Ok(turn)
    }

    async fn record_result(
        &self,
        turn_id: i64,
        status: TurnStatus,
        text: &str,
        error: Option<&str>,
    ) -> Result<(), OmpTurnError> {
        self.runner
            .store
            .finish_turn(turn_id, status, (!text.is_empty()).then_some(text), error)
            .await
            .map_err(|error| OmpTurnError::Uncertain(error.to_string()))?;
        if status == TurnStatus::Aborted && !self.cancellation.is_cancelled() {
            self.activity.update("Previous turn stopped; continuing");
        }
        if let Some(notice) = outcome_notice(turn_id, status, error)? {
            self.delivery.notice(notice)?;
            self.notified.lock().await.insert(turn_id);
        }
        Ok(())
    }

    async fn save_session(&self, path: &std::path::Path) -> Result<(), OmpTurnError> {
        self.runner
            .store
            .set_session_path(self.thread_id, path)
            .await
            .map_err(|error| OmpTurnError::Uncertain(error.to_string()))
    }
}

struct RunnerUi<'a> {
    messenger: Arc<dyn ThreadMessenger>,
    thread_id: ThreadId,
    activity: &'a ActivityReporter,
    delivery: &'a MessageDelivery,
}

#[async_trait]
impl OmpUi for RunnerUi<'_> {
    fn message(&self, text: &str) -> Result<(), OmpTurnError> {
        self.delivery.message(text)
    }

    fn activity(&self, status: &'static str) {
        self.activity.update(status);
    }

    async fn confirm(&self, request: ApprovalRequest) -> bool {
        self.activity.update("Waiting for your approval");
        let approved = self.messenger.confirm(self.thread_id, request).await;
        self.activity.update("Working");
        approved
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

    async fn thread_participants(&self) -> Result<String, String> {
        self.messenger
            .thread_participants(self.thread_id)
            .await
            .map_err(|error| format!("{error:#}"))
    }

    async fn thread_title(&self, title: &str, status: &str) -> Result<String, String> {
        self.messenger
            .thread_title(self.thread_id, title, status)
            .await
            .map_err(|error| format!("{error:#}"))
    }
}

fn outcome_notice(
    turn_id: i64,
    status: TurnStatus,
    error: Option<&str>,
) -> Result<Option<String>, OmpTurnError> {
    match status {
        TurnStatus::Done => Ok(None),
        TurnStatus::Aborted => Ok(Some(format!(
            "OMP turn {turn_id} interrupted or cancelled."
        ))),
        TurnStatus::Failed | TurnStatus::Uncertain => {
            let error = error.unwrap_or("unknown error").to_owned();
            let error = if status == TurnStatus::Uncertain {
                OmpTurnError::Uncertain(error)
            } else {
                OmpTurnError::Rejected(error)
            };
            Ok(Some(turn_error_message(&error)))
        }
        TurnStatus::Queued | TurnStatus::Running => Err(OmpTurnError::Protocol(
            "nonterminal turn returned for delivery".into(),
        )),
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
    use std::{path::PathBuf, sync::Arc, time::Duration};

    use tokio::sync::{Mutex, mpsc};

    use super::*;
    use crate::{
        domain::{ChannelId, GuildId, NewSession, UserId},
        omp::OmpTurnResult,
        store::{EnqueueResult, TurnSource},
    };

    const THREAD: ThreadId = ThreadId::new(10);

    struct ControlledTransport {
        observed: mpsc::UnboundedSender<i64>,
        recorded: Option<mpsc::UnboundedSender<i64>>,
        finish: Semaphore,
        lose_connection: bool,
        completed: Vec<String>,
        answer: String,
    }

    impl ControlledTransport {
        fn new(observed: mpsc::UnboundedSender<i64>, lose_connection: bool) -> Self {
            Self {
                observed,
                recorded: None,
                finish: Semaphore::new(0),
                lose_connection,
                completed: Vec::new(),
                answer: "finished answer".into(),
            }
        }

        fn publish_completed(&self, ui: &dyn OmpUi) -> Result<String, OmpTurnError> {
            for text in &self.completed {
                ui.message(text)?;
            }
            Ok(self.completed.join("\n\n"))
        }
    }

    #[async_trait]
    impl AgentTransport for ControlledTransport {
        async fn usage(&self) -> anyhow::Result<String> {
            Ok("OpenAI/Codex quota usage: Weekly: 20.0% used; resets <t:1795021679:F>".into())
        }

        async fn run_turn(
            &self,
            _session: &crate::domain::Session,
            input: TurnInput<'_>,
            cancellation: CancellationToken,
            ui: &dyn OmpUi,
        ) -> Result<OmpTurnResult, OmpTurnError> {
            let path = PathBuf::from("/tmp/fake-session.jsonl");
            input.control.save_session(&path).await?;
            let mut current = input.first.clone();
            let mut text = self.publish_completed(ui)?;
            self.observed.send(current.id).unwrap();
            loop {
                if let Some(next) = input.control.next_turn().await? {
                    if self.lose_connection {
                        self.observed.send(next.id).unwrap();
                        return Err(OmpTurnError::Uncertain("lost RPC after dispatch".into()));
                    }
                    if !text.is_empty() {
                        text.push_str("\n\n");
                    }
                    text.push_str("partial output must not be delivered");
                    input
                        .control
                        .record_result(
                            current.id,
                            TurnStatus::Aborted,
                            &text,
                            Some("interrupted by a newer message"),
                        )
                        .await?;
                    current = next;
                    text = self.publish_completed(ui)?;
                    self.observed.send(current.id).unwrap();
                    continue;
                }
                let aborted = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => true,
                    _ = input.interrupts.notified() => continue,
                    permit = self.finish.acquire() => {
                        permit.unwrap().forget();
                        false
                    }
                };
                if !aborted {
                    ui.message(&self.answer)?;
                    if !text.is_empty() {
                        text.push_str("\n\n");
                    }
                    text.push_str(&self.answer);
                }
                input
                    .control
                    .record_result(
                        current.id,
                        if aborted {
                            TurnStatus::Aborted
                        } else {
                            TurnStatus::Done
                        },
                        &text,
                        None,
                    )
                    .await?;
                if let Some(recorded) = &self.recorded {
                    recorded.send(current.id).unwrap();
                }
                return Ok(OmpTurnResult {
                    turn_id: current.id,
                    text,
                    session_path: path,
                    session_id: "fake".into(),
                    aborted,
                });
            }
        }
    }

    #[derive(Default)]
    struct FakeMessenger {
        sent: Mutex<Vec<String>>,
        attempts: Mutex<Vec<String>>,
        delivered: Option<mpsc::UnboundedSender<String>>,
        send_gate: Option<(mpsc::UnboundedSender<()>, Arc<Semaphore>)>,
        edit_gate: Option<(mpsc::UnboundedSender<()>, Arc<Semaphore>)>,
        fail_after: Mutex<Option<usize>>,
        statuses: Mutex<Vec<String>>,
        status_attempts: Mutex<Vec<String>>,
        status_gate: Option<(&'static str, mpsc::UnboundedSender<()>, Arc<Semaphore>)>,
        fail_status: bool,
    }

    #[async_trait]
    impl ThreadMessenger for FakeMessenger {
        async fn send_assistant(&self, thread_id: ThreadId, content: &str) -> Result<()> {
            self.send(thread_id, content).await
        }

        async fn send(&self, _thread_id: ThreadId, content: &str) -> Result<()> {
            self.attempts.lock().await.push(content.to_owned());
            if let Some((started, gate)) = &self.send_gate {
                started.send(())?;
                gate.acquire().await?.forget();
            }
            let mut fail_after = self.fail_after.lock().await;
            if let Some(remaining) = fail_after.as_mut() {
                if *remaining == 0 {
                    *fail_after = None;
                    bail!("injected Discord response-send failure");
                }
                *remaining -= 1;
            }
            self.sent.lock().await.push(content.to_owned());
            if let Some(delivered) = &self.delivered {
                delivered.send(content.to_owned())?;
            }
            Ok(())
        }

        async fn create_activity(
            &self,
            _thread_id: ThreadId,
            _content: &str,
            _finished: bool,
        ) -> Result<MessageId> {
            Ok(MessageId::new(1))
        }

        async fn edit_activity(
            &self,
            _thread_id: ThreadId,
            message_id: MessageId,
            _content: &str,
            _finished: bool,
        ) -> Result<MessageId> {
            if let Some((started, gate)) = &self.edit_gate {
                started.send(())?;
                gate.acquire().await?.forget();
            }
            Ok(message_id)
        }

        async fn activity_phase(&self, _thread_id: ThreadId, _label: &str) -> Result<()> {
            Ok(())
        }

        async fn confirm(&self, _thread_id: ThreadId, _request: ApprovalRequest) -> bool {
            false
        }

        async fn notify(&self, _thread_id: ThreadId, _message: &str) {}

        async fn channel_history(&self, _thread_id: ThreadId, _days: u16) -> Result<String> {
            Ok("[]".to_owned())
        }

        async fn thread_participants(&self, _thread_id: ThreadId) -> Result<String> {
            Ok("[]".to_owned())
        }

        async fn thread_title(
            &self,
            _thread_id: ThreadId,
            _title: &str,
            _status: &str,
        ) -> Result<String> {
            unreachable!("thread titles must only change through an explicit model tool call")
        }

        async fn thread_status(&self, _thread_id: ThreadId, status: &str) -> Result<()> {
            self.status_attempts.lock().await.push(status.to_owned());
            if let Some((blocked_status, started, gate)) = &self.status_gate
                && status == *blocked_status
            {
                started.send(())?;
                gate.acquire().await?.forget();
            }
            anyhow::ensure!(!self.fail_status, "injected Discord thread-status failure");
            self.statuses.lock().await.push(status.to_owned());
            Ok(())
        }
    }

    async fn fixture() -> (tempfile::TempDir, Store) {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(&directory.path().join("bridge.sqlite"))
            .await
            .unwrap();
        store
            .create_session(&NewSession {
                thread_id: THREAD,
                guild_id: GuildId::new(1),
                parent_channel_id: ChannelId::new(2),
                trigger_message_id: MessageId::new(3),
                cwd: directory.path().to_owned(),
                created_by_user_id: UserId::new(4),
            })
            .await
            .unwrap();
        (directory, store)
    }

    async fn enqueue(store: &Store, message_id: u64) -> i64 {
        match store
            .enqueue_turn(
                THREAD,
                MessageId::new(message_id),
                UserId::new(4),
                "instruction",
                TurnSource::Human,
            )
            .await
            .unwrap()
        {
            EnqueueResult::Enqueued(id) => id,
            EnqueueResult::Duplicate => panic!("unexpected duplicate"),
        }
    }

    async fn received<T>(receiver: &mut mpsc::UnboundedReceiver<T>) -> T {
        tokio::time::timeout(Duration::from_secs(3), receiver.recv())
            .await
            .expect("runner stalled")
            .expect("runner stopped")
    }

    async fn idle(runner: &ThreadQueueRunner) {
        tokio::time::timeout(Duration::from_secs(3), runner.wait_idle(THREAD))
            .await
            .expect("runner did not settle");
    }

    #[tokio::test]
    async fn idle_status_waits_for_messages_and_activity_and_new_work_restores_working() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let (recorded, mut outcomes) = mpsc::unbounded_channel();
        let mut transport = ControlledTransport::new(observed, false);
        transport.recorded = Some(recorded);
        let transport = Arc::new(transport);
        let (sending, mut sends) = mpsc::unbounded_channel();
        let send_gate = Arc::new(Semaphore::new(0));
        let (editing, mut edits) = mpsc::unbounded_channel();
        let edit_gate = Arc::new(Semaphore::new(0));
        let messenger = Arc::new(FakeMessenger {
            send_gate: Some((sending, send_gate.clone())),
            edit_gate: Some((editing, edit_gate.clone())),
            ..FakeMessenger::default()
        });
        let runner = ThreadQueueRunner::new(
            store.clone(),
            transport.clone(),
            messenger.clone(),
            1_900,
            1,
        );
        for message_id in [20, 21] {
            let turn = enqueue(&store, message_id).await;
            runner.poke(THREAD).await;
            assert_eq!(received(&mut incoming).await, turn);
            assert_eq!(messenger.statuses.lock().await.last().unwrap(), "working");
            transport.finish.add_permits(1);
            received(&mut sends).await;
            assert_eq!(received(&mut outcomes).await, turn);
            let counts = store.counts(THREAD).await.unwrap();
            assert_eq!((counts.running, counts.queued), (0, 0));
            assert_eq!(messenger.statuses.lock().await.last().unwrap(), "working");
            send_gate.add_permits(1);
            received(&mut edits).await;
            let counts = store.counts(THREAD).await.unwrap();
            assert_eq!((counts.running, counts.queued), (0, 0));
            assert!(runner.active.lock().await.contains_key(&THREAD));
            assert_eq!(messenger.statuses.lock().await.last().unwrap(), "working");
            edit_gate.add_permits(1);
            idle(&runner).await;
            assert_eq!(messenger.statuses.lock().await.last().unwrap(), "completed");
        }
        assert_eq!(
            *messenger.statuses.lock().await,
            vec!["working", "completed", "working", "completed"]
        );
    }

    #[tokio::test]
    async fn persisted_running_turn_suppresses_idle_without_an_active_transport() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let transport = Arc::new(ControlledTransport::new(observed, false));
        let messenger = Arc::new(FakeMessenger::default());
        let runner = ThreadQueueRunner::new(
            store.clone(),
            transport,
            messenger.clone(),
            1_900,
            1,
        );
        let turn = enqueue(&store, 20).await;
        assert_eq!(store.claim_next(THREAD).await.unwrap().unwrap().id, turn);
        runner.poke(THREAD).await;
        idle(&runner).await;
        assert_eq!(store.counts(THREAD).await.unwrap().running, 1);
        assert!(!runner.active.lock().await.contains_key(&THREAD));
        assert!(messenger.status_attempts.lock().await.is_empty());
        assert!(incoming.try_recv().is_err());
    }

    #[tokio::test]
    async fn arrival_during_idle_update_is_drained_and_restores_working() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let transport = Arc::new(ControlledTransport::new(observed, false));
        let (updating, mut updates) = mpsc::unbounded_channel();
        let gate = Arc::new(Semaphore::new(0));
        let messenger = Arc::new(FakeMessenger {
            status_gate: Some(("completed", updating, gate.clone())),
            ..FakeMessenger::default()
        });
        let runner = ThreadQueueRunner::new(
            store.clone(),
            transport.clone(),
            messenger.clone(),
            1_900,
            1,
        );
        let first = enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, first);
        transport.finish.add_permits(1);
        received(&mut updates).await;
        assert!(!runner.active.lock().await.contains_key(&THREAD));
        let second = enqueue(&store, 21).await;
        timeout(Duration::from_secs(3), runner.poke(THREAD))
            .await
            .expect("status HTTP held the draining mutex");
        assert_eq!(store.counts(THREAD).await.unwrap().queued, 1);
        gate.add_permits(1);
        assert_eq!(received(&mut incoming).await, second);
        assert_eq!(
            *messenger.statuses.lock().await,
            vec!["working", "completed", "working"]
        );
        transport.finish.add_permits(1);
        received(&mut updates).await;
        gate.add_permits(1);
        idle(&runner).await;
        assert_eq!(
            *messenger.statuses.lock().await,
            vec!["working", "completed", "working", "completed"]
        );
        let counts = store.counts(THREAD).await.unwrap();
        assert_eq!((counts.running, counts.queued), (0, 0));
    }

    #[tokio::test]
    async fn stopped_arrival_during_idle_update_is_busy_until_reset_discards_it() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let transport = Arc::new(ControlledTransport::new(observed, false));
        let (updating, mut updates) = mpsc::unbounded_channel();
        let gate = Arc::new(Semaphore::new(0));
        let messenger = Arc::new(FakeMessenger {
            status_gate: Some(("completed", updating, gate.clone())),
            ..FakeMessenger::default()
        });
        let runner = ThreadQueueRunner::new(
            store.clone(),
            transport.clone(),
            messenger.clone(),
            1_900,
            1,
        );
        let first = enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, first);
        transport.finish.add_permits(1);
        received(&mut updates).await;
        enqueue(&store, 21).await;
        assert!(!runner.stop(THREAD).await.unwrap());
        runner.poke(THREAD).await;
        gate.add_permits(1);
        idle(&runner).await;
        assert_eq!(
            *messenger.statuses.lock().await,
            vec!["working", "completed", "working"]
        );
        assert_eq!(store.counts(THREAD).await.unwrap().queued, 1);
        assert!(incoming.try_recv().is_err());

        let permit = runner.permits.acquire().await.unwrap();
        assert_eq!(runner.new_session(THREAD).await.unwrap(), 1);
        received(&mut updates).await;
        gate.add_permits(1);
        idle(&runner).await;
        drop(permit);
        assert_eq!(messenger.statuses.lock().await.last().unwrap(), "completed");
        assert_eq!(store.counts(THREAD).await.unwrap().queued, 0);
        assert!(incoming.try_recv().is_err());
    }

    #[tokio::test]
    async fn failed_thread_status_updates_never_fail_or_wedge_turns() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let transport = Arc::new(ControlledTransport::new(observed, false));
        let messenger = Arc::new(FakeMessenger {
            fail_status: true,
            ..FakeMessenger::default()
        });
        let runner = ThreadQueueRunner::new(
            store.clone(),
            transport.clone(),
            messenger.clone(),
            1_900,
            1,
        );
        for message_id in [20, 21] {
            let turn = enqueue(&store, message_id).await;
            runner.poke(THREAD).await;
            assert_eq!(received(&mut incoming).await, turn);
            transport.finish.add_permits(1);
            idle(&runner).await;
            assert_eq!(
                store.next_outcome(THREAD, turn - 1).await.unwrap().unwrap().status,
                TurnStatus::Done
            );
        }
        assert_eq!(
            *messenger.status_attempts.lock().await,
            vec!["working", "completed", "working", "completed"]
        );
        assert!(messenger.statuses.lock().await.is_empty());
        assert_eq!(
            *messenger.sent.lock().await,
            vec!["finished answer", "finished answer"]
        );
    }

    #[tokio::test]
    async fn timed_out_thread_status_updates_release_the_queue_without_retries() {
        for blocked_status in ["working", "completed"] {
            let (_directory, store) = fixture().await;
            let (observed, mut incoming) = mpsc::unbounded_channel();
            let transport = Arc::new(ControlledTransport::new(observed, false));
            let (updating, mut updates) = mpsc::unbounded_channel();
            let messenger = Arc::new(FakeMessenger {
                status_gate: Some((blocked_status, updating, Arc::new(Semaphore::new(0)))),
                ..FakeMessenger::default()
            });
            let runner = ThreadQueueRunner::new(
                store.clone(),
                transport.clone(),
                messenger.clone(),
                1_900,
                1,
            );
            for message_id in [20, 21] {
                let turn = enqueue(&store, message_id).await;
                runner.poke(THREAD).await;
                if blocked_status == "working" {
                    received(&mut updates).await;
                    tokio::time::pause();
                    tokio::time::advance(DELIVERY_TIMEOUT).await;
                    tokio::time::resume();
                }
                assert_eq!(received(&mut incoming).await, turn);
                transport.finish.add_permits(1);
                if blocked_status == "completed" {
                    received(&mut updates).await;
                    tokio::time::pause();
                    tokio::time::advance(DELIVERY_TIMEOUT).await;
                    tokio::time::resume();
                }
                idle(&runner).await;
                assert_eq!(
                    store.next_outcome(THREAD, turn - 1).await.unwrap().unwrap().status,
                    TurnStatus::Done
                );
            }
            assert_eq!(
                *messenger.status_attempts.lock().await,
                vec!["working", "completed", "working", "completed"]
            );
            assert_eq!(
                *messenger.sent.lock().await,
                vec!["finished answer", "finished answer"]
            );
        }
    }

    #[tokio::test]
    async fn completed_messages_arrive_while_transport_is_active_and_final_is_sent_once() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let mut transport = ControlledTransport::new(observed, false);
        transport.completed = vec!["Checking the code".into(), "Found the cause".into()];
        let transport = Arc::new(transport);
        let (delivered, mut messages) = mpsc::unbounded_channel();
        let messenger = Arc::new(FakeMessenger {
            delivered: Some(delivered),
            ..FakeMessenger::default()
        });
        let runner = ThreadQueueRunner::new(
            store.clone(),
            transport.clone(),
            messenger.clone(),
            1_900,
            1,
        );
        let first = enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, first);
        assert_eq!(received(&mut messages).await, "Checking the code");
        assert_eq!(received(&mut messages).await, "Found the cause");
        assert_eq!(store.counts(THREAD).await.unwrap().running, 1);
        assert!(store.next_outcome(THREAD, 0).await.unwrap().is_none());

        transport.finish.add_permits(1);
        idle(&runner).await;
        assert_eq!(received(&mut messages).await, "finished answer");
        assert!(messages.try_recv().is_err());
        let outcome = store.next_outcome(THREAD, 0).await.unwrap().unwrap();
        assert_eq!(outcome.status, TurnStatus::Done);
        assert_eq!(
            outcome.response.as_deref(),
            Some("Checking the code\n\nFound the cause\n\nfinished answer")
        );
        runner.poke(THREAD).await;
        idle(&runner).await;
        assert!(incoming.try_recv().is_err());
        assert_eq!(
            *messenger.sent.lock().await,
            vec!["Checking the code", "Found the cause", "finished answer"]
        );
    }

    #[tokio::test]
    async fn interruption_keeps_completed_messages_and_orders_notice_before_replacement() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let mut transport = ControlledTransport::new(observed, false);
        transport.completed = vec!["completed commentary".into()];
        let transport = Arc::new(transport);
        let (delivered, mut messages) = mpsc::unbounded_channel();
        let messenger = Arc::new(FakeMessenger {
            delivered: Some(delivered),
            ..FakeMessenger::default()
        });
        let runner = ThreadQueueRunner::new(
            store.clone(),
            transport.clone(),
            messenger.clone(),
            1_900,
            1,
        );
        let first = enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, first);
        assert_eq!(received(&mut messages).await, "completed commentary");
        let second = enqueue(&store, 21).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, second);
        let notice = format!("OMP turn {first} interrupted or cancelled.");
        assert_eq!(received(&mut messages).await, notice);
        let interrupted = store.next_outcome(THREAD, 0).await.unwrap().unwrap();
        assert_eq!(interrupted.status, TurnStatus::Aborted);
        assert_eq!(
            interrupted.response.as_deref(),
            Some("completed commentary\n\npartial output must not be delivered")
        );
        assert_eq!(received(&mut messages).await, "completed commentary");
        transport.finish.add_permits(1);
        idle(&runner).await;
        assert_eq!(
            *messenger.sent.lock().await,
            vec![
                "completed commentary".to_owned(),
                notice,
                "completed commentary".to_owned(),
                "finished answer".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn slow_discord_send_does_not_block_claiming_an_interrupting_prompt() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let mut transport = ControlledTransport::new(observed, false);
        transport.completed = vec!["completed commentary".into()];
        let transport = Arc::new(transport);
        let (started, mut sends) = mpsc::unbounded_channel();
        let gate = Arc::new(Semaphore::new(0));
        let messenger = Arc::new(FakeMessenger {
            send_gate: Some((started, gate.clone())),
            ..FakeMessenger::default()
        });
        let runner = ThreadQueueRunner::new(
            store.clone(),
            transport.clone(),
            messenger.clone(),
            1_900,
            1,
        );
        let first = enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, first);
        received(&mut sends).await;
        let second = enqueue(&store, 21).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, second);
        assert!(messenger.sent.lock().await.is_empty());
        assert_eq!(
            store.next_outcome(THREAD, 0).await.unwrap().unwrap().status,
            TurnStatus::Aborted
        );
        assert_eq!(store.counts(THREAD).await.unwrap().running, 1);

        transport.finish.add_permits(1);
        gate.add_permits(4);
        idle(&runner).await;
        assert_eq!(
            *messenger.sent.lock().await,
            vec![
                "completed commentary".to_owned(),
                format!("OMP turn {first} interrupted or cancelled."),
                "completed commentary".to_owned(),
                "finished answer".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn failed_message_skips_its_remaining_chunks_but_keeps_later_messages() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let long_message = "completed commentary ".repeat(20);
        let chunks = chunk_message(&long_message, 100);
        assert!(chunks.len() > 2);
        let mut transport = ControlledTransport::new(observed, false);
        transport.completed = vec![long_message.clone(), "independent message".into()];
        let transport = Arc::new(transport);
        let (delivered, mut messages) = mpsc::unbounded_channel();
        let messenger = Arc::new(FakeMessenger {
            delivered: Some(delivered),
            fail_after: Mutex::new(Some(1)),
            ..FakeMessenger::default()
        });
        let runner =
            ThreadQueueRunner::new(store.clone(), transport.clone(), messenger.clone(), 100, 1);
        let first = enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, first);
        assert_eq!(received(&mut messages).await, chunks[0]);
        assert_eq!(received(&mut messages).await, "independent message");
        assert_eq!(store.counts(THREAD).await.unwrap().running, 1);
        transport.finish.add_permits(1);
        idle(&runner).await;
        let expected_attempts = vec![
            chunks[0].clone(),
            chunks[1].clone(),
            "independent message".to_owned(),
            "finished answer".to_owned(),
        ];
        assert_eq!(*messenger.attempts.lock().await, expected_attempts);
        assert_eq!(
            *messenger.sent.lock().await,
            vec![
                chunks[0].clone(),
                "independent message".to_owned(),
                "finished answer".to_owned(),
            ]
        );
        let outcome = store.next_outcome(THREAD, 0).await.unwrap().unwrap();
        assert_eq!(outcome.status, TurnStatus::Done);
        assert_eq!(
            outcome.response,
            Some(format!(
                "{long_message}\n\nindependent message\n\nfinished answer"
            ))
        );
        runner.poke(THREAD).await;
        idle(&runner).await;
        assert!(incoming.try_recv().is_err());
        assert_eq!(*messenger.attempts.lock().await, expected_attempts);
    }

    #[tokio::test]
    async fn overflowing_delivery_retains_the_full_outcome_and_notifies_failure_once() {
        struct BurstTransport {
            snapshot: String,
        }

        #[async_trait]
        impl AgentTransport for BurstTransport {
            async fn usage(&self) -> anyhow::Result<String> {
                anyhow::bail!("usage unavailable in burst fixture")
            }

            async fn run_turn(
                &self,
                _session: &crate::domain::Session,
                input: TurnInput<'_>,
                _cancellation: CancellationToken,
                ui: &dyn OmpUi,
            ) -> Result<OmpTurnResult, OmpTurnError> {
                for _ in 0..MAX_PENDING_MESSAGES {
                    ui.message("completed message")?;
                }
                let error = ui
                    .message("not queued message")
                    .expect_err("burst exceeds the bounded delivery queue");
                input
                    .control
                    .record_result(
                        input.first.id,
                        TurnStatus::Uncertain,
                        &self.snapshot,
                        Some(&error.to_string()),
                    )
                    .await?;
                Err(error)
            }
        }

        let (_directory, store) = fixture().await;
        let mut completed = vec!["completed message"; MAX_PENDING_MESSAGES];
        completed.push("not queued message");
        let snapshot = completed.join("\n\n");
        let messenger = Arc::new(FakeMessenger::default());
        let runner = ThreadQueueRunner::new(
            store.clone(),
            Arc::new(BurstTransport {
                snapshot: snapshot.clone(),
            }),
            messenger.clone(),
            1_900,
            1,
        );
        enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        idle(&runner).await;
        let outcome = store.next_outcome(THREAD, 0).await.unwrap().unwrap();
        assert_eq!(outcome.status, TurnStatus::Uncertain);
        assert_eq!(outcome.response, Some(snapshot));
        assert!(outcome.error.unwrap().contains("queue is full"));
        let sent = messenger.sent.lock().await.clone();
        assert_eq!(sent.len(), MAX_PENDING_MESSAGES + 1);
        assert!(
            sent[..MAX_PENDING_MESSAGES]
                .iter()
                .all(|text| text == "completed message")
        );
        assert!(sent.last().unwrap().contains("queue is full"));
        runner.poke(THREAD).await;
        idle(&runner).await;
        assert_eq!(*messenger.sent.lock().await, sent);
    }

    #[test]
    fn delivery_queue_overflow_and_closed_worker_return_explicit_errors() {
        let (delivery, mut receiver) = MessageDelivery::new();
        for index in 0..MAX_PENDING_MESSAGES {
            delivery.message(index.to_string()).unwrap();
        }
        let error = delivery.message("overflow").unwrap_err();
        assert!(error.is_uncertain());
        assert!(error.to_string().contains("queue is full"));
        for index in 0..MAX_PENDING_MESSAGES {
            assert_eq!(receiver.try_recv().unwrap().text, index.to_string());
        }
        assert!(receiver.try_recv().is_err());
        drop(receiver);
        let error = delivery.message("closed").unwrap_err();
        assert!(error.to_string().contains("worker stopped"));
    }

    #[tokio::test(start_paused = true)]
    async fn timed_out_send_is_not_retried_and_worker_drains_later_messages() {
        let (started, mut sends) = mpsc::unbounded_channel();
        let gate = Arc::new(Semaphore::new(0));
        let messenger = FakeMessenger {
            send_gate: Some((started, gate.clone())),
            ..FakeMessenger::default()
        };
        let long_message = "slow completed message ".repeat(20);
        let chunks = chunk_message(&long_message, 100);
        assert!(chunks.len() > 1);
        let (delivery, receiver) = MessageDelivery::new();
        delivery.message(&long_message).unwrap();
        delivery.message("later message").unwrap();
        drop(delivery);
        let release = async {
            received(&mut sends).await;
            tokio::time::advance(DELIVERY_TIMEOUT).await;
            received(&mut sends).await;
            gate.add_permits(1);
        };
        tokio::join!(
            deliver_messages(&messenger, THREAD, receiver, 100, DELIVERY_TIMEOUT),
            release,
        );
        assert_eq!(
            *messenger.attempts.lock().await,
            vec![chunks[0].clone(), "later message".to_owned()]
        );
        assert_eq!(*messenger.sent.lock().await, vec!["later message"]);
    }

    #[tokio::test]
    async fn new_messages_interrupt_in_order_without_delivering_partial_answers() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let transport = Arc::new(ControlledTransport::new(observed, false));
        let messenger = Arc::new(FakeMessenger::default());
        let runner = ThreadQueueRunner::new(
            store.clone(),
            transport.clone(),
            messenger.clone(),
            1_900,
            1,
        );
        let first = enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, first);
        let second = enqueue(&store, 21).await;
        let third = enqueue(&store, 22).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, second);
        assert_eq!(received(&mut incoming).await, third);
        assert_eq!(
            store.next_outcome(THREAD, 0).await.unwrap().unwrap().status,
            TurnStatus::Aborted
        );
        assert_eq!(store.counts(THREAD).await.unwrap().running, 1);
        assert_eq!(
            *messenger.statuses.lock().await,
            vec!["working", "working", "working"]
        );
        transport.finish.add_permits(1);
        idle(&runner).await;
        assert_eq!(
            *messenger.statuses.lock().await,
            vec!["working", "working", "working", "completed"]
        );
        let final_outcome = store.next_outcome(THREAD, second).await.unwrap().unwrap();
        assert_eq!(final_outcome.id, third);
        assert_eq!(final_outcome.status, TurnStatus::Done);
        let sent = messenger.sent.lock().await;
        assert_eq!(
            sent.iter()
                .filter(|text| *text == "finished answer")
                .count(),
            1
        );
        assert!(sent.iter().all(|text| !text.contains("partial output")));
        assert_eq!(
            store
                .enqueue_turn(
                    THREAD,
                    MessageId::new(22),
                    UserId::new(4),
                    "duplicate",
                    TurnSource::Human
                )
                .await
                .unwrap(),
            EnqueueResult::Duplicate
        );
    }

    #[tokio::test]
    async fn connection_loss_marks_all_dispatched_inputs_uncertain_without_replay() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let transport = Arc::new(ControlledTransport::new(observed, true));
        let messenger = Arc::new(FakeMessenger::default());
        let runner = ThreadQueueRunner::new(store.clone(), transport, messenger.clone(), 1_900, 1);
        let first = enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, first);
        let second = enqueue(&store, 21).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, second);
        idle(&runner).await;
        let counts = store.counts(THREAD).await.unwrap();
        assert_eq!((counts.running, counts.queued, counts.uncertain), (0, 0, 2));
        assert_eq!(
            *messenger.statuses.lock().await,
            vec!["working", "working", "completed"]
        );
        let sent = messenger.sent.lock().await.clone();
        assert_eq!(sent.len(), 2);
        assert!(
            sent.iter()
                .all(|text| text.contains("not replayed automatically"))
        );
        runner.poke(THREAD).await;
        idle(&runner).await;
        assert!(incoming.try_recv().is_err());
        assert_eq!(*messenger.sent.lock().await, sent);
    }

    #[tokio::test]
    async fn message_arriving_during_final_delivery_is_not_lost() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let transport = Arc::new(ControlledTransport::new(observed, false));
        let (editing, mut edits) = mpsc::unbounded_channel();
        let gate = Arc::new(Semaphore::new(0));
        let messenger = Arc::new(FakeMessenger {
            edit_gate: Some((editing, gate.clone())),
            ..FakeMessenger::default()
        });
        let runner = ThreadQueueRunner::new(
            store.clone(),
            transport.clone(),
            messenger.clone(),
            1_900,
            1,
        );
        let first = enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, first);
        transport.finish.add_permits(1);
        received(&mut edits).await;
        let second = enqueue(&store, 21).await;
        runner.poke(THREAD).await;
        assert_eq!(store.counts(THREAD).await.unwrap().queued, 1);
        assert_eq!(*messenger.statuses.lock().await, vec!["working"]);
        gate.add_permits(1);
        assert_eq!(received(&mut incoming).await, second);
        assert_eq!(*messenger.statuses.lock().await, vec!["working", "working"]);
        transport.finish.add_permits(1);
        received(&mut edits).await;
        gate.add_permits(1);
        idle(&runner).await;
        assert_eq!(
            *messenger.statuses.lock().await,
            vec!["working", "working", "completed"]
        );
        assert_eq!(store.counts(THREAD).await.unwrap().running, 0);
        assert_eq!(
            *messenger.sent.lock().await,
            vec!["finished answer", "finished answer"]
        );
    }

    #[tokio::test]
    async fn stop_cancels_active_work_and_rejects_new_admissions() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let transport = Arc::new(ControlledTransport::new(observed, false));
        let messenger = Arc::new(FakeMessenger::default());
        let runner = ThreadQueueRunner::new(
            store.clone(),
            transport,
            messenger.clone(),
            1_900,
            1,
        );
        let first = enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, first);
        assert!(runner.stop(THREAD).await.unwrap());
        idle(&runner).await;
        assert_eq!(
            *messenger.statuses.lock().await,
            vec!["working", "completed"]
        );
        assert_eq!(
            store.next_outcome(THREAD, 0).await.unwrap().unwrap().status,
            TurnStatus::Aborted
        );
        assert!(
            store
                .enqueue_turn(
                    THREAD,
                    MessageId::new(21),
                    UserId::new(4),
                    "new",
                    TurnSource::Human
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn failed_interruption_notice_does_not_suppress_the_completed_replacement() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let transport = Arc::new(ControlledTransport::new(observed, false));
        let messenger = Arc::new(FakeMessenger {
            fail_after: Mutex::new(Some(0)),
            ..FakeMessenger::default()
        });
        let runner = ThreadQueueRunner::new(
            store.clone(),
            transport.clone(),
            messenger.clone(),
            1_900,
            1,
        );
        let first = enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, first);
        let second = enqueue(&store, 21).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, second);
        transport.finish.add_permits(1);
        idle(&runner).await;
        assert_eq!(*messenger.sent.lock().await, vec!["finished answer"]);
        assert_eq!(
            store.next_outcome(THREAD, 0).await.unwrap().unwrap().status,
            TurnStatus::Aborted
        );
        assert_eq!(
            store
                .next_outcome(THREAD, first)
                .await
                .unwrap()
                .unwrap()
                .status,
            TurnStatus::Done
        );
        runner.poke(THREAD).await;
        idle(&runner).await;
        assert!(incoming.try_recv().is_err());
        assert_eq!(*messenger.sent.lock().await, vec!["finished answer"]);
    }

    #[tokio::test]
    async fn failed_response_chunk_retains_complete_result_without_replaying_after_restart() {
        let (directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let answer = "complete response ".repeat(20);
        let mut transport = ControlledTransport::new(observed, false);
        transport.answer = answer.clone();
        let transport = Arc::new(transport);
        let messenger = Arc::new(FakeMessenger {
            fail_after: Mutex::new(Some(1)),
            ..FakeMessenger::default()
        });
        let runner =
            ThreadQueueRunner::new(store.clone(), transport.clone(), messenger.clone(), 100, 1);
        let first = enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, first);
        transport.finish.add_permits(1);
        idle(&runner).await;
        let chunks = chunk_message(&answer, 100);
        assert!(chunks.len() > 2);
        assert_eq!(*messenger.sent.lock().await, vec![chunks[0].clone()]);
        assert_eq!(*messenger.attempts.lock().await, chunks[..2]);
        drop(runner);
        drop(store);

        let reopened = Store::open(&directory.path().join("bridge.sqlite"))
            .await
            .unwrap();
        assert_eq!(reopened.recover_running().await.unwrap(), 0);
        let outcome = reopened.next_outcome(THREAD, 0).await.unwrap().unwrap();
        assert_eq!(outcome.status, TurnStatus::Done);
        assert_eq!(outcome.response.as_deref(), Some(answer.as_str()));
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let transport = Arc::new(ControlledTransport::new(observed, false));
        let restarted = ThreadQueueRunner::new(
            reopened.clone(),
            transport.clone(),
            messenger.clone(),
            1_900,
            1,
        );
        restarted.poke(THREAD).await;
        idle(&restarted).await;
        assert!(incoming.try_recv().is_err());
        assert_eq!(*messenger.sent.lock().await, vec![chunks[0].clone()]);
        let second = enqueue(&reopened, 21).await;
        restarted.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, second);
        transport.finish.add_permits(1);
        idle(&restarted).await;
        assert_eq!(
            *messenger.sent.lock().await,
            vec![chunks[0].clone(), "finished answer".to_owned()]
        );
    }

    #[tokio::test]
    async fn stop_while_waiting_for_capacity_preserves_queued_input_without_dispatch() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let transport = Arc::new(ControlledTransport::new(observed, false));
        let messenger = Arc::new(FakeMessenger::default());
        let runner = ThreadQueueRunner::new(
            store.clone(),
            transport,
            messenger.clone(),
            1_900,
            1,
        );
        let permit = runner.permits.acquire().await.unwrap();
        enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        assert!(!runner.stop(THREAD).await.unwrap());
        drop(permit);
        idle(&runner).await;
        runner.poke(THREAD).await;
        idle(&runner).await;
        assert!(incoming.try_recv().is_err());
        let counts = store.counts(THREAD).await.unwrap();
        assert_eq!((counts.queued, counts.running, counts.uncertain), (1, 0, 0));
        assert!(store.next_outcome(THREAD, 0).await.unwrap().is_none());
        assert!(messenger.status_attempts.lock().await.is_empty());
    }

    #[tokio::test]
    async fn status_shows_provider_quota_without_interrupting_active_work() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let transport = Arc::new(ControlledTransport::new(observed, false));
        let runner = ThreadQueueRunner::new(
            store.clone(),
            transport.clone(),
            Arc::new(FakeMessenger::default()),
            1_900,
            1,
        );
        let first = enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, first);
        let status = runner.status(THREAD).await.unwrap();
        assert!(status.contains("1 running, 0 queued, 0 uncertain"));
        assert!(status.contains("Weekly: 20.0% used; resets <t:1795021679:F>"));
        assert!(store.next_outcome(THREAD, 0).await.unwrap().is_none());
        assert!(incoming.try_recv().is_err());
        transport.finish.add_permits(1);
        idle(&runner).await;
        assert_eq!(
            store.next_outcome(THREAD, 0).await.unwrap().unwrap().status,
            TurnStatus::Done
        );
    }
}

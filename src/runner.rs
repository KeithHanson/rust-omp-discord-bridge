use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex as StdMutex, Weak},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde_json::Value;
use tokio::{
    sync::{Mutex, Notify, Semaphore, mpsc, watch},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::{
    activity::ActivityReporter,
    domain::{MessageId, SessionStatus, ThreadId, Turn, TurnStatus},
    omp::{AgentTransport, ApprovalRequest, OmpTurnError, OmpUi, TurnControl, TurnInput, UploadRequest},
    presence::{PresenceSnapshot, PresenceTracker, WorkingPresence},
    render::chunk_message,
    store::Store,
};

const MAX_PENDING_MESSAGES: usize = 64;
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);
const STATUS_REQUEST_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const STATUS_WORKER_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const STATUS_REVIEW_INTERVAL: Duration = Duration::from_secs(60);
const MAX_STATUS_ATTEMPTS: u32 = 5;

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
        // Parse/render the complete assistant message before Discord chunking:
        // a table may span many text-message limits.
        let parts = if message.assistant {
            match crate::table_images::prepare(&message.text, message_limit).await {
                Ok(parts) => parts,
                Err(error) => {
                    tracing::error!(%thread_id, %error, "assistant table preparation failed");
                    let notice = "Unable to render this response's table images. The original response remains in the saved session.";
                    if let Err(error) = timeout(send_timeout, messenger.send(thread_id, notice))
                        .await
                        .context("table error notice timed out")
                        .and_then(|result| result)
                    {
                        tracing::error!(%thread_id, %error, "table error notice failed");
                    }
                    continue;
                }
            }
        } else {
            chunk_message(&message.text, message_limit)
                .into_iter()
                .map(crate::table_images::AssistantPart::Text)
                .collect()
        };
        for part in parts {
            let result = timeout(send_timeout, async {
                match part {
                    crate::table_images::AssistantPart::Text(text) if message.assistant => {
                        messenger.send_assistant(thread_id, &text).await
                    }
                    crate::table_images::AssistantPart::Text(text) => {
                        messenger.send(thread_id, &text).await
                    }
                    crate::table_images::AssistantPart::Image { data, filename } => {
                        messenger.send_image(thread_id, data, filename).await
                    }
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
    async fn send_image(&self, thread_id: ThreadId, data: Vec<u8>, filename: String) -> Result<()>;
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
    async fn confirm(&self, thread_id: ThreadId, request: ApprovalRequest) -> bool;
    async fn prompt(&self, thread_id: ThreadId, request: Value) -> Value;
    async fn notify(&self, thread_id: ThreadId, message: &str);
    async fn channel_history(&self, thread_id: ThreadId, days: u16) -> Result<String>;
    async fn thread_participants(&self, thread_id: ThreadId) -> Result<String>;
    async fn upload_file(&self, thread_id: ThreadId, request: UploadRequest) -> Result<String>;
    /// Review a working session's topic/phase, or mark its current topic completed.
    async fn thread_status(&self, thread_id: ThreadId, status: &str) -> Result<()>;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ThreadStatus {
    Working,
    Completed,
}

impl ThreadStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Working => "working",
            Self::Completed => "completed",
        }
    }
}

type StatusSenders = StdMutex<HashMap<ThreadId, watch::Sender<ThreadStatus>>>;

struct ThreadStatusReconciler {
    messenger: Arc<dyn ThreadMessenger>,
    senders: Arc<StatusSenders>,
}

impl ThreadStatusReconciler {
    fn set(&self, thread_id: ThreadId, status: ThreadStatus) {
        let mut senders = self.senders.lock().expect("thread status registry poisoned");
        if let Some(sender) = senders.get(&thread_id) {
            sender.send_if_modified(|desired| {
                if *desired == status {
                    return false;
                }
                *desired = status;
                true
            });
            return;
        }
        let (sender, receiver) = watch::channel(status);
        senders.insert(thread_id, sender);
        tokio::spawn(reconcile_thread_status(
            Arc::clone(&self.messenger),
            Arc::downgrade(&self.senders),
            thread_id,
            receiver,
        ));
    }
}

async fn reconcile_thread_status(
    messenger: Arc<dyn ThreadMessenger>,
    senders: Weak<StatusSenders>,
    thread_id: ThreadId,
    mut desired: watch::Receiver<ThreadStatus>,
) {
    'latest: loop {
        let status = *desired.borrow_and_update();
        for attempt in 1..=MAX_STATUS_ATTEMPTS {
            let result = tokio::select! {
                biased;
                changed = desired.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    // Drop obsolete HTTP/rate-limit waits before applying the latest state.
                    continue 'latest;
                }
                result = timeout(
                    STATUS_REQUEST_TIMEOUT,
                    messenger.thread_status(thread_id, status.as_str()),
                ) => result.context("thread status update timed out").and_then(|result| result),
            };
            match result {
                Ok(()) => break,
                Err(error) => {
                    tracing::warn!(
                        %thread_id, status = status.as_str(), attempt, %error,
                        "thread status update failed"
                    );
                    if attempt == MAX_STATUS_ATTEMPTS {
                        tracing::error!(
                            %thread_id, status = status.as_str(),
                            "thread status retry batch exhausted"
                        );
                        break;
                    }
                }
            }
            tokio::select! {
                biased;
                changed = desired.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    continue 'latest;
                }
                _ = tokio::time::sleep(Duration::from_secs(1 << attempt)) => {}
            }
        }
        if status == ThreadStatus::Working {
            tokio::select! {
                biased;
                changed = desired.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                _ = tokio::time::sleep(STATUS_REVIEW_INTERVAL) => {}
            }
            continue;
        }
        match timeout(STATUS_WORKER_IDLE_TIMEOUT, desired.changed()).await {
            Ok(Ok(())) => continue,
            Ok(Err(_)) => return,
            Err(_) => {
                let Some(senders) = senders.upgrade() else {
                    return;
                };
                let mut senders = senders.lock().expect("thread status registry poisoned");
                // Removal and publication share this lock, so a concurrent arrival
                // either keeps this worker alive or starts its replacement.
                if desired.has_changed().unwrap_or(false) {
                    continue;
                }
                senders.remove(&thread_id);
                return;
            }
        }
    }
}

pub struct ThreadQueueRunner {
    store: Store,
    transport: Arc<dyn AgentTransport>,
    messenger: Arc<dyn ThreadMessenger>,
    message_limit: usize,
    permits: Arc<Semaphore>,
    draining: Mutex<HashMap<ThreadId, CancellationToken>>,
    active: StdMutex<HashMap<ThreadId, ActiveRun>>,
    presence: Arc<PresenceTracker>,
    statuses: ThreadStatusReconciler,
    idle: Notify,
}

#[derive(Clone)]
struct ActiveRun {
    cancellation: CancellationToken,
    interrupts: Arc<Notify>,
}

struct ActiveRunGuard<'a> {
    runner: &'a ThreadQueueRunner,
    thread_id: ThreadId,
}

impl Drop for ActiveRunGuard<'_> {
    fn drop(&mut self) {
        if let Some(active) = self.runner.active.lock().unwrap().remove(&self.thread_id) {
            active.cancellation.cancel();
        }
    }
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
            statuses: ThreadStatusReconciler {
                messenger: Arc::clone(&messenger),
                senders: Arc::new(StdMutex::new(HashMap::new())),
            },
            messenger,
            message_limit,
            permits: Arc::new(Semaphore::new(max_concurrency)),
            draining: Mutex::new(HashMap::new()),
            active: StdMutex::new(HashMap::new()),
            presence: Arc::new(PresenceTracker::default()),
            idle: Notify::new(),
        })
    }

    pub(crate) fn subscribe_presence(&self) -> watch::Receiver<PresenceSnapshot> {
        self.presence.subscribe()
    }

    /// Caller must exclude gateway handlers while inspecting this boundary.
    /// Drains include delivery and activity finalization; title workers are
    /// separate OMP processes and must also finish before replacing the daemon.
    pub(crate) async fn rollover_idle(&self) -> bool {
        self.draining.lock().await.is_empty()
            && self.statuses.senders.lock().unwrap().is_empty()
    }

    pub async fn poke(self: &Arc<Self>, thread_id: ThreadId) {
        match self.store.is_thread_excluded(thread_id).await {
            Ok(false) => {}
            Ok(true) => return,
            Err(error) => {
                tracing::error!(thread_id = %thread_id, error = %error, "failed to inspect thread exclusion");
                return;
            }
        }
        if let Some(active) = self.active.lock().unwrap().get(&thread_id) {
            active.interrupts.notify_one();
        }
        let cancellation = {
            let mut draining = self.draining.lock().await;
            if draining.contains_key(&thread_id) {
                return;
            }
            let cancellation = CancellationToken::new();
            draining.insert(thread_id, cancellation.clone());
            cancellation
        };
        let runner = Arc::clone(self);
        tokio::spawn(async move {
            // One engagement spans permit waits, replacement turns, delivery,
            // final activity edits, and the last queue recheck.
            let mut presence = None;
            let mut cancellation = cancellation;
            'drain: loop {
                if let Err(error) = runner.drain(thread_id, &cancellation, &mut presence).await {
                    tracing::error!(thread_id = %thread_id, error = %error, "thread queue drain failed");
                    let _ = timeout(
                        DELIVERY_TIMEOUT,
                        runner
                            .messenger
                            .send(thread_id, &format!("Bridge queue failed: {error}")),
                    )
                    .await;
                }
                let mut draining = runner.draining.lock().await;
                let remaining = async {
                    if runner.store.is_thread_excluded(thread_id).await? {
                        return Ok(None);
                    }
                    let session = runner.store.session(thread_id).await?;
                    let counts = runner.store.counts(thread_id).await?;
                    let idle = counts.queued == 0
                        && counts.running == 0
                        && !runner.active.lock().unwrap().contains_key(&thread_id);
                    Ok::<_, anyhow::Error>(Some((
                        session.is_some_and(|session| session.status == SessionStatus::Active)
                            && counts.queued > 0,
                        idle,
                    )))
                }
                .await;
                match remaining {
                    Ok(Some((true, _))) => {
                        // Reset may make new work runnable before a stopped drain settles.
                        if cancellation.is_cancelled() {
                            cancellation = CancellationToken::new();
                            draining.insert(thread_id, cancellation.clone());
                        }
                        continue 'drain;
                    }
                    Ok(Some((false, idle))) => runner.statuses.set(
                        thread_id,
                        if idle {
                            ThreadStatus::Completed
                        } else {
                            ThreadStatus::Working
                        },
                    ),
                    Ok(None) => {}
                    Err(error) => {
                        tracing::error!(thread_id = %thread_id, error = %error, "failed to inspect remaining queue");
                    }
                }
                draining.remove(&thread_id);
                drop(presence);
                runner.idle.notify_waiters();
                break;
            }
        });
    }

    pub async fn stop(&self, thread_id: ThreadId) -> Result<bool> {
        if !self.store.is_thread_excluded(thread_id).await? {
            self.store
                .set_session_status(thread_id, SessionStatus::Stopped)
                .await?;
        }
        if let Some(cancellation) = self.draining.lock().await.get(&thread_id) {
            cancellation.cancel();
        }
        let active = self.active.lock().unwrap().get(&thread_id).cloned();
        if let Some(active) = active {
            active.cancellation.cancel();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn new_session(self: &Arc<Self>, thread_id: ThreadId) -> Result<u64> {
        if self.store.is_thread_excluded(thread_id).await? {
            bail!("the bot has been kicked from this thread");
        }
        if self.active.lock().unwrap().contains_key(&thread_id) {
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

    /// Wait for turn execution and message/activity delivery, not title reconciliation.
    pub async fn wait_idle(&self, thread_id: ThreadId) {
        loop {
            let notified = self.idle.notified();
            if !self.draining.lock().await.contains_key(&thread_id) {
                return;
            }
            notified.await;
        }
    }

    async fn drain(
        self: &Arc<Self>,
        thread_id: ThreadId,
        cancellation: &CancellationToken,
        presence: &mut Option<WorkingPresence>,
    ) -> Result<()> {
        loop {
            if cancellation.is_cancelled() || self.store.is_thread_excluded(thread_id).await? {
                return Ok(());
            }
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
            self.statuses.set(thread_id, ThreadStatus::Working);
            let presence = presence.get_or_insert_with(|| self.presence.engage(thread_id));

            let _permit = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(()),
                permit = Arc::clone(&self.permits).acquire_owned() =>
                    permit.context("global turn semaphore closed")?,
            };
            let Some(turn) = self.store.claim_next(thread_id).await? else {
                return Ok(());
            };
            let active = ActiveRun {
                // Stop may cancel the drain after a claim but before active insertion.
                cancellation: cancellation.child_token(),
                interrupts: Arc::new(Notify::new()),
            };
            self.active.lock().unwrap().insert(thread_id, active.clone());
            let active_guard = ActiveRunGuard { runner: self, thread_id };
            let result = self.process_turn(&turn, &active, presence).await;
            drop(active_guard);
            if let Err(error) = result {
                tracing::error!(thread_id = %thread_id, turn_id = turn.id, error = %error, "turn processing failed");
                self.store
                    .finish_running_uncertain(thread_id, &error.to_string())
                    .await?;
            }
        }
    }

    async fn process_turn(
        &self,
        turn: &Turn,
        active: &ActiveRun,
        presence: &WorkingPresence,
    ) -> Result<()> {
        let session = self
            .store
            .session(turn.thread_id)
            .await?
            .context("claimed turn has no session mapping")?;
        if session.status == SessionStatus::Stopped
            || self.store.is_thread_excluded(turn.thread_id).await?
        {
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
                presence,
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
        self.runner
            .store
            .claim_next_interrupting(self.thread_id)
            .await
            .map_err(|error| OmpTurnError::Uncertain(error.to_string()))
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
    presence: &'a WorkingPresence,
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
        let _approval = self.presence.awaiting_approval();
        self.activity.update("Waiting for your approval");
        let approved = self.messenger.confirm(self.thread_id, request).await;
        self.activity.update("Working");
        approved
    }

    async fn prompt(&self, request: Value) -> Value {
        let _waiting = self.presence.awaiting_approval();
        self.activity.update("Waiting for your answer");
        let response = self.messenger.prompt(self.thread_id, request).await;
        self.activity.update("Working");
        response
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

    async fn upload_file(&self, request: UploadRequest) -> Result<String, String> {
        self.messenger
            .upload_file(self.thread_id, request)
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
    #[tokio::test]
    #[ignore = "requires uv and system Chromium; exercises the real table renderer"]
    async fn table_images_are_prepared_before_message_chunking() {
        let messenger = FakeMessenger::default();
        let (delivery, receiver) = MessageDelivery::new();
        let text = format!(
            "Before table.\n\n| Item | Description |\n| --- | --- |\n| Example | {} |\n\nAfter table.",
            "Long but readable description. ".repeat(8),
        );
        delivery.message(text).unwrap();
        drop(delivery);
        deliver_messages(&messenger, ThreadId::new(3), receiver, 100, DELIVERY_TIMEOUT).await;
        let sent = messenger.sent.lock().await;
        assert_eq!(sent.len(), 3);
        assert_eq!(sent[0].trim(), "Before table.");
        assert!(sent[1].starts_with("[image: "));
        assert_eq!(sent[2].trim(), "After table.");
    }

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
        waiting: Option<mpsc::UnboundedSender<i64>>,
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
                waiting: None,
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
                if let Some(waiting) = &self.waiting {
                    waiting.send(current.id).unwrap();
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
        approval_gate: Option<(mpsc::UnboundedSender<()>, Arc<Semaphore>)>,
        prompts: Mutex<Vec<(ThreadId, Value)>>,
        prompt_response: Option<Value>,
        edit_gate: Option<(mpsc::UnboundedSender<()>, Arc<Semaphore>)>,
        fail_after: Mutex<Option<usize>>,
        statuses: Mutex<Vec<String>>,
        status_attempts: Mutex<Vec<String>>,
        status_changed: Notify,
        status_attempted: Notify,
        status_gate: Option<(&'static str, mpsc::UnboundedSender<()>, Arc<Semaphore>)>,
        fail_status: std::sync::atomic::AtomicBool,
    }

    impl FakeMessenger {
        async fn wait_status(&self, expected: &str) {
            timeout(Duration::from_secs(3), async {
                loop {
                    let changed = self.status_changed.notified();
                    if self
                        .statuses
                        .lock()
                        .await
                        .last()
                        .is_some_and(|status| status == expected)
                    {
                        return;
                    }
                    changed.await;
                }
            })
            .await
            .expect("thread status was not applied");
        }

        async fn wait_status_attempts(&self, count: usize) {
            timeout(Duration::from_secs(3), async {
                loop {
                    let attempted = self.status_attempted.notified();
                    if self.status_attempts.lock().await.len() >= count {
                        return;
                    }
                    attempted.await;
                }
            })
            .await
            .expect("thread status was not attempted");
        }

        async fn wait_status_attempt(&self, expected: &str) {
            timeout(Duration::from_secs(3), async {
                loop {
                    let attempted = self.status_attempted.notified();
                    if self
                        .status_attempts
                        .lock()
                        .await
                        .last()
                        .is_some_and(|status| status == expected)
                    {
                        return;
                    }
                    attempted.await;
                }
            })
            .await
            .expect("desired thread status was not attempted");
        }
    }

    #[async_trait]
    impl ThreadMessenger for FakeMessenger {
        async fn send_assistant(&self, thread_id: ThreadId, content: &str) -> Result<()> {
            self.send(thread_id, content).await
        }

        async fn send_image(&self, thread_id: ThreadId, _data: Vec<u8>, filename: String) -> Result<()> {
            self.send(thread_id, &format!("[image: {filename}]")).await
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

        async fn confirm(&self, _thread_id: ThreadId, _request: ApprovalRequest) -> bool {
            if let Some((started, gate)) = &self.approval_gate {
                started.send(()).unwrap();
                gate.acquire().await.unwrap().forget();
            }
            false
        }

        async fn prompt(&self, thread_id: ThreadId, request: Value) -> Value {
            self.prompts.lock().await.push((thread_id, request));
            if let Some((started, gate)) = &self.approval_gate {
                started.send(()).unwrap();
                gate.acquire().await.unwrap().forget();
            }
            self.prompt_response.clone().expect("unexpected test prompt")
        }

        async fn notify(&self, _thread_id: ThreadId, _message: &str) {}

        async fn channel_history(&self, _thread_id: ThreadId, _days: u16) -> Result<String> {
            Ok("[]".to_owned())
        }

        async fn thread_participants(&self, _thread_id: ThreadId) -> Result<String> {
            Ok("[]".to_owned())
        }

        async fn upload_file(&self, _thread_id: ThreadId, _request: UploadRequest) -> Result<String> {
            panic!("unexpected file upload")
        }

        async fn thread_status(&self, _thread_id: ThreadId, status: &str) -> Result<()> {
            self.status_attempts.lock().await.push(status.to_owned());
            self.status_attempted.notify_waiters();
            if let Some((blocked_status, started, gate)) = &self.status_gate
                && status == *blocked_status
            {
                started.send(())?;
                gate.acquire().await?.forget();
            }
            anyhow::ensure!(
                !self.fail_status.load(std::sync::atomic::Ordering::Relaxed),
                "injected Discord thread-status failure"
            );
            self.statuses.lock().await.push(status.to_owned());
            self.status_changed.notify_waiters();
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
    async fn approvals_count_once_and_restore_working_on_return_and_drop() {
        let tracker = Arc::new(PresenceTracker::default());
        let snapshots = tracker.subscribe();
        let working = tracker.engage(THREAD);
        let other = tracker.engage(ThreadId::new(11));
        let (started, mut requests) = mpsc::unbounded_channel();
        let gate = Arc::new(Semaphore::new(0));
        let messenger = Arc::new(FakeMessenger {
            approval_gate: Some((started, gate.clone())),
            ..FakeMessenger::default()
        });
        let activity = ActivityReporter::start(messenger.clone(), THREAD);
        let (delivery, _receiver) = MessageDelivery::new();
        let ui = RunnerUi {
            messenger,
            thread_id: THREAD,
            activity: &activity,
            delivery: &delivery,
            presence: &working,
        };
        let request = ApprovalRequest { title: "Allow?".into(), details: "test".into() };
        let mut first = Box::pin(ui.confirm(request.clone()));
        let mut second = Box::pin(ui.confirm(request));
        tokio::select! {
            biased;
            _ = &mut first => panic!("approval unexpectedly completed"),
            _ = &mut second => panic!("approval unexpectedly completed"),
            _ = std::future::ready(()) => {}
        }
        received(&mut requests).await;
        received(&mut requests).await;
        assert_eq!(*snapshots.borrow(), PresenceSnapshot { working: 1, awaiting_approval: 1 });
        drop(first);
        assert_eq!(*snapshots.borrow(), PresenceSnapshot { working: 1, awaiting_approval: 1 });
        gate.add_permits(1);
        assert!(!second.await);
        assert_eq!(*snapshots.borrow(), PresenceSnapshot { working: 2, awaiting_approval: 0 });
        let mut cancelled = Box::pin(ui.confirm(ApprovalRequest {
            title: "Cancel?".into(),
            details: String::new(),
        }));
        tokio::select! {
            biased;
            _ = &mut cancelled => panic!("approval unexpectedly completed"),
            _ = std::future::ready(()) => {}
        }
        assert_eq!(snapshots.borrow().awaiting_approval, 1);
        drop(cancelled);
        assert_eq!(*snapshots.borrow(), PresenceSnapshot { working: 2, awaiting_approval: 0 });
        drop(working);
        drop(other);
        assert_eq!(*snapshots.borrow(), PresenceSnapshot::default());
        activity.finish("Finished").await;
    }

    #[tokio::test]
    async fn prompts_forward_exact_frames_and_restore_presence_on_return_and_drop() {
        let tracker = Arc::new(PresenceTracker::default());
        let snapshots = tracker.subscribe();
        let working = tracker.engage(THREAD);
        let (started, mut requests) = mpsc::unbounded_channel();
        let gate = Arc::new(Semaphore::new(0));
        let response = serde_json::json!({
            "type": "extension_ui_response", "id": "form-field", "value": "answer",
        });
        let messenger = Arc::new(FakeMessenger {
            approval_gate: Some((started, gate.clone())),
            prompt_response: Some(response.clone()),
            ..FakeMessenger::default()
        });
        let activity = ActivityReporter::start(messenger.clone(), THREAD);
        let (delivery, _receiver) = MessageDelivery::new();
        let ui = RunnerUi {
            messenger: messenger.clone(),
            thread_id: THREAD,
            activity: &activity,
            delivery: &delivery,
            presence: &working,
        };
        let request = serde_json::json!({
            "type": "extension_ui_request", "id": "form-field", "method": "input",
            "title": "Your answer", "placeholder": "Type here",
        });
        let mut prompt = Box::pin(ui.prompt(request.clone()));
        tokio::select! {
            biased;
            _ = &mut prompt => panic!("prompt unexpectedly completed"),
            _ = std::future::ready(()) => {}
        }
        received(&mut requests).await;
        assert_eq!(*messenger.prompts.lock().await, vec![(THREAD, request.clone())]);
        assert_eq!(*snapshots.borrow(), PresenceSnapshot { working: 0, awaiting_approval: 1 });
        gate.add_permits(1);
        assert_eq!(prompt.await, response);
        assert_eq!(*snapshots.borrow(), PresenceSnapshot { working: 1, awaiting_approval: 0 });
        let mut cancelled = Box::pin(ui.prompt(request));
        tokio::select! {
            biased;
            _ = &mut cancelled => panic!("prompt unexpectedly completed"),
            _ = std::future::ready(()) => {}
        }
        assert_eq!(snapshots.borrow().awaiting_approval, 1);
        drop(cancelled);
        assert_eq!(*snapshots.borrow(), PresenceSnapshot { working: 1, awaiting_approval: 0 });
        activity.finish("Finished").await;
    }

    #[tokio::test]
    async fn aborting_a_drain_cleans_up_active_run_presence_and_capacity() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let runner = ThreadQueueRunner::new(
            store.clone(),
            Arc::new(ControlledTransport::new(observed, false)),
            Arc::new(FakeMessenger::default()),
            1_900,
            1,
        );
        let turn = enqueue(&store, 20).await;
        let snapshots = runner.subscribe_presence();
        let task_runner = runner.clone();
        let task = tokio::spawn(async move {
            let mut presence = None;
            task_runner.drain(THREAD, &CancellationToken::new(), &mut presence).await
        });
        assert_eq!(received(&mut incoming).await, turn);
        assert_eq!(snapshots.borrow().working, 1);
        let cancellation = runner.active.lock().unwrap().get(&THREAD).unwrap().cancellation.clone();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(*snapshots.borrow(), PresenceSnapshot::default());
        assert!(runner.active.lock().unwrap().is_empty());
        assert!(cancellation.is_cancelled());
        assert_eq!(runner.permits.available_permits(), 1);
    }

    #[tokio::test]
    async fn drain_cancellation_reaches_an_active_turn() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let runner = ThreadQueueRunner::new(
            store.clone(),
            Arc::new(ControlledTransport::new(observed, false)),
            Arc::new(FakeMessenger::default()),
            1_900,
            1,
        );
        let turn = enqueue(&store, 20).await;
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task_runner = runner.clone();
        let task = tokio::spawn(async move {
            let mut presence = None;
            task_runner.drain(THREAD, &task_cancellation, &mut presence).await
        });
        assert_eq!(received(&mut incoming).await, turn);
        let active = runner.active.lock().unwrap().get(&THREAD).unwrap().cancellation.clone();

        cancellation.cancel();

        assert!(active.is_cancelled());
        timeout(Duration::from_secs(3), task).await.unwrap().unwrap().unwrap();
        assert!(runner.active.lock().unwrap().is_empty());
        assert_eq!(runner.permits.available_permits(), 1);
        assert_eq!(
            store.next_outcome(THREAD, 0).await.unwrap().unwrap().status,
            TurnStatus::Aborted
        );
    }

    #[tokio::test]
    async fn concurrent_threads_settle_independently() {
        let (directory, store) = fixture().await;
        let other = ThreadId::new(11);
        store.create_session(&NewSession {
            thread_id: other,
            guild_id: GuildId::new(1),
            parent_channel_id: ChannelId::new(2),
            trigger_message_id: MessageId::new(30),
            cwd: directory.path().to_owned(),
            created_by_user_id: UserId::new(4),
        }).await.unwrap();
        enqueue(&store, 20).await;
        store.enqueue_turn(other, MessageId::new(21), UserId::new(4), "other", TurnSource::Human)
            .await.unwrap();
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let runner = ThreadQueueRunner::new(
            store,
            Arc::new(ControlledTransport::new(observed, false)),
            Arc::new(FakeMessenger::default()),
            1_900,
            2,
        );
        let snapshots = runner.subscribe_presence();
        runner.poke(THREAD).await;
        runner.poke(other).await;
        received(&mut incoming).await;
        received(&mut incoming).await;
        assert_eq!(*snapshots.borrow(), PresenceSnapshot { working: 2, awaiting_approval: 0 });
        assert!(runner.stop(THREAD).await.unwrap());
        idle(&runner).await;
        assert_eq!(*snapshots.borrow(), PresenceSnapshot { working: 1, awaiting_approval: 0 });
        assert!(runner.stop(other).await.unwrap());
        timeout(Duration::from_secs(3), runner.wait_idle(other)).await.unwrap();
        assert_eq!(*snapshots.borrow(), PresenceSnapshot::default());
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
        let snapshots = runner.subscribe_presence();
        for message_id in [20, 21] {
            let turn = enqueue(&store, message_id).await;
            runner.poke(THREAD).await;
            assert_eq!(received(&mut incoming).await, turn);
            messenger.wait_status("working").await;
            assert_eq!(*snapshots.borrow(), PresenceSnapshot { working: 1, awaiting_approval: 0 });
            transport.finish.add_permits(1);
            received(&mut sends).await;
            assert_eq!(received(&mut outcomes).await, turn);
            let counts = store.counts(THREAD).await.unwrap();
            assert_eq!((counts.running, counts.queued), (0, 0));
            assert_eq!(messenger.statuses.lock().await.last().unwrap(), "working");
            assert_eq!(*snapshots.borrow(), PresenceSnapshot { working: 1, awaiting_approval: 0 });
            send_gate.add_permits(1);
            received(&mut edits).await;
            let counts = store.counts(THREAD).await.unwrap();
            assert_eq!((counts.running, counts.queued), (0, 0));
            assert!(runner.active.lock().unwrap().contains_key(&THREAD));
            assert_eq!(messenger.statuses.lock().await.last().unwrap(), "working");
            assert_eq!(*snapshots.borrow(), PresenceSnapshot { working: 1, awaiting_approval: 0 });
            edit_gate.add_permits(1);
            idle(&runner).await;
            assert_eq!(*snapshots.borrow(), PresenceSnapshot::default());
            messenger.wait_status("completed").await;
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
        assert!(!runner.active.lock().unwrap().contains_key(&THREAD));
        messenger.wait_status("working").await;
        assert!(incoming.try_recv().is_err());
    }

    #[tokio::test]
    async fn pending_completion_is_superseded_by_new_work_without_blocking_turns() {
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
        messenger.wait_status("working").await;
        transport.finish.add_permits(1);
        received(&mut updates).await;
        idle(&runner).await;
        let second = enqueue(&store, 21).await;
        timeout(Duration::from_secs(3), runner.poke(THREAD))
            .await
            .expect("status HTTP held the draining mutex");
        assert_eq!(received(&mut incoming).await, second);
        messenger.wait_status_attempts(3).await;
        messenger.wait_status("working").await;
        // Making the obsolete request ready cannot let completed win again.
        gate.add_permits(1);
        assert!(messenger.statuses.lock().await.iter().all(|status| status == "working"));
        transport.finish.add_permits(1);
        received(&mut updates).await;
        idle(&runner).await;
        messenger.wait_status("completed").await;
        assert_eq!(
            messenger.statuses.lock().await.iter().filter(|status| *status == "completed").count(),
            1
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
        messenger.wait_status("working").await;
        transport.finish.add_permits(1);
        received(&mut updates).await;
        enqueue(&store, 21).await;
        assert!(!runner.stop(THREAD).await.unwrap());
        runner.poke(THREAD).await;
        idle(&runner).await;
        messenger.wait_status_attempts(3).await;
        messenger.wait_status("working").await;
        assert!(messenger.statuses.lock().await.iter().all(|status| status == "working"));
        assert_eq!(store.counts(THREAD).await.unwrap().queued, 1);
        assert!(incoming.try_recv().is_err());

        let permit = runner.permits.acquire().await.unwrap();
        assert_eq!(runner.new_session(THREAD).await.unwrap(), 1);
        received(&mut updates).await;
        gate.add_permits(1);
        idle(&runner).await;
        drop(permit);
        messenger.wait_status("completed").await;
        assert_eq!(store.counts(THREAD).await.unwrap().queued, 0);
        assert!(incoming.try_recv().is_err());
    }

    #[tokio::test]
    async fn title_failures_do_not_block_turns_or_lose_later_completion() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let transport = Arc::new(ControlledTransport::new(observed, false));
        let messenger = Arc::new(FakeMessenger {
            fail_status: true.into(),
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
        messenger.wait_status_attempt("working").await;
        transport.finish.add_permits(1);
        idle(&runner).await;
        messenger.wait_status_attempt("completed").await;
        assert!(messenger.statuses.lock().await.is_empty());

        let second = enqueue(&store, 21).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, second);
        messenger.wait_status_attempt("working").await;
        messenger
            .fail_status
            .store(false, std::sync::atomic::Ordering::Relaxed);
        transport.finish.add_permits(1);
        idle(&runner).await;
        messenger.wait_status("completed").await;
        for turn in [first, second] {
            assert_eq!(
                store.next_outcome(THREAD, turn - 1).await.unwrap().unwrap().status,
                TurnStatus::Done
            );
        }
        assert_eq!(
            *messenger.sent.lock().await,
            vec!["finished answer", "finished answer"]
        );
    }

    #[tokio::test]
    async fn slow_completion_survives_the_old_ten_second_timeout() {
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
        let turn = enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, turn);
        messenger.wait_status("working").await;
        transport.finish.add_permits(1);
        received(&mut updates).await;
        idle(&runner).await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::time::resume();
        assert_eq!(*messenger.statuses.lock().await, vec!["working"]);
        gate.add_permits(1);
        messenger.wait_status("completed").await;
        assert_eq!(
            *messenger.status_attempts.lock().await,
            vec!["working", "completed"]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn failed_completion_retries_without_a_new_turn() {
        let messenger = Arc::new(FakeMessenger {
            fail_status: true.into(),
            ..FakeMessenger::default()
        });
        let statuses = ThreadStatusReconciler {
            messenger: messenger.clone(),
            senders: Arc::new(StdMutex::new(HashMap::new())),
        };
        statuses.set(THREAD, ThreadStatus::Completed);
        messenger.wait_status_attempts(1).await;
        messenger
            .fail_status
            .store(false, std::sync::atomic::Ordering::Relaxed);
        tokio::time::advance(Duration::from_secs(2)).await;
        messenger.wait_status("completed").await;
        assert_eq!(
            *messenger.status_attempts.lock().await,
            vec!["completed", "completed"]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn status_requests_have_a_long_bound_and_retry_exhaustion_is_finite() {
        let (updating, mut updates) = mpsc::unbounded_channel();
        let gate = Arc::new(Semaphore::new(0));
        let messenger = Arc::new(FakeMessenger {
            status_gate: Some(("completed", updating, gate)),
            ..FakeMessenger::default()
        });
        let statuses = ThreadStatusReconciler {
            messenger: messenger.clone(),
            senders: Arc::new(StdMutex::new(HashMap::new())),
        };
        statuses.set(THREAD, ThreadStatus::Completed);
        let sender = statuses.senders.lock().unwrap()[&THREAD].clone();
        for _ in 1..=MAX_STATUS_ATTEMPTS {
            // The paused clock advances through backoff while waiting for the
            // observable start of each request, never through its long timeout.
            timeout(STATUS_WORKER_IDLE_TIMEOUT, updates.recv())
                .await
                .expect("thread status retry stalled")
                .expect("thread status worker stopped");
            tokio::time::advance(STATUS_REQUEST_TIMEOUT).await;
        }
        timeout(STATUS_WORKER_IDLE_TIMEOUT + Duration::from_secs(1), sender.closed())
            .await
            .expect("exhausted title worker did not retire");
        assert!(statuses.senders.lock().unwrap().is_empty());
        assert_eq!(
            messenger.status_attempts.lock().await.len(),
            MAX_STATUS_ATTEMPTS as usize
        );
        assert!(messenger.statuses.lock().await.is_empty());
        statuses.set(THREAD, ThreadStatus::Working);
        messenger.wait_status("working").await;
    }

    #[tokio::test(start_paused = true)]
    async fn working_reviews_repeat_stop_on_completion_and_reopen_after_retirement() {
        let messenger = Arc::new(FakeMessenger::default());
        let statuses = ThreadStatusReconciler {
            messenger: messenger.clone(),
            senders: Arc::new(StdMutex::new(HashMap::new())),
        };
        statuses.set(THREAD, ThreadStatus::Working);
        messenger.wait_status_attempts(1).await;
        tokio::time::advance(STATUS_REVIEW_INTERVAL).await;
        messenger.wait_status_attempts(2).await;
        assert_eq!(*messenger.statuses.lock().await, vec!["working", "working"]);

        statuses.set(THREAD, ThreadStatus::Completed);
        messenger.wait_status("completed").await;
        let sender = statuses.senders.lock().unwrap()[&THREAD].clone();
        sender.closed().await;
        assert!(statuses.senders.lock().unwrap().is_empty());
        tokio::time::advance(STATUS_REVIEW_INTERVAL * 3).await;
        assert_eq!(messenger.status_attempts.lock().await.len(), 3);

        statuses.set(THREAD, ThreadStatus::Working);
        messenger.wait_status_attempts(4).await;
        tokio::time::advance(STATUS_REVIEW_INTERVAL).await;
        messenger.wait_status_attempts(5).await;
        assert_eq!(
            *messenger.statuses.lock().await,
            vec!["working", "working", "completed", "working", "working"],
        );
    }

    #[tokio::test(start_paused = true)]
    async fn failed_working_reviews_resume_after_retry_batch_exhaustion() {
        let messenger = Arc::new(FakeMessenger {
            fail_status: true.into(),
            ..FakeMessenger::default()
        });
        let statuses = ThreadStatusReconciler {
            messenger: messenger.clone(),
            senders: Arc::new(StdMutex::new(HashMap::new())),
        };
        statuses.set(THREAD, ThreadStatus::Working);
        for attempt in 1..=MAX_STATUS_ATTEMPTS {
            messenger.wait_status_attempts(attempt as usize).await;
            tokio::time::advance(Duration::from_secs(1 << attempt)).await;
        }
        messenger.fail_status.store(false, std::sync::atomic::Ordering::Relaxed);
        tokio::time::advance(STATUS_REVIEW_INTERVAL).await;
        messenger.wait_status("working").await;
        assert_eq!(messenger.status_attempts.lock().await.len(), MAX_STATUS_ATTEMPTS as usize + 1);
    }

    #[tokio::test]
    async fn dropping_an_idle_runner_cancels_pending_title_work() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let transport = Arc::new(ControlledTransport::new(observed, false));
        let (updating, mut updates) = mpsc::unbounded_channel();
        let messenger = Arc::new(FakeMessenger {
            status_gate: Some(("completed", updating, Arc::new(Semaphore::new(0)))),
            ..FakeMessenger::default()
        });
        let runner = ThreadQueueRunner::new(store.clone(), transport.clone(), messenger, 1_900, 1);
        let turn = enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, turn);
        transport.finish.add_permits(1);
        received(&mut updates).await;
        idle(&runner).await;
        let weak = Arc::downgrade(&runner);
        drop(runner);
        assert!(
            timeout(Duration::from_secs(3), updates.recv())
                .await
                .unwrap()
                .is_none()
        );
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn rollover_waits_for_turn_and_final_title_worker() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let transport = Arc::new(ControlledTransport::new(observed, false));
        let (updating, mut updates) = mpsc::unbounded_channel();
        let title_gate = Arc::new(Semaphore::new(0));
        let messenger = Arc::new(FakeMessenger {
            status_gate: Some(("completed", updating, title_gate.clone())),
            ..FakeMessenger::default()
        });
        let runner = ThreadQueueRunner::new(store.clone(), transport.clone(), messenger.clone(), 1_900, 1);
        assert!(runner.rollover_idle().await);
        let turn = enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, turn);
        assert!(!runner.rollover_idle().await);
        transport.finish.add_permits(1);
        received(&mut updates).await;
        idle(&runner).await;
        assert!(!runner.rollover_idle().await, "title subprocess still running");
        let sender = runner.statuses.senders.lock().unwrap()[&THREAD].clone();
        title_gate.add_permits(1);
        messenger.wait_status("completed").await;
        assert_eq!(store.next_outcome(THREAD, 0).await.unwrap().unwrap().status, TurnStatus::Done);
        // SQLite uses real blocking workers; pause only after database work settles.
        tokio::time::pause();
        sender.closed().await;
        assert!(runner.rollover_idle().await);
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
        let mut snapshots = runner.subscribe_presence();
        assert_eq!(snapshots.borrow_and_update().working, 1);
        let second = enqueue(&store, 21).await;
        let third = enqueue(&store, 22).await;
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, second);
        assert_eq!(received(&mut incoming).await, third);
        assert!(!snapshots.has_changed().unwrap(), "interrupt replacements must not change presence");
        assert_eq!(
            store.next_outcome(THREAD, 0).await.unwrap().unwrap().status,
            TurnStatus::Aborted
        );
        assert_eq!(store.counts(THREAD).await.unwrap().running, 1);
        messenger.wait_status("working").await;
        assert!(messenger.statuses.lock().await.iter().all(|status| status == "working"));
        transport.finish.add_permits(1);
        idle(&runner).await;
        messenger.wait_status("completed").await;
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
        assert_eq!(*runner.subscribe_presence().borrow(), PresenceSnapshot::default());
        let counts = store.counts(THREAD).await.unwrap();
        assert_eq!((counts.running, counts.queued, counts.uncertain), (0, 0, 2));
        messenger.wait_status("completed").await;
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
        let mut snapshots = runner.subscribe_presence();
        assert_eq!(snapshots.borrow_and_update().working, 1);
        transport.finish.add_permits(1);
        received(&mut edits).await;
        let second = enqueue(&store, 21).await;
        runner.poke(THREAD).await;
        assert_eq!(store.counts(THREAD).await.unwrap().queued, 1);
        messenger.wait_status("working").await;
        gate.add_permits(1);
        assert_eq!(received(&mut incoming).await, second);
        assert!(!snapshots.has_changed().unwrap(), "continuing the drain must not publish idle presence");
        assert!(messenger.statuses.lock().await.iter().all(|status| status == "working"));
        transport.finish.add_permits(1);
        received(&mut edits).await;
        gate.add_permits(1);
        idle(&runner).await;
        assert_eq!(*snapshots.borrow(), PresenceSnapshot::default());
        messenger.wait_status("completed").await;
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
        messenger.wait_status("completed").await;
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
    async fn exclusion_cancels_active_work_discards_queue_and_leaves_other_threads_running() {
        let (directory, store) = fixture().await;
        let other = ThreadId::new(11);
        store.create_session(&NewSession {
            thread_id: other,
            guild_id: GuildId::new(1),
            parent_channel_id: ChannelId::new(2),
            trigger_message_id: MessageId::new(30),
            cwd: directory.path().to_owned(),
            created_by_user_id: UserId::new(4),
        }).await.unwrap();
        let first = enqueue(&store, 20).await;
        let other_turn = match store.enqueue_turn(
            other,
            MessageId::new(30),
            UserId::new(4),
            "other",
            TurnSource::Human,
        ).await.unwrap() {
            EnqueueResult::Enqueued(id) => id,
            EnqueueResult::Duplicate => panic!("unexpected duplicate"),
        };
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let (waiting, mut waits) = mpsc::unbounded_channel();
        let mut transport = ControlledTransport::new(observed, false);
        transport.waiting = Some(waiting);
        let transport = Arc::new(transport);
        let runner = ThreadQueueRunner::new(
            store.clone(),
            transport.clone(),
            Arc::new(FakeMessenger::default()),
            1_900,
            2,
        );
        let snapshots = runner.subscribe_presence();
        runner.poke(THREAD).await;
        assert_eq!(received(&mut incoming).await, first);
        assert_eq!(received(&mut waits).await, first);
        runner.poke(other).await;
        assert_eq!(received(&mut incoming).await, other_turn);
        assert_eq!(received(&mut waits).await, other_turn);
        let queued = enqueue(&store, 21).await;
        assert_eq!(store.counts(THREAD).await.unwrap().queued, 1);

        store.exclude_thread(THREAD).await.unwrap();
        assert!(runner.stop(THREAD).await.unwrap());
        idle(&runner).await;

        let first_outcome = store.next_outcome(THREAD, 0).await.unwrap().unwrap();
        assert_eq!((first_outcome.id, first_outcome.status), (first, TurnStatus::Aborted));
        let queued_outcome = store.next_outcome(THREAD, first).await.unwrap().unwrap();
        assert_eq!((queued_outcome.id, queued_outcome.status), (queued, TurnStatus::Aborted));
        let counts = store.counts(THREAD).await.unwrap();
        assert_eq!((counts.queued, counts.running, counts.uncertain), (0, 0, 0));
        assert!(runner.new_session(THREAD).await.is_err());
        assert_eq!(store.session(THREAD).await.unwrap().unwrap().status, SessionStatus::Stopped);
        runner.poke(THREAD).await;
        assert!(!runner.draining.lock().await.contains_key(&THREAD));
        assert!(incoming.try_recv().is_err());
        assert_eq!(*snapshots.borrow(), PresenceSnapshot { working: 1, awaiting_approval: 0 });
        assert!(!store.is_thread_excluded(other).await.unwrap());
        assert_eq!(store.counts(other).await.unwrap().running, 1);

        transport.finish.add_permits(1);
        timeout(Duration::from_secs(3), runner.wait_idle(other)).await.unwrap();
        assert_eq!(
            store.next_outcome(other, 0).await.unwrap().unwrap().status,
            TurnStatus::Done
        );
        assert_eq!(*snapshots.borrow(), PresenceSnapshot::default());
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
        let mut snapshots = runner.subscribe_presence();
        let permit = runner.permits.acquire().await.unwrap();
        enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        timeout(Duration::from_secs(3), snapshots.changed()).await.unwrap().unwrap();
        assert_eq!(*snapshots.borrow(), PresenceSnapshot { working: 1, awaiting_approval: 0 });
        assert!(!runner.stop(THREAD).await.unwrap());
        idle(&runner).await;
        assert_eq!(*snapshots.borrow(), PresenceSnapshot::default());
        drop(permit);
        runner.poke(THREAD).await;
        idle(&runner).await;
        assert!(incoming.try_recv().is_err());
        let counts = store.counts(THREAD).await.unwrap();
        assert_eq!((counts.queued, counts.running, counts.uncertain), (1, 0, 0));
        assert!(store.next_outcome(THREAD, 0).await.unwrap().is_none());
        messenger.wait_status("working").await;
    }

    #[tokio::test]
    async fn exclusion_while_waiting_for_capacity_discards_input_without_dispatch() {
        let (_directory, store) = fixture().await;
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let runner = ThreadQueueRunner::new(
            store.clone(),
            Arc::new(ControlledTransport::new(observed, false)),
            Arc::new(FakeMessenger::default()),
            1_900,
            1,
        );
        let mut snapshots = runner.subscribe_presence();
        let permit = runner.permits.acquire().await.unwrap();
        let turn = enqueue(&store, 20).await;
        runner.poke(THREAD).await;
        timeout(Duration::from_secs(3), snapshots.changed()).await.unwrap().unwrap();

        store.exclude_thread(THREAD).await.unwrap();
        assert!(!runner.stop(THREAD).await.unwrap());
        idle(&runner).await;
        drop(permit);
        runner.poke(THREAD).await;

        assert!(!runner.draining.lock().await.contains_key(&THREAD));
        assert!(incoming.try_recv().is_err());
        assert_eq!(*snapshots.borrow(), PresenceSnapshot::default());
        let counts = store.counts(THREAD).await.unwrap();
        assert_eq!((counts.queued, counts.running, counts.uncertain), (0, 0, 0));
        let outcome = store.next_outcome(THREAD, 0).await.unwrap().unwrap();
        assert_eq!((outcome.id, outcome.status), (turn, TurnStatus::Aborted));
    }

    #[tokio::test]
    async fn excluded_unmapped_threads_can_be_stopped_but_never_started() {
        let (_directory, store) = fixture().await;
        let excluded = ThreadId::new(11);
        store.exclude_thread(excluded).await.unwrap();
        let (observed, mut incoming) = mpsc::unbounded_channel();
        let runner = ThreadQueueRunner::new(
            store.clone(),
            Arc::new(ControlledTransport::new(observed, false)),
            Arc::new(FakeMessenger::default()),
            1_900,
            1,
        );

        assert!(!runner.stop(excluded).await.unwrap());
        assert!(runner.new_session(excluded).await.is_err());
        runner.poke(excluded).await;

        assert!(!runner.draining.lock().await.contains_key(&excluded));
        assert!(store.session(excluded).await.unwrap().is_none());
        assert!(incoming.try_recv().is_err());
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

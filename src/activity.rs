use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Instant, MissedTickBehavior, interval_at, timeout},
};

use crate::{domain::ThreadId, runner::ThreadMessenger};

const UPDATE_INTERVAL: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, PartialEq, Eq)]
struct Activity {
    label: &'static str,
    finished: bool,
}

pub(crate) struct ActivityReporter {
    sender: watch::Sender<Activity>,
    worker: JoinHandle<()>,
}

impl ActivityReporter {
    pub(crate) fn start(messenger: Arc<dyn ThreadMessenger>, thread_id: ThreadId) -> Self {
        let (sender, receiver) = watch::channel(Activity {
            label: "Starting OMP",
            finished: false,
        });
        let worker = tokio::spawn(async move {
            if let Err(error) = report(messenger.as_ref(), thread_id, receiver).await {
                // Status delivery must never cancel or replay the actual turn.
                tracing::warn!(%thread_id, %error, "activity message delivery stopped");
            }
        });
        Self { sender, worker }
    }

    pub(crate) fn update(&self, label: &'static str) {
        self.sender.send_if_modified(|state| {
            if state.label == label || state.finished {
                return false;
            }
            state.label = label;
            true
        });
    }

    pub(crate) async fn finish(self, label: &'static str) {
        self.sender.send_replace(Activity {
            label,
            finished: true,
        });
        if let Err(error) = self.worker.await {
            tracing::warn!(%error, "activity message task failed");
        }
    }
}

async fn report(
    messenger: &dyn ThreadMessenger,
    thread_id: ThreadId,
    mut receiver: watch::Receiver<Activity>,
) -> Result<()> {
    let started = Instant::now();
    let mut message_id = timeout(
        REQUEST_TIMEOUT,
        messenger.create_activity(thread_id, "OMP: Starting OMP — 0s elapsed", false),
    )
    .await
    .context("activity creation timed out")??;
    let mut ticks = interval_at(Instant::now() + UPDATE_INTERVAL, UPDATE_INTERVAL);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        let mut state = *receiver.borrow_and_update();
        if !state.finished {
            tokio::select! {
                _ = ticks.tick() => {},
                changed = receiver.changed() => {
                    if changed.is_ok() {
                        // Coalesce intermediate events; terminal updates bypass the timer.
                        if !receiver.borrow().finished {
                            continue;
                        }
                    } else {
                        state = Activity { label: "Interrupted; check status", finished: true };
                    }
                }
            }
            if !state.finished {
                state = *receiver.borrow_and_update();
            }
        }
        let content = format!(
            "OMP: {} — {}s elapsed",
            state.label,
            started.elapsed().as_secs()
        );
        message_id = timeout(
            REQUEST_TIMEOUT,
            messenger.edit_activity(thread_id, message_id, &content, state.finished),
        )
        .await
        .context("activity edit timed out")??;
        if state.finished {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use tokio::sync::mpsc;

    use super::*;
    use crate::{domain::MessageId, omp::ApprovalRequest};

    struct Messenger {
        events: mpsc::UnboundedSender<(Option<MessageId>, String, bool)>,
        fail_edit: bool,
        hang_create: bool,
    }

    impl Messenger {
        fn new(events: mpsc::UnboundedSender<(Option<MessageId>, String, bool)>) -> Self {
            Self {
                events,
                fail_edit: false,
                hang_create: false,
            }
        }
    }

    #[async_trait]
    impl ThreadMessenger for Messenger {
        async fn send_assistant(&self, _: ThreadId, _: &str) -> Result<()> {
            unreachable!("activity must use the activity delivery methods")
        }

        async fn send(&self, _: ThreadId, _: &str) -> Result<()> {
            unreachable!("activity must use the activity delivery methods")
        }

        async fn create_activity(
            &self,
            _: ThreadId,
            content: &str,
            finished: bool,
        ) -> Result<MessageId> {
            self.events.send((None, content.to_owned(), finished))?;
            if self.hang_create {
                std::future::pending::<()>().await;
            }
            Ok(MessageId::new(42))
        }

        async fn edit_activity(
            &self,
            _: ThreadId,
            message_id: MessageId,
            content: &str,
            finished: bool,
        ) -> Result<MessageId> {
            self.events
                .send((Some(message_id), content.to_owned(), finished))?;
            anyhow::ensure!(!self.fail_edit, "message deleted");
            Ok(MessageId::new(message_id.get() + 1))
        }

        async fn confirm(&self, _: ThreadId, _: ApprovalRequest) -> bool {
            unreachable!()
        }

        async fn prompt(&self, _: ThreadId, _: serde_json::Value) -> serde_json::Value {
            unreachable!("activity must not request prompts")
        }

        async fn notify(&self, _: ThreadId, _: &str) {
            unreachable!()
        }

        async fn channel_history(&self, _: ThreadId, _: u16) -> Result<String> {
            unreachable!()
        }

        async fn thread_participants(&self, _: ThreadId) -> Result<String> {
            unreachable!()
        }

        async fn upload_file(
            &self,
            _: ThreadId,
            _: crate::omp::UploadRequest,
        ) -> Result<String> {
            unreachable!("activity must not upload files")
        }

        async fn thread_status(&self, _: ThreadId, _: &str) -> Result<()> {
            unreachable!("activity must not change thread status")
        }
    }

    #[tokio::test(start_paused = true)]
    async fn coalesces_updates_and_carries_replacement_ids_through_completion() {
        let (events, mut received) = mpsc::unbounded_channel();
        let reporter = ActivityReporter::start(
            Arc::new(Messenger::new(events)),
            ThreadId::new(1),
        );
        let (id, _, finished) = received.recv().await.unwrap();
        assert_eq!(id, None);
        assert!(!finished);
        reporter.update("Thinking");
        reporter.update("Inspecting files");
        tokio::task::yield_now().await;
        assert!(received.try_recv().is_err());
        tokio::time::advance(UPDATE_INTERVAL).await;
        let (id, content, finished) = received.recv().await.unwrap();
        assert_eq!(id, Some(MessageId::new(42)));
        assert!(content.contains("Inspecting files"));
        assert!(content.contains("5s elapsed"));
        assert!(!finished);
        reporter.update("Inspecting files");
        tokio::time::advance(UPDATE_INTERVAL).await;
        let (id, content, finished) = received.recv().await.unwrap();
        assert_eq!(id, Some(MessageId::new(43)));
        assert!(content.contains("10s elapsed"));
        assert!(!finished);
        reporter.update("Writing a response");
        let finished_at = Instant::now();
        reporter.finish("OMP completed").await;
        assert_eq!(Instant::now(), finished_at, "completion bypasses the timer");
        let (id, content, finished) = received.recv().await.unwrap();
        assert_eq!(id, Some(MessageId::new(44)));
        assert!(content.contains("OMP completed"));
        assert!(finished);
        assert!(received.recv().await.is_none(), "no edits after completion");
    }

    #[tokio::test(start_paused = true)]
    async fn failed_status_edit_is_not_recreated_or_retried() {
        let (events, mut received) = mpsc::unbounded_channel();
        let reporter = ActivityReporter::start(
            Arc::new(Messenger {
                fail_edit: true,
                ..Messenger::new(events)
            }),
            ThreadId::new(1),
        );
        received.recv().await.unwrap();
        tokio::time::advance(UPDATE_INTERVAL).await;
        assert_eq!(received.recv().await.unwrap().0, Some(MessageId::new(42)));
        reporter.finish("Failed").await;
        assert!(received.recv().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_status_creation_cannot_hold_completion_forever() {
        let (events, mut received) = mpsc::unbounded_channel();
        let reporter = ActivityReporter::start(
            Arc::new(Messenger {
                hang_create: true,
                ..Messenger::new(events)
            }),
            ThreadId::new(1),
        );
        received.recv().await.unwrap();
        let started = Instant::now();
        reporter.finish("Stopped").await;
        assert_eq!(started.elapsed(), REQUEST_TIMEOUT);
        assert!(received.recv().await.is_none());
    }

}

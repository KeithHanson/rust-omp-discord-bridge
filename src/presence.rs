use std::{collections::HashMap, sync::{Arc, Mutex}};

use tokio::sync::watch;

use crate::domain::ThreadId;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PresenceSnapshot {
    pub(crate) working: usize,
    pub(crate) awaiting_approval: usize,
}

#[derive(Default)]
struct ThreadPresence {
    engaged: usize,
    approvals: usize,
}

impl ThreadPresence {
    fn snapshot(&self) -> PresenceSnapshot {
        PresenceSnapshot {
            working: usize::from(self.engaged > 0 && self.approvals == 0),
            awaiting_approval: usize::from(self.engaged > 0 && self.approvals > 0),
        }
    }
}

pub(crate) struct PresenceTracker {
    threads: Mutex<HashMap<ThreadId, ThreadPresence>>,
    snapshot: watch::Sender<PresenceSnapshot>,
}

impl Default for PresenceTracker {
    fn default() -> Self {
        Self {
            threads: Mutex::new(HashMap::new()),
            snapshot: watch::channel(PresenceSnapshot::default()).0,
        }
    }
}

impl PresenceTracker {
    pub(crate) fn subscribe(&self) -> watch::Receiver<PresenceSnapshot> {
        self.snapshot.subscribe()
    }

    pub(crate) fn engage(self: &Arc<Self>, thread_id: ThreadId) -> WorkingPresence {
        self.update(thread_id, |thread| thread.engaged += 1);
        WorkingPresence { tracker: Arc::clone(self), thread_id }
    }

    fn update(&self, thread_id: ThreadId, update: impl FnOnce(&mut ThreadPresence)) {
        // Serialize the counters and publication together; no await or network I/O.
        let mut threads = self.threads.lock().unwrap();
        let thread = threads.entry(thread_id).or_default();
        let before = thread.snapshot();
        update(thread);
        let after = thread.snapshot();
        if thread.engaged == 0 && thread.approvals == 0 {
            threads.remove(&thread_id);
        }
        if before != after {
            self.snapshot.send_modify(|snapshot| {
                snapshot.working = snapshot.working - before.working + after.working;
                snapshot.awaiting_approval = snapshot.awaiting_approval
                    - before.awaiting_approval + after.awaiting_approval;
            });
        }
    }
}

pub(crate) struct WorkingPresence {
    tracker: Arc<PresenceTracker>,
    thread_id: ThreadId,
}

impl WorkingPresence {
    pub(crate) fn awaiting_approval(&self) -> ApprovalPresence<'_> {
        self.tracker.update(self.thread_id, |thread| thread.approvals += 1);
        ApprovalPresence(self)
    }
}

impl Drop for WorkingPresence {
    fn drop(&mut self) {
        self.tracker.update(self.thread_id, |thread| thread.engaged -= 1);
    }
}

pub(crate) struct ApprovalPresence<'a>(&'a WorkingPresence);

impl Drop for ApprovalPresence<'_> {
    fn drop(&mut self) {
        self.0.tracker.update(self.0.thread_id, |thread| thread.approvals -= 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_unique_threads_and_nested_approvals_without_duplicate_updates() {
        let tracker = Arc::new(PresenceTracker::default());
        let mut snapshots = tracker.subscribe();
        let first = tracker.engage(ThreadId::new(1));
        let replacement = tracker.engage(ThreadId::new(1));
        let second = tracker.engage(ThreadId::new(2));
        assert_eq!(*snapshots.borrow_and_update(), PresenceSnapshot { working: 2, awaiting_approval: 0 });
        drop(replacement);
        assert!(!snapshots.has_changed().unwrap());
        let approval = first.awaiting_approval();
        assert_eq!(*snapshots.borrow_and_update(), PresenceSnapshot { working: 1, awaiting_approval: 1 });
        let nested = first.awaiting_approval();
        assert!(!snapshots.has_changed().unwrap());
        drop(approval);
        assert!(!snapshots.has_changed().unwrap());
        drop(nested);
        assert_eq!(*snapshots.borrow(), PresenceSnapshot { working: 2, awaiting_approval: 0 });
        drop(first);
        drop(second);
        assert_eq!(*snapshots.borrow(), PresenceSnapshot::default());
        assert!(tracker.threads.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn dropping_pending_future_cleans_up_engagement_and_approval() {
        let tracker = Arc::new(PresenceTracker::default());
        let snapshots = tracker.subscribe();
        let mut pending = Box::pin(async {
            let working = tracker.engage(ThreadId::new(1));
            let _approval = working.awaiting_approval();
            std::future::pending::<()>().await;
        });
        tokio::select! {
            biased;
            _ = &mut pending => panic!("approval unexpectedly completed"),
            _ = std::future::ready(()) => {}
        }
        assert_eq!(*snapshots.borrow(), PresenceSnapshot { working: 0, awaiting_approval: 1 });
        drop(pending);
        assert_eq!(*snapshots.borrow(), PresenceSnapshot::default());
        assert!(tracker.threads.lock().unwrap().is_empty());
    }
}

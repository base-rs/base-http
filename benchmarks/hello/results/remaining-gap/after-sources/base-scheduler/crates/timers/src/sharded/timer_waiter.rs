// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{Publication, TimerAssist, Wake, lock_watchers};
use std::{
    sync::{Arc, atomic::Ordering},
    thread,
    time::Instant,
};

/// A thread-bound waiter watching the deadline hints of several queues.
///
/// Construct on the waiting thread during setup. Construction reserves watcher lists; repeated parking allocates nothing. Earlier deadlines and shutdown unpark it. Publication happens before unpark, and park tokens are never reset after inspection, so a concurrent insertion cannot be lost. Spurious wakes are permitted. Visit queues after waking; parking does not collect values.
pub struct TimerWaiter {
    publications: Vec<Arc<Publication>>,
    wake: Arc<Wake>,
}

impl TimerWaiter {
    /// Watch these queues from the current thread.
    pub fn new<Value>(queues: &[TimerAssist<Value>]) -> Self {
        let wake = Arc::new(Wake(thread::current()));
        let publications = queues
            .iter()
            .map(|queue| {
                lock_watchers(&queue.publication.watchers).push(Arc::clone(&wake));
                Arc::clone(&queue.publication)
            })
            .collect();
        Self { publications, wake }
    }

    /// Wait until a watched hint is due, a notification arrives, or a spurious wake.
    ///
    /// Returns immediately if all watched queues are closed (or the set is empty).
    pub fn park(&self) {
        assert_eq!(
            self.wake.0.id(),
            thread::current().id(),
            "timer waiter moved after binding"
        );
        if self
            .publications
            .iter()
            .all(|queue| queue.closed.load(Ordering::Acquire))
        {
            return;
        }
        match self
            .publications
            .iter()
            .filter_map(|queue| queue.deadline())
            .min()
        {
            Some(deadline) => {
                thread::park_timeout(deadline.saturating_duration_since(Instant::now()))
            }
            None => thread::park(),
        }
    }
}

impl Drop for TimerWaiter {
    fn drop(&mut self) {
        for publication in &self.publications {
            lock_watchers(&publication.watchers)
                .retain(|watcher| !Arc::ptr_eq(watcher, &self.wake));
        }
    }
}

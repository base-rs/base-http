// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Worker-owned timer queues with opportunistic assistance and deadline wakes.
//!
//! Each queue has one mutex, reusable slots and the usual pending-buffer/heap algorithm. Helpers can collect values while the owner is stalled outside queue operations. Timer identity stays with its original queue. This module creates no threads and does not integrate with an executor.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::Thread;
use std::time::{Duration, Instant};

mod sleep;
mod timer_assist;
mod timer_waiter;
mod worker_timers;

pub use sleep::{Sleep, SleepError, SleepWake};
pub use timer_assist::TimerAssist;
pub use timer_waiter::TimerWaiter;
pub use worker_timers::WorkerTimers;

const EMPTY: u64 = u64::MAX;

fn lock_watchers<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

struct Wake(Thread);

// Deadline writes must not invalidate another worker's publication cache line.
#[repr(align(128))]
pub(crate) struct Publication {
    pub(crate) epoch: Instant,
    next: AtomicU64,
    closed: AtomicBool,
    watchers: Mutex<Vec<Arc<Wake>>>,
}

impl Publication {
    fn new() -> Self {
        let publication = Self {
            epoch: Instant::now(),
            next: AtomicU64::new(EMPTY),
            closed: AtomicBool::new(false),
            watchers: Mutex::new(Vec::new()),
        };
        // Include macOS's lazy native mutex initialization in setup.
        drop(lock_watchers(&publication.watchers));
        publication
    }

    fn tick(&self, deadline: Instant) -> u64 {
        deadline
            .saturating_duration_since(self.epoch)
            .as_nanos()
            .min(u128::from(EMPTY - 1)) as u64
    }

    fn deadline(&self) -> Option<Instant> {
        let tick = self.next.load(Ordering::Acquire);
        (tick != EMPTY).then(|| self.epoch + Duration::from_nanos(tick))
    }

    fn due(&self, now: Instant) -> bool {
        let tick = self.next.load(Ordering::Acquire);
        // Zero includes deadlines before the epoch and cleanup requests. Treat it as immediately due even when the caller supplies an older snapshot.
        tick != EMPTY && (tick == 0 || now >= self.epoch + Duration::from_nanos(tick))
    }

    // All writers hold their queue's mutex. A store suffices; there is no global atomic read-modify-write on registration or cancellation.
    pub(crate) fn lower(&self, deadline: Instant) {
        let tick = self.tick(deadline);
        if tick < self.next.load(Ordering::Relaxed) {
            self.next.store(tick, Ordering::Release);
            self.notify();
        }
    }

    pub(crate) fn refresh(&self, deadline: Option<Instant>) {
        let next = if self.closed.load(Ordering::Relaxed) {
            EMPTY
        } else {
            deadline.map_or(EMPTY, |deadline| self.tick(deadline))
        };
        self.next.store(next, Ordering::Release);
    }

    fn notify(&self) {
        for watcher in lock_watchers(&self.watchers).iter() {
            watcher.0.unpark();
        }
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.next.store(EMPTY, Ordering::Release);
        self.notify();
    }
}

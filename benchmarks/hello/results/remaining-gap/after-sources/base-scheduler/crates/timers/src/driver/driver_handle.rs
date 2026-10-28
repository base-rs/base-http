// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{Pool, lock};
use std::{
    sync::{Arc, atomic::Ordering as AtomicOrdering},
    time::Instant,
};

/// A movable cancellation handle. Dropping it does not cancel the timer.
pub struct DriverHandle<Value> {
    pub(super) pool: Arc<Pool<Value>>,
    pub(super) index: usize,
    pub(super) deadline: Instant,
}

impl<Value> DriverHandle<Value> {
    /// The timer's monotonic deadline.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Claim and release the value immediately, outside synchronization.
    ///
    /// Returns false when cancellation, expiry or shutdown already claimed it.
    pub fn cancel(&self) -> bool {
        let value = {
            let mut slot = lock(&self.pool.slots[self.index]);
            let value = slot.value.take();
            if value.is_some() {
                slot.cancelled = true;
                if slot.in_heap {
                    self.pool
                        .cancelled_heap
                        .fetch_add(1, AtomicOrdering::Relaxed);
                }
            }
            value
        };
        let cancelled = value.is_some();
        if cancelled {
            self.pool.notification.send();
        }
        drop(value);
        cancelled
    }

    /// Whether this handle successfully cancelled its timer.
    pub fn is_cancelled(&self) -> bool {
        self.pool.is_cancelled(self.index)
    }
}

impl<Value> Drop for DriverHandle<Value> {
    fn drop(&mut self) {
        let mut slot = lock(&self.pool.slots[self.index]);
        slot.handle = false;
        self.pool.release(self.index, &slot);
    }
}

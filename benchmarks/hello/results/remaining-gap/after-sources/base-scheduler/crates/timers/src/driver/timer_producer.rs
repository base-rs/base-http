// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{DriverHandle, Pool, Slot, lock};
use std::{sync::Arc, time::Instant};

/// A fixed-capacity registration path, intended to be owned by one worker.
///
/// Use a dedicated producer for external submissions. Moving a task does not move its timer: its cancellation handle retains the original pool.
pub struct TimerProducer<Value> {
    pub(super) pool: Arc<Pool<Value>>,
}

impl<Value> TimerProducer<Value> {
    /// Register without allocating, returning the value if full or closed.
    ///
    /// Capacity includes cancelled entries not yet drained and completed timers whose handles still exist. Shutdown serializes with this producer only.
    pub fn try_schedule(
        &mut self,
        deadline: Instant,
        value: Value,
    ) -> Result<DriverHandle<Value>, Value> {
        let closed = lock(&self.pool.closed);
        if *closed {
            return Err(value);
        }
        let Some(index) = self.pool.free.pop() else {
            return Err(value);
        };
        *lock(&self.pool.slots[index]) = Slot {
            value: Some(value),
            handle: true,
            queued: true,
            in_heap: false,
            cancelled: false,
        };
        // Each inbox command retains a distinct slot, so acquiring a free slot guarantees inbox space, even when consumer and producer overlap.
        self.pool.inbox.push((deadline, index)).unwrap();
        drop(closed);
        self.pool.notification.send();
        Ok(DriverHandle {
            pool: Arc::clone(&self.pool),
            index,
            deadline,
        })
    }
}

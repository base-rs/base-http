// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{Publication, Sleep, SleepWake};
use crate::TimerHandle;
use crate::timers::{Shared, try_schedule_shared};
use std::{
    sync::{Arc, TryLockError},
    time::Instant,
};

/// Deadline observation and nonblocking access to a worker's existing queue.
///
/// Dropping the owner shuts down the queue even if assistance views survive.
pub struct TimerAssist<Value> {
    pub(super) shared: Shared<Value>,
    pub(super) publication: Arc<Publication>,
}

impl<Value> Clone for TimerAssist<Value> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            publication: Arc::clone(&self.publication),
        }
    }
}

impl<Value> TimerAssist<Value> {
    /// Register from an external caller through this queue's existing mutex.
    ///
    /// Returns the original value when the queue is full or its owner shut down.
    pub fn try_schedule(
        &self,
        deadline: Instant,
        value: Value,
    ) -> Result<TimerHandle<Value>, Value> {
        try_schedule_shared(&self.shared, deadline, value)
    }

    /// A conservative published wake time, including unflushed submissions.
    ///
    /// It may be earlier than the next live timer after cancellation. Deadlines before queue construction are represented by the construction instant; timers beyond the nanosecond hint's range are represented earlier.
    pub fn next_deadline(&self) -> Option<Instant> {
        self.publication.deadline()
    }

    /// Collect due values or reclaim capacity without waiting for a queue lock.
    ///
    /// None means the queue is busy. Some(0) means no work was collected. Helpers should visit other queues after a busy result. Values are owned by the caller on return; route wakeups to an available worker outside this call.
    pub fn try_poll(&self, now: Instant, output: &mut Vec<Value>) -> Option<usize> {
        if output.len() == output.capacity() || !self.publication.due(now) {
            return Some(0);
        }
        self.collect(now, output)
    }

    fn collect(&self, now: Instant, output: &mut Vec<Value>) -> Option<usize> {
        let mut state = match self.shared.try_lock() {
            Ok(state) => state,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
            Err(TryLockError::WouldBlock) => return None,
        };
        if state.is_closed() {
            return Some(0);
        }
        Some(state.pop_expired_batch(now, output))
    }
}

impl TimerAssist<SleepWake> {
    /// Create a lazy sleep. Only its first poll registers a timer.
    pub fn sleep_until(&self, deadline: Instant) -> Sleep {
        Sleep::new(Arc::clone(&self.shared), deadline)
    }
}

#[cfg(test)]
mod tests;

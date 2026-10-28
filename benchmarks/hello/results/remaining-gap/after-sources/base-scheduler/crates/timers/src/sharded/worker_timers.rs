// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{Publication, TimerAssist};
use crate::timers::lock;
use crate::{TimerHandle, Timers};
use std::{sync::Arc, time::Instant};

/// A queue normally driven by one worker, independently of other workers.
///
/// Capacity is reserved during construction. Scheduling publishes a deadline before returning, even while registrations are still in the pending buffer. Handles support remote cancellation and cloning; values need only be Send for cross-thread use. No per-timer mutex, inbox or notification allocation.
pub struct WorkerTimers<Value> {
    timers: Timers<Value>,
    publication: Arc<Publication>,
}

impl<Value> WorkerTimers<Value> {
    /// Reserve at least this many slots and both heap buffers.
    pub fn with_capacity(capacity: usize) -> Self {
        let timers = Timers::with_capacity(capacity);
        let publication = Arc::new(Publication::new());
        lock(&timers.shared).publication = Some(Arc::clone(&publication));
        Self {
            timers,
            publication,
        }
    }

    /// Actual reserved capacity, including completed handles and tombstones.
    pub fn capacity(&self) -> usize {
        self.timers.capacity()
    }

    /// Number of live, unclaimed timers.
    pub fn len(&self) -> usize {
        self.timers.len()
    }

    /// Whether no live timers remain.
    pub fn is_empty(&self) -> bool {
        self.timers.is_empty()
    }

    /// Schedule without allocating, returning the original value when full.
    pub fn try_schedule(
        &mut self,
        deadline: Instant,
        value: Value,
    ) -> Result<TimerHandle<Value>, Value> {
        self.timers.try_schedule(deadline, value)
    }

    /// Merge submissions, reclaim cancellations and return a live deadline.
    pub fn next_deadline(&mut self) -> Option<Instant> {
        self.timers.next_deadline()
    }

    /// Collect due values within the output's spare capacity, in any order.
    ///
    /// A full output leaves the queue unchanged. Process values after returning.
    pub fn poll(&mut self, now: Instant, output: &mut Vec<Value>) -> usize {
        self.timers.pop_expired_batch(now, output)
    }

    /// A clonable view allowing another worker to assist this queue.
    pub fn assistance(&self) -> TimerAssist<Value> {
        TimerAssist {
            shared: Arc::clone(&self.timers.shared),
            publication: Arc::clone(&self.publication),
        }
    }
}

#[cfg(test)]
mod tests;

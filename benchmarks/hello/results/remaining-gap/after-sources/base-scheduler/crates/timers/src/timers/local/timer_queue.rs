// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::super::*;

/// A registration key belonging to its originating [`TimerQueue`]. Release it after expiry or cancellation so its slot can be reused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerKey(Key);

/// An exclusively owned queue using the shared timer engine's batching and reusable slots, without synchronization or shared allocations.
pub struct TimerQueue<Value>(State<Value>);

impl<Value> TimerQueue<Value> {
    /// Reserve at least `capacity` simultaneous registrations. Outstanding keys retain capacity after expiry until released.
    pub fn with_capacity(capacity: usize) -> Self {
        let mut state = State::new();
        state.reserve(capacity);
        Self(state)
    }

    /// Total reserved registration capacity.
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// Number of live timers, excluding cancellations and collected expiries.
    pub fn len(&self) -> usize {
        self.0.live
    }

    /// Whether no live timers remain.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Schedule within reserved storage, returning the original value when full. Capacity pressure reclaims canceled entries, including non-earliest heap entries.
    pub fn try_schedule(&mut self, deadline: Instant, value: Value) -> Result<TimerKey, Value> {
        if self.0.occupied() == self.0.capacity() {
            self.0.flush();
            if self.0.cancelled > 0 {
                let mut entries = std::mem::take(&mut self.0.heap).into_vec();
                entries.retain(|entry| {
                    if self.0.is_cancelled(entry.key) {
                        self.0.remove(entry.key);
                        false
                    } else {
                        true
                    }
                });
                self.0.heap = BinaryHeap::from(entries);
            }
        }
        self.0.try_insert(deadline, value).map(TimerKey)
    }

    /// Replace a pending value. Return the replacement unchanged if its key has expired or was released; the caller decides whether it needs waking.
    pub fn update(&mut self, key: TimerKey, value: Value) -> Result<Value, Value> {
        let slot = &mut self.0.slots[key.0.index];
        if slot.generation != key.0.generation || slot.handles == 0 {
            return Err(value);
        }
        let Some(previous) = slot.value.as_mut() else {
            return Err(value);
        };
        Ok(std::mem::replace(previous, value))
    }

    /// Cancel and release a key, including after expiry. Return any pending value for destruction outside queue borrows. Already released or stale keys are ignored.
    pub fn release(&mut self, key: TimerKey) -> Option<Value> {
        let slot = &self.0.slots[key.0.index];
        if slot.generation != key.0.generation || slot.handles == 0 {
            return None;
        }
        let value = self.0.cancel(key.0);
        self.0.drop_handle(key.0);
        value
    }

    /// Earliest live deadline after flushing pending registrations and canceled roots.
    pub fn next_deadline(&mut self) -> Option<Instant> {
        self.0.flush();
        self.0.discard_cancelled();
        self.0.heap.peek().map(|entry| entry.deadline)
    }

    /// Remove the earliest expired value. Invoke callbacks only after releasing any outer queue borrow.
    pub fn pop_expired(&mut self, now: Instant) -> Option<Value> {
        if self.next_deadline()? > now {
            return None;
        }
        self.pop()
    }

    /// Remove the earliest value, including a future deadline, for shutdown.
    pub fn pop(&mut self) -> Option<Value> {
        self.next_deadline()?;
        let entry = self.0.heap.pop().unwrap();
        self.0.remove(entry.key)
    }
}

#[cfg(test)]
mod tests;

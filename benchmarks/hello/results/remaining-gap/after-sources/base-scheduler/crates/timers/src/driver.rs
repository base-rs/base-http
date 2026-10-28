// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Experimental bounded producer inboxes feeding a single heap owner.
//!
//! Producers and handles can move between threads. After its first poll or deadline query, the driver must remain on that thread. Handles are deliberately not clonable. Dropping a handle leaves its timer scheduled; cancellation is explicit. There is no global heap lock, but each fixed slot has a small mutex and each producer has an admission mutex that synchronizes with shutdown.

use crossbeam_queue::ArrayQueue;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};
use std::thread::{self, Thread};
use std::time::Instant;

mod driver_handle;
mod timer_driver;
mod timer_producer;

pub use driver_handle::DriverHandle;
pub use timer_driver::TimerDriver;
pub use timer_producer::TimerProducer;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Default)]
struct Notification {
    thread: OnceLock<Thread>,
    pending: AtomicBool,
}

impl Notification {
    fn send(&self) {
        if !self.pending.swap(true, AtomicOrdering::AcqRel) {
            if let Some(thread) = self.thread.get() {
                thread.unpark();
            }
        }
    }

    fn begin_poll(&self) {
        let owner = self.thread.get_or_init(thread::current);
        assert_eq!(
            owner.id(),
            thread::current().id(),
            "timer driver moved after binding"
        );
        // Reset BEFORE draining. Publications before this acquire are visible; publications afterwards leave a token for the next park. Never clear the notification between inspecting inboxes and parking.
        self.pending.swap(false, AtomicOrdering::AcqRel);
    }
}

struct Slot<Value> {
    value: Option<Value>,
    handle: bool,
    queued: bool,
    in_heap: bool,
    cancelled: bool,
}

struct Pool<Value> {
    slots: Box<[Mutex<Slot<Value>>]>,
    free: ArrayQueue<usize>,
    inbox: ArrayQueue<(Instant, usize)>,
    closed: Mutex<bool>,
    cancelled_heap: AtomicUsize,
    notification: Notification,
}

impl<Value> Pool<Value> {
    fn release(&self, index: usize, slot: &Slot<Value>) {
        // A slot cannot be reused until both its command/heap entry and handle are gone. No generations or per-entry Arc allocations are necessary.
        if !slot.handle && !slot.queued {
            self.free.push(index).unwrap();
        }
    }

    fn retire(&self, index: usize) -> Option<Value> {
        let mut slot = lock(&self.slots[index]);
        if slot.in_heap && slot.cancelled {
            self.cancelled_heap.fetch_sub(1, AtomicOrdering::Relaxed);
        }
        slot.in_heap = false;
        slot.queued = false;
        let value = slot.value.take();
        self.release(index, &slot);
        value
    }

    fn admit(&self, index: usize) -> bool {
        let mut slot = lock(&self.slots[index]);
        if slot.cancelled {
            slot.queued = false;
            self.release(index, &slot);
            false
        } else {
            // Cancellation and admission share this lock, so only admitted entries contribute to the compaction count. This includes the brief interval between filtering and actual heap insertion.
            slot.in_heap = true;
            true
        }
    }

    fn is_cancelled(&self, index: usize) -> bool {
        lock(&self.slots[index]).cancelled
    }
}

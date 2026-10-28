// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::{
    io,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

struct Entry {
    id: usize,
    waker: Waker,
}
#[derive(Default)]
struct State {
    // Highest issued ID, never reused even when a slot is recycled.
    last_id: usize,
    entries: Vec<Option<Entry>>,
}

/// A broadcast notification with registration and waker storage in one step.
/// All arbitrary waker callbacks run outside the lock, including clone/drop.
#[derive(Default)]
pub(super) struct Notification {
    state: Mutex<State>,
    notified_through: AtomicUsize,
}
impl Notification {
    pub(super) fn new() -> Self {
        Self::default()
    }
    pub(super) fn listen(&self) -> Listener<'_> {
        Listener {
            notification: self,
            key: None,
        }
    }
    pub(super) fn notify(&self) {
        let mut first = None;
        let mut rest = Vec::new();
        {
            let mut state = self.state.lock().unwrap();
            for entry in &mut state.entries {
                if let Some(entry) = entry.take() {
                    if first.is_none() {
                        first = Some(entry.waker);
                    } else {
                        rest.push(entry.waker);
                    }
                }
            }
            // Detached listeners need no lock on completion/drop. New
            // registrations get higher IDs, including when reusing a slot.
            self.notified_through
                .store(state.last_id, Ordering::Release);
        }
        if let Some(waker) = first {
            waker.wake();
        }
        for waker in rest {
            waker.wake();
        }
    }
}

pub(super) struct Listener<'a> {
    notification: &'a Notification,
    key: Option<(usize, usize)>,
}
impl Listener<'_> {
    fn notified(&self) -> bool {
        self.key
            .is_some_and(|(id, _)| id <= self.notification.notified_through.load(Ordering::Acquire))
    }
    pub(super) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.notified() {
            return Poll::Ready(Ok(()));
        }
        // Clone before taking the lock: even cloning a custom waker can
        // reenter the notifier. The locked recheck observes that notification.
        let mut replacement = Some(cx.waker().clone());
        let result = {
            let mut state = self.notification.state.lock().unwrap();
            if self.notified() {
                Poll::Ready(Ok(()))
            } else if let Some((id, index)) = self.key {
                let entry = state.entries[index].as_mut().unwrap();
                debug_assert_eq!(entry.id, id);
                if !entry.waker.will_wake(cx.waker()) {
                    std::mem::swap(&mut entry.waker, replacement.as_mut().unwrap());
                }
                Poll::Pending
            } else if let Some(id) = state.last_id.checked_add(1) {
                state.last_id = id;
                let entry = Some(Entry {
                    id,
                    waker: replacement.take().unwrap(),
                });
                let index = match state.entries.iter().position(Option::is_none) {
                    Some(index) => {
                        state.entries[index] = entry;
                        index
                    }
                    None => {
                        state.entries.push(entry);
                        state.entries.len() - 1
                    }
                };
                self.key = Some((id, index));
                Poll::Pending
            } else {
                Poll::Ready(Err(io::Error::other("readiness listener IDs exhausted")))
            }
        };
        drop(replacement);
        result
    }
}
impl Drop for Listener<'_> {
    fn drop(&mut self) {
        if let Some((id, index)) = self.key {
            if self.notified() {
                return;
            }
            let entry = {
                let mut state = self.notification.state.lock().unwrap();
                if self.notified() {
                    None
                } else {
                    debug_assert_eq!(state.entries[index].as_ref().unwrap().id, id);
                    state.entries[index].take()
                }
            };
            drop(entry);
        }
    }
}

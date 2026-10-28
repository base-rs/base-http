// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{DirectScheduler, thread};
use base_scheduler::{Cancellation, Clock, OperationResult};
use std::{
    collections::BTreeMap,
    future::{Future, poll_fn},
    io,
    pin::pin,
    sync::{Arc, Condvar, Mutex},
    task::{Poll, Waker},
    time::Instant,
};

struct State {
    closed: bool,
    next: usize,
    timers: BTreeMap<usize, (Instant, Waker)>,
    #[cfg(test)]
    paused: Option<Instant>,
    #[cfg(test)]
    waits: Option<std::sync::mpsc::Sender<bool>>,
}
struct Shared {
    state: Mutex<State>,
    changed: Condvar,
}
pub(crate) struct TimerService {
    shared: Arc<Shared>,
    started: Mutex<bool>,
}
struct Registration {
    shared: Arc<Shared>,
    id: usize,
}
impl Drop for Registration {
    fn drop(&mut self) {
        let timer = {
            let mut state = self.shared.state.lock().unwrap();
            state.timers.remove(&self.id)
        };
        // A waker destructor may reenter the clock, just like a wake callback.
        drop(timer);
        self.shared.changed.notify_one();
    }
}
impl State {
    fn now(&self) -> Instant {
        #[cfg(test)]
        if let Some(now) = self.paused {
            return now;
        }
        Instant::now()
    }
    fn expired(&mut self) -> Vec<Waker> {
        let now = self.now();
        let expired: Vec<_> = self
            .timers
            .iter()
            .filter(|(_, (deadline, _))| *deadline <= now)
            .map(|(&id, _)| id)
            .collect();
        expired
            .into_iter()
            .map(|id| self.timers.remove(&id).unwrap().1)
            .collect()
    }
}
impl TimerService {
    pub(crate) fn new() -> Self {
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    closed: false,
                    next: 0,
                    timers: BTreeMap::new(),
                    #[cfg(test)]
                    paused: None,
                    #[cfg(test)]
                    waits: None,
                }),
                changed: Condvar::new(),
            }),
            started: Mutex::new(false),
        }
    }
    fn start(&self) -> io::Result<()> {
        // Controlled clocks are driven by advance(), through the same registration and expiry paths, without racing a native driver thread.
        #[cfg(test)]
        if self.shared.state.lock().unwrap().paused.is_some() {
            return Ok(());
        }
        let mut started = self.started.lock().unwrap();
        if !*started {
            let shared = Arc::clone(&self.shared);
            thread::start("base-direct-timer", move || work(shared))?;
            *started = true;
        }
        Ok(())
    }
    pub(crate) fn now(&self) -> Instant {
        self.shared.state.lock().unwrap().now()
    }
    pub(crate) async fn sleep(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        if self.now() >= deadline {
            return Ok(());
        }
        self.start()?;
        let mut registration: Option<Registration> = None;
        let mut cancelled = pin!(cancellation.cancelled());
        poll_fn(|context| {
            // Expiry wins cancellation once the wait has started.
            if self.now() >= deadline {
                return Poll::Ready(Ok(()));
            }
            if cancelled.as_mut().poll(context).is_ready() {
                return Poll::Ready(Err(base_scheduler::Cancelled.into()));
            }
            // Clone the current waker and release any displaced waker outside the timer lock.
            let waker = context.waker().clone();
            let previous = {
                let mut state = self.shared.state.lock().unwrap();
                if state.closed {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "clock is closed",
                    )
                    .into()));
                }
                let id = if let Some(registration) = &registration {
                    registration.id
                } else {
                    let id = state.next;
                    let Some(next) = id.checked_add(1) else {
                        return Poll::Ready(Err(
                            io::Error::other("timer identifiers exhausted").into()
                        ));
                    };
                    state.next = next;
                    registration = Some(Registration {
                        shared: Arc::clone(&self.shared),
                        id,
                    });
                    id
                };
                state.timers.insert(id, (deadline, waker))
            };
            drop(previous);
            self.shared.changed.notify_one();
            Poll::Pending
        })
        .await
    }
    pub(crate) fn close(&self) {
        let timers = {
            let mut state = self.shared.state.lock().unwrap();
            state.closed = true;
            std::mem::take(&mut state.timers)
        };
        self.shared.changed.notify_all();
        for (_, (_, waker)) in timers {
            waker.wake();
        }
    }
}
impl Drop for TimerService {
    fn drop(&mut self) {
        self.close();
    }
}
fn work(shared: Arc<Shared>) {
    let mut state = shared.state.lock().unwrap();
    loop {
        if state.closed {
            return;
        }
        let wakes = state.expired();
        if !wakes.is_empty() {
            drop(state);
            for waker in wakes {
                waker.wake();
            }
            state = shared.state.lock().unwrap();
            continue;
        }
        let next = state.timers.values().map(|(deadline, _)| *deadline).min();
        #[cfg(test)]
        tests::observe_wait(&state, next.is_some());
        state = match next {
            Some(deadline) => {
                shared
                    .changed
                    .wait_timeout(state, deadline.saturating_duration_since(Instant::now()))
                    .unwrap()
                    .0
            }
            None => shared.changed.wait(state).unwrap(),
        };
    }
}
impl Clock for DirectScheduler {
    fn now(&self) -> Instant {
        self.timers.now()
    }
    async fn sleep_until(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.timers.sleep(deadline, cancellation).await
    }
}

#[cfg(test)]
mod tests;

mod direct_timer;
pub use direct_timer::DirectTimer;

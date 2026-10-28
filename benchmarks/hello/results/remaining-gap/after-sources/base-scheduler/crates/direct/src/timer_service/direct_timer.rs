// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;

/// Experimental reusable Direct timer, constructed through `DirectScheduler::timer`. The timer service remains the reference implementation's dedicated thread.
pub struct DirectTimer {
    shared: Arc<Shared>,
    registration: Option<Registration>,
    armed: Option<Instant>,
    waker: Option<Waker>,
}
impl base_scheduler::Timers for DirectScheduler {
    type Timer = DirectTimer;

    fn timer(&self) -> DirectTimer {
        DirectTimer {
            shared: Arc::clone(&self.timers.shared),
            registration: None,
            armed: None,
            waker: None,
        }
    }

    async fn timer_wait_until(
        &self,
        timer: &mut DirectTimer,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        if !Arc::ptr_eq(&timer.shared, &self.timers.shared) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "timer belongs to another scheduler",
            )
            .into());
        }
        if self.now() >= deadline {
            timer.registration = None;
            timer.armed = None;
            timer.waker = None;
            return Ok(());
        }
        self.timers.start()?;
        let mut cancelled = pin!(cancellation.cancelled());
        let result = poll_fn(|context| {
            let now = self.now();
            if now >= deadline {
                return Poll::Ready(Ok(()));
            }
            if cancelled.as_mut().poll(context).is_ready() {
                return Poll::Ready(Err(base_scheduler::Cancelled.into()));
            }
            // Retain an earlier wake while postponing a timeout. Reuse its identifier when that wake expires.
            let armed = timer
                .armed
                .filter(|armed| *armed <= deadline && *armed > now)
                .unwrap_or(deadline);
            let changed = timer.armed != Some(armed)
                || timer
                    .waker
                    .as_ref()
                    .is_none_or(|waker| !waker.will_wake(context.waker()));
            // All arbitrary waker callbacks stay outside the service mutex.
            let replacement = changed.then(|| context.waker().clone());
            let cached = changed.then(|| context.waker().clone());
            let previous = {
                let mut state = self.timers.shared.state.lock().unwrap();
                if state.closed {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "clock is closed",
                    )
                    .into()));
                }
                if changed {
                    let id = if let Some(registration) = &timer.registration {
                        registration.id
                    } else {
                        let id = state.next;
                        state.next = id
                            .checked_add(1)
                            .ok_or_else(|| io::Error::other("timer identifiers exhausted"))?;
                        timer.registration = Some(Registration {
                            shared: Arc::clone(&self.timers.shared),
                            id,
                        });
                        id
                    };
                    state.timers.insert(id, (armed, replacement.unwrap()))
                } else {
                    None
                }
            };
            drop(previous);
            if changed {
                timer.waker = cached;
                timer.armed = Some(armed);
                self.timers.shared.changed.notify_one();
            }
            Poll::Pending
        })
        .await;
        timer.registration = None;
        timer.armed = None;
        timer.waker = None;
        result
    }
}

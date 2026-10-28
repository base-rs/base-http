// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{Command, Registration};
use crate::worker_timers;
use crate::{NativeScheduler, Selector, task_service::Shared, worker};
use base_scheduler::{Cancellation, Clock, OperationResult, Timers};
use futures_channel::oneshot;
use std::{
    future::{Future, poll_fn},
    io,
    pin::{Pin, pin},
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll, Waker},
    time::Instant,
};

/// Experimental timer resource, constructed through `NativeScheduler::timer`. Pending registrations retain their original worker when the waiting task moves.
pub struct NativeTimer {
    shared: Arc<Shared>,
    armed: Option<Instant>,
    registration: Option<Registration>,
    pending: Option<oneshot::Receiver<io::Result<Registration>>>,
    waker: Option<Waker>,
}
impl NativeTimer {
    fn disarm(&mut self) {
        self.registration = None;
        self.pending = None;
        self.armed = None;
        self.waker = None;
    }
    fn poll(
        &mut self,
        shared: &Arc<Shared>,
        deadline: Instant,
        now: Instant,
        context: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        // Extending a pending timeout need not touch the queue. Its old wake will rearm for the current desired deadline when polled.
        if self
            .armed
            .is_none_or(|armed| deadline < armed || now >= armed)
        {
            self.disarm();
            self.armed = Some(deadline);
        }
        if self.registration.is_none() && self.pending.is_none() {
            let mut waker = Some(context.waker().clone());
            let local = worker::timers(shared, None, |owner, timers| {
                (owner, timers.try_schedule(deadline, waker.take().unwrap()))
            });
            if let Some((owner, result)) = local {
                self.registration = Some(worker_timers::registered(shared, owner, result)?);
            } else {
                let owner =
                    shared.timer_next.fetch_add(1, Ordering::Relaxed) % shared.workers.len();
                let (reply, receive) = oneshot::channel();
                shared.send_timer(
                    owner,
                    Command::Register {
                        deadline,
                        waker: waker.unwrap(),
                        reply,
                    },
                );
                self.pending = Some(receive);
            }
            self.waker = Some(context.waker().clone());
        }
        if let Some(pending) = self.pending.as_mut() {
            let result = match Pin::new(pending).poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result.expect("worker acknowledges timer registration"),
            };
            self.pending = None;
            self.registration = Some(result?);
        }
        if !self.waker.as_ref().unwrap().will_wake(context.waker()) {
            let replacement = context.waker().clone();
            self.waker = Some(replacement.clone());
            let registration = self.registration.as_ref().unwrap();
            worker_timers::dispatch(
                shared,
                registration.owner,
                Command::Update(registration.key, replacement),
            );
        }
        Poll::Pending
    }
}
impl<S: Selector> Timers for NativeScheduler<S> {
    type Timer = NativeTimer;

    fn timer(&self) -> NativeTimer {
        NativeTimer {
            shared: Arc::clone(&self.tasks.shared),
            armed: None,
            registration: None,
            pending: None,
            waker: None,
        }
    }

    async fn timer_wait_until(
        &self,
        timer: &mut NativeTimer,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        let shared = &self.tasks.shared;
        if !Arc::ptr_eq(&timer.shared, shared) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "timer belongs to another scheduler",
            )
            .into());
        }
        let mut cancelled = pin!(cancellation.cancelled());
        let result = poll_fn(|context| {
            let now = self.now();
            if now >= deadline {
                return Poll::Ready(Ok(()));
            }
            if cancelled.as_mut().poll(context).is_ready() {
                return Poll::Ready(Err(base_scheduler::Cancelled.into()));
            }
            if shared.is_closed() {
                return Poll::Ready(Err(worker_timers::closed().into()));
            }
            timer
                .poll(shared, deadline, now, context)
                .map_err(Into::into)
        })
        .await;
        timer.disarm();
        result
    }
}

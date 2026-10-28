// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

mod support;
use base_scheduler::{Cancellation, Clock, OperationResult};
use base_scheduler_conformance::{ClockFixture, clock};
use futures_lite::future::block_on;
use std::{
    collections::BTreeMap,
    future::{Future, poll_fn},
    pin::pin,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, PartialEq)]
enum Fault {
    None,
    Cancellation,
    EarlyExpiry,
    LeakedTimer,
    RelativeConstruction,
    CancellationWins,
    MissingWake,
    RelativeCancellation,
    PendingSuccess,
    CancellationSelfWake,
    ExpiryError,
}
struct State {
    now: Instant,
    next: usize,
    timers: BTreeMap<usize, (Instant, Waker)>,
}
struct Fixture {
    state: Arc<Mutex<State>>,
    fault: Fault,
}
impl Fixture {
    fn new(fault: Fault) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                now: Instant::now(),
                next: 0,
                timers: BTreeMap::new(),
            })),
            fault,
        }
    }
}
struct Registration {
    state: Arc<Mutex<State>>,
    id: usize,
    leak: bool,
}
impl Drop for Registration {
    fn drop(&mut self) {
        if !self.leak {
            self.state.lock().unwrap().timers.remove(&self.id);
        }
    }
}
impl Clock for Fixture {
    fn now(&self) -> Instant {
        self.state.lock().unwrap().now
    }
    async fn sleep_until(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        if self.fault != Fault::Cancellation {
            cancellation.check()?;
        }
        if self.now() >= deadline || self.fault == Fault::EarlyExpiry {
            return Ok(());
        }
        let id = {
            let mut state = self.state.lock().unwrap();
            state.next += 1;
            state.next
        };
        let _registration = Registration {
            state: Arc::clone(&self.state),
            id,
            leak: self.fault == Fault::LeakedTimer,
        };
        let mut cancelled = pin!(cancellation.cancelled());
        poll_fn(|context| {
            if self.fault == Fault::CancellationWins && cancellation.is_cancelled() {
                return Poll::Ready(Err(base_scheduler::Cancelled.into()));
            }
            let mut state = self.state.lock().unwrap();
            if state.now >= deadline {
                if self.fault == Fault::ExpiryError {
                    return Poll::Ready(
                        Err(std::io::Error::other("clock failed at expiry").into()),
                    );
                }
                return Poll::Ready(Ok(()));
            }
            state.timers.insert(id, (deadline, context.waker().clone()));
            if self.fault == Fault::CancellationSelfWake {
                context.waker().wake_by_ref();
                return Poll::Pending;
            }
            if cancelled.as_mut().poll(context).is_ready() {
                if self.fault == Fault::PendingSuccess {
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Err(base_scheduler::Cancelled.into()))
            } else {
                Poll::Pending
            }
        })
        .await
    }
    fn sleep(
        &self,
        duration: Duration,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send {
        let constructed = self.now();
        async move {
            if self.fault == Fault::RelativeCancellation && cancellation.is_cancelled() {
                return Ok(());
            }
            cancellation.check()?;
            let start = if self.fault == Fault::RelativeConstruction {
                constructed
            } else {
                self.now()
            };
            let deadline = start
                .checked_add(duration)
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "overflow"))?;
            self.sleep_until(deadline, cancellation).await
        }
    }
}
impl ClockFixture for Fixture {
    type Scheduler = Self;
    fn scheduler(&self) -> &Self {
        self
    }
    fn advance(&self, duration: Duration) {
        let wakes = {
            let mut state = self.state.lock().unwrap();
            state.now += duration;
            state
                .timers
                .values()
                .filter(|(deadline, _)| *deadline <= state.now)
                .map(|(_, waker)| waker.clone())
                .collect::<Vec<_>>()
        };
        if self.fault != Fault::MissingWake {
            for waker in wakes {
                waker.wake();
            }
        }
    }
    fn registrations(&self) -> usize {
        self.state.lock().unwrap().timers.len()
    }
}
fn run_case(test: impl AsyncFnOnce(&Fixture) + Send + 'static) {
    support::run(move || block_on(test(&Fixture::new(Fault::None))));
}
mod generated {
    base_scheduler_conformance::clock_tests!(run: super::run_case);
}

#[test]
fn rejects_clock_contract_violations() {
    for (fault, case) in [
        (Fault::Cancellation, 0),
        (Fault::EarlyExpiry, 1),
        (Fault::LeakedTimer, 2),
        (Fault::RelativeConstruction, 3),
        (Fault::CancellationWins, 4),
        (Fault::MissingWake, 1),
        (Fault::RelativeCancellation, 0),
        (Fault::PendingSuccess, 5),
        (Fault::CancellationSelfWake, 5),
        (Fault::ExpiryError, 6),
    ] {
        support::run(move || {
            let fixture = Fixture::new(fault);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                block_on(async {
                    match case {
                        0 => clock::pre_cancelled(&fixture).await,
                        1 => clock::deadline(&fixture).await,
                        2 => clock::registration(&fixture).await,
                        3 => clock::relative(&fixture).await,
                        4 => clock::expiry_wins(&fixture).await,
                        5 => clock::cancellation(&fixture).await,
                        _ => clock::timeout(&fixture).await,
                    }
                })
            }));
            assert!(result.is_err(), "clock suite accepted an incorrect backend");
        });
    }
}

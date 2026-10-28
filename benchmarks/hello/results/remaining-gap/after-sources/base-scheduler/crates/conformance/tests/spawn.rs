// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

mod support;
use base_scheduler::{Spawn, SpawnError, TaskError};
use base_scheduler_conformance::{SpawnFixture, spawn};
use futures_lite::future::block_on;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

#[derive(Clone, Copy, PartialEq)]
enum Fault {
    None,
    Detach,
    LosePanic,
    PollClosed,
    IgnoreClose,
    Unregistered,
    NoHandleWake,
    JoinActivePoll,
}
trait Runnable: Send + Sync {
    fn tick(&self) -> bool;
}
struct Task<T> {
    future: Mutex<Option<Pin<Box<dyn Future<Output = T> + Send>>>>,
    result: Mutex<Option<Result<T, TaskError>>>,
    waiter: Mutex<Option<Waker>>,
    abandoned: AtomicBool,
    fault: Fault,
}
impl<T: Send + 'static> Runnable for Task<T> {
    fn tick(&self) -> bool {
        let mut future = self.future.lock().unwrap();
        if self.abandoned.load(Ordering::SeqCst) {
            future.take();
            return false;
        }
        let Some(task) = future.as_mut() else {
            return false;
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            task.as_mut().poll(&mut Context::from_waker(Waker::noop()))
        }));
        let result = match result {
            Ok(Poll::Pending) => return true,
            Ok(Poll::Ready(output)) => Ok(output),
            Err(payload) if self.fault != Fault::LosePanic => Err(TaskError::Panicked(payload)),
            Err(_) => Err(TaskError::Cancelled),
        };
        future.take();
        *self.result.lock().unwrap() = Some(result);
        if let Some(waker) = self.waiter.lock().unwrap().take() {
            if self.fault != Fault::NoHandleWake {
                waker.wake();
            }
        }
        false
    }
}
struct Handle<T>(Arc<Task<T>>);
impl<T> Future for Handle<T> {
    type Output = Result<T, TaskError>;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut result = self.0.result.lock().unwrap();
        if let Some(result) = result.take() {
            Poll::Ready(result)
        } else {
            *self.0.waiter.lock().unwrap() = Some(context.waker().clone());
            if self.0.fault == Fault::NoHandleWake {
                context.waker().wake_by_ref();
            }
            Poll::Pending
        }
    }
}
impl<T> Drop for Handle<T> {
    fn drop(&mut self) {
        if self.0.fault != Fault::Detach {
            self.0.abandoned.store(true, Ordering::SeqCst);
            if self.0.fault == Fault::JoinActivePoll {
                self.0.future.lock().unwrap().take();
            } else if let Ok(mut future) = self.0.future.try_lock() {
                future.take();
            }
        }
    }
}
struct Fixture {
    registrations: Arc<AtomicUsize>,
    jobs: Mutex<Vec<Arc<dyn Runnable>>>,
    closed: AtomicBool,
    fault: Fault,
}
impl Fixture {
    fn new(fault: Fault) -> Self {
        Self {
            registrations: Arc::new(AtomicUsize::new(0)),
            jobs: Mutex::default(),
            closed: AtomicBool::new(false),
            fault,
        }
    }
}
impl Spawn for Fixture {
    type Handle<T: Send + 'static> = Handle<T>;
    fn spawn<F: Future + Send + 'static>(&self, future: F) -> Result<Handle<F::Output>, SpawnError>
    where
        F::Output: Send + 'static,
    {
        if self.closed.load(Ordering::SeqCst) && self.fault != Fault::IgnoreClose {
            if self.fault == Fault::PollClosed {
                let mut future = Box::pin(future);
                let _ = future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()));
            }
            return Err(SpawnError::SchedulerClosed);
        }
        let task = Arc::new(Task {
            future: Mutex::new(Some(Box::pin(future))),
            result: Mutex::new(None),
            waiter: Mutex::new(None),
            abandoned: AtomicBool::new(false),
            fault: self.fault,
        });
        if self.fault != Fault::Unregistered {
            self.registrations.fetch_add(1, Ordering::SeqCst);
        }
        self.jobs.lock().unwrap().push(task.clone());
        Ok(Handle(task))
    }
}
impl SpawnFixture for Fixture {
    type Scheduler = Self;
    fn scheduler(&self) -> &Self {
        self
    }
    fn registration_check(&self) -> impl FnOnce() + Send + 'static {
        let registrations = Arc::clone(&self.registrations);
        move || {
            assert!(
                registrations.load(Ordering::SeqCst) > 0,
                "task polled before registration"
            )
        }
    }
    async fn settle(&self) {
        let jobs = std::mem::take(&mut *self.jobs.lock().unwrap());
        let pending = jobs
            .into_iter()
            .filter(|job| job.tick())
            .collect::<Vec<_>>();
        self.jobs.lock().unwrap().extend(pending);
    }
    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}
fn run_case(test: impl AsyncFnOnce(&Fixture) + Send + 'static) {
    support::run(move || block_on(test(&Fixture::new(Fault::None))));
}
mod dispatch {
    base_scheduler_conformance::spawn_dispatch_tests!(run: super::run_case);
}
mod generated {
    base_scheduler_conformance::spawn_tests!(run: super::run_case);
}
#[test]
fn rejects_detaching_lost_panics_and_closed_service_violations() {
    for fault in [
        Fault::Detach,
        Fault::LosePanic,
        Fault::PollClosed,
        Fault::IgnoreClose,
        Fault::Unregistered,
        Fault::NoHandleWake,
    ] {
        support::run(move || {
            let fixture = Fixture::new(fault);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                block_on(async {
                    match fault {
                        Fault::Detach => spawn::abandonment(&fixture).await,
                        Fault::LosePanic => spawn::panic_payload(&fixture).await,
                        Fault::Unregistered => spawn::registration(&fixture).await,
                        Fault::NoHandleWake => spawn::cancellation(&fixture).await,
                        _ => spawn::closed(&fixture).await,
                    }
                })
            }));
            assert!(result.is_err(), "task suite accepted an incorrect backend");
        });
    }
}

#[test]
fn rejects_detaching_an_abandoned_active_task() {
    support::run(|| {
        let fixture = Fixture::new(Fault::Detach);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(
                spawn::active_poll(&fixture)
            )))
            .is_err()
        );
    });
}

#[test]
fn handle_drop_may_wait_for_an_active_poll() {
    support::run(|| {
        block_on(spawn::active_poll(&Fixture::new(Fault::JoinActivePoll)));
    });
}

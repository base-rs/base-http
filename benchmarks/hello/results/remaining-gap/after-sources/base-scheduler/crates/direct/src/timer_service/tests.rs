// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::test_support;
use base_scheduler_conformance::ClockFixture;
use futures_lite::future::{block_on, poll_once};
use std::sync::{
    Weak,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, RawWaker, RawWakerVTable};
use std::time::Duration;

pub(super) fn observe_wait(state: &State, deadline: bool) {
    if let Some(waits) = &state.waits {
        let _ = waits.send(deadline);
    }
}
fn wait_for_driver(waits: &std::sync::mpsc::Receiver<bool>, deadline: bool) {
    while waits.recv_timeout(Duration::from_secs(5)).unwrap() != deadline {}
}

impl ClockFixture for DirectScheduler {
    type Scheduler = Self;
    fn scheduler(&self) -> &Self {
        self
    }
    fn advance(&self, duration: Duration) {
        let wakes = {
            let mut state = self.timers.shared.state.lock().unwrap();
            *state.paused.as_mut().unwrap() += duration;
            state.expired()
        };
        for waker in wakes {
            waker.wake();
        }
        self.timers.shared.changed.notify_all();
    }
    fn registrations(&self) -> usize {
        self.timers.shared.state.lock().unwrap().timers.len()
    }
}
fn run(test: impl AsyncFnOnce(&DirectScheduler) + Send + 'static) {
    test_support::run(move || {
        let scheduler = test_support::scheduler();
        scheduler.timers.shared.state.lock().unwrap().paused = Some(Instant::now());
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(test(&scheduler))));
        block_on(scheduler.shutdown());
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
mod conformance {
    base_scheduler_conformance::clock_tests!(run: super::run);
}

#[test]
fn native_timer_driver_wakes_and_retires_deadlines() {
    test_support::run(|| {
        block_on(async {
            let scheduler = test_support::scheduler();
            let (waits, observed) = std::sync::mpsc::channel();
            scheduler.timers.shared.state.lock().unwrap().waits = Some(waits);
            scheduler.timers.start().unwrap();
            wait_for_driver(&observed, false);
            let never = Cancellation::never();
            let mut distant = Box::pin(scheduler.sleep(Duration::from_secs(3600), &never));
            assert!(poll_once(&mut distant).await.is_none());
            wait_for_driver(&observed, true);
            drop(distant);
            wait_for_driver(&observed, false);
            let deadline = scheduler.now() + Duration::from_millis(50);
            scheduler
                .sleep_until(deadline, &Cancellation::never())
                .await
                .unwrap();
            assert!(scheduler.now() >= deadline);
            scheduler.shutdown().await;
        })
    });
}
#[test]
fn startup_failure_exhaustion_and_shutdown_release_waiters() {
    run(async |scheduler| {
        scheduler.timers.shared.state.lock().unwrap().paused = None;
        let never = Cancellation::never();
        thread::FAIL_AFTER.set(Some(0));
        assert!(
            scheduler
                .sleep(Duration::from_secs(10), &never)
                .await
                .is_err()
        );
        scheduler.timers.shared.state.lock().unwrap().next = usize::MAX;
        assert!(
            scheduler
                .sleep(Duration::from_secs(10), &never)
                .await
                .is_err()
        );
        scheduler.timers.shared.state.lock().unwrap().next = 0;
        let mut wait = Box::pin(scheduler.sleep(Duration::from_secs(10), &never));
        assert!(poll_once(&mut wait).await.is_none());
        scheduler.close();
        assert_eq!(wait.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    });
}

struct WakerProbe {
    shared: Weak<Shared>,
    cloned: AtomicUsize,
    dropped: AtomicUsize,
    locked: AtomicUsize,
}

impl WakerProbe {
    const VTABLE: RawWakerVTable =
        RawWakerVTable::new(Self::clone, Self::wake, Self::wake_by_ref, Self::drop);

    fn observe(&self, callback: &AtomicUsize) {
        callback.fetch_add(1, Ordering::Relaxed);
        if self.shared.upgrade().unwrap().state.try_lock().is_err() {
            self.locked.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn waker(self: Arc<Self>) -> Waker {
        let raw = RawWaker::new(Arc::into_raw(self).cast(), &Self::VTABLE);
        // SAFETY: Every raw waker owns one Arc, with thread-safe clone, wake and drop callbacks.
        unsafe { Waker::from_raw(raw) }
    }

    unsafe fn clone(data: *const ()) -> RawWaker {
        // SAFETY: A live raw waker retains this allocation throughout the callback.
        let probe = unsafe { &*data.cast::<Self>() };
        probe.observe(&probe.cloned);
        // SAFETY: The new raw waker receives its own strong reference.
        unsafe { Arc::increment_strong_count(data.cast::<Self>()) };
        RawWaker::new(data, &Self::VTABLE)
    }

    unsafe fn wake(data: *const ()) {
        // SAFETY: Consuming wake releases exactly the reference owned by its raw waker.
        unsafe { Self::drop(data) };
    }

    unsafe fn wake_by_ref(_data: *const ()) {}

    unsafe fn drop(data: *const ()) {
        // SAFETY: Each raw waker transfers its strong reference here exactly once.
        let probe = unsafe { Arc::from_raw(data.cast::<Self>()) };
        probe.observe(&probe.dropped);
    }
}

#[test]
fn timer_waker_callbacks_run_outside_the_registry_lock() {
    run(async |scheduler| {
        let probe = Arc::new(WakerProbe {
            shared: Arc::downgrade(&scheduler.timers.shared),
            cloned: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            locked: AtomicUsize::new(0),
        });
        let waker = Arc::clone(&probe).waker();
        let mut context = Context::from_waker(&waker);
        let never = Cancellation::never();
        let deadline = scheduler.now() + Duration::from_secs(3600);

        // Registration clones the waker; repolling and abandonment release stored wakers.
        let mut sleep = Box::pin(scheduler.sleep_until(deadline, &never));
        assert!(sleep.as_mut().poll(&mut context).is_pending());
        assert_eq!(probe.cloned.load(Ordering::Relaxed), 1);
        assert_eq!(probe.locked.load(Ordering::Relaxed), 0);
        assert!(sleep.as_mut().poll(&mut context).is_pending());
        assert_eq!(probe.cloned.load(Ordering::Relaxed), 2);
        assert_eq!(probe.dropped.load(Ordering::Relaxed), 1);
        assert_eq!(probe.locked.load(Ordering::Relaxed), 0);
        drop(sleep);
        assert_eq!(probe.dropped.load(Ordering::Relaxed), 2);
        assert_eq!(probe.locked.load(Ordering::Relaxed), 0);
        assert_eq!(scheduler.registrations(), 0);

        // Rejected registrations also release their new waker after unlocking.
        scheduler.timers.shared.state.lock().unwrap().next = usize::MAX;
        let mut exhausted = Box::pin(scheduler.sleep_until(deadline, &never));
        assert!(exhausted.as_mut().poll(&mut context).is_ready());
        scheduler.close();
        let mut closed = Box::pin(scheduler.sleep_until(deadline, &never));
        assert!(closed.as_mut().poll(&mut context).is_ready());
        assert_eq!(probe.cloned.load(Ordering::Relaxed), 4);
        assert_eq!(probe.dropped.load(Ordering::Relaxed), 4);

        waker.wake_by_ref();
        waker.wake();
        assert_eq!(probe.locked.load(Ordering::Relaxed), 0);
    });
}

#[test]
fn reusable_timer_startup_and_identifier_exhaustion() {
    use base_scheduler::Timers;
    run(async |scheduler| {
        scheduler.timers.shared.state.lock().unwrap().paused = None;
        let mut timer = scheduler.timer();
        let never = Cancellation::never();
        let far = scheduler.now() + Duration::from_secs(30);
        thread::FAIL_AFTER.set(Some(0));
        assert!(
            scheduler
                .timer_wait_until(&mut timer, far, &never)
                .await
                .is_err()
        );
        scheduler.timers.shared.state.lock().unwrap().next = usize::MAX;
        assert!(
            scheduler
                .timer_wait_until(&mut timer, far, &never)
                .await
                .is_err()
        );
    });
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::Cancellation;
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Wake, Waker};

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn checks_clones_and_first_and_repeated_waits_do_not_allocate() {
    let root = Cancellation::new();
    let child = root.child();
    let never = Cancellation::never();
    let first = Waker::from(Arc::new(WakeCounter::default()));
    let second = Waker::from(Arc::new(WakeCounter::default()));

    // Count on the polling thread, excluding token and executor-waker setup. In particular, do not warm up the token with an unmeasured first wait.
    let allocations = allocation_counter::measure(|| {
        for token in [&root, &child, &never] {
            for _ in 0..16 {
                let clone = token.clone();
                assert!(!clone.is_cancelled());
                assert!(clone.check().is_ok());
                let mut waiting = pin!(clone.cancelled());
                let mut concurrent = pin!(token.cancelled());
                assert!(
                    waiting
                        .as_mut()
                        .poll(&mut Context::from_waker(&first))
                        .is_pending()
                );
                assert!(
                    concurrent
                        .as_mut()
                        .poll(&mut Context::from_waker(&first))
                        .is_pending()
                );
                assert!(
                    waiting
                        .as_mut()
                        .poll(&mut Context::from_waker(&second))
                        .is_pending()
                );
                // Both registered waits are dropped without cancelling the signal.
            }
        }
    });
    assert_eq!(allocations.count_total, 0);
}

#[test]
fn observing_cancellation_does_not_allocate() {
    let token = Cancellation::new();
    let mut waiting = pin!(token.cancelled());
    let counter = Arc::new(WakeCounter::default());
    let waker = &Waker::from(Arc::clone(&counter));
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(waker))
            .is_pending()
    );
    // Cancellation propagation itself is outside the allocation-free contract.
    token.cancel();
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);

    let allocations = allocation_counter::measure(|| {
        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(waker))
                .is_ready()
        );
        assert!(token.check().is_err());
        for _ in 0..16 {
            let mut already_cancelled = pin!(token.cancelled());
            assert!(
                already_cancelled
                    .as_mut()
                    .poll(&mut Context::from_waker(waker))
                    .is_ready()
            );
        }
    });
    assert_eq!(allocations.count_total, 0);
}

#[test]
fn a_registered_wait_can_be_polled_on_another_thread() {
    let token = Cancellation::new();
    let mut waiting = pin!(token.cancelled());
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                assert!(
                    waiting
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
            })
            .join()
            .unwrap();
    });
    token.cancel();
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
}

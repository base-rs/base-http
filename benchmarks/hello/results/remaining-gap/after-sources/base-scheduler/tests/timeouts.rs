// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{Cancellation, Clock, OperationError, OperationResult, TimeoutError};
use futures_lite::future::{block_on, poll_once};
use std::{
    error::Error,
    future::{Future, pending, poll_fn, ready},
    pin::pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

// One controlled timer, with all storage allocated before polling.
struct TestClock {
    state: Mutex<State>,
}

struct State {
    now: Instant,
    reads: usize,
    deadline: Option<Instant>,
    waker: Option<Waker>,
}

impl TestClock {
    fn new() -> Self {
        let clock = Self {
            state: Mutex::new(State {
                now: Instant::now(),
                reads: 0,
                deadline: None,
                waker: None,
            }),
        };
        // Some platforms initialize a mutex's native storage on its first lock.
        drop(clock.state.lock().unwrap());
        clock
    }

    fn advance(&self, duration: Duration) {
        let wake = {
            let mut state = self.state.lock().unwrap();
            state.now += duration;
            state.waker.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}

struct Registration<'a>(&'a TestClock);

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap();
        state.deadline = None;
        state.waker = None;
    }
}

impl Clock for TestClock {
    fn now(&self) -> Instant {
        let mut state = self.state.lock().unwrap();
        state.reads += 1;
        state.now
    }

    async fn sleep_until(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        let _registration = Registration(self);
        let mut cancelled = pin!(cancellation.cancelled());
        poll_fn(|context| {
            let mut state = self.state.lock().unwrap();
            if state.now >= deadline {
                return Poll::Ready(Ok(()));
            }
            if cancelled.as_mut().poll(context).is_ready() {
                return Poll::Ready(Err(base_scheduler::Cancelled.into()));
            }
            state.deadline = Some(deadline);
            state.waker = Some(context.waker().clone());
            Poll::Pending
        })
        .await
    }
}

struct DropFlag<'a>(&'a AtomicBool);

impl Drop for DropFlag<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

#[test]
fn ready_operations_preserve_outputs_without_reading_the_clock_or_allocating() {
    let clock = TestClock::new();
    let allocations = allocation_counter::measure(|| {
        for duration in [Duration::ZERO, Duration::MAX] {
            let mut timeout = pin!(clock.timeout(duration, ready(Err::<(), _>("queue closed"))));
            assert!(matches!(
                timeout
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Ok(Err("queue closed")))
            ));
        }
    });
    assert_eq!(allocations.count_total, 0);
    let state = clock.state.lock().unwrap();
    assert_eq!(state.reads, 0);
    assert!(state.deadline.is_none());
}

#[test]
fn timeout_is_lazy_and_dropping_an_unpolled_timeout_drops_its_operation() {
    let clock = TestClock::new();
    let dropped = AtomicBool::new(false);
    let guard = DropFlag(&dropped);
    let timeout = clock.timeout(Duration::from_secs(5), async move {
        let _guard = guard;
        pending::<()>().await;
    });
    assert_eq!(clock.state.lock().unwrap().reads, 0);
    assert!(!dropped.load(Ordering::Relaxed));
    drop(timeout);
    assert!(dropped.load(Ordering::Relaxed));
    assert_eq!(clock.state.lock().unwrap().reads, 0);
}

#[test]
fn relative_timeout_registers_only_when_polled_and_expires_in_its_clock_domain() {
    let clock = TestClock::new();
    let dropped = AtomicBool::new(false);
    let mut timeout = pin!(clock.timeout(Duration::from_secs(5), async {
        let _guard = DropFlag(&dropped);
        pending::<()>().await;
    }));
    clock.advance(Duration::from_secs(20));
    let start = clock.now();
    assert!(block_on(poll_once(&mut timeout)).is_none());
    assert_eq!(
        clock.state.lock().unwrap().deadline,
        Some(start + Duration::from_secs(5))
    );
    clock.advance(Duration::from_secs(4));
    assert!(block_on(poll_once(&mut timeout)).is_none());
    clock.advance(Duration::from_secs(1));
    assert!(matches!(block_on(&mut timeout), Err(TimeoutError::Elapsed)));
    // Cleanup happens on completion, even if the completed wrapper remains alive.
    assert!(dropped.load(Ordering::Relaxed));
    assert!(clock.state.lock().unwrap().deadline.is_none());
}

#[test]
fn operation_completion_wins_expiry_and_releases_the_timer_before_returning() {
    let clock = TestClock::new();
    let (send, receive) = futures_channel::oneshot::channel();
    let mut timeout = pin!(clock.timeout(Duration::from_secs(5), receive));
    assert!(block_on(poll_once(&mut timeout)).is_none());
    send.send(String::from("message")).unwrap();
    clock.advance(Duration::from_secs(5));
    assert_eq!(block_on(&mut timeout).unwrap().unwrap(), "message");
    assert!(clock.state.lock().unwrap().deadline.is_none());
}

#[test]
fn abandoning_a_pending_timeout_releases_both_waits() {
    let clock = TestClock::new();
    let cancellation = Cancellation::new();
    let dropped = AtomicBool::new(false);
    let probe = Arc::new(WakeCounter::default());
    let waker = Waker::from(Arc::clone(&probe));
    {
        let mut timeout = pin!(clock.timeout(Duration::from_secs(5), async {
            let _guard = DropFlag(&dropped);
            cancellation.cancelled().await;
        }));
        assert!(
            timeout
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(clock.state.lock().unwrap().deadline.is_some());
    }
    assert!(dropped.load(Ordering::Relaxed));
    assert!(clock.state.lock().unwrap().deadline.is_none());
    cancellation.cancel();
    clock.advance(Duration::from_secs(5));
    assert_eq!(probe.0.load(Ordering::Relaxed), 0);
}

#[test]
fn timeout_composition_and_timer_cleanup_do_not_allocate() {
    let clock = TestClock::new();
    let allocations = allocation_counter::measure(|| {
        let (send, receive) = (AtomicBool::new(false), AtomicBool::new(false));
        let operation = poll_fn(|_| {
            if send.load(Ordering::Relaxed) {
                receive.store(true, Ordering::Relaxed);
                Poll::Ready(42)
            } else {
                Poll::Pending
            }
        });
        let mut timeout = pin!(clock.timeout(Duration::from_secs(5), operation));
        let mut context = Context::from_waker(Waker::noop());
        assert!(timeout.as_mut().poll(&mut context).is_pending());
        assert!(timeout.as_mut().poll(&mut context).is_pending());
        send.store(true, Ordering::Relaxed);
        assert!(matches!(
            timeout.as_mut().poll(&mut context),
            Poll::Ready(Ok(42))
        ));
        assert!(receive.load(Ordering::Relaxed));
    });
    assert_eq!(allocations.count_total, 0);
    assert!(clock.state.lock().unwrap().deadline.is_none());
}

#[test]
fn zero_duration_expires_a_pending_operation_and_overflow_is_a_clock_error() {
    let clock = TestClock::new();
    assert!(matches!(
        block_on(clock.timeout(Duration::ZERO, pending::<()>())),
        Err(TimeoutError::Elapsed)
    ));
    let dropped = AtomicBool::new(false);
    let error = block_on(clock.timeout(Duration::MAX, async {
        let _guard = DropFlag(&dropped);
        pending::<()>().await;
    }))
    .unwrap_err();
    assert!(matches!(
        error,
        TimeoutError::Clock(OperationError::Io(error))
            if error.kind() == std::io::ErrorKind::InvalidInput
    ));
    assert!(dropped.load(Ordering::Relaxed));
    assert!(clock.state.lock().unwrap().deadline.is_none());
}

#[test]
fn timeout_errors_report_expiry_and_preserve_the_clock_error_chain() {
    let elapsed = TimeoutError::Elapsed;
    assert_eq!(elapsed.to_string(), "operation timed out");
    assert!(elapsed.source().is_none());
    let error = TimeoutError::Clock(std::io::Error::other("clock closed").into());
    assert_eq!(error.to_string(), "timeout clock failed: clock closed");
    assert_eq!(error.source().unwrap().to_string(), "clock closed");
    assert_eq!(
        error.source().unwrap().source().unwrap().to_string(),
        "clock closed"
    );
}

#[test]
fn generic_clock_timeouts_are_send_and_can_borrow_the_operation() {
    fn send_future(_: impl Future + Send) {}
    fn generic<C: Clock>(clock: &C) {
        let mut value = 42;
        send_future(clock.timeout(Duration::from_secs(5), async { &mut value }));
    }
    generic(&TestClock::new());
}

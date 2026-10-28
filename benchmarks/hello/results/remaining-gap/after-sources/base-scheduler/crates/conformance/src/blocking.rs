// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Blocking-work assertions. The caller drives futures on its own executor; synchronous closures run on the backend's blocking workers.
pub use crate::BlockingFixture;
use base_scheduler::{Blocking, BlockingError, Cancellation, TaskError};
use futures_lite::future::poll_once;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct CountDrop(
    Arc<AtomicUsize>,
    Option<futures_channel::oneshot::Sender<()>>,
);
impl Drop for CountDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        if let Some(completion) = self.1.take() {
            let _ = completion.send(());
        }
    }
}

/// Check lazy and cancelled admission, successful output, and preservation of panic payloads.
pub async fn lifecycle<S: Blocking>(scheduler: &S) {
    lazy(scheduler).await;
    pre_cancelled(scheduler).await;
    output(scheduler).await;
    panic_payload(scheduler).await;
}

/// An unpolled blocking submission drops its captures without executing the closure.
pub async fn lazy<S: Blocking>(scheduler: &S) {
    let drops = Arc::new(AtomicUsize::new(0));
    let capture = CountDrop(Arc::clone(&drops), None);
    let calls = Arc::new(AtomicUsize::new(0));
    let called = Arc::clone(&calls);
    let never = Cancellation::never();
    drop(scheduler.spawn_blocking(
        move || {
            called.fetch_add(1, Ordering::SeqCst);
            drop(capture);
            1
        },
        &never,
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// Pre-cancelled blocking submission rejects execution and releases captures.
pub async fn pre_cancelled<S: Blocking>(scheduler: &S) {
    let drops = Arc::new(AtomicUsize::new(0));
    let token = Cancellation::new();
    token.cancel();
    let capture = CountDrop(Arc::clone(&drops), None);
    let result = scheduler
        .spawn_blocking(
            move || {
                drop(capture);
                2
            },
            &token,
        )
        .await;
    assert!(matches!(result, Err(BlockingError::Cancelled)));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// Blocking handles return the closure's output.
pub async fn output<S: Blocking>(scheduler: &S) {
    let never = Cancellation::never();
    assert_eq!(
        scheduler
            .spawn_blocking(|| 42, &never)
            .await
            .unwrap()
            .await
            .unwrap(),
        42
    );
}

/// Blocking handles preserve the original panic payload.
pub async fn panic_payload<S: Blocking>(scheduler: &S) {
    let never = Cancellation::never();
    let result = scheduler
        .spawn_blocking(
            || std::panic::panic_any(String::from("conformance panic")),
            &never,
        )
        .await
        .unwrap()
        .await;
    let TaskError::Panicked(payload) = result.unwrap_err() else {
        panic!("expected retained panic payload");
    };
    assert_eq!(*payload.downcast::<String>().unwrap(), "conformance panic");
}

/// Check queued cancellation and bounded, cancellation-aware admission while workers are held by the fixture.
pub async fn queue<F: BlockingFixture>(fixture: &F) {
    queued_cancellation(fixture).await;
    abandoned_handle(fixture).await;
}

/// Cancel queued work and cancellation-aware admission while execution is held.
pub async fn queued_cancellation<F: BlockingFixture>(fixture: &F) {
    let scheduler = fixture.scheduler();
    let guard = fixture.hold_workers().await;
    let token = Cancellation::new();
    let queued = scheduler
        .spawn_blocking(|| panic!("cancelled queued job ran"), &token)
        .await
        .unwrap();
    let waiting_token = Cancellation::new();
    let mut waiting =
        std::pin::pin!(scheduler.spawn_blocking(|| panic!("unadmitted job ran"), &waiting_token));
    assert!(poll_once(waiting.as_mut()).await.is_none());
    waiting_token.cancel();
    assert!(matches!(waiting.await, Err(BlockingError::Cancelled)));
    token.cancel();
    drop(guard);
    assert!(matches!(queued.await, Err(TaskError::Cancelled)));
}

/// Dropping a queued handle releases its capture without running the closure.
pub async fn abandoned_handle<F: BlockingFixture>(fixture: &F) {
    let scheduler = fixture.scheduler();
    let guard = fixture.hold_workers().await;
    let drops = Arc::new(AtomicUsize::new(0));
    let (completion, completed) = futures_channel::oneshot::channel();
    let capture = CountDrop(Arc::clone(&drops), Some(completion));
    let calls = Arc::new(AtomicUsize::new(0));
    let called = Arc::clone(&calls);
    let never = Cancellation::never();
    let queued = scheduler
        .spawn_blocking(
            move || {
                called.fetch_add(1, Ordering::SeqCst);
                drop(capture);
            },
            &never,
        )
        .await
        .unwrap();
    drop(queued);
    drop(guard);
    completed.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

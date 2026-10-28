// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Owning task-handle assertions, independent of executor and scheduling order.

use crate::{
    SpawnFixture,
    probe::{DropProbe, WakeProbe},
};
use base_scheduler::{Cancellation, Spawn, SpawnError, TaskError};
use std::{
    future::poll_fn,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

/// Awaiting returns the output; moving a handle transfers ownership and does not abort the task.
pub async fn output<F: SpawnFixture>(fixture: &F) {
    let drops = Arc::new(AtomicUsize::new(0));
    let capture = DropProbe(Arc::clone(&drops));
    let handle = fixture.scheduler().spawn(async move { capture }).unwrap();
    let handle = std::thread::spawn(move || handle).join().unwrap();
    fixture.settle().await;
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let output = handle.await.unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(output);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// An unconsumed successful result is destroyed when its owning handle is dropped.
pub async fn completed_abandonment<F: SpawnFixture>(fixture: &F) {
    let drops = Arc::new(AtomicUsize::new(0));
    let capture = DropProbe(Arc::clone(&drops));
    let handle = fixture.scheduler().spawn(async move { capture }).unwrap();
    fixture.settle().await;
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(handle);
    fixture.settle().await;
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// Registration is visible before a task can be polled.
pub async fn registration<F: SpawnFixture>(fixture: &F) {
    let check = fixture.registration_check();
    let handle = fixture
        .scheduler()
        .spawn(async move {
            check();
        })
        .unwrap();
    fixture.settle().await;
    handle.await.unwrap();
}

/// Panics are retained as task errors with their original payload.
pub async fn panic_payload<F: SpawnFixture>(fixture: &F) {
    let handle = fixture
        .scheduler()
        .spawn(async { std::panic::panic_any(String::from("task conformance panic")) })
        .unwrap();
    fixture.settle().await;
    let TaskError::Panicked(payload) = handle.await.unwrap_err() else {
        panic!("expected original task panic");
    };
    assert_eq!(
        *payload.downcast::<String>().unwrap(),
        "task conformance panic"
    );
}

/// A token captured explicitly by a task supports cooperative cancellation, waking its pending owning handle on completion.
pub async fn cancellation<F: SpawnFixture>(fixture: &F) {
    let token = Cancellation::new();
    let child = token.clone();
    let handle = fixture
        .scheduler()
        .spawn(async move {
            child.cancelled().await;
            73
        })
        .unwrap();
    fixture.settle().await;
    let mut handle = pin!(handle);
    let probe = Arc::new(WakeProbe::default());
    let waker = Waker::from(Arc::clone(&probe));
    assert!(
        handle
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    probe.0.store(0, Ordering::SeqCst);
    token.cancel();
    fixture.settle().await;
    assert!(
        probe.0.load(Ordering::SeqCst) > 0,
        "completed task did not wake its owning handle"
    );
    assert_eq!(handle.await.unwrap(), 73);
}

/// Dropping an owning handle destroys a pending task and its captures. No task body continuation is required for cleanup.
pub async fn abandonment<F: SpawnFixture>(fixture: &F) {
    for poll_first in [false, true] {
        let drops = Arc::new(AtomicUsize::new(0));
        let polls = Arc::new(AtomicUsize::new(0));
        let capture = DropProbe(Arc::clone(&drops));
        let polled = Arc::clone(&polls);
        let handle = fixture
            .scheduler()
            .spawn(poll_fn(move |_| {
                let _capture = &capture;
                polled.fetch_add(1, Ordering::SeqCst);
                Poll::<()>::Pending
            }))
            .unwrap();
        if poll_first {
            fixture.settle().await;
            assert!(polls.load(Ordering::SeqCst) > 0);
        }
        drop(handle);
        fixture.settle().await;
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "dropping the handle detached the task"
        );
    }
}

/// A closed service rejects and destroys an input without polling it.
pub async fn closed<F: SpawnFixture>(fixture: &F) {
    fixture.close();
    let drops = Arc::new(AtomicUsize::new(0));
    let capture = DropProbe(Arc::clone(&drops));
    let polls = Arc::new(AtomicUsize::new(0));
    let polled = Arc::clone(&polls);
    let result = fixture.scheduler().spawn(async move {
        let _capture = capture;
        polled.fetch_add(1, Ordering::SeqCst);
    });
    assert!(matches!(result, Err(SpawnError::SchedulerClosed)));
    fixture.settle().await;
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

struct PollingTask {
    entered: std::sync::mpsc::Sender<()>,
    released: std::sync::mpsc::Receiver<()>,
    polls: Arc<AtomicUsize>,
    _capture: DropProbe,
}
impl std::future::Future for PollingTask {
    type Output = ();
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<()> {
        if self.polls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.entered.send(()).unwrap();
            self.released
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("poll release deadline exceeded");
        }
        Poll::Pending
    }
}
struct Release(std::sync::mpsc::Sender<()>);
impl Drop for Release {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

/// An active task stays alive through its poll and is destroyed after its owning handle is dropped. Drop may wait for the active poll to return. Requires a fixture shareable with its background executor driver, without adding bounds to `Spawn`.
pub async fn active_poll<F: SpawnFixture + Sync>(fixture: &F) {
    let drops = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0));
    let (entered, started) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let handle = fixture
        .scheduler()
        .spawn(PollingTask {
            entered,
            released,
            polls: Arc::clone(&polls),
            _capture: DropProbe(Arc::clone(&drops)),
        })
        .unwrap();
    std::thread::scope(|scope| {
        let release = Release(release);
        let driving = scope.spawn(|| futures_lite::future::block_on(fixture.settle()));
        started
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("task poll start deadline exceeded");
        let dropping = scope.spawn(move || {
            drop(handle);
        });
        assert_eq!(
            drops.load(Ordering::SeqCst),
            0,
            "task destroyed during its active poll"
        );
        drop(release);
        dropping.join().unwrap();
        driving.join().unwrap();
    });
    fixture.settle().await;
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Signal subscription ownership and controlled-delivery assertions.

use crate::{SignalsFixture, probe::DropProbe};
use base_scheduler::{Cancellation, Notify, Signals};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};

struct Target<T> {
    events: Arc<Mutex<Vec<T>>>,
    _capture: DropProbe,
}
impl<T: Send> Notify<T> for Target<T> {
    fn notify(&self, signal: T) {
        self.events.lock().unwrap().push(signal);
    }
}

/// Subscriptions select signals independently; dropping one leaves the other live and releases its target.
pub async fn subscriptions<F: SignalsFixture>(fixture: &F) {
    let [first, second] = fixture.signals();
    assert_ne!(first, second);
    let events = Arc::new(Mutex::new(Vec::new()));
    let drops = Arc::new(AtomicUsize::new(0));
    let first_subscription = fixture
        .scheduler()
        .signal_subscribe(
            &[first],
            Target {
                events: Arc::clone(&events),
                _capture: DropProbe(Arc::clone(&drops)),
            },
        )
        .unwrap();
    let (sender, receiver) = mpsc::channel();
    let second_subscription = fixture
        .scheduler()
        .signal_subscribe(&[second], sender)
        .unwrap();
    fixture.emit(second).await;
    assert!(events.lock().unwrap().is_empty());
    assert_eq!(receiver.try_recv().unwrap(), second);
    fixture.emit(first).await;
    assert_eq!(*events.lock().unwrap(), [first]);
    assert!(receiver.try_recv().is_err());
    drop(first_subscription);
    fixture.emit(first).await;
    assert_eq!(*events.lock().unwrap(), [first]);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    fixture.emit(second).await;
    assert_eq!(receiver.try_recv().unwrap(), second);
    drop(second_subscription);
    fixture.emit(second).await;
    assert!(receiver.try_recv().is_err());
}

/// Multiple subscriptions to one signal are independent; a cancellation token is a valid target.
pub async fn multiple_targets<F: SignalsFixture>(fixture: &F) {
    let [first, second] = fixture.signals();
    let token = Cancellation::new();
    let (sender, receiver) = mpsc::channel();
    let subscription = fixture
        .scheduler()
        .signal_subscribe(&[first, second], token.clone())
        .unwrap();
    let other = fixture
        .scheduler()
        .signal_subscribe(&[first, second], sender)
        .unwrap();
    fixture.emit(first).await;
    assert!(token.is_cancelled());
    assert_eq!(receiver.try_recv().unwrap(), first);
    fixture.emit(second).await;
    assert_eq!(receiver.try_recv().unwrap(), second);
    drop(subscription);
    fixture.emit(first).await;
    assert_eq!(receiver.try_recv().unwrap(), first);
    drop(other);
}

/// Setup failure is synchronous and releases the supplied target after backend cleanup quiesces.
pub async fn errors<F: SignalsFixture>(fixture: &F) {
    let drops = Arc::new(AtomicUsize::new(0));
    let result = fixture.scheduler().signal_subscribe(
        &[fixture.invalid_signal()],
        Target {
            events: Arc::default(),
            _capture: DropProbe(Arc::clone(&drops)),
        },
    );
    assert!(
        result.is_err(),
        "invalid subscription unexpectedly succeeded"
    );
    fixture.emit(fixture.signals()[0]).await;
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

struct Reentrant<S: Signals> {
    scheduler: Arc<S>,
    calls: Arc<AtomicUsize>,
}
impl<S: Signals> Notify<S::Signal> for Reentrant<S> {
    fn notify(&self, signal: S::Signal) {
        let subscription = self
            .scheduler
            .signal_subscribe(&[signal], Cancellation::never())
            .unwrap();
        drop(subscription);
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

/// A callback can subscribe/unsubscribe without deadlocking on dispatch locks.
pub async fn reentrant<F: SignalsFixture>(fixture: &F) {
    let [first, _] = fixture.signals();
    let calls = Arc::new(AtomicUsize::new(0));
    let subscription = fixture
        .scheduler()
        .signal_subscribe(
            &[first],
            Reentrant {
                scheduler: Arc::clone(fixture.scheduler()),
                calls: Arc::clone(&calls),
            },
        )
        .unwrap();
    fixture.emit(first).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(subscription);
}

struct InProgress {
    entered: mpsc::Sender<()>,
    released: Mutex<mpsc::Receiver<()>>,
    _capture: DropProbe,
}
impl<T> Notify<T> for InProgress {
    fn notify(&self, _: T) {
        self.entered.send(()).unwrap();
        self.released
            .lock()
            .unwrap()
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("callback release deadline exceeded");
    }
}
struct Release(mpsc::Sender<()>);
impl Drop for Release {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

/// Unsubscribe does not synchronously join an active callback, and its target stays alive until dispatch finishes. Requires a fixture shareable with its background dispatch driver; no additional bound is imposed on `Signals`.
pub async fn in_progress<F: SignalsFixture + Sync>(fixture: &F) {
    let [signal, _] = fixture.signals();
    let drops = Arc::new(AtomicUsize::new(0));
    let (entered, started) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let subscription = fixture
        .scheduler()
        .signal_subscribe(
            &[signal],
            InProgress {
                entered,
                released: Mutex::new(released),
                _capture: DropProbe(Arc::clone(&drops)),
            },
        )
        .unwrap();
    std::thread::scope(|scope| {
        let release = Release(release);
        let emitting = scope.spawn(move || futures_lite::future::block_on(fixture.emit(signal)));
        started
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("dispatch start deadline exceeded");
        let (finished, done) = mpsc::channel();
        let dropping = scope.spawn(move || {
            drop(subscription);
            finished.send(()).unwrap();
        });
        done.recv_timeout(std::time::Duration::from_secs(2))
            .expect("unsubscribe synchronously joined active dispatch");
        assert_eq!(
            drops.load(Ordering::SeqCst),
            0,
            "target released during dispatch"
        );
        drop(release);
        dropping.join().unwrap();
        emitting.join().unwrap();
    });
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use std::{
    mem::ManuallyDrop,
    sync::Arc,
    task::{RawWaker, RawWakerVTable, Wake},
};

#[derive(Default)]
struct Count(AtomicUsize);
impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}
fn counter() -> (Arc<Count>, Waker) {
    let count = Arc::new(Count::default());
    let waker = Waker::from(Arc::clone(&count));
    (count, waker)
}
fn pending(listener: &mut Listener<'_>, waker: &Waker) {
    assert!(listener.poll(&mut Context::from_waker(waker)).is_pending());
}
fn ready(listener: &mut Listener<'_>, waker: &Waker) {
    assert!(matches!(
        listener.poll(&mut Context::from_waker(waker)),
        Poll::Ready(Ok(()))
    ));
}

#[test]
fn broadcast_replaces_wakers_and_abandonment_only_removes_its_owner() {
    let notification = Notification::new();
    let (first, first_waker) = counter();
    let (second, second_waker) = counter();
    let (replacement, replacement_waker) = counter();
    let mut one = notification.listen();
    let mut two = notification.listen();
    let mut abandoned = notification.listen();
    pending(&mut one, &first_waker);
    pending(&mut one, &first_waker);
    pending(&mut two, &second_waker);
    pending(&mut two, &replacement_waker);
    pending(&mut abandoned, &second_waker);
    drop(abandoned);
    notification.notify();
    assert_eq!(first.0.load(Ordering::Relaxed), 1);
    assert_eq!(second.0.load(Ordering::Relaxed), 0);
    assert_eq!(replacement.0.load(Ordering::Relaxed), 1);
    ready(&mut one, &first_waker);
    ready(&mut two, &replacement_waker);
}

#[test]
fn notification_cutoff_does_not_cover_new_listeners_or_reused_slots() {
    let notification = Notification::new();
    let (count, waker) = counter();
    notification.notify();
    let mut old = notification.listen();
    pending(&mut old, &waker);
    notification.notify();
    let mut new = notification.listen();
    pending(&mut new, &waker);
    assert_eq!(old.key.unwrap().1, new.key.unwrap().1);
    ready(&mut old, &waker);
    drop(old);
    // The old guard cannot remove or complete the new occupant of its slot.
    pending(&mut new, &waker);
    notification.notify();
    assert_eq!(count.0.load(Ordering::Relaxed), 2);
    ready(&mut new, &waker);
}

#[test]
fn simultaneous_notifications_deliver_each_registration_once() {
    let notification = Notification::new();
    let (count, waker) = counter();
    let mut listeners = (0..8).map(|_| notification.listen()).collect::<Vec<_>>();
    for listener in &mut listeners {
        pending(listener, &waker);
    }
    let barrier = std::sync::Barrier::new(3);
    std::thread::scope(|scope| {
        for _ in 0..3 {
            scope.spawn(|| {
                barrier.wait();
                notification.notify();
            });
        }
    });
    assert_eq!(count.0.load(Ordering::Relaxed), 8);
    for listener in &mut listeners {
        ready(listener, &waker);
    }
}

#[test]
fn racing_cancellation_and_notification_leave_no_stale_waiters() {
    let notification = Notification::new();
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        let sender = scope.spawn(|| {
            for _ in 0..1024 {
                barrier.wait();
                notification.notify();
                barrier.wait();
            }
        });
        for _ in 0..1024 {
            let mut listener = notification.listen();
            pending(&mut listener, Waker::noop());
            barrier.wait();
            drop(listener);
            barrier.wait();
        }
        sender.join().unwrap();
    });
    assert!(
        notification
            .state
            .lock()
            .unwrap()
            .entries
            .iter()
            .all(Option::is_none)
    );
    let mut listener = notification.listen();
    pending(&mut listener, Waker::noop());
    notification.notify();
    ready(&mut listener, Waker::noop());
}

type Callback = Box<dyn FnOnce() + Send>;
struct Hook {
    notification: Arc<Notification>,
    cloned: Mutex<Option<Callback>>,
    dropped: Mutex<Option<Callback>>,
    woken: Mutex<Option<Callback>>,
}
impl Hook {
    fn new(notification: &Arc<Notification>) -> Arc<Self> {
        Arc::new(Self {
            notification: Arc::clone(notification),
            cloned: Mutex::new(None),
            dropped: Mutex::new(None),
            woken: Mutex::new(None),
        })
    }
    fn invoke(&self, callback: &Mutex<Option<Callback>>) {
        assert!(
            self.notification.state.try_lock().is_ok(),
            "waker callback under notifier lock"
        );
        let callback = callback.lock().unwrap().take();
        if let Some(callback) = callback {
            callback();
        }
    }
    fn waker(self: &Arc<Self>) -> Waker {
        // SAFETY: All raw wakers own exactly one Arc reference; the vtable
        // clones, consumes, or temporarily borrows that reference as required.
        unsafe {
            Waker::from_raw(RawWaker::new(
                Arc::into_raw(Arc::clone(self)).cast(),
                &VTABLE,
            ))
        }
    }
}
unsafe fn clone_hook(data: *const ()) -> RawWaker {
    // SAFETY: The caller retains its Arc reference, which is only borrowed.
    let hook = ManuallyDrop::new(unsafe { Arc::<Hook>::from_raw(data.cast()) });
    hook.invoke(&hook.cloned);
    RawWaker::new(Arc::into_raw(Arc::clone(&hook)).cast(), &VTABLE)
}
unsafe fn wake_hook(data: *const ()) {
    // SAFETY: wake consumes this raw waker's owned Arc reference.
    let hook = unsafe { Arc::<Hook>::from_raw(data.cast()) };
    hook.invoke(&hook.woken);
}
unsafe fn wake_hook_by_ref(data: *const ()) {
    // SAFETY: wake_by_ref only borrows this raw waker's Arc reference.
    let hook = ManuallyDrop::new(unsafe { Arc::<Hook>::from_raw(data.cast()) });
    hook.invoke(&hook.woken);
}
unsafe fn drop_hook(data: *const ()) {
    // SAFETY: drop consumes this raw waker's owned Arc reference.
    let hook = unsafe { Arc::<Hook>::from_raw(data.cast()) };
    hook.invoke(&hook.dropped);
}
static VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_hook, wake_hook, wake_hook_by_ref, drop_hook);

#[test]
fn waker_clone_drop_and_wake_can_reenter_notification() {
    let notification = Arc::new(Notification::new());
    let hook = Hook::new(&notification);
    let waker = hook.waker();
    let mut listener = notification.listen();
    pending(&mut listener, &waker);
    // Clone can publish before the locked recheck on a later poll.
    let notify = Arc::clone(&notification);
    *hook.cloned.lock().unwrap() = Some(Box::new(move || notify.notify()));
    ready(&mut listener, &waker);
    drop(listener);

    // Replacing a stored waker drops the old waker after releasing the lock.
    let mut listener = notification.listen();
    pending(&mut listener, &waker);
    let notify = Arc::clone(&notification);
    *hook.dropped.lock().unwrap() = Some(Box::new(move || notify.notify()));
    pending(&mut listener, Waker::noop());
    ready(&mut listener, Waker::noop());
    drop(listener);

    // Cancellation drops its stored waker outside the lock too.
    let mut listener = notification.listen();
    pending(&mut listener, &waker);
    let notify = Arc::clone(&notification);
    *hook.dropped.lock().unwrap() = Some(Box::new(move || notify.notify()));
    drop(listener);

    let mut listener = notification.listen();
    pending(&mut listener, &waker);
    let notify = Arc::clone(&notification);
    *hook.woken.lock().unwrap() = Some(Box::new(move || notify.notify()));
    notification.notify();
    ready(&mut listener, &waker);
    waker.wake_by_ref();
}

#[test]
fn id_exhaustion_preserves_live_waiters_and_rejects_registration() {
    let notification = Notification::new();
    // The reachable boundary after issuing all but the final ID.
    notification.state.lock().unwrap().last_id = usize::MAX - 1;
    let (count, waker) = counter();
    let mut last = notification.listen();
    pending(&mut last, &waker);
    let mut exhausted = notification.listen();
    assert!(
        matches!(exhausted.poll(&mut Context::from_waker(&waker)), Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::Other)
    );
    notification.notify();
    ready(&mut last, &waker);
    assert_eq!(count.0.load(Ordering::Relaxed), 1);
    assert!(matches!(
        exhausted.poll(&mut Context::from_waker(&waker)),
        Poll::Ready(Err(_))
    ));
}

#[test]
fn lazy_registration_checks_readiness_and_closure_after_waker_clone() {
    use super::super::{Direction, create};
    use base_scheduler::Cancellation;
    use std::{future::Future, pin::pin};
    for close in [false, true] {
        let (group, _drivers) = create(1, Cancellation::new()).unwrap();
        let Some(group) = group else { return };
        let direction = Arc::new(Direction::new());
        let hook = Hook::new(&Arc::new(Notification::new()));
        let publish = Arc::clone(&direction);
        let owner = Arc::clone(&group);
        *hook.cloned.lock().unwrap() = Some(Box::new(move || {
            if close {
                owner.close();
            } else {
                publish.publish();
            }
        }));
        let waker = hook.waker();
        let mut waiting = pin!(direction.wait(&group));
        match waiting.as_mut().poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(Ok(())) => assert!(!close),
            Poll::Ready(Err(error)) => {
                assert!(close);
                assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
            }
            Poll::Pending => panic!("publication during registration must complete the wait"),
        }
    }
}

#[test]
fn a_notified_direction_wait_completes_even_after_another_reader_clears_readiness() {
    use super::super::{Direction, create};
    use base_scheduler::Cancellation;
    use std::{future::Future, pin::pin};
    let (group, _drivers) = create(1, Cancellation::new()).unwrap();
    let Some(group) = group else { return };
    let direction = Direction::new();
    let mut first = pin!(direction.wait(&group));
    let mut second = pin!(direction.wait(&group));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    direction.publish();
    assert!(first.as_mut().poll(&mut cx).is_ready());
    direction.clear(direction.snapshot());
    assert!(second.as_mut().poll(&mut cx).is_ready());
    let mut later = pin!(direction.wait(&group));
    assert!(later.as_mut().poll(&mut cx).is_pending());
    direction.publish();
    assert!(later.as_mut().poll(&mut cx).is_ready());
}

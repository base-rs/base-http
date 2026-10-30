// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Reuse storage for pinned operations, never their live state.
use crossbeam_utils::atomic::AtomicCell;
use futures_util::{Sink, Stream};
use pin_project_lite::pin_project;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll},
};

pub(crate) struct Cache<F> {
    idle: AtomicCell<Option<Pin<Box<Entry<F>>>>>,
}
impl<F> Cache<F> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            idle: AtomicCell::new(None),
        })
    }
    pub(crate) fn take(self: &Arc<Self>, make: impl FnOnce() -> F) -> Pin<Box<Entry<F>>> {
        let mut entry = self.idle.take().unwrap_or_else(|| {
            Box::pin(Entry {
                work: Slot::Empty,
                cache: Arc::downgrade(self),
            })
        });
        // Select storage before constructing the large operation state.
        // Generated replacement also lets Rust 1.87 initialize only live
        // captures, avoiding a copy of the future's inactive union storage.
        entry
            .as_mut()
            .project()
            .work
            .project_replace(Slot::Occupied { work: make() });
        entry
    }
}

pin_project! {
    pub(crate) struct Entry<F> {
        #[pin]
        work: Slot<F>,
        cache: Weak<Cache<F>>,
    }
}

// Generated pinned replacement drops the occupied field in place without
// moving it out. On Rust 1.87 this also avoids the large inactive-payload copy
// emitted when clearing a pinned Option with Pin::set(None).
pin_project! {
    #[project = SlotProjection]
    #[project_replace = SlotReplacement]
    #[repr(u8)]
    enum Slot<F> {
        Empty,
        Occupied { #[pin] work: F },
    }
}
impl<F> Entry<F> {
    fn work(self: Pin<&mut Self>) -> Pin<&mut F> {
        match self.project().work.project() {
            SlotProjection::Occupied { work } => work,
            SlotProjection::Empty => unreachable!("idle operation polled"),
        }
    }
}

pub(crate) trait Recycle {
    fn recycle(self: Pin<Box<Self>>);
}
impl<F> Recycle for Entry<F> {
    fn recycle(mut self: Pin<Box<Self>>) {
        // Drop captures (including connection owners) in place before storing
        // the allocation. An idle entry must never retain an exchange or form
        // a connection -> cache -> operation -> connection ownership cycle.
        self.as_mut().project().work.project_replace(Slot::Empty);
        if let Some(cache) = self.cache.upgrade() {
            // Keep at most one empty allocation; overlapping returns may
            // replace an earlier one. Drop displaced storage outside the cell.
            drop(cache.idle.swap(Some(self)));
        }
    }
}
impl<F: Future> Future for Entry<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.work().poll(cx)
    }
}
impl<F: Stream> Stream for Entry<F> {
    type Item = F::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.work().poll_next(cx)
    }
}
impl<F: Sink<T>, T> Sink<T> for Entry<F> {
    type Error = F::Error;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.work().poll_ready(cx)
    }
    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.work().start_send(item)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.work().poll_flush(cx)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.work().poll_close(cx)
    }
}

/// Owning handle whose drop clears the operation and returns its allocation.
/// The operation itself remains pinned until it has been dropped in place.
pub(crate) struct Reused<T: Recycle + ?Sized>(Option<Pin<Box<T>>>);
impl<T: Recycle + ?Sized> Reused<T> {
    pub(crate) fn new(work: Pin<Box<T>>) -> Self {
        Self(Some(work))
    }
}
impl<T: Recycle + ?Sized> Drop for Reused<T> {
    fn drop(&mut self) {
        self.0.take().unwrap().recycle();
    }
}
impl<T: Future + Recycle + ?Sized> Future for Reused<T> {
    type Output = T::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().unwrap().as_mut().poll(cx)
    }
}
impl<T: Stream + Recycle + ?Sized> Stream for Reused<T> {
    type Item = T::Item;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.as_mut().unwrap().as_mut().poll_next(cx)
    }
}
impl<T: Sink<I> + Recycle + ?Sized, I> Sink<I> for Reused<T> {
    type Error = T::Error;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.as_mut().unwrap().as_mut().poll_ready(cx)
    }
    fn start_send(mut self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        self.0.as_mut().unwrap().as_mut().start_send(item)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.as_mut().unwrap().as_mut().poll_flush(cx)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.as_mut().unwrap().as_mut().poll_close(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::future::block_on;
    use futures_util::{SinkExt, task::noop_waker};
    use std::{
        marker::PhantomPinned,
        sync::atomic::{AtomicUsize, Ordering},
    };

    struct PinnedOperation {
        address: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
        _capture: Arc<()>,
        _pinned: PhantomPinned,
    }
    impl Future for PinnedOperation {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            let this = self.as_ref().get_ref();
            let address = this as *const Self as usize;
            let previous = this.address.swap(address, Ordering::SeqCst);
            assert!([0, address].contains(&previous));
            Poll::Ready(())
        }
    }
    impl Drop for PinnedOperation {
        fn drop(&mut self) {
            let address = self.address.load(Ordering::SeqCst);
            if address != 0 {
                assert_eq!(address, self as *const Self as usize);
            }
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn pinned_state_is_dropped_in_place_before_storage_is_reused() {
        let cache = Cache::new();
        let capture = Arc::new(());
        let drops = Arc::new(AtomicUsize::new(0));
        let mut previous = 0;
        for iteration in 0..3 {
            let address = Arc::new(AtomicUsize::new(0));
            let mut operation = Reused::new(cache.take(|| PinnedOperation {
                address: address.clone(),
                drops: drops.clone(),
                _capture: capture.clone(),
                _pinned: PhantomPinned,
            }));
            assert!(
                Pin::new(&mut operation)
                    .poll(&mut Context::from_waker(&noop_waker()))
                    .is_ready()
            );
            let current = address.load(Ordering::SeqCst);
            if previous != 0 {
                assert_eq!(current, previous);
            }
            previous = current;
            drop(operation);
            assert_eq!(drops.load(Ordering::SeqCst), iteration + 1);
            assert_eq!(
                Arc::strong_count(&capture),
                1,
                "idle storage retains no captures"
            );
        }
        let weak = Arc::downgrade(&cache);
        drop(cache);
        assert!(
            weak.upgrade().is_none(),
            "idle entries do not keep their cache alive"
        );
    }

    #[test]
    fn retained_operations_never_share_storage_and_surplus_is_discarded() {
        let cache = Cache::new();
        let mut first = Reused::new(cache.take(|| std::future::ready(())));
        assert!(
            Pin::new(&mut first)
                .poll(&mut Context::from_waker(&noop_waker()))
                .is_ready()
        );
        let first_address = first.0.as_ref().unwrap().as_ref().get_ref() as *const _;
        let second = Reused::new(cache.take(|| std::future::ready(())));
        let second_address = second.0.as_ref().unwrap().as_ref().get_ref() as *const _;
        assert_ne!(first_address, second_address);
        drop(second);
        drop(first);
        let third = Reused::new(cache.take(|| std::future::ready(())));
        assert_eq!(
            first_address,
            third.0.as_ref().unwrap().as_ref().get_ref() as *const _
        );
        // An outstanding entry can also outlive the connection/cache.
        let weak = Arc::downgrade(&cache);
        drop(cache);
        assert!(weak.upgrade().is_none());
        drop(third);
    }

    #[test]
    fn dropping_unpolled_operation_clears_its_owners() {
        let cache = Cache::new();
        let capture = Arc::new(());
        let drops = Arc::new(AtomicUsize::new(0));
        let operation = Reused::new(cache.take(|| PinnedOperation {
            address: Arc::new(AtomicUsize::new(0)),
            drops: drops.clone(),
            _capture: capture.clone(),
            _pinned: PhantomPinned,
        }));
        drop(operation);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(Arc::strong_count(&capture), 1);
    }

    #[test]
    fn storage_can_be_taken_and_returned_from_different_threads() {
        let cache = Cache::new();
        let capture = Arc::new(());
        let drops = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let cache = &cache;
                let capture = &capture;
                let drops = &drops;
                scope.spawn(move || {
                    for _ in 0..100 {
                        let mut operation = Reused::new(cache.take(|| PinnedOperation {
                            address: Arc::new(AtomicUsize::new(0)),
                            drops: drops.clone(),
                            _capture: capture.clone(),
                            _pinned: PhantomPinned,
                        }));
                        assert!(
                            Pin::new(&mut operation)
                                .poll(&mut Context::from_waker(&noop_waker()))
                                .is_ready()
                        );
                        std::thread::yield_now();
                    }
                });
            }
        });
        assert_eq!(drops.load(Ordering::SeqCst), 400);
        assert_eq!(Arc::strong_count(&capture), 1);
    }

    // A sink recording what it received and whether it was closed.
    #[derive(Default)]
    struct Recorder {
        sent: Vec<u8>,
        closed: bool,
    }
    impl Sink<u8> for Recorder {
        type Error = ();
        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Ready(Ok(()))
        }
        fn start_send(self: Pin<&mut Self>, item: u8) -> Result<(), ()> {
            self.get_mut().sent.push(item);
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            self.get_mut().closed = true;
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn reused_sinks_forward_sends_and_close() {
        let cache = Cache::new();
        let mut sink = Reused::new(cache.take(Recorder::default));
        block_on(async {
            sink.send(1).await.unwrap();
            sink.close().await.unwrap();
        });
        let recorder = sink.0.as_mut().unwrap().as_mut().work();
        assert_eq!(recorder.sent, [1]);
        assert!(recorder.closed);
    }
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Sequential writes with an owned codec, preserving pinned in-flight state.
//!
//! Equivalent to sink::unfold at this boundary, with pinned replacement on
//! every transition. Pin::set emitted copies of inactive future storage on
//! Rust 1.87, including when a completed write restored its small codec owner.
use futures_util::Sink;
use pin_project_lite::pin_project;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pin_project! {
    #[project = StateProjection]
    #[project_replace = StateReplacement]
    enum State<T, F> {
        Idle { owner: T },
        Writing { #[pin] future: F },
        Empty,
    }
}

pin_project! {
    pub(crate) struct OwnedSink<T, M, F> {
        make: M,
        #[pin]
        state: State<T, F>,
    }
}

pub(crate) fn owned_sink<T, M, F, I, E>(owner: T, make: M) -> OwnedSink<T, M, F>
where
    M: FnMut(T, I) -> F,
    F: Future<Output = Result<T, E>>,
{
    OwnedSink {
        make,
        state: State::Idle { owner },
    }
}

impl<T, M, F, I, E> Sink<I> for OwnedSink<T, M, F>
where
    M: FnMut(T, I) -> F,
    F: Future<Output = Result<T, E>>,
{
    type Error = E;

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), E> {
        let mut this = self.project();
        let StateReplacement::Idle { owner } = this.state.as_mut().project_replace(State::Empty)
        else {
            panic!("write started before previous write completed");
        };
        let future = (this.make)(owner, item);
        this.state.project_replace(State::Writing { future });
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        let mut state = self.project().state;
        let StateProjection::Writing { future } = state.as_mut().project() else {
            return Poll::Ready(Ok(()));
        };
        match future.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => Poll::Ready(match result {
                Ok(owner) => {
                    state.project_replace(State::Idle { owner });
                    Ok(())
                }
                Err(error) => {
                    state.project_replace(State::Empty);
                    Err(error)
                }
            }),
        }
    }

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        self.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        self.poll_flush(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::future::block_on;
    use futures_util::{SinkExt, task::noop_waker};
    use std::{
        cell::{Cell, RefCell},
        marker::PhantomPinned,
        pin::pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    struct Owner(Arc<AtomicUsize>);
    impl Drop for Owner {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Write {
        owner: RefCell<Option<Owner>>,
        address: Cell<usize>,
        pending: Cell<bool>,
        fail: bool,
        drops: Arc<AtomicUsize>,
        _pinned: PhantomPinned,
    }
    impl Write {
        fn new(owner: Owner, fail: bool, drops: Arc<AtomicUsize>) -> Self {
            Self {
                owner: RefCell::new(Some(owner)),
                address: Cell::new(0),
                pending: Cell::new(true),
                fail,
                drops,
                _pinned: PhantomPinned,
            }
        }
    }
    impl Future for Write {
        type Output = Result<Owner, ()>;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.as_ref().get_ref();
            let address = this as *const Self as usize;
            let previous = this.address.replace(address);
            assert!(previous == 0 || previous == address);
            if this.pending.replace(false) {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else if this.fail {
                Poll::Ready(Err(()))
            } else {
                Poll::Ready(Ok(this.owner.borrow_mut().take().unwrap()))
            }
        }
    }
    impl Drop for Write {
        fn drop(&mut self) {
            let address = self.address.get();
            assert!(address == 0 || address == self as *const Self as usize);
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn successive_writes_keep_the_owner_and_drop_pinned_futures_in_place() {
        let owners = Arc::new(AtomicUsize::new(0));
        let writes = Arc::new(AtomicUsize::new(0));
        {
            let sink = owned_sink(Owner(owners.clone()), |owner, ()| {
                Write::new(owner, false, writes.clone())
            });
            let mut sink = pin!(sink);
            block_on(async {
                for _ in 0..3 {
                    sink.send(()).await.unwrap();
                }
                sink.close().await.unwrap();
            });
            assert_eq!(owners.load(Ordering::SeqCst), 0);
            assert_eq!(writes.load(Ordering::SeqCst), 3);
        }
        assert_eq!(owners.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn error_and_pending_drop_release_each_owner_once() {
        for finish in [false, true] {
            let owners = Arc::new(AtomicUsize::new(0));
            let writes = Arc::new(AtomicUsize::new(0));
            {
                let sink = owned_sink(Owner(owners.clone()), |owner, ()| {
                    Write::new(owner, true, writes.clone())
                });
                let mut sink = pin!(sink);
                let waker = noop_waker();
                let mut cx = Context::from_waker(&waker);
                assert!(sink.as_mut().poll_ready(&mut cx).is_ready());
                sink.as_mut().start_send(()).unwrap();
                assert!(sink.as_mut().poll_flush(&mut cx).is_pending());
                if finish {
                    assert_eq!(sink.as_mut().poll_flush(&mut cx), Poll::Ready(Err(())));
                    assert_eq!(writes.load(Ordering::SeqCst), 1);
                    assert_eq!(owners.load(Ordering::SeqCst), 1);
                }
            }
            assert_eq!(writes.load(Ordering::SeqCst), 1);
            assert_eq!(owners.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    #[should_panic(expected = "write started before previous write completed")]
    fn overlapping_writes_are_rejected() {
        let drops = Arc::new(AtomicUsize::new(0));
        let sink = owned_sink(Owner(drops.clone()), |owner, ()| {
            Write::new(owner, false, drops.clone())
        });
        let mut sink = pin!(sink);
        sink.as_mut().start_send(()).unwrap();
        let _ = sink.as_mut().start_send(());
    }
}

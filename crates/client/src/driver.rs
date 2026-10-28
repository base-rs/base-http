// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    Connection, Endpoint, Error, Options, ResponseBody, connection,
    outgoing::{Observer, OutgoingBody},
};
use base_http::{Request, Response, body::ReadableBody};
use base_pool::{Acquisition, MultiplexBuildPermit, MultiplexPool, MultiplexUse};
use base_scheduler::{
    Cancellation, SocketClose, SocketConnect, SocketOpen, SocketStreamReading, SocketStreamWriting,
};
use event_listener::Event;
use futures_channel::oneshot;
use futures_util::stream::{FuturesUnordered, StreamExt};
use pin_project_lite::pin_project;
use std::{
    collections::VecDeque,
    future::{Future, poll_fn},
    pin::{Pin, pin},
    sync::{Arc, Mutex, MutexGuard, PoisonError, atomic::AtomicUsize},
    task::Poll,
};

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
pub(crate) type Work<'scope, T> = Pin<Box<dyn Future<Output = T> + Send + 'scope>>;
type Factory<'scope> = Box<
    dyn Fn(
            MultiplexBuildPermit<Connection<'scope>>,
            Cancellation,
        ) -> Work<'scope, Result<(), Error>>
        + Send
        + Sync
        + 'scope,
>;
pub(crate) type Reply = oneshot::Sender<Result<Response<ResponseBody>, Error>>;

pub(crate) struct Operation<'scope> {
    pub request: Request<OutgoingBody<'scope>>,
    pub interim: Box<dyn Observer + 'scope>,
    pub reply: Option<Reply>,
    pub cancellation: Cancellation,
    pub lifetime: Cancellation,
    pub _pending: Option<Pending<'scope>>,
}
pub(crate) struct Assigned<'scope> {
    pub operation: Operation<'scope>,
    pub active: MultiplexUse<Connection<'scope>>,
}
struct State<'scope> {
    requests: VecDeque<Operation<'scope>>,
    work: VecDeque<Work<'scope, ()>>,
    pending: usize,
    closed: bool,
    abandoned: bool,
}
pub(crate) struct Queue<'scope> {
    state: Mutex<State<'scope>>,
    pub changed: Event,
    pub handles: AtomicUsize,
    limit: usize,
}
impl<'scope> Queue<'scope> {
    pub fn push(&self, operation: Operation<'scope>) -> Result<(), Error> {
        let mut state = lock(&self.state);
        if state.closed {
            return Err(Error::Closed);
        }
        state.requests.push_back(operation);
        drop(state);
        self.changed.notify(usize::MAX);
        Ok(())
    }
    pub(crate) fn push_work(&self, work: Work<'scope, ()>) -> Result<(), Error> {
        let mut state = lock(&self.state);
        if state.abandoned {
            drop(state);
            drop(work);
            return Err(Error::Closed);
        }
        state.work.push_back(work);
        drop(state);
        self.changed.notify(usize::MAX);
        Ok(())
    }
    pub fn begin_close(&self) {
        lock(&self.state).closed = true;
        self.changed.notify(usize::MAX);
    }
    fn abandon(&self) {
        let discarded = {
            let mut state = lock(&self.state);
            state.closed = true;
            state.abandoned = true;
            (
                std::mem::take(&mut state.requests),
                std::mem::take(&mut state.work),
            )
        };
        self.changed.notify(usize::MAX);
        drop(discarded);
    }
}
pub(crate) struct Pending<'scope>(Arc<Queue<'scope>>);
impl<'scope> Pending<'scope> {
    pub fn new(queue: Arc<Queue<'scope>>) -> Result<Self, Error> {
        {
            let mut state = lock(&queue.state);
            if state.closed {
                return Err(Error::Closed);
            }
            if state.pending >= queue.limit {
                return Err(Error::Busy);
            }
            state.pending += 1;
        }
        Ok(Self(queue))
    }
}
impl Drop for Pending<'_> {
    fn drop(&mut self) {
        let drained = {
            let mut state = lock(&self.0.state);
            state.pending -= 1;
            state.closed && state.pending == 0
        };
        // Completion only changes the supervisor's work when a closing client
        // has drained. Submission and deferred cleanup notify independently.
        if drained {
            self.0.changed.notify(usize::MAX);
        }
    }
}

pub(crate) struct Context<'scope> {
    pool: MultiplexPool<Connection<'scope>>,
    factory: Factory<'scope>,
    pub(crate) cancellation: Cancellation,
}
impl<'scope> Context<'scope> {
    pub(crate) async fn acquire(
        &self,
        cancellation: &Cancellation,
    ) -> Result<MultiplexUse<Connection<'scope>>, Error> {
        loop {
            match self.pool.acquire_or_reserve(cancellation).await? {
                Acquisition::Existing(reservation) => match reservation.activate() {
                    Ok(active) => return Ok(active),
                    Err(base_pool::Error::AdmissionChanged) => continue,
                    Err(error) => return Err(error.into()),
                },
                Acquisition::Build(build) => (self.factory)(build, cancellation.clone()).await?,
            }
        }
    }
    async fn cleanup(&self) -> Result<(), Error> {
        let mut failure = None;
        while let Some(retired) = self.pool.next_retirement(&Cancellation::never()).await? {
            retired.stop();
            if let Err(error) = retired.wait_closed().await {
                failure.get_or_insert(error);
            }
            retired.complete();
        }
        failure.map_or(Ok(()), Err)
    }
}

/// Explicitly supervised HTTP progress. Poll it concurrently with client calls
/// and body consumers. Cancellation forces cleanup; await it to join all lane
/// retirements and socket closes. Established plaintext direct requests run in
/// their calling tasks. Direct request/response handles must be polled or dropped
/// during cancellation so their codecs can return. The current scoped
/// implementation polls all supervised connection work inside this one future;
/// it does not distribute connections across task workers. Drop provides no
/// asynchronous join.
pub struct Driver<'scope> {
    work: Work<'scope, Result<(), Error>>,
    cancellation: Cancellation,
    abandon: Box<dyn Fn() + Send + Sync + 'scope>,
}
impl Driver<'_> {
    pub async fn run(mut self, cancellation: &Cancellation) -> Result<(), Error> {
        let mut cancelled = pin!(cancellation.cancelled());
        poll_fn(|cx| {
            if !self.cancellation.is_cancelled() && cancelled.as_mut().poll(cx).is_ready() {
                self.cancellation.cancel();
            }
            self.work.as_mut().poll(cx)
        })
        .await
    }
}
impl Drop for Driver<'_> {
    fn drop(&mut self) {
        self.cancellation.cancel();
        (self.abandon)();
    }
}

pub(crate) struct ClientParts<'scope> {
    pub queue: Arc<Queue<'scope>>,
    pub pool: MultiplexPool<Connection<'scope>>,
    pub driver: Driver<'scope>,
    pub context: Arc<Context<'scope>>,
}

pub(crate) fn make<'scope, S>(
    scheduler: &'scope S,
    endpoint: Endpoint,
    options: Options,
) -> Result<ClientParts<'scope>, Error>
where
    S: SocketOpen
        + SocketConnect
        + SocketClose
        + SocketStreamReading
        + SocketStreamWriting
        + 'scope,
    S::Socket: 'static,
{
    let pool = MultiplexPool::new(options.pool.clone())?;
    let queue = Arc::new(Queue {
        state: Mutex::new(State {
            requests: VecDeque::new(),
            work: VecDeque::new(),
            pending: 0,
            closed: false,
            abandoned: false,
        }),
        changed: Event::new(),
        handles: AtomicUsize::new(1),
        limit: options.max_requests,
    });
    let cancellation = Cancellation::new();
    let weak_queue = Arc::downgrade(&queue);
    let connection_lifetime = cancellation.clone();
    let factory = Box::new(move |build, cancelled: Cancellation| {
        let options = options.clone();
        let endpoint = endpoint.clone();
        let lifetime = connection_lifetime.clone();
        let queue = weak_queue.upgrade();
        Box::pin(async move {
            let queue = queue.ok_or(Error::Closed)?;
            let (ready, established) = oneshot::channel();
            queue.push_work(Box::pin(connection::run(
                scheduler,
                endpoint,
                options,
                build,
                cancelled,
                lifetime,
                ready,
                Arc::downgrade(&queue),
            )))?;
            established.await.map_err(|_| Error::Closed)?
        }) as Work<'scope, _>
    });
    let context = Arc::new(Context {
        pool: pool.clone(),
        factory,
        cancellation: cancellation.clone(),
    });
    let abandoned = queue.clone();
    let abandoned_pool = pool.clone();
    let driver = Driver {
        work: Box::pin(run(queue.clone(), context.clone())),
        cancellation,
        abandon: Box::new(move || {
            abandoned.abandon();
            abandoned_pool.begin_close();
        }),
    };
    Ok(ClientParts {
        queue,
        pool,
        driver,
        context,
    })
}

async fn assign<'scope>(mut operation: Operation<'scope>, context: Arc<Context<'scope>>) {
    let result = context.acquire(&operation.cancellation).await;
    match result {
        Ok(active) => {
            let connection = active.clone_handle();
            connection.submit(Assigned { operation, active });
        }
        Err(error) => {
            operation.request.body.close(None);
            if let Some(reply) = operation.reply.take() {
                let _ = reply.send(Err(error));
            }
        }
    }
}

async fn run<'scope>(
    queue: Arc<Queue<'scope>>,
    context: Arc<Context<'scope>>,
) -> Result<(), Error> {
    let mut jobs: FuturesUnordered<Work<'scope, ()>> = FuturesUnordered::new();
    let cleanup_context = context.clone();
    let mut cleanup = pin!(cleanup_context.cleanup());
    let mut cancellation = pin!(context.cancellation.cancelled());
    let mut stopping = false;
    let mut cleaned = None;
    loop {
        event_listener::listener!(queue.changed => listener);
        let (requests, work) = {
            let mut state = lock(&queue.state);
            (
                std::mem::take(&mut state.requests),
                std::mem::take(&mut state.work),
            )
        };
        for operation in requests {
            let context = context.clone();
            jobs.push(Box::pin(async move {
                let local = operation.cancellation.clone();
                let lifetime = operation.lifetime.clone();
                let driver = context.cancellation.clone();
                linked(assign(operation, context), &local, &lifetime, &driver).await;
            }));
        }
        jobs.extend(work);
        let close = {
            let state = lock(&queue.state);
            state.closed && state.pending == 0
        };
        if stopping || close {
            context.pool.begin_close();
        }
        let mut listener = pin!(listener);
        let finished = poll_fn(|cx| {
            if !stopping && cancellation.as_mut().poll(cx).is_ready() {
                stopping = true;
                queue.begin_close();
                context.pool.begin_close();
            }
            if cleaned.is_none() {
                if let Poll::Ready(result) = cleanup.as_mut().poll(cx) {
                    cleaned = Some(result);
                }
            }
            if cleaned.is_some() && jobs.is_empty() && lock(&queue.state).work.is_empty() {
                return Poll::Ready(cleaned.take());
            }
            if let Poll::Ready(Some(())) = jobs.poll_next_unpin(cx) {
                return Poll::Ready(None);
            }
            if listener.as_mut().poll(cx).is_ready() {
                return Poll::Ready(None);
            }
            Poll::Pending
        })
        .await;
        if let Some(result) = finished {
            return result;
        }
    }
}

pin_project! {
    struct Checked<'a, F, C> {
        #[pin]
        work: F,
        #[pin]
        cancelled: C,
        cancellation: &'a Cancellation,
    }
}
impl<T, F, C> Future for Checked<'_, F, C>
where
    F: Future<Output = Result<T, Error>>,
    C: Future<Output = ()>,
{
    type Output = Result<T, Error>;
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if this.cancellation.is_cancelled() {
            return Poll::Ready(Err(Error::Cancelled));
        }
        match this.work.poll(cx) {
            ready @ Poll::Ready(_) => ready,
            Poll::Pending => {
                // Subscribe only when work actually waits. Polling cancelled
                // after Pending also closes the race with the initial check.
                if this.cancelled.poll(cx).is_ready() {
                    Poll::Ready(Err(Error::Cancelled))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}
pub(crate) fn checked<T>(
    work: impl Future<Output = Result<T, Error>>,
    cancellation: &Cancellation,
) -> impl Future<Output = Result<T, Error>> {
    // Store the nested future where it will be polled. An async wrapper with
    // pin!(work) needs another large state slot and moves work on first poll.
    Checked {
        work,
        cancelled: cancellation.cancelled(),
        cancellation,
    }
}
// Propagate lifetime cancellation without dropping owned construction/cleanup.
pub(crate) async fn linked<T>(
    work: impl Future<Output = T>,
    cancellation: &Cancellation,
    lifetime: &Cancellation,
    driver: &Cancellation,
) -> T {
    let mut work = pin!(work);
    let mut a = pin!(lifetime.cancelled());
    let mut b = pin!(driver.cancelled());
    poll_fn(|cx| {
        if !cancellation.is_cancelled()
            && (a.as_mut().poll(cx).is_ready() || b.as_mut().poll(cx).is_ready())
        {
            cancellation.cancel();
        }
        work.as_mut().poll(cx)
    })
    .await
}
pub(crate) struct CancelOnDrop(pub Cancellation, pub bool);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.1 {
            self.0.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::Ordering,
        task::{Context, Wake, Waker},
    };

    #[derive(Default)]
    struct Wakes(AtomicUsize);
    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn checked_prioritizes_existing_cancellation_and_wakes_blocked_work() {
        use futures_lite::future::{block_on, poll_once};
        use std::cell::Cell;
        let cancel = Cancellation::new();
        cancel.cancel();
        let polled = Cell::new(false);
        let work = std::future::poll_fn(|_| {
            polled.set(true);
            Poll::Ready(Ok::<_, Error>(()))
        });
        assert!(matches!(
            block_on(checked(work, &cancel)),
            Err(Error::Cancelled)
        ));
        assert!(!polled.get());

        let cancel = Cancellation::new();
        let wakes = Arc::new(Wakes::default());
        let waker = Waker::from(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        let mut work = pin!(checked(
            std::future::pending::<Result<(), Error>>(),
            &cancel
        ));
        assert!(work.as_mut().poll(&mut cx).is_pending());
        cancel.cancel();
        assert!(wakes.0.load(Ordering::Relaxed) > 0);
        assert!(matches!(
            work.as_mut().poll(&mut cx),
            Poll::Ready(Err(Error::Cancelled))
        ));

        // Cancellation occurring during a Pending poll must be noticed in
        // that poll, even before a listener has ever been registered.
        let cancel = Cancellation::new();
        let pending = std::future::poll_fn(|_| {
            cancel.cancel();
            Poll::Pending::<Result<(), Error>>
        });
        let mut work = pin!(checked(pending, &cancel));
        assert!(matches!(
            block_on(poll_once(work.as_mut())),
            Some(Err(Error::Cancelled))
        ));
    }

    #[test]
    fn pending_completion_wakes_supervisor_only_when_closed_and_drained() {
        let queue = Arc::new(Queue {
            state: Mutex::new(State {
                requests: VecDeque::new(),
                work: VecDeque::new(),
                pending: 0,
                closed: false,
                abandoned: false,
            }),
            changed: Event::new(),
            handles: AtomicUsize::new(1),
            limit: 2,
        });
        let wakes = Arc::new(Wakes::default());
        let waker = Waker::from(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        let mut changed = pin!(queue.changed.listen());
        assert!(changed.as_mut().poll(&mut cx).is_pending());

        let first = Pending::new(queue.clone()).unwrap();
        let second = Pending::new(queue.clone()).unwrap();
        assert!(matches!(Pending::new(queue.clone()), Err(Error::Busy)));
        drop(first);
        assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
        // Admission is a synchronous check; completion restores it without
        // requiring any supervisor work.
        let third = Pending::new(queue.clone()).unwrap();
        queue.begin_close();
        assert!(changed.as_mut().poll(&mut cx).is_ready());
        let mut changed = pin!(queue.changed.listen());
        assert!(changed.as_mut().poll(&mut cx).is_pending());
        wakes.0.store(0, Ordering::Relaxed);
        assert!(matches!(Pending::new(queue.clone()), Err(Error::Closed)));
        drop(second);
        assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
        assert!(changed.as_mut().poll(&mut cx).is_pending());
        drop(third);
        assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
        assert!(changed.as_mut().poll(&mut cx).is_ready());
        assert_eq!(lock(&queue.state).pending, 0);
    }
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Connection, Endpoint, Error, Options, connection};
use base_pool::{Acquisition, MultiplexBuildPermit, MultiplexPool, MultiplexUse};
use base_scheduler::{
    Cancellation, Parent, SocketClose, SocketConnect, SocketOpen, SocketSetOption,
    SocketStreamReading, SocketStreamWriting, TcpNoDelay,
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
struct State<'scope> {
    work: VecDeque<Work<'scope, ()>>,
    /// Connection drivers, which the driver places when it collects them.
    connections: VecDeque<Work<'scope, ()>>,
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
    pub(crate) fn push_work(&self, work: Work<'scope, ()>) -> Result<(), Error> {
        self.push(work, |state| &mut state.work)
    }
    /// Queue a connection's driver, which runs where the driver's parent decides.
    fn push_connection(&self, work: Work<'scope, ()>) -> Result<(), Error> {
        self.push(work, |state| &mut state.connections)
    }
    fn push(
        &self,
        work: Work<'scope, ()>,
        list: for<'s> fn(&'s mut State<'scope>) -> &'s mut VecDeque<Work<'scope, ()>>,
    ) -> Result<(), Error> {
        let mut state = lock(&self.state);
        if state.abandoned {
            drop(state);
            drop(work);
            return Err(Error::Closed);
        }
        list(&mut state).push_back(work);
        drop(state);
        self.changed.notify(usize::MAX);
        Ok(())
    }
    /// Whether the driver was dropped, abandoning its work.
    pub(crate) fn is_abandoned(&self) -> bool {
        lock(&self.state).abandoned
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
                std::mem::take(&mut state.work),
                std::mem::take(&mut state.connections),
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
            // Admission can change between reservation and activation. Either outcome without a use acquires again, which reports a closed pool. A connection the peer closed while idle is retired before any request bytes are sent, so acquiring again is always safe.
            let active = match self.pool.acquire_or_reserve(cancellation).await? {
                Acquisition::Existing(reservation) => {
                    reservation.activate().ok().and_then(|active| {
                        if active.probe() {
                            Some(active)
                        } else {
                            active.complete();
                            None
                        }
                    })
                }
                Acquisition::Build(build) => {
                    (self.factory)(build, cancellation.clone()).await?;
                    None
                }
            };
            if let Some(active) = active {
                return Ok(active);
            }
        }
    }
    async fn cleanup(&self) -> Result<(), Error> {
        let mut failure = None;
        while let Some(retired) = self
            .pool
            .next_retirement(&Cancellation::never())
            .await
            .expect("an uncancellable retirement wait cannot fail")
        {
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
/// during cancellation so their codecs can return. Drop provides no
/// asynchronous join.
pub struct Driver<'scope> {
    queue: Arc<Queue<'scope>>,
    context: Arc<Context<'scope>>,
    cancellation: Cancellation,
    abandon: Box<dyn Fn() + Send + Sync + 'scope>,
}
impl<'scope> Driver<'scope> {
    /// Supervise the client until it is closed and its connections have
    /// retired. `parent` decides where each connection's driver runs: the
    /// scheduler, `&scheduler`, polls them within this future, interleaving on
    /// whichever worker polls it; a scope of the scheduler's scoped spawn runs
    /// each as its own task, so connections progress on more than one worker.
    /// Calls on established connections run in their calling tasks either way,
    /// and this future still owns admission, cancellation and the joined
    /// retirement of every connection; queued calls and deferred closing stay
    /// within it. A connection that cannot be spawned, as when the scheduler is
    /// closed, is reported closed to the call that opened it.
    pub fn run<P: Parent<'scope>>(
        self,
        parent: P,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        // Stating `Send` on the returned type proves it here, for every parent
        // lifetime, rather than in each caller's future.
        drive(self, parent, cancellation)
    }
}

async fn drive<'scope, P: Parent<'scope>>(
    driver: Driver<'scope>,
    parent: P,
    cancellation: &Cancellation,
) -> Result<(), Error> {
    let mut work = pin!(run(driver.queue.clone(), driver.context.clone(), parent));
    let mut cancelled = pin!(cancellation.cancelled());
    poll_fn(|cx| {
        if !driver.cancellation.is_cancelled() && cancelled.as_mut().poll(cx).is_ready() {
            driver.cancellation.cancel();
        }
        work.as_mut().poll(cx)
    })
    .await
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
) -> ClientParts<'scope>
where
    S: SocketOpen
        + SocketConnect
        + SocketClose
        + SocketSetOption<TcpNoDelay>
        + SocketStreamReading
        + SocketStreamWriting
        + 'scope,
    S::Socket: 'static,
{
    let pool =
        MultiplexPool::new(options.pool.clone()).expect("Client::new validated the pool options");
    let queue = Arc::new(Queue {
        state: Mutex::new(State {
            work: VecDeque::new(),
            connections: VecDeque::new(),
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
        // Acquirers hold the queue: every request retains it through its client handle.
        let queue = weak_queue.upgrade().expect("an acquirer retains the queue");
        Box::pin(async move {
            let (ready, established) = oneshot::channel();
            // The driver places the connection, in place or as a scope task. One destroyed,
            // panicking or never spawned before it reports readiness drops `ready`, which
            // reports the connection closed.
            queue.push_connection(Box::pin(connection::run(
                scheduler,
                endpoint,
                options,
                build,
                cancelled,
                lifetime,
                ready,
                Arc::downgrade(&queue),
            )))?;
            // Only an abandoned driver drops construction before it reports readiness.
            established.await.unwrap_or(Err(Error::Closed))
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
        queue: queue.clone(),
        context: context.clone(),
        cancellation,
        abandon: Box::new(move || {
            abandoned.abandon();
            abandoned_pool.begin_close();
        }),
    };
    ClientParts {
        queue,
        pool,
        driver,
        context,
    }
}

async fn run<'scope, P: Parent<'scope>>(
    queue: Arc<Queue<'scope>>,
    context: Arc<Context<'scope>>,
    parent: P,
) -> Result<(), Error> {
    let mut jobs: FuturesUnordered<Work<'scope, ()>> = FuturesUnordered::new();
    // Placed connection drivers, polled here in place or joined as scope tasks.
    let mut connections = FuturesUnordered::new();
    let cleanup_context = context.clone();
    let mut cleanup = pin!(cleanup_context.cleanup());
    let mut cancellation = pin!(context.cancellation.cancelled());
    let mut stopping = false;
    let mut cleaned = None;
    loop {
        event_listener::listener!(queue.changed => listener);
        let (work, placing) = {
            let mut state = lock(&queue.state);
            (
                std::mem::take(&mut state.work),
                std::mem::take(&mut state.connections),
            )
        };
        jobs.extend(work);
        // A connection that cannot be spawned is dropped, reporting it closed.
        connections.extend(
            placing
                .into_iter()
                .filter_map(|work| parent.place(work).ok()),
        );
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
            if cleaned.is_none()
                && let Poll::Ready(result) = cleanup.as_mut().poll(cx)
            {
                cleaned = Some(result);
            }
            if cleaned.is_some() && jobs.is_empty() && connections.is_empty() && {
                let state = lock(&queue.state);
                state.work.is_empty() && state.connections.is_empty()
            } {
                return Poll::Ready(cleaned.take());
            }
            if let Poll::Ready(Some(())) = jobs.poll_next_unpin(cx) {
                return Poll::Ready(None);
            }
            // A connection's driver reports its own outcome; its task's panic or destruction drops its readiness.
            if let Poll::Ready(Some(_)) = connections.poll_next_unpin(cx) {
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
    use crate::{Endpoint, Options};

    use base_scheduler_native::NativeScheduler;
    use std::{
        sync::atomic::Ordering,
        task::{Context, Wake, Waker},
    };

    #[derive(Default)]
    struct Wakes(AtomicUsize);

    // Work which records whether it was polled.
    struct Probe<'a>(&'a std::cell::Cell<bool>);
    impl Future for Probe<'_> {
        type Output = Result<(), Error>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            self.0.set(true);
            Poll::Ready(Ok(()))
        }
    }
    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn checked_prioritizes_existing_cancellation_and_wakes_blocked_work() {
        use futures_lite::future::{block_on, poll_once};
        use std::cell::Cell;
        let polled = Cell::new(false);
        block_on(checked(Probe(&polled), &Cancellation::new())).unwrap();
        assert!(polled.get());

        let cancel = Cancellation::new();
        cancel.cancel();
        let polled = Cell::new(false);
        let error = block_on(checked(Probe(&polled), &cancel)).unwrap_err();
        assert_eq!(error.to_string(), Error::Cancelled.to_string());
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
        assert_eq!(
            format!("{:?}", work.as_mut().poll(&mut cx)),
            "Ready(Err(Cancelled))"
        );

        // Cancellation occurring during a Pending poll must be noticed in
        // that poll, even before a listener has ever been registered.
        let cancel = Cancellation::new();
        let pending = std::future::poll_fn(|_| {
            cancel.cancel();
            Poll::Pending::<Result<(), Error>>
        });
        let mut work = pin!(checked(pending, &cancel));
        assert_eq!(
            format!("{:?}", block_on(poll_once(work.as_mut()))),
            "Some(Err(Cancelled))"
        );
    }

    #[test]
    fn pending_completion_wakes_supervisor_only_when_closed_and_drained() {
        let queue = Arc::new(Queue {
            state: Mutex::new(State {
                work: VecDeque::new(),
                connections: VecDeque::new(),
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
        let error = Pending::new(queue.clone()).err().unwrap();
        assert_eq!(error.to_string(), Error::Busy.to_string());
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
        let error = Pending::new(queue.clone()).err().unwrap();
        assert_eq!(error.to_string(), Error::Closed.to_string());
        drop(second);
        assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
        assert!(changed.as_mut().poll(&mut cx).is_pending());
        drop(third);
        assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
        assert!(changed.as_mut().poll(&mut cx).is_ready());
        assert_eq!(lock(&queue.state).pending, 0);
    }

    fn parts(scheduler: &NativeScheduler) -> ClientParts<'_> {
        let endpoint = Endpoint::new("127.0.0.1:1".parse().unwrap());
        make(scheduler, endpoint, Options::default())
    }

    #[test]
    fn acquiring_from_a_closed_pool_fails() {
        let scheduler = NativeScheduler::new().unwrap();
        let parts = parts(&scheduler);
        parts.pool.begin_close();
        let error = futures_lite::future::block_on(parts.context.acquire(&Cancellation::never()))
            .err()
            .unwrap();
        assert_eq!(error.to_string(), Error::Closed.to_string());
    }

    #[test]
    fn construction_after_the_queue_is_abandoned_releases_its_build() {
        let scheduler = NativeScheduler::new().unwrap();
        let parts = parts(&scheduler);
        // Abandonment closes the queue before the pool; a build reserved between the two cannot start.
        parts.queue.abandon();
        let error = futures_lite::future::block_on(parts.context.acquire(&Cancellation::never()))
            .err()
            .unwrap();
        assert_eq!(error.to_string(), Error::Closed.to_string());
        assert_eq!(parts.pool.stats().total(), 0);
    }
}

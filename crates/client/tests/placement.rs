// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! The spawned connection mode: given a scope, a server and a client run each connection as a task of it, so connections progress on more than one worker, while `run` still joins every connection it started.
use base_http::{
    Method, Request, Response, Status,
    body::{BufferedBody, EmptyBody, ReadableBody},
};
use base_http_client::{Client, Endpoint};
use base_http_fixtures::Latch;
use base_http_server::{Handler, Options, Server, ServerContext, Statistics};
use base_scheduler::{
    Cancellation, Clock, SocketBind, SocketListen, SocketLocalAddress, SocketOpen, SocketType,
    SpawnScoped,
};
use base_scheduler_native::{NativeOptions, NativeScheduler};
use futures_lite::future::{block_on, zip};
use std::{
    collections::HashSet,
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread::{self, ThreadId},
    time::Duration,
};

fn scheduler() -> NativeScheduler {
    scheduler_with(2)
}

fn scheduler_with(workers: usize) -> NativeScheduler {
    NativeScheduler::with_options(NativeOptions {
        workers,
        ..Default::default()
    })
    .unwrap()
}

// Run `test` on a thread of its own, failing rather than hanging if it does not finish in time.
fn bounded(test: impl FnOnce() + Send + 'static) {
    let (done, finished) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let _ = done.send(std::panic::catch_unwind(std::panic::AssertUnwindSafe(test)));
    });
    if let Err(panic) = finished
        .recv_timeout(Duration::from_secs(30))
        .expect("the test did not finish")
    {
        std::panic::resume_unwind(panic);
    }
}

async fn listen(
    scheduler: &NativeScheduler,
) -> (
    <NativeScheduler as base_scheduler::SocketOperation>::Socket,
    SocketAddr,
) {
    let never = Cancellation::never();
    let address: SocketAddr = ([127, 0, 0, 1], 0).into();
    let socket = scheduler
        .socket_open(address.into(), SocketType::Stream, None, &never)
        .await
        .unwrap();
    scheduler
        .socket_bind(&socket, address, &never)
        .await
        .unwrap();
    scheduler.socket_listen(&socket, 16, &never).await.unwrap();
    let address = scheduler.socket_local_address(&socket).unwrap();
    (socket, address)
}

/// Connections each side opens at once.
const CONNECTIONS: usize = 8;

// Holds every request until all `CONNECTIONS` are in flight, without blocking a worker, recording the threads it ran on.
#[derive(Default)]
struct Concurrent {
    arrived: AtomicUsize,
    all: Latch,
    threads: Mutex<HashSet<ThreadId>>,
}

impl<'h> Handler<'h, ServerContext> for Concurrent {
    type Error = Infallible;
    type Body<B: ReadableBody + 'h> = BufferedBody<&'static [u8]>;

    async fn call<B: ReadableBody + 'h>(
        &self,
        _: Request<B>,
        _: &mut ServerContext,
        cancellation: &Cancellation,
    ) -> Result<Response<Self::Body<B>>, Infallible> {
        self.threads.lock().unwrap().insert(thread::current().id());
        if self.arrived.fetch_add(1, Ordering::SeqCst) + 1 == CONNECTIONS {
            self.all.release();
        }
        let _ = self.all.wait(cancellation).await;
        Ok(Response::new(
            Status::OK,
            BufferedBody::from(b"concurrent".as_slice()),
        ))
    }
}

// Calls from tasks of the scope, each on its own connection, all in flight at once while the server's connections run as tasks too. Native admits spawned tasks to its workers in rotation, but an awake worker may steal and finish all of them before the others wake, as on a host with few CPUs, so the scenario is repeated until both sides' connections are seen on more than one worker; on two CPUs one attempt fails about one time in thirteen.
#[test]
fn spawned_connections_progress_on_more_than_one_worker() {
    bounded(|| {
        let attempts = 50;
        let spread = (0..attempts).any(|_| {
            let (server, client) = spawned_connections_progress_in_parallel();
            server >= 2 && client >= 2
        });
        assert!(
            spread,
            "connections ran on one worker on each side in {attempts} attempts"
        );
    });
}

// The number of workers the server's and the client's connections ran on.
fn spawned_connections_progress_in_parallel() -> (usize, usize) {
    let scheduler = scheduler_with(4);
    let handler = Concurrent::default();
    let options = Options::default();
    let client_threads = Mutex::new(HashSet::new());
    let stop = Cancellation::new();
    // Every call builds its own connection at once, rather than waiting for another's.
    let client_options = base_http_client::Options {
        pool: base_pool::Options {
            max_constructing: CONNECTIONS,
            ..Default::default()
        },
        ..Default::default()
    };
    let (statistics, statuses) = scheduler.scope(async |scope| {
        let (listener, address) = listen(scope.scheduler()).await;
        let (client, driver) =
            Client::new(scope.scheduler(), Endpoint::new(address), client_options).unwrap();
        let server = Server::new(
            &handler,
            base_http_server::Endpoint::new(listener),
            &options,
        )
        .run(scope, &stop);
        let client_threads = &client_threads;
        let calls = async {
            let tasks: Vec<_> = (0..CONNECTIONS)
                .map(|_| {
                    let client = client.clone();
                    scope
                        .spawn(async move {
                            client_threads
                                .lock()
                                .unwrap()
                                .insert(thread::current().id());
                            let request = Request::new(Method::GET, "/", EmptyBody);
                            client
                                .call(request, &Cancellation::never())
                                .await
                                .unwrap()
                                .status
                        })
                        .unwrap()
                })
                .collect();
            let mut statuses = Vec::new();
            for task in tasks {
                statuses.push(task.await.unwrap());
            }
            client.begin_close();
            drop(client);
            statuses
        };
        let lifetime = Cancellation::new();
        let (statuses, statistics) = zip(
            async {
                let (statuses, driven) = zip(calls, driver.run(scope, &lifetime)).await;
                driven.unwrap();
                stop.cancel();
                statuses
            },
            server,
        )
        .await;
        (statistics.unwrap(), statuses)
    });
    block_on(scheduler.shutdown());
    assert_eq!(statuses, vec![Status::OK; CONNECTIONS]);
    assert_eq!(statistics.connections, CONNECTIONS as u64);
    assert_eq!(statistics.requests, CONNECTIONS as u64);
    let server_threads = handler.threads.into_inner().unwrap();
    let client_threads = client_threads.into_inner().unwrap();
    // Connections ran on the workers, not on the test's thread.
    assert!(!server_threads.contains(&thread::current().id()));
    (server_threads.len(), client_threads.len())
}

struct Panicking;

impl<'h> Handler<'h, ServerContext> for Panicking {
    type Error = Infallible;
    type Body<B: ReadableBody + 'h> = EmptyBody;

    async fn call<B: ReadableBody + 'h>(
        &self,
        _: Request<B>,
        _: &mut ServerContext,
        _: &Cancellation,
    ) -> Result<Response<EmptyBody>, Infallible> {
        panic!("the handler panicked")
    }
}

// Serve one call on `handler` in the spawned mode, then stop the server, returning its statistics and whether the call was answered.
fn serve_one<H: for<'h> Handler<'h, ServerContext> + Sync>(
    scheduler: &NativeScheduler,
    handler: &H,
) -> (Statistics, bool) {
    let options = Options::default();
    let stop = Cancellation::new();
    scheduler.scope(async |scope| {
        let (listener, address) = listen(scope.scheduler()).await;
        let (client, driver) = Client::new(
            scope.scheduler(),
            Endpoint::new(address),
            Default::default(),
        )
        .unwrap();
        let server = Server::new(handler, base_http_server::Endpoint::new(listener), &options)
            .run(scope, &stop);
        let lifetime = Cancellation::new();
        let calling = async {
            let request = Request::new(Method::GET, "/", EmptyBody);
            let answered = client.call(request, &lifetime).await.is_ok();
            client.begin_close();
            drop(client);
            answered
        };
        let (answered, statistics) = zip(
            async {
                let (answered, driven) = zip(calling, driver.run(scope, &lifetime)).await;
                driven.unwrap();
                stop.cancel();
                answered
            },
            server,
        )
        .await;
        (statistics.unwrap(), answered)
    })
}

// A panicking connection is counted as a failed connection, and the server still stops cleanly.
#[test]
fn a_panicking_spawned_connection_counts_as_failed() {
    let scheduler = scheduler();
    let (statistics, answered) = serve_one(&scheduler, &Panicking);
    block_on(scheduler.shutdown());
    assert!(!answered, "a panicked handler produced a response");
    assert_eq!(statistics.connections, 1);
    assert_eq!(statistics.failed_connections, 1);
}

// Holds every request until cancelled, recording, after a delay, that its exchange was destroyed.
struct Holding<'a> {
    entered: &'a AtomicUsize,
    destroyed: &'a AtomicUsize,
}

struct Destroyed<'a>(&'a AtomicUsize);

impl Drop for Destroyed<'_> {
    fn drop(&mut self) {
        thread::sleep(Duration::from_millis(20));
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl<'h> Handler<'h, ServerContext> for Holding<'_> {
    type Error = Infallible;
    type Body<B: ReadableBody + 'h> = EmptyBody;

    async fn call<B: ReadableBody + 'h>(
        &self,
        _: Request<B>,
        _: &mut ServerContext,
        cancellation: &Cancellation,
    ) -> Result<Response<EmptyBody>, Infallible> {
        let _destroyed = Destroyed(self.destroyed);
        self.entered.fetch_add(1, Ordering::SeqCst);
        cancellation.cancelled().await;
        Ok(Response::new(Status::OK, EmptyBody))
    }
}

// Cancelling the server while a spawned connection holds a request returns only after that connection has retired.
#[test]
fn run_joins_spawned_connections_after_cancellation() {
    let scheduler = scheduler();
    let (entered, destroyed) = (AtomicUsize::new(0), AtomicUsize::new(0));
    let handler = Holding {
        entered: &entered,
        destroyed: &destroyed,
    };
    let options = Options::default();
    let stop = Cancellation::new();
    let destroyed_at_return = scheduler.scope(async |scope| {
        let (listener, address) = listen(scope.scheduler()).await;
        let (client, driver) = Client::new(
            scope.scheduler(),
            Endpoint::new(address),
            Default::default(),
        )
        .unwrap();
        let server = Server::new(
            &handler,
            base_http_server::Endpoint::new(listener),
            &options,
        )
        .run(scope, &stop);
        let lifetime = Cancellation::new();
        let call = scope
            .spawn({
                let client = client.clone();
                async move {
                    let request = Request::new(Method::GET, "/", EmptyBody);
                    client.call(request, &Cancellation::never()).await.map(drop)
                }
            })
            .unwrap();
        let stopping = async {
            while entered.load(Ordering::SeqCst) == 0 {
                scope
                    .scheduler()
                    .sleep(Duration::from_millis(1), &Cancellation::never())
                    .await
                    .unwrap();
            }
            stop.cancel();
        };
        // The client's driver runs throughout, so the call's connection can be established.
        let client_side = async {
            let _ = call.await;
            client.begin_close();
            drop(client);
        };
        let serving = async {
            let (statistics, ()) = zip(server, stopping).await;
            statistics.unwrap();
            destroyed.load(Ordering::SeqCst)
        };
        let ((destroyed_at_return, ()), driven) =
            zip(zip(serving, client_side), driver.run(scope, &lifetime)).await;
        let _ = driven;
        destroyed_at_return
    });
    block_on(scheduler.shutdown());
    assert_eq!(
        destroyed_at_return, 1,
        "the server returned before its connection retired"
    );
}

// Spawning fails as a scoped spawn's can: the connection is dropped unspawned, which reports it closed to its call.
#[derive(Clone, Copy)]
struct Refusing<'a> {
    scheduler: &'a NativeScheduler,
    error: fn() -> base_scheduler::SpawnError,
}

impl<'a> base_scheduler::Parent<'a> for Refusing<'a> {
    type Scheduler = NativeScheduler;

    fn scheduler(self) -> &'a NativeScheduler {
        self.scheduler
    }

    fn place<F>(
        self,
        _: F,
    ) -> Result<
        impl Future<Output = Result<F::Output, base_scheduler::TaskError>> + Send + 'a,
        base_scheduler::SpawnError,
    >
    where
        F: Future + Send + 'a,
        F::Output: Send + 'a,
    {
        Err::<std::future::Pending<_>, _>((self.error)())
    }
}

#[test]
fn a_refused_connection_spawn_fails_the_call() {
    let scheduler = scheduler();
    for error in [
        (|| base_scheduler::SpawnError::SchedulerClosed) as fn() -> _,
        || base_scheduler::SpawnError::IdentifiersExhausted,
    ] {
        let parent = Refusing {
            scheduler: &scheduler,
            error,
        };
        let address: SocketAddr = ([127, 0, 0, 1], 9).into();
        let (client, driver) =
            Client::new(&scheduler, Endpoint::new(address), Default::default()).unwrap();
        let lifetime = Cancellation::new();
        let calling = async {
            let request = Request::new(Method::GET, "/", EmptyBody);
            let result = client.call(request, &lifetime).await;
            client.begin_close();
            result
        };
        let (result, driven) = block_on(zip(calling, driver.run(parent, &lifetime)));
        drop(client);
        driven.unwrap();
        assert!(matches!(result, Err(base_http_client::Error::Closed)));
    }
    block_on(scheduler.shutdown());
}

#[test]
fn a_connection_dropped_after_the_lifetime_ends_cancels_the_call() {
    // The lifetime ends after the call queues its connection and before the
    // driver first runs, which then drops the connection without spawning it.
    let scheduler = scheduler();
    let parent = Refusing {
        scheduler: &scheduler,
        error: || base_scheduler::SpawnError::SchedulerClosed,
    };
    let address: SocketAddr = ([127, 0, 0, 1], 9).into();
    let (client, driver) =
        Client::new(&scheduler, Endpoint::new(address), Default::default()).unwrap();
    let lifetime = Cancellation::new();
    let calling = async {
        let never = Cancellation::never();
        let mut call =
            std::pin::pin!(client.call(Request::new(Method::GET, "/", EmptyBody), &never));
        assert!(
            futures_lite::future::poll_once(call.as_mut())
                .await
                .is_none()
        );
        lifetime.cancel();
        let result = call.await;
        client.begin_close();
        result
    };
    let (result, driven) = block_on(zip(calling, driver.run(parent, &lifetime)));
    drop(client);
    driven.unwrap();
    let error = result.err();
    assert!(
        matches!(error, Some(base_http_client::Error::Cancelled)),
        "{error:?}"
    );
    block_on(scheduler.shutdown());
}

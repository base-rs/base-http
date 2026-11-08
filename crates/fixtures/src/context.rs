// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Latch, RawPeer, Scheduler, native};
use base_http::{
    InterimHandler, InterimResponses, Method, Request, Response, Status, Version,
    body::{BufferedBody, ReadableBody},
};
use base_http_client::{Client, Driver, Endpoint};
use base_http_server::Handler;
use base_scheduler::{Cancellation, Clock, SocketType, Spawn, TaskError};
use base_scheduler_native::{NativeOptions, NativeScheduler};
use futures_lite::future::{block_on, or, zip};
use std::{
    any::Any,
    convert::Infallible,
    fmt,
    future::{Future, poll_fn},
    net::SocketAddr,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::pin,
    sync::{
        Arc,
        mpsc::{self, RecvTimeoutError},
    },
    task::Poll,
    thread,
    time::Duration,
};

/// The default bound on a test body.
pub const TEST_TIMEOUT: Duration = Duration::from_secs(10);
/// The default bound on closing the client and stopping the server after a test.
pub const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// The default application: every request receives `200 OK` with `Hello World!`.
#[derive(Clone, Copy, Debug, Default)]
pub struct Hello;

impl Handler for Hello {
    type Error = Infallible;
    type Body<B: ReadableBody> = BufferedBody<&'static [u8]>;

    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        _: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<Response<Self::Body<B>>, Infallible> {
        Ok(Response::new(
            Status::OK,
            BufferedBody::from(b"Hello World!".as_slice()),
        ))
    }
}

#[derive(Clone, Copy, Debug)]
struct Limits {
    test: Duration,
    teardown: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            test: TEST_TIMEOUT,
            teardown: TEARDOWN_TIMEOUT,
        }
    }
}

#[cfg(feature = "rustls")]
#[derive(Clone, Debug)]
struct Tls {
    server: std::sync::Arc<rustls::ServerConfig>,
    client: std::sync::Arc<rustls::ClientConfig>,
}

#[cfg(feature = "rustls")]
impl Tls {
    // A fresh self-signed identity for `localhost`, trusted only by this fixture's client.
    fn generate() -> Self {
        use rustls::{ClientConfig, RootCertStore, ServerConfig, pki_types::PrivatePkcs8KeyDer};
        let identity = rcgen::generate_simple_self_signed(vec![SERVER_NAME.to_owned()])
            .expect("generate a test certificate");
        let certificate = identity.cert.der().clone();
        let key = PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der());
        let mut roots = RootCertStore::empty();
        roots
            .add(certificate.clone())
            .expect("trust the test certificate");
        let client = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let server = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certificate], key.into())
            .expect("configure the test certificate");
        Self {
            server: std::sync::Arc::new(server),
            client: std::sync::Arc::new(client),
        }
    }
}

/// The TLS server name of a fixture's generated certificate.
#[cfg(feature = "rustls")]
pub const SERVER_NAME: &str = "localhost";

/// A server bound to an OS-selected loopback port, with a client connected to it, for the duration of one test.
///
/// This follows `sus-fixtures-async-http`'s `ServerContext`: builder methods replace its overridable `app`, `protocol`, endpoint and client options. [`ServerContext::run`] binds the listener before serving, so the client never races the server's start, then runs the test beside the server and the client's driver.
///
/// Rust has no asynchronous destructor, so teardown happens inside `run`, in order: the client stops accepting calls and retires its connections, then the server stops, closing its listener and joining its connections. Teardown is bounded by [`ServerContext::teardown_timeout`]; when it expires, the client and server are cancelled, still joined, and the test fails. A panic in the test is propagated after teardown. The test must not return a response body it has not finished with, since its connection cannot retire.
#[derive(Debug)]
pub struct ServerContext<H = Hello> {
    handler: H,
    version: Version,
    server: base_http_server::Options,
    client: base_http_client::Options,
    limits: Limits,
    #[cfg(feature = "rustls")]
    tls: Option<Tls>,
}

impl ServerContext {
    /// Serve [`Hello`] over HTTP/1.1 with default options.
    pub fn new() -> Self {
        Self {
            handler: Hello,
            version: Version::Http11,
            server: base_http_server::Options::default(),
            client: base_http_client::Options::default(),
            limits: Limits::default(),
            #[cfg(feature = "rustls")]
            tls: None,
        }
    }
}

impl Default for ServerContext {
    fn default() -> Self {
        Self::new()
    }
}

impl<H> ServerContext<H> {
    /// Serve `handler` instead, as overriding `app` does.
    pub fn handler<A>(self, handler: A) -> ServerContext<A> {
        ServerContext {
            handler,
            version: self.version,
            server: self.server,
            client: self.client,
            limits: self.limits,
            #[cfg(feature = "rustls")]
            tls: self.tls,
        }
    }

    /// The HTTP version of requests built with [`Context::request`].
    pub fn version(mut self, version: Version) -> Self {
        self.version = version;
        self
    }

    /// Options for the server.
    pub fn server_options(mut self, options: base_http_server::Options) -> Self {
        self.server = options;
        self
    }

    /// Options for the client.
    pub fn client_options(mut self, options: base_http_client::Options) -> Self {
        self.client = options;
        self
    }

    /// Bound the test body, which fails when it is still running after `timeout`. Defaults to [`TEST_TIMEOUT`].
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.limits.test = timeout;
        self
    }

    /// Bound teardown, which is forced and fails the test after `timeout`. Defaults to [`TEARDOWN_TIMEOUT`].
    pub fn teardown_timeout(mut self, timeout: Duration) -> Self {
        self.limits.teardown = timeout;
        self
    }

    /// Serve HTTPS with a freshly generated certificate for [`SERVER_NAME`], which only this fixture's client trusts.
    #[cfg(feature = "rustls")]
    pub fn tls(mut self) -> Self {
        self.tls = Some(Tls::generate());
        self
    }
}

impl<H: Handler> ServerContext<H> {
    /// Run `test` with a fresh Native scheduler, which is shut down afterwards.
    pub fn run<T>(self, test: impl AsyncFnOnce(&Context<'_, NativeScheduler, H>) -> T) -> T {
        native(|scheduler| self.run_on(scheduler, test))
    }

    /// Run `test` on `scheduler`, which the caller owns and shuts down.
    pub fn run_on<'s, S: Scheduler, T>(
        self,
        scheduler: &'s S,
        test: impl AsyncFnOnce(&Context<'s, S, H>) -> T,
    ) -> T {
        let Self {
            handler,
            version,
            server,
            client,
            limits,
            #[cfg(feature = "rustls")]
            tls,
        } = self;
        block_on(async {
            let listener = listen(scheduler).await;
            let address = scheduler
                .socket_local_address(&listener)
                .expect("listener address");
            let endpoint = Endpoint::new(address);
            #[cfg(feature = "rustls")]
            let endpoint = match &tls {
                Some(tls) => endpoint.with_tls(tls.client.clone(), SERVER_NAME),
                None => endpoint,
            };
            let (client, driver) =
                Client::new(scheduler, endpoint.clone(), client).expect("valid client options");
            let context = Context {
                scheduler,
                handler: Arc::new(handler),
                client,
                endpoint,
                address,
                version,
            };
            let stop = Cancellation::new();
            let serving = async {
                #[cfg(feature = "rustls")]
                if let Some(tls) = &tls {
                    return base_http_server::serve_tls(
                        scheduler,
                        listener,
                        tls.server.clone(),
                        &*context.handler,
                        &server,
                        &stop,
                    )
                    .await;
                }
                base_http_server::serve(scheduler, listener, &*context.handler, &server, &stop)
                    .await
            };
            let supervised = supervise(
                scheduler,
                limits,
                &context.client,
                driver,
                serving,
                &stop,
                test(&context),
            )
            .await;
            // A test failure is reported first: it is usually the cause of a server failure.
            let Supervised {
                tested,
                served,
                forced,
            } = supervised;
            // `finish` may panic for the test, a forced teardown or a remaining connection before the server's result is reported, so print it rather than drop it.
            if let Err(error) = &served {
                eprintln!("server failed: {error}");
            }
            let output = finish(tested, forced, &context.client, limits);
            if let Err(error) = served {
                panic!("server failed: {error}");
            }
            output
        })
    }
}

impl<H: Handler + Send + Sync + 'static> ServerContext<H> {
    /// Run `test` against a server spawned onto the workers of a fresh two-worker Native scheduler, with the client driven from the test's thread, then stop the server and shut the scheduler down.
    ///
    /// Unlike [`ServerContext::run`], the server makes progress on the scheduler's workers rather than on the test's thread, so it runs in parallel with the client. That exposes cross-thread wake-ups and races which a single `block_on` hides, at the cost of reproducible interleavings. The fixture relies only on the server running on the scheduler's workers; how the server spreads its connections across them is the server's choice and may change. The context owns the scheduler and the application, so the application must be `Send + Sync + 'static`, as a server spawning its own tasks needs.
    ///
    /// This has the guarantees of [`ServerContext::run`]: the listener is bound before the test starts, the test is bounded, the client retires its connections before the server stops, the server is cancelled and must stop within the teardown timeout, a server error fails the test, a server panic is resumed with its own payload, and a test failure is reported first, with any server failure printed beforehand. The server's teardown timeout is measured on a thread of its own, so a server occupying every worker still fails its join on time. This is a temporary workaround for the Native scheduler stranding timers and I/O behind a busy worker. Since scheduler 0.5 Native rescues stranded timers, within its bound only while another worker is idle, but not yet readiness ([base-rs/base-scheduler#93](https://github.com/base-rs/base-scheduler/pull/93)); the workaround is to be removed once Native rescues both. The test's timeout and the client's teardown are still measured by the scheduler's clock, so they are bounded only while the scheduler's timers can run.
    ///
    /// Shutting the scheduler down afterwards waits for every task's current poll to return. A poll which never returns, such as a handler blocked forever, cannot be preempted, so it hangs the test after the failure is reported.
    pub fn run_spawned<T>(
        self,
        test: impl AsyncFnOnce(&Context<'_, NativeScheduler, H>) -> T,
    ) -> T {
        shared_native(2, |scheduler| self.run_spawned_on(scheduler, test))
    }

    // Spawn the server onto `scheduler`, run the test with a client on this thread, then join the server, reporting failures in order.
    fn run_spawned_on<S, T>(
        self,
        scheduler: &Arc<S>,
        test: impl AsyncFnOnce(&Context<'_, S, H>) -> T,
    ) -> T
    where
        S: Scheduler + Spawn + Send + Sync + 'static,
        S::Socket: Send,
    {
        let mut server = self.spawn(scheduler);
        let tested = catch_unwind(AssertUnwindSafe(|| server.run(test)));
        let stopped = block_on(server.join());
        let output = match tested {
            Ok(output) => output,
            Err(panic) => {
                // The test's failure is usually the cause of a server failure, so report it first.
                if let Err(failure) = &stopped {
                    eprintln!("server failed: {failure}");
                }
                resume_unwind(panic)
            }
        };
        stopped.unwrap_or_else(|failure| failure.raise());
        output
    }

    // Bind a listener, then spawn the server's public entry point as one task. The task must own a `'static` handle to the scheduler and the application, so both are shared through an `Arc`.
    fn spawn<S>(self, scheduler: &Arc<S>) -> Spawned<S, H>
    where
        S: Scheduler + Spawn + Send + Sync + 'static,
        S::Socket: Send,
    {
        let Self {
            handler,
            version,
            server,
            client,
            limits,
            #[cfg(feature = "rustls")]
            tls,
        } = self;
        let listener = block_on(listen(&**scheduler));
        let address = scheduler
            .socket_local_address(&listener)
            .expect("listener address");
        let endpoint = Endpoint::new(address);
        #[cfg(feature = "rustls")]
        let endpoint = match &tls {
            Some(tls) => endpoint.with_tls(tls.client.clone(), SERVER_NAME),
            None => endpoint,
        };
        let handler = Arc::new(handler);
        let stop = Cancellation::new();
        let serving = {
            let (scheduler, handler, stop) = (scheduler.clone(), handler.clone(), stop.clone());
            #[cfg(feature = "rustls")]
            let tls = tls.map(|tls| tls.server);
            async move {
                #[cfg(feature = "rustls")]
                if let Some(config) = tls {
                    return base_http_server::serve_tls(
                        &*scheduler,
                        listener,
                        config,
                        &*handler,
                        &server,
                        &stop,
                    )
                    .await;
                }
                base_http_server::serve(&*scheduler, listener, &*handler, &server, &stop).await
            }
        };
        let task = scheduler.spawn(serving).expect("spawn the server");
        Spawned {
            scheduler: scheduler.clone(),
            task: Some(task),
            stop,
            handler,
            endpoint,
            address,
            version,
            client,
            limits,
        }
    }
}

// A server spawned onto a scheduler's workers. `run_spawned_on` always joins it, so it needs no destructor.
struct Spawned<S: Spawn, H> {
    scheduler: Arc<S>,
    // `None` once the server has been joined.
    task: Option<S::Handle<Served>>,
    stop: Cancellation,
    handler: Arc<H>,
    endpoint: Endpoint,
    address: SocketAddr,
    version: Version,
    client: base_http_client::Options,
    limits: Limits,
}

type Served = Result<base_http_server::Statistics, base_http_server::Error>;

// Why a spawned server did not stop cleanly.
enum Failure {
    Server(base_http_server::Error),
    Task(TaskError),
    Stalled(Duration),
}

impl Failure {
    // Re-raise a server panic with its own payload, and fail for anything else.
    fn raise(self) -> ! {
        match self {
            Self::Task(TaskError::Panicked(panic)) => resume_unwind(panic),
            failure => panic!("server failed: {failure}"),
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Server(error) => error.fmt(formatter),
            Self::Task(error) => error.fmt(formatter),
            Self::Stalled(timeout) => write!(formatter, "it did not stop within {timeout:?}"),
        }
    }
}

impl<S: Scheduler + Spawn, H> Spawned<S, H> {
    // Run `test` with a fresh client of this server, driven on this thread, with the test and client teardown bounded as in `run`.
    fn run<T>(&self, test: impl AsyncFnOnce(&Context<'_, S, H>) -> T) -> T {
        let scheduler = &*self.scheduler;
        block_on(async {
            let (client, driver) =
                Client::new(scheduler, self.endpoint.clone(), self.client.clone())
                    .expect("valid client options");
            let context = Context {
                scheduler,
                handler: self.handler.clone(),
                client,
                endpoint: self.endpoint.clone(),
                address: self.address,
                version: self.version,
            };
            let stop = Cancellation::new();
            let supervised = supervise(
                scheduler,
                self.limits,
                &context.client,
                driver,
                async {},
                &stop,
                test(&context),
            )
            .await;
            finish(
                supervised.tested,
                supervised.forced,
                &context.client,
                self.limits,
            )
        })
    }

    // Cancel the server and join it within the teardown timeout. A server which overruns is destroyed once its task handle is dropped and any poll in progress returns.
    async fn join(&mut self) -> Result<(), Failure> {
        self.stop.cancel();
        let Some(task) = self.task.take() else {
            unreachable!("a spawned server is joined only once")
        };
        let timeout = self.limits.teardown;
        let joined = or(async { Some(task.await) }, async {
            expiry(timeout).await;
            None
        })
        .await;
        match joined {
            Some(Ok(served)) => served.map(drop).map_err(Failure::Server),
            Some(Err(error)) => Err(Failure::Task(error)),
            None => Err(Failure::Stalled(timeout)),
        }
    }
}

// Expire after `timeout`, measured on a thread of its own rather than by the scheduler's clock, so a server occupying every worker, such as a handler which blocks, still fails its join. Shutting the scheduler down afterwards still waits for that poll to return. Dropping the future stops the thread.
//
// Temporary: Native strands timers and I/O registered from outside a worker behind a busy worker, so the scheduler's clock cannot bound the join. Scheduler 0.5 rescues stranded timers, within its bound only while another worker is idle, but not yet readiness; remove this once Native rescues both. See https://github.com/base-rs/base-scheduler/pull/93.
async fn expiry(timeout: Duration) {
    let expired = Latch::new();
    let (_cancel, cancelled) = mpsc::channel::<()>();
    let release = expired.clone();
    thread::spawn(move || {
        if let Err(RecvTimeoutError::Timeout) = cancelled.recv_timeout(timeout) {
            release.release();
        }
    });
    let _ = expired.wait(&Cancellation::never()).await;
}

// Run `work` with a fresh Native scheduler of `workers` workers, shared through an `Arc` so spawned tasks can own it, then shut it down, also when `work` panics.
fn shared_native<T>(workers: usize, work: impl FnOnce(&Arc<NativeScheduler>) -> T) -> T {
    let scheduler = Arc::new(
        NativeScheduler::with_options(NativeOptions {
            workers,
            ..Default::default()
        })
        .expect("construct a Native scheduler"),
    );
    let result = catch_unwind(AssertUnwindSafe(|| work(&scheduler)));
    block_on(scheduler.shutdown());
    result.unwrap_or_else(|panic| resume_unwind(panic))
}

async fn listen<S: Scheduler>(scheduler: &S) -> S::Socket {
    let never = Cancellation::never();
    let address: SocketAddr = ([127, 0, 0, 1], 0).into();
    let socket = scheduler
        .socket_open(address.into(), SocketType::Stream, None, &never)
        .await
        .expect("open a listener");
    scheduler
        .socket_bind(&socket, address, &never)
        .await
        .expect("bind a listener");
    scheduler
        .socket_listen(&socket, 128, &never)
        .await
        .expect("listen");
    socket
}

/// What a test receives from [`ServerContext::run`] or [`ServerContext::run_spawned`].
pub struct Context<'s, S, H> {
    scheduler: &'s S,
    // Shared with a spawned server's task.
    handler: Arc<H>,
    client: Client<'s>,
    endpoint: Endpoint,
    address: SocketAddr,
    version: Version,
}

impl<'s, S, H> Context<'s, S, H> {
    /// The scheduler running the server and client.
    pub fn scheduler(&self) -> &'s S {
        self.scheduler
    }

    /// The application being served.
    pub fn handler(&self) -> &H {
        &self.handler
    }

    /// A client of the server. Its driver runs beside the test.
    pub fn client(&self) -> &Client<'s> {
        &self.client
    }

    /// The client's endpoint, including any TLS configuration, for constructing further clients.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// The server's listening address.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// The server's URL, such as `http://127.0.0.1:49152`.
    pub fn url(&self) -> String {
        format!("{}://{}", self.endpoint.scheme(), self.endpoint.authority)
    }

    /// The HTTP version this context tests.
    pub fn version(&self) -> Version {
        self.version
    }

    /// A request for `path` using this context's HTTP version.
    pub fn request<B>(&self, method: Method, path: &str, body: B) -> Request<B> {
        let mut request = Request::new(method, path, body);
        request.version = Some(self.version);
        request
    }

    /// Run `work` with a raw client connected to the server, for wire-level requests, on its own thread. It does not use TLS.
    ///
    /// The client's driver, and with [`ServerContext::run`] the server too, runs on the test's thread, so a blocking peer there would stall them; the test instead awaits the peer thread without blocking. A panic in `work` propagates.
    pub async fn raw<T: Send + 'static>(
        &self,
        work: impl FnOnce(RawPeer) -> T + Send + 'static,
    ) -> T {
        let address = self.address;
        let finished = Latch::new();
        let release = Released(finished.clone());
        let thread = thread::spawn(move || {
            // Release when `work` returns or unwinds.
            let _release = release;
            work(RawPeer::connect(address))
        });
        finished
            .wait(&Cancellation::never())
            .await
            .expect("an uncancellable wait");
        match thread.join() {
            Ok(output) => output,
            Err(panic) => resume_unwind(panic),
        }
    }
}

// Releases its latch when dropped.
struct Released(Latch);

impl Drop for Released {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// A client of an existing endpoint, such as a [`RawServer`](crate::RawServer), for the duration of one test.
///
/// The client's driver runs beside the test. Teardown follows [`ServerContext`]: the client stops accepting calls and retires its connections within a bounded time, the test fails if any connection remains, and a panic in the test is propagated afterwards.
#[derive(Debug)]
pub struct ClientContext {
    endpoint: Endpoint,
    options: base_http_client::Options,
    limits: Limits,
}

impl ClientContext {
    /// A client of `endpoint` with default options.
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            options: base_http_client::Options::default(),
            limits: Limits::default(),
        }
    }

    /// Options for the client.
    pub fn options(mut self, options: base_http_client::Options) -> Self {
        self.options = options;
        self
    }

    /// Bound the test body. Defaults to [`TEST_TIMEOUT`].
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.limits.test = timeout;
        self
    }

    /// Bound teardown. Defaults to [`TEARDOWN_TIMEOUT`].
    pub fn teardown_timeout(mut self, timeout: Duration) -> Self {
        self.limits.teardown = timeout;
        self
    }

    /// Run `test` with a fresh Native scheduler, which is shut down afterwards.
    pub fn run<T>(self, test: impl AsyncFnOnce(&Client<'_>) -> T) -> T {
        native(|scheduler| self.run_on(scheduler, test))
    }

    /// Run `test` on `scheduler`, which the caller owns and shuts down.
    pub fn run_on<'s, S: Scheduler, T>(
        self,
        scheduler: &'s S,
        test: impl AsyncFnOnce(&Client<'s>) -> T,
    ) -> T {
        block_on(async {
            let (client, driver) =
                Client::new(scheduler, self.endpoint, self.options).expect("valid client options");
            let stop = Cancellation::new();
            let supervised = supervise(
                scheduler,
                self.limits,
                &client,
                driver,
                async {},
                &stop,
                test(&client),
            )
            .await;
            finish(supervised.tested, supervised.forced, &client, self.limits)
        })
    }
}

struct Supervised<T, R> {
    // `None` when the test exceeded its timeout.
    tested: Option<Result<T, Box<dyn Any + Send>>>,
    served: R,
    forced: bool,
}

// Return the test's output, or fail for a test panic or timeout, a forced teardown or a remaining connection.
fn finish<T>(
    tested: Option<Result<T, Box<dyn Any + Send>>>,
    forced: bool,
    client: &Client<'_>,
    limits: Limits,
) -> T {
    let output = match tested {
        Some(Ok(output)) => output,
        Some(Err(panic)) => resume_unwind(panic),
        None => panic!("test exceeded {:?}", limits.test),
    };
    assert!(
        !forced,
        "teardown exceeded {:?}; the client and server were cancelled",
        limits.teardown
    );
    assert_eq!(
        client.pool_stats().total(),
        0,
        "client connections remain after teardown"
    );
    output
}

// Run the test beside the server and the client's driver, then close the client and stop the server, forcing both down if teardown exceeds its limit.
async fn supervise<S: Clock, T, R>(
    scheduler: &S,
    limits: Limits,
    client: &Client<'_>,
    driver: Driver<'_>,
    server: impl Future<Output = R>,
    stop: &Cancellation,
    test: impl Future<Output = T>,
) -> Supervised<T, R> {
    let lifetime = Cancellation::new();
    let finished = Cancellation::new();
    let retired = Cancellation::new();
    let work = async {
        let client_side = async {
            let tested = async {
                let tested = bounded(scheduler, limits.test, test).await;
                finished.cancel();
                client.begin_close();
                tested
            };
            let (driven, tested) = zip(driver.run(&lifetime), tested).await;
            stop.cancel();
            // A driver only fails when its lifetime is cancelled, which forced teardown reports.
            let _ = driven;
            tested
        };
        let (served, tested) = zip(server, client_side).await;
        retired.cancel();
        (served, tested)
    };
    let watchdog = async {
        finished.cancelled().await;
        let expired = scheduler.sleep(limits.teardown, &retired).await.is_ok();
        if expired {
            lifetime.cancel();
            stop.cancel();
        }
        expired
    };
    let ((served, tested), forced) = zip(work, watchdog).await;
    Supervised {
        tested,
        served,
        forced,
    }
}

// Poll the test until it completes, panics or exceeds `timeout`. Dropping a timed-out test releases what it borrows before teardown.
async fn bounded<S: Clock, T>(
    scheduler: &S,
    timeout: Duration,
    test: impl Future<Output = T>,
) -> Option<Result<T, Box<dyn Any + Send>>> {
    let never = Cancellation::never();
    let mut test = pin!(test);
    let mut deadline = pin!(scheduler.sleep(timeout, &never));
    poll_fn(|cx| {
        match catch_unwind(AssertUnwindSafe(|| test.as_mut().poll(cx))) {
            Ok(Poll::Pending) => {}
            Ok(Poll::Ready(output)) => return Poll::Ready(Some(Ok(output))),
            Err(panic) => return Poll::Ready(Some(Err(panic))),
        }
        deadline.as_mut().poll(cx).map(|_| None)
    })
    .await
}

#[cfg(test)]
mod tests;

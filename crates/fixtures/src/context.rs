// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Latch, RawPeer, Scheduler, native};
use base_http::{
    InterimHandler, InterimResponses, Method, Request, Response, Status, Version,
    body::{BufferedBody, ReadableBody},
};
use base_http_client::{Client, Driver, Endpoint};
use base_http_server::Handler;
use base_scheduler::{Cancellation, Clock, SocketType};
use base_scheduler_native::NativeScheduler;
use futures_lite::future::{block_on, zip};
use std::{
    any::Any,
    convert::Infallible,
    future::{Future, poll_fn},
    net::SocketAddr,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::pin,
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
                handler,
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
                        &context.handler,
                        &server,
                        &stop,
                    )
                    .await;
                }
                base_http_server::serve(scheduler, listener, &context.handler, &server, &stop).await
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

/// What a test receives from [`ServerContext::run`].
pub struct Context<'s, S, H> {
    scheduler: &'s S,
    handler: H,
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
    /// The server runs on the test's thread, so a blocking peer there would stall it; the test instead awaits the peer thread without blocking. A panic in `work` propagates.
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

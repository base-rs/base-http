// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Application handlers and supervised HTTP/1 connections over Base scheduler
//! capabilities. All connection futures are owned and joined by the server.
#![doc = include_str!("../readme.md")]
#![forbid(unsafe_code)]

use base_endpoint::stream::ReadableStream;
use base_http::Scheme;
use base_http_protocol::ProtocolError;

mod connection;
use base_scheduler::{
    Cancellation, Clock, OperationError, Placement, SocketAccept, SocketBind, SocketClose,
    SocketListen, SocketLocalAddress, SocketOpen, SocketOperation, SocketPeerAddress,
    SocketSetOption, SocketStreamReading, SocketStreamWriting, SocketType, SpawnError, TaskError,
    TcpNoDelay,
};
use connection::ServerConnection;
use futures_util::{
    future::{Either, select},
    stream::{FuturesUnordered, StreamExt},
};
use std::{
    fmt,
    future::{Future, poll_fn},
    io,
    net::SocketAddr,
    pin::pin,
    sync::Arc,
    task::Poll,
    time::Duration,
};

/// The shared request interface, implemented by server applications for
/// [`ServerContext`], which carries the connection's [`Peer`].
pub use base_http::{Handler, HandlerContext, Peer, ServerContext, call_send};

#[derive(Clone, Debug)]
pub struct Options {
    pub http1: base_http1::Options,
    /// HTTP engine selection, shared by plain and encrypted establishment.
    pub protocols: base_http_protocol::HttpProtocols,
    pub max_connections: usize,
    /// Disable Nagle's algorithm on each accepted connection, so small writes
    /// such as streamed frames are not held back waiting for acknowledgement.
    /// This is a best-effort latency hint: a socket that rejects it is still
    /// served.
    pub tcp_nodelay: bool,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            http1: base_http1::Options::default(),
            protocols: base_http_protocol::HttpProtocols::default(),
            max_connections: 256,
            tcp_nodelay: true,
        }
    }
}

/// A server failure. Protocol and negotiation errors are transparent; other wrapping variants describe this layer and report their cause as the source, so a cause chain never repeats a message.
#[derive(Debug)]
pub enum Error {
    /// The connection's engine failed, such as on a malformed request.
    Protocol(ProtocolError),
    Handler(Box<dyn std::error::Error + Send + Sync>),
    /// A handler returned a final response which the protocol writer refused before writing any of it, such as an informational status or conflicting framing.
    Response(ProtocolError),
    Io(std::io::Error),
    Cancelled,
    Negotiation(base_http_protocol::NegotiationError),
    /// A connection could not be spawned into its scope, as when the scheduler is closed.
    Spawn(SpawnError),
    /// A spawned connection panicked; it is counted as a failed connection.
    Panicked,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(e) => e.fmt(f),
            Self::Handler(_) => f.write_str("request handler failed"),
            Self::Response(_) => f.write_str("request handler returned an invalid response"),
            Self::Io(_) => f.write_str("HTTP server I/O failed"),
            Self::Cancelled => f.write_str("HTTP server cancelled"),
            Self::Negotiation(error) => error.fmt(f),
            Self::Spawn(_) => f.write_str("HTTP connection could not be spawned"),
            Self::Panicked => f.write_str("HTTP connection panicked"),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Protocol(e) => e.source(),
            Self::Handler(e) => Some(e.as_ref()),
            Self::Response(e) => Some(e),
            Self::Io(e) => Some(e),
            Self::Cancelled => None,
            Self::Negotiation(error) => error.source(),
            Self::Spawn(error) => Some(error),
            Self::Panicked => None,
        }
    }
}
impl From<base_http1::Error> for Error {
    fn from(e: base_http1::Error) -> Self {
        ProtocolError::from(e).into()
    }
}
impl From<ProtocolError> for Error {
    fn from(error: ProtocolError) -> Self {
        match error {
            ProtocolError::Http1(base_http1::Error::Cancelled) => Self::Cancelled,
            error => Self::Protocol(error),
        }
    }
}
impl From<SpawnError> for Error {
    fn from(error: SpawnError) -> Self {
        Self::Spawn(error)
    }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<base_scheduler::OperationError> for Error {
    fn from(error: base_scheduler::OperationError) -> Self {
        base_http1::Error::from(error).into()
    }
}

pub(crate) async fn cancellable<F: Future>(
    future: F,
    cancellation: &Cancellation,
) -> Result<F::Output, Error> {
    cancellation.check().map_err(|_| Error::Cancelled)?;
    if cancellation.is_never() {
        return Ok(future.await);
    }
    let mut future = pin!(future);
    let mut cancelled = pin!(cancellation.cancelled());
    poll_fn(|cx| {
        if cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(Error::Cancelled));
        }
        future.as_mut().poll(cx).map(Ok)
    })
    .await
}

/// Own one accepted socket through request admission, handler execution, output,
/// uncancellable lane retirement, and finally exclusive socket closure. The
/// engine is `options.protocols`'s plaintext selection. Await completion after
/// cooperative cancellation; dropping this future is not a join.
pub fn serve_connection<'scope, S, H>(
    scheduler: &'scope S,
    socket: S::Socket,
    handler: &H,
    options: &Options,
    cancellation: &Cancellation,
) -> impl Future<Output = Result<u64, Error>> + Send
where
    S: SocketStreamReading
        + SocketStreamWriting
        + SocketClose
        + SocketPeerAddress
        + SocketLocalAddress,
    S::Socket: 'static,
    H: Handler<'scope, ServerContext>,
    S: Sync,
    S::Socket: Send,
{
    // Stating `Send` on the returned type proves it here, for every request
    // body lifetime, rather than in each caller's future.
    serve_connection_inner(scheduler, socket, handler, options, cancellation)
}

async fn serve_connection_inner<'scope, S, H>(
    scheduler: &'scope S,
    socket: S::Socket,
    handler: &H,
    options: &Options,
    cancellation: &Cancellation,
) -> Result<u64, Error>
where
    S: SocketStreamReading
        + SocketStreamWriting
        + SocketClose
        + SocketPeerAddress
        + SocketLocalAddress,
    S::Socket: 'static,
    H: Handler<'scope, ServerContext>,
{
    let outcome = connection_outcome(scheduler, socket, handler, options, cancellation).await;
    outcome.result.map(|()| outcome.requests)
}

struct Outcome {
    requests: u64,
    result: Result<(), Error>,
}

async fn connection_outcome<'scope, S, H>(
    scheduler: &'scope S,
    socket: S::Socket,
    handler: &H,
    options: &Options,
    cancellation: &Cancellation,
) -> Outcome
where
    S: SocketStreamReading
        + SocketStreamWriting
        + SocketClose
        + SocketPeerAddress
        + SocketLocalAddress,
    S::Socket: 'static,
    H: Handler<'scope, ServerContext>,
{
    let mut requests = 0;
    // The outer owner closes the socket even when capability construction fails.
    let result = async {
        options.http1.validate()?;
        let protocol = options.protocols.plaintext()?;
        let mut reader = scheduler.socket_stream_reader(&socket)?;
        let writer = match scheduler.socket_stream_writer(&socket) {
            Ok(writer) => writer,
            Err(error) => {
                // Report the construction failure in preference to a retirement failure.
                let _ = ReadableStream::close(&mut reader).await;
                return Err(Error::Io(error));
            }
        };
        // Addresses a transport cannot report, such as a Unix socket's, are absent.
        let peer = Arc::new(Peer::new(
            scheduler.socket_peer_address(&socket).ok(),
            scheduler.socket_local_address(&socket).ok(),
        ));
        ServerConnection::new(protocol, reader, writer, options)
            .serve(handler, &peer, cancellation, &mut requests, &Scheme::HTTP)
            .await
    }
    .await;
    let closed = scheduler.socket_close(socket).await;
    Outcome {
        requests,
        result: result.and_then(|()| Ok(closed?)),
    }
}

#[derive(Default, Debug)]
pub struct Statistics {
    pub connections: u64,
    pub requests: u64,
    pub failed_connections: u64,
    /// Transient accept failures which the listener survived, such as a peer
    /// aborting before acceptance or descriptor exhaustion.
    pub accept_errors: u64,
}

/// How long acceptance pauses after a resource-exhaustion failure. The pending
/// peer stays queued in the backlog, while existing connections progress and
/// may release descriptors or memory meanwhile.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// Consecutive immediate retries before acceptance backs off anyway. A
/// failure which completes without suspending would otherwise spin within one
/// poll, starving connections and cancellation, if it persists rather than
/// belonging to one peer.
const ACCEPT_RETRIES: u32 = 16;

/// How a listener responds to a failed accept.
#[derive(Debug, PartialEq, Eq)]
enum AcceptFailure {
    /// The failure belongs to one pending peer or interrupted the call; the
    /// next accept can succeed immediately.
    Retry,
    /// The process or system lacks descriptors, buffers or memory; wait before
    /// accepting again so the loop does not spin.
    Backoff,
    /// The listener itself or the scheduler failed; stop serving.
    Fatal,
}

fn accept_failure(error: &io::Error) -> AcceptFailure {
    use io::ErrorKind::*;
    match error.kind() {
        ConnectionAborted | ConnectionReset | ConnectionRefused | Interrupted | WouldBlock
        | NetworkDown | NetworkUnreachable | HostUnreachable => return AcceptFailure::Retry,
        OutOfMemory => return AcceptFailure::Backoff,
        _ => {}
    }
    match error.raw_os_error() {
        Some(code) if os::RETRY.contains(&code) => AcceptFailure::Retry,
        Some(code) if os::BACKOFF.contains(&code) => AcceptFailure::Backoff,
        _ => AcceptFailure::Fatal,
    }
}

/// Error codes which `io::ErrorKind` leaves uncategorized.
#[cfg(unix)]
mod os {
    /// Linux reports pending network errors of the new connection from
    /// `accept`, and documents that they should be retried, and reports a
    /// firewall rejection of the pending peer as `EPERM`. `EACCES`, which
    /// shares its `io::ErrorKind`, is left fatal: a security module denying
    /// every accept would otherwise loop silently.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub const RETRY: &[i32] = &[
        libc::EPERM,
        libc::EPROTO,
        libc::ENOPROTOOPT,
        libc::EHOSTDOWN,
        libc::ENONET,
    ];
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    pub const RETRY: &[i32] = &[libc::EPROTO];

    pub const BACKOFF: &[i32] = &[libc::EMFILE, libc::ENFILE, libc::ENOBUFS];
}

/// Winsock codes which `io::ErrorKind` leaves uncategorized.
#[cfg(windows)]
mod os {
    /// `WSAEINTR`.
    pub const RETRY: &[i32] = &[10004];
    /// `WSAEMFILE` and `WSAENOBUFS`.
    pub const BACKOFF: &[i32] = &[10024, 10055];
}

#[cfg(not(any(unix, windows)))]
mod os {
    pub const RETRY: &[i32] = &[];
    pub const BACKOFF: &[i32] = &[];
}

/// Accept one peer, surviving transient failures. Cancellation and scheduler
/// closure arrive as errors from the accept or the backoff wait; a failed
/// backoff wait also ends acceptance, since retrying without one could spin.
async fn accept<S: SocketAccept + Clock>(
    scheduler: &S,
    listener: &S::Socket,
    accept_errors: &mut u64,
    cancellation: &Cancellation,
) -> Result<S::Socket, OperationError> {
    let mut retries = 0;
    loop {
        let error = match scheduler.socket_accept(listener, cancellation).await {
            Ok((socket, _)) => return Ok(socket),
            Err(OperationError::Io(error)) => error,
            Err(error) => return Err(error),
        };
        let backoff = match accept_failure(&error) {
            AcceptFailure::Retry => {
                retries += 1;
                retries > ACCEPT_RETRIES
            }
            AcceptFailure::Backoff => true,
            AcceptFailure::Fatal => return Err(error.into()),
        };
        *accept_errors += 1;
        if backoff {
            retries = 0;
            scheduler.sleep(ACCEPT_BACKOFF, cancellation).await?;
        }
    }
}

/// Where a [`Server`] accepts connections, and whether it encrypts them.
pub struct Endpoint<Socket> {
    listener: Socket,
    #[cfg(feature = "rustls")]
    tls: Option<std::sync::Arc<rustls::ServerConfig>>,
}

/// The listen backlog [`Endpoint::bind`] requests.
pub const BACKLOG: i32 = 128;

impl<Socket> Endpoint<Socket> {
    /// Accept plaintext connections on an already bound, listening socket.
    pub fn new(listener: Socket) -> Self {
        Self {
            listener,
            #[cfg(feature = "rustls")]
            tls: None,
        }
    }

    /// Open a TCP socket, bind it to `address` and listen on it with a backlog
    /// of [`BACKLOG`], for plaintext connections. Bind port 0 for an OS-selected
    /// port, which [`listener`](Self::listener) reports. A socket that fails to
    /// bind or listen is closed before the error is returned.
    pub async fn bind<S>(scheduler: &S, address: SocketAddr) -> Result<Self, Error>
    where
        S: SocketOpen + SocketBind + SocketListen + SocketClose + SocketOperation<Socket = Socket>,
    {
        let never = Cancellation::never();
        let socket = scheduler
            .socket_open(address.into(), SocketType::Stream, None, &never)
            .await?;
        let listening = async {
            scheduler.socket_bind(&socket, address, &never).await?;
            scheduler.socket_listen(&socket, BACKLOG, &never).await
        }
        .await;
        if let Err(error) = listening {
            // The bind or listen failure explains a consequent close failure.
            let _ = scheduler.socket_close(socket).await;
            return Err(error.into());
        }
        Ok(Self::new(socket))
    }

    /// The listening socket, for example to read the address it is bound to.
    pub fn listener(&self) -> &Socket {
        &self.listener
    }

    /// Encrypt every accepted connection with `config`. The server sets its
    /// ALPN protocols from [`Options::protocols`], so the handshake selects an
    /// enabled engine; handshakes occupy connection slots and progress
    /// concurrently with acceptance.
    #[cfg(feature = "rustls")]
    pub fn with_tls(mut self, config: std::sync::Arc<rustls::ServerConfig>) -> Self {
        self.tls = Some(config);
        self
    }
}

/// An HTTP server: a handler, the endpoint it serves and its options, run until
/// cancelled.
///
/// [`run`](Self::run) takes where connections run: the scheduler, `&scheduler`,
/// supervises them within the returned future, interleaving on whichever worker
/// polls it; a scope of the scheduler's scoped spawn gives each connection its
/// own task, so connections progress on more than one worker. Spawned
/// connections borrow the handler and options, so create them before entering
/// the scope:
///
/// ```rust,ignore
/// let handler = Application::new();
/// let options = Options::default();
/// scheduler.scope(async |scope| {
///     let server = Server::new(&handler, Endpoint::new(listener), &options);
///     server.run(scope, &cancellation).await
/// });
/// ```
pub struct Server<'h, H, Socket> {
    handler: &'h H,
    endpoint: Endpoint<Socket>,
    options: &'h Options,
}

impl<'h, H, Socket> Server<'h, H, Socket> {
    pub fn new(handler: &'h H, endpoint: Endpoint<Socket>, options: &'h Options) -> Self {
        Self {
            handler,
            endpoint,
            options,
        }
    }

    /// Serve the endpoint until cancelled, with bounded concurrent connections.
    /// Cancellation stops acceptance, closes the owned listener, then cancels
    /// active exchanges and joins every connection's retirement, including
    /// spawned ones. Individual peer and handler failures are counted without
    /// stopping other peers; a spawned connection that panics is counted as a
    /// failed connection.
    ///
    /// Transient accept failures do not stop the listener: those belonging to one
    /// pending peer, such as `ECONNABORTED`, are retried immediately, and resource
    /// exhaustion, such as `EMFILE`, pauses acceptance briefly on the scheduler's
    /// clock while existing connections continue. Repeated per-peer failures also
    /// pause after a few immediate retries, so a persistent one cannot spin. Both
    /// are counted in [`Statistics::accept_errors`]. Any other accept failure,
    /// including scheduler closure or a failed backoff wait, is returned after the
    /// same shutdown sequence, in preference to a listener close failure, as is a
    /// failure to spawn a connection.
    pub fn run<'p, P, S>(
        self,
        placement: P,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<Statistics, Error>> + Send
    where
        'h: 'p,
        P: Placement<'p, Scheduler = S>,
        S: SocketAccept<Socket = Socket>
            + Clock
            + SocketSetOption<TcpNoDelay>
            + SocketStreamReading
            + SocketStreamWriting
            + SocketClose
            + SocketPeerAddress
            + SocketLocalAddress
            + Sync
            + 'p,
        Socket: Send + Sync + 'static,
        H: Handler<'p, ServerContext> + Sync,
    {
        // Stating `Send` on the returned type proves it here, for every request
        // body lifetime, rather than in each caller's future.
        run(self, placement, cancellation)
    }
}

async fn run<'h, 'p, P, S, H, Socket>(
    server: Server<'h, H, Socket>,
    placement: P,
    cancellation: &Cancellation,
) -> Result<Statistics, Error>
where
    'h: 'p,
    P: Placement<'p, Scheduler = S>,
    S: SocketAccept<Socket = Socket>
        + Clock
        + SocketSetOption<TcpNoDelay>
        + SocketStreamReading
        + SocketStreamWriting
        + SocketClose
        + SocketPeerAddress
        + SocketLocalAddress
        + Sync
        + 'p,
    Socket: Send + Sync + 'static,
    H: Handler<'p, ServerContext> + Sync,
{
    let Server {
        handler,
        endpoint,
        options,
    } = server;
    let scheduler = placement.scheduler();
    // The endpoint's choice is made once, entering fully typed connection code.
    #[cfg(feature = "rustls")]
    if let Some(config) = endpoint.tls {
        use base_endpoint::tls::{TlsAcceptor, rustls::RustlsAcceptor};
        let mut config = (*config).clone();
        config.alpn_protocols = options.protocols.alpn_protocols();
        // Shared by every connection, which a spawned connection cannot borrow from this future.
        let acceptor = Arc::new(RustlsAcceptor::new(Arc::new(config)));
        return serve_with(
            placement,
            endpoint.listener,
            options,
            cancellation,
            move |socket, cancellation| {
                let acceptor = Arc::clone(&acceptor);
                async move {
                    nodelay(scheduler, &socket, options, &cancellation).await;
                    match acceptor.accept(scheduler, socket, &cancellation).await {
                        Ok(connection) => {
                            tls_connection_outcome(
                                scheduler,
                                connection,
                                handler,
                                options,
                                &cancellation,
                            )
                            .await
                        }
                        Err(error) => Outcome {
                            requests: 0,
                            result: Err(error.into()),
                        },
                    }
                }
            },
        )
        .await;
    }
    serve_with(
        placement,
        endpoint.listener,
        options,
        cancellation,
        move |socket, cancellation| async move {
            nodelay(scheduler, &socket, options, &cancellation).await;
            connection_outcome(scheduler, socket, handler, options, &cancellation).await
        },
    )
    .await
}

// A latency hint only: the connection is served without it.
async fn nodelay<S: SocketSetOption<TcpNoDelay>>(
    scheduler: &S,
    socket: &S::Socket,
    options: &Options,
    cancellation: &Cancellation,
) {
    if options.tcp_nodelay {
        let _ = scheduler
            .socket_set_option(socket, TcpNoDelay, true, cancellation)
            .await;
    }
}

async fn serve_with<'p, P, S, F, Fut>(
    placement: P,
    listener: S::Socket,
    options: &Options,
    cancellation: &Cancellation,
    connection: F,
) -> Result<Statistics, Error>
where
    P: Placement<'p, Scheduler = S>,
    S: SocketAccept + Clock + SocketClose + 'p,
    F: Fn(S::Socket, Cancellation) -> Fut,
    Fut: Future<Output = Outcome> + Send + 'p,
{
    let scheduler = placement.scheduler();
    let connections_cancel = cancellation.child();
    let mut connections = FuturesUnordered::new();
    let mut statistics = Statistics::default();
    // Counted apart from `statistics`, which completions update while an accept is pending.
    let mut accept_errors = 0;
    let result = async {
        options.http1.validate()?;
        if options.max_connections == 0 {
            return Err(base_http1::Error::Invalid("zero connection limit").into());
        }
        loop {
            if connections.len() == options.max_connections {
                record(
                    &mut statistics,
                    cancellable(connections.next(), cancellation)
                        .await?
                        .unwrap(),
                );
                continue;
            }
            // Keep this accept alive while existing connections complete. A
            // backend may already own an accepted socket between polls; dropping
            // and recreating acceptance can silently discard that new peer.
            let accepted = {
                let mut accept = pin!(accept(
                    scheduler,
                    &listener,
                    &mut accept_errors,
                    cancellation
                ));
                loop {
                    let completed = pin!(async {
                        if connections.is_empty() {
                            std::future::pending().await
                        } else {
                            connections.next().await.unwrap()
                        }
                    });
                    match select(accept.as_mut(), completed).await {
                        Either::Left((result, _)) => break result,
                        Either::Right((result, _)) => record(&mut statistics, result),
                    }
                }
            };
            let socket = accepted.map_err(base_http1::Error::from)?;
            statistics.connections += 1;
            // A connection owns everything it uses but what outlives the placement, so a scope can run it as a task.
            connections.push(placement.place(connection(socket, connections_cancel.clone()))?);
        }
    }
    .await;
    statistics.accept_errors = accept_errors;
    // Close the listener first, so new peers are refused rather than left
    // queued in a backlog which nothing accepts while connections retire.
    let closed = scheduler.socket_close(listener).await;
    connections_cancel.cancel();
    while let Some(result) = connections.next().await {
        record(&mut statistics, result);
    }
    // The failure which ended serving explains a consequent close failure.
    match result {
        Ok(()) | Err(Error::Cancelled) => {
            closed?;
            Ok(statistics)
        }
        Err(error) => Err(error),
    }
}

// A placed connection's outcome, or the panic or destruction of its task.
fn record(statistics: &mut Statistics, placed: Result<Outcome, TaskError>) {
    let outcome = placed.unwrap_or_else(|error| Outcome {
        requests: 0,
        result: Err(match error {
            TaskError::Cancelled => Error::Cancelled,
            TaskError::Panicked(_) => Error::Panicked,
        }),
    });
    statistics.requests += outcome.requests;
    match outcome.result {
        Ok(()) => {}
        Err(Error::Cancelled) => {}
        Err(_) => statistics.failed_connections += 1,
    }
}

/// Serve HTTP/1 over an already authenticated TLS connection. ALPN is
/// interpreted by HTTP before any request is admitted.
#[cfg(feature = "rustls")]
pub fn serve_tls_connection<'scope, S, H>(
    scheduler: &'scope S,
    connection: base_endpoint::tls::rustls::RustlsConnection<S::Socket>,
    handler: &H,
    options: &Options,
    cancellation: &Cancellation,
) -> impl Future<Output = Result<u64, Error>> + Send
where
    S: SocketStreamReading
        + SocketStreamWriting
        + SocketClose
        + SocketPeerAddress
        + SocketLocalAddress,
    S::Socket: 'static,
    H: Handler<'scope, ServerContext>,
    S: Sync,
    S::Socket: Send,
{
    // Stating `Send` on the returned type proves it here, for every request
    // body lifetime, rather than in each caller's future.
    serve_tls_connection_inner(scheduler, connection, handler, options, cancellation)
}

#[cfg(feature = "rustls")]
async fn serve_tls_connection_inner<'scope, S, H>(
    scheduler: &'scope S,
    connection: base_endpoint::tls::rustls::RustlsConnection<S::Socket>,
    handler: &H,
    options: &Options,
    cancellation: &Cancellation,
) -> Result<u64, Error>
where
    S: SocketStreamReading
        + SocketStreamWriting
        + SocketClose
        + SocketPeerAddress
        + SocketLocalAddress,
    S::Socket: 'static,
    H: Handler<'scope, ServerContext>,
{
    let outcome =
        tls_connection_outcome(scheduler, connection, handler, options, cancellation).await;
    outcome.result.map(|()| outcome.requests)
}

#[cfg(feature = "rustls")]
async fn tls_connection_outcome<'scope, S, H>(
    scheduler: &'scope S,
    connection: base_endpoint::tls::rustls::RustlsConnection<S::Socket>,
    handler: &H,
    options: &Options,
    cancellation: &Cancellation,
) -> Outcome
where
    S: SocketStreamReading
        + SocketStreamWriting
        + SocketClose
        + SocketPeerAddress
        + SocketLocalAddress,
    S::Socket: 'static,
    H: Handler<'scope, ServerContext>,
{
    use base_endpoint::tls::TlsSession;
    let selected = connection.negotiated_protocol().map(<[u8]>::to_vec);
    let socket = connection.socket();
    let peer = Arc::new(
        Peer::new(
            scheduler.socket_peer_address(socket).ok(),
            scheduler.socket_local_address(socket).ok(),
        )
        .encrypted(selected.as_deref()),
    );
    let mut requests = 0;
    let result = connection
        .with_duplex(scheduler, cancellation, |reader, writer| async {
            options.http1.validate()?;
            let protocol = options.protocols.negotiated(selected.as_deref())?;
            ServerConnection::new(protocol, reader, writer, options)
                .serve(handler, &peer, cancellation, &mut requests, &Scheme::HTTPS)
                .await
        })
        .await;
    Outcome { requests, result }
}

impl From<base_http_protocol::NegotiationError> for Error {
    fn from(error: base_http_protocol::NegotiationError) -> Self {
        Self::Negotiation(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{error::Error as _, io};

    #[test]
    fn protocol_errors_are_transparent_and_io_errors_have_context() {
        let error = Error::from(base_http1::Error::Io(io::Error::from(
            io::ErrorKind::ConnectionReset,
        )));
        assert_eq!(error.to_string(), "HTTP/1 connection I/O failed");
        assert_eq!(error.source().unwrap().to_string(), "connection reset");

        let error = Error::from(io::Error::from(io::ErrorKind::ConnectionReset));
        assert_eq!(error.to_string(), "HTTP server I/O failed");
        assert_eq!(error.source().unwrap().to_string(), "connection reset");
        // A panicked connection has no source error to report; a failed spawn reports the scheduler's.
        assert_eq!(Error::Panicked.to_string(), "HTTP connection panicked");
        assert!(Error::Panicked.source().is_none());
    }

    #[test]
    fn handler_errors_are_reported_as_the_source() {
        let error = Error::Handler(Box::new(io::Error::other("database unavailable")));
        assert_eq!(error.to_string(), "request handler failed");
        assert_eq!(error.source().unwrap().to_string(), "database unavailable");
    }

    #[test]
    fn invalid_responses_report_the_refusal_as_the_source() {
        let error = Error::Response(base_http1::Error::Invalid("final response required").into());
        assert_eq!(
            error.to_string(),
            "request handler returned an invalid response"
        );
        assert_eq!(
            error.source().unwrap().to_string(),
            "invalid HTTP/1 message: final response required"
        );
    }

    #[test]
    fn scheduler_operation_errors_keep_cancellation_distinct() {
        let error = Error::from(base_scheduler::OperationError::from(
            base_scheduler::Cancelled,
        ));
        assert_eq!(format!("{error:?}"), "Cancelled");
        assert_eq!(error.to_string(), "HTTP server cancelled");
        assert!(error.source().is_none());

        let error = Error::from(base_scheduler::OperationError::from(io::Error::from(
            io::ErrorKind::ConnectionReset,
        )));
        let expected = Error::Protocol(
            base_http1::Error::Io(io::Error::from(io::ErrorKind::ConnectionReset)).into(),
        );
        assert_eq!(format!("{error:?}"), format!("{expected:?}"));
        assert_eq!(error.source().unwrap().to_string(), "connection reset");
    }

    #[test]
    fn accept_failures_are_classified_by_their_effect_on_the_listener() {
        for kind in [
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::Interrupted,
            io::ErrorKind::WouldBlock,
            io::ErrorKind::NetworkDown,
            io::ErrorKind::NetworkUnreachable,
            io::ErrorKind::HostUnreachable,
        ] {
            assert_eq!(
                accept_failure(&kind.into()),
                AcceptFailure::Retry,
                "{kind:?}"
            );
        }
        assert_eq!(
            accept_failure(&io::ErrorKind::OutOfMemory.into()),
            AcceptFailure::Backoff
        );
        for kind in [
            io::ErrorKind::InvalidInput,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::Other,
        ] {
            assert_eq!(
                accept_failure(&kind.into()),
                AcceptFailure::Fatal,
                "{kind:?}"
            );
        }
        for &code in os::RETRY {
            let error = io::Error::from_raw_os_error(code);
            assert_eq!(accept_failure(&error), AcceptFailure::Retry, "{error}");
        }
        for &code in os::BACKOFF {
            let error = io::Error::from_raw_os_error(code);
            assert_eq!(accept_failure(&error), AcceptFailure::Backoff, "{error}");
        }
        #[cfg(unix)]
        for code in [
            libc::EBADF,
            libc::EINVAL,
            libc::ENOTSOCK,
            libc::EOPNOTSUPP,
            libc::EACCES,
        ] {
            let error = io::Error::from_raw_os_error(code);
            assert_eq!(accept_failure(&error), AcceptFailure::Fatal, "{error}");
        }
    }

    #[test]
    fn negotiation_errors_are_transparent() {
        let error = Error::from(base_http_protocol::NegotiationError::Missing);
        assert_eq!(format!("{error:?}"), "Negotiation(Missing)");
        assert_eq!(
            error.to_string(),
            "HTTP application protocol was not negotiated"
        );
        assert!(error.source().is_none());
    }
}

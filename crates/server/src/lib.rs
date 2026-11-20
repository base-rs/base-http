// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Application handlers and supervised HTTP/1 connections over Base scheduler
//! capabilities. All connection futures are owned and joined by the server.
#![doc = include_str!("../readme.md")]
#![forbid(unsafe_code)]

use base_endpoint::stream::{ReadableStream, WritableStream};
use base_http::{
    InterimHandler, InterimResponses, Request, Response, Scheme, Status, body::ReadableBody,
};
use base_http1::{LentInput, RequestReader, ResponseWriter};
use base_scheduler::{
    Cancellation, Clock, OperationError, SocketAccept, SocketClose, SocketSetOption,
    SocketStreamReading, SocketStreamWriting, TcpNoDelay,
};
use futures_util::{
    future::{Either, select},
    stream::{FuturesUnordered, StreamExt},
};
use std::{
    fmt,
    future::{Future, poll_fn},
    io,
    pin::pin,
    task::Poll,
    time::Duration,
};

/// Generic bodies and hooks keep handlers independent of the HTTP wire version
/// and scheduler. A returned body may retain the request body.
///
/// The request body is an independent value that lives for `'scope`, the
/// server's borrow of its scheduler, so a handler can pass it to work that
/// outlives the call, such as a client forwarding it upstream. Its connection
/// reads the next request only once the body has ended, or has been closed or
/// dropped; when the connection is cancelled or fails, it takes its input back
/// at once and the body's later reads fail with
/// [`base_http1::Error::Closed`].
///
/// Generic code awaiting a handler that is bound to one `'scope` inside its own
/// `Send` future may be rejected by rustc with "implementation of `Handler` is
/// not general enough"; awaiting [`call_send`] instead avoids it. The server's
/// entry points need no such care.
///
/// An error responds with a bodiless status and closes the connection, which then reports it as [`Error::Handler`]: 400 when a request read failed, such as a malformed or truncated body, even if the error has another cause, and otherwise 500. Return an ordinary response for other client errors. A final response which HTTP/1 cannot send, such as an informational status, conflicting framing, a `Content-Length` field its body contradicts or an unsolicited upgrade, likewise receives a bodiless 500 and closes the connection, which reports it as [`Error::Response`].
pub trait Handler<'scope>: Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    type Body<B: ReadableBody + 'scope>: ReadableBody;

    fn call<B: ReadableBody + 'scope, I: InterimHandler>(
        &self,
        request: Request<B>,
        interim: &mut InterimResponses<I>,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<Response<Self::Body<B>>, Self::Error>> + Send;
}

/// Call `handler` through a future whose type states `Send`, for generic code
/// that awaits a handler bound to one `'scope` within its own `Send` future,
/// which rustc may otherwise reject as "not general enough".
pub fn call_send<'scope, H, B, I>(
    handler: &H,
    request: Request<B>,
    interim: &mut InterimResponses<I>,
    cancellation: &Cancellation,
) -> impl Future<Output = Result<Response<H::Body<B>>, H::Error>> + Send
where
    H: Handler<'scope>,
    B: ReadableBody + 'scope,
    I: InterimHandler,
{
    handler.call(request, interim, cancellation)
}

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
    Protocol(base_http1::Error),
    Handler(Box<dyn std::error::Error + Send + Sync>),
    /// A handler returned a final response which the protocol writer refused before writing any of it, such as an informational status or conflicting framing.
    Response(base_http1::Error),
    Io(std::io::Error),
    Cancelled,
    Negotiation(base_http_protocol::NegotiationError),
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
        }
    }
}
impl From<base_http1::Error> for Error {
    fn from(e: base_http1::Error) -> Self {
        match e {
            base_http1::Error::Cancelled => Self::Cancelled,
            e => Self::Protocol(e),
        }
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

async fn cancellable<F: Future>(
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
/// uncancellable lane retirement, and finally exclusive socket closure. Await
/// completion after cooperative cancellation; dropping this future is not a join.
pub fn serve_connection<'scope, S, H>(
    scheduler: &'scope S,
    socket: S::Socket,
    handler: &H,
    options: &base_http1::Options,
    cancellation: &Cancellation,
) -> impl Future<Output = Result<u64, Error>> + Send
where
    S: SocketStreamReading + SocketStreamWriting + SocketClose,
    S::Socket: 'static,
    H: Handler<'scope>,
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
    options: &base_http1::Options,
    cancellation: &Cancellation,
) -> Result<u64, Error>
where
    S: SocketStreamReading + SocketStreamWriting + SocketClose,
    S::Socket: 'static,
    H: Handler<'scope>,
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
    options: &base_http1::Options,
    cancellation: &Cancellation,
) -> Outcome
where
    S: SocketStreamReading + SocketStreamWriting + SocketClose,
    S::Socket: 'static,
    H: Handler<'scope>,
{
    let mut requests = 0;
    // The outer owner closes the socket even when capability construction fails.
    let result = async {
        options.validate()?;
        let mut reader = scheduler.socket_stream_reader(&socket)?;
        let writer = match scheduler.socket_stream_writer(&socket) {
            Ok(writer) => writer,
            Err(error) => {
                // Report the construction failure in preference to a retirement failure.
                let _ = ReadableStream::close(&mut reader).await;
                return Err(Error::Io(error));
            }
        };
        // The plaintext engine is HTTP/1, whose codecs fail to construct only when `options.validate` above fails.
        let (input, output) = base_http_protocol::HttpProtocols::default()
            .plaintext()
            .server(reader, writer, options.clone())
            .expect("validated HTTP/1 options construct server codecs");
        serve_codecs(
            input,
            output,
            handler,
            cancellation,
            &mut requests,
            &Scheme::HTTP,
        )
        .await
    }
    .await;
    let closed = scheduler.socket_close(socket).await;
    Outcome {
        requests,
        result: result.and_then(|()| Ok(closed?)),
    }
}

/// Serve exchanges over established codecs, then retire both directions.
async fn serve_codecs<'scope, R, W, H>(
    input: RequestReader<R>,
    mut output: ResponseWriter<W>,
    handler: &H,
    cancellation: &Cancellation,
    count: &mut u64,
    scheme: &Scheme,
) -> Result<(), Error>
where
    R: ReadableStream + Send + 'scope,
    W: WritableStream,
    H: Handler<'scope>,
{
    let mut input = LentInput::new(input);
    let result = exchanges(
        &mut input,
        &mut output,
        handler,
        cancellation,
        count,
        scheme,
    )
    .await;
    // After a normal end, wait for the last request body before retiring input,
    // so work still reading it, such as a forwarded upload, can finish.
    // Cancellation, or any failure, takes the input back from the body at once.
    if result.is_ok() {
        let _ = input.returned(cancellation).await;
    }
    // Both cleanups run regardless of the exchange or the other cleanup result.
    let input_closed = input.close().await;
    let output_closed = output.close().await;
    result?;
    input_closed?;
    output_closed?;
    Ok(())
}

async fn exchanges<'scope, R, W, H>(
    input: &mut LentInput<R>,
    output: &mut ResponseWriter<W>,
    handler: &H,
    cancellation: &Cancellation,
    count: &mut u64,
    scheme: &Scheme,
) -> Result<(), Error>
where
    R: ReadableStream + Send + 'scope,
    W: WritableStream,
    H: Handler<'scope>,
{
    loop {
        let (mut request, exchange) = match input.read_request(cancellation).await {
            Ok(Some(pair)) => pair,
            Ok(None) => return Ok(()),
            Err(error) => {
                if !matches!(error, base_http1::Error::Cancelled) {
                    let status = if matches!(error, base_http1::Error::Limit(_)) {
                        Status::new(431).unwrap()
                    } else {
                        Status::BAD_REQUEST
                    };
                    // A bad request cannot be reused even if rejection succeeds.
                    let _ = output.reject(status, cancellation).await;
                }
                return Err(error.into());
            }
        };
        request.scheme.get_or_insert_with(|| scheme.clone());
        let result = {
            let mut interim = InterimResponses::new(output.interim(exchange.version()));
            let result = {
                let called = pin!(handler.call(request, &mut interim, cancellation));
                cancellable(called, cancellation).await?
            };
            interim.finish();
            result
        };
        // The response can borrow the input lane, which a failure inspects, so only a successful result leaves this block.
        let mut response = 'handled: {
            let error = match result.map_err(|error| Error::Handler(Box::new(error))) {
                Ok(response) => break 'handled response,
                Err(error) => error,
            };
            // Cancellation can arrive between polling the outer signal and polling a handler which propagates a cancelled body operation.
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            // No final head has been written, so the peer can still learn of the failure: failed request input means the request was bad, and anything else is a server error. The input position may be unknown and unread input may remain, so the connection always closes afterwards; output already out of sync refuses the response and just closes. The input is taken back from a request body that outlived the handler, whose later reads fail.
            input.reclaim().await;
            let status = if input.input().is_some_and(RequestReader::is_poisoned) {
                Status::BAD_REQUEST
            } else {
                Status::INTERNAL_SERVER_ERROR
            };
            let _ = output.reject(status, cancellation).await;
            return Err(error);
        };
        let written = output
            .write_response(&exchange, &mut response, cancellation)
            .await;
        drop(response);
        let persistent = match written {
            Ok(persistent) => persistent,
            // Usable output means the writer refused the response before writing any of it, so the peer can still learn of the failure; the request may be unfinished, so the connection closes afterwards.
            Err(error) if output.is_usable() && !matches!(error, base_http1::Error::Cancelled) => {
                let _ = output
                    .reject(Status::INTERNAL_SERVER_ERROR, cancellation)
                    .await;
                return Err(Error::Response(error));
            }
            Err(error) => return Err(error.into()),
        };
        *count += 1;
        if !persistent {
            return Ok(());
        }
        // The request body may outlive its handler, such as one a client is
        // still forwarding; the next request waits until it ends or is dropped.
        input.returned(cancellation).await?;
        if !input.is_reusable() {
            return Ok(());
        }
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

/// Serve an already-bound listener with bounded concurrent connection futures.
/// No hidden tasks or detached jobs are created. Cancellation stops acceptance,
/// closes the owned listener, then cancels active exchanges and joins their
/// retirement. Individual peer/handler failures are counted without stopping
/// other peers.
///
/// Transient accept failures do not stop the listener: those belonging to one
/// pending peer, such as `ECONNABORTED`, are retried immediately, and resource
/// exhaustion, such as `EMFILE`, pauses acceptance briefly on the scheduler's
/// clock while existing connections continue. Repeated per-peer failures also
/// pause after a few immediate retries, so a persistent one cannot spin. Both
/// are counted in [`Statistics::accept_errors`]. Any other accept failure,
/// including scheduler closure or a failed backoff wait, is returned after the
/// same shutdown sequence, in preference to a listener close failure.
pub fn serve<'scope, S, H>(
    scheduler: &'scope S,
    listener: S::Socket,
    handler: &H,
    options: &Options,
    cancellation: &Cancellation,
) -> impl Future<Output = Result<Statistics, Error>> + Send
where
    S: SocketAccept
        + Clock
        + SocketSetOption<TcpNoDelay>
        + SocketStreamReading
        + SocketStreamWriting
        + SocketClose,
    S::Socket: 'static,
    H: Handler<'scope>,
    S: Sync,
    S::Socket: Send,
{
    // Stating `Send` on the returned type proves it here, for every request
    // body lifetime, rather than in each caller's future.
    serve_inner(scheduler, listener, handler, options, cancellation)
}

async fn serve_inner<'scope, S, H>(
    scheduler: &'scope S,
    listener: S::Socket,
    handler: &H,
    options: &Options,
    cancellation: &Cancellation,
) -> Result<Statistics, Error>
where
    S: SocketAccept
        + Clock
        + SocketSetOption<TcpNoDelay>
        + SocketStreamReading
        + SocketStreamWriting
        + SocketClose,
    S::Socket: 'static,
    H: Handler<'scope>,
{
    serve_with(
        scheduler,
        listener,
        options,
        cancellation,
        |socket, cancellation| async move {
            connection_outcome(scheduler, socket, handler, &options.http1, &cancellation).await
        },
    )
    .await
}

async fn serve_with<S, F, Fut>(
    scheduler: &S,
    listener: S::Socket,
    options: &Options,
    cancellation: &Cancellation,
    connection: F,
) -> Result<Statistics, Error>
where
    S: SocketAccept + Clock + SocketSetOption<TcpNoDelay> + SocketClose,
    F: Fn(S::Socket, Cancellation) -> Fut,
    Fut: Future<Output = Outcome>,
{
    let connections_cancel = cancellation.child();
    let mut connections = FuturesUnordered::new();
    let mut statistics = Statistics::default();
    // Counted apart from `statistics`, which completions update while an accept is pending.
    let mut accept_errors = 0;
    let result = async {
        options.http1.validate()?;
        if options.max_connections == 0 {
            return Err(Error::Protocol(base_http1::Error::Invalid(
                "zero connection limit",
            )));
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
            let cancellation = connections_cancel.clone();
            connections.push(async {
                if options.tcp_nodelay {
                    // A latency hint only: the connection is served without it.
                    let _ = scheduler
                        .socket_set_option(&socket, TcpNoDelay, true, &cancellation)
                        .await;
                }
                connection(socket, cancellation).await
            });
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

fn record(statistics: &mut Statistics, outcome: Outcome) {
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
    S: SocketStreamReading + SocketStreamWriting + SocketClose,
    S::Socket: 'static,
    H: Handler<'scope>,
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
    S: SocketStreamReading + SocketStreamWriting + SocketClose,
    S::Socket: 'static,
    H: Handler<'scope>,
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
    S: SocketStreamReading + SocketStreamWriting + SocketClose,
    S::Socket: 'static,
    H: Handler<'scope>,
{
    use base_endpoint::tls::TlsSession;
    let selected = connection.negotiated_protocol().map(<[u8]>::to_vec);
    let mut requests = 0;
    let result = connection
        .with_duplex(scheduler, cancellation, |reader, writer| async {
            let protocol = options.protocols.negotiated(selected.as_deref())?;
            let (input, output) = protocol.server(reader, writer, options.http1.clone())?;
            serve_codecs(
                input,
                output,
                handler,
                cancellation,
                &mut requests,
                &Scheme::HTTPS,
            )
            .await
        })
        .await;
    Outcome { requests, result }
}

/// Configure HTTP/1 ALPN and serve TLS on an owned TCP listener. Handshakes
/// occupy bounded connection slots and progress concurrently with acceptance.
#[cfg(feature = "rustls")]
pub fn serve_tls<'scope, S, H>(
    scheduler: &'scope S,
    listener: S::Socket,
    config: std::sync::Arc<rustls::ServerConfig>,
    handler: &H,
    options: &Options,
    cancellation: &Cancellation,
) -> impl Future<Output = Result<Statistics, Error>> + Send
where
    S: SocketAccept
        + Clock
        + SocketSetOption<TcpNoDelay>
        + SocketStreamReading
        + SocketStreamWriting
        + SocketClose,
    S::Socket: 'static,
    H: Handler<'scope>,
    S: Sync,
    S::Socket: Send,
{
    // Stating `Send` on the returned type proves it here, for every request
    // body lifetime, rather than in each caller's future.
    serve_tls_inner(scheduler, listener, config, handler, options, cancellation)
}

#[cfg(feature = "rustls")]
async fn serve_tls_inner<'scope, S, H>(
    scheduler: &'scope S,
    listener: S::Socket,
    config: std::sync::Arc<rustls::ServerConfig>,
    handler: &H,
    options: &Options,
    cancellation: &Cancellation,
) -> Result<Statistics, Error>
where
    S: SocketAccept
        + Clock
        + SocketSetOption<TcpNoDelay>
        + SocketStreamReading
        + SocketStreamWriting
        + SocketClose,
    S::Socket: 'static,
    H: Handler<'scope>,
{
    use base_endpoint::tls::{TlsAcceptor, rustls::RustlsAcceptor};
    let mut config = (*config).clone();
    config.alpn_protocols = options.protocols.alpn_protocols();
    let acceptor = RustlsAcceptor::new(std::sync::Arc::new(config));
    serve_with(
        scheduler,
        listener,
        options,
        cancellation,
        |socket, cancellation| {
            let acceptor = &acceptor;
            async move {
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
    .await
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
    }

    #[test]
    fn handler_errors_are_reported_as_the_source() {
        let error = Error::Handler(Box::new(io::Error::other("database unavailable")));
        assert_eq!(error.to_string(), "request handler failed");
        assert_eq!(error.source().unwrap().to_string(), "database unavailable");
    }

    #[test]
    fn invalid_responses_report_the_refusal_as_the_source() {
        let error = Error::Response(base_http1::Error::Invalid("final response required"));
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
        let expected = Error::Protocol(base_http1::Error::Io(io::Error::from(
            io::ErrorKind::ConnectionReset,
        )));
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

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
use base_http1::{RequestReader, ResponseWriter};
use base_scheduler::{
    Cancellation, SocketAccept, SocketClose, SocketStreamReading, SocketStreamWriting,
};
use futures_util::{
    future::{Either, select},
    stream::{FuturesUnordered, StreamExt},
};
use std::{
    fmt,
    future::{Future, poll_fn},
    pin::pin,
    task::Poll,
};

/// Generic bodies and hooks keep handlers independent of the HTTP wire version
/// and scheduler. A returned body may retain the request body, including borrows.
pub trait Handler: Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    type Body<B: ReadableBody>: ReadableBody;

    fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        request: Request<B>,
        interim: &mut InterimResponses<I>,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<Response<Self::Body<B>>, Self::Error>> + Send;
}

#[derive(Clone, Debug)]
pub struct Options {
    pub http1: base_http1::Options,
    /// HTTP engine selection, shared by plain and encrypted establishment.
    pub protocols: base_http_protocol::HttpProtocols,
    pub max_connections: usize,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            http1: base_http1::Options::default(),
            protocols: base_http_protocol::HttpProtocols::default(),
            max_connections: 256,
        }
    }
}

/// A server failure. Protocol and negotiation errors are transparent; other wrapping variants describe this layer and report their cause as the source, so a cause chain never repeats a message.
#[derive(Debug)]
pub enum Error {
    Protocol(base_http1::Error),
    Handler(Box<dyn std::error::Error + Send + Sync>),
    Io(std::io::Error),
    Cancelled,
    Negotiation(base_http_protocol::NegotiationError),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(e) => e.fmt(f),
            Self::Handler(_) => f.write_str("request handler failed"),
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
pub async fn serve_connection<S, H>(
    scheduler: &S,
    socket: S::Socket,
    handler: &H,
    options: &base_http1::Options,
    cancellation: &Cancellation,
) -> Result<u64, Error>
where
    S: SocketStreamReading + SocketStreamWriting + SocketClose,
    S::Socket: 'static,
    H: Handler,
{
    let outcome = connection_outcome(scheduler, socket, handler, options, cancellation).await;
    outcome.result.map(|()| outcome.requests)
}

struct Outcome {
    requests: u64,
    result: Result<(), Error>,
}

async fn connection_outcome<S, H>(
    scheduler: &S,
    socket: S::Socket,
    handler: &H,
    options: &base_http1::Options,
    cancellation: &Cancellation,
) -> Outcome
where
    S: SocketStreamReading + SocketStreamWriting + SocketClose,
    S::Socket: 'static,
    H: Handler,
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
async fn serve_codecs<R, W, H>(
    mut input: RequestReader<R>,
    mut output: ResponseWriter<W>,
    handler: &H,
    cancellation: &Cancellation,
    count: &mut u64,
    scheme: &Scheme,
) -> Result<(), Error>
where
    R: ReadableStream,
    W: WritableStream,
    H: Handler,
{
    let result = exchanges(
        &mut input,
        &mut output,
        handler,
        cancellation,
        count,
        scheme,
    )
    .await;
    // Both cleanups run regardless of the exchange or the other cleanup result.
    let input_closed = input.close().await;
    let output_closed = output.close().await;
    result?;
    input_closed?;
    output_closed?;
    Ok(())
}

async fn exchanges<R, W, H>(
    input: &mut RequestReader<R>,
    output: &mut ResponseWriter<W>,
    handler: &H,
    cancellation: &Cancellation,
    count: &mut u64,
    scheme: &Scheme,
) -> Result<(), Error>
where
    R: ReadableStream,
    W: WritableStream,
    H: Handler,
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
        let mut response = result.map_err(|error| {
            // Cancellation can arrive between polling the outer signal and
            // polling a handler which propagates a cancelled body operation.
            if cancellation.is_cancelled() {
                Error::Cancelled
            } else {
                Error::Handler(Box::new(error))
            }
        })?;
        let persistent = output
            .write_response(&exchange, &mut response, cancellation)
            .await?;
        drop(response);
        *count += 1;
        if !persistent || !input.is_reusable() {
            return Ok(());
        }
    }
}

#[derive(Default, Debug)]
pub struct Statistics {
    pub connections: u64,
    pub requests: u64,
    pub failed_connections: u64,
}

/// Serve an already-bound listener with bounded concurrent connection futures.
/// No hidden tasks or detached jobs are created. Cancellation stops acceptance,
/// cancels active exchanges, joins retirement, and closes the owned listener.
/// Individual peer/handler failures are counted without stopping other peers.
pub async fn serve<S, H>(
    scheduler: &S,
    listener: S::Socket,
    handler: &H,
    options: &Options,
    cancellation: &Cancellation,
) -> Result<Statistics, Error>
where
    S: SocketAccept + SocketStreamReading + SocketStreamWriting + SocketClose,
    S::Socket: 'static,
    H: Handler,
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
    S: SocketAccept + SocketClose,
    F: Fn(S::Socket, Cancellation) -> Fut,
    Fut: Future<Output = Outcome>,
{
    let connections_cancel = cancellation.child();
    let mut connections = FuturesUnordered::new();
    let mut statistics = Statistics::default();
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
                let mut accept = pin!(scheduler.socket_accept(&listener, cancellation));
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
            let (socket, _) = accepted.map_err(base_http1::Error::from)?;
            statistics.connections += 1;
            connections.push(connection(socket, connections_cancel.clone()));
        }
    }
    .await;
    connections_cancel.cancel();
    while let Some(result) = connections.next().await {
        record(&mut statistics, result);
    }
    let closed = scheduler.socket_close(listener).await;
    closed?;
    match result {
        Ok(()) | Err(Error::Cancelled) => Ok(statistics),
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
pub async fn serve_tls_connection<S, H>(
    scheduler: &S,
    connection: base_endpoint::tls::rustls::RustlsConnection<S::Socket>,
    handler: &H,
    options: &Options,
    cancellation: &Cancellation,
) -> Result<u64, Error>
where
    S: SocketStreamReading + SocketStreamWriting + SocketClose,
    S::Socket: 'static,
    H: Handler,
{
    let outcome =
        tls_connection_outcome(scheduler, connection, handler, options, cancellation).await;
    outcome.result.map(|()| outcome.requests)
}

#[cfg(feature = "rustls")]
async fn tls_connection_outcome<S, H>(
    scheduler: &S,
    connection: base_endpoint::tls::rustls::RustlsConnection<S::Socket>,
    handler: &H,
    options: &Options,
    cancellation: &Cancellation,
) -> Outcome
where
    S: SocketStreamReading + SocketStreamWriting + SocketClose,
    S::Socket: 'static,
    H: Handler,
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
pub async fn serve_tls<S, H>(
    scheduler: &S,
    listener: S::Socket,
    config: std::sync::Arc<rustls::ServerConfig>,
    handler: &H,
    options: &Options,
    cancellation: &Cancellation,
) -> Result<Statistics, Error>
where
    S: SocketAccept + SocketStreamReading + SocketStreamWriting + SocketClose,
    S::Socket: 'static,
    H: Handler,
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

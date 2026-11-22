// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Inbound connections speaking the selected protocol.
use crate::{Error, Handler, Options, Peer, ServerContext, cancellable};
use base_endpoint::stream::{ReadableStream, WritableStream};
use base_http::{InterimHandler, InterimRelay, Scheme, Status};
use base_http_protocol::HttpProtocol;
use base_http1::{LentInput, RequestReader, ResponseWriter};
use base_scheduler::Cancellation;
use futures_util::future::{Either, select};
use std::{pin::pin, sync::Arc};

/// One inbound connection speaking the selected protocol, as Async::HTTP's
/// `Protocol::HTTP1::Server` is: it reads each request, calls the handler with
/// it and writes the response, until the connection ends. There is one variant
/// for each engine compiled in, on which the server dispatches.
pub(crate) enum ServerConnection<R, W> {
    Http1(Http1Connection<R, W>),
}

impl<R: ReadableStream + Send, W: WritableStream> ServerConnection<R, W> {
    /// Construct a connection of `protocol` over an established stream, with
    /// options the caller has validated.
    pub(crate) fn new(protocol: HttpProtocol, reader: R, writer: W, options: &Options) -> Self {
        match protocol {
            HttpProtocol::Http1 => Self::Http1(Http1Connection {
                input: LentInput::new(
                    RequestReader::new(reader, options.http1.clone())
                        .expect("validated options construct server codecs"),
                ),
                output: ResponseWriter::new(writer),
            }),
        }
    }

    /// Serve requests with `handler` until the connection ends, counting them
    /// in `count`, then retire the connection's directions. The caller closes
    /// the transport.
    pub(crate) async fn serve<'scope, H>(
        self,
        handler: &H,
        peer: &Arc<Peer>,
        cancellation: &Cancellation,
        count: &mut u64,
        scheme: &Scheme,
    ) -> Result<(), Error>
    where
        R: 'scope,
        H: Handler<'scope, ServerContext>,
    {
        match self {
            Self::Http1(connection) => {
                connection
                    .serve(handler, peer, cancellation, count, scheme)
                    .await
            }
        }
    }
}

/// An HTTP/1 connection: its request input, lent to each request body, and its
/// response output.
pub(crate) struct Http1Connection<R, W> {
    input: LentInput<R>,
    output: ResponseWriter<W>,
}

impl<R: ReadableStream + Send, W: WritableStream> Http1Connection<R, W> {
    /// Serve requests until the connection ends, then retire both directions.
    async fn serve<'scope, H>(
        mut self,
        handler: &H,
        peer: &Arc<Peer>,
        cancellation: &Cancellation,
        count: &mut u64,
        scheme: &Scheme,
    ) -> Result<(), Error>
    where
        R: 'scope,
        H: Handler<'scope, ServerContext>,
    {
        let Self { input, output } = &mut self;
        let result = exchanges(input, output, handler, peer, cancellation, count, scheme).await;
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
}

async fn exchanges<'scope, R, W, H>(
    input: &mut LentInput<R>,
    output: &mut ResponseWriter<W>,
    handler: &H,
    peer: &Arc<Peer>,
    cancellation: &Cancellation,
    count: &mut u64,
    scheme: &Scheme,
) -> Result<(), Error>
where
    R: ReadableStream + Send + 'scope,
    W: WritableStream,
    H: Handler<'scope, ServerContext>,
{
    let relay = InterimRelay::new();
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
            let mut context = ServerContext::new(peer.clone(), relay.sender());
            let mut called = pin!(handler.call(request, &mut context, cancellation));
            // Write each interim response the handler hands over while still
            // polling the handler, which waits for it. Each wait is cancellable
            // on its own, rather than as one block, so that the interim writer,
            // large and rarely used, is pinned here when used instead of being
            // moved with the block for every request.
            let result = loop {
                let response =
                    match cancellable(select(called.as_mut(), pin!(relay.next())), cancellation)
                        .await
                    {
                        Ok(Either::Left((result, _))) => break Ok(result),
                        Ok(Either::Right((response, _))) => response,
                        Err(error) => break Err(error),
                    };
                let mut writer = output.interim(exchange.version());
                let written = pin!(writer.on_interim_response(response, cancellation));
                match cancellable(select(written, called.as_mut()), cancellation).await {
                    Ok(Either::Left((written, _))) => relay.complete(written),
                    // The handler abandoned its send; dropping the write
                    // poisons output if it had started.
                    Ok(Either::Right((result, _))) => break Ok(result),
                    Err(error) => break Err(error),
                }
            };
            relay.finish();
            result?
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
                return Err(Error::Response(error.into()));
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

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    Endpoint, Error, Options, ResponseBody,
    body::Consumption,
    driver::{Assigned, Operation, Queue, checked, linked, lock},
    outgoing::Interim,
    writable::{
        COMPLETE, ExchangeCompletion, FINALIZE, Open, OpenFactory, Opening, Return, ReturnTo,
    },
};
use base_endpoint::{
    AddressEndpoint, ConnectEndpoint,
    stream::{ReadableStream, WritableStream},
};
use base_http::{
    InterimResponses, Response,
    body::{BodyError, BodyReader, ReadableBody},
};
use base_http1::{RequestWriter, ResponseReader};
use base_pool::{Admission, MultiplexBuildPermit, MultiplexResource};
use base_scheduler::{
    Cancellation, OperationError, OperationResult, SocketClose, SocketConnect, SocketOpen,
    SocketSetOption, SocketStreamReading, SocketStreamWriting, TcpNoDelay,
};
use event_listener::Event;
use futures_channel::oneshot;
use futures_util::future::join;
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, Ordering},
};

struct Inner<'scope> {
    assigned: Mutex<Option<Assigned<'scope>>>,
    direct: Mutex<Option<Arc<dyn DirectConnection<'scope> + 'scope>>>,
    changed: Event,
    stop: Cancellation,
    result: Mutex<Option<Result<(), Error>>>,
}

/// An established HTTP/1 session retaining its transport and codec buffers.
/// Direct requests take and return its directions in the calling task;
/// supervised work handles construction, queued calls and retirement.
#[derive(Clone)]
pub struct Connection<'scope> {
    inner: Arc<Inner<'scope>>,
}
impl<'scope> Connection<'scope> {
    fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                assigned: Mutex::new(None),
                direct: Mutex::new(None),
                changed: Event::new(),
                stop: Cancellation::new(),
                result: Mutex::new(None),
            }),
        }
    }
    pub(crate) fn clone_handle(&self) -> Self {
        self.clone()
    }
    pub(crate) fn submit(&self, assigned: Assigned<'scope>) {
        let mut slot = lock(&self.inner.assigned);
        assert!(slot.is_none(), "HTTP/1 admits one exchange at a time");
        *slot = Some(assigned);
        drop(slot);
        self.inner.changed.notify(usize::MAX);
    }
    pub(crate) fn open(&self, opening: Open<'scope>) -> Opening<'scope> {
        // Admission is published only after the session installs its direct state, which it never removes.
        let direct = lock(&self.inner.direct)
            .clone()
            .expect("an admitted connection has direct state");
        direct.open(opening)
    }
    async fn next(&self, lifetime: &Cancellation) -> Option<Assigned<'scope>> {
        loop {
            event_listener::listener!(self.inner.changed => listener);
            if let Some(assigned) = lock(&self.inner.assigned).take() {
                return Some(assigned);
            }
            if checked(
                checked(
                    async {
                        listener.await;
                        Ok(())
                    },
                    &self.inner.stop,
                ),
                lifetime,
            )
            .await
            .is_err()
            {
                return None;
            }
        }
    }
    pub(crate) fn stop(&self) {
        self.inner.stop.cancel();
    }
    pub(crate) async fn wait_closed(&self) -> Result<(), Error> {
        loop {
            event_listener::listener!(self.inner.changed => listener);
            if let Some(result) = lock(&self.inner.result).clone() {
                return result;
            }
            listener.await;
        }
    }
    fn finish(&self, result: Result<(), Error>) {
        let mut current = lock(&self.inner.result);
        if current.is_none() {
            *current = Some(result);
        }
        drop(current);
        self.inner.changed.notify(usize::MAX);
    }
}

struct Owner<'scope> {
    connection: Connection<'scope>,
    registration: Option<MultiplexResource<Connection<'scope>>>,
}
impl Drop for Owner<'_> {
    fn drop(&mut self) {
        if let Some(registration) = &self.registration {
            registration.retire();
        }
        let abandoned = lock(&self.connection.inner.assigned).take();
        drop(abandoned);
        self.connection.finish(Err(Error::Closed));
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run<'scope, S>(
    scheduler: &'scope S,
    endpoint: Endpoint,
    options: Options,
    build: MultiplexBuildPermit<Connection<'scope>>,
    cancellation: Cancellation,
    lifetime: Cancellation,
    ready: oneshot::Sender<Result<(), Error>>,
    queue: Weak<Queue<'scope>>,
) where
    S: SocketOpen
        + SocketConnect
        + SocketClose
        + SocketSetOption<TcpNoDelay>
        + SocketStreamReading
        + SocketStreamWriting,
    S::Socket: 'static,
{
    let connection = Connection::new();
    let mut owner = Owner {
        connection: connection.clone(),
        registration: None,
    };
    let mut build = Some(build);
    let mut ready = Some(ready);
    let result = async {
        let address = endpoint.address;
        let endpoint_transport = TcpEndpoint {
            address: AddressEndpoint::new(address),
            nodelay: options.tcp_nodelay,
        };
        #[cfg(feature = "rustls")]
        if let Some(tls) = endpoint.tls {
            use base_endpoint::tls::{TlsClientEndpoint, TlsSession, rustls::RustlsConnector};
            let mut config = (*tls.config).clone();
            config.alpn_protocols = options.protocols.alpn_protocols();
            let endpoint = TlsClientEndpoint::new(
                endpoint_transport,
                RustlsConnector::new(Arc::new(config)),
                tls.server_name,
            );
            let transport = endpoint
                .connect(scheduler, &cancellation)
                .await
                .map_err(|error| Error::connect(address, error))?;
            let selected = transport.negotiated_protocol().map(<[u8]>::to_vec);
            return transport
                .with_duplex(scheduler, &lifetime, |reader, writer| async {
                    let protocol = options.protocols.negotiated(selected.as_deref())?;
                    let (input, output) = protocol
                        .client(reader, writer, options.http1.clone())
                        .expect("Client::new validated the HTTP/1 options");
                    session(
                        input,
                        output,
                        &mut owner,
                        &mut build,
                        &mut ready,
                        &options,
                        &cancellation,
                        &lifetime,
                        queue.clone(),
                    )
                    .await
                })
                .await;
        }
        let socket = endpoint_transport
            .connect(scheduler, &cancellation)
            .await
            .map_err(|error| Error::connect(address, error))?;
        let initialized = async {
            let mut reader = scheduler.socket_stream_reader(&socket)?;
            let writer = match scheduler.socket_stream_writer(&socket) {
                Ok(writer) => writer,
                Err(error) => {
                    let _ = ReadableStream::close(&mut reader).await;
                    return Err(Error::from(error));
                }
            };
            Ok::<_, Error>(
                options
                    .protocols
                    .plaintext()
                    .client(reader, writer, options.http1.clone())
                    .expect("Client::new validated the HTTP/1 options"),
            )
        }
        .await;
        let result = match initialized {
            Ok((input, output)) => {
                session(
                    input,
                    output,
                    &mut owner,
                    &mut build,
                    &mut ready,
                    &options,
                    &cancellation,
                    &lifetime,
                    queue.clone(),
                )
                .await
            }
            Err(error) => Err(error),
        };
        let closed = scheduler.socket_close(socket).await;
        result?;
        closed?;
        Ok(())
    }
    .await;
    // Failed construction remains charged through socket/lane cleanup.
    drop(build);
    if let Some(ready) = ready {
        let _ = ready.send(Err(result.as_ref().err().cloned().unwrap_or(Error::Closed)));
    }
    connection.finish(result);
}

/// A TCP connection, configured before any plaintext or TLS bytes are sent.
struct TcpEndpoint {
    address: AddressEndpoint,
    nodelay: bool,
}
impl<S> ConnectEndpoint<S> for TcpEndpoint
where
    S: SocketOpen + SocketConnect + SocketClose + SocketSetOption<TcpNoDelay>,
{
    type Connection = S::Socket;

    async fn connect(
        &self,
        scheduler: &S,
        cancellation: &Cancellation,
    ) -> OperationResult<S::Socket> {
        let socket = self.address.connect(scheduler, cancellation).await?;
        if self.nodelay {
            // A latency hint only: the connection remains usable without it.
            // Cancellation still ends establishment without further work.
            let result = scheduler
                .socket_set_option(&socket, TcpNoDelay, true, cancellation)
                .await;
            if let Err(error @ OperationError::Cancelled(_)) = result {
                let _ = scheduler.socket_close(socket).await;
                return Err(error);
            }
        }
        Ok(socket)
    }
}

// A connection retains its typed HTTP buffers and transport directions. Only
// an admitted exchange may take them; short locks protect transfer, never I/O.
trait DirectConnection<'scope>: Send + Sync {
    fn open(self: Arc<Self>, opening: Open<'scope>) -> Opening<'scope>;
}
struct Codecs<R, W> {
    input: Option<ResponseReader<R>>,
    output: Option<RequestWriter<W>>,
}
struct Http1Connection<'scope, R, W> {
    codecs: Mutex<Codecs<R, W>>,
    changed: Event,
    closing: AtomicBool,
    registration: MultiplexResource<Connection<'scope>>,
    queue: Weak<Queue<'scope>>,
    open: OpenFactory<'scope, R, W>,
}
impl<R: ReadableStream, W: WritableStream> Http1Connection<'_, R, W> {
    fn take(&self) -> Result<(ResponseReader<R>, RequestWriter<W>), Error> {
        let mut codecs = lock(&self.codecs);
        if self.closing.load(Ordering::Acquire) || codecs.input.is_none() || codecs.output.is_none()
        {
            return Err(Error::Closed);
        }
        Ok((codecs.input.take().unwrap(), codecs.output.take().unwrap()))
    }
    fn returned(&self) {
        if self.closing.load(Ordering::Acquire) {
            self.changed.notify(usize::MAX);
        }
    }
    async fn close(&self) -> Result<(), Error> {
        self.closing.store(true, Ordering::Release);
        let (mut input, mut output) = loop {
            event_listener::listener!(self.changed => listener);
            let returned = {
                let mut codecs = lock(&self.codecs);
                if codecs.input.is_some() && codecs.output.is_some() {
                    Some((codecs.input.take().unwrap(), codecs.output.take().unwrap()))
                } else {
                    None
                }
            };
            if let Some(codecs) = returned {
                break codecs;
            }
            listener.await;
        };
        let a = input.close().await;
        let b = output.close().await;
        drop(input);
        drop(output);
        a?;
        b?;
        Ok(())
    }
}
impl<'scope, R: ReadableStream + 'scope, W: WritableStream + 'scope> DirectConnection<'scope>
    for Http1Connection<'scope, R, W>
{
    fn open(self: Arc<Self>, opening: Open<'scope>) -> Opening<'scope> {
        let (input, output) = match self.take() {
            Ok(codecs) => codecs,
            Err(error) => {
                self.registration.retire();
                opening.active.complete();
                return crate::writable::failed_open(error);
            }
        };
        let completion = ExchangeCompletion::new(
            opening.active,
            opening.pending,
            opening.cancellation.clone(),
            self.registration.clone(),
        );
        // Construct guards before the future: even dropping an unpolled open
        // returns both directions and acknowledges their retirement obligation.
        let input = Return::new(input, completion.clone(), self.clone(), false);
        let output = Return::new(output, completion, self.clone(), true);
        (self.open)(
            input,
            output,
            opening.metadata,
            opening.interim,
            opening.lifetime,
        )
    }
}
impl<'scope, R: ReadableStream + 'scope, W: WritableStream + 'scope>
    ReturnTo<'scope, ResponseReader<R>> for Http1Connection<'scope, R, W>
{
    fn give_back(
        self: Arc<Self>,
        input: ResponseReader<R>,
        mode: u8,
        completion: Arc<ExchangeCompletion<'scope>>,
    ) {
        let reusable = mode == COMPLETE && input.is_reusable();
        lock(&self.codecs).input = Some(input);
        self.returned();
        completion.returned(1, reusable, None);
    }
}
impl<'scope, R: ReadableStream + 'scope, W: WritableStream + 'scope>
    ReturnTo<'scope, RequestWriter<W>> for Http1Connection<'scope, R, W>
{
    fn give_back(
        self: Arc<Self>,
        output: RequestWriter<W>,
        mode: u8,
        completion: Arc<ExchangeCompletion<'scope>>,
    ) {
        if mode == FINALIZE {
            // The exchange's pending admission retains the queue until both directions return.
            let queue = self
                .queue
                .upgrade()
                .expect("an unfinished exchange retains its queue");
            let cancellation = completion.cancellation().clone();
            let mut owner = Return::new(output, completion, self, true);
            let work = Box::pin(async move {
                let result = owner
                    .value
                    .as_mut()
                    .unwrap()
                    .write_frame(base_http::body::BodyFrame::<Vec<u8>>::End, &cancellation)
                    .await;
                match result {
                    Ok(()) => owner.complete(),
                    Err(error) => owner.report_error(error.into()),
                }
                // Failure/drop returns the poisoned writer through the guard.
            });
            // An abandoned driver drops the work, whose guard abandons the output.
            let _ = queue.push_work(work);
            return;
        }
        let reusable = mode == COMPLETE && output.is_reusable();
        lock(&self.codecs).output = Some(output);
        self.returned();
        completion.returned(
            2,
            reusable,
            Some(if mode == COMPLETE {
                Ok(())
            } else {
                Err(Error::Abandoned)
            }),
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn session<'scope, R, W>(
    mut input: ResponseReader<R>,
    mut output: RequestWriter<W>,
    owner: &mut Owner<'scope>,
    build: &mut Option<MultiplexBuildPermit<Connection<'scope>>>,
    ready: &mut Option<oneshot::Sender<Result<(), Error>>>,
    options: &Options,
    cancellation: &Cancellation,
    lifetime: &Cancellation,
    queue: Weak<Queue<'scope>>,
) -> Result<(), Error>
where
    R: ReadableStream + 'scope,
    W: WritableStream + 'scope,
{
    if cancellation.is_cancelled() {
        let _ = input.close().await;
        let _ = output.close().await;
        return Err(Error::Cancelled);
    }
    // Set the direct interface before publication; no admitted caller can see
    // a connection without its codec storage.
    let Ok(registration) = build
        .take()
        .unwrap()
        .commit(owner.connection.clone(), Admission::concurrent(0))
    else {
        // Commit fails only once shutdown has closed the pool, which queues this connection for retirement. Its waiter sees the closed client, and only a failed close is a driver failure.
        let ready = ready.take().expect("a session reports readiness once");
        let _ = ready.send(Err(Error::Closed));
        let input = input.close().await;
        let output = output.close().await;
        input?;
        output?;
        return Ok(());
    };
    owner.registration = Some(registration.clone());
    let state = Arc::new(Http1Connection {
        codecs: Mutex::new(Codecs {
            input: Some(input),
            output: Some(output),
        }),
        changed: Event::new(),
        closing: AtomicBool::new(false),
        registration,
        queue,
        open: crate::writable::factory(),
    });
    *lock(&owner.connection.inner.direct) = Some(state.clone());
    // A registration that is already draining publishes no admission; its retirement stops the session below.
    state.registration.update(Admission::concurrent(1));
    // Construction runs one session, and reports readiness itself only if the session never does.
    let ready = ready.take().expect("a session reports readiness once");
    let _ = ready.send(Ok(()));
    // Only queued pull-body calls run here. Imperative open/write/read acquires the retained state directly on the calling task.
    while let Some(Assigned {
        mut operation,
        active,
    }) = owner.connection.next(lifetime).await
    {
        // Direct exchanges restore both directions before releasing their use, and closing begins only after this loop.
        let (mut input, mut output) = state
            .take()
            .expect("an admitted exchange finds both codec directions");
        let local = operation.cancellation.clone();
        let caller = operation.lifetime.clone();
        let outcome = linked(
            exchange(&mut input, &mut output, &mut operation, options),
            &local,
            &caller,
            lifetime,
        )
        .await;
        operation.request.body.close(None);
        if let Some(reply) = operation.reply.take() {
            let _ = reply.send(Err(outcome
                .as_ref()
                .err()
                .cloned()
                .unwrap_or(Error::Closed)));
        }
        let reusable = matches!(outcome, Ok(true));
        if !reusable {
            state.registration.retire();
        }
        {
            let mut codecs = lock(&state.codecs);
            codecs.input = Some(input);
            codecs.output = Some(output);
        }
        drop(operation);
        active.complete();
        if !reusable {
            break;
        }
    }
    state.registration.retire();
    state.close().await
}

async fn exchange<R: ReadableStream, W: WritableStream>(
    input: &mut ResponseReader<R>,
    output: &mut RequestWriter<W>,
    operation: &mut Operation<'_>,
    options: &Options,
) -> Result<bool, Error> {
    let Operation {
        request,
        interim,
        reply,
        cancellation,
        ..
    } = operation;
    let metadata = request.metadata.clone();
    let headers = AtomicBool::new(false);
    let send = async {
        let result = checked(
            async {
                output
                    .write_request(request, cancellation)
                    .await
                    .map_err(Error::from)
            },
            cancellation,
        )
        .await;
        // Preserve a readable final response when the peer refuses further
        // upload. The connection is retired even if that response succeeds.
        if result.is_err() && !headers.load(Ordering::Acquire) {
            cancellation.cancel();
        }
        result
    };
    let receive = async {
        let mut writer = None;
        let result = checked(
            async {
                let mut observer = InterimResponses::new(Interim(interim.as_mut()));
                let mut response = input
                    .read_response(&metadata, &mut observer, cancellation)
                    .await?;
                let consumption = Consumption::new(cancellation.clone());
                let empty = response.body.is_empty();
                let (reader, mut producer) = BodyReader::bounded(
                    options.response_buffer_bytes,
                    options.response_buffer_frames,
                );
                // Publish known-empty input as completed before handing it to
                // another task. Its consumer may drop it immediately, without
                // racing a redundant End write or retiring a healthy session.
                if empty {
                    consumption.complete();
                    producer.close_write(None);
                }
                let body = ResponseBody {
                    reader,
                    consumption: consumption.clone(),
                    length: response.body.length(),
                    trailers: response.body.may_have_trailers(),
                };
                writer = Some(producer);
                let returned = Response::from_parts(response.metadata, body);
                if reply.take().unwrap().send(Ok(returned)).is_err() {
                    return Err(Error::Abandoned);
                }
                headers.store(true, Ordering::Release);
                if !empty {
                    loop {
                        let frame = response.body.read(cancellation).await?;
                        let end = frame.is_end();
                        writer
                            .as_mut()
                            .unwrap()
                            .write_frame(frame, cancellation)
                            .await?;
                        if end {
                            break;
                        }
                    }
                }
                consumption.wait().await
            },
            cancellation,
        )
        .await;
        if let Err(error) = &result {
            if let Some(writer) = &mut writer {
                writer.close_write(Some(match error {
                    Error::Body(error) => error.clone(),
                    error => BodyError::new(error.clone()),
                }));
            }
            cancellation.cancel();
        }
        result
    };
    let (sent, received) = join(send, receive).await;
    match (sent, received) {
        (Ok(persistent), Ok(())) => Ok(persistent && input.is_reusable()),
        (Err(_), Ok(())) => Ok(false),
        (Err(error), Err(Error::Cancelled)) if !matches!(error, Error::Cancelled) => Err(error),
        (_, Err(error)) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Client, driver::Pending, outgoing::tests::ignore};
    use base_http::{Method, RequestMetadata};
    use base_scheduler_direct::DirectScheduler;
    use futures_lite::future::{block_on, yield_now, zip};
    use std::{io::Read, net::TcpListener, thread};

    #[test]
    fn opening_an_admitted_use_after_the_session_closes_fails_and_releases_it() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let peer = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut bytes = Vec::new();
            stream.read_to_end(&mut bytes).unwrap();
            assert!(bytes.is_empty());
        });
        let scheduler = DirectScheduler::new(Default::default()).unwrap();
        let (client, driver) =
            Client::new(&scheduler, Endpoint::new(address), Options::default()).unwrap();
        let lifetime = Cancellation::new();
        let application = async {
            let active = client
                .context
                .acquire(&Cancellation::never())
                .await
                .unwrap();
            let connection = active.clone_handle();
            let pending = Pending::new(client.queue.clone()).unwrap();
            // Another thread can close the session between admission and opening its codecs.
            lifetime.cancel();
            while lock(&connection.inner.result).is_none() {
                yield_now().await;
            }
            let opening = Open {
                metadata: RequestMetadata::new(Method::GET),
                interim: Box::new(ignore),
                active,
                pending,
                cancellation: Cancellation::new(),
                lifetime: Cancellation::never(),
            };
            let error = connection.open(opening).await.err().unwrap();
            assert_eq!(error.to_string(), Error::Closed.to_string());
        };
        let (result, ()) = block_on(zip(driver.run(&lifetime), application));
        result.unwrap();
        assert_eq!(client.pool_stats().total(), 0);
        peer.join().unwrap();
        block_on(scheduler.shutdown());
    }
}

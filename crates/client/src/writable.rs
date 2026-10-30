// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Imperative HTTP/1 output with direct, independently owned codec directions.

use crate::{
    Client, Error,
    driver::{CancelOnDrop, Pending, checked, lock},
    outgoing::{Interim, Observer},
    reuse::{Cache, Recycle, Reused},
    sink::owned_sink,
};
use base_endpoint::stream::{ReadableStream, WritableStream};
use base_http::{
    InterimHandler, InterimResponses, Method, Path, RequestMetadata, Response, ResponseMetadata,
    body::{BodyFrame, ReadableBody, WritableBody},
};
use base_http1::{RequestWriter, ResponseReader};
use base_scheduler::Cancellation;
use futures_util::{Sink, SinkExt, Stream, StreamExt, task::AtomicWaker};
use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
    },
    task::{Context, Poll},
};

trait Push: Sink<(BodyFrame<Vec<u8>>, Cancellation), Error = Error> + Recycle {}
impl<T: Sink<(BodyFrame<Vec<u8>>, Cancellation), Error = Error> + Recycle> Push for T {}
trait Receive: Stream<Item = Result<ResponseEvent, Error>> + Recycle {}
impl<T: Stream<Item = Result<ResponseEvent, Error>> + Recycle> Receive for T {}
pub(crate) trait OpeningOperation<'scope>:
    Future<Output = Result<WritableRequest<'scope>, Error>> + Recycle
{
}
impl<'scope, T: Future<Output = Result<WritableRequest<'scope>, Error>> + Recycle>
    OpeningOperation<'scope> for T
{
}
type PushSink<'scope> = Reused<dyn Push + Send + 'scope>;
type ResponseStream<'scope> = Reused<dyn Receive + Send + 'scope>;
pub(crate) type Opening<'scope> = Reused<dyn OpeningOperation<'scope> + Send + 'scope>;
pub(crate) type OpenFactory<'scope, R, W> = Box<
    dyn Fn(
            Return<'scope, ResponseReader<R>>,
            Return<'scope, RequestWriter<W>>,
            RequestMetadata,
            Box<dyn Observer + 'scope>,
            Cancellation,
        ) -> Opening<'scope>
        + Send
        + Sync
        + 'scope,
>;

pub(crate) fn failed_open<'scope>(error: Error) -> Opening<'scope> {
    Reused::new(Cache::new().take(|| std::future::ready(Err(error))))
}

impl<'scope> Client<'scope> {
    /// Acquire a connection and send metadata for an imperative upload.
    /// Unknown length uses chunked HTTP/1.1 framing. Drive the client driver
    /// concurrently for establishment, TLS progress and cleanup. Established
    /// plaintext requests run entirely in the calling future.
    pub async fn open(
        &self,
        method: Method,
        path: impl Into<Path>,
        cancellation: &Cancellation,
    ) -> Result<WritableRequest<'scope>, Error> {
        let mut metadata = RequestMetadata::new(method);
        metadata.path = Some(path.into());
        self.open_request(metadata, cancellation).await
    }

    /// Open with explicit fields, authority and optional content length.
    /// Managed tunnels/upgrades and automatic request replay remain unsupported.
    pub async fn open_request(
        &self,
        metadata: RequestMetadata,
        cancellation: &Cancellation,
    ) -> Result<WritableRequest<'scope>, Error> {
        self.open_request_with_interim(
            metadata,
            |_: ResponseMetadata| async { Ok::<_, Infallible>(()) },
            cancellation,
        )
        .await
    }

    /// Observe informational responses while the pending response is polled.
    /// Opening and writing alone do not read input or wait for 100 Continue.
    pub async fn open_request_with_interim<I: InterimHandler + 'scope>(
        &self,
        mut metadata: RequestMetadata,
        interim: I,
        cancellation: &Cancellation,
    ) -> Result<WritableRequest<'scope>, Error> {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if metadata.method == Method::CONNECT
            || metadata.protocol.is_some()
            || metadata.fields.contains("upgrade")
        {
            return Err(Error::Unsupported("managed client tunnels and upgrades"));
        }
        if metadata
            .scheme
            .as_deref()
            .is_some_and(|scheme| scheme != self.endpoint.scheme())
        {
            return Err(Error::Unsupported(
                "request scheme does not match the endpoint",
            ));
        }
        metadata
            .scheme
            .get_or_insert_with(|| self.endpoint.request_scheme());
        metadata
            .authority
            .get_or_insert_with(|| self.endpoint.authority.clone());
        let pending = Pending::new(self.queue.clone())?;
        let local = self.context.cancellation.child();
        let mut guard = CancelOnDrop(local.clone(), true);
        let active = checked(
            checked(self.context.acquire(&local), &self.context.cancellation),
            cancellation,
        )
        .await?;
        let connection = active.clone_handle();
        let opening = Open {
            metadata,
            interim: Box::new(interim),
            active,
            pending,
            cancellation: local,
            lifetime: cancellation.clone(),
        };
        let result = checked(connection.open(opening), cancellation).await?;
        guard.1 = false;
        Ok(result)
    }
}

/// An active request, distinct from the semantic `Request<B>` message value.
/// Writes drive HTTP output. `finish` consumes this handle and returns the
/// response. `split` permits independent upload and response consumption.
/// Sequential upload can deadlock against a peer which blocks on response
/// output before consuming the request; proxies must drive both directions.
#[must_use = "finish or split the request; dropping it abandons the exchange"]
pub struct WritableRequest<'scope> {
    output: RequestBodyWriter<'scope>,
    response: PendingResponse<'scope>,
}
impl<'scope> WritableRequest<'scope> {
    pub async fn write(
        &mut self,
        data: impl Into<Vec<u8>>,
        cancellation: &Cancellation,
    ) -> Result<(), Error> {
        self.output.write(data, cancellation).await
    }
    pub async fn write_frame(
        &mut self,
        frame: BodyFrame<Vec<u8>>,
        cancellation: &Cancellation,
    ) -> Result<(), Error> {
        self.output.write_frame(frame, cancellation).await
    }
    pub async fn flush(&mut self, cancellation: &Cancellation) -> Result<(), Error> {
        self.output.flush(cancellation).await
    }
    pub fn split(self) -> (RequestBodyWriter<'scope>, PendingResponse<'scope>) {
        (self.output, self.response)
    }
    /// Complete output, then read final response metadata. For early responses
    /// or simultaneous streaming, split and poll both directions concurrently.
    pub async fn finish(
        self,
        cancellation: &Cancellation,
    ) -> Result<Response<StreamingResponseBody<'scope>>, Error> {
        let (output, response) = self.split();
        output.finish(cancellation).await?;
        checked(response, cancellation).await
    }
}

/// Direct request output. Vectors move into the encoder; slices are copied by
/// `Into<Vec<u8>>`. One boxed sink retains in-flight state, without allocating
/// a future or crossing a producer queue for each frame.
#[must_use = "finish request output or explicitly abort it"]
pub struct RequestBodyWriter<'scope> {
    sink: Option<PushSink<'scope>>,
    exchange: Arc<ExchangeCompletion<'scope>>,
    ended: bool,
    completion: Option<Result<(), Error>>,
}
struct Writing<'a, 'scope> {
    sink: &'a mut Option<PushSink<'scope>>,
    cancellation: Cancellation,
    armed: bool,
}
impl Drop for Writing<'_, '_> {
    fn drop(&mut self) {
        if self.armed {
            self.cancellation.cancel();
            self.sink.take();
        }
    }
}
impl RequestBodyWriter<'_> {
    pub async fn write(
        &mut self,
        data: impl Into<Vec<u8>>,
        cancellation: &Cancellation,
    ) -> Result<(), Error> {
        self.write_frame(
            BodyFrame::Data {
                data: data.into(),
                end: false,
            },
            cancellation,
        )
        .await
    }
    pub async fn write_frame(
        &mut self,
        frame: BodyFrame<Vec<u8>>,
        cancellation: &Cancellation,
    ) -> Result<(), Error> {
        if cancellation.is_cancelled() || self.exchange.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if self.ended {
            return Err(Error::Closed);
        }
        let end = frame.is_end();
        let mut guard = Writing {
            sink: &mut self.sink,
            cancellation: self.exchange.cancellation.clone(),
            armed: true,
        };
        checked(
            guard
                .sink
                .as_mut()
                .expect("unended output retains its sink until the exchange is cancelled")
                .send((frame, cancellation.clone())),
            cancellation,
        )
        .await?;
        guard.armed = false;
        drop(guard);
        if end {
            self.ended = true;
            self.sink.take();
            self.completion = Some(Ok(()));
        }
        Ok(())
    }
    /// Every accepted frame is already flushed to the transport. After normal
    /// `close_write`, also wait for final framing. This does not promise that
    /// the peer has received or processed the bytes.
    pub async fn flush(&mut self, cancellation: &Cancellation) -> Result<(), Error> {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if let Some(result) = &self.completion {
            return result.clone();
        }
        if self.exchange.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if self.ended {
            self.wait_finished(cancellation).await
        } else {
            Ok(())
        }
    }
    async fn wait_finished(&mut self, cancellation: &Cancellation) -> Result<(), Error> {
        if let Some(result) = &self.completion {
            return result.clone();
        }
        // The result belongs to the exchange, so a cancelled or dropped wait
        // cannot consume it before a later flush.
        let result = self.exchange.wait_output(cancellation).await;
        if matches!(result, Err(Error::Cancelled)) {
            return result;
        }
        self.completion = Some(result.clone());
        result
    }
    pub async fn finish(mut self, cancellation: &Cancellation) -> Result<(), Error> {
        if !self.ended {
            self.write_frame(BodyFrame::End, cancellation).await?;
        }
        self.wait_finished(cancellation).await
    }
    /// Signal normal closure. The connection supervisor sends final framing;
    /// use asynchronous `finish` when completion must be observed.
    pub fn close_write(&mut self, error: Option<Error>) {
        if !self.ended {
            if let Some(error) = error {
                self.completion = Some(Err(error));
                self.exchange.cancellation.cancel();
            } else {
                self.exchange.output_mode.store(FINALIZE, Ordering::Release);
            }
            self.sink.take();
            self.ended = true;
        }
    }
    pub fn abort(&mut self) {
        self.exchange.cancellation.cancel();
        self.sink.take();
        self.ended = true;
    }
}
impl WritableBody for RequestBodyWriter<'_> {
    type Data = Vec<u8>;
    type Error = Error;
    async fn write_frame(
        &mut self,
        frame: BodyFrame<Vec<u8>>,
        cancellation: &Cancellation,
    ) -> Result<(), Error> {
        Self::write_frame(self, frame, cancellation).await
    }
    async fn flush(&mut self, cancellation: &Cancellation) -> Result<(), Error> {
        Self::flush(self, cancellation).await
    }
    fn close_write(&mut self, error: Option<Error>) {
        Self::close_write(self, error);
    }
    fn abort(&mut self) {
        Self::abort(self);
    }
}

/// A future for final response metadata. Poll independently of output to accept
/// an early response. Dropping it retires unfinished HTTP/1 input.
#[must_use = "poll the response or drop it to abandon input"]
pub struct PendingResponse<'scope> {
    stream: Option<ResponseStream<'scope>>,
    cancellation: Cancellation,
}
impl<'scope> Future for PendingResponse<'scope> {
    type Output = Result<Response<StreamingResponseBody<'scope>>, Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let event = if self.cancellation.is_cancelled() {
            Err(Error::Cancelled)
        } else {
            match self
                .stream
                .as_mut()
                .expect("response already polled")
                .poll_next_unpin(cx)
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(event) => event
                    .transpose()
                    .and_then(|event| event.ok_or(Error::Closed)),
            }
        };
        match event {
            Ok(ResponseEvent::Metadata(metadata, length, trailers, empty)) => {
                let mut stream = self.stream.take();
                if empty {
                    // The decoder has already returned its input direction.
                    // Release storage even if this empty body is retained.
                    stream.take();
                }
                Poll::Ready(Ok(Response::from_parts(
                    metadata,
                    StreamingResponseBody {
                        stream,
                        cancellation: self.cancellation.clone(),
                        length,
                        trailers,
                        empty,
                    },
                )))
            }
            Ok(ResponseEvent::Frame(_)) => unreachable!("first response event is metadata"),
            Err(error) => {
                self.cancellation.cancel();
                self.stream.take();
                Poll::Ready(Err(error))
            }
        }
    }
}
impl Drop for PendingResponse<'_> {
    fn drop(&mut self) {
        if self.stream.is_some() {
            self.cancellation.cancel();
        }
    }
}
enum ResponseEvent {
    Metadata(ResponseMetadata, Option<u64>, bool, bool),
    Frame(BodyFrame<Vec<u8>>),
}

/// Pull input backed by the connection's HTTP reader, with no response frame
/// queue. The boxed stream retains the decoder's borrowed state across reads.
/// Dropping a pending read retains that state; cancelling an active read retires
/// the exchange. Return/drop active handles before joining cancelled cleanup.
pub struct StreamingResponseBody<'scope> {
    stream: Option<ResponseStream<'scope>>,
    cancellation: Cancellation,
    length: Option<u64>,
    trailers: bool,
    empty: bool,
}
impl ReadableBody for StreamingResponseBody<'_> {
    type Data = Vec<u8>;
    type Error = Error;
    fn length(&self) -> Option<u64> {
        self.length
    }
    fn may_have_trailers(&self) -> bool {
        self.trailers
    }
    fn is_empty(&self) -> bool {
        self.empty
    }
    fn close(&mut self, _: Option<Error>) {
        self.stream.take();
        self.empty = true;
    }
    async fn read(&mut self, cancellation: &Cancellation) -> Result<BodyFrame<Vec<u8>>, Error> {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if self.empty {
            return Ok(BodyFrame::End);
        }
        let event = checked(
            async {
                self.stream
                    .as_mut()
                    .ok_or(Error::Closed)?
                    .next()
                    .await
                    .transpose()?
                    .ok_or(Error::Closed)
            },
            cancellation,
        )
        .await;
        match event {
            Ok(ResponseEvent::Frame(frame)) => {
                if frame.is_end() {
                    self.empty = true;
                    self.stream.take();
                }
                Ok(frame)
            }
            Ok(ResponseEvent::Metadata(..)) => unreachable!("response metadata is delivered once"),
            Err(error) => {
                self.cancellation.cancel();
                self.stream.take();
                Err(error)
            }
        }
    }
}

pub(crate) struct Open<'scope> {
    pub metadata: RequestMetadata,
    pub interim: Box<dyn Observer + 'scope>,
    pub active: base_pool::MultiplexUse<crate::Connection<'scope>>,
    pub pending: Pending<'scope>,
    pub cancellation: Cancellation,
    pub lifetime: Cancellation,
}

// Last-direction completion is synchronous. Codec owners first restore their
// values to the connection, then release admission or retire a poisoned use.
pub(crate) struct ExchangeCompletion<'scope> {
    state: Mutex<CompletionState<'scope>>,
    cancellation: Cancellation,
    registration: base_pool::MultiplexResource<crate::Connection<'scope>>,
    output_mode: AtomicU8,
    output_finished: AtomicWaker,
}
struct CompletionState<'scope> {
    active: Option<base_pool::MultiplexUse<crate::Connection<'scope>>>,
    pending: Option<Pending<'scope>>,
    directions: u8,
    reusable: bool,
    output_result: Option<Result<(), Error>>,
}
impl<'scope> ExchangeCompletion<'scope> {
    pub(crate) fn new(
        active: base_pool::MultiplexUse<crate::Connection<'scope>>,
        pending: Pending<'scope>,
        cancellation: Cancellation,
        registration: base_pool::MultiplexResource<crate::Connection<'scope>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(CompletionState {
                active: Some(active),
                pending: Some(pending),
                directions: 0,
                reusable: true,
                output_result: None,
            }),
            cancellation,
            registration,
            output_mode: AtomicU8::new(ABANDONED),
            output_finished: AtomicWaker::new(),
        })
    }
    pub(crate) fn cancellation(&self) -> &Cancellation {
        &self.cancellation
    }
    async fn wait_output(&self, cancellation: &Cancellation) -> Result<(), Error> {
        // Only the request writer waits for output completion. Unregister its
        // task even if this particular flush future is dropped or cancelled.
        struct Waiting<'a>(&'a AtomicWaker);
        impl Drop for Waiting<'_> {
            fn drop(&mut self) {
                self.0.take();
            }
        }
        let _waiting = Waiting(&self.output_finished);
        checked(
            std::future::poll_fn(|cx| {
                self.output_finished.register(cx.waker());
                match lock(&self.state).output_result.clone() {
                    Some(result) => Poll::Ready(result),
                    None => Poll::Pending,
                }
            }),
            cancellation,
        )
        .await
    }
    pub(crate) fn returned(
        &self,
        direction: u8,
        reusable: bool,
        output: Option<Result<(), Error>>,
    ) {
        let mut state = lock(&self.state);
        state.directions |= direction;
        state.reusable &= reusable;
        if let Some(output) = output {
            state.output_result.get_or_insert(output);
        }
        let completed = if state.directions == 3 {
            Some((
                state.pending.take(),
                state.active.take().unwrap(),
                !state.reusable || self.cancellation.is_cancelled(),
            ))
        } else {
            None
        };
        drop(state);
        if let Some((pending, active, retire)) = completed {
            if retire {
                self.registration.retire();
            }
            drop(pending);
            active.complete();
        }
        if direction == 2 {
            self.output_finished.wake();
        }
    }
}

pub(crate) const ABANDONED: u8 = 0;
pub(crate) const COMPLETE: u8 = 1;
pub(crate) const FINALIZE: u8 = 2;
pub(crate) trait ReturnTo<'scope, T>: Send + Sync {
    fn give_back(self: Arc<Self>, value: T, mode: u8, completion: Arc<ExchangeCompletion<'scope>>);
}
pub(crate) struct Return<'scope, T> {
    pub value: Option<T>,
    return_to: Arc<dyn ReturnTo<'scope, T> + 'scope>,
    mode: u8,
    output: bool,
    completion: Arc<ExchangeCompletion<'scope>>,
}
impl<'scope, T> Return<'scope, T> {
    pub(crate) fn new(
        value: T,
        completion: Arc<ExchangeCompletion<'scope>>,
        return_to: Arc<dyn ReturnTo<'scope, T> + 'scope>,
        output: bool,
    ) -> Self {
        Self {
            value: Some(value),
            return_to,
            mode: ABANDONED,
            output,
            completion,
        }
    }
    pub(crate) fn complete(&mut self) {
        self.mode = COMPLETE;
        self.release();
    }
    pub(crate) fn report_error(&mut self, error: Error) {
        lock(&self.completion.state)
            .output_result
            .get_or_insert(Err(error));
    }
    fn release(&mut self) {
        if let Some(value) = self.value.take() {
            let mode = if self.output && self.mode == ABANDONED {
                // Consume FINALIZE once. A dropped deferred-finalization guard
                // must abandon output rather than enqueue another finalizer.
                self.completion
                    .output_mode
                    .swap(ABANDONED, Ordering::AcqRel)
            } else {
                self.mode
            };
            if mode == ABANDONED {
                self.completion.cancellation.cancel();
            }
            self.return_to
                .clone()
                .give_back(value, mode, self.completion.clone());
        }
    }
}
impl<T> Drop for Return<'_, T> {
    fn drop(&mut self) {
        self.release();
    }
}

/// Each connection retains only empty adapter allocations. The factory hides
/// their concrete future types without changing caller-driven HTTP/1 progress.
pub(crate) fn factory<'scope, R, W>() -> OpenFactory<'scope, R, W>
where
    R: ReadableStream + 'scope,
    W: WritableStream + 'scope,
{
    let writing_cache = Cache::new();
    let receiving_cache = Cache::new();
    let adapters = move |mut input: Return<'scope, ResponseReader<R>>,
                         output: Return<'scope, RequestWriter<W>>,
                         metadata: RequestMetadata,
                         observer: Box<dyn Observer + 'scope>,
                         lifetime: Cancellation| {
        let exchange = output.completion.clone();
        let local = exchange.cancellation.clone();
        let writing_lifetime = lifetime.clone();
        let writing = writing_cache.take(|| {
            owned_sink(
                output,
                move |mut owner, (frame, cancellation): (BodyFrame<Vec<u8>>, Cancellation)| {
                    let lifetime = writing_lifetime.clone();
                    async move {
                        let end = frame.is_end();
                        checked(
                            checked(
                                async {
                                    owner
                                        .value
                                        .as_mut()
                                        .expect(
                                            "request output drops its sink after the final frame",
                                        )
                                        .write_frame(frame, &owner.completion.cancellation)
                                        .await
                                        .map_err(Error::from)
                                },
                                &cancellation,
                            ),
                            &lifetime,
                        )
                        .await?;
                        if end {
                            owner.complete();
                        }
                        Ok(owner)
                    }
                },
            )
        });
        let receive_cancel = local.clone();
        let receiving = receiving_cache.take(|| {
            async_stream::try_stream! {
                let mut observer = observer;
                let mut interim = InterimResponses::new(Interim(observer.as_mut()));
                // Socket reads and interim callbacks already observe receive_cancel.
                // Keep only the separate caller-lifetime check around that operation.
                let response = checked(
                    async {
                        input.value.as_mut().unwrap()
                            .read_response(&metadata, &mut interim, &receive_cancel)
                            .await.map_err(Error::from)
                    },
                    &lifetime,
                ).await?;
                let (metadata, mut body) = response.into_parts();
                let length = body.length();
                let trailers = body.may_have_trailers();
                if body.is_empty() {
                    drop(body);
                    input.complete();
                    yield ResponseEvent::Metadata(metadata, length, trailers, true);
                } else {
                    yield ResponseEvent::Metadata(metadata, length, trailers, false);
                    loop {
                        let frame = checked(
                            async { body.read(&receive_cancel).await.map_err(Error::from) },
                            &lifetime,
                        ).await?;
                        if frame.is_end() {
                            drop(body);
                            input.complete();
                            yield ResponseEvent::Frame(frame);
                            break;
                        }
                        yield ResponseEvent::Frame(frame);
                    }
                }
            }
        });
        let response = PendingResponse {
            stream: Some(Reused::new(receiving)),
            cancellation: local.clone(),
        };
        WritableRequest {
            output: RequestBodyWriter {
                sink: Some(Reused::new(writing)),
                exchange,
                ended: false,
                completion: None,
            },
            response,
        }
    };
    let adapters = Arc::new(adapters);
    let opening_cache = Cache::new();
    Box::new(move |input, mut output, metadata, observer, lifetime| {
        let adapters = adapters.clone();
        Reused::new(opening_cache.take(|| async move {
            let local = output.completion.cancellation.clone();
            checked(
                async {
                    output
                        .value
                        .as_mut()
                        .unwrap()
                        .begin_request(&metadata, &local)
                        .await
                        .map_err(Error::from)
                },
                &lifetime,
            )
            .await?;
            Ok(adapters(input, output, metadata, observer, lifetime))
        }))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_opening_reports_its_error() {
        let opening = failed_open(Error::Closed);
        let error = futures_lite::future::block_on(opening).err().unwrap();
        assert_eq!(error.to_string(), "HTTP client is closed");
    }
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::{
    Method, Request,
    body::{BodyFrame, EmptyBody, ReadableBody},
};
use base_http_client::{Client, Endpoint, Error, Options};
use base_scheduler::{
    BufferPool, BufferPoolOptions, BufferPooling, Cancellation, IoBuf, IoBufSequence,
    IoBufSequenceMut, OperationError, OperationResult, SocketAddress, SocketClose, SocketConnect,
    SocketDomain, SocketOpen, SocketOperation, SocketProtocol, SocketRecvFlags, SocketRecvResult,
    SocketSendFlags, SocketStreamReader, SocketStreamReading, SocketStreamWriter,
    SocketStreamWriting, SocketType,
};
use base_scheduler_direct::{DirectScheduler, DirectSocket};
use futures_channel::oneshot;
use futures_lite::future::{block_on, or, poll_once, yield_now, zip};
use std::{
    future::Future,
    io::{self, BufRead, BufReader, IoSlice, IoSliceMut, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    pin::pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

fn peer(work: impl FnOnce(TcpListener) + Send + 'static) -> (SocketAddr, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    (address, thread::spawn(move || work(listener)))
}
fn accept(listener: &TcpListener) -> BufReader<TcpStream> {
    let (stream, _) = listener.accept().unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    BufReader::new(stream)
}
fn head(stream: &mut impl BufRead) -> String {
    let mut result = String::new();
    loop {
        let mut line = String::new();
        assert_ne!(stream.read_line(&mut line).unwrap(), 0, "EOF before fields");
        result.push_str(&line);
        if line == "\r\n" {
            return result;
        }
    }
}
// Read until the client closes the connection, returning everything it sent.
fn remaining(stream: &mut impl Read) -> Vec<u8> {
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).unwrap();
    bytes
}
fn respond_no_content(listener: &TcpListener) {
    let mut stream = accept(listener);
    assert!(head(&mut stream).starts_with("GET /next HTTP/1.1\r\n"));
    stream
        .get_mut()
        .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
        .unwrap();
    assert!(remaining(&mut stream).is_empty());
}
async fn next_call(client: &Client<'_>) {
    let response = client
        .call(
            Request::new(Method::GET, "/next", EmptyBody),
            &Cancellation::never(),
        )
        .await
        .unwrap();
    assert!(response.body.is_empty());
}
async fn payload(body: &mut impl ReadableBody) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    loop {
        let frame = body
            .read(&Cancellation::never())
            .await
            .map_err(|error| error.to_string())?;
        let end = frame.is_end();
        if let BodyFrame::Data { data, .. } = frame {
            bytes.extend_from_slice(data.as_ref());
        }
        if end {
            return Ok(bytes);
        }
    }
}
fn injected(name: &str) -> io::Error {
    io::Error::other(format!("injected {name} failure"))
}
fn source(error: &Error) -> String {
    std::error::Error::source(error).unwrap().to_string()
}
async fn wait_until(condition: impl Fn() -> bool) {
    while !condition() {
        yield_now().await;
    }
}

// Drive one client and its driver together. The watchdog only bounds a broken test.
fn drive<'scope, S>(
    scheduler: &'scope S,
    address: SocketAddr,
    lifetime: &Cancellation,
    application: impl AsyncFnOnce(Client<'scope>),
) -> Result<(), Error>
where
    S: SocketOpen + SocketConnect + SocketClose + SocketStreamReading + SocketStreamWriting,
    S::Socket: 'static,
{
    let options = Options {
        pool: base_pool::Options {
            max_resources: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let (client, driver) = Client::new(scheduler, Endpoint::new(address), options).unwrap();
    let stats = client.clone();
    let watchdog = lifetime.clone();
    let (done, wait) = mpsc::channel::<()>();
    let timer = thread::spawn(move || {
        if wait.recv_timeout(Duration::from_secs(15)).is_err() {
            watchdog.cancel();
        }
    });
    let (result, ()) = block_on(zip(driver.run(lifetime), application(client)));
    let _ = done.send(());
    timer.join().unwrap();
    assert_eq!(stats.pool_stats().total(), 0);
    result
}

struct Gate {
    after: bool,
    reached: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

// Faults injected into an otherwise real scheduler.
#[derive(Default)]
struct Plan {
    reader: AtomicBool,
    writer: AtomicBool,
    reader_close: AtomicBool,
    writer_close: AtomicBool,
    socket_close: AtomicBool,
    reader_closes: AtomicUsize,
    writer_closes: AtomicUsize,
    // Pause the next connect until released, before or after the TCP handshake.
    connect: Mutex<Option<Gate>>,
    // Block the polling thread after the next nonempty read until another thread proceeds.
    read: Mutex<Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>>,
}
impl Plan {
    fn gate(&self, after: bool) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached, reached_wait) = oneshot::channel();
        let (release, release_wait) = oneshot::channel();
        *self.connect.lock().unwrap() = Some(Gate {
            after,
            reached,
            release: release_wait,
        });
        (reached_wait, release)
    }
    fn pause_read(&self) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (reached, reached_wait) = mpsc::channel();
        let (proceed, proceed_wait) = mpsc::channel();
        *self.read.lock().unwrap() = Some((reached, proceed_wait));
        (reached_wait, proceed)
    }
}
struct Faults<'a> {
    inner: &'a DirectScheduler,
    plan: Arc<Plan>,
}
impl<'a> Faults<'a> {
    fn new(inner: &'a DirectScheduler) -> Self {
        Self {
            inner,
            plan: Arc::default(),
        }
    }
}
impl SocketOperation for Faults<'_> {
    type Socket = DirectSocket;
}
impl BufferPooling for Faults<'_> {
    type Pool = <DirectScheduler as BufferPooling>::Pool;
    fn buffer_pool(&self, options: BufferPoolOptions) -> io::Result<Self::Pool> {
        self.inner.buffer_pool(options)
    }
}
impl SocketOpen for Faults<'_> {
    fn socket_open(
        &self,
        domain: SocketDomain,
        kind: SocketType,
        protocol: Option<SocketProtocol>,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<DirectSocket>> + Send {
        self.inner.socket_open(domain, kind, protocol, cancellation)
    }
}
impl SocketConnect for Faults<'_> {
    async fn socket_connect(
        &self,
        socket: &DirectSocket,
        address: SocketAddress,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        let gate = self.plan.connect.lock().unwrap().take();
        let Some(gate) = gate else {
            return self
                .inner
                .socket_connect(socket, address, cancellation)
                .await;
        };
        let pause = async {
            gate.reached.send(()).unwrap();
            gate.release.await.unwrap();
        };
        if gate.after {
            let result = self
                .inner
                .socket_connect(socket, address, cancellation)
                .await;
            pause.await;
            result
        } else {
            pause.await;
            self.inner
                .socket_connect(socket, address, cancellation)
                .await
        }
    }
}
impl SocketClose for Faults<'_> {
    async fn socket_close(&self, socket: DirectSocket) -> io::Result<()> {
        self.inner.socket_close(socket).await?;
        if self.plan.socket_close.load(Ordering::SeqCst) {
            return Err(injected("socket close"));
        }
        Ok(())
    }
    fn socket_close_submit(
        &self,
        socket: DirectSocket,
    ) -> impl Future<Output = io::Result<()>> + Send {
        self.inner.socket_close_submit(socket)
    }
}
impl<'a> SocketStreamReading for Faults<'a> {
    fn socket_stream_reader<'scheduler>(
        &'scheduler self,
        socket: &DirectSocket,
    ) -> io::Result<
        impl SocketStreamReader<Buffer = <Self::Pool as BufferPool>::Buffer>
        + 'scheduler
        + use<'scheduler, 'a>,
    > {
        if self.plan.reader.load(Ordering::SeqCst) {
            return Err(injected("reader"));
        }
        Ok(FaultReader {
            inner: self.inner.socket_stream_reader(socket)?,
            plan: self.plan.clone(),
        })
    }
    fn socket_stream_reader_with_pool<'scheduler>(
        &'scheduler self,
        socket: &DirectSocket,
        pool: Self::Pool,
    ) -> io::Result<
        impl SocketStreamReader<Buffer = <Self::Pool as BufferPool>::Buffer>
        + 'scheduler
        + use<'scheduler, 'a>,
    > {
        Ok(FaultReader {
            inner: self.inner.socket_stream_reader_with_pool(socket, pool)?,
            plan: self.plan.clone(),
        })
    }
}
impl<'a> SocketStreamWriting for Faults<'a> {
    fn socket_stream_writer<'scheduler>(
        &'scheduler self,
        socket: &DirectSocket,
    ) -> io::Result<
        impl SocketStreamWriter<Buffer = <Self::Pool as BufferPool>::Buffer>
        + 'scheduler
        + use<'scheduler, 'a>,
    > {
        if self.plan.writer.load(Ordering::SeqCst) {
            return Err(injected("writer"));
        }
        Ok(FaultWriter {
            inner: self.inner.socket_stream_writer(socket)?,
            plan: self.plan.clone(),
        })
    }
    fn socket_stream_writer_with_buffer<'scheduler, B: IoBuf>(
        &'scheduler self,
        socket: &DirectSocket,
    ) -> io::Result<impl SocketStreamWriter<Buffer = B> + 'scheduler + use<'scheduler, 'a, B>> {
        Ok(FaultWriter {
            inner: self.inner.socket_stream_writer_with_buffer::<B>(socket)?,
            plan: self.plan.clone(),
        })
    }
}
struct FaultReader<R> {
    inner: R,
    plan: Arc<Plan>,
}
impl<R: SocketStreamReader> SocketStreamReader for FaultReader<R> {
    type Buffer = R::Buffer;
    fn read(
        &mut self,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Option<Self::Buffer>>> + Send {
        self.inner.read(cancellation)
    }
    async fn read_into(
        &mut self,
        buffer: &mut [u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        let length = self.inner.read_into(buffer, cancellation).await?;
        if length > 0 {
            let pause = self.plan.read.lock().unwrap().take();
            if let Some((reached, proceed)) = pause {
                // Hold this poll after data arrived but before the codec observes it.
                reached.send(()).unwrap();
                proceed.recv().unwrap();
            }
        }
        Ok(length)
    }
    fn read_vectored(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<usize>> + Send {
        self.inner.read_vectored(buffers, cancellation)
    }
    fn read_vectored_with_flags(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<SocketRecvResult>> + Send {
        self.inner
            .read_vectored_with_flags(buffers, flags, cancellation)
    }
    fn read_owned_vectored<Buffers: IoBufSequenceMut>(
        &mut self,
        buffers: Buffers,
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = (OperationResult<SocketRecvResult>, Buffers)> + Send {
        self.inner.read_owned_vectored(buffers, flags, cancellation)
    }
    async fn close(&mut self) -> io::Result<()> {
        self.plan.reader_closes.fetch_add(1, Ordering::SeqCst);
        self.inner.close().await?;
        if self.plan.reader_close.load(Ordering::SeqCst) {
            return Err(injected("reader close"));
        }
        Ok(())
    }
}
struct FaultWriter<W> {
    inner: W,
    plan: Arc<Plan>,
}
impl<W: SocketStreamWriter> SocketStreamWriter for FaultWriter<W> {
    type Buffer = W::Buffer;
    fn write(
        &mut self,
        buffer: &[u8],
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<usize>> + Send {
        self.inner.write(buffer, cancellation)
    }
    fn write_vectored(
        &mut self,
        buffers: &[IoSlice<'_>],
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<usize>> + Send {
        self.inner.write_vectored(buffers, cancellation)
    }
    fn submit(
        &mut self,
        buffer: Self::Buffer,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<(), (OperationError, Self::Buffer)>> + Send {
        self.inner.submit(buffer, cancellation)
    }
    fn flush(
        &mut self,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send {
        self.inner.flush(cancellation)
    }
    fn write_vectored_with_flags(
        &mut self,
        buffers: &[IoSlice<'_>],
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<usize>> + Send {
        self.inner
            .write_vectored_with_flags(buffers, flags, cancellation)
    }
    fn write_owned_vectored<Buffers: IoBufSequence>(
        &mut self,
        buffers: Buffers,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = (OperationResult<usize>, Buffers)> + Send {
        self.inner
            .write_owned_vectored(buffers, flags, cancellation)
    }
    async fn close(&mut self) -> io::Result<()> {
        self.plan.writer_closes.fetch_add(1, Ordering::SeqCst);
        self.inner.close().await?;
        if self.plan.writer_close.load(Ordering::SeqCst) {
            return Err(injected("writer close"));
        }
        Ok(())
    }
}

#[test]
fn failed_stream_setup_closes_the_socket_and_releases_construction() {
    for writer in [false, true] {
        let (address, peer) = peer(|listener| {
            // The failed attempt connects, then closes without sending a request.
            assert!(remaining(&mut accept(&listener)).is_empty());
            respond_no_content(&listener);
        });
        let scheduler = DirectScheduler::new(Default::default()).unwrap();
        let faults = Faults::new(&scheduler);
        let plan = faults.plan.clone();
        let fault = if writer { &plan.writer } else { &plan.reader };
        fault.store(true, Ordering::SeqCst);
        let result = drive(&faults, address, &Cancellation::new(), async |client| {
            let error = client
                .call(
                    Request::new(Method::GET, "/", EmptyBody),
                    &Cancellation::never(),
                )
                .await
                .err()
                .unwrap();
            assert!(matches!(error, Error::Io(_)), "{error:?}");
            let expected = if writer { "writer" } else { "reader" };
            assert_eq!(source(&error), format!("injected {expected} failure"));
            // A reader created before the writer failed is closed, not leaked.
            assert_eq!(
                plan.reader_closes.load(Ordering::SeqCst),
                usize::from(writer)
            );
            assert_eq!(client.pool_stats().total(), 0);
            fault.store(false, Ordering::SeqCst);
            next_call(&client).await;
            client.begin_close();
        });
        result.unwrap();
        peer.join().unwrap();
        block_on(scheduler.shutdown());
    }
}

#[test]
fn cleanup_failures_are_reported_by_the_driver_after_closing_everything() {
    for fault in ["socket close", "reader close", "writer close"] {
        let (address, peer) = peer(|listener| respond_no_content(&listener));
        let scheduler = DirectScheduler::new(Default::default()).unwrap();
        let faults = Faults::new(&scheduler);
        let plan = faults.plan.clone();
        match fault {
            "socket close" => &plan.socket_close,
            "reader close" => &plan.reader_close,
            _ => &plan.writer_close,
        }
        .store(true, Ordering::SeqCst);
        let result = drive(&faults, address, &Cancellation::new(), async |client| {
            next_call(&client).await;
            client.begin_close();
        });
        let error = result.unwrap_err();
        assert!(matches!(error, Error::Io(_)), "{error:?}");
        assert_eq!(source(&error), format!("injected {fault} failure"));
        // Each direction is closed once even when the other one fails.
        assert_eq!(plan.reader_closes.load(Ordering::SeqCst), 1);
        assert_eq!(plan.writer_closes.load(Ordering::SeqCst), 1);
        peer.join().unwrap();
        block_on(scheduler.shutdown());
    }
}

#[test]
fn cancellation_during_and_after_tcp_connect_releases_construction() {
    for after in [false, true] {
        let (address, peer) = peer(move |listener| {
            if after {
                // The connected but unpublished transport is closed without a request.
                assert!(remaining(&mut accept(&listener)).is_empty());
            }
            respond_no_content(&listener);
        });
        let scheduler = DirectScheduler::new(Default::default()).unwrap();
        let faults = Faults::new(&scheduler);
        let (reached, release) = faults.plan.gate(after);
        let result = drive(&faults, address, &Cancellation::new(), async |client| {
            let cancel = Cancellation::new();
            let mut call = pin!(client.call(Request::new(Method::GET, "/", EmptyBody), &cancel));
            assert!(poll_once(call.as_mut()).await.is_none());
            reached.await.unwrap();
            assert_eq!(client.pool_stats().building, 1);
            cancel.cancel();
            assert!(matches!(call.await, Err(Error::Cancelled)));
            release.send(()).unwrap();
            wait_until(|| client.pool_stats().total() == 0).await;
            next_call(&client).await;
            client.begin_close();
        });
        result.unwrap();
        peer.join().unwrap();
        block_on(scheduler.shutdown());
    }
}

#[test]
fn driver_shutdown_during_establishment_retires_a_late_connection() {
    // A clean close of the late connection is not a driver failure; its close errors still are.
    for fault in [None, Some("reader close"), Some("writer close")] {
        let (address, peer) = peer(|listener| {
            assert!(remaining(&mut accept(&listener)).is_empty());
        });
        let scheduler = DirectScheduler::new(Default::default()).unwrap();
        let faults = Faults::new(&scheduler);
        let plan = faults.plan.clone();
        match fault {
            Some("reader close") => plan.reader_close.store(true, Ordering::SeqCst),
            Some(_) => plan.writer_close.store(true, Ordering::SeqCst),
            None => {}
        }
        let (reached, release) = plan.gate(true);
        let lifetime = Cancellation::new();
        let result = drive(&faults, address, &lifetime, async |client| {
            let never = Cancellation::never();
            let mut call = pin!(client.call(Request::new(Method::GET, "/", EmptyBody), &never));
            assert!(poll_once(call.as_mut()).await.is_none());
            reached.await.unwrap();
            // Construction resumes in the same supervisor poll that closes the pool, before the waiting call observes shutdown.
            release.send(()).unwrap();
            lifetime.cancel();
            let error = call.await.err().unwrap();
            assert!(matches!(error, Error::Closed), "{error:?}");
        });
        match fault {
            Some(fault) => {
                let error = result.unwrap_err();
                assert_eq!(source(&error), format!("injected {fault} failure"));
            }
            None => result.unwrap(),
        }
        // Both directions are closed even when one fails.
        assert_eq!(plan.reader_closes.load(Ordering::SeqCst), 1);
        assert_eq!(plan.writer_closes.load(Ordering::SeqCst), 1);
        peer.join().unwrap();
        block_on(scheduler.shutdown());
    }
}

#[test]
fn driver_cancellation_waits_for_direct_handles_to_return_their_codecs() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        assert!(head(&mut stream).starts_with("POST / HTTP/1.1\r\n"));
        assert_eq!(remaining(&mut stream), b"5\r\nhello\r\n");
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    let lifetime = Cancellation::new();
    let result = drive(&scheduler, address, &lifetime, async |client| {
        let never = Cancellation::never();
        let (mut output, response) = client
            .open(Method::POST, "/", &never)
            .await
            .unwrap()
            .split();
        output.write(b"hello", &never).await.unwrap();
        lifetime.cancel();
        // Retirement precedes closing; the session now waits for both directions.
        wait_until(|| client.pool_stats().draining == 1).await;
        drop(output);
        drop(response);
    });
    result.unwrap();
    peer.join().unwrap();
    block_on(scheduler.shutdown());
}

#[test]
fn dropping_the_driver_discards_deferred_finalization() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        assert!(head(&mut stream).starts_with("POST / HTTP/1.1\r\n"));
        // The final chunk is never sent once the supervisor is gone.
        assert_eq!(remaining(&mut stream), b"5\r\nhello\r\n");
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    block_on(async {
        let (client, driver) =
            Client::new(&scheduler, Endpoint::new(address), Options::default()).unwrap();
        let never = Cancellation::never();
        let opened = or(
            async {
                driver.run(&never).await.unwrap();
                unreachable!("the driver runs until the request is open")
            },
            async {
                let (mut output, response) = client
                    .open(Method::POST, "/", &never)
                    .await
                    .unwrap()
                    .split();
                output.write(b"hello", &never).await.unwrap();
                (output, response)
            },
        );
        // Returning from `or` drops the driver.
        let (mut output, response) = opened.await;
        output.close_write(None);
        let error = output.finish(&never).await.unwrap_err();
        assert!(matches!(error, Error::Abandoned), "{error:?}");
        assert!(response.await.is_err());
        drop(client);
        scheduler.shutdown().await;
    });
    peer.join().unwrap();
}

struct Failing(bool);
impl ReadableBody for Failing {
    type Data = Vec<u8>;
    type Error = io::Error;
    fn close(&mut self, _: Option<Self::Error>) {}
    async fn read(&mut self, _: &Cancellation) -> Result<BodyFrame<Vec<u8>>, Self::Error> {
        if std::mem::replace(&mut self.0, true) {
            return Err(io::Error::other("upload failed"));
        }
        Ok(BodyFrame::Data {
            data: b"hello".to_vec(),
            end: false,
        })
    }
}

#[test]
fn request_body_failure_before_a_response_fails_the_call_with_that_error() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        assert!(head(&mut stream).contains("chunked"));
        // The connection is retired with the request unfinished.
        assert_eq!(remaining(&mut stream), b"5\r\nhello\r\n");
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    let result = drive(&scheduler, address, &Cancellation::new(), async |client| {
        let error = client
            .call(
                Request::new(Method::POST, "/", Failing(false)),
                &Cancellation::never(),
            )
            .await
            .err()
            .unwrap();
        // The request codec reports the upload failure it observed.
        assert!(matches!(error, Error::Protocol(_)), "{error:?}");
        assert!(format!("{error:?}").contains("upload failed"), "{error:?}");
        client.begin_close();
    });
    result.unwrap();
    peer.join().unwrap();
    block_on(scheduler.shutdown());
}

struct Stalled;
impl ReadableBody for Stalled {
    type Data = Vec<u8>;
    type Error = std::convert::Infallible;
    fn close(&mut self, _: Option<Self::Error>) {}
    async fn read(&mut self, _: &Cancellation) -> Result<BodyFrame<Vec<u8>>, Self::Error> {
        std::future::pending().await
    }
}

#[test]
fn cancelling_during_upload_and_response_wait_retires_the_connection() {
    let (sent, request) = oneshot::channel();
    let (address, peer) = peer(move |listener| {
        let mut stream = accept(&listener);
        assert!(head(&mut stream).starts_with("POST / HTTP/1.1\r\n"));
        sent.send(()).unwrap();
        assert!(remaining(&mut stream).is_empty());
        respond_no_content(&listener);
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    let result = drive(&scheduler, address, &Cancellation::new(), async |client| {
        let cancel = Cancellation::new();
        let mut call = pin!(client.call(Request::new(Method::POST, "/", Stalled), &cancel));
        assert!(poll_once(call.as_mut()).await.is_none());
        request.await.unwrap();
        cancel.cancel();
        assert!(matches!(call.await, Err(Error::Cancelled)));
        next_call(&client).await;
        client.begin_close();
    });
    result.unwrap();
    peer.join().unwrap();
    block_on(scheduler.shutdown());
}

#[test]
fn malformed_response_fails_the_call_and_retires_the_connection() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        stream.get_mut().write_all(b"NOT HTTP\r\n\r\n").unwrap();
        assert!(remaining(&mut stream).is_empty());
        respond_no_content(&listener);
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    let result = drive(&scheduler, address, &Cancellation::new(), async |client| {
        let error = client
            .call(
                Request::new(Method::GET, "/", EmptyBody),
                &Cancellation::never(),
            )
            .await
            .err()
            .unwrap();
        assert!(matches!(error, Error::Protocol(_)), "{error:?}");
        next_call(&client).await;
        client.begin_close();
    });
    result.unwrap();
    peer.join().unwrap();
    block_on(scheduler.shutdown());
}

#[test]
fn truncated_response_body_reports_an_error_to_its_reader() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc")
            .unwrap();
        drop(stream);
        respond_no_content(&listener);
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    let result = drive(&scheduler, address, &Cancellation::new(), async |client| {
        let mut response = client
            .call(
                Request::new(Method::GET, "/", EmptyBody),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        assert!(payload(&mut response.body).await.is_err());
        drop(response);
        next_call(&client).await;
        client.begin_close();
    });
    result.unwrap();
    peer.join().unwrap();
    block_on(scheduler.shutdown());
}

// Run the driver on another thread, so the application can act while a driver poll is held inside a read.
fn drive_threaded(
    faults: &Faults<'_>,
    address: SocketAddr,
    application: impl for<'scope> FnOnce(&Client<'scope>),
) {
    let (client, driver) = Client::new(faults, Endpoint::new(address), Options::default()).unwrap();
    let lifetime = Cancellation::new();
    thread::scope(|scope| {
        let driving = scope.spawn(|| block_on(driver.run(&lifetime)));
        application(&client);
        client.begin_close();
        driving.join().unwrap().unwrap();
    });
    assert_eq!(client.pool_stats().total(), 0);
}

#[test]
fn call_dropped_as_its_response_arrives_retires_the_connection() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
            .unwrap();
        assert!(remaining(&mut stream).is_empty());
        respond_no_content(&listener);
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    let faults = Faults::new(&scheduler);
    let (reached, proceed) = faults.plan.pause_read();
    drive_threaded(&faults, address, |client| {
        block_on(async {
            let never = Cancellation::never();
            let mut call = Box::pin(client.call(Request::new(Method::GET, "/", EmptyBody), &never));
            assert!(poll_once(call.as_mut()).await.is_none());
            // The driver holds the response head; abandon the call before it is delivered.
            reached.recv().unwrap();
            drop(call);
            proceed.send(()).unwrap();
            next_call(client).await;
        });
    });
    peer.join().unwrap();
    block_on(scheduler.shutdown());
}

#[test]
fn response_dropped_while_its_body_arrives_retires_the_connection() {
    let (send_body, body_requested) = mpsc::channel::<()>();
    let (address, peer) = peer(move |listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n")
            .unwrap();
        body_requested.recv().unwrap();
        stream.get_mut().write_all(b"hello").unwrap();
        assert!(remaining(&mut stream).is_empty());
        respond_no_content(&listener);
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    let faults = Faults::new(&scheduler);
    let plan = faults.plan.clone();
    drive_threaded(&faults, address, |client| {
        block_on(async {
            let response = client
                .call(
                    Request::new(Method::GET, "/", EmptyBody),
                    &Cancellation::never(),
                )
                .await
                .unwrap();
            let (reached, proceed) = plan.pause_read();
            send_body.send(()).unwrap();
            // The driver holds body bytes; abandon the response before they are forwarded.
            reached.recv().unwrap();
            drop(response);
            proceed.send(()).unwrap();
            next_call(client).await;
        });
    });
    peer.join().unwrap();
    block_on(scheduler.shutdown());
}

#[cfg(feature = "rustls")]
mod tls {
    use super::*;
    use rustls::{ClientConfig, RootCertStore};

    fn client_config() -> Arc<ClientConfig> {
        let identity = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let mut roots = RootCertStore::empty();
        roots.add(identity.cert.der().clone()).unwrap();
        Arc::new(
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        )
    }

    #[test]
    fn cancelled_tls_handshakes_close_the_transport_and_release_construction() {
        for driver_cancelled in [false, true] {
            let (hello, received) = oneshot::channel();
            let (address, peer) = peer(move |listener| {
                let mut stream = accept(&listener);
                // Receive the client hello and never answer it.
                let mut bytes = [0; 1024];
                assert_ne!(stream.get_mut().read(&mut bytes).unwrap(), 0);
                hello.send(()).unwrap();
                match stream.get_mut().read_to_end(&mut Vec::new()) {
                    Ok(_) => {}
                    Err(error) => assert_eq!(error.kind(), io::ErrorKind::ConnectionReset),
                }
            });
            let scheduler = DirectScheduler::new(Default::default()).unwrap();
            let lifetime = Cancellation::new();
            let endpoint = Endpoint::new(address).with_tls(client_config(), "localhost");
            let (client, driver) = Client::new(&scheduler, endpoint, Options::default()).unwrap();
            let application = async {
                let cancel = Cancellation::new();
                let mut call =
                    pin!(client.call(Request::new(Method::GET, "/", EmptyBody), &cancel));
                assert!(poll_once(call.as_mut()).await.is_none());
                received.await.unwrap();
                if driver_cancelled {
                    lifetime.cancel();
                } else {
                    cancel.cancel();
                }
                assert!(matches!(call.await, Err(Error::Cancelled)));
                wait_until(|| client.pool_stats().total() == 0).await;
                client.begin_close();
            };
            let (result, ()) = block_on(zip(driver.run(&lifetime), application));
            result.unwrap();
            assert_eq!(client.pool_stats().total(), 0);
            peer.join().unwrap();
            block_on(scheduler.shutdown());
        }
    }
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

// A scripted in-memory scheduler serves one connection whose transport capabilities can fail deterministically. The input script is followed by EOF, so every exchange completes without timing assumptions.
use base_http::{
    InterimHandler, InterimResponses, Request, Response, Status,
    body::{BufferedBody, ReadableBody},
};
use base_http_server::{Error, Handler};
use base_scheduler::{
    BufferLease, BufferPoolOptions, BufferPooling, Cancellation, Cancelled, HeapBufferPool, IoBuf,
    IoBufSequence, IoBufSequenceMut, OperationError, OperationResult, SocketAccept, SocketAddress,
    SocketClose, SocketOperation, SocketRecvFlags, SocketRecvResult, SocketSendFlags,
    SocketSetOption, SocketStreamReader, SocketStreamReading, SocketStreamWriter,
    SocketStreamWriting, TcpNoDelay,
};
use futures_lite::future::block_on;
use std::{
    collections::VecDeque,
    error::Error as _,
    io::{self, IoSlice, IoSliceMut},
    marker::PhantomData,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

const REQUEST: &[u8] = b"GET / HTTP/1.1\r\nHost: example\r\nConnection: close\r\n\r\n";

#[derive(Default)]
struct Faults {
    reader_unavailable: bool,
    writer_unavailable: bool,
    reader_close_error: bool,
    writer_close_error: bool,
    socket_close_error: bool,
    cancel_on_read: Option<Cancellation>,
}

#[derive(Default)]
struct Memory {
    input: Mutex<VecDeque<u8>>,
    output: Mutex<Vec<u8>>,
    faults: Mutex<Faults>,
    reader_closes: AtomicUsize,
    writer_closes: AtomicUsize,
    socket_closes: AtomicUsize,
    // Accept one connection, then wait for cancellation, which closing that connection requests.
    accept: Mutex<Option<Cancellation>>,
    closing: Mutex<Option<Cancellation>>,
    nodelay_attempts: AtomicUsize,
}

impl Memory {
    fn new(input: &[u8]) -> Self {
        Self {
            input: Mutex::new(input.iter().copied().collect()),
            ..Self::default()
        }
    }

    fn faults(&self) -> std::sync::MutexGuard<'_, Faults> {
        self.faults.lock().unwrap()
    }

    fn output(&self) -> String {
        String::from_utf8(self.output.lock().unwrap().clone()).unwrap()
    }

    fn closes(&self) -> (usize, usize, usize) {
        (
            self.reader_closes.load(Ordering::SeqCst),
            self.writer_closes.load(Ordering::SeqCst),
            self.socket_closes.load(Ordering::SeqCst),
        )
    }
}

struct Reader<'a>(&'a Memory);

impl SocketStreamReader for Reader<'_> {
    type Buffer = BufferLease;

    async fn read(&mut self, _: &Cancellation) -> OperationResult<Option<BufferLease>> {
        unreachable!("HTTP/1 uses borrowed reads")
    }

    async fn read_into(
        &mut self,
        buffer: &mut [u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        cancellation.check()?;
        let mut input = self.0.input.lock().unwrap();
        let count = buffer.len().min(input.len());
        for (target, byte) in buffer.iter_mut().zip(input.drain(..count)) {
            *target = byte;
        }
        // Successful progress wins a cancellation requested meanwhile.
        if let Some(cancel) = self.0.faults().cancel_on_read.take() {
            cancel.cancel();
        }
        Ok(count)
    }

    async fn read_vectored(
        &mut self,
        _: &mut [IoSliceMut<'_>],
        _: &Cancellation,
    ) -> OperationResult<usize> {
        unreachable!("HTTP/1 uses borrowed reads")
    }

    async fn read_vectored_with_flags(
        &mut self,
        _: &mut [IoSliceMut<'_>],
        _: SocketRecvFlags,
        _: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        unreachable!("HTTP/1 uses borrowed reads")
    }

    async fn read_owned_vectored<B: IoBufSequenceMut>(
        &mut self,
        _: B,
        _: SocketRecvFlags,
        _: &Cancellation,
    ) -> (OperationResult<SocketRecvResult>, B) {
        unreachable!("HTTP/1 uses borrowed reads")
    }

    async fn close(&mut self) -> io::Result<()> {
        self.0.reader_closes.fetch_add(1, Ordering::SeqCst);
        if self.0.faults().reader_close_error {
            Err(io::Error::other("reader retirement failed"))
        } else {
            Ok(())
        }
    }
}

struct Writer<'a, B> {
    memory: &'a Memory,
    buffer: PhantomData<fn() -> B>,
}

impl<B: IoBuf> SocketStreamWriter for Writer<'_, B> {
    type Buffer = B;

    async fn write(
        &mut self,
        buffer: &[u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        cancellation.check()?;
        self.memory.output.lock().unwrap().extend_from_slice(buffer);
        Ok(buffer.len())
    }

    async fn write_vectored(
        &mut self,
        _: &[IoSlice<'_>],
        _: &Cancellation,
    ) -> OperationResult<usize> {
        unreachable!("HTTP/1 uses borrowed writes")
    }

    async fn submit(&mut self, _: B, _: &Cancellation) -> Result<(), (OperationError, B)> {
        unreachable!("HTTP/1 uses borrowed writes")
    }

    async fn flush(&mut self, cancellation: &Cancellation) -> OperationResult<()> {
        cancellation.check()?;
        Ok(())
    }

    async fn write_vectored_with_flags(
        &mut self,
        _: &[IoSlice<'_>],
        _: SocketSendFlags,
        _: &Cancellation,
    ) -> OperationResult<usize> {
        unreachable!("HTTP/1 uses borrowed writes")
    }

    async fn write_owned_vectored<S: IoBufSequence>(
        &mut self,
        _: S,
        _: SocketSendFlags,
        _: &Cancellation,
    ) -> (OperationResult<usize>, S) {
        unreachable!("HTTP/1 uses borrowed writes")
    }

    async fn close(&mut self) -> io::Result<()> {
        self.memory.writer_closes.fetch_add(1, Ordering::SeqCst);
        if self.memory.faults().writer_close_error {
            Err(io::Error::other("writer retirement failed"))
        } else {
            Ok(())
        }
    }
}

impl SocketOperation for Memory {
    type Socket = ();
}

impl BufferPooling for Memory {
    type Pool = HeapBufferPool;

    fn buffer_pool(&self, options: BufferPoolOptions) -> io::Result<HeapBufferPool> {
        HeapBufferPool::with_options(options)
    }
}

impl SocketStreamReading for Memory {
    fn socket_stream_reader<'scheduler>(
        &'scheduler self,
        _: &(),
    ) -> io::Result<impl SocketStreamReader<Buffer = BufferLease> + 'scheduler + use<'scheduler>>
    {
        if self.faults().reader_unavailable {
            return Err(io::Error::other("reader unavailable"));
        }
        Ok(Reader(self))
    }

    fn socket_stream_reader_with_pool<'scheduler>(
        &'scheduler self,
        socket: &(),
        _: HeapBufferPool,
    ) -> io::Result<impl SocketStreamReader<Buffer = BufferLease> + 'scheduler + use<'scheduler>>
    {
        self.socket_stream_reader(socket)
    }
}

impl SocketStreamWriting for Memory {
    fn socket_stream_writer<'scheduler>(
        &'scheduler self,
        socket: &(),
    ) -> io::Result<impl SocketStreamWriter<Buffer = BufferLease> + 'scheduler + use<'scheduler>>
    {
        self.socket_stream_writer_with_buffer(socket)
    }

    fn socket_stream_writer_with_buffer<'scheduler, B: IoBuf>(
        &'scheduler self,
        _: &(),
    ) -> io::Result<impl SocketStreamWriter<Buffer = B> + 'scheduler + use<'scheduler, B>> {
        if self.faults().writer_unavailable {
            return Err(io::Error::other("writer unavailable"));
        }
        Ok(Writer {
            memory: self,
            buffer: PhantomData,
        })
    }
}

impl SocketAccept for Memory {
    async fn socket_accept(
        &self,
        _: &(),
        cancellation: &Cancellation,
    ) -> OperationResult<((), SocketAddress)> {
        if let Some(closing) = self.accept.lock().unwrap().take() {
            *self.closing.lock().unwrap() = Some(closing);
            return Ok(((), "127.0.0.1:1".parse().unwrap()));
        }
        cancellation.cancelled().await;
        Err(Cancelled.into())
    }
}

impl SocketSetOption<TcpNoDelay> for Memory {
    async fn socket_set_option(
        &self,
        _: &(),
        _: TcpNoDelay,
        _: bool,
        _: &Cancellation,
    ) -> OperationResult<()> {
        self.nodelay_attempts.fetch_add(1, Ordering::SeqCst);
        Err(io::Error::from(io::ErrorKind::Unsupported).into())
    }
}

impl SocketClose for Memory {
    async fn socket_close(&self, _: ()) -> io::Result<()> {
        self.socket_closes.fetch_add(1, Ordering::SeqCst);
        if let Some(closing) = self.closing.lock().unwrap().take() {
            closing.cancel();
        }
        if self.faults().socket_close_error {
            Err(io::Error::other("socket close failed"))
        } else {
            Ok(())
        }
    }

    async fn socket_close_submit(&self, socket: ()) -> io::Result<()> {
        self.socket_close(socket).await
    }
}

#[derive(Default)]
struct Hello {
    called: AtomicBool,
}
impl Handler for Hello {
    type Error = io::Error;
    type Body<B: ReadableBody> = BufferedBody;
    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        _: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<Response<BufferedBody>, io::Error> {
        self.called.store(true, Ordering::SeqCst);
        Ok(Response::new(
            Status::OK,
            BufferedBody::from(b"hello".to_vec()),
        ))
    }
}

fn serve(
    memory: &Memory,
    handler: &impl Handler,
    options: &base_http1::Options,
    cancellation: &Cancellation,
) -> Result<u64, Error> {
    block_on(base_http_server::serve_connection(
        memory,
        (),
        handler,
        options,
        cancellation,
    ))
}

#[test]
fn never_cancelled_connection_serves_and_retires_every_capability() {
    let memory = Memory::new(REQUEST);
    let served = serve(
        &memory,
        &Hello::default(),
        &Default::default(),
        &Cancellation::never(),
    );
    assert_eq!(served.unwrap(), 1);
    let output = memory.output();
    assert!(output.starts_with("HTTP/1.1 200 "), "{output:?}");
    assert!(output.ends_with("\r\n\r\nhello"), "{output:?}");
    assert_eq!(memory.closes(), (1, 1, 1));
}

#[test]
fn invalid_options_close_the_socket_without_constructing_streams() {
    let memory = Memory::new(REQUEST);
    let handler = Hello::default();
    let options = base_http1::Options {
        read_size: 0,
        ..Default::default()
    };
    let served = serve(&memory, &handler, &options, &Cancellation::new());
    assert!(matches!(
        served,
        Err(Error::Protocol(base_http1::Error::Invalid(_)))
    ));
    assert!(!handler.called.load(Ordering::SeqCst));
    assert_eq!(memory.output(), "");
    assert_eq!(memory.closes(), (0, 0, 1));
}

#[test]
fn unavailable_reader_is_reported_and_the_socket_closed() {
    let memory = Memory::new(REQUEST);
    memory.faults().reader_unavailable = true;
    let served = serve(
        &memory,
        &Hello::default(),
        &Default::default(),
        &Cancellation::new(),
    );
    let error = served.unwrap_err();
    assert!(matches!(error, Error::Io(_)));
    assert_eq!(error.source().unwrap().to_string(), "reader unavailable");
    assert_eq!(memory.closes(), (0, 0, 1));
}

#[test]
fn unavailable_writer_retires_the_reader_and_closes_the_socket() {
    let memory = Memory::new(REQUEST);
    memory.faults().writer_unavailable = true;
    let served = serve(
        &memory,
        &Hello::default(),
        &Default::default(),
        &Cancellation::new(),
    );
    let error = served.unwrap_err();
    assert!(matches!(error, Error::Io(_)));
    assert_eq!(error.source().unwrap().to_string(), "writer unavailable");
    assert_eq!(memory.closes(), (1, 0, 1));
}

#[test]
fn unavailable_writer_is_reported_in_preference_to_reader_retirement_failure() {
    let memory = Memory::new(REQUEST);
    memory.faults().writer_unavailable = true;
    memory.faults().reader_close_error = true;
    let served = serve(
        &memory,
        &Hello::default(),
        &Default::default(),
        &Cancellation::new(),
    );
    let error = served.unwrap_err();
    assert!(matches!(error, Error::Io(_)));
    assert_eq!(error.source().unwrap().to_string(), "writer unavailable");
    assert_eq!(memory.closes(), (1, 0, 1));
}

#[test]
fn retirement_failures_are_reported_after_a_complete_exchange() {
    type Fault = fn(&mut Faults);
    let cases: [(Fault, &str); 3] = [
        (|f| f.reader_close_error = true, "reader retirement failed"),
        (|f| f.writer_close_error = true, "writer retirement failed"),
        (|f| f.socket_close_error = true, "socket close failed"),
    ];
    for (inject, message) in cases {
        let memory = Memory::new(REQUEST);
        inject(&mut memory.faults());
        let served = serve(
            &memory,
            &Hello::default(),
            &Default::default(),
            &Cancellation::new(),
        );
        let error = served.unwrap_err();
        assert!(matches!(error, Error::Io(_)), "{message}");
        assert_eq!(error.source().unwrap().to_string(), message);
        // The response was delivered and every capability was still retired.
        assert!(memory.output().ends_with("hello"), "{message}");
        assert_eq!(memory.closes(), (1, 1, 1), "{message}");
    }
}

#[test]
fn cancellation_before_a_request_head_does_not_reject_the_peer() {
    let memory = Memory::new(REQUEST);
    let handler = Hello::default();
    let cancellation = Cancellation::new();
    cancellation.cancel();
    let served = serve(&memory, &handler, &Default::default(), &cancellation);
    assert!(matches!(served, Err(Error::Cancelled)));
    assert!(!handler.called.load(Ordering::SeqCst));
    assert_eq!(memory.output(), "");
    assert_eq!(memory.closes(), (1, 1, 1));
}

#[test]
fn cancellation_while_a_head_is_read_prevents_handler_admission() {
    let memory = Memory::new(REQUEST);
    let handler = Hello::default();
    let cancellation = Cancellation::new();
    memory.faults().cancel_on_read = Some(cancellation.clone());
    let served = serve(&memory, &handler, &Default::default(), &cancellation);
    assert!(matches!(served, Err(Error::Cancelled)));
    assert!(!handler.called.load(Ordering::SeqCst));
    assert_eq!(memory.output(), "");
    assert_eq!(memory.closes(), (1, 1, 1));
}

// A handler which stops the server and reports the resulting failure, as one observing a cancelled operation would.
struct Shutdown(Cancellation);
impl Handler for Shutdown {
    type Error = io::Error;
    type Body<B: ReadableBody> = BufferedBody;
    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        _: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<Response<BufferedBody>, io::Error> {
        self.0.cancel();
        Err(io::Error::other("operation cancelled"))
    }
}

#[test]
fn handler_failure_after_cancellation_is_reported_as_cancellation() {
    let memory = Memory::new(REQUEST);
    let cancellation = Cancellation::new();
    let handler = Shutdown(cancellation.clone());
    let served = serve(&memory, &handler, &Default::default(), &cancellation);
    assert!(matches!(served, Err(Error::Cancelled)));
    assert_eq!(memory.output(), "");
    assert_eq!(memory.closes(), (1, 1, 1));
}

#[test]
fn listener_close_failure_is_reported_after_cancelled_acceptance() {
    let memory = Memory::default();
    memory.faults().socket_close_error = true;
    let cancellation = Cancellation::new();
    cancellation.cancel();
    let served = block_on(base_http_server::serve(
        &memory,
        (),
        &Hello::default(),
        &Default::default(),
        &cancellation,
    ));
    let error = served.unwrap_err();
    assert!(matches!(error, Error::Io(_)));
    assert_eq!(error.source().unwrap().to_string(), "socket close failed");
    assert_eq!(memory.closes(), (0, 0, 1));
}

#[test]
fn protocol_codecs_reject_invalid_limits_at_construction() {
    let memory = Memory::default();
    let options = base_http1::Options {
        max_fields: 0,
        ..Default::default()
    };
    let protocol = base_http_protocol::HttpProtocols::default().plaintext();
    let reader = memory.socket_stream_reader(&()).unwrap();
    let writer = memory.socket_stream_writer(&()).unwrap();
    assert!(matches!(
        protocol.server(reader, writer, options.clone()),
        Err(base_http1::Error::Invalid(_))
    ));
    let reader = memory.socket_stream_reader(&()).unwrap();
    let writer = memory.socket_stream_writer(&()).unwrap();
    assert!(matches!(
        protocol.client(reader, writer, options),
        Err(base_http1::Error::Invalid(_))
    ));
}

#[test]
fn a_listener_serves_connections_that_reject_nodelay() {
    for tcp_nodelay in [true, false] {
        let memory = Memory::new(REQUEST);
        let cancellation = Cancellation::new();
        *memory.accept.lock().unwrap() = Some(cancellation.clone());
        let options = base_http_server::Options {
            tcp_nodelay,
            ..Default::default()
        };
        let statistics = block_on(base_http_server::serve(
            &memory,
            (),
            &Hello::default(),
            &options,
            &cancellation,
        ))
        .unwrap();
        assert_eq!(
            (
                statistics.connections,
                statistics.requests,
                statistics.failed_connections
            ),
            (1, 1, 0)
        );
        assert!(memory.output().ends_with("\r\n\r\nhello"));
        assert_eq!(
            memory.nodelay_attempts.load(Ordering::SeqCst),
            usize::from(tcp_nodelay)
        );
    }
}

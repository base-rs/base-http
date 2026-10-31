// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

// Real TCP connections served through a scheduler that injects scripted accept failures and records the order in which sockets close.
use base_http::{
    InterimHandler, InterimResponses, Request, Response, Status,
    body::{BufferedBody, ReadableBody},
};
use base_http_server::{Error, Handler, Options, Statistics};
use base_scheduler::{
    BufferPool, BufferPoolOptions, BufferPooling, Cancellation, Clock, IoBuf, OperationResult,
    SocketAccept, SocketAddress, SocketClose, SocketOperation, SocketPeerAddress, SocketSetOption,
    SocketStreamReader, SocketStreamReading, SocketStreamWriter, SocketStreamWriting, TcpNoDelay,
};
use base_scheduler_direct::{DirectScheduler, DirectSocket};
use futures_lite::future::block_on;
use std::{
    collections::VecDeque,
    future::Future,
    io::{self, ErrorKind, Read, Write},
    net::{SocketAddr, TcpStream},
    sync::Mutex,
    time::{Duration, Instant},
};

mod support;
use support::*;

#[derive(Debug, PartialEq)]
enum Closed {
    Listener,
    Connection,
}

struct Scripted<'a> {
    inner: &'a DirectScheduler,
    // Each accept takes the next entry: an error to report, or `None` to accept a real peer. An empty script accepts real peers.
    script: Mutex<VecDeque<Option<io::Error>>>,
    closed: Mutex<Vec<Closed>>,
}
impl<'a> Scripted<'a> {
    fn new(
        inner: &'a DirectScheduler,
        script: impl IntoIterator<Item = Option<io::Error>>,
    ) -> Self {
        Self {
            inner,
            script: Mutex::new(script.into_iter().collect()),
            closed: Mutex::default(),
        }
    }
}
impl SocketOperation for Scripted<'_> {
    type Socket = DirectSocket;
}
impl BufferPooling for Scripted<'_> {
    type Pool = <DirectScheduler as BufferPooling>::Pool;
    fn buffer_pool(&self, options: BufferPoolOptions) -> io::Result<Self::Pool> {
        self.inner.buffer_pool(options)
    }
}
impl SocketAccept for Scripted<'_> {
    async fn socket_accept(
        &self,
        socket: &DirectSocket,
        cancellation: &Cancellation,
    ) -> OperationResult<(DirectSocket, SocketAddress)> {
        cancellation.check()?;
        let next = self.script.lock().unwrap().pop_front();
        match next {
            Some(Some(error)) => Err(error.into()),
            Some(None) | None => self.inner.socket_accept(socket, cancellation).await,
        }
    }
}
impl Clock for Scripted<'_> {
    fn now(&self) -> Instant {
        self.inner.now()
    }
    fn sleep_until(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send {
        self.inner.sleep_until(deadline, cancellation)
    }
}
impl SocketSetOption<TcpNoDelay> for Scripted<'_> {
    fn socket_set_option(
        &self,
        socket: &DirectSocket,
        option: TcpNoDelay,
        value: bool,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send {
        self.inner
            .socket_set_option(socket, option, value, cancellation)
    }
}
impl SocketClose for Scripted<'_> {
    async fn socket_close(&self, socket: DirectSocket) -> io::Result<()> {
        // Only an accepted connection has a peer.
        let closed = match self.inner.socket_peer_address(&socket) {
            Ok(_) => Closed::Connection,
            Err(_) => Closed::Listener,
        };
        self.closed.lock().unwrap().push(closed);
        self.inner.socket_close(socket).await
    }
    fn socket_close_submit(
        &self,
        socket: DirectSocket,
    ) -> impl Future<Output = io::Result<()>> + Send {
        self.inner.socket_close_submit(socket)
    }
}
impl<'a> SocketStreamReading for Scripted<'a> {
    fn socket_stream_reader<'scheduler>(
        &'scheduler self,
        socket: &DirectSocket,
    ) -> io::Result<
        impl SocketStreamReader<Buffer = <Self::Pool as BufferPool>::Buffer>
        + 'scheduler
        + use<'scheduler, 'a>,
    > {
        self.inner.socket_stream_reader(socket)
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
        self.inner.socket_stream_reader_with_pool(socket, pool)
    }
}
impl<'a> SocketStreamWriting for Scripted<'a> {
    fn socket_stream_writer<'scheduler>(
        &'scheduler self,
        socket: &DirectSocket,
    ) -> io::Result<
        impl SocketStreamWriter<Buffer = <Self::Pool as BufferPool>::Buffer>
        + 'scheduler
        + use<'scheduler, 'a>,
    > {
        self.inner.socket_stream_writer(socket)
    }
    fn socket_stream_writer_with_buffer<'scheduler, B: IoBuf>(
        &'scheduler self,
        socket: &DirectSocket,
    ) -> io::Result<impl SocketStreamWriter<Buffer = B> + 'scheduler + use<'scheduler, 'a, B>> {
        self.inner.socket_stream_writer_with_buffer::<B>(socket)
    }
}

struct Hello;
impl Handler for Hello {
    type Error = io::Error;
    type Body<B: ReadableBody> = BufferedBody;
    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        _: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<Response<BufferedBody>, io::Error> {
        Ok(Response::new(
            Status::OK,
            BufferedBody::from(b"hello".to_vec()),
        ))
    }
}

#[cfg(unix)]
fn os_error(unix: i32, _windows: i32) -> io::Error {
    io::Error::from_raw_os_error(unix)
}

#[cfg(windows)]
fn os_error(_unix: i32, windows: i32) -> io::Error {
    io::Error::from_raw_os_error(windows)
}

// The process has no free descriptor for the accepted peer.
fn too_many_open_files() -> io::Error {
    #[cfg(unix)]
    let unix = libc::EMFILE;
    #[cfg(not(unix))]
    let unix = 0;
    os_error(unix, 10024)
}

// Exchange one request over a new connection, then cancel the server.
fn exchange_then_cancel(address: SocketAddr, cancellation: Cancellation) -> Vec<u8> {
    let mut peer = connect(address);
    peer.get_mut()
        .write_all(b"GET / HTTP/1.1\r\nHost: example\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut response = Vec::new();
    let read = peer.read_to_end(&mut response);
    cancellation.cancel();
    read.unwrap();
    response
}

// Serve the listener through the script while `peer` runs on another thread, returning the result, the peer's result, the socket close order and whether the port then refuses connections.
fn serve_scripted<T: Send + 'static>(
    script: impl IntoIterator<Item = Option<io::Error>>,
    peer: impl FnOnce(SocketAddr, Cancellation) -> T + Send + 'static,
) -> (Result<Statistics, Error>, T, Vec<Closed>, bool) {
    let scheduler = scheduler();
    let scripted = Scripted::new(&scheduler, script);
    let result = block_on(async {
        let (listener, address) = listener(&scheduler).await;
        let cancellation = Cancellation::new();
        let peer = {
            let cancellation = cancellation.clone();
            std::thread::spawn(move || peer(address, cancellation))
        };
        let served = base_http_server::serve(
            &scripted,
            listener,
            &Hello,
            &Options::default(),
            &cancellation,
        )
        .await;
        let peer = peer.join().unwrap();
        let refused = TcpStream::connect(address)
            .is_err_and(|error| error.kind() == ErrorKind::ConnectionRefused);
        (served, peer, refused)
    });
    block_on(scheduler.shutdown());
    let (served, peer, refused) = result;
    (served, peer, scripted.closed.into_inner().unwrap(), refused)
}

#[test]
fn transient_accept_errors_are_survived_and_counted() {
    #[cfg(unix)]
    let protocol = io::Error::from_raw_os_error(libc::EPROTO);
    #[cfg(not(unix))]
    let protocol = io::Error::from(ErrorKind::Interrupted);
    let started = Instant::now();
    let (served, response, closed, refused) = serve_scripted(
        [
            Some(io::Error::from(ErrorKind::ConnectionAborted)),
            Some(too_many_open_files()),
            Some(io::Error::from(ErrorKind::ConnectionReset)),
            Some(protocol),
        ],
        exchange_then_cancel,
    );
    let statistics = served.unwrap();
    assert!(response.ends_with(b"\r\n\r\nhello"));
    assert_eq!(statistics.connections, 1);
    assert_eq!(statistics.requests, 1);
    assert_eq!(statistics.failed_connections, 0);
    assert_eq!(statistics.accept_errors, 4);
    // Descriptor exhaustion paused acceptance rather than retrying at once.
    assert!(started.elapsed() >= Duration::from_millis(100));
    assert_eq!(closed, [Closed::Connection, Closed::Listener]);
    assert!(refused);
}

#[test]
fn connections_progress_and_cancellation_interrupts_an_accept_backoff() {
    // After the first peer, every accept reports exhaustion for far longer than the test runs.
    let script = std::iter::once(None).chain((0..1000).map(|_| Some(too_many_open_files())));
    let started = Instant::now();
    let (served, response, closed, refused) = serve_scripted(script, exchange_then_cancel);
    let statistics = served.unwrap();
    assert!(response.ends_with(b"\r\n\r\nhello"));
    assert_eq!((statistics.connections, statistics.requests), (1, 1));
    assert!(statistics.accept_errors >= 1);
    assert!(started.elapsed() < Duration::from_secs(10));
    // The connection completed before cancellation, so it closed first here.
    assert_eq!(closed, [Closed::Connection, Closed::Listener]);
    assert!(refused);
}

#[test]
fn a_fatal_accept_error_closes_the_listener_before_draining_connections() {
    let (served, response, closed, refused) = serve_scripted(
        [None, Some(io::Error::from(ErrorKind::InvalidInput))],
        |address, _| {
            // Hold an idle connection open until the server drains it.
            let mut peer = connect(address);
            let mut response = Vec::new();
            peer.read_to_end(&mut response).unwrap();
            response
        },
    );
    let error = served.unwrap_err();
    assert_eq!(error.to_string(), "HTTP/1 connection I/O failed");
    let source = std::error::Error::source(&error).unwrap();
    assert_eq!(source.to_string(), "invalid input parameter");
    assert!(response.is_empty());
    assert_eq!(closed, [Closed::Listener, Closed::Connection]);
    assert!(refused);
}

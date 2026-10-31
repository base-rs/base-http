// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![cfg(any(unix, windows))]

// Real TCP connections served through a scheduler that injects scripted accept failures and records the order in which sockets close.
use base_http::{
    InterimHandler, InterimResponses, Request, Response, Status,
    body::{BufferedBody, ReadableBody},
};
use base_http_server::{Error, Handler, Options, Statistics};
use base_scheduler::{
    BufferPool, BufferPoolOptions, BufferPooling, Cancellation, Clock, IoBuf, OperationError,
    OperationResult, SocketAccept, SocketAddress, SocketClose, SocketOperation, SocketPeerAddress,
    SocketSetOption, SocketStreamReader, SocketStreamReading, SocketStreamWriter,
    SocketStreamWriting, TcpNoDelay,
};
use base_scheduler_direct::{DirectScheduler, DirectSocket};
use futures_lite::future::block_on;
use std::{
    collections::VecDeque,
    future::Future,
    io::{self, ErrorKind, Read, Write},
    net::{SocketAddr, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
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
    // Set when a backoff wait starts, so a peer can act during it.
    sleeping: Arc<AtomicBool>,
    // Whether each finished backoff wait was cancelled.
    sleeps: Mutex<Vec<bool>>,
    // Report a failure after closing the listener.
    fail_listener_close: bool,
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
    async fn sleep_until(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.sleeping.store(true, Ordering::SeqCst);
        let slept = self.inner.sleep_until(deadline, cancellation).await;
        let cancelled = matches!(slept, Err(OperationError::Cancelled(_)));
        self.sleeps.lock().unwrap().push(cancelled);
        slept
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
        let listener = closed == Closed::Listener;
        self.closed.lock().unwrap().push(closed);
        self.inner.socket_close(socket).await?;
        if listener && self.fail_listener_close {
            return Err(io::Error::other("listener close failed"));
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

struct Served<T> {
    result: Result<Statistics, Error>,
    peer: T,
    closed: Vec<Closed>,
    sleeps: Vec<bool>,
    // Whether the port refused connections after serving.
    refused: bool,
}

// Serve the listener through the script while `peer` runs on another thread with the server's address, its cancellation and a flag set once a backoff wait starts.
fn serve_scripted<T: Send + 'static>(
    script: impl IntoIterator<Item = Option<io::Error>>,
    fail_listener_close: bool,
    peer: impl FnOnce(SocketAddr, Cancellation, Arc<AtomicBool>) -> T + Send + 'static,
) -> Served<T> {
    let scheduler = scheduler();
    let sleeping = Arc::new(AtomicBool::new(false));
    let scripted = Scripted {
        inner: &scheduler,
        script: Mutex::new(script.into_iter().collect()),
        closed: Mutex::default(),
        sleeping: sleeping.clone(),
        sleeps: Mutex::default(),
        fail_listener_close,
    };
    let (result, peer, refused) = block_on(async {
        let (listener, address) = listener(&scheduler).await;
        let cancellation = Cancellation::new();
        let peer = {
            let cancellation = cancellation.clone();
            std::thread::spawn(move || peer(address, cancellation, sleeping))
        };
        let result = base_http_server::serve(
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
        (result, peer, refused)
    });
    block_on(scheduler.shutdown());
    Served {
        result,
        peer,
        closed: scripted.closed.into_inner().unwrap(),
        sleeps: scripted.sleeps.into_inner().unwrap(),
        refused,
    }
}

// Exchange one request over a new connection, then cancel the server.
fn exchange(address: SocketAddr, cancellation: Cancellation, _: Arc<AtomicBool>) -> Vec<u8> {
    exchange_then_cancel(address, cancellation)
}

#[test]
fn transient_accept_errors_are_survived_and_counted() {
    #[cfg(unix)]
    let protocol = io::Error::from_raw_os_error(libc::EPROTO);
    #[cfg(not(unix))]
    let protocol = io::Error::from(ErrorKind::Interrupted);
    let started = Instant::now();
    let served = serve_scripted(
        [
            Some(io::Error::from(ErrorKind::ConnectionAborted)),
            Some(too_many_open_files()),
            Some(io::Error::from(ErrorKind::ConnectionReset)),
            Some(protocol),
        ],
        false,
        exchange,
    );
    let statistics = served.result.unwrap();
    assert!(served.peer.ends_with(b"\r\n\r\nhello"));
    assert_eq!(statistics.connections, 1);
    assert_eq!(statistics.requests, 1);
    assert_eq!(statistics.failed_connections, 0);
    assert_eq!(statistics.accept_errors, 4);
    // Only descriptor exhaustion paused acceptance, for the whole backoff.
    assert_eq!(served.sleeps, [false]);
    assert!(started.elapsed() >= Duration::from_millis(100));
    assert_eq!(served.closed, [Closed::Connection, Closed::Listener]);
    assert!(served.refused);
}

#[test]
fn persistent_per_peer_accept_errors_back_off_instead_of_spinning() {
    let script = (0..40).map(|_| Some(io::Error::from(ErrorKind::ConnectionAborted)));
    let served = serve_scripted(script, false, exchange);
    let statistics = served.result.unwrap();
    assert!(served.peer.ends_with(b"\r\n\r\nhello"));
    assert_eq!((statistics.connections, statistics.requests), (1, 1));
    assert_eq!(statistics.accept_errors, 40);
    // Each run of 17 consecutive failures ends in one backoff.
    assert_eq!(served.sleeps, [false, false]);
}

#[test]
fn connections_progress_during_an_accept_backoff() {
    // After the first peer, every accept reports exhaustion for far longer than the test runs.
    let script = std::iter::once(None).chain((0..1000).map(|_| Some(too_many_open_files())));
    let served = serve_scripted(script, false, exchange);
    let statistics = served.result.unwrap();
    assert!(served.peer.ends_with(b"\r\n\r\nhello"));
    assert_eq!((statistics.connections, statistics.requests), (1, 1));
    assert!(statistics.accept_errors >= 1);
    // The connection completed before cancellation, so it closed first here.
    assert_eq!(served.closed, [Closed::Connection, Closed::Listener]);
    assert!(served.refused);
}

#[test]
fn cancellation_interrupts_an_accept_backoff() {
    let script = (0..1000).map(|_| Some(too_many_open_files()));
    let served = serve_scripted(script, false, |_, cancellation, sleeping| {
        let started = Instant::now();
        while !sleeping.load(Ordering::SeqCst) {
            assert!(started.elapsed() < Duration::from_secs(5));
            std::thread::sleep(Duration::from_millis(1));
        }
        cancellation.cancel();
    });
    let statistics = served.result.unwrap();
    assert_eq!(statistics.connections, 0);
    assert_eq!(statistics.accept_errors, served.sleeps.len() as u64);
    // The wait in progress at cancellation ended early rather than expiring.
    assert_eq!(served.sleeps.last(), Some(&true));
    assert_eq!(served.closed, [Closed::Listener]);
    assert!(served.refused);
}

#[test]
fn a_fatal_accept_error_closes_the_listener_before_draining_connections() {
    let served = serve_scripted(
        [None, Some(io::Error::from(ErrorKind::InvalidInput))],
        false,
        |address, _, _| {
            // Hold an idle connection open until the server drains it.
            let mut peer = connect(address);
            let mut response = Vec::new();
            peer.read_to_end(&mut response).unwrap();
            response
        },
    );
    let error = served.result.unwrap_err();
    assert_eq!(error.to_string(), "HTTP/1 connection I/O failed");
    let source = std::error::Error::source(&error).unwrap();
    assert_eq!(source.to_string(), "invalid input parameter");
    assert!(served.peer.is_empty());
    assert_eq!(served.closed, [Closed::Listener, Closed::Connection]);
    assert!(served.refused);
}

#[test]
fn a_fatal_accept_error_is_reported_in_preference_to_a_listener_close_failure() {
    let served = serve_scripted(
        [Some(io::Error::from(ErrorKind::InvalidInput))],
        true,
        |_, _, _| {},
    );
    let error = served.result.unwrap_err();
    let source = std::error::Error::source(&error).unwrap();
    assert_eq!(source.to_string(), "invalid input parameter");
    assert_eq!(served.closed, [Closed::Listener]);
    assert!(served.refused);
}

#[test]
fn a_listener_close_failure_is_reported_after_cancellation() {
    let served = serve_scripted([], true, |_, cancellation, _| cancellation.cancel());
    let error = served.result.unwrap_err();
    assert_eq!(error.to_string(), "HTTP server I/O failed");
    let source = std::error::Error::source(&error).unwrap();
    assert_eq!(source.to_string(), "listener close failed");
    assert!(served.refused);
}

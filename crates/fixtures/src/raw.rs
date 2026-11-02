// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Independent HTTP/1 peers over blocking `std::net` sockets.
//!
//! These peers use no Base encoder, decoder or scheduler, so a client and server sharing a bug cannot hide it. Each socket is blocking with read and write timeouts of [`PEER_TIMEOUT`], so a stalled implementation fails the test instead of hanging it. Every operation panics on failure; run peers on their own threads, through [`RawServer`], and join them to propagate their assertions.

use crate::{Latch, socket};
use base_http_client::Endpoint;
use std::{
    io::{self, BufRead, BufReader, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    panic::resume_unwind,
    thread,
    time::{Duration, Instant},
};

/// The read, write and accept timeout of every raw peer.
pub const PEER_TIMEOUT: Duration = Duration::from_secs(10);

/// One blocking connection, as a client of a server under test or as a server's accepted peer.
///
/// It buffers input, so read messages through it rather than through [`RawPeer::get_mut`].
#[derive(Debug)]
pub struct RawPeer {
    stream: BufReader<TcpStream>,
}

impl RawPeer {
    /// Connect to `address` as a raw client.
    pub fn connect(address: SocketAddr) -> Self {
        Self::new(TcpStream::connect(address).expect("connect a raw peer"))
    }

    fn new(stream: TcpStream) -> Self {
        // An accepted socket inherits O_NONBLOCK from its listener on macOS, the BSDs and Windows, but not on Linux. Reset it, or a read reports `WouldBlock` instead of waiting for the request.
        stream
            .set_nonblocking(false)
            .expect("make the raw peer blocking");
        stream
            .set_read_timeout(Some(PEER_TIMEOUT))
            .expect("set the raw peer read timeout");
        stream
            .set_write_timeout(Some(PEER_TIMEOUT))
            .expect("set the raw peer write timeout");
        // Send each write at once, so the peer observes writes as the test sequences them.
        stream
            .set_nodelay(true)
            .expect("disable Nagle's algorithm for the raw peer");
        Self {
            stream: BufReader::new(stream),
        }
    }

    /// The underlying socket, for writes and socket options. Reading from it bypasses buffered input.
    pub fn get_mut(&mut self) -> &mut TcpStream {
        self.stream.get_mut()
    }

    /// Write all of `bytes` at once.
    pub fn write(&mut self, bytes: impl AsRef<[u8]>) {
        self.get_mut()
            .write_all(bytes.as_ref())
            .expect("write to the raw peer");
    }

    /// Write `bytes` one byte per write, as a slow client or server does, so the other side receives fragmented input.
    pub fn trickle(&mut self, bytes: impl AsRef<[u8]>) {
        for byte in bytes.as_ref() {
            self.write([*byte]);
        }
    }

    /// Read one CRLF-terminated line, without its terminator.
    pub fn read_line(&mut self) -> String {
        read_line(&mut self.stream)
    }

    /// Read a request or response head. See [`read_head`].
    pub fn read_head(&mut self) -> Head {
        read_head(&mut self.stream)
    }

    /// Read the body that follows `head`. See [`read_body`].
    pub fn read_body(&mut self, head: &Head) -> Body {
        read_body(&mut self.stream, head)
    }

    /// Read a head and the body it frames.
    pub fn read_message(&mut self) -> (Head, Body) {
        let head = self.read_head();
        let body = self.read_body(&head);
        (head, body)
    }

    /// Read a prefix of chunked data. See [`read_chunked_prefix`].
    pub fn read_chunked_prefix(&mut self, length: usize) -> Vec<u8> {
        read_chunked_prefix(&mut self.stream, length)
    }

    /// Read until the other side closes its direction of the connection.
    pub fn read_to_end(&mut self) -> Vec<u8> {
        let mut bytes = Vec::new();
        self.stream
            .read_to_end(&mut bytes)
            .expect("read the raw peer to EOF");
        bytes
    }

    /// Require that the other side closes the connection without sending anything more.
    pub fn expect_closed(&mut self) {
        let remainder = self.read_to_end();
        assert!(
            remainder.is_empty(),
            "unexpected input before close: {:?}",
            String::from_utf8_lossy(&remainder)
        );
    }

    /// Close this side's direction of the connection, keeping input readable.
    pub fn shutdown_write(&mut self) {
        self.get_mut()
            .shutdown(Shutdown::Write)
            .expect("shut down raw peer output");
    }

    /// Wait until the other side's kernel has acknowledged everything written, including a write shutdown, so its next read observes it. See [`socket::wait_acknowledged`]; on platforms other than Linux and macOS this sleeps for 50 ms instead.
    ///
    /// Call this before signalling a test through another channel, such as a [`Latch`], that the other side can now observe these writes.
    pub fn wait_acknowledged(&self) {
        socket::wait_acknowledged(self.stream.get_ref());
    }

    /// Write `bytes` and wait until the other side has acknowledged them.
    pub fn write_acknowledged(&mut self, bytes: impl AsRef<[u8]>) {
        self.write(bytes);
        self.wait_acknowledged();
    }

    /// Shut down writes and wait until the other side has acknowledged everything, so its next read observes EOF.
    pub fn close_write_acknowledged(&mut self) {
        self.shutdown_write();
        self.wait_acknowledged();
    }
}

impl Read for RawPeer {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buffer)
    }
}

impl BufRead for RawPeer {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.stream.fill_buf()
    }
    fn consume(&mut self, amount: usize) {
        self.stream.consume(amount)
    }
}

/// Read one CRLF-terminated line from `input`, without its terminator.
pub fn read_line(input: &mut impl BufRead) -> String {
    let mut line = String::new();
    input.read_line(&mut line).expect("read a line");
    assert!(line.ends_with("\r\n"), "unterminated line: {line:?}");
    line.truncate(line.len() - 2);
    line
}

/// Read a request or response head from `input`: its start line and fields.
///
/// This and the other free functions accept any buffered input, such as a TLS stream, so a peer wrapping its socket can still use them.
pub fn read_head(input: &mut impl BufRead) -> Head {
    let line = read_line(input);
    let mut fields = Vec::new();
    loop {
        let field = read_line(input);
        if field.is_empty() {
            return Head { line, fields };
        }
        let (name, value) = field.split_once(':').expect("malformed field line");
        fields.push((name.to_owned(), value.trim().to_owned()));
    }
}

/// Read the body that follows `head` from `input`, using its framing: chunked with trailers, a `Content-Length`, a bodyless status, a request without framing, or a response delimited by the connection closing. A response to HEAD has no body; do not read one.
pub fn read_body(input: &mut impl BufRead, head: &Head) -> Body {
    let mut body = Body::default();
    if head.get("transfer-encoding") == Some("chunked") {
        loop {
            let size = read_chunk_size(input);
            if size == 0 {
                break;
            }
            read_chunk(input, size, &mut body.data);
        }
        loop {
            let line = read_line(input);
            if line.is_empty() {
                break;
            }
            let (name, value) = line.split_once(':').expect("malformed trailer line");
            body.trailers
                .push((name.to_owned(), value.trim().to_owned()));
        }
    } else if let Some(length) = head.get("content-length") {
        body.data
            .resize(length.parse().expect("a numeric Content-Length"), 0);
        input
            .read_exact(&mut body.data)
            .expect("read a fixed-length body");
    } else if head.is_response() && !head.is_bodyless() {
        input
            .read_to_end(&mut body.data)
            .expect("read a body delimited by close");
    }
    body
}

/// Read `length` bytes of chunked data from `input` without waiting for the terminating chunk, and without requiring the sender's chunk boundaries to survive the transport.
pub fn read_chunked_prefix(input: &mut impl BufRead, length: usize) -> Vec<u8> {
    let mut data = Vec::new();
    while data.len() < length {
        let size = read_chunk_size(input);
        assert!(
            size > 0 && size <= length - data.len(),
            "chunk of {size} bytes overruns the expected prefix"
        );
        read_chunk(input, size, &mut data);
    }
    data
}

fn read_chunk_size(input: &mut impl BufRead) -> usize {
    let line = read_line(input);
    let size = line.split(';').next().unwrap_or_default().trim();
    usize::from_str_radix(size, 16).expect("malformed chunk size line")
}

fn read_chunk(input: &mut impl BufRead, size: usize, data: &mut Vec<u8>) {
    let start = data.len();
    data.resize(start + size, 0);
    input
        .read_exact(&mut data[start..])
        .expect("read chunk data");
    assert_eq!(read_line(input), "", "chunk data overruns its size");
}

/// A received message head. Names keep their spelling; values are trimmed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Head {
    /// The start line without its terminator, such as `GET / HTTP/1.1` or `HTTP/1.1 200 OK`.
    pub line: String,
    /// Fields in received order.
    pub fields: Vec<(String, String)>,
}

impl Head {
    /// Whether this is a response head.
    pub fn is_response(&self) -> bool {
        self.line.starts_with("HTTP/")
    }

    /// The response status code. Panics for a request head.
    pub fn status(&self) -> u16 {
        assert!(self.is_response(), "not a response head: {:?}", self.line);
        self.line
            .split(' ')
            .nth(1)
            .and_then(|status| status.parse().ok())
            .expect("malformed status line")
    }

    fn is_bodyless(&self) -> bool {
        matches!(self.status(), 100..=199 | 204 | 304)
    }

    /// The first value of a field, compared case-insensitively.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.values(name).next()
    }

    /// Every value of a field, compared case-insensitively, in received order.
    pub fn values<'a, 'n>(&'a self, name: &'n str) -> impl Iterator<Item = &'a str> + use<'a, 'n> {
        self.fields
            .iter()
            .filter(move |(field, _)| field.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// A received body.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Body {
    /// The decoded payload.
    pub data: Vec<u8>,
    /// Trailer fields of a chunked body, in received order.
    pub trailers: Vec<(String, String)>,
}

/// A listening socket owned by a [`RawServer`] thread.
#[derive(Debug)]
pub struct RawListener {
    listener: TcpListener,
}

impl RawListener {
    /// Accept the next connection, failing after [`PEER_TIMEOUT`].
    ///
    /// `std` cannot bound a blocking accept, so the listener is nonblocking and this polls it until the deadline. The accepted socket is then made blocking explicitly, since it inherits the listener's mode on some platforms.
    pub fn accept(&self) -> RawPeer {
        let deadline = Instant::now() + PEER_TIMEOUT;
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => return RawPeer::new(stream),
                Err(error) => {
                    assert_eq!(
                        error.kind(),
                        io::ErrorKind::WouldBlock,
                        "accept failed: {error}"
                    );
                    assert!(
                        Instant::now() < deadline,
                        "no connection within {PEER_TIMEOUT:?}"
                    );
                    thread::sleep(Duration::from_millis(1));
                }
            }
        }
    }

    /// Require that no connection is waiting to be accepted.
    pub fn expect_no_connection(&self) {
        let pending = self.listener.accept().map(|(_, address)| address);
        assert!(
            matches!(&pending, Err(error) if error.kind() == io::ErrorKind::WouldBlock),
            "unexpected connection: {pending:?}"
        );
    }
}

/// A raw server on its own thread, listening on an OS-selected loopback port.
///
/// The listener is bound before [`RawServer::spawn`] returns, so connections never race its start. Call [`RawServer::join`] after the test to propagate the peer's assertions.
///
/// The canned servers which close a connection or write outside an exchange wait for the client's kernel to acknowledge it, with [`RawPeer::wait_acknowledged`], before they continue or release a [`Latch`]. A client's next read therefore observes the close or the input. On platforms other than Linux and macOS that wait is a 50 ms sleep, which makes the race unlikely rather than impossible.
#[derive(Debug)]
#[must_use = "join the raw server to propagate its assertions"]
pub struct RawServer {
    address: SocketAddr,
    thread: thread::JoinHandle<()>,
}

impl RawServer {
    /// Listen on `127.0.0.1:0` and run `work` with the listener on a new thread.
    pub fn spawn(work: impl FnOnce(RawListener) + Send + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind a raw server");
        let address = listener.local_addr().expect("raw server address");
        listener
            .set_nonblocking(true)
            .expect("make the raw listener nonblocking");
        let thread = thread::spawn(move || work(RawListener { listener }));
        Self { address, thread }
    }

    /// Accept one connection, read one request, write `response`, then close the connection once the client has acknowledged it.
    pub fn respond_then_close(response: impl Into<Vec<u8>>) -> Self {
        let response = response.into();
        Self::spawn(move |listener| {
            let mut peer = listener.accept();
            peer.read_message();
            peer.write(response);
            peer.close_write_acknowledged();
        })
    }

    /// Respond with a `Content-Length` of `declared` but only the bytes of `body`, then close the connection.
    pub fn truncate_body(declared: usize, body: &[u8]) -> Self {
        let mut response =
            format!("HTTP/1.1 200 OK\r\nContent-Length: {declared}\r\n\r\n").into_bytes();
        response.extend_from_slice(body);
        Self::respond_then_close(response)
    }

    /// Write `head` in response to one request, then stall until `release` before closing the connection.
    pub fn stall_after_head(head: impl Into<Vec<u8>>, release: Latch) -> Self {
        let head = head.into();
        Self::spawn(move |listener| {
            let mut peer = listener.accept();
            peer.read_message();
            peer.write(head);
            release.wait_blocking();
        })
    }

    /// Answer one request with `first`, close that connection while it is idle, as a keep-alive timeout does, and release `closed` once the client has acknowledged the close. Answer one request on a second connection with `next`, then require the client to close it without further input.
    pub fn close_while_idle(
        first: impl Into<Vec<u8>>,
        next: impl Into<Vec<u8>>,
        closed: Latch,
    ) -> Self {
        let (first, next) = (first.into(), next.into());
        Self::spawn(move |listener| {
            let mut peer = listener.accept();
            peer.read_message();
            peer.write(first);
            peer.close_write_acknowledged();
            closed.release();
            let mut peer = listener.accept();
            peer.read_message();
            peer.write(next);
            peer.expect_closed();
        })
    }

    /// Answer one request with `first`, then write `unsolicited` outside any exchange and release `written` once the client has acknowledged it. Answer one request on a second connection with `next`, then require the client to close both connections without further input.
    pub fn write_while_idle(
        first: impl Into<Vec<u8>>,
        unsolicited: impl Into<Vec<u8>>,
        next: impl Into<Vec<u8>>,
        written: Latch,
    ) -> Self {
        let (first, unsolicited, next) = (first.into(), unsolicited.into(), next.into());
        Self::spawn(move |listener| {
            let mut retired = listener.accept();
            retired.read_message();
            retired.write(first);
            retired.write_acknowledged(unsolicited);
            written.release();
            let mut peer = listener.accept();
            peer.read_message();
            peer.write(next);
            peer.expect_closed();
            retired.expect_closed();
        })
    }

    /// The listening address.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// A plain HTTP endpoint for the listening address.
    pub fn endpoint(&self) -> Endpoint {
        Endpoint::new(self.address)
    }

    /// Wait for the peer thread, propagating its panic.
    pub fn join(self) {
        self.thread
            .join()
            .unwrap_or_else(|panic| resume_unwind(panic));
    }
}

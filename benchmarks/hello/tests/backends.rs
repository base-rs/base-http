// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Start the server with every backend and check that it answers, as `run.py`
//! does before each sample. Nothing here is timed: the watchdog only turns a
//! hung backend into a failure that names it, instead of a stalled job.

use std::{
    io::{self, BufRead, BufReader, Read, Write},
    net::{Shutdown, TcpStream},
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::Duration,
};

const HTTP: &[&str] = &[
    "base-native",
    "base-native-background",
    "base-tokio",
    "hyper-tokio",
    "hyper-task-tokio",
    #[cfg(target_os = "linux")]
    "base-iouring",
];

const RAW: &[&str] = &[
    "base-native-raw",
    "base-tokio-raw",
    "tokio-raw",
    "tokio-task-raw",
    "tokio-connections-raw",
    #[cfg(target_os = "linux")]
    "base-iouring-raw",
    #[cfg(target_os = "linux")]
    "base-iouring-owned-raw",
    #[cfg(target_os = "linux")]
    "base-iouring-connections-raw",
    #[cfg(target_os = "linux")]
    "base-iouring-owned-connections-raw",
];

/// Bounds each wait on a server, so that one which hangs fails the test
/// promptly. It is far longer than any healthy reply takes, even under load.
const WATCHDOG: Duration = Duration::from_secs(60);

/// Describe a failed read or write, naming the watchdog when its timeout expired,
/// which the operating system reports as `WouldBlock` or `TimedOut`.
fn failure(error: &io::Error) -> String {
    match error.kind() {
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => {
            format!("the watchdog expired: no progress within {WATCHDOG:?}")
        }
        _ => error.to_string(),
    }
}

/// The complete response every backend sends.
const RESPONSE: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 13\r\n\r\nHello, world!";

/// A server process, killed when dropped so that a failed check cannot leave it running.
struct Server(Child);

impl Server {
    /// Start `backend` and connect to the address it reports once it listens.
    fn connect(backend: &str) -> (Self, TcpStream) {
        let mut child = Command::new(env!("CARGO_BIN_EXE_base-http-hello-benchmark"))
            .arg(backend)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap_or_else(|error| panic!("{backend}: starting the server failed: {error}"));
        let stdout = child.stdout.take().unwrap();
        let server = Self(child);
        // Read the address on another thread so that the wait can be bounded;
        // if it overruns, dropping the server kills the process, which ends the read.
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            let mut address = String::new();
            let _ = sender.send(
                BufReader::new(stdout)
                    .read_line(&mut address)
                    .map(|_| address),
            );
        });
        let address = match receiver.recv_timeout(WATCHDOG) {
            Ok(Ok(address)) => address,
            Ok(Err(error)) => panic!("{backend}: reading the server's address failed: {error}"),
            Err(_) => {
                panic!("{backend}: the server did not report its address within {WATCHDOG:?}")
            }
        };
        assert!(
            !address.is_empty(),
            "{backend}: the server exited without reporting its address"
        );
        let stream = TcpStream::connect(address.trim())
            .unwrap_or_else(|error| panic!("{backend}: connecting to {address:?} failed: {error}"));
        stream.set_read_timeout(Some(WATCHDOG)).unwrap();
        stream.set_write_timeout(Some(WATCHDOG)).unwrap();
        (server, stream)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Write one request in a single write, so that a raw backend, which replies to
/// every read, sees it as one read.
fn request(backend: &str, step: &str, stream: &mut TcpStream, fields: &str) {
    let request = format!("GET / HTTP/1.1\r\nHost: localhost\r\n{fields}\r\n");
    let written = stream.write(request.as_bytes()).unwrap_or_else(|error| {
        panic!(
            "{backend}: {step}: writing the request failed: {}",
            failure(&error)
        )
    });
    assert_eq!(
        written,
        request.len(),
        "{backend}: {step}: the request was written partially"
    );
}

/// Read one complete response, returning its lowercase field lines and body.
fn response(backend: &str, step: &str, stream: &mut TcpStream) -> (Vec<String>, Vec<u8>) {
    let fail = |error: io::Error| -> ! {
        panic!(
            "{backend}: {step}: reading the response failed: {}",
            failure(&error)
        )
    };
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        stream
            .read_exact(&mut byte)
            .unwrap_or_else(|error| fail(error));
        head.push(byte[0]);
    }
    let head = String::from_utf8(head).unwrap_or_else(|error| {
        panic!("{backend}: {step}: the response head is not UTF-8: {error}")
    });
    let mut lines = head.lines();
    assert_eq!(
        lines.next(),
        Some("HTTP/1.1 200 OK"),
        "{backend}: {step}: {head}"
    );
    let fields: Vec<String> = lines
        .filter(|line| !line.is_empty())
        .map(str::to_ascii_lowercase)
        .collect();
    let length = fields
        .iter()
        .find_map(|field| field.strip_prefix("content-length: "))
        .unwrap_or_else(|| panic!("{backend}: {step}: no content-length in {fields:?}"))
        .parse()
        .unwrap_or_else(|error| panic!("{backend}: {step}: invalid content-length: {error}"));
    let mut body = vec![0; length];
    stream
        .read_exact(&mut body)
        .unwrap_or_else(|error| fail(error));
    (fields, body)
}

fn check(backend: &str, step: &str, fields: &[String], body: &[u8]) {
    assert_eq!(body, b"Hello, world!", "{backend}: {step}: body");
    assert!(
        fields
            .iter()
            .any(|field| field == "content-type: text/plain"),
        "{backend}: {step}: no content-type: text/plain in {fields:?}"
    );
    assert!(
        !fields.iter().any(|field| field.starts_with("date:")),
        "{backend}: {step}: unexpected date in {fields:?}"
    );
}

/// Read until the server closes the connection, and require that it sent nothing more.
fn expect_close(backend: &str, step: &str, stream: &mut TcpStream) {
    let mut rest = Vec::new();
    stream.read_to_end(&mut rest).unwrap_or_else(|error| {
        panic!(
            "{backend}: {step}: waiting for the server to close failed: {}",
            failure(&error)
        )
    });
    assert!(
        rest.is_empty(),
        "{backend}: {step}: the server sent {} bytes more: {:?}",
        rest.len(),
        String::from_utf8_lossy(&rest)
    );
}

#[test]
fn http_backends_reuse_and_close_connections() {
    for backend in HTTP {
        let (_server, mut stream) = Server::connect(backend);
        for index in 1..=2 {
            let step = format!("keep-alive request {index}");
            request(backend, &step, &mut stream, "");
            let (fields, body) = response(backend, &step, &mut stream);
            check(backend, &step, &fields, &body);
        }
        let step = "closing request";
        request(backend, step, &mut stream, "Connection: close\r\n");
        let (fields, body) = response(backend, step, &mut stream);
        check(backend, step, &fields, &body);
        expect_close(backend, "after the closing request", &mut stream);
    }
}

/// A raw backend writes one fixed response for every read and closes when it
/// reads end of file. Each request is one write, answered before the next is
/// sent, so a correct backend replies exactly once to each: after the replies
/// to every request and the client's end of file, it must close without
/// sending anything more.
#[test]
fn raw_backends_reply_once_to_each_request() {
    const REQUESTS: usize = 3;
    for backend in RAW {
        let (_server, mut stream) = Server::connect(backend);
        for index in 1..=REQUESTS {
            let step = format!("request {index}");
            request(backend, &step, &mut stream, "");
            let mut reply = vec![0; RESPONSE.len()];
            stream.read_exact(&mut reply).unwrap_or_else(|error| {
                panic!(
                    "{backend}: {step}: reading the reply failed: {}",
                    failure(&error)
                )
            });
            assert_eq!(
                reply,
                RESPONSE,
                "{backend}: {step}: reply {:?}",
                String::from_utf8_lossy(&reply)
            );
        }
        stream.shutdown(Shutdown::Write).unwrap_or_else(|error| {
            panic!("{backend}: shutting down the request stream failed: {error}")
        });
        expect_close(backend, &format!("after {REQUESTS} requests"), &mut stream);
    }
}

#[test]
fn unknown_backend_fails() {
    let output = Command::new(env!("CARGO_BIN_EXE_base-http-hello-benchmark"))
        .arg("unknown")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("usage:"));
}

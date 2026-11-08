// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Start the server with every backend and check that it answers, as `run.py`
//! does before each sample. Nothing here is timed.

use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
    process::{Child, Command, Stdio},
};

const HTTP: &[&str] = &[
    "base-native",
    "base-native-background",
    "base-direct",
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

/// A server process, killed when dropped so that a failed check cannot leave it running.
struct Server(Child);

impl Server {
    /// Start `backend` and connect to the address it reports once it listens.
    fn connect(backend: &str) -> (Self, TcpStream) {
        let mut child = Command::new(env!("CARGO_BIN_EXE_base-http-hello-benchmark"))
            .arg(backend)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut address = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut address)
            .unwrap();
        let server = Self(child);
        assert!(!address.is_empty(), "{backend} did not report its address");
        let stream = TcpStream::connect(address.trim()).unwrap();
        (server, stream)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Send one request and read one complete response, returning its lowercase field lines and body.
fn exchange(stream: &mut TcpStream, fields: &str) -> (Vec<String>, Vec<u8>) {
    write!(stream, "GET / HTTP/1.1\r\nHost: localhost\r\n{fields}\r\n").unwrap();
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        stream.read_exact(&mut byte).unwrap();
        head.push(byte[0]);
    }
    let head = String::from_utf8(head).unwrap();
    let mut lines = head.lines();
    assert_eq!(lines.next(), Some("HTTP/1.1 200 OK"), "{head}");
    let fields: Vec<String> = lines
        .filter(|line| !line.is_empty())
        .map(str::to_ascii_lowercase)
        .collect();
    let length = fields
        .iter()
        .find_map(|field| field.strip_prefix("content-length: "))
        .expect("content-length")
        .parse()
        .unwrap();
    let mut body = vec![0; length];
    stream.read_exact(&mut body).unwrap();
    (fields, body)
}

fn check(fields: &[String], body: &[u8]) {
    assert_eq!(body, b"Hello, world!");
    assert!(
        fields
            .iter()
            .any(|field| field == "content-type: text/plain"),
        "{fields:?}"
    );
    assert!(
        !fields.iter().any(|field| field.starts_with("date:")),
        "{fields:?}"
    );
}

#[test]
fn http_backends_reuse_and_close_connections() {
    for backend in HTTP {
        let (_server, mut stream) = Server::connect(backend);
        for _ in 0..2 {
            let (fields, body) = exchange(&mut stream, "");
            check(&fields, &body);
        }
        let (fields, body) = exchange(&mut stream, "Connection: close\r\n");
        check(&fields, &body);
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).unwrap();
        assert!(
            rest.is_empty(),
            "{backend} sent bytes after closing: {rest:?}"
        );
    }
}

#[test]
fn raw_backends_reply_to_every_read() {
    for backend in RAW {
        let (_server, mut stream) = Server::connect(backend);
        for _ in 0..3 {
            let (fields, body) = exchange(&mut stream, "");
            check(&fields, &body);
        }
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

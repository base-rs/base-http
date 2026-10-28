// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![allow(dead_code)]

use base_http_server::{Error, Handler};
use base_scheduler::{
    Cancellation, SocketAccept, SocketBind, SocketClose, SocketListen, SocketLocalAddress,
    SocketOpen, SocketType,
};
use base_scheduler_direct::{DirectOptions, DirectScheduler};
use futures_lite::future::block_on;
use std::{
    io::{BufRead, BufReader},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

pub fn scheduler() -> DirectScheduler {
    DirectScheduler::new(DirectOptions {
        threads: 2,
        capacity: 64,
    })
    .unwrap()
}

pub async fn listener(
    scheduler: &DirectScheduler,
) -> (base_scheduler_direct::DirectSocket, SocketAddr) {
    let never = Cancellation::never();
    let address: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let socket = scheduler
        .socket_open(address.into(), SocketType::Stream, None, &never)
        .await
        .unwrap();
    scheduler
        .socket_bind(&socket, address, &never)
        .await
        .unwrap();
    scheduler.socket_listen(&socket, 32, &never).await.unwrap();
    let address = scheduler.socket_local_address(&socket).unwrap();
    (socket, address)
}

pub fn connect(address: SocketAddr) -> BufReader<TcpStream> {
    let socket = TcpStream::connect(address).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    BufReader::new(socket)
}

pub fn connection<H: Handler, T: Send + 'static>(
    handler: H,
    peer: impl FnOnce(BufReader<TcpStream>, Cancellation) -> T + Send + 'static,
) -> (Result<u64, Error>, T) {
    connection_options(handler, base_http1::Options::default(), peer)
}

pub fn connection_options<H: Handler, T: Send + 'static>(
    handler: H,
    options: base_http1::Options,
    peer: impl FnOnce(BufReader<TcpStream>, Cancellation) -> T + Send + 'static,
) -> (Result<u64, Error>, T) {
    let scheduler = scheduler();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on(async {
            let (listener, address) = listener(&scheduler).await;
            let cancellation = Cancellation::new();
            let peer_cancel = cancellation.clone();
            let peer = std::thread::spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    peer(connect(address), peer_cancel.clone())
                }));
                if result.is_err() {
                    peer_cancel.cancel();
                }
                result
            });
            let (socket, _) = scheduler
                .socket_accept(&listener, &cancellation)
                .await
                .unwrap();
            scheduler.socket_close(listener).await.unwrap();
            let served = base_http_server::serve_connection(
                &scheduler,
                socket,
                &handler,
                &options,
                &cancellation,
            )
            .await;
            let peer = peer
                .join()
                .unwrap()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
            (served, peer)
        })
    }));
    block_on(scheduler.shutdown());
    result.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

#[derive(Debug)]
pub struct Head {
    pub status: u16,
    pub fields: Vec<(String, String)>,
}
impl Head {
    pub fn get(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

// Deliberately independent raw peer: no Base HTTP/1 encoder or decoder is used.
pub fn head(peer: &mut impl BufRead) -> Head {
    let mut line = String::new();
    peer.read_line(&mut line).unwrap();
    assert!(line.ends_with("\r\n"), "missing response line: {line:?}");
    let status = line.split(' ').nth(1).unwrap().parse().unwrap();
    let mut fields = Vec::new();
    loop {
        line.clear();
        peer.read_line(&mut line).unwrap();
        if line == "\r\n" {
            break;
        }
        assert!(line.ends_with("\r\n"));
        let (name, value) = line[..line.len() - 2].split_once(':').unwrap();
        fields.push((name.to_owned(), value.trim().to_owned()));
    }
    Head { status, fields }
}

pub fn body(peer: &mut impl BufRead, head: &Head) -> (Vec<u8>, Vec<String>) {
    let mut data = Vec::new();
    let mut trailers = Vec::new();
    if head.get("transfer-encoding") == Some("chunked") {
        loop {
            let mut line = String::new();
            peer.read_line(&mut line).unwrap();
            let length = usize::from_str_radix(line.trim_end(), 16).unwrap();
            if length == 0 {
                break;
            }
            let start = data.len();
            data.resize(start + length, 0);
            peer.read_exact(&mut data[start..]).unwrap();
            let mut crlf = [0; 2];
            peer.read_exact(&mut crlf).unwrap();
            assert_eq!(&crlf, b"\r\n");
        }
        loop {
            let mut line = String::new();
            peer.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
            assert!(!line.is_empty());
            trailers.push(line.trim_end().to_owned());
        }
    } else if let Some(length) = head.get("content-length") {
        data.resize(length.parse().unwrap(), 0);
        peer.read_exact(&mut data).unwrap();
    } else {
        peer.read_to_end(&mut data).unwrap();
    }
    (data, trailers)
}

// Read a streaming prefix without requiring transport reads to preserve the
// sender's chunk boundaries or waiting for the response's terminating chunk.
pub fn chunked_prefix(peer: &mut impl BufRead, length: usize) -> Vec<u8> {
    let mut data = Vec::new();
    while data.len() < length {
        let mut line = String::new();
        peer.read_line(&mut line).unwrap();
        let size = usize::from_str_radix(line.trim_end(), 16).unwrap();
        assert!(size > 0 && size <= length - data.len());
        let start = data.len();
        data.resize(start + size, 0);
        peer.read_exact(&mut data[start..]).unwrap();
        let mut crlf = [0; 2];
        peer.read_exact(&mut crlf).unwrap();
        assert_eq!(&crlf, b"\r\n");
    }
    data
}

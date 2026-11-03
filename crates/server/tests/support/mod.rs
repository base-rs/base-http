// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![allow(dead_code)]

use base_http_fixtures::raw;
use base_http_server::{Error, Handler};
use base_scheduler::{
    Cancellation, SocketAccept, SocketBind, SocketClose, SocketListen, SocketLocalAddress,
    SocketOpen, SocketType,
};
use base_scheduler_native::{NativeOptions, NativeScheduler};
use futures_lite::future::block_on;
use std::{
    io::{BufRead, BufReader},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

pub fn scheduler() -> NativeScheduler {
    NativeScheduler::with_options(NativeOptions {
        workers: 2,
        ..Default::default()
    })
    .unwrap()
}

pub async fn listener(
    scheduler: &NativeScheduler,
) -> (base_scheduler_native::NativeSocket, SocketAddr) {
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
    let head = raw::read_head(peer);
    Head {
        status: head.status(),
        fields: head.fields,
    }
}

/// The body framed by `head`, with each trailer as `name: value`.
pub fn body(peer: &mut impl BufRead, head: &Head) -> (Vec<u8>, Vec<String>) {
    let head = raw::Head {
        line: format!("HTTP/1.1 {}", head.status),
        fields: head.fields.clone(),
    };
    let body = raw::read_body(peer, &head);
    let trailers = body
        .trailers
        .into_iter()
        .map(|(name, value)| format!("{name}: {value}"))
        .collect();
    (body.data, trailers)
}

// Read a streaming prefix without requiring transport reads to preserve the
// sender's chunk boundaries or waiting for the response's terminating chunk.
pub fn chunked_prefix(peer: &mut impl BufRead, length: usize) -> Vec<u8> {
    raw::read_chunked_prefix(peer, length)
}

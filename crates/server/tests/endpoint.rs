// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! `Endpoint::bind` opens, binds and listens, and reports a failure at any step.
use base_http_server::Endpoint;
use base_scheduler::SocketLocalAddress;
use base_scheduler_native::NativeScheduler;
use futures_lite::future::block_on;
use std::{
    io,
    net::{SocketAddr, TcpListener, TcpStream},
};

fn loopback() -> SocketAddr {
    ([127, 0, 0, 1], 0).into()
}

#[test]
fn bind_listens_on_an_os_selected_port() {
    let scheduler = NativeScheduler::new().unwrap();
    let endpoint = block_on(Endpoint::bind(&scheduler, loopback())).unwrap();
    let address = scheduler.socket_local_address(endpoint.listener()).unwrap();
    assert_ne!(address.port(), 0);
    // The socket is listening, so a peer can connect before anything accepts.
    TcpStream::connect(address).unwrap();
    drop(endpoint);
    block_on(scheduler.shutdown());
}

// The cause of an error, as reported through its source chain.
fn io_kind(error: &(dyn std::error::Error + 'static)) -> Option<io::ErrorKind> {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(error) = error.downcast_ref::<io::Error>() {
            return Some(error.kind());
        }
        current = error.source();
    }
    None
}

#[test]
fn a_failed_bind_reports_the_error() {
    let scheduler = NativeScheduler::new().unwrap();
    let occupied = TcpListener::bind(loopback()).unwrap();
    let error = block_on(Endpoint::bind(&scheduler, occupied.local_addr().unwrap()))
        .err()
        .unwrap();
    assert_eq!(io_kind(&error), Some(io::ErrorKind::AddrInUse), "{error}");
    block_on(scheduler.shutdown());
}

#[test]
fn binding_with_a_closed_scheduler_fails() {
    let scheduler = NativeScheduler::new().unwrap();
    block_on(scheduler.shutdown());
    assert!(block_on(Endpoint::bind(&scheduler, loopback())).is_err());
}

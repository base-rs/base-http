// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Standard-library TCP import retains established connections and listeners.

use crate::SocketTestScheduler;
use base_scheduler::*;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

fn listener(domain: SocketDomain) -> TcpListener {
    TcpListener::bind(match domain {
        SocketDomain::Ipv4 => "127.0.0.1:0",
        SocketDomain::Ipv6 => "[::1]:0",
    })
    .unwrap()
}

async fn exchange<S: SocketTestScheduler>(scheduler: &S, socket: &S::Socket, peer: &mut TcpStream) {
    let never = Cancellation::never();
    let mut source = b"hello".to_vec();
    let mut sent = 0;
    while sent < 5 {
        let (result, returned) =
            crate::io::socket_write(scheduler, socket, source.slice(sent..), &never).await;
        let count = result.unwrap();
        assert!(count > 0 && count <= 5 - sent);
        sent += count;
        source = returned.into_inner();
    }
    let mut received = [0; 5];
    peer.read_exact(&mut received).unwrap();
    assert_eq!(&received, b"hello");
    peer.write_all(b"reply").unwrap();
    let mut received = Vec::new();
    while received.len() < 5 {
        let (result, buffer) =
            crate::io::socket_read(scheduler, socket, vec![0; 5 - received.len()], &never).await;
        let count = result.unwrap();
        assert!(count > 0 && count <= 5 - received.len());
        received.extend_from_slice(&buffer[..count]);
    }
    assert_eq!(received, b"reply");
}

/// Import an established stream, preserving endpoints and bidirectional data, then observe native peer closure as EOF.
pub async fn stream<
    S: SocketRegistration
        + SocketTestScheduler
        + SocketTestScheduler
        + SocketClose
        + SocketLocalAddress
        + SocketPeerAddress,
>(
    scheduler: &S,
    domain: SocketDomain,
) {
    let listener = listener(domain);
    let native = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut peer, _) = listener.accept().unwrap();
    let local = native.local_addr().unwrap();
    let remote = native.peer_addr().unwrap();
    let socket = scheduler.register_socket(native).unwrap();
    assert_eq!(scheduler.socket_local_address(&socket).unwrap(), local);
    assert_eq!(scheduler.socket_peer_address(&socket).unwrap(), remote);
    exchange(scheduler, &socket, &mut peer).await;
    drop(peer);
    let (result, _) =
        crate::io::socket_read(scheduler, &socket, vec![0; 1], &Cancellation::never()).await;
    assert_eq!(result.unwrap(), 0);
    scheduler.socket_close(socket).await.unwrap();
}

/// Import a listening socket without rebinding/relistening; preserve already queued and newly arriving connections.
pub async fn listening<
    S: SocketRegistration
        + SocketAccept
        + SocketTestScheduler
        + SocketTestScheduler
        + SocketClose
        + SocketLocalAddress
        + SocketPeerAddress,
>(
    scheduler: &S,
    domain: SocketDomain,
) {
    let native = listener(domain);
    let local = native.local_addr().unwrap();
    let mut queued = Some(TcpStream::connect(local).unwrap());
    let listener = scheduler.register_listener(native).unwrap();
    assert_eq!(scheduler.socket_local_address(&listener).unwrap(), local);
    for pending in [true, false] {
        let mut peer = if pending {
            queued.take().unwrap()
        } else {
            TcpStream::connect(local).unwrap()
        };
        let (socket, address) = scheduler
            .socket_accept(&listener, &Cancellation::never())
            .await
            .unwrap();
        assert_eq!(address, peer.local_addr().unwrap());
        assert_eq!(scheduler.socket_local_address(&socket).unwrap(), local);
        assert_eq!(scheduler.socket_peer_address(&socket).unwrap(), address);
        exchange(scheduler, &socket, &mut peer).await;
        scheduler.socket_close(socket).await.unwrap();
    }
    scheduler.socket_close(listener).await.unwrap();
}

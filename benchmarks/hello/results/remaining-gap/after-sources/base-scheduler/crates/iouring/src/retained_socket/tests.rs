// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::{IoUringSelector, test_buffers::LargeMapping};
use base_scheduler::{Cancellation, SocketClose, SocketDomain, SocketOpen, SocketType};
use futures_lite::future::block_on;
use std::{
    io::{Read, Write},
    os::unix::net::{UnixDatagram, UnixStream},
};

fn registered(socket: impl Into<Socket>) -> RegisteredSocket {
    Arc::new(async_io::Async::new(socket.into()).unwrap())
}

#[test]
fn scalar_stream_reads_preserve_peek_empty_ranges_and_would_block() {
    let (socket, mut peer) = UnixStream::pair().unwrap();
    let socket = registered(socket);
    let mut bytes = [0; 16];
    assert_eq!(
        recv(&socket, &mut bytes, SocketRecvFlags::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    peer.write_all(b"hello").unwrap();
    assert_eq!(
        recv(&socket, &mut [], SocketRecvFlags::default()).unwrap(),
        0
    );
    assert_eq!(
        recv(&socket, &mut bytes[..2], SocketRecvFlags { peek: true }).unwrap(),
        2
    );
    assert_eq!(&bytes[..2], b"he");
    assert_eq!(
        recv(&socket, &mut bytes, SocketRecvFlags::default()).unwrap(),
        5
    );
    assert_eq!(&bytes[..5], b"hello");
    drop(peer);
    assert_eq!(
        recv(&socket, &mut bytes, SocketRecvFlags::default()).unwrap(),
        0
    );
}

#[test]
fn scalar_stream_writes_preserve_empty_ranges_and_report_broken_peers() {
    let (socket, mut peer) = UnixStream::pair().unwrap();
    let socket = registered(socket);
    assert_eq!(send(&socket, &[], SocketSendFlags::default()).unwrap(), 0);
    assert_eq!(
        send(&socket, b"hello", SocketSendFlags::default()).unwrap(),
        5
    );
    let mut bytes = [0; 5];
    peer.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"hello");
    drop(peer);
    assert_eq!(
        send(&socket, b"hello", SocketSendFlags::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[test]
fn scalar_datagrams_transfer_and_consume_empty_messages() {
    let (sender, receiver) = UnixDatagram::pair().unwrap();
    let sender = registered(sender);
    let receiver = registered(receiver);
    let mut bytes = [0; 16];
    assert_eq!(send(&sender, &[], SocketSendFlags::default()).unwrap(), 0);
    assert_eq!(
        recv(&receiver, &mut bytes, SocketRecvFlags::default()).unwrap(),
        0
    );
    assert_eq!(
        recv(&receiver, &mut bytes, SocketRecvFlags::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        send(&sender, b"hello", SocketSendFlags { dont_route: true }).unwrap(),
        5
    );
    assert_eq!(
        recv(&receiver, &mut [], SocketRecvFlags::default()).unwrap(),
        0
    );
    assert_eq!(
        recv(&receiver, &mut bytes, SocketRecvFlags::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
}

#[test]
fn oversized_native_messages_are_rejected_before_accessing_payload_pages() {
    let (selector, _driver) = IoUringSelector::new(Default::default()).unwrap();
    let socket = block_on(selector.socket_open(
        SocketDomain::Ipv4,
        SocketType::Datagram,
        None,
        &Cancellation::never(),
    ))
    .unwrap();
    let retained = socket.retain_socket();
    let mut mapping = LargeMapping::new();
    assert_eq!(
        recvmsg(
            &retained,
            &mut [IoSliceMut::new(mapping.bytes_mut())],
            SocketRecvFlags::default(),
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        sendmsg(
            &retained,
            &[IoSlice::new(mapping.bytes())],
            None,
            SocketSendFlags::default(),
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
    drop(mapping);
    drop(retained);
    block_on(selector.socket_close(socket)).unwrap();
}

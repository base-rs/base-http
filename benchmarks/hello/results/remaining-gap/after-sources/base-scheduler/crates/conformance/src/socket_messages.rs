// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Message assertions use both prepared packages and ordinary owned sequences.

use crate::SocketTestScheduler;
use crate::io::SocketMessage;
use base_scheduler::*;
use futures_lite::future::poll_once;
use std::pin::pin;

fn cancelled<T>(result: OperationResult<T>) {
    assert!(matches!(result, Err(OperationError::Cancelled(_))));
}

/// Check one short vectored stream send and advance only its returned package. The fixture must supply an unread stream whose send capacity is smaller than the supplied nonempty payload, while allowing the first send to make progress.
pub async fn partial_send<S: SocketTestScheduler>(
    scheduler: &S,
    socket: &S::Socket,
    buffers: [Vec<u8>; 2],
) {
    let total: usize = buffers.iter().map(Vec::len).sum();
    let pointers = buffers.each_ref().map(|buffer| buffer.as_ptr() as usize);
    let message = SocketMessage {
        buffers: IoBuffers::new(buffers),
        address: None,
    };
    let (result, mut message) = crate::io::socket_sendmsg(
        scheduler,
        socket,
        message,
        SocketSendFlags::default(),
        &Cancellation::never(),
    )
    .await;
    let bytes = result.unwrap();
    assert!(
        bytes > 0 && bytes < total,
        "fixture must force a short send"
    );
    assert_eq!(
        (0..message.buffers.buffer_count())
            .map(|i| message.buffers.buffer(i).len())
            .sum::<usize>(),
        total
    );
    message.buffers.advance(bytes);
    assert_eq!(
        (0..message.buffers.buffer_count())
            .map(|i| message.buffers.buffer(i).len())
            .sum::<usize>(),
        total - bytes
    );
    assert_eq!(
        message
            .buffers
            .into_inner()
            .each_ref()
            .map(|buffer| buffer.as_ptr() as usize),
        pointers
    );
}

/// Check prepared vectors, explicit advancement after short stream reads, collection ownership, laziness, and pre-cancellation on an isolated stream pair.
pub async fn stream<S: SocketTestScheduler>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    pre_cancelled(scheduler, sender, receiver).await;
    transfer(scheduler, sender, receiver).await;
}

/// Pre-cancelled prepared sends and receives return their original owners.
pub async fn pre_cancelled<S: SocketTestScheduler>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let token = Cancellation::new();
    token.cancel();
    let source = IoBuffers::new([b"head".to_vec(), b"body".to_vec()]);
    let original = source.buffer(0).as_ptr() as usize;
    let (result, message) = crate::io::socket_sendmsg(
        scheduler,
        sender,
        SocketMessage {
            buffers: source,
            address: None,
        },
        SocketSendFlags::default(),
        &token,
    )
    .await;
    cancelled(result);
    assert_eq!(message.buffers.buffer(0).as_ptr() as usize, original);
    let never = Cancellation::never();
    let (result, _) = crate::io::socket_sendmsg(
        scheduler,
        sender,
        SocketMessage {
            buffers: [vec![0xff]],
            address: None,
        },
        SocketSendFlags::default(),
        &never,
    )
    .await;
    assert_eq!(result.unwrap(), 1);
    let buffers = IoBuffersMut::new([vec![0x5a; 3], vec![0x5a; 3]]);
    let original = buffers.buffer(0).as_ptr() as usize;
    let (result, buffers) = crate::io::socket_recvmsg(
        scheduler,
        receiver,
        buffers,
        SocketRecvFlags::default(),
        &token,
    )
    .await;
    cancelled(result);
    assert_eq!(buffers.buffer(0).as_ptr() as usize, original);
    assert_eq!(buffers.into_inner(), [vec![0x5a; 3], vec![0x5a; 3]]);
    let (result, buffers) = crate::io::socket_recvmsg(
        scheduler,
        receiver,
        [vec![0; 1]],
        SocketRecvFlags::default(),
        &never,
    )
    .await;
    assert_eq!(result.unwrap().bytes, 1);
    assert_eq!(buffers, [vec![0xff]]);
}

/// Check prepared vectors, explicit advancement after short receives, collection ownership, and lazy submission on a fresh stream.
pub async fn transfer<S: SocketTestScheduler>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let header = b"head".to_vec();
    let body = b"body".to_vec();
    let original = header.as_ptr() as usize;
    // Separate arrays also exercise the no-prepared-vector fallback below.
    let mut message = SocketMessage {
        buffers: IoBuffers::new([header, body]),
        address: None,
    };
    drop(crate::io::socket_sendmsg(
        scheduler,
        sender,
        SocketMessage {
            buffers: [b"unpolled".to_vec()],
            address: None,
        },
        SocketSendFlags::default(),
        &never,
    ));
    let mut sent = 0;
    while sent < 8 {
        let (result, returned) = crate::io::socket_sendmsg(
            scheduler,
            sender,
            message,
            SocketSendFlags::default(),
            &never,
        )
        .await;
        let count = result.unwrap();
        assert!(count > 0 && count <= 8 - sent);
        message = returned;
        message.buffers.advance(count);
        sent += count;
    }
    assert_eq!(message.buffers.into_inner()[0].as_ptr() as usize, original);
    let mut buffers = IoBuffersMut::new([vec![0; 3], vec![0; 3]]);
    let pointer = buffers.buffer(0).as_ptr() as usize;
    drop(crate::io::socket_recvmsg(
        scheduler,
        receiver,
        [vec![0; 8]],
        SocketRecvFlags::default(),
        &never,
    ));
    let mut received = 0;
    while received < 6 {
        let (result, returned) = crate::io::socket_recvmsg(
            scheduler,
            receiver,
            buffers,
            SocketRecvFlags::default(),
            &never,
        )
        .await;
        let result = result.unwrap();
        assert!(result.bytes > 0 && result.bytes <= 6 - received);
        assert!(!result.truncated);
        assert!(!result.control_truncated);
        buffers = returned;
        buffers.advance(result.bytes);
        received += result.bytes;
    }
    let [first, second] = buffers.into_inner();
    assert_eq!(first.as_ptr() as usize, pointer);
    assert_eq!(first, b"hea");
    assert_eq!(second, b"dbo");
    // The prepared receive capacity is smaller than the queued stream: its first transfer must be short relative to the eight sent bytes.
    let mut tail = Vec::new();
    while tail.len() < 2 {
        let (result, buffers) = crate::io::socket_recvmsg(
            scheduler,
            receiver,
            [vec![0; 2 - tail.len()]],
            SocketRecvFlags::default(),
            &never,
        )
        .await;
        let bytes = result.unwrap().bytes;
        assert!(bytes > 0 && bytes <= 2 - tail.len());
        tail.extend_from_slice(&buffers[0][..bytes]);
    }
    assert_eq!(tail, b"dy");
}

/// Verify message boundaries, addresses, scatter/gather order, peek, truncation/discard, empty datagrams, and pending cancellation. Both sockets must be bound, unconnected datagram sockets with empty receive queues.
pub async fn datagrams<S: SocketTestScheduler + SocketLocalAddress>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    datagram_truncation(scheduler, sender, receiver).await;
    datagram_boundaries(scheduler, sender, receiver).await;
    pending_cancellation(scheduler, receiver).await;
}

/// Check source/destination addresses, scatter/gather ordering, peek, truncation, and discarded remainder on bound datagram sockets.
pub async fn datagram_truncation<S: SocketTestScheduler + SocketLocalAddress>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let destination = scheduler.socket_local_address(receiver).unwrap();
    let source = scheduler.socket_local_address(sender).unwrap();
    let message = SocketMessage {
        buffers: [b"abc".to_vec(), b"def".to_vec()],
        address: Some(destination),
    };
    let first = message.buffers[0].as_ptr() as usize;
    let (result, message) = crate::io::socket_sendmsg(
        scheduler,
        sender,
        message,
        SocketSendFlags { dont_route: true },
        &never,
    )
    .await;
    assert_eq!(result.unwrap(), 6);
    assert_eq!(message.buffers[0].as_ptr() as usize, first);
    assert_eq!(message.address, Some(destination));
    let (result, buffers) = crate::io::socket_recvmsg(
        scheduler,
        receiver,
        [vec![0; 2], vec![0; 2]],
        SocketRecvFlags { peek: true },
        &never,
    )
    .await;
    let result = result.unwrap();
    assert_eq!(result.bytes, 4);
    assert_eq!(result.address, Some(source));
    assert!(result.truncated);
    assert_eq!(buffers, [b"ab".to_vec(), b"cd".to_vec()]);
    let (result, buffers) = crate::io::socket_recvmsg(
        scheduler,
        receiver,
        IoBuffersMut::new([vec![0; 1], vec![0; 2]]),
        SocketRecvFlags::default(),
        &never,
    )
    .await;
    let result = result.unwrap();
    assert_eq!(result.bytes, 3);
    assert_eq!(result.address, Some(source));
    assert!(result.truncated);
    assert_eq!(buffers.into_inner(), [b"a".to_vec(), b"bc".to_vec()]);
    // A later, distinct datagram must be received intact: cancellation alone cannot prove that the truncated remainder was discarded by a queued backend.
    let (result, _) = crate::io::socket_sendmsg(
        scheduler,
        sender,
        SocketMessage {
            buffers: [b"sentinel".to_vec()],
            address: Some(destination),
        },
        SocketSendFlags::default(),
        &never,
    )
    .await;
    assert_eq!(result.unwrap(), 8);
    let (result, buffers) = crate::io::socket_recvmsg(
        scheduler,
        receiver,
        [vec![0; 16]],
        SocketRecvFlags::default(),
        &never,
    )
    .await;
    let result = result.unwrap();
    assert_eq!(result.bytes, 8);
    assert_eq!(result.address, Some(source));
    assert!(!result.truncated);
    assert!(!result.control_truncated);
    assert_eq!(&buffers[0][..result.bytes], b"sentinel");
}

/// Check empty datagrams and message boundaries on bound sockets.
pub async fn datagram_boundaries<S: SocketTestScheduler + SocketLocalAddress>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let destination = scheduler.socket_local_address(receiver).unwrap();
    let source = scheduler.socket_local_address(sender).unwrap();
    for payload in [Vec::<Vec<u8>>::new(), vec![vec![]], vec![b"next".to_vec()]] {
        let expected: Vec<_> = payload.iter().flatten().copied().collect();
        let (result, _) = crate::io::socket_sendmsg(
            scheduler,
            sender,
            SocketMessage {
                buffers: payload,
                address: Some(destination),
            },
            SocketSendFlags::default(),
            &never,
        )
        .await;
        assert_eq!(result.unwrap(), expected.len());
        let (result, buffers) = crate::io::socket_recvmsg(
            scheduler,
            receiver,
            vec![vec![0; 8]],
            SocketRecvFlags::default(),
            &never,
        )
        .await;
        let result = result.unwrap();
        assert_eq!(result.bytes, expected.len());
        assert_eq!(result.address, Some(source));
        assert!(!result.truncated);
        assert!(!result.control_truncated);
        assert_eq!(&buffers[0][..result.bytes], expected);
    }
}

/// Cancel a message receive on a quiet socket without changing its initialized input.
pub async fn pending_cancellation<S: SocketTestScheduler>(scheduler: &S, receiver: &S::Socket) {
    let token = Cancellation::new();
    let mut reading = pin!(crate::io::socket_recvmsg(
        scheduler,
        receiver,
        [vec![0x5a; 4]],
        SocketRecvFlags::default(),
        &token
    ));
    assert!(poll_once(&mut reading).await.is_none());
    token.cancel();
    let (result, buffers) = reading.await;
    cancelled(result);
    assert_eq!(buffers, [vec![0x5a; 4]]);
}

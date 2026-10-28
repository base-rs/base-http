// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Reusable IP socket assertions. Fixtures supply isolated connections and enforce test deadlines.

use crate::SocketTestScheduler;
use crate::{SocketIoFixture, SocketIoGuard};
use base_scheduler::*;
use futures_lite::future::{poll_once, zip};
use std::{
    future::{Future, poll_fn},
    net::Shutdown,
    pin::pin,
};

pub(crate) fn cancelled<T>(result: OperationResult<T>) {
    assert!(
        matches!(result, Err(OperationError::Cancelled(_))),
        "expected cooperative cancellation"
    );
}

/// Create an isolated loopback connection, exercising separate open/bind/listen/connect/accept operations and pre-cancellation. The listener is explicitly closed; the caller owns both returned ends.
pub async fn stream_pair<
    S: SocketOpen
        + SocketBind
        + SocketListen
        + SocketConnect
        + SocketAccept
        + SocketLocalAddress
        + SocketPeerAddress
        + SocketClose,
>(
    scheduler: &S,
    domain: SocketDomain,
) -> (S::Socket, S::Socket) {
    let never = Cancellation::never();
    let token = Cancellation::new();
    token.cancel();
    cancelled(
        scheduler
            .socket_open(
                domain,
                SocketType::Stream,
                Some(SocketProtocol::Tcp),
                &token,
            )
            .await,
    );
    drop(scheduler.socket_open(domain, SocketType::Stream, None, &never));
    let listener = scheduler
        .socket_open(
            domain,
            SocketType::Stream,
            Some(SocketProtocol::Tcp),
            &never,
        )
        .await
        .unwrap();
    let local = match domain {
        SocketDomain::Ipv4 => "127.0.0.1:0",
        SocketDomain::Ipv6 => "[::1]:0",
    }
    .parse()
    .unwrap();
    let unbound_address = scheduler.socket_local_address(&listener).ok();
    drop(scheduler.socket_bind(&listener, local, &never));
    cancelled(scheduler.socket_bind(&listener, local, &token).await);
    // Winsock rejects address lookup before binding; Unix generally reports port zero.
    assert_eq!(
        scheduler.socket_local_address(&listener).ok(),
        unbound_address
    );
    scheduler
        .socket_bind(&listener, local, &never)
        .await
        .unwrap();
    let local = scheduler.socket_local_address(&listener).unwrap();
    assert_ne!(local.port(), 0);
    drop(scheduler.socket_listen(&listener, 8, &never));
    cancelled(scheduler.socket_listen(&listener, 8, &token).await);
    scheduler.socket_listen(&listener, 8, &never).await.unwrap();
    let client = scheduler
        .socket_open(domain, SocketType::Stream, None, &never)
        .await
        .unwrap();
    // A prior explicit client binding must survive connection.
    scheduler
        .socket_bind(
            &client,
            match domain {
                SocketDomain::Ipv4 => "127.0.0.1:0",
                SocketDomain::Ipv6 => "[::1]:0",
            }
            .parse()
            .unwrap(),
            &never,
        )
        .await
        .unwrap();
    let client_address = scheduler.socket_local_address(&client).unwrap();
    drop(scheduler.socket_connect(&client, local, &never));
    cancelled(scheduler.socket_connect(&client, local, &token).await);
    assert!(scheduler.socket_peer_address(&client).is_err());
    drop(scheduler.socket_accept(&listener, &never));
    cancelled(scheduler.socket_accept(&listener, &token).await);
    let (connected, accepted) = zip(
        scheduler.socket_connect(&client, local, &never),
        scheduler.socket_accept(&listener, &never),
    )
    .await;
    connected.unwrap();
    let (server, peer) = accepted.unwrap();
    assert_eq!(peer, client_address);
    assert_eq!(
        scheduler.socket_local_address(&client).unwrap(),
        client_address
    );
    assert_eq!(scheduler.socket_peer_address(&client).unwrap(), local);
    assert_eq!(scheduler.socket_local_address(&server).unwrap(), local);
    assert_eq!(scheduler.socket_peer_address(&server).unwrap(), peer);
    scheduler.socket_close(listener).await.unwrap();
    (client, server)
}

/// Check lazy and pre-cancelled scalar I/O, allocation identity, short reads, readiness, write shutdown, and EOF on a fresh connected pair.
pub async fn stream<S: SocketTestScheduler + SocketShutdown>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    pre_cancelled(scheduler, sender, receiver).await;
    transfer(scheduler, sender, receiver).await;
    shutdown(scheduler, sender, receiver).await;
}

/// Pre-cancelled scalar I/O preserves the original owners and submits no payload.
pub async fn pre_cancelled<S: SocketTestScheduler>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let token = Cancellation::new();
    token.cancel();

    let source = b"abcdef".to_vec();
    let allocation = source.as_ptr() as usize;
    let (result, source) = crate::io::socket_write(scheduler, sender, source, &token).await;
    cancelled(result);
    assert_eq!(source.as_ptr() as usize, allocation);
    // A distinct sentinel exposes writes which incorrectly transferred bytes while reporting cancellation, and reads which consumed data despite pre-cancellation.
    let (result, _) = crate::io::socket_write(scheduler, sender, vec![0xff], &never).await;
    assert_eq!(result.unwrap(), 1);
    let target = vec![0x5a; 2];
    let target_allocation = target.as_ptr() as usize;
    let (result, target) = crate::io::socket_read(scheduler, receiver, target, &token).await;
    cancelled(result);
    assert_eq!(target.as_ptr() as usize, target_allocation);
    assert_eq!(target, vec![0x5a; 2]);
    let (result, buffer) = crate::io::socket_read(scheduler, receiver, vec![0; 1], &never).await;
    assert_eq!(result.unwrap(), 1);
    assert_eq!(buffer, vec![0xff]);
}

/// Check lazy scalar operations, short transfers, readiness, payload, and allocation identity on a fresh stream.
pub async fn transfer<S: SocketTestScheduler>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let mut source = b"abcdef".to_vec();
    let allocation = source.as_ptr() as usize;
    let mut target = vec![0; 2];
    let target_allocation = target.as_ptr() as usize;
    drop(crate::io::socket_write(
        scheduler,
        sender,
        b"unpolled".to_vec(),
        &never,
    ));
    let mut sent = 0;
    while sent < 6 {
        let (result, view) =
            crate::io::socket_write(scheduler, sender, source.slice(sent..), &never).await;
        let count = result.unwrap();
        assert!(count > 0 && count <= 6 - sent);
        sent += count;
        source = view.into_inner();
    }
    assert_eq!(source.as_ptr() as usize, allocation);

    drop(crate::io::socket_read(
        scheduler,
        receiver,
        vec![0; 6],
        &never,
    ));
    let mut received = Vec::new();
    while received.len() < 6 {
        let (result, returned) = crate::io::socket_read(scheduler, receiver, target, &never).await;
        let count = result.unwrap();
        assert!(count > 0 && count <= 2);
        assert_eq!(returned.as_ptr() as usize, target_allocation);
        assert_eq!(returned.len(), 2);
        received.extend_from_slice(&returned[..count]);
        target = returned;
    }
    assert_eq!(received, b"abcdef");
}

/// Check lazy write shutdown and EOF on a fresh stream.
pub async fn shutdown<S: SocketTestScheduler + SocketShutdown>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let target = vec![0; 2];
    let target_allocation = target.as_ptr() as usize;
    drop(scheduler.socket_shutdown(sender, Shutdown::Write));
    // The dropped shutdown must not prevent this write.
    let (result, _) = crate::io::socket_write(scheduler, sender, vec![7], &never).await;
    assert_eq!(result.unwrap(), 1);
    let (result, _) = crate::io::socket_read(scheduler, receiver, vec![0; 1], &never).await;
    assert_eq!(result.unwrap(), 1);
    scheduler
        .socket_shutdown(sender, Shutdown::Write)
        .await
        .unwrap();
    let (result, returned) = crate::io::socket_read(scheduler, receiver, target, &never).await;
    assert_eq!(result.unwrap(), 0);
    assert_eq!(returned.as_ptr() as usize, target_allocation);
}

/// Cancel a pending read on a quiet stream and check that initialized bytes and allocation identity survive.
pub async fn pending_read_cancellation<S: SocketTestScheduler>(scheduler: &S, socket: &S::Socket) {
    let token = Cancellation::new();
    let buffer = vec![0x5a; 8];
    let allocation = buffer.as_ptr() as usize;
    let mut reading = pin!(crate::io::socket_read(scheduler, socket, buffer, &token));
    assert!(
        poll_once(&mut reading).await.is_none(),
        "quiet stream read unexpectedly completed"
    );
    token.cancel();
    let (result, buffer) = reading.await;
    cancelled(result);
    assert_eq!(buffer.as_ptr() as usize, allocation);
    assert_eq!(buffer, vec![0x5a; 8]);
}

/// Cancel a pending borrowed read on a quiet stream without accessing its stack storage after suspension.
pub async fn pending_borrowed_cancellation<S: SocketTestScheduler>(
    scheduler: &S,
    socket: &S::Socket,
) {
    let token = Cancellation::new();
    let mut reader = scheduler.socket_stream_reader(socket).unwrap();
    let mut buffer = [0];
    {
        let mut waiting = pin!(reader.read_into(&mut buffer, &token));
        assert!(poll_once(&mut waiting).await.is_none());
        token.cancel();
        cancelled(waiting.await);
    }
    reader.close().await.unwrap();
}

/// Check that a failed write preserves the original owner. The fixture supplies a socket whose writes fail, without relying on portable error kinds for broken connections.
pub async fn write_error<S: SocketTestScheduler>(scheduler: &S, socket: &S::Socket) {
    let buffer = vec![7; 3];
    let allocation = buffer.as_ptr() as usize;
    let (result, buffer) =
        crate::io::socket_write(scheduler, socket, buffer, &Cancellation::never()).await;
    assert!(matches!(result, Err(OperationError::Io(_))));
    assert_eq!(buffer, vec![7; 3]);
    assert_eq!(buffer.as_ptr() as usize, allocation);
}

/// A successful already-started I/O attempt wins concurrent cancellation and returns its original input.
pub async fn completion_wins<F: SocketIoFixture>(fixture: &F) {
    let token = Cancellation::new();
    let buffer = fixture.input();
    let mut reading = pin!(fixture.operation(buffer, &token));
    assert!(poll_once(&mut reading).await.is_none());
    let mut starting = pin!(fixture.start());
    // Submission may itself await capacity. Keep driving the operation until the fixture reaches its access boundary.
    let mut guard = poll_fn(|context| {
        assert!(
            reading.as_mut().poll(context).is_pending(),
            "operation completed before fixture release"
        );
        starting.as_mut().poll(context)
    })
    .await;
    token.cancel();
    fixture.assert_retained();
    guard.release();
    let (result, buffer) = reading.await;
    fixture.assert_completed(&result.unwrap(), &buffer);
    drop(buffer);
    guard.retired().await;
    fixture.assert_released();
}

/// Abandoning an accepted operation cannot destroy its input while backend access remains possible.
pub async fn abandoned_operation<F: SocketIoFixture>(fixture: &F) {
    let token = Cancellation::new();
    let mut reading = Box::pin(fixture.operation(fixture.input(), &token));
    assert!(poll_once(&mut reading).await.is_none());
    let mut starting = pin!(fixture.start());
    let mut guard = poll_fn(|context| {
        assert!(
            reading.as_mut().poll(context).is_pending(),
            "operation completed before fixture release"
        );
        starting.as_mut().poll(context)
    })
    .await;
    drop(reading);
    fixture.assert_retained();
    guard.release();
    guard.retired().await;
    fixture.assert_released();
}

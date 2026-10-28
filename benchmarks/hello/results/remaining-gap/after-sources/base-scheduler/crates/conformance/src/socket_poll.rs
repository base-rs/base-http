// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Borrowed nonblocking I/O assertions. Fixtures provide fresh sockets and drive readiness while enforcing test deadlines.

use crate::SocketTestScheduler;
use crate::probe::WakeProbe;
use base_scheduler::*;
use futures_lite::future::poll_once;
use std::{
    future::Future,
    io::{IoSlice, IoSliceMut},
    pin::pin,
    sync::{Arc, atomic::Ordering},
    task::{Context, Waker},
};

fn cancelled<T>(result: OperationResult<T>) {
    assert!(matches!(result, Err(OperationError::Cancelled(_))));
}

/// With a queued datagram, a fresh borrowed message receive must attempt and complete in its first poll. Empty datagrams also consume a message and cannot be withheld across `Pending`.
pub async fn datagram_first_poll<S>(scheduler: &S, sender: &S::Socket, receiver: &S::Socket)
where
    S: SocketTestScheduler + SocketLocalAddress,
{
    let never = Cancellation::never();
    let destination = scheduler.socket_local_address(receiver).unwrap();
    let mut reader = scheduler.socket_packet_reader(receiver).unwrap();
    let mut writer = scheduler.socket_packet_writer(sender).unwrap();
    for payload in [b"".as_slice(), b"x"] {
        assert_eq!(
            writer
                .write(payload, Some(destination), &never)
                .await
                .unwrap(),
            payload.len()
        );
        let mut peek = [0];
        assert_eq!(
            reader
                .read_into_with_flags(&mut peek, SocketRecvFlags { peek: true }, &never)
                .await
                .unwrap()
                .bytes,
            payload.len()
        );
        let mut target = [0x5a];
        let result = poll_once(pin!(reader.read_into(&mut target, &never)))
            .await
            .expect("a borrowed receive deferred its first attempt on a queued packet")
            .unwrap();
        assert_eq!(result.bytes, payload.len());
        assert_eq!(&target[..payload.len()], payload);
    }
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}

/// Pre-cancelled scalar operations leave storage and the receive queue unchanged. Unpolled operations perform no I/O.
pub async fn scalar_pre_cancelled<S: SocketTestScheduler>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let token = Cancellation::new();
    token.cancel();
    let never = Cancellation::never();
    let mut target = [0x5a; 1];
    drop(crate::io::socket_send_poll(
        scheduler,
        sender,
        b"unpolled",
        SocketSendFlags::default(),
        &never,
    ));
    cancelled(
        crate::io::socket_send_poll(
            scheduler,
            sender,
            b"cancelled",
            SocketSendFlags::default(),
            &token,
        )
        .await,
    );
    assert_eq!(
        crate::io::socket_send_poll(
            scheduler,
            sender,
            &[0xff],
            SocketSendFlags::default(),
            &never
        )
        .await
        .unwrap(),
        1
    );
    drop(crate::io::socket_recv_poll(
        scheduler,
        receiver,
        &mut target,
        SocketRecvFlags::default(),
        &never,
    ));
    cancelled(
        crate::io::socket_recv_poll(
            scheduler,
            receiver,
            &mut target,
            SocketRecvFlags::default(),
            &token,
        )
        .await,
    );
    assert_eq!(target, [0x5a]);
    assert_eq!(
        crate::io::socket_recv_poll(
            scheduler,
            receiver,
            &mut target,
            SocketRecvFlags::default(),
            &never
        )
        .await
        .unwrap(),
        1
    );
    assert_eq!(target, [0xff]);
}

/// Stack buffers support short reads and semantic peek flags without losing stream data.
pub async fn scalar_transfer<S: SocketTestScheduler>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let source = *b"abcdef";
    let mut sent = 0;
    while sent < source.len() {
        let count = crate::io::socket_send_poll(
            scheduler,
            sender,
            &source[sent..],
            SocketSendFlags::default(),
            &never,
        )
        .await
        .unwrap();
        assert!(count > 0 && count <= source.len() - sent);
        sent += count;
    }
    let mut peek = [0; 1];
    assert_eq!(
        crate::io::socket_recv_poll(
            scheduler,
            receiver,
            &mut peek,
            SocketRecvFlags { peek: true },
            &never
        )
        .await
        .unwrap(),
        1
    );
    assert_eq!(peek, [b'a']);
    let mut target = [0; 6];
    let mut received = 0;
    while received < target.len() {
        let mut chunk = [0; 2];
        let count = crate::io::socket_recv_poll(
            scheduler,
            receiver,
            &mut chunk[..(6 - received).min(2)],
            SocketRecvFlags::default(),
            &never,
        )
        .await
        .unwrap();
        assert!(count > 0 && count <= 2);
        target[received..received + count].copy_from_slice(&chunk[..count]);
        received += count;
    }
    assert_eq!(target, source);
}

/// Quiet scalar receives must register a cancellation wakeup and leave initialized storage unchanged.
pub async fn scalar_cancellation<S: SocketTestScheduler>(scheduler: &S, receiver: &S::Socket) {
    let token = Cancellation::new();
    let mut target = [0x5a; 8];
    {
        let mut receiving = pin!(crate::io::socket_recv_poll(
            scheduler,
            receiver,
            &mut target,
            SocketRecvFlags::default(),
            &token
        ));
        let probe = Arc::new(WakeProbe::default());
        let waker = Waker::from(Arc::clone(&probe));
        assert!(
            receiving
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        token.cancel();
        assert!(
            probe.0.load(Ordering::SeqCst) > 0,
            "cancellation did not wake the receiver"
        );
        cancelled(receiving.await);
    }
    assert_eq!(target, [0x5a; 8]);
}

/// Dropping or forgetting a pending receive releases the borrow safely. Subsequent I/O must neither access its old stack bytes nor consume data on its behalf.
pub async fn scalar_abandonment<S: SocketTestScheduler>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let mut writer = scheduler.socket_stream_writer(sender).unwrap();
    for forget in [false, true] {
        let mut reader = scheduler.socket_stream_reader(receiver).unwrap();
        let mut old = [0x5a; 8];
        {
            let mut receiving = Box::pin(reader.read_into(&mut old, &never));
            assert!(poll_once(&mut receiving).await.is_none());
            if forget {
                std::mem::forget(receiving);
            } else {
                drop(receiving);
            }
        }
        old.fill(0xa5);
        // An abandoned readiness registration may keep this lane busy. Closing establishes retirement without relying on the forgotten future's destructor.
        reader.close().await.unwrap();
        drop(reader);
        let mut reader = scheduler.socket_stream_reader(receiver).unwrap();
        assert_eq!(writer.write(b"x", &never).await.unwrap(), 1);
        let mut target = [0; 1];
        assert_eq!(reader.read_into(&mut target, &never).await.unwrap(), 1);
        assert_eq!(target, *b"x");
        assert_eq!(old, [0xa5; 8]);
        reader.close().await.unwrap();
    }
    writer.close().await.unwrap();
}

/// A pending receive wakes when data arrives; write shutdown produces EOF without modifying storage.
pub async fn scalar_readiness_and_eof<S: SocketTestScheduler + SocketShutdown>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let mut target = [0x5a; 2];
    {
        let mut receiving = pin!(crate::io::socket_recv_poll(
            scheduler,
            receiver,
            &mut target,
            SocketRecvFlags::default(),
            &never
        ));
        assert!(poll_once(&mut receiving).await.is_none());
        assert_eq!(
            crate::io::socket_send_poll(
                scheduler,
                sender,
                b"x",
                SocketSendFlags::default(),
                &never
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(receiving.await.unwrap(), 1);
    }
    assert_eq!(target, [b'x', 0x5a]);
    scheduler
        .socket_shutdown(sender, std::net::Shutdown::Write)
        .await
        .unwrap();
    assert_eq!(
        crate::io::socket_recv_poll(
            scheduler,
            receiver,
            &mut target,
            SocketRecvFlags::default(),
            &never
        )
        .await
        .unwrap(),
        0
    );
    assert_eq!(target, [b'x', 0x5a]);
}

/// Pre-cancelled and unpolled vector operations preserve both the queue and initialized stack storage.
pub async fn message_pre_cancelled<S: SocketTestScheduler>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let token = Cancellation::new();
    token.cancel();
    let never = Cancellation::never();
    let source = [IoSlice::new(b"unpolled")];
    drop(crate::io::socket_sendmsg_poll(
        scheduler,
        sender,
        &source,
        None,
        SocketSendFlags::default(),
        &never,
    ));
    cancelled(
        crate::io::socket_sendmsg_poll(
            scheduler,
            sender,
            &source,
            None,
            SocketSendFlags::default(),
            &token,
        )
        .await,
    );
    assert_eq!(
        crate::io::socket_sendmsg_poll(
            scheduler,
            sender,
            &[IoSlice::new(&[0xff])],
            None,
            SocketSendFlags::default(),
            &never
        )
        .await
        .unwrap(),
        1
    );
    let mut target = [0x5a; 2];
    {
        let mut vectors = [IoSliceMut::new(&mut target)];
        drop(crate::io::socket_recvmsg_poll(
            scheduler,
            receiver,
            &mut vectors,
            SocketRecvFlags::default(),
            &never,
        ));
        cancelled(
            crate::io::socket_recvmsg_poll(
                scheduler,
                receiver,
                &mut vectors,
                SocketRecvFlags::default(),
                &token,
            )
            .await,
        );
    }
    assert_eq!(target, [0x5a; 2]);
    assert_eq!(
        crate::io::socket_recvmsg_poll(
            scheduler,
            receiver,
            &mut [IoSliceMut::new(&mut target)],
            SocketRecvFlags::default(),
            &never
        )
        .await
        .unwrap()
        .bytes,
        1
    );
    assert_eq!(target, [0xff, 0x5a]);
}

/// Stack vectors and payloads reach stream I/O with explicit advancement after short transfers. Neither send nor receive advances the caller's vector entries.
pub async fn message_transfer<S: SocketTestScheduler>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let header = *b"head";
    let body = *b"body";
    let mut source = [IoSlice::new(&header), IoSlice::new(&body)];
    let mut pending = &mut source[..];
    while !pending.is_empty() {
        let before: usize = pending.iter().map(|buffer| buffer.len()).sum();
        let count = crate::io::socket_sendmsg_poll(
            scheduler,
            sender,
            pending,
            None,
            SocketSendFlags::default(),
            &never,
        )
        .await
        .unwrap();
        assert!(count > 0 && count <= before);
        assert_eq!(
            pending.iter().map(|buffer| buffer.len()).sum::<usize>(),
            before
        );
        IoSlice::advance_slices(&mut pending, count);
    }
    let mut first = [0x5a; 3];
    let mut second = [0x5a; 5];
    {
        let mut vectors = [IoSliceMut::new(&mut first), IoSliceMut::new(&mut second)];
        let mut pending = &mut vectors[..];
        let mut received = 0;
        while !pending.is_empty() {
            let before: usize = pending.iter().map(|buffer| buffer.len()).sum();
            let result = crate::io::socket_recvmsg_poll(
                scheduler,
                receiver,
                pending,
                SocketRecvFlags::default(),
                &never,
            )
            .await
            .unwrap();
            assert!(result.bytes > 0 && result.bytes <= before);
            assert_eq!(
                pending.iter().map(|buffer| buffer.len()).sum::<usize>(),
                before
            );
            received += result.bytes;
            IoSliceMut::advance_slices(&mut pending, result.bytes);
        }
        assert_eq!(received, 8);
    }
    assert_eq!(first, *b"hea");
    assert_eq!(second, *b"dbody");
}

/// A quiet vector receive registers cancellation and readiness wakeups; its stack vectors remain unchanged on cancellation.
pub async fn message_cancellation<S: SocketTestScheduler>(scheduler: &S, receiver: &S::Socket) {
    let token = Cancellation::new();
    let mut target = [0x5a; 8];
    {
        let mut vectors = [IoSliceMut::new(&mut target)];
        let pointer = vectors[0].as_ptr();
        {
            let mut receiving = pin!(crate::io::socket_recvmsg_poll(
                scheduler,
                receiver,
                &mut vectors,
                SocketRecvFlags::default(),
                &token
            ));
            let probe = Arc::new(WakeProbe::default());
            let waker = Waker::from(Arc::clone(&probe));
            assert!(
                receiving
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            token.cancel();
            assert!(probe.0.load(Ordering::SeqCst) > 0);
            cancelled(receiving.await);
        }
        assert_eq!(vectors[0].as_ptr(), pointer);
        assert_eq!(vectors[0].len(), 8);
    }
    assert_eq!(target, [0x5a; 8]);
}

/// Neither dropping nor forgetting a pending vector receive leaves access to its stack vectors or payload.
pub async fn message_abandonment<S: SocketTestScheduler>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let mut writer = scheduler.socket_stream_writer(sender).unwrap();
    for forget in [false, true] {
        let mut reader = scheduler.socket_stream_reader(receiver).unwrap();
        let mut old = [0x5a; 8];
        {
            let mut vectors = [IoSliceMut::new(&mut old)];
            let mut receiving = Box::pin(reader.read_vectored(&mut vectors, &never));
            assert!(poll_once(&mut receiving).await.is_none());
            if forget {
                std::mem::forget(receiving);
            } else {
                drop(receiving);
            }
        }
        old.fill(0xa5);
        // An abandoned readiness registration may keep this lane busy. Closing establishes retirement without relying on the forgotten future's destructor.
        reader.close().await.unwrap();
        drop(reader);
        let mut reader = scheduler.socket_stream_reader(receiver).unwrap();
        assert_eq!(writer.write(b"x", &never).await.unwrap(), 1);
        let mut target = [0; 1];
        assert_eq!(reader.read_into(&mut target, &never).await.unwrap(), 1);
        assert_eq!(target, *b"x");
        assert_eq!(old, [0xa5; 8]);
        reader.close().await.unwrap();
    }
    writer.close().await.unwrap();
}

/// A pending vector receive resumes on readiness and preserves initialized bytes beyond its returned count.
pub async fn message_readiness<S: SocketTestScheduler>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let mut first = [0x5a; 1];
    let mut second = [0x5a; 2];
    {
        let mut vectors = [IoSliceMut::new(&mut first), IoSliceMut::new(&mut second)];
        let mut receiving = pin!(crate::io::socket_recvmsg_poll(
            scheduler,
            receiver,
            &mut vectors,
            SocketRecvFlags::default(),
            &never
        ));
        assert!(poll_once(&mut receiving).await.is_none());
        assert_eq!(
            crate::io::socket_sendmsg_poll(
                scheduler,
                sender,
                &[IoSlice::new(b"x")],
                None,
                SocketSendFlags::default(),
                &never
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(receiving.await.unwrap().bytes, 1);
    }
    assert_eq!(first, *b"x");
    assert_eq!(second, [0x5a; 2]);
}

/// Addressed borrowed messages preserve peek, truncation, discard, source metadata, and datagram boundaries.
pub async fn datagram_truncation<S: SocketTestScheduler + SocketLocalAddress>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let destination = scheduler.socket_local_address(receiver).unwrap();
    let source = scheduler.socket_local_address(sender).unwrap();
    let header = *b"abc";
    let body = *b"def";
    assert_eq!(
        crate::io::socket_sendmsg_poll(
            scheduler,
            sender,
            &[IoSlice::new(&header), IoSlice::new(&body)],
            Some(destination),
            SocketSendFlags { dont_route: true },
            &never
        )
        .await
        .unwrap(),
        6
    );
    let mut first = [0; 2];
    let mut second = [0; 2];
    let result = crate::io::socket_recvmsg_poll(
        scheduler,
        receiver,
        &mut [IoSliceMut::new(&mut first), IoSliceMut::new(&mut second)],
        SocketRecvFlags { peek: true },
        &never,
    )
    .await
    .unwrap();
    assert_eq!(result.bytes, 4);
    assert_eq!(result.address, Some(source));
    assert!(result.truncated);
    assert_eq!(first, *b"ab");
    assert_eq!(second, *b"cd");
    let mut target = [0; 3];
    let result = crate::io::socket_recvmsg_poll(
        scheduler,
        receiver,
        &mut [IoSliceMut::new(&mut target)],
        SocketRecvFlags::default(),
        &never,
    )
    .await
    .unwrap();
    assert_eq!(result.bytes, 3);
    assert!(result.truncated);
    assert_eq!(target, *b"abc");
    assert_eq!(
        crate::io::socket_sendmsg_poll(
            scheduler,
            sender,
            &[IoSlice::new(b"next")],
            Some(destination),
            SocketSendFlags::default(),
            &never
        )
        .await
        .unwrap(),
        4
    );
    let mut target = [0x5a; 8];
    let result = crate::io::socket_recvmsg_poll(
        scheduler,
        receiver,
        &mut [IoSliceMut::new(&mut target)],
        SocketRecvFlags::default(),
        &never,
    )
    .await
    .unwrap();
    assert_eq!(result.bytes, 4);
    assert_eq!(result.address, Some(source));
    assert!(!result.truncated && !result.control_truncated);
    assert_eq!(&target[..4], b"next");
    assert_eq!(&target[4..], &[0x5a; 4]);
}

/// Empty vector arrays still represent real empty datagrams and provide a usable zero-capacity receive.
pub async fn datagram_empty<S: SocketTestScheduler + SocketLocalAddress>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let destination = scheduler.socket_local_address(receiver).unwrap();
    let source = scheduler.socket_local_address(sender).unwrap();
    assert_eq!(
        crate::io::socket_sendmsg_poll(
            scheduler,
            sender,
            &[],
            Some(destination),
            SocketSendFlags::default(),
            &never
        )
        .await
        .unwrap(),
        0
    );
    let result = crate::io::socket_recvmsg_poll(
        scheduler,
        receiver,
        &mut [],
        SocketRecvFlags::default(),
        &never,
    )
    .await
    .unwrap();
    assert_eq!(result.bytes, 0);
    // Zero-capacity receive consumption, sender metadata, and truncation reporting are OS-specific.
    assert_eq!(
        crate::io::socket_sendmsg_poll(
            scheduler,
            sender,
            &[IoSlice::new(&[])],
            Some(destination),
            SocketSendFlags::default(),
            &never
        )
        .await
        .unwrap(),
        0
    );
    let mut target = [0x5a; 1];
    let result = crate::io::socket_recvmsg_poll(
        scheduler,
        receiver,
        &mut [IoSliceMut::new(&mut target)],
        SocketRecvFlags::default(),
        &never,
    )
    .await
    .unwrap();
    assert_eq!(result.bytes, 0);
    assert_eq!(result.address, Some(source));
    assert_eq!(target, [0x5a]);
}

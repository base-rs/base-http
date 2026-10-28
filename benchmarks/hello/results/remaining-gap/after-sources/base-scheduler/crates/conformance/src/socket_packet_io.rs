// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Scheduler-bound packet handles over fresh, bound, unconnected datagram pairs.

use crate::socket::cancelled;
use base_scheduler::*;
use futures_lite::future::{poll_once, zip};
use std::{
    io::{IoSlice, IoSliceMut},
    pin::pin,
};

/// Receive sender metadata, preserve packet boundaries, report truncation, and send empty datagrams through reusable borrowed handles.
pub async fn borrowed<S>(scheduler: &S, sender: &S::Socket, receiver: &S::Socket)
where
    S: SocketPacketReading + SocketPacketWriting + SocketLocalAddress,
    S::Socket: 'static,
{
    let never = Cancellation::never();
    let address = scheduler.socket_local_address(receiver).unwrap();
    let peer = scheduler.socket_local_address(sender).unwrap();
    let mut writer = scheduler.socket_packet_writer(sender).unwrap();
    let mut reader = scheduler.socket_packet_reader(receiver).unwrap();
    let mut prefix = [0; 2];
    let mut suffix = [0; 2];
    let mut vectors = [IoSliceMut::new(&mut prefix), IoSliceMut::new(&mut suffix)];
    let sending = [IoSlice::new(b"abc"), IoSlice::new(b"def")];
    let (read, sent) = zip(
        reader.read_vectored(&mut vectors, &never),
        writer.write_vectored(&sending, Some(address), &never),
    )
    .await;
    let read = read.unwrap();
    assert_eq!(sent.unwrap(), 6);
    assert_eq!(read.bytes, 4);
    assert_eq!(read.address, Some(peer));
    assert!(read.truncated);
    assert!(!read.control_truncated);
    assert_eq!((&prefix, &suffix), (b"ab", b"cd"));
    let mut bytes = [0; 8];
    let (read, sent) = zip(
        reader.read_into(&mut bytes, &never),
        writer.write(b"next", Some(address), &never),
    )
    .await;
    let read = read.unwrap();
    assert_eq!(sent.unwrap(), 4);
    assert_eq!(read.bytes, 4);
    assert_eq!(&bytes[..read.bytes], b"next");
    assert!(!read.truncated);
    let (read, sent) = zip(
        reader.read_into(&mut bytes, &never),
        writer.write(b"", Some(address), &never),
    )
    .await;
    assert_eq!(sent.unwrap(), 0);
    let read = read.unwrap();
    assert_eq!(read.bytes, 0);
    assert_eq!(read.address, Some(peer));
    assert!(!read.truncated);
    let source = b"owned".to_vec();
    let pointer = source.as_ptr() as usize;
    let (read, (sent, source)) = zip(
        reader.read_into(&mut bytes, &never),
        writer.write_owned(source, Some(address), &never),
    )
    .await;
    assert_eq!(sent.unwrap(), 5);
    assert_eq!(source.as_ptr() as usize, pointer);
    assert_eq!(read.unwrap().bytes, 5);
    assert_eq!(&bytes[..5], b"owned");
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}

/// Owned packets retain their sender, empty payload, truncated prefix, and independent pool ownership after reader retirement.
pub async fn owned<S>(scheduler: &S, sender: &S::Socket, receiver: &S::Socket)
where
    S: SocketPacketReading + SocketPacketWriting + SocketLocalAddress,
    S::Socket: 'static,
{
    let never = Cancellation::never();
    let address = scheduler.socket_local_address(receiver).unwrap();
    let peer = scheduler.socket_local_address(sender).unwrap();
    let pool = scheduler.buffer_pool(BufferPoolOptions::new(2, 4)).unwrap();
    let mut reader = scheduler
        .socket_packet_reader_with_pool(receiver, pool.clone())
        .unwrap();
    let mut writer = scheduler.socket_packet_writer(sender).unwrap();
    let (first, sent) = zip(
        reader.read(&never),
        writer.write(b"abcdef", Some(address), &never),
    )
    .await;
    assert_eq!(sent.unwrap(), 6);
    let mut first = first.unwrap().unwrap();
    assert_eq!(first.payload(), b"abcd");
    assert!(first.truncated());
    assert_eq!(first.address().unwrap().to_socket_address(), peer);
    first.payload_mut()[0] = b'A';
    let first = first.into_payload().freeze();
    let (empty, sent) = zip(
        reader.read(&never),
        writer.write(b"", Some(address), &never),
    )
    .await;
    assert_eq!(sent.unwrap(), 0);
    let empty = empty.unwrap().unwrap();
    assert!(empty.payload().is_empty());
    assert!(!empty.truncated());
    assert_eq!(empty.address().unwrap().to_socket_address(), peer);
    assert!(pool.try_acquire().is_none());
    let cancellation = Cancellation::new();
    {
        let mut read = pin!(reader.read(&cancellation));
        assert!(poll_once(read.as_mut()).await.is_none());
        cancellation.cancel();
        cancelled(read.await);
    }
    reader.close().await.unwrap();
    writer.close().await.unwrap();
    assert_eq!(first.as_ref(), b"Abcd");
    assert!(empty.payload().is_empty());
    drop((first, empty));
    assert!(pool.try_acquire().is_some());
    assert!(reader.read(&never).await.unwrap().is_none());
}

/// Cancel and abandon pending reads, then reuse the handles; pre-cancelled writes preserve their owner and emit no datagram.
pub async fn cancellation<S>(scheduler: &S, sender: &S::Socket, receiver: &S::Socket)
where
    S: SocketPacketReading + SocketPacketWriting + SocketLocalAddress,
    S::Socket: 'static,
{
    let never = Cancellation::never();
    let address = scheduler.socket_local_address(receiver).unwrap();
    let pool = scheduler
        .buffer_pool(BufferPoolOptions::new(1, 16))
        .unwrap();
    let mut reader = scheduler
        .socket_packet_reader_with_pool(receiver, pool.clone())
        .unwrap();
    let mut writer = scheduler.socket_packet_writer(sender).unwrap();
    let cancellation = Cancellation::new();
    {
        let mut read = pin!(reader.read(&cancellation));
        assert!(poll_once(read.as_mut()).await.is_none());
        assert!(
            pool.try_acquire().is_some(),
            "idle packet readers must return unused leases"
        );
        cancellation.cancel();
        cancelled(read.await);
    }
    let mut bytes = [0; 16];
    cancelled(reader.read_into(&mut bytes, &cancellation).await);
    cancelled(reader.read(&cancellation).await);
    cancelled(
        writer
            .write(b"cancelled", Some(address), &cancellation)
            .await,
    );
    cancelled(
        writer
            .write_vectored(&[IoSlice::new(b"cancelled")], Some(address), &cancellation)
            .await,
    );
    let source = b"cancelled".to_vec();
    let pointer = source.as_ptr() as usize;
    let (result, source) = writer
        .write_owned(source, Some(address), &cancellation)
        .await;
    cancelled(result);
    assert_eq!(source.as_ptr() as usize, pointer);
    {
        let mut read = pin!(reader.read_into(&mut bytes, &never));
        assert!(poll_once(read.as_mut()).await.is_none());
    }
    {
        let mut read = pin!(reader.read(&never));
        assert!(poll_once(read.as_mut()).await.is_none());
    }
    assert!(pool.try_acquire().is_some());
    let (read, sent) = zip(
        reader.read(&never),
        writer.write(b"sentinel", Some(address), &never),
    )
    .await;
    assert_eq!(sent.unwrap(), 8);
    assert_eq!(read.unwrap().unwrap().payload(), b"sentinel");
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}

/// Generate individually reported packet-handle cases. The runner supplies a fresh isolated bound datagram pair, drives I/O, enforces a deadline, and cleans up both sockets.
#[macro_export]
macro_rules! socket_packet_io_tests {
    (run: $run:path $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            borrowed => |scheduler, sender, receiver| {
                $crate::socket_packet_io::borrowed(scheduler, sender, receiver).await;
            },
            owned => |scheduler, sender, receiver| {
                $crate::socket_packet_io::owned(scheduler, sender, receiver).await;
            },
            cancellation => |scheduler, sender, receiver| {
                $crate::socket_packet_io::cancellation(scheduler, sender, receiver).await;
            },
        }
    };
}

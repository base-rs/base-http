// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![cfg(target_os = "linux")]

use base_scheduler::*;
use base_scheduler_iouring::{IoUringDriver, IoUringOptions, IoUringSelector};
use base_scheduler_selector::NativeSocket;
use futures_lite::future::{block_on, poll_once};
use std::{
    future::Future,
    io::{self, IoSlice, IoSliceMut},
    pin::pin,
    time::{Duration, Instant},
};

fn drive<F: Future>(driver: &mut IoUringDriver, future: F) -> F::Output {
    let mut future = pin!(future);
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(result) = block_on(poll_once(&mut future)) {
            return result;
        }
        assert!(
            Instant::now() < deadline,
            "resource operation did not retire"
        );
        driver.turn(Some(Duration::from_millis(10))).unwrap();
    }
}

fn run(test: impl AsyncFnOnce(&IoUringSelector)) {
    let (scheduler, mut driver) = IoUringSelector::new(IoUringOptions {
        entries: 16,
        ..Default::default()
    })
    .unwrap();
    drive(&mut driver, test(&scheduler));
    drive(&mut driver, scheduler.shutdown());
}

async fn stream_pair(scheduler: &IoUringSelector) -> (NativeSocket, NativeSocket) {
    base_scheduler_conformance::socket::stream_pair(scheduler, SocketDomain::Ipv4).await
}

async fn packet_pair(scheduler: &IoUringSelector) -> (NativeSocket, NativeSocket) {
    let never = Cancellation::never();
    let sender = scheduler
        .socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never)
        .await
        .unwrap();
    let receiver = scheduler
        .socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never)
        .await
        .unwrap();
    for socket in [&sender, &receiver] {
        scheduler
            .socket_bind(socket, "127.0.0.1:0".parse().unwrap(), &never)
            .await
            .unwrap();
    }
    (sender, receiver)
}

#[test]
fn stream_flags_owned_vectors_and_default_pool_preserve_byte_order() {
    run(async |scheduler| {
        let (sender, receiver) = stream_pair(scheduler).await;
        let never = Cancellation::never();
        let cancelled = Cancellation::new();
        cancelled.cancel();
        let mut reader = scheduler.socket_stream_reader(&receiver).unwrap();
        let mut writer = scheduler.socket_stream_writer(&sender).unwrap();
        assert!(scheduler.socket_packet_reader(&sender).is_err());
        assert!(scheduler.socket_packet_writer(&sender).is_err());
        assert_eq!(reader.read_owned(Vec::new(), &never).await.0.unwrap(), 0);
        assert!(reader.read_owned(Vec::new(), &cancelled).await.0.is_err());
        assert!(reader.read_owned(vec![0; 1], &cancelled).await.0.is_err());
        assert!(
            reader
                .read_owned_vectored(vec![vec![0]], SocketRecvFlags::default(), &cancelled)
                .await
                .0
                .is_err()
        );
        assert_eq!(
            reader
                .read_owned_vectored(vec![Vec::new()], SocketRecvFlags::default(), &never)
                .await
                .0
                .unwrap()
                .bytes,
            0
        );
        assert_eq!(
            reader
                .read_vectored_with_flags(&mut [], SocketRecvFlags::default(), &never)
                .await
                .unwrap()
                .bytes,
            0
        );
        assert!(
            reader
                .read_vectored_with_flags(&mut [], SocketRecvFlags::default(), &cancelled)
                .await
                .is_err()
        );
        writer
            .write_with_flags(b"peek", SocketSendFlags::default(), &never)
            .await
            .unwrap();
        let (result, parts) = reader
            .read_owned_vectored(
                vec![vec![0; 2], vec![0; 2]],
                SocketRecvFlags { peek: true },
                &never,
            )
            .await;
        assert_eq!(result.unwrap().bytes, 4);
        assert_eq!(parts, [b"pe", b"ek"]);
        let mut bytes = [0; 4];
        assert_eq!(
            reader
                .read_vectored(&mut [IoSliceMut::new(&mut bytes)], &never)
                .await
                .unwrap(),
            4
        );
        assert_eq!(&bytes, b"peek");
        writer.write(b"pool", &never).await.unwrap();
        assert_eq!(
            reader.read(&never).await.unwrap().unwrap().as_ref(),
            b"pool"
        );
        writer.close().await.unwrap();
        scheduler
            .socket_shutdown(&sender, std::net::Shutdown::Write)
            .await
            .unwrap();
        assert_eq!(reader.read_owned(vec![0; 1], &never).await.0.unwrap(), 0);
        assert_eq!(reader.read_owned(vec![0; 1], &never).await.0.unwrap(), 0);
        assert!(reader.read(&never).await.unwrap().is_none());
        reader.close().await.unwrap();
        assert!(reader.read_into(&mut bytes, &never).await.is_err());
        assert!(reader.read_vectored(&mut [], &never).await.is_err());
        assert!(
            reader
                .read_vectored_with_flags(&mut [], SocketRecvFlags::default(), &never)
                .await
                .is_err()
        );
        assert!(reader.read_owned(vec![0], &never).await.0.is_err());
        assert!(
            reader
                .read_owned_vectored(vec![vec![0]], SocketRecvFlags::default(), &never)
                .await
                .0
                .is_err()
        );
        assert!(
            writer
                .write_with_flags(b"closed", SocketSendFlags::default(), &never)
                .await
                .is_err()
        );
        assert!(writer.write_owned(vec![0], &never).await.0.is_err());
        assert!(
            writer
                .write_owned_vectored(vec![vec![0]], SocketSendFlags::default(), &never)
                .await
                .0
                .is_err()
        );
        assert!(writer.write_vectored(&[], &never).await.is_err());
        drop((reader, writer));
        scheduler.socket_close(sender).await.unwrap();
        scheduler.socket_close(receiver).await.unwrap();
    });
}

#[test]
fn packet_flags_preserve_addresses_and_owned_ranges_after_close_or_cancellation() {
    run(async |scheduler| {
        let (sender, receiver) = packet_pair(scheduler).await;
        let never = Cancellation::never();
        let cancelled = Cancellation::new();
        cancelled.cancel();
        let destination = scheduler.socket_local_address(&receiver).unwrap();
        let source = scheduler.socket_local_address(&sender).unwrap();
        let mut reader = scheduler.socket_packet_reader(&receiver).unwrap();
        let mut writer = scheduler.socket_packet_writer(&sender).unwrap();
        assert!(scheduler.socket_stream_reader(&sender).is_err());
        assert!(scheduler.socket_stream_writer(&sender).is_err());
        assert!(
            reader
                .read_owned_vectored(vec![vec![0]], SocketRecvFlags::default(), &cancelled)
                .await
                .0
                .is_err()
        );
        assert!(
            reader
                .read_vectored_with_flags(&mut [], SocketRecvFlags::default(), &cancelled)
                .await
                .is_err()
        );
        assert!(
            writer
                .write_vectored_with_flags(
                    &[],
                    Some(destination),
                    SocketSendFlags::default(),
                    &cancelled
                )
                .await
                .is_err()
        );
        assert!(
            writer
                .write_owned_vectored(
                    vec![vec![1]],
                    Some(destination),
                    SocketSendFlags::default(),
                    &cancelled
                )
                .await
                .0
                .is_err()
        );
        writer
            .write_vectored_with_flags(
                &[IoSlice::new(b"ab"), IoSlice::new(b"cd")],
                Some(destination),
                SocketSendFlags::default(),
                &never,
            )
            .await
            .unwrap();
        let (result, parts) = reader
            .read_owned_vectored(
                vec![vec![0; 2], vec![0; 2]],
                SocketRecvFlags { peek: true },
                &never,
            )
            .await;
        let result = result.unwrap();
        assert_eq!(result.bytes, 4);
        assert_eq!(result.address, Some(source));
        assert_eq!(parts, [b"ab", b"cd"]);
        let packet = reader.read(&never).await.unwrap().unwrap();
        assert_eq!(packet.payload(), b"abcd");
        assert_eq!(packet.address().unwrap().to_socket_address(), source);
        writer
            .write_owned_vectored(
                vec![b"ef".to_vec()],
                Some(destination),
                SocketSendFlags::default(),
                &never,
            )
            .await
            .0
            .unwrap();
        let mut bytes = [0; 2];
        assert_eq!(
            reader
                .read_vectored(&mut [IoSliceMut::new(&mut bytes)], &never)
                .await
                .unwrap()
                .bytes,
            2
        );
        assert_eq!(&bytes, b"ef");
        reader.close().await.unwrap();
        writer.close().await.unwrap();
        assert!(reader.read(&never).await.unwrap().is_none());
        assert!(reader.read_into(&mut bytes, &never).await.is_err());
        assert!(
            reader
                .read_vectored_with_flags(&mut [], SocketRecvFlags::default(), &never)
                .await
                .is_err()
        );
        assert!(
            reader
                .read_owned_vectored(vec![vec![0]], SocketRecvFlags::default(), &never)
                .await
                .0
                .is_err()
        );
        assert!(
            writer
                .write(b"closed", Some(destination), &never)
                .await
                .is_err()
        );
        assert!(
            writer
                .write_vectored_with_flags(
                    &[],
                    Some(destination),
                    SocketSendFlags::default(),
                    &never
                )
                .await
                .is_err()
        );
        assert!(
            writer
                .write_owned_vectored(
                    vec![vec![0]],
                    Some(destination),
                    SocketSendFlags::default(),
                    &never
                )
                .await
                .0
                .is_err()
        );
        drop((reader, writer));
        scheduler.socket_close(sender).await.unwrap();
        scheduler.socket_close(receiver).await.unwrap();
    });
}

#[test]
fn each_stream_write_form_follows_accepted_output_without_reordering() {
    run(async |scheduler| {
        let (sender, receiver) = stream_pair(scheduler).await;
        let never = Cancellation::never();
        let mut writer = scheduler
            .socket_stream_writer_with_buffer::<Vec<u8>>(&sender)
            .unwrap();
        let mut reader = scheduler.socket_stream_reader(&receiver).unwrap();
        writer.submit(b"a".to_vec(), &never).await.unwrap();
        writer.write(b"b", &never).await.unwrap();
        writer.submit(b"c".to_vec(), &never).await.unwrap();
        writer
            .write_vectored(&[IoSlice::new(b"d")], &never)
            .await
            .unwrap();
        writer.submit(b"e".to_vec(), &never).await.unwrap();
        writer
            .write_with_flags(b"f", SocketSendFlags::default(), &never)
            .await
            .unwrap();
        writer.submit(b"g".to_vec(), &never).await.unwrap();
        writer.write_owned(b"h".to_vec(), &never).await.0.unwrap();
        writer.submit(b"i".to_vec(), &never).await.unwrap();
        writer
            .write_owned_vectored(vec![b"j".to_vec()], SocketSendFlags::default(), &never)
            .await
            .0
            .unwrap();
        let mut bytes = [0; 10];
        let mut received = 0;
        while received < bytes.len() {
            let count = reader
                .read_into(&mut bytes[received..], &never)
                .await
                .unwrap();
            assert!(count > 0);
            received += count;
        }
        assert_eq!(&bytes, b"abcdefghij");
        writer.close().await.unwrap();
        reader.close().await.unwrap();
        drop((reader, writer));
        scheduler.socket_close(sender).await.unwrap();
        scheduler.socket_close(receiver).await.unwrap();
    });
}

#[test]
fn cancelled_successors_preserve_their_owners_and_leave_cleanup_available() {
    for form in 0..5 {
        run(async |scheduler| {
            let (sender, receiver) = stream_pair(scheduler).await;
            let never = Cancellation::never();
            let cancelled = Cancellation::new();
            cancelled.cancel();
            let mut writer = scheduler
                .socket_stream_writer_with_buffer::<Vec<u8>>(&sender)
                .unwrap();
            writer
                .submit(vec![0x55; 8 * 1024 * 1024], &never)
                .await
                .unwrap();
            match form {
                0 => assert!(writer.write(b"next", &cancelled).await.is_err()),
                1 => assert!(
                    writer
                        .write_vectored(&[IoSlice::new(b"next")], &cancelled)
                        .await
                        .is_err()
                ),
                2 => assert!(
                    writer
                        .write_with_flags(b"next", SocketSendFlags::default(), &cancelled)
                        .await
                        .is_err()
                ),
                3 => {
                    let bytes = b"next".to_vec();
                    let pointer = bytes.as_ptr();
                    let (result, bytes) = writer.write_owned(bytes, &cancelled).await;
                    assert!(result.is_err());
                    assert_eq!(bytes.as_ptr(), pointer);
                }
                _ => {
                    let (result, parts) = writer
                        .write_owned_vectored(
                            vec![b"next".to_vec()],
                            SocketSendFlags::default(),
                            &cancelled,
                        )
                        .await;
                    assert!(result.is_err());
                    assert_eq!(parts, [b"next"]);
                }
            }
            writer.close().await.unwrap();
            drop(writer);
            scheduler.socket_close(sender).await.unwrap();
            scheduler.socket_close(receiver).await.unwrap();
        });
    }
}

#[test]
fn stream_storage_supports_owned_peeks_cancellation_and_every_eof_form() {
    run(async |scheduler| {
        let (sender, receiver) = stream_pair(scheduler).await;
        let never = Cancellation::never();
        let pool = scheduler
            .buffer_pool(BufferPoolOptions::new(4, 64))
            .unwrap();
        let mut reader = scheduler
            .socket_stream_reader_with_pool(&receiver, pool)
            .unwrap();
        let mut writer = scheduler.socket_stream_writer(&sender).unwrap();
        writer.write(b"start", &never).await.unwrap();
        assert_eq!(
            reader.read(&never).await.unwrap().unwrap().as_ref(),
            b"start"
        );
        assert_eq!(reader.read_owned(Vec::new(), &never).await.0.unwrap(), 0);
        let cancelled = Cancellation::new();
        cancelled.cancel();
        assert!(reader.read_vectored(&mut [], &cancelled).await.is_err());
        assert!(
            reader
                .read_owned_vectored(vec![vec![0]], SocketRecvFlags::default(), &cancelled)
                .await
                .0
                .is_err()
        );
        let cancellation = Cancellation::new();
        {
            let mut reading = Box::pin(reader.read_owned_vectored(
                vec![vec![0; 2]],
                SocketRecvFlags::default(),
                &cancellation,
            ));
            assert!(poll_once(reading.as_mut()).await.is_none());
            cancellation.cancel();
            assert!(reading.await.0.is_err());
        }
        writer.write(b"data", &never).await.unwrap();
        let (result, parts) = reader
            .read_owned_vectored(vec![vec![0]], SocketRecvFlags { peek: true }, &never)
            .await;
        assert_eq!(result.unwrap().bytes, 1);
        assert_eq!(parts, [b"d"]);
        let (result, bytes) = reader.read_owned(vec![0; 2], &never).await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(bytes, b"da");
        let mut bytes = [0; 2];
        assert_eq!(
            reader
                .read_vectored(&mut [IoSliceMut::new(&mut bytes)], &never)
                .await
                .unwrap(),
            2
        );
        assert_eq!(&bytes, b"ta");
        writer.close().await.unwrap();
        scheduler
            .socket_shutdown(&sender, std::net::Shutdown::Write)
            .await
            .unwrap();
        let (result, parts) = reader
            .read_owned_vectored(vec![vec![0; 1]], SocketRecvFlags::default(), &never)
            .await;
        assert_eq!(result.unwrap().bytes, 0);
        assert_eq!(parts, [vec![0]]);
        assert_eq!(reader.read_into(&mut bytes, &never).await.unwrap(), 0);
        assert_eq!(
            reader
                .read_vectored(&mut [IoSliceMut::new(&mut bytes)], &never)
                .await
                .unwrap(),
            0
        );
        assert!(reader.read(&never).await.unwrap().is_none());
        assert_eq!(
            reader
                .read_vectored_with_flags(&mut [], SocketRecvFlags::default(), &never)
                .await
                .unwrap()
                .bytes,
            0
        );
        reader.close().await.unwrap();
        assert!(reader.read_vectored(&mut [], &never).await.is_err());
        assert!(
            reader
                .read_owned_vectored(vec![vec![0]], SocketRecvFlags::default(), &never)
                .await
                .0
                .is_err()
        );
        drop((reader, writer));
        scheduler.socket_close(sender).await.unwrap();
        scheduler.socket_close(receiver).await.unwrap();
    });
}

#[test]
fn packet_borrowing_preserves_peek_and_cancellation_before_closed_rejection() {
    run(async |scheduler| {
        let (sender, receiver) = packet_pair(scheduler).await;
        let never = Cancellation::never();
        let address = scheduler.socket_local_address(&receiver).unwrap();
        let pool = scheduler
            .buffer_pool(BufferPoolOptions::new(4, 64))
            .unwrap();
        let mut reader = scheduler
            .socket_packet_reader_with_pool(&receiver, pool)
            .unwrap();
        let mut writer = scheduler.socket_packet_writer(&sender).unwrap();
        writer.write(b"start", Some(address), &never).await.unwrap();
        assert_eq!(
            reader.read(&never).await.unwrap().unwrap().payload(),
            b"start"
        );
        let cancellation = Cancellation::new();
        let mut bytes = [0; 4];
        {
            let mut vectors = [IoSliceMut::new(&mut bytes)];
            let mut reading = Box::pin(reader.read_vectored(&mut vectors, &cancellation));
            assert!(poll_once(reading.as_mut()).await.is_none());
            cancellation.cancel();
            assert!(reading.await.is_err());
        }
        writer.write(b"peek", Some(address), &never).await.unwrap();
        let (result, parts) = reader
            .read_owned_vectored(vec![vec![0; 2]], SocketRecvFlags { peek: true }, &never)
            .await;
        assert!(result.unwrap().truncated);
        assert_eq!(parts, [b"pe"]);
        let result = reader
            .read_vectored(&mut [IoSliceMut::new(&mut bytes)], &never)
            .await
            .unwrap();
        assert_eq!(result.bytes, 4);
        assert!(!result.truncated);
        assert_eq!(&bytes, b"peek");
        reader.close().await.unwrap();
        assert!(
            reader
                .read_vectored_with_flags(&mut [], SocketRecvFlags::default(), &never)
                .await
                .is_err()
        );
        assert!(
            reader
                .read_owned_vectored(vec![vec![0]], SocketRecvFlags::default(), &never)
                .await
                .0
                .is_err()
        );
        writer.close().await.unwrap();
        drop((reader, writer));
        scheduler.socket_close(sender).await.unwrap();
        scheduler.socket_close(receiver).await.unwrap();
    });
}

#[test]
fn stopped_selector_rejects_lazy_pool_setup_and_file_writer_construction() {
    run(async |scheduler| {
        let (sender, receiver) = stream_pair(scheduler).await;
        let (packet_sender, packet_receiver) = packet_pair(scheduler).await;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("file");
        std::fs::write(&path, b"file").unwrap();
        let never = Cancellation::never();
        let file = scheduler
            .file_open(&path, FileOptions::READ, &never)
            .await
            .unwrap();
        let mut stream = scheduler.socket_stream_reader(&receiver).unwrap();
        let mut packet = scheduler.socket_packet_reader(&packet_receiver).unwrap();
        scheduler.close();
        assert!(scheduler.file_writer(&file).is_err());
        assert!(scheduler.socket_stream_reader(&receiver).is_err());
        assert!(scheduler.socket_packet_reader(&packet_receiver).is_err());
        assert!(scheduler.socket_stream_writer(&sender).is_err());
        assert!(scheduler.socket_packet_writer(&packet_sender).is_err());
        assert_eq!(
            stream.read(&never).await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            packet.read(&never).await.err().unwrap().kind(),
            io::ErrorKind::BrokenPipe
        );
        stream.close().await.unwrap();
        packet.close().await.unwrap();
        drop((
            stream,
            packet,
            sender,
            receiver,
            packet_sender,
            packet_receiver,
            file,
        ));
    });
}

#[test]
fn abandoned_operations_reject_owned_successors_until_retirement() {
    run(async |scheduler| {
        let (sender, receiver) = stream_pair(scheduler).await;
        let never = Cancellation::never();
        let mut reader = scheduler.socket_stream_reader(&receiver).unwrap();
        let mut reading = Box::pin(reader.read_owned(vec![0; 8], &never));
        assert!(poll_once(reading.as_mut()).await.is_none());
        drop(reading);
        assert_eq!(
            reader.read(&never).await.err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        let mut target = [0; 1];
        assert_eq!(
            reader
                .read_vectored(&mut [IoSliceMut::new(&mut target)], &never)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            reader
                .read_vectored_with_flags(
                    &mut [IoSliceMut::new(&mut target)],
                    SocketRecvFlags::default(),
                    &never
                )
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        let bytes = vec![0x5a; 4];
        let pointer = bytes.as_ptr();
        let (result, bytes) = reader.read_owned(bytes, &never).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        assert_eq!(bytes.as_ptr(), pointer);
        let (result, parts) = reader
            .read_owned_vectored(vec![bytes], SocketRecvFlags::default(), &never)
            .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        assert_eq!(parts[0].as_ptr(), pointer);
        reader.close().await.unwrap();
        drop(reader);

        let mut writer = scheduler.socket_stream_writer(&sender).unwrap();
        let mut writing = Box::pin(writer.write_owned_vectored(
            vec![vec![1; 8]],
            SocketSendFlags::default(),
            &never,
        ));
        assert!(poll_once(writing.as_mut()).await.is_none());
        drop(writing);
        let (result, bytes) = writer.write_owned(vec![0x5a; 4], &never).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        assert_eq!(bytes, [0x5a; 4]);
        let (result, parts) = writer
            .write_owned_vectored(vec![bytes], SocketSendFlags::default(), &never)
            .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        assert_eq!(parts, [[0x5a; 4]]);
        writer.close().await.unwrap();
        drop(writer);
        scheduler.socket_close(sender).await.unwrap();
        scheduler.socket_close(receiver).await.unwrap();

        let (sender, receiver) = packet_pair(scheduler).await;
        let destination = scheduler.socket_local_address(&receiver).unwrap();
        let mut reader = scheduler.socket_packet_reader(&receiver).unwrap();
        let mut reading = Box::pin(reader.read_owned_vectored(
            vec![vec![0; 8]],
            SocketRecvFlags::default(),
            &never,
        ));
        assert!(poll_once(reading.as_mut()).await.is_none());
        drop(reading);
        assert_eq!(
            reader.read(&never).await.err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        let mut target = [0; 1];
        assert_eq!(
            reader
                .read_vectored_with_flags(
                    &mut [IoSliceMut::new(&mut target)],
                    SocketRecvFlags::default(),
                    &never
                )
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        let (result, parts) = reader
            .read_owned_vectored(vec![vec![0x5a; 4]], SocketRecvFlags::default(), &never)
            .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        assert_eq!(parts, [[0x5a; 4]]);
        reader.close().await.unwrap();
        drop(reader);
        let mut writer = scheduler.socket_packet_writer(&sender).unwrap();
        let mut writing = Box::pin(writer.write_owned_vectored(
            vec![vec![1; 8]],
            Some(destination),
            SocketSendFlags::default(),
            &never,
        ));
        assert!(poll_once(writing.as_mut()).await.is_none());
        drop(writing);
        assert_eq!(
            writer
                .write_vectored_with_flags(
                    &[IoSlice::new(b"busy")],
                    Some(destination),
                    SocketSendFlags::default(),
                    &never
                )
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        let (result, parts) = writer
            .write_owned_vectored(
                vec![vec![0x5a; 4]],
                Some(destination),
                SocketSendFlags::default(),
                &never,
            )
            .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        assert_eq!(parts, [[0x5a; 4]]);
        writer.close().await.unwrap();
        drop(writer);
        scheduler.socket_close(sender).await.unwrap();
        scheduler.socket_close(receiver).await.unwrap();
    });
}

#[test]
fn stream_read_errors_and_cancellation_release_pool_capacity() {
    run(async |scheduler| {
        let never = Cancellation::never();
        let unconnected = scheduler
            .socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &never)
            .await
            .unwrap();
        let mut reader = scheduler.socket_stream_reader(&unconnected).unwrap();
        assert!(reader.read(&never).await.is_err());
        let mut bytes = [0x5a; 4];
        assert!(
            reader
                .read_vectored(&mut [IoSliceMut::new(&mut bytes)], &never)
                .await
                .is_err()
        );
        assert_eq!(bytes, [0x5a; 4]);
        reader.close().await.unwrap();
        drop(reader);
        scheduler.socket_close(unconnected).await.unwrap();

        let (sender, receiver) = stream_pair(scheduler).await;
        let pool = scheduler.buffer_pool(BufferPoolOptions::new(1, 8)).unwrap();
        let mut reader = scheduler
            .socket_stream_reader_with_pool(&receiver, pool.clone())
            .unwrap();
        let cancelled = Cancellation::new();
        cancelled.cancel();
        assert!(reader.read(&cancelled).await.is_err());
        // The first wait exhausts storage without registering I/O. The second has capacity and must retire its quiet socket readiness registration.
        for exhausted in [true, false] {
            let held = exhausted.then(|| pool.try_acquire().unwrap());
            let cancellation = Cancellation::new();
            {
                let mut reading = Box::pin(reader.read(&cancellation));
                assert!(poll_once(reading.as_mut()).await.is_none());
                cancellation.cancel();
                assert!(reading.await.is_err());
            }
            drop(held);
            assert!(pool.try_acquire().is_some());
        }
        reader.close().await.unwrap();
        drop(reader);
        scheduler
            .socket_shutdown(&sender, std::net::Shutdown::Write)
            .await
            .unwrap();
        let mut reader = scheduler.socket_stream_reader(&receiver).unwrap();
        assert_eq!(
            reader
                .read_vectored(&mut [IoSliceMut::new(&mut bytes)], &never)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            reader
                .read_vectored(&mut [IoSliceMut::new(&mut bytes)], &never)
                .await
                .unwrap(),
            0
        );
        reader.close().await.unwrap();
        drop(reader);
        scheduler.socket_close(sender).await.unwrap();
        scheduler.socket_close(receiver).await.unwrap();
    });
}

#[test]
fn stream_read_cancellation_and_closed_admission_preserve_cleanup() {
    run(async |scheduler| {
        let (sender, receiver) = stream_pair(scheduler).await;
        let pool = scheduler
            .buffer_pool(BufferPoolOptions::new(4, 64))
            .unwrap();
        let mut reader = scheduler
            .socket_stream_reader_with_pool(&receiver, pool)
            .unwrap();
        let cancellation = Cancellation::new();
        {
            let mut reading = Box::pin(reader.read(&cancellation));
            assert!(poll_once(reading.as_mut()).await.is_none());
            cancellation.cancel();
            assert!(reading.await.is_err());
        }
        reader.close().await.unwrap();
        drop(reader);
        scheduler.socket_close(sender).await.unwrap();
        scheduler.socket_close(receiver).await.unwrap();
    });
    run(async |scheduler| {
        let (sender, receiver) = stream_pair(scheduler).await;
        let pool = scheduler
            .buffer_pool(BufferPoolOptions::new(4, 64))
            .unwrap();
        let mut reader = scheduler
            .socket_stream_reader_with_pool(&receiver, pool)
            .unwrap();
        scheduler.close();
        assert!(reader.read(&Cancellation::never()).await.is_err());
        reader.close().await.unwrap();
        drop((reader, sender, receiver));
    });
}

#[test]
fn stream_socket_failures_preserve_borrowed_and_owned_storage() {
    for owned in [false, true] {
        run(async |scheduler| {
            let never = Cancellation::never();
            let socket = scheduler
                .socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &never)
                .await
                .unwrap();
            let pool = scheduler
                .buffer_pool(BufferPoolOptions::new(4, 64))
                .unwrap();
            let mut reader = scheduler
                .socket_stream_reader_with_pool(&socket, pool)
                .unwrap();
            // An unconnected stream rejects the transfer without touching caller storage.
            if owned {
                let (result, parts) = reader
                    .read_owned_vectored(vec![vec![0x5a; 4]], SocketRecvFlags::default(), &never)
                    .await;
                assert!(result.is_err());
                assert_eq!(parts, [[0x5a; 4]]);
            } else {
                let mut bytes = [0x5a; 4];
                assert!(
                    reader
                        .read_vectored(&mut [IoSliceMut::new(&mut bytes)], &never)
                        .await
                        .is_err()
                );
                assert_eq!(bytes, [0x5a; 4]);
            }
            reader.close().await.unwrap();
            drop(reader);
            scheduler.socket_close(socket).await.unwrap();
        });
    }
}

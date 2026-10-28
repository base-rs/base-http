// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::*;
use base_scheduler::*;
use std::{
    io::{IoSlice, IoSliceMut},
    net::Shutdown,
};

fn run(test: impl AsyncFnOnce(&TokioScheduler)) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let scheduler = TokioScheduler::new(runtime.handle().clone());
    runtime.block_on(async {
        test(&scheduler).await;
        scheduler.shutdown().await;
    });
}

#[test]
fn default_stream_pool_and_eof_are_consistent_across_read_forms() {
    run(async |scheduler| {
        let never = Cancellation::never();
        let cancelled = Cancellation::new();
        cancelled.cancel();
        for mode in 0..4 {
            let (sender, socket) =
                base_scheduler_conformance::socket::stream_pair(scheduler, SocketDomain::Ipv4)
                    .await;
            let mut reader = scheduler.socket_stream_reader(&socket).unwrap();
            let mut writer = scheduler
                .socket_stream_writer_with_buffer::<Vec<u8>>(&sender)
                .unwrap();
            assert_eq!(writer.write(b"x", &never).await.unwrap(), 1);
            assert_eq!(reader.read(&never).await.unwrap().unwrap().as_ref(), b"x");
            assert!(reader.read(&cancelled).await.is_err());
            assert_eq!(
                reader
                    .read_into_with_flags(&mut [], SocketRecvFlags::default(), &never)
                    .await
                    .unwrap(),
                0
            );
            assert!(
                reader
                    .read_into_with_flags(&mut [], SocketRecvFlags::default(), &cancelled)
                    .await
                    .is_err()
            );
            let (result, _) = reader
                .read_owned_vectored([Vec::<u8>::new()], SocketRecvFlags::default(), &never)
                .await;
            assert_eq!(result.unwrap().bytes, 0);
            let (result, _) = reader
                .read_owned_vectored([Vec::<u8>::new()], SocketRecvFlags::default(), &cancelled)
                .await;
            assert!(result.is_err());
            assert_eq!(reader.read_owned(Vec::new(), &never).await.0.unwrap(), 0);
            assert!(reader.read_owned(Vec::new(), &cancelled).await.0.is_err());
            scheduler
                .socket_shutdown(&sender, Shutdown::Write)
                .await
                .unwrap();
            let count = match mode {
                0 => reader.read_into(&mut [0], &never).await.unwrap(),
                1 => reader
                    .read_vectored(&mut [IoSliceMut::new(&mut [0])], &never)
                    .await
                    .unwrap(),
                2 => reader.read_owned(vec![0], &never).await.0.unwrap(),
                _ => {
                    reader
                        .read_owned_vectored([vec![0]], SocketRecvFlags::default(), &never)
                        .await
                        .0
                        .unwrap()
                        .bytes
                }
            };
            assert_eq!(count, 0);
            assert!(reader.read(&never).await.unwrap().is_none());
            reader.close().await.unwrap();
            assert!(reader.read_into(&mut [0], &never).await.is_err());
            assert!(
                reader
                    .read_vectored(&mut [IoSliceMut::new(&mut [0])], &never)
                    .await
                    .is_err()
            );
            assert!(
                reader
                    .read_into_with_flags(&mut [0], SocketRecvFlags::default(), &never)
                    .await
                    .is_err()
            );
            assert!(reader.read_owned(vec![0], &never).await.0.is_err());
            assert!(
                reader
                    .read_owned_vectored([vec![0]], SocketRecvFlags::default(), &never)
                    .await
                    .0
                    .is_err()
            );
            writer.close().await.unwrap();
        }
    });
}

#[test]
fn default_packet_pool_and_closed_lanes_preserve_message_boundaries() {
    run(async |scheduler| {
        let never = Cancellation::never();
        let cancelled = Cancellation::new();
        cancelled.cancel();
        let socket = scheduler
            .socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never)
            .await
            .unwrap();
        scheduler
            .socket_bind(&socket, "127.0.0.1:0".parse().unwrap(), &never)
            .await
            .unwrap();
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        sender
            .send_to(b"x", scheduler.socket_local_address(&socket).unwrap())
            .unwrap();
        let mut reader = scheduler.socket_packet_reader(&socket).unwrap();
        assert_eq!(reader.read(&never).await.unwrap().unwrap().payload(), b"x");
        assert!(
            reader
                .read_into_with_flags(&mut [0], SocketRecvFlags::default(), &cancelled)
                .await
                .is_err()
        );
        assert!(
            reader
                .read_owned_vectored([vec![0]], SocketRecvFlags::default(), &cancelled)
                .await
                .0
                .is_err()
        );
        reader.close().await.unwrap();
        assert!(reader.read_into(&mut [0], &never).await.is_err());
        assert!(
            reader
                .read_into_with_flags(&mut [0], SocketRecvFlags::default(), &never)
                .await
                .is_err()
        );
        assert!(
            reader
                .read_owned_vectored([vec![0]], SocketRecvFlags::default(), &never)
                .await
                .0
                .is_err()
        );
        let mut writer = scheduler.socket_packet_writer(&socket).unwrap();
        assert!(
            writer
                .write_with_flags(b"x", None, SocketSendFlags::default(), &cancelled)
                .await
                .is_err()
        );
        assert!(
            writer
                .write_owned_vectored([vec![0]], None, SocketSendFlags::default(), &cancelled)
                .await
                .0
                .is_err()
        );
        writer.close().await.unwrap();
        assert!(
            writer
                .write_vectored(&[IoSlice::new(b"x")], None, &never)
                .await
                .is_err()
        );
        assert!(
            writer
                .write_with_flags(b"x", None, SocketSendFlags::default(), &never)
                .await
                .is_err()
        );
        assert!(
            writer
                .write_owned_vectored([vec![0]], None, SocketSendFlags::default(), &never)
                .await
                .0
                .is_err()
        );
    });
}

#[test]
fn file_submission_rejection_keeps_the_original_buffer() {
    run(async |scheduler| {
        let file = TokioFile::from_std(tempfile::tempfile().unwrap());
        let mut writer = scheduler.file_writer(&file).unwrap();
        scheduler.close();
        let buffer = vec![1, 2];
        let pointer = buffer.as_ptr();
        let (error, returned) = writer
            .write_submit(buffer, &Cancellation::never())
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
        assert_eq!(returned.as_ptr(), pointer);
        writer.close().await.unwrap();
    });
}

#[test]
fn owned_stream_reads_report_syscall_and_wait_errors_and_lazy_setup_rejects_shutdown() {
    run(async |scheduler| {
        let never = Cancellation::never();
        let socket = scheduler
            .socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &never)
            .await
            .unwrap();
        let mut reader = scheduler.socket_stream_reader(&socket).unwrap();
        assert!(reader.read(&never).await.is_err());
        reader.close().await.unwrap();
        let (sender, socket) =
            base_scheduler_conformance::socket::stream_pair(scheduler, SocketDomain::Ipv4).await;
        let readiness_cancellation = Cancellation::new();
        let mut readiness = Box::pin(scheduler.socket_wait(
            &socket,
            crate::Interest::Readable,
            &readiness_cancellation,
        ));
        assert!(
            futures_lite::future::poll_once(readiness.as_mut())
                .await
                .is_none()
        );
        readiness_cancellation.cancel();
        assert!(readiness.await.is_err());
        let mut reader = scheduler.socket_stream_reader(&socket).unwrap();
        let cancellation = Cancellation::new();
        let mut read = Box::pin(reader.read(&cancellation));
        assert!(
            futures_lite::future::poll_once(read.as_mut())
                .await
                .is_none()
        );
        cancellation.cancel();
        assert!(read.await.is_err());
        reader.close().await.unwrap();
        let mut reader = scheduler.socket_stream_reader(&sender).unwrap();
        let packet = scheduler
            .socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never)
            .await
            .unwrap();
        let mut packet = scheduler.socket_packet_reader(&packet).unwrap();
        scheduler.close();
        assert!(reader.read(&never).await.is_err());
        assert!(packet.read(&never).await.is_err());
        assert!(
            scheduler
                .buffer_pool(BufferPoolOptions::new(2, 16))
                .is_err()
        );
        reader.close().await.unwrap();
        packet.close().await.unwrap();
    });
}

#[test]
fn file_completion_collection_does_not_hold_a_retired_file_open() {
    run(async |scheduler| {
        let file = TokioFile::from_std(tempfile::tempfile().unwrap());
        let mut writer = scheduler.file_writer(&file).unwrap();
        let completion = writer
            .write_submit(vec![1, 2], &Cancellation::never())
            .await
            .unwrap();
        writer.close().await.unwrap();
        drop(writer);
        scheduler.file_close(file).await.unwrap();
        let (result, buffer) = completion.await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(buffer, [1, 2]);
    });
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{NativeSocket, ReadinessSelector};
use base_scheduler::*;
use base_scheduler_conformance::{socket, socket_stream_writing};
use futures_lite::future::block_on;
use std::io::{self, IoSlice, IoSliceMut};

fn scheduler() -> ReadinessSelector {
    ReadinessSelector::with_options(crate::SelectorOptions {
        threads: 2,
        capacity: 8,
    })
    .unwrap()
}

fn stream_case(test: impl AsyncFnOnce(&ReadinessSelector, &NativeSocket, &NativeSocket)) {
    let scheduler = scheduler();
    block_on(async {
        let (sender, receiver) = socket::stream_pair(&scheduler, SocketDomain::Ipv4).await;
        test(&scheduler, &sender, &receiver).await;
        scheduler.socket_close(sender).await.unwrap();
        scheduler.socket_close(receiver).await.unwrap();
        scheduler.shutdown().await;
    });
}

#[test]
fn stream_output_orders_submissions() {
    stream_case(socket_stream_writing::ordering);
}

#[test]
fn stream_output_releases_owners_independently() {
    stream_case(socket_stream_writing::independent_progress);
}

#[test]
fn stream_output_preserves_accepted_work_after_abandonment() {
    stream_case(socket_stream_writing::abandoned_admission);
}

#[test]
fn stream_output_close_retires_pending_work() {
    stream_case(async |scheduler, sender, _| {
        socket_stream_writing::close_pending(scheduler, sender).await
    });
}

#[test]
fn stream_output_supports_borrowed_and_owned_transfers() {
    stream_case(socket_stream_writing::bound_io);
}

#[test]
fn stream_input_preserves_peek_and_owned_ranges_and_rejects_closed_calls() {
    stream_case(async |scheduler, sender, receiver| {
        let never = Cancellation::never();
        let cancelled = Cancellation::new();
        cancelled.cancel();
        let mut writer = scheduler.socket_stream_writer(sender).unwrap();
        let mut reader = scheduler.socket_stream_reader(receiver).unwrap();
        let mut bytes = [0; 4];
        assert_eq!(reader.read_vectored(&mut [], &never).await.unwrap(), 0);
        assert_eq!(reader.read_into(&mut [], &never).await.unwrap(), 0);
        assert!(reader.read_into(&mut [], &cancelled).await.is_err());
        assert!(reader.read_vectored(&mut [], &cancelled).await.is_err());
        assert!(
            reader
                .read_vectored_with_flags(&mut [], SocketRecvFlags::default(), &cancelled)
                .await
                .is_err()
        );
        assert_eq!(
            reader
                .read_vectored_with_flags(&mut [], SocketRecvFlags::default(), &never)
                .await
                .unwrap()
                .bytes,
            0
        );
        assert_eq!(reader.read_owned(Vec::new(), &never).await.0.unwrap(), 0);
        assert!(reader.read_owned(Vec::new(), &cancelled).await.0.is_err());
        writer.write(b"peek", &never).await.unwrap();
        assert_eq!(
            reader
                .read_into_with_flags(&mut bytes, SocketRecvFlags { peek: true }, &never)
                .await
                .unwrap(),
            4
        );
        assert_eq!(&bytes, b"peek");
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
            .socket_shutdown(sender, std::net::Shutdown::Write)
            .await
            .unwrap();
        assert_eq!(reader.read_owned(vec![0; 1], &never).await.0.unwrap(), 0);
        assert_eq!(reader.read_owned(vec![0; 1], &never).await.0.unwrap(), 0);
        reader.close().await.unwrap();
        assert!(reader.read_into(&mut bytes, &never).await.is_err());
        assert!(reader.read_vectored(&mut [], &never).await.is_err());
        assert!(
            reader
                .read_vectored_with_flags(&mut [], SocketRecvFlags::default(), &never)
                .await
                .is_err()
        );
        assert!(reader.read_owned(vec![0; 1], &never).await.0.is_err());
        assert!(
            reader
                .read_owned_vectored(vec![vec![0; 1]], SocketRecvFlags::default(), &never)
                .await
                .0
                .is_err()
        );
        assert!(writer.write(b"closed", &never).await.is_err());
        assert!(writer.write_owned(vec![1], &never).await.0.is_err());
        assert!(
            writer
                .write_owned_vectored(vec![vec![1]], SocketSendFlags::default(), &never)
                .await
                .0
                .is_err()
        );
        assert!(scheduler.socket_packet_reader(sender).is_err());
        assert!(scheduler.socket_packet_writer(sender).is_err());
    });
}

#[test]
fn packet_resources_keep_addresses_and_peeking_and_close_each_transfer_form() {
    let scheduler = scheduler();
    block_on(async {
        let never = Cancellation::never();
        let cancelled = Cancellation::new();
        cancelled.cancel();
        let sender = scheduler
            .socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never)
            .await
            .unwrap();
        let receiver = scheduler
            .socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never)
            .await
            .unwrap();
        scheduler
            .socket_bind(&receiver, SocketAddress::from(([127, 0, 0, 1], 0)), &never)
            .await
            .unwrap();
        scheduler
            .socket_bind(&sender, SocketAddress::from(([127, 0, 0, 1], 0)), &never)
            .await
            .unwrap();
        let address = scheduler.socket_local_address(&receiver).unwrap();
        let mut reader = scheduler.socket_packet_reader(&receiver).unwrap();
        let mut writer = scheduler.socket_packet_writer(&sender).unwrap();
        assert!(scheduler.socket_stream_reader(&sender).is_err());
        assert!(scheduler.socket_stream_writer(&sender).is_err());
        let mut bytes = [0; 4];
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
                    Some(address),
                    SocketSendFlags::default(),
                    &cancelled
                )
                .await
                .is_err()
        );
        writer
            .write_vectored(&[IoSlice::new(b"pool")], Some(address), &never)
            .await
            .unwrap();
        let packet = reader.read(&never).await.unwrap().unwrap();
        assert_eq!(packet.payload(), b"pool");
        assert_eq!(
            packet.address().unwrap().to_socket_address(),
            scheduler.socket_local_address(&sender).unwrap()
        );
        writer.write(b"peek", Some(address), &never).await.unwrap();
        assert_eq!(
            reader
                .read_into_with_flags(&mut bytes, SocketRecvFlags { peek: true }, &never)
                .await
                .unwrap()
                .bytes,
            4
        );
        assert_eq!(
            reader
                .read_vectored(&mut [IoSliceMut::new(&mut bytes)], &never)
                .await
                .unwrap()
                .bytes,
            4
        );
        reader.close().await.unwrap();
        writer.close().await.unwrap();
        assert!(reader.read_into(&mut bytes, &never).await.is_err());
        assert!(
            reader
                .read_vectored_with_flags(&mut [], SocketRecvFlags::default(), &never)
                .await
                .is_err()
        );
        assert!(
            reader
                .read_owned_vectored(vec![vec![0; 1]], SocketRecvFlags::default(), &never)
                .await
                .0
                .is_err()
        );
        assert!(
            writer
                .write(b"closed", Some(address), &never)
                .await
                .is_err()
        );
        assert!(
            writer
                .write_vectored_with_flags(&[], Some(address), SocketSendFlags::default(), &never)
                .await
                .is_err()
        );
        assert!(
            writer
                .write_owned_vectored(
                    vec![vec![1]],
                    Some(address),
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
        scheduler.shutdown().await;
    });
}

#[test]
fn stopped_selector_rejects_factories_and_pooled_read_admission() {
    let scheduler = scheduler();
    block_on(async {
        let (sender, receiver) = socket::stream_pair(&scheduler, SocketDomain::Ipv4).await;
        let mut reader = scheduler.socket_stream_reader(&receiver).unwrap();
        let mut writer = scheduler
            .socket_stream_writer_with_buffer::<Vec<u8>>(&sender)
            .unwrap();
        scheduler.close();
        assert!(scheduler.socket_stream_reader(&receiver).is_err());
        assert!(scheduler.socket_stream_writer(&sender).is_err());
        assert!(scheduler.socket_packet_reader(&receiver).is_err());
        assert!(scheduler.socket_packet_writer(&sender).is_err());
        assert_eq!(
            reader
                .read(&Cancellation::never())
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        assert!(
            writer
                .submit(vec![1], &Cancellation::never())
                .await
                .is_err()
        );
        reader.close().await.unwrap();
        writer.close().await.unwrap();
        scheduler.shutdown().await;
    });
}

#[test]
fn pending_stream_output_keeps_order_when_later_waiters_are_cancelled() {
    stream_case(async |scheduler, sender, receiver| {
        let never = Cancellation::never();
        let cancelled = Cancellation::new();
        cancelled.cancel();
        let mut writer = scheduler
            .socket_stream_writer_with_buffer::<Vec<u8>>(sender)
            .unwrap();
        let payload = vec![0x44; 8 * 1024 * 1024];
        writer.submit(payload.clone(), &never).await.unwrap();
        assert!(writer.write(b"suffix", &cancelled).await.is_err());
        assert!(
            writer
                .write_vectored(&[IoSlice::new(b"suffix")], &cancelled)
                .await
                .is_err()
        );
        let owned = b"suffix".to_vec();
        let pointer = owned.as_ptr();
        let (result, returned) = writer.write_owned(owned, &cancelled).await;
        assert!(result.is_err());
        assert_eq!(returned.as_ptr(), pointer);
        assert!(
            writer
                .write_owned_vectored(vec![vec![1]], SocketSendFlags::default(), &cancelled)
                .await
                .0
                .is_err()
        );
        assert!(writer.submit(vec![1], &cancelled).await.is_err());
        let mut reader = scheduler.socket_stream_reader(receiver).unwrap();
        let mut received = 0;
        let mut bytes = [0; 64 * 1024];
        while received < payload.len() {
            let count = reader.read_into(&mut bytes, &never).await.unwrap();
            assert!(count > 0);
            assert!(bytes[..count].iter().all(|byte| *byte == 0x44));
            received += count;
        }
        writer.flush(&never).await.unwrap();
        writer.close().await.unwrap();
        reader.close().await.unwrap();
    });
}

#[test]
fn stream_socket_errors_remain_observable_after_collection() {
    stream_case(async |scheduler, sender, _| {
        let never = Cancellation::never();
        let mut writer = scheduler
            .socket_stream_writer_with_buffer::<Vec<u8>>(sender)
            .unwrap();
        scheduler
            .socket_shutdown(sender, std::net::Shutdown::Write)
            .await
            .unwrap();
        writer.submit(vec![1], &never).await.unwrap();
        let error = writer.flush(&never).await.unwrap_err();
        assert!(error.raw_os_error().is_some());
        assert_eq!(
            writer.flush(&never).await.unwrap_err().raw_os_error(),
            error.raw_os_error()
        );
        writer.close().await.unwrap();
    });
}

#[test]
fn owned_vectors_support_empty_reads_and_packet_addresses() {
    stream_case(async |scheduler, sender, receiver| {
        let never = Cancellation::never();
        let cancelled = Cancellation::new();
        cancelled.cancel();
        let mut reader = scheduler.socket_stream_reader(receiver).unwrap();
        let mut writer = scheduler.socket_stream_writer(sender).unwrap();
        assert!(
            reader
                .read_owned_vectored(vec![vec![0]], SocketRecvFlags::default(), &cancelled)
                .await
                .0
                .is_err()
        );
        assert_eq!(
            reader
                .read_owned_vectored(vec![vec![], vec![]], SocketRecvFlags::default(), &never)
                .await
                .0
                .unwrap()
                .bytes,
            0
        );
        writer
            .write_owned_vectored(
                vec![b"ab".to_vec(), b"cd".to_vec()],
                SocketSendFlags::default(),
                &never,
            )
            .await
            .0
            .unwrap();
        let (result, parts) = reader
            .read_owned_vectored(
                vec![vec![0; 2], vec![0; 2]],
                SocketRecvFlags::default(),
                &never,
            )
            .await;
        assert_eq!(result.unwrap().bytes, 4);
        assert_eq!(parts, [b"ab", b"cd"]);
        writer.close().await.unwrap();
        scheduler
            .socket_shutdown(sender, std::net::Shutdown::Write)
            .await
            .unwrap();
        let mut bytes = [0; 1];
        assert_eq!(
            reader
                .read_vectored(&mut [IoSliceMut::new(&mut bytes)], &never)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            reader
                .read_vectored_with_flags(
                    &mut [IoSliceMut::new(&mut bytes)],
                    SocketRecvFlags::default(),
                    &never
                )
                .await
                .unwrap()
                .bytes,
            0
        );
        reader.close().await.unwrap();
    });
    let scheduler = scheduler();
    block_on(async {
        let never = Cancellation::never();
        let sender = scheduler
            .socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never)
            .await
            .unwrap();
        let receiver = scheduler
            .socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never)
            .await
            .unwrap();
        scheduler
            .socket_bind(&receiver, SocketAddress::from(([127, 0, 0, 1], 0)), &never)
            .await
            .unwrap();
        let address = scheduler.socket_local_address(&receiver).unwrap();
        let mut reader = scheduler.socket_packet_reader(&receiver).unwrap();
        let mut writer = scheduler.socket_packet_writer(&sender).unwrap();
        writer
            .write_with_flags(b"flag", Some(address), SocketSendFlags::default(), &never)
            .await
            .unwrap();
        let (result, parts) = reader
            .read_owned_vectored(
                vec![vec![0; 2], vec![0; 2]],
                SocketRecvFlags::default(),
                &never,
            )
            .await;
        assert_eq!(result.unwrap().bytes, 4);
        assert_eq!(parts, [b"fl", b"ag"]);
        writer
            .write_owned(vec![1], Some(address), &never)
            .await
            .0
            .unwrap();
        reader.read(&never).await.unwrap();
        writer
            .write_owned_vectored(
                vec![vec![2]],
                Some(address),
                SocketSendFlags::default(),
                &never,
            )
            .await
            .0
            .unwrap();
        reader.read(&never).await.unwrap();
        let cancelled = Cancellation::new();
        cancelled.cancel();
        assert!(reader.read_into(&mut [0; 1], &cancelled).await.is_err());
        assert!(
            writer
                .write(b"cancelled", Some(address), &cancelled)
                .await
                .is_err()
        );
        let mut uninitialized = scheduler.socket_packet_reader(&receiver).unwrap();
        scheduler.close();
        assert!(uninitialized.read(&never).await.is_err());
        uninitialized.close().await.unwrap();
        reader.close().await.unwrap();
        writer.close().await.unwrap();
        scheduler.shutdown().await;
    });
}

#[test]
fn rejected_work_admission_returns_file_and_stream_owners() {
    let mut scheduler = scheduler();
    let (sender, receiver) = block_on(socket::stream_pair(&scheduler, SocketDomain::Ipv4));
    scheduler.work.fail_start_at = Some(0);
    block_on(async {
        let never = Cancellation::never();
        let file = crate::NativeFile::from_std(tempfile::tempfile().unwrap());
        let mut writer = scheduler.file_writer(&file).unwrap();
        let bytes = vec![0x5a; 4];
        let pointer = bytes.as_ptr();
        let (error, bytes) = writer.write_submit(bytes, &never).await.err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(bytes.as_ptr(), pointer);
        writer.close().await.unwrap();
        let mut writer = scheduler
            .socket_stream_writer_with_buffer::<Vec<u8>>(&sender)
            .unwrap();
        let (error, bytes) = writer.submit(bytes, &never).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(bytes.as_ptr(), pointer);
        writer.close().await.unwrap();
        scheduler.shutdown().await;
    });
    drop((sender, receiver));
}

#[test]
fn stream_read_errors_preserve_storage_and_closed_selector_waits_retire() {
    stream_case(async |scheduler, _, receiver| {
        let cancelled = Cancellation::new();
        cancelled.cancel();
        let mut reader = scheduler.socket_stream_reader(receiver).unwrap();
        assert!(reader.read(&cancelled).await.is_err());
        let mut bytes = [0; 4];
        assert!(
            reader
                .read_vectored(&mut [IoSliceMut::new(&mut bytes)], &cancelled)
                .await
                .is_err()
        );
        crate::socket_io::SOCKET_TYPE_ERROR
            .with(|error| error.set(Some(io::ErrorKind::PermissionDenied)));
        assert_eq!(
            crate::socket_io::recvmsg_socket_type(receiver.get_ref())
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(bytes, [0; 4]);
        reader.close().await.unwrap();
    });
    let scheduler = scheduler();
    block_on(async {
        let never = Cancellation::never();
        let (sender, receiver) = socket::stream_pair(&scheduler, SocketDomain::Ipv4).await;
        let pool = scheduler.buffer_pool(BufferPoolOptions::new(2, 4)).unwrap();
        let mut waiting_reader = scheduler
            .socket_stream_reader_with_pool(&receiver, pool.clone())
            .unwrap();
        let mut immediate_reader = scheduler
            .socket_stream_reader_with_pool(&receiver, pool.clone())
            .unwrap();
        let mut waiting = Box::pin(waiting_reader.read(&never));
        assert!(
            futures_lite::future::poll_once(&mut waiting)
                .await
                .is_none()
        );
        scheduler.close();
        assert_eq!(waiting.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(
            immediate_reader.read(&never).await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        waiting_reader.close().await.unwrap();
        immediate_reader.close().await.unwrap();
        assert!(pool.try_acquire().is_some());
        scheduler.shutdown().await;
        drop((sender, receiver));
    });
}

#[test]
fn owned_stream_writes_wait_for_earlier_accepted_output() {
    stream_case(async |scheduler, sender, receiver| {
        let never = Cancellation::never();
        let mut writer = scheduler
            .socket_stream_writer_with_buffer::<Vec<u8>>(sender)
            .unwrap();
        writer.submit(b"a".to_vec(), &never).await.unwrap();
        writer.write_owned(b"b".to_vec(), &never).await.0.unwrap();
        writer.submit(b"c".to_vec(), &never).await.unwrap();
        writer
            .write_owned_vectored(vec![b"d".to_vec()], SocketSendFlags::default(), &never)
            .await
            .0
            .unwrap();
        let mut reader = scheduler.socket_stream_reader(receiver).unwrap();
        let mut bytes = [0; 4];
        let mut received = 0;
        while received < bytes.len() {
            received += reader
                .read_into(&mut bytes[received..], &never)
                .await
                .unwrap();
        }
        assert_eq!(&bytes, b"abcd");
        writer.close().await.unwrap();
        assert!(writer.write_vectored(&[], &never).await.is_err());
        reader.close().await.unwrap();
    });
}

#[test]
fn accepted_output_overlaps_the_next_read() {
    stream_case(socket_stream_writing::overlapped_read_write);
}

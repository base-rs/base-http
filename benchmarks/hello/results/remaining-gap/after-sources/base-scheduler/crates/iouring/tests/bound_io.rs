// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![cfg(target_os = "linux")]

use base_scheduler::*;
use base_scheduler_iouring::{IoUringDriver, IoUringSelector};
use futures_lite::future::{block_on, poll_once};
use std::{
    future::Future,
    io::IoSliceMut,
    pin::pin,
    time::{Duration, Instant},
};

fn drive<F: Future>(driver: &mut IoUringDriver, future: F) -> F::Output {
    let mut future = pin!(future);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(result) = block_on(poll_once(&mut future)) {
            return result;
        }
        assert!(
            Instant::now() < deadline,
            "bound resource operation did not progress"
        );
        driver.turn(Some(Duration::from_millis(10))).unwrap();
    }
}

#[test]
fn stream_reads_can_switch_between_owned_and_borrowed_storage() {
    let (scheduler, mut driver) = IoUringSelector::new(Default::default()).unwrap();
    let (sender, receiver) = block_on(base_scheduler_conformance::socket::stream_pair(
        &scheduler,
        SocketDomain::Ipv4,
    ));
    let never = Cancellation::never();
    let pool = scheduler
        .buffer_pool(BufferPoolOptions::new(4, 128))
        .unwrap();
    let mut reader = scheduler
        .socket_stream_reader_with_pool(&receiver, pool)
        .unwrap();
    sender
        .try_send(b"start", SocketSendFlags::default())
        .unwrap();
    assert_eq!(
        drive(&mut driver, reader.read(&never))
            .unwrap()
            .unwrap()
            .as_ref(),
        b"start"
    );
    sender
        .try_send(b"abcdef", SocketSendFlags::default())
        .unwrap();
    let mut peek = [0; 2];
    assert_eq!(
        drive(
            &mut driver,
            reader.read_into_with_flags(&mut peek, SocketRecvFlags { peek: true }, &never)
        )
        .unwrap(),
        2
    );
    assert_eq!(&peek, b"ab");
    let cancelled = Cancellation::new();
    cancelled.cancel();
    let mut untouched = [42; 2];
    assert!(block_on(reader.read_into(&mut untouched, &cancelled)).is_err());
    assert_eq!(untouched, [42; 2]);
    assert_eq!(block_on(reader.read_into(&mut [], &never)).unwrap(), 0);
    let (empty, _) = block_on(reader.read_owned_vectored(
        [Vec::<u8>::new(), Vec::new()],
        SocketRecvFlags::default(),
        &never,
    ));
    assert_eq!(empty.unwrap().bytes, 0);
    let mut first = [0; 1];
    let mut second = [0; 1];
    assert_eq!(
        drive(
            &mut driver,
            reader.read_vectored(
                &mut [IoSliceMut::new(&mut first), IoSliceMut::new(&mut second)],
                &never
            )
        )
        .unwrap(),
        2
    );
    assert_eq!([first[0], second[0]], *b"ab");
    assert_eq!(
        drive(&mut driver, reader.read(&never))
            .unwrap()
            .unwrap()
            .as_ref(),
        b"cdef"
    );
    drive(&mut driver, reader.close()).unwrap();
    assert!(block_on(reader.read(&never)).unwrap().is_none());
    assert!(block_on(reader.read_into(&mut [0; 1], &never)).is_err());
    drop(reader);
    block_on(scheduler.socket_close(sender)).unwrap();
    block_on(scheduler.socket_close(receiver)).unwrap();
    drive(&mut driver, scheduler.shutdown());
}

#[test]
fn packet_reads_keep_peek_addresses_truncation_and_empty_datagrams() {
    let (scheduler, mut driver) = IoUringSelector::new(Default::default()).unwrap();
    let never = Cancellation::never();
    let receiver =
        block_on(scheduler.socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never))
            .unwrap();
    block_on(scheduler.socket_bind(&receiver, "127.0.0.1:0".parse().unwrap(), &never)).unwrap();
    let address = scheduler.socket_local_address(&receiver).unwrap();
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let peer = sender.local_addr().unwrap();
    let pool = scheduler
        .buffer_pool(BufferPoolOptions::new(4, 128))
        .unwrap();
    let mut reader = scheduler
        .socket_packet_reader_with_pool(&receiver, pool)
        .unwrap();
    sender.send_to(b"start", address).unwrap();
    assert_eq!(
        drive(&mut driver, reader.read(&never))
            .unwrap()
            .unwrap()
            .payload(),
        b"start"
    );
    sender.send_to(b"abcdef", address).unwrap();
    let mut peek = [0; 2];
    let result = drive(
        &mut driver,
        reader.read_into_with_flags(&mut peek, SocketRecvFlags { peek: true }, &never),
    )
    .unwrap();
    assert_eq!(result.address, Some(peer));
    assert!(result.truncated);
    assert_eq!(&peek, b"ab");
    let cancelled = Cancellation::new();
    cancelled.cancel();
    let mut untouched = [42; 2];
    assert!(block_on(reader.read_into(&mut untouched, &cancelled)).is_err());
    assert_eq!(untouched, [42; 2]);
    let (result, buffers) = drive(
        &mut driver,
        reader.read_owned_vectored([vec![0; 1], vec![0; 1]], SocketRecvFlags::default(), &never),
    );
    let result = result.unwrap();
    assert_eq!(result.address, Some(peer));
    assert!(result.truncated);
    assert_eq!(buffers, [b"a".to_vec(), b"b".to_vec()]);
    sender.send_to(b"", address).unwrap();
    let result = drive(&mut driver, reader.read_into(&mut [], &never)).unwrap();
    assert_eq!(result.bytes, 0);
    assert_eq!(result.address, Some(peer));
    assert!(!result.truncated);
    sender.send_to(b"next", address).unwrap();
    assert_eq!(
        drive(&mut driver, reader.read(&never))
            .unwrap()
            .unwrap()
            .payload(),
        b"next"
    );
    drive(&mut driver, reader.close()).unwrap();
    drop(reader);
    block_on(scheduler.socket_close(receiver)).unwrap();
    drive(&mut driver, scheduler.shutdown());
}

#[test]
fn abandoned_owned_socket_transfers_reserve_the_lane_until_retirement() {
    let (scheduler, mut driver) = IoUringSelector::new(Default::default()).unwrap();
    let never = Cancellation::never();
    let (sender, receiver) = block_on(base_scheduler_conformance::socket::stream_pair(
        &scheduler,
        SocketDomain::Ipv4,
    ));
    macro_rules! exercise {
        ($operation:expr, $successor:expr, $close:expr) => {{
            let mut operation = Box::pin($operation);
            assert!(block_on(poll_once(operation.as_mut())).is_none());
            drop(operation);
            let mut successor = Box::pin($successor);
            assert_eq!(
                block_on(poll_once(successor.as_mut()))
                    .expect("busy successors must finish immediately")
                    .unwrap_err()
                    .kind(),
                std::io::ErrorKind::WouldBlock
            );
            drop(successor);
            let mut close = Box::pin($close);
            assert!(
                block_on(poll_once(close.as_mut())).is_none(),
                "close must await accepted payload access"
            );
            drive(&mut driver, close).unwrap();
        }};
    }
    {
        let mut reader = scheduler.socket_stream_reader(&receiver).unwrap();
        let mut bytes = [0; 1];
        exercise!(
            reader.read_owned(vec![0; 8], &never),
            reader.read_into(&mut bytes, &never),
            reader.close()
        );
    }
    {
        let mut writer = scheduler.socket_stream_writer(&sender).unwrap();
        exercise!(
            writer.write_owned_vectored([vec![1; 8]], SocketSendFlags::default(), &never),
            writer.write(b"next", &never),
            writer.close()
        );
    }
    {
        let bytes = vec![1; 64 * 1024];
        loop {
            match sender.try_send(&bytes, SocketSendFlags::default()) {
                Ok(count) => assert!(count > 0),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
                    break;
                }
            }
        }
        let mut writer = scheduler.socket_stream_writer(&sender).unwrap();
        exercise!(
            writer.write_owned(vec![1; 8], &never),
            writer.write(b"next", &never),
            writer.close()
        );
    }
    block_on(scheduler.socket_close(sender)).unwrap();
    block_on(scheduler.socket_close(receiver)).unwrap();
    let socket =
        block_on(scheduler.socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never))
            .unwrap();
    block_on(scheduler.socket_bind(&socket, "127.0.0.1:0".parse().unwrap(), &never)).unwrap();
    let address = scheduler.socket_local_address(&socket).unwrap();
    {
        let mut reader = scheduler.socket_packet_reader(&socket).unwrap();
        let mut bytes = [0; 1];
        exercise!(
            reader.read_owned_vectored([vec![0; 8]], SocketRecvFlags::default(), &never),
            reader.read_into(&mut bytes, &never),
            reader.close()
        );
    }
    {
        let mut writer = scheduler.socket_packet_writer(&socket).unwrap();
        exercise!(
            writer.write_owned_vectored(
                [vec![1; 8]],
                Some(address),
                SocketSendFlags::default(),
                &never
            ),
            writer.write(b"next", Some(address), &never),
            writer.close()
        );
    }
    block_on(scheduler.socket_close(socket)).unwrap();
    drive(&mut driver, scheduler.shutdown());
}

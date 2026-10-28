// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![cfg(target_os = "linux")]

use base_scheduler::*;
use base_scheduler_iouring::{IoUringDriver, IoUringSelector};
use futures_lite::future::{block_on, or, poll_once};
use std::{net::UdpSocket, pin::pin, sync::Arc, thread::JoinHandle, time::Duration};

struct Fixture {
    selector: Arc<IoUringSelector>,
    drivers: Vec<JoinHandle<()>>,
}
impl Fixture {
    fn new(workers: usize) -> Self {
        let (selector, drivers) =
            IoUringSelector::with_workers(Default::default(), workers).unwrap();
        Self {
            selector: Arc::new(selector),
            drivers: drivers
                .into_iter()
                .map(|mut driver: IoUringDriver| {
                    std::thread::spawn(move || {
                        while !driver.turn(Some(Duration::from_millis(100))).unwrap() {}
                    })
                })
                .collect(),
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.selector.close();
        for driver in self.drivers.drain(..) {
            driver.join().unwrap();
        }
        block_on(self.selector.shutdown());
    }
}

fn run(future: impl std::future::Future<Output = ()>) {
    block_on(or(future, async {
        async_io::Timer::after(Duration::from_secs(10)).await;
        panic!("shared pool operation did not make progress");
    }));
}

#[test]
fn shared_pool_preserves_capacity_and_retained_payloads_across_workers() {
    let fixture = Fixture::new(4);
    let scheduler = &*fixture.selector;
    run(async {
        let never = Cancellation::never();
        let pool = scheduler
            .buffer_pool(BufferPoolOptions::new(2, 64))
            .unwrap();
        assert_eq!(pool.capacity(), 2);
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        let mut sockets = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..4 {
            let socket = scheduler
                .socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never)
                .await
                .unwrap();
            scheduler
                .socket_bind(&socket, "127.0.0.1:0".parse().unwrap(), &never)
                .await
                .unwrap();
            receivers.push(
                scheduler
                    .socket_packet_reader_with_pool(&socket, pool.clone())
                    .unwrap(),
            );
            sockets.push(socket);
        }
        sender
            .send_to(
                b"retained",
                scheduler.socket_local_address(&sockets[0]).unwrap(),
            )
            .unwrap();
        let first = receivers[0].read(&never).await.unwrap().unwrap();
        let address = first.payload().as_ptr() as usize;
        let retained = first.into_payload().freeze();
        for index in 1..4 {
            sender
                .send_to(
                    &[index as u8],
                    scheduler.socket_local_address(&sockets[index]).unwrap(),
                )
                .unwrap();
            let message = receivers[index].read(&never).await.unwrap().unwrap();
            assert_eq!(message.payload(), &[index as u8]);
            assert_eq!(
                message.address().map(|address| address.to_socket_address()),
                Some(sender.local_addr().unwrap())
            );
            assert!(!message.truncated());
            assert_eq!(retained.as_ref().as_ptr() as usize, address);
            assert_eq!(retained.as_ref(), b"retained");
        }
        // A retained view can move to another thread, including through its final release.
        std::thread::spawn(move || assert_eq!(retained.as_ref(), b"retained"))
            .join()
            .unwrap();
        for mut receiver in receivers {
            receiver.close().await.unwrap();
        }
        for socket in sockets {
            scheduler.socket_close(socket).await.unwrap();
        }
        let mut buffers = Vec::new();
        for _ in 0..pool.capacity() {
            buffers.push(pool.acquire(&never).await.unwrap());
        }
        assert!(pool.try_acquire().is_none());
        drop(buffers);
    });
}

#[test]
fn shared_receive_preserves_alignment_empty_datagrams_and_wait_cancellation() {
    let fixture = Fixture::new(2);
    let scheduler = &*fixture.selector;
    run(async {
        let never = Cancellation::never();
        let pool = scheduler
            .buffer_pool(BufferPoolOptions {
                alignment: 4096,
                ..BufferPoolOptions::new(1, 3)
            })
            .unwrap();
        let socket = scheduler
            .socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never)
            .await
            .unwrap();
        scheduler
            .socket_bind(&socket, "127.0.0.1:0".parse().unwrap(), &never)
            .await
            .unwrap();
        let address = scheduler.socket_local_address(&socket).unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        let mut receiver = scheduler
            .socket_packet_reader_with_pool(&socket, pool.clone())
            .unwrap();
        let token = Cancellation::new();
        {
            let mut receiving = pin!(receiver.read(&token));
            assert!(poll_once(receiving.as_mut()).await.is_none());
            token.cancel();
            assert!(receiving.await.is_err());
        }
        for (bytes, expected) in [(b"abcdef".as_slice(), b"abc".as_slice()), (b"", b"")] {
            sender.send_to(bytes, address).unwrap();
            let mut message = receiver.read(&never).await.unwrap().unwrap();
            assert_eq!(message.payload(), expected);
            assert_eq!(message.payload().as_ptr() as usize % 4096, 0);
            assert_eq!(message.truncated(), bytes.len() > 3);
            if !expected.is_empty() {
                message.payload_mut()[0] = b'X';
            }
        }
        receiver.close().await.unwrap();
        drop(receiver);
        scheduler.socket_close(socket).await.unwrap();
        assert!(pool.try_acquire().is_some());
    });
}

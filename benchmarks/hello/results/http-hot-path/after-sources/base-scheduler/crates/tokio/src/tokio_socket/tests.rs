// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use std::time::Duration;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn socket() -> socket2::Socket {
    socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap()
}

#[test]
fn explicit_close_rejects_retained_socket_ownership() {
    let runtime = runtime();
    let descriptor = TokioSocket::new(runtime.handle(), socket()).unwrap();
    let retained = descriptor.retain();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| descriptor.into_close()));
    assert!(result.is_err());
    assert!(retained.get_ref().set_nonblocking(true).is_ok());
    retained.into_close()().unwrap();
}

#[test]
fn nonblocking_configuration_errors_are_returned() {
    let runtime = runtime();
    let error = TokioSocket::new_with(
        runtime.handle(),
        socket(),
        |_| Err(io::Error::from(io::ErrorKind::PermissionDenied)),
        |_, _| unreachable!("socket registration follows nonblocking configuration"),
    )
    .err()
    .unwrap();

    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
}

#[test]
fn registration_errors_are_returned() {
    let runtime = runtime();
    let error = TokioSocket::new_with(
        runtime.handle(),
        socket(),
        |_| Ok(()),
        |_, _| Err(io::Error::from(io::ErrorKind::PermissionDenied)),
    )
    .err()
    .unwrap();

    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
}

#[test]
fn invalid_accept_returns_without_waiting_for_readiness() {
    let runtime = runtime();
    let socket = runtime.block_on(async { TokioSocket::new(runtime.handle(), socket()).unwrap() });

    let result = runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(1),
            socket.io(Interest::Readable, |socket| socket.accept().map(|_| ())),
        )
        .await
    });

    assert!(
        result
            .expect("invalid accept waited for readiness")
            .is_err()
    );
}
#[test]
fn transient_would_block_retries_with_the_requested_readiness_direction() {
    use base_scheduler::Cancellation;
    crate::test_support::run(|| {
        futures_lite::future::block_on(async {
            let scheduler = crate::test_support::scheduler();
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let native = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            let (peer, _) = listener.accept().unwrap();
            // These sockets have no readiness cache yet. A synthetic first WouldBlock models the initial registration race without clearing a real readiness event on an already-writable socket.
            let client = TokioSocket::new(&scheduler.runtime, native.into()).unwrap();
            let server = TokioSocket::new(&scheduler.runtime, peer.into()).unwrap();
            // Model a socket becoming ready between the initial syscall and readiness registration. Both reactor directions must retry the actual syscall.
            let mut writes = 0;
            let sent = client
                .io(Interest::Writable, |socket| {
                    writes += 1;
                    if writes == 1 {
                        return Err(io::ErrorKind::WouldBlock.into());
                    }
                    crate::socket_io::send(socket, b"x")
                })
                .await
                .unwrap();
            assert_eq!(sent, 1);
            assert!(writes >= 2);
            let mut reads = 0;
            let mut buffer = [0];
            let received = server
                .io(Interest::Readable, |socket| {
                    reads += 1;
                    if reads == 1 {
                        return Err(io::ErrorKind::WouldBlock.into());
                    }
                    crate::socket_io::recv(socket, &mut buffer)
                })
                .await
                .unwrap();
            assert_eq!(received, 1);
            assert_eq!(&buffer, b"x");
            assert!(reads >= 2);
            // Confirm normal public operations still use the same registration.
            use base_scheduler::SocketClose;
            scheduler
                .socket_wait(&client, Interest::Writable, &Cancellation::never())
                .await
                .unwrap();
            scheduler.socket_close(client).await.unwrap();
            scheduler.socket_close(server).await.unwrap();
            scheduler.shutdown().await;
        })
    });
}

#[cfg(unix)]
#[test]
fn registration_is_deferred_until_waiting_and_reused_after_cancellation() {
    use base_scheduler::*;
    use futures_lite::future::{block_on, poll_once};
    use std::io::Write;
    crate::test_support::run(|| {
        block_on(async {
            let scheduler = crate::test_support::scheduler();
            let never = Cancellation::never();
            let socket = scheduler
                .socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &never)
                .await
                .unwrap();
            assert!(socket.inner.registration.get().is_none());
            assert_eq!(Arc::strong_count(&socket.inner.socket), 1);
            // Linux reports hangup on an unconnected stream. Observe native readiness without letting Tokio cache those permanent closed bits.
            #[cfg(target_os = "linux")]
            for interest in [Interest::Readable, Interest::Writable] {
                scheduler
                    .socket_wait(&socket, interest, &never)
                    .await
                    .unwrap();
                assert!(socket.inner.registration.get().is_none());
            }
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            scheduler
                .socket_connect(&socket, listener.local_addr().unwrap(), &never)
                .await
                .unwrap();
            let (mut peer, _) = listener.accept().unwrap();
            let token = Cancellation::new();
            let mut target = [0x5a];
            let mut receiving = Box::pin(scheduler.socket_recv_poll(
                &socket,
                &mut target,
                SocketRecvFlags::default(),
                &token,
            ));
            assert!(poll_once(&mut receiving).await.is_none());
            let registration = socket.inner.registration.get().unwrap() as *const _;
            assert_eq!(Arc::strong_count(&socket.inner.socket), 2);
            token.cancel();
            assert!(matches!(receiving.await, Err(OperationError::Cancelled(_))));
            assert_eq!(target, [0x5a]);
            assert!(!socket.inner.read_waiting.load(Ordering::Acquire));
            peer.write_all(b"x").unwrap();
            assert_eq!(
                scheduler
                    .socket_recv_poll(&socket, &mut target, SocketRecvFlags::default(), &never)
                    .await
                    .unwrap(),
                1
            );
            assert_eq!(target, *b"x");
            assert_eq!(
                socket.inner.registration.get().unwrap() as *const _,
                registration
            );
            let weak = Arc::downgrade(&socket.inner.socket);
            scheduler.socket_close(socket).await.unwrap();
            assert_eq!(weak.strong_count(), 0);
            scheduler.shutdown().await;
        });
    });
}

#[cfg(unix)]
#[test]
fn reserved_and_concurrent_waiters_both_receive_readiness_wakeups() {
    use std::{
        future::Future,
        io::{Read, Write},
        sync::mpsc,
        task::{Context, Poll, Wake, Waker},
    };
    struct Notify(usize, mpsc::Sender<usize>);
    impl Wake for Notify {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            let _ = self.1.send(self.0);
        }
    }
    crate::test_support::run(|| {
        for interest in [Interest::Readable, Interest::Writable] {
            // A Unix socket pair stays full until its peer reads. TCP acknowledgements can reopen a send window after WouldBlock and make the initial Pending assertion timing-dependent.
            let (native, mut peer) = std::os::unix::net::UnixStream::pair().unwrap();
            let native = socket2::Socket::from(native);
            native.set_send_buffer_size(4096).unwrap();
            peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            let descriptor =
                TokioSocket::new(crate::test_support::runtime().handle(), native).unwrap();
            let mut filled = 0;
            if interest == Interest::Writable {
                loop {
                    match crate::socket_io::send(descriptor.get_ref(), &[0; 65536]) {
                        Ok(count) => filled += count,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                        Err(error) => panic!("fill socket: {error}"),
                    }
                }
            }
            let mut first_buffer = [0];
            let mut second_buffer = [0];
            let operation = |socket: &socket2::Socket, buffer: &mut [u8]| {
                if interest == Interest::Readable {
                    crate::socket_io::recv(socket, buffer)
                } else {
                    crate::socket_io::send(socket, b"x")
                }
            };
            let mut first =
                Box::pin(descriptor.io(interest, |socket| operation(socket, &mut first_buffer)));
            let mut second =
                Box::pin(descriptor.io(interest, |socket| operation(socket, &mut second_buffer)));
            let (notified, notifications) = mpsc::channel();
            let first_waker = Waker::from(Arc::new(Notify(0, notified.clone())));
            let second_waker = Waker::from(Arc::new(Notify(1, notified)));
            assert!(matches!(
                first.as_mut().poll(&mut Context::from_waker(&first_waker)),
                Poll::Pending
            ));
            assert!(matches!(
                second
                    .as_mut()
                    .poll(&mut Context::from_waker(&second_waker)),
                Poll::Pending
            ));
            let draining = if interest == Interest::Readable {
                peer.write_all(b"ab").unwrap();
                None
            } else {
                Some(std::thread::spawn(move || {
                    let mut remaining = filled + 2;
                    let mut buffer = [0; 65536];
                    while remaining > 0 {
                        let count = peer.read(&mut buffer).unwrap();
                        assert_ne!(count, 0);
                        remaining -= count;
                    }
                }))
            };
            let mut seen = [false; 2];
            while !seen.iter().all(|seen| *seen) {
                seen[notifications
                    .recv_timeout(Duration::from_secs(5))
                    .expect("both independent waiters must be woken")] = true;
            }
            let (first, second) =
                futures_lite::future::block_on(futures_lite::future::zip(first, second));
            assert_eq!(first.unwrap(), 1);
            assert_eq!(second.unwrap(), 1);
            if let Some(draining) = draining {
                draining.join().unwrap();
            }
            let waiting = if interest == Interest::Readable {
                &descriptor.inner.read_waiting
            } else {
                &descriptor.inner.write_waiting
            };
            assert!(!waiting.load(Ordering::Acquire));
        }
    });
}

#[cfg(unix)]
#[test]
fn dropping_or_forgetting_a_reserved_waiter_preserves_other_waiters() {
    use futures_lite::future::{block_on, poll_once};
    use std::io::Write;
    crate::test_support::run(|| {
        for forget in [false, true] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let native = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            let (mut peer, _) = listener.accept().unwrap();
            let descriptor =
                TokioSocket::new(crate::test_support::runtime().handle(), native.into()).unwrap();
            let mut first = Box::pin(descriptor.io(Interest::Readable, |socket| {
                crate::socket_io::recv(socket, &mut [0])
            }));
            assert!(block_on(poll_once(&mut first)).is_none());
            assert!(descriptor.inner.read_waiting.load(Ordering::Acquire));
            let mut second = Box::pin(descriptor.io(Interest::Readable, |socket| {
                crate::socket_io::recv(socket, &mut [0])
            }));
            assert!(block_on(poll_once(&mut second)).is_none());
            if forget {
                std::mem::forget(first);
            } else {
                drop(first);
            }
            assert_eq!(
                descriptor.inner.read_waiting.load(Ordering::Acquire),
                forget
            );
            peer.write_all(b"ab").unwrap();
            assert_eq!(block_on(second).unwrap(), 1);
            assert_eq!(
                block_on(
                    descriptor.io(Interest::Readable, |socket| crate::socket_io::recv(
                        socket,
                        &mut [0]
                    ))
                )
                .unwrap(),
                1
            );
        }
    });
}

#[cfg(unix)]
#[test]
fn runtime_shutdown_wakes_both_reserved_and_concurrent_waiters() {
    use futures_lite::future::{block_on, poll_once};
    let runtime = runtime();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let native = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (_peer, _) = listener.accept().unwrap();
    let descriptor = TokioSocket::new(runtime.handle(), native.into()).unwrap();
    let mut first = Box::pin(descriptor.io(Interest::Readable, |socket| {
        crate::socket_io::recv(socket, &mut [0])
    }));
    let mut second = Box::pin(descriptor.io(Interest::Readable, |socket| {
        crate::socket_io::recv(socket, &mut [0])
    }));
    assert!(block_on(poll_once(&mut first)).is_none());
    assert!(block_on(poll_once(&mut second)).is_none());
    drop(runtime);
    assert!(block_on(first).is_err());
    assert!(block_on(second).is_err());
    assert!(!descriptor.inner.read_waiting.load(Ordering::Acquire));
}

#[cfg(unix)]
#[test]
fn established_stream_reads_only_retry_stale_readiness_once() {
    use base_scheduler::*;
    use futures_lite::future::{block_on, poll_once};
    use std::io::Write;
    crate::test_support::run(|| {
        block_on(async {
            for borrowed in [false, true] {
                for concurrent in [false, true] {
                    let scheduler = crate::test_support::scheduler();
                    let never = Cancellation::never();
                    let (native, mut peer) = tcp_pair();
                    let socket = TokioSocket::new(&scheduler.runtime, native.into()).unwrap();
                    // A real pending operation reserves the single-waker slot. The second reader must use the independent waiter path.
                    let mut reserved_buffer = [0; 8];
                    let mut reserved = Box::pin(socket.io(Interest::Readable, |socket| {
                        crate::socket_io::recv(socket, &mut reserved_buffer)
                    }));
                    if concurrent {
                        assert!(poll_once(&mut reserved).await.is_none());
                    }
                    let mut buffer = [0; 8];
                    let mut receiving = Box::pin(async {
                        if borrowed {
                            scheduler
                                .socket_recv_poll(
                                    &socket,
                                    &mut buffer,
                                    SocketRecvFlags::default(),
                                    &never,
                                )
                                .await
                                .unwrap()
                        } else {
                            let (result, received) =
                                scheduler.socket_read(&socket, vec![0; 8], &never).await;
                            buffer.copy_from_slice(&received);
                            result.unwrap()
                        }
                    });
                    assert!(poll_once(&mut receiving).await.is_none());
                    peer.write_all(b"x").unwrap();
                    assert_eq!(receiving.await, 1);
                    assert_eq!(buffer[0], b'x');
                    let mut attempts = 0;
                    let mut next = Box::pin(socket.io_with(
                        Interest::Readable,
                        |socket| {
                            attempts += 1;
                            crate::socket_io::recv(socket, &mut buffer)
                        },
                        true,
                    ));
                    assert!(poll_once(&mut next).await.is_none());
                    drop(next);
                    assert_eq!(
                        attempts, 1,
                        "the next read should clear stale readiness without an extra speculative syscall"
                    );
                    peer.write_all(b"y").unwrap();
                    assert_eq!(
                        scheduler
                            .socket_recv_poll(
                                &socket,
                                &mut buffer,
                                SocketRecvFlags::default(),
                                &never
                            )
                            .await
                            .unwrap(),
                        1
                    );
                    assert_eq!(buffer[0], b'y');
                    drop(reserved);
                    scheduler.socket_close(socket).await.unwrap();
                    scheduler.shutdown().await;
                }
            }
        });
    });
}

#[cfg(unix)]
#[test]
fn full_peeking_and_datagram_reads_retain_readability() {
    use base_scheduler::*;
    use futures_lite::future::{block_on, poll_once};
    use std::io::Write;
    crate::test_support::run(|| {
        block_on(async {
            let scheduler = crate::test_support::scheduler();
            let never = Cancellation::never();
            for peek in [false, true] {
                let (native, mut peer) = tcp_pair();
                let socket = TokioSocket::new(&scheduler.runtime, native.into()).unwrap();
                // A full read leaves bytes queued; a short peek leaves all bytes queued.
                let mut buffer = vec![0; if peek { 8 } else { 1 }];
                let mut receiving = Box::pin(scheduler.socket_recv_poll(
                    &socket,
                    &mut buffer,
                    SocketRecvFlags { peek },
                    &never,
                ));
                assert!(poll_once(&mut receiving).await.is_none());
                peer.write_all(b"xy").unwrap();
                assert_eq!(receiving.await.unwrap(), if peek { 2 } else { 1 });
                assert!(
                    poll_once(socket.inner.registration.get().unwrap().readable())
                        .await
                        .is_some()
                );
                let mut rest = [0; 8];
                assert_eq!(
                    scheduler
                        .socket_recv_poll(&socket, &mut rest, SocketRecvFlags::default(), &never)
                        .await
                        .unwrap(),
                    if peek { 2 } else { 1 }
                );
                assert_eq!(rest[0], if peek { b'x' } else { b'y' });
                scheduler.socket_close(socket).await.unwrap();
            }
            let native = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let address = native.local_addr().unwrap();
            let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let socket = TokioSocket::new(&scheduler.runtime, native.into()).unwrap();
            let mut buffer = [0; 8];
            let mut receiving = Box::pin(scheduler.socket_recv_poll(
                &socket,
                &mut buffer,
                SocketRecvFlags::default(),
                &never,
            ));
            assert!(poll_once(&mut receiving).await.is_none());
            peer.send_to(b"x", address).unwrap();
            peer.send_to(b"y", address).unwrap();
            assert_eq!(receiving.await.unwrap(), 1);
            assert!(
                poll_once(socket.inner.registration.get().unwrap().readable())
                    .await
                    .is_some()
            );
            assert_eq!(
                scheduler
                    .socket_recv_poll(&socket, &mut buffer, SocketRecvFlags::default(), &never)
                    .await
                    .unwrap(),
                1
            );
            assert_eq!(buffer[0], b'y');
            scheduler.socket_close(socket).await.unwrap();
            scheduler.shutdown().await;
        });
    });
}

#[cfg(target_os = "linux")]
#[test]
fn registered_listeners_still_report_invalid_read_errors_immediately() {
    use base_scheduler::*;
    use futures_lite::future::{block_on, poll_once};
    crate::test_support::run(|| {
        block_on(async {
            let scheduler = crate::test_support::scheduler();
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let socket = TokioSocket::new(&scheduler.runtime, listener.into()).unwrap();
            let cancel = Cancellation::new();
            let mut accepting = Box::pin(scheduler.socket_accept(&socket, &cancel));
            assert!(poll_once(&mut accepting).await.is_none());
            cancel.cancel();
            assert!(matches!(accepting.await, Err(OperationError::Cancelled(_))));
            assert!(socket.inner.registration.get().is_some());
            let never = Cancellation::never();
            for length in [0, 8] {
                let mut buffer = vec![0; length];
                let result = poll_once(scheduler.socket_recv_poll(
                    &socket,
                    &mut buffer,
                    SocketRecvFlags::default(),
                    &never,
                ))
                .await;
                assert!(matches!(result, Some(Err(OperationError::Io(_)))));
                let (result, _) =
                    poll_once(scheduler.socket_read(&socket, vec![0; length], &never))
                        .await
                        .expect("invalid read must not wait for listener readability");
                assert!(matches!(result, Err(OperationError::Io(_))));
            }
            scheduler.socket_close(socket).await.unwrap();
            scheduler.shutdown().await;
        });
    });
}

#[cfg(unix)]
#[test]
fn established_stream_empty_reads_do_not_require_reactor_progress() {
    use base_scheduler::*;
    use futures_lite::future::{block_on, poll_once};
    use std::io::Write;
    let runtime = runtime();
    let scheduler = crate::TokioScheduler::with_options(
        runtime.handle().clone(),
        crate::TokioOptions {
            threads: 1,
            capacity: 4,
        },
    )
    .unwrap();
    let never = Cancellation::never();
    let (native, mut peer) = tcp_pair();
    let socket = TokioSocket::new(runtime.handle(), native.into()).unwrap();
    let registration = block_on(socket.inner.registration()).unwrap();
    assert!(block_on(poll_once(registration.readable())).is_none());
    peer.write_all(b"x").unwrap();
    wait_for_readable(&socket);
    let mut byte = [0];
    assert_eq!(
        block_on(scheduler.socket_recv_poll(
            &socket,
            &mut byte,
            SocketRecvFlags::default(),
            &never
        ))
        .unwrap(),
        1
    );
    assert!(socket.inner.connected_stream.load(Ordering::Relaxed));
    // The current-thread reactor is deliberately stopped, so the next peer write cannot update cached readiness. Empty reads must still return the syscall result immediately.
    peer.write_all(b"y").unwrap();
    wait_for_readable(&socket);
    assert!(matches!(
        block_on(poll_once(scheduler.socket_recv_poll(
            &socket,
            &mut [],
            SocketRecvFlags::default(),
            &never
        ))),
        Some(Ok(0))
    ));
    let (result, buffer) = block_on(poll_once(scheduler.socket_read(
        &socket,
        Vec::new(),
        &never,
    )))
    .expect("empty owned read must not await reactor progress");
    assert_eq!(result.unwrap(), 0);
    assert!(buffer.is_empty());
    // A nonempty established read uses the reactor and resumes when its selected runtime is driven again.
    let mut reading = Box::pin(scheduler.socket_recv_poll(
        &socket,
        &mut byte,
        SocketRecvFlags::default(),
        &never,
    ));
    assert!(block_on(poll_once(&mut reading)).is_none());
    assert_eq!(runtime.block_on(reading).unwrap(), 1);
    assert_eq!(&byte, b"y");
    block_on(scheduler.socket_close(socket)).unwrap();
    block_on(scheduler.shutdown());
}

#[cfg(unix)]
fn tcp_pair() -> (std::net::TcpStream, std::net::TcpStream) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (server, _) = listener.accept().unwrap();
    client.set_nodelay(true).unwrap();
    server.set_nodelay(true).unwrap();
    (client, server)
}

#[cfg(unix)]
fn wait_for_readable(socket: &TokioSocket) {
    use std::os::fd::AsRawFd;
    let mut descriptor = libc::pollfd {
        fd: socket.get_ref().as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: One initialized descriptor remains live through this bounded synchronous wait. This observes packet arrival without driving Tokio's deliberately stopped reactor.
    assert_eq!(unsafe { libc::poll(&mut descriptor, 1, 5000) }, 1);
}

#[cfg(unix)]
#[test]
fn datagrams_preserve_direct_syscall_behavior() {
    use base_scheduler::*;
    use futures_lite::future::block_on;
    let runtime = runtime();
    let scheduler = crate::TokioScheduler::with_options(
        runtime.handle().clone(),
        crate::TokioOptions {
            threads: 1,
            capacity: 4,
        },
    )
    .unwrap();
    let never = Cancellation::never();
    let native = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let address = native.local_addr().unwrap();
    let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let socket = TokioSocket::new(runtime.handle(), native.into()).unwrap();
    assert!(!socket.inner.stream);
    block_on(socket.inner.registration()).unwrap();
    let mut buffer = [0; 8];
    for byte in *b"xy" {
        peer.send_to(&[byte], address).unwrap();
        wait_for_readable(&socket);
        assert_eq!(
            block_on(scheduler.socket_recv_poll(
                &socket,
                &mut buffer,
                SocketRecvFlags::default(),
                &never
            ))
            .unwrap(),
            1
        );
        assert_eq!(buffer[0], byte);
        assert!(!socket.inner.connected_stream.load(Ordering::Relaxed));
    }
    block_on(scheduler.socket_close(socket)).unwrap();
    block_on(scheduler.shutdown());
}

#[cfg(target_os = "linux")]
#[test]
fn urgent_tcp_boundary_keeps_trailing_bytes_readable() {
    use base_scheduler::*;
    use futures_lite::future::block_on;
    use std::io::Write;
    let runtime = runtime();
    let scheduler = crate::TokioScheduler::with_options(
        runtime.handle().clone(),
        crate::TokioOptions {
            threads: 1,
            capacity: 4,
        },
    )
    .unwrap();
    let never = Cancellation::never();
    let (native, mut peer) = tcp_pair();
    let socket = TokioSocket::new(runtime.handle(), native.into()).unwrap();
    let registration = block_on(socket.inner.registration()).unwrap();
    let mut buffer = [0; 64];
    peer.write_all(b"seed").unwrap();
    assert_eq!(
        block_on(scheduler.socket_recv_poll(
            &socket,
            &mut buffer,
            SocketRecvFlags::default(),
            &never
        ))
        .unwrap(),
        4
    );
    assert!(socket.inner.connected_stream.load(Ordering::Relaxed));
    peer.write_all(b"before").unwrap();
    let peer_socket = socket2::SockRef::from(&peer);
    assert_eq!(peer_socket.send_with_flags(b"!", libc::MSG_OOB).unwrap(), 1);
    peer.write_all(b"after").unwrap();
    // Let every loopback segment arrive before the current-thread reactor captures its edge.
    std::thread::sleep(Duration::from_millis(10));
    runtime.block_on(async {
        registration.readable().await.unwrap().retain_ready();
    });
    let count = runtime
        .block_on(scheduler.socket_recv_poll(
            &socket,
            &mut buffer,
            SocketRecvFlags::default(),
            &never,
        ))
        .unwrap();
    assert_eq!(&buffer[..count], b"before");
    let result = runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_millis(100),
            scheduler.socket_recv_poll(&socket, &mut buffer, SocketRecvFlags::default(), &never),
        )
        .await
    });
    // The first read stops at the urgent mark. The following ordinary bytes are already queued; another peer write must not be necessary.
    assert!(
        result.is_ok(),
        "trailing normal TCP bytes stalled after a short read at an urgent mark"
    );
    let count = result.unwrap().unwrap();
    assert_eq!(&buffer[..count], b"after");
    block_on(scheduler.socket_close(socket)).unwrap();
    block_on(scheduler.shutdown());
}

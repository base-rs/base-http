// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::super::tests::{drain, drive, pair};
use super::*;
use crate::SocketMessage;
use futures_lite::future::{block_on, poll_once};
use std::sync::atomic::AtomicUsize;

async fn socket(selector: &IoUringSelector) -> NativeSocket {
    let never = Cancellation::never();
    let socket = selector
        .socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never)
        .await
        .unwrap();
    selector
        .socket_bind(&socket, "127.0.0.1:0".parse().unwrap(), &never)
        .await
        .unwrap();
    socket
}

async fn operation<B: IoBufSequenceMut>(
    selector: &IoUringSelector,
    socket: &NativeSocket,
    buffers: B,
    send: bool,
    token: &Cancellation,
) -> (OperationResult<usize>, B) {
    if send {
        let (result, message) = selector
            .socket_sendmsg(
                socket,
                SocketMessage {
                    buffers,
                    address: Some(socket.local_addr().unwrap()),
                },
                SocketSendFlags::default(),
                token,
            )
            .await;
        (result, message.buffers)
    } else {
        let (result, buffers) = selector
            .socket_recvmsg(socket, buffers, SocketRecvFlags::default(), token)
            .await;
        (result.map(|result| result.bytes), buffers)
    }
}

struct Observed {
    bytes: Vec<u8>,
    drops: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for Observed {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsMut<[u8]> for Observed {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
// SAFETY: Vec owns initialized, exclusively accessible stable storage.
unsafe impl IoBuf for Observed {}
// SAFETY: This owner has exclusive access to its initialized bytes.
unsafe impl IoBufMut for Observed {}
impl Drop for Observed {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn message_cancellation_and_abandonment_retain_payload_until_original_completion() {
    block_on(async {
        for send in [false, true] {
            for abandon in [false, true] {
                for success in [false, true] {
                    let (selector, mut driver) = pair(1);
                    let socket = socket(&selector).await;
                    let drops = Arc::new(AtomicUsize::new(0));
                    let buffers = [Observed {
                        bytes: vec![7],
                        drops: Arc::clone(&drops),
                    }];
                    let pointer = buffers[0].bytes.as_ptr();
                    let token = Cancellation::new();
                    let mut future = Box::pin(operation(&selector, &socket, buffers, send, &token));
                    assert!(poll_once(&mut future).await.is_none());
                    let Command::Submit(mut request) = driver
                        .reactor
                        .as_ref()
                        .unwrap()
                        .incoming
                        .try_recv()
                        .unwrap()
                    else {
                        panic!("expected submission")
                    };
                    assert_eq!(
                        request.entry().get_opcode() as u8,
                        if send {
                            opcode::SendMsg::CODE
                        } else {
                            opcode::RecvMsg::CODE
                        }
                    );
                    assert!(request.begin_cancellation());
                    assert!(!request.begin_cancellation());
                    token.cancel();
                    assert!(poll_once(&mut future).await.is_none());
                    assert!(
                        matches!(driver.reactor.as_ref().unwrap().incoming.try_recv().unwrap(), Command::Cancel(id) if id == request.identifier())
                    );
                    assert_eq!(drops.load(Ordering::SeqCst), 0);
                    let result = if success {
                        Ok(0)
                    } else {
                        Err(io::Error::from_raw_os_error(libc::ECANCELED))
                    };
                    if abandon {
                        drop(future);
                        assert_eq!(drops.load(Ordering::SeqCst), 0);
                        request.finish(result);
                    } else {
                        request.finish(result);
                        let (result, buffers) = future.await;
                        if success {
                            assert_eq!(result.unwrap(), 0);
                        } else {
                            assert!(matches!(result, Err(OperationError::Cancelled(_))));
                        }
                        assert_eq!(buffers[0].bytes.as_ptr(), pointer);
                        drop(buffers);
                    }
                    assert_eq!(drops.load(Ordering::SeqCst), 1);
                    selector.socket_close(socket).await.unwrap();
                    drain(&selector, &mut driver);
                }
            }
        }
    });
}

#[test]
fn queued_or_disconnected_message_requests_return_owners_and_unknown_completion_panics() {
    block_on(async {
        for send in [false, true] {
            for lose_reply in [false, true] {
                let (selector, mut driver) = pair(1);
                let socket = socket(&selector).await;
                let never = Cancellation::never();
                let mut future = Box::pin(operation(&selector, &socket, [vec![9]], send, &never));
                assert!(poll_once(&mut future).await.is_none());
                let Command::Submit(request) = driver
                    .reactor
                    .as_ref()
                    .unwrap()
                    .incoming
                    .try_recv()
                    .unwrap()
                else {
                    panic!("expected submission")
                };
                if lose_reply {
                    drop(request.take_reply());
                    assert!(request.take_reply().is_none());
                    // This harness never submitted to the kernel, so it can release the request. Production failure retains unknown in-flight storage.
                    drop(request);
                    assert!(
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(future)))
                            .is_err()
                    );
                } else {
                    drop(request);
                    let (result, buffers) = future.await;
                    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
                    assert_eq!(buffers, [vec![9]]);
                }
                selector.socket_close(socket).await.unwrap();
                drain(&selector, &mut driver);
            }
        }
    });
}

#[test]
fn message_admission_is_lazy_cancellable_and_reports_closure_or_exhaustion() {
    block_on(async {
        let (selector, mut driver) = pair(1);
        let socket = socket(&selector).await;
        let never = Cancellation::never();
        for send in [false, true] {
            let cancelled = Cancellation::new();
            cancelled.cancel();
            let buffers = [vec![42]];
            let pointer = buffers[0].as_ptr();
            let (result, buffers) = operation(&selector, &socket, buffers, send, &cancelled).await;
            assert!(matches!(result, Err(OperationError::Cancelled(_))));
            assert_eq!(buffers, [vec![42]]);
            assert_eq!(buffers[0].as_ptr(), pointer);
            drop(operation(&selector, &socket, [vec![1]], send, &never));
            assert!(
                driver
                    .reactor
                    .as_ref()
                    .unwrap()
                    .incoming
                    .try_recv()
                    .is_err()
            );
        }
        let mut first = Box::pin(operation(&selector, &socket, [vec![1]], false, &never));
        assert!(poll_once(&mut first).await.is_none());
        for send in [false, true] {
            let token = Cancellation::new();
            let mut second = Box::pin(operation(&selector, &socket, [vec![2]], send, &token));
            assert!(poll_once(&mut second).await.is_none());
            token.cancel();
            let (result, buffers) = second.await;
            assert!(matches!(result, Err(OperationError::Cancelled(_))));
            assert_eq!(buffers, [vec![2]]);
        }
        drop(first);
        drain(&selector, &mut driver);
        for send in [false, true] {
            let (result, buffers) = operation(&selector, &socket, [vec![3]], send, &never).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffers, [vec![3]]);
        }
        let (selector, mut driver) = pair(1);
        let socket = self::socket(&selector).await;
        selector.controllers[0]
            .next_identifier
            .store(CANCEL_TAG - 1, Ordering::Relaxed);
        for send in [false, true] {
            assert_eq!(
                operation(&selector, &socket, [vec![4]], send, &never)
                    .await
                    .0
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::Other
            );
        }
        drain(&selector, &mut driver);
    });
}

#[test]
fn message_length_validation_rejects_overflow_without_truncating_datagrams() {
    for (count, lengths) in [
        (i32::MAX as usize + 1, vec![]),
        (1, vec![i32::MAX as usize + 1]),
        (2, vec![usize::MAX, 1]),
    ] {
        assert_eq!(
            validate_lengths(count, lengths.into_iter())
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
    validate_lengths(1, [i32::MAX as usize].into_iter()).unwrap();
    validate_lengths(0, [].into_iter()).unwrap();
}

#[test]
fn ordinary_sequences_preserve_datagram_addresses_flags_and_payloads() {
    let (selector, mut driver) = pair(2);
    let sender = block_on(socket(&selector));
    let receiver = block_on(socket(&selector));
    let never = Cancellation::never();
    let address = receiver.local_addr().unwrap();
    let (result, message) = drive(
        &mut driver,
        selector.socket_sendmsg(
            &sender,
            SocketMessage {
                buffers: [
                    b"abc".to_vec().into_boxed_slice(),
                    b"def".to_vec().into_boxed_slice(),
                ],
                address: Some(address),
            },
            SocketSendFlags { dont_route: true },
            &never,
        ),
    );
    assert_eq!(result.unwrap(), 6);
    assert_eq!(message.address, Some(address));
    for peek in [true, false] {
        let (result, buffers) = drive(
            &mut driver,
            selector.socket_recvmsg(
                &receiver,
                [vec![0; 2].into_boxed_slice(), vec![0; 2].into_boxed_slice()],
                SocketRecvFlags { peek },
                &never,
            ),
        );
        let result = result.unwrap();
        assert_eq!(result.bytes, 4);
        assert!(result.truncated);
        assert_eq!(result.address, Some(sender.local_addr().unwrap()));
        assert_eq!(&*buffers[0], b"ab");
        assert_eq!(&*buffers[1], b"cd");
    }
    // Neither side supplies ancillary storage. Linux reports discarded packet metadata.
    let retained = retain_socket(&receiver);
    let enabled: libc::c_int = 1;
    // SAFETY: setsockopt reads a live integer synchronously; socket and registration are retained and unchanged.
    assert_eq!(
        unsafe {
            libc::setsockopt(
                retained.get_ref().as_raw_fd(),
                libc::IPPROTO_IP,
                libc::IP_PKTINFO,
                (&enabled as *const libc::c_int).cast(),
                std::mem::size_of_val(&enabled) as _,
            )
        },
        0
    );
    drop(retained);
    drive(
        &mut driver,
        selector.socket_sendmsg(
            &sender,
            SocketMessage {
                buffers: [vec![42]],
                address: Some(address),
            },
            SocketSendFlags::default(),
            &never,
        ),
    )
    .0
    .unwrap();
    let (result, buffers) = drive(
        &mut driver,
        selector.socket_recvmsg(&receiver, [vec![0]], SocketRecvFlags::default(), &never),
    );
    assert!(result.unwrap().control_truncated);
    assert_eq!(buffers, [vec![42]]);
    block_on(selector.socket_close(sender)).unwrap();
    block_on(selector.socket_close(receiver)).unwrap();
    drain(&selector, &mut driver);
}

#[test]
fn empty_receive_consumes_one_datagram_without_replacing_the_sequence() {
    let (selector, mut driver) = pair(2);
    let socket = block_on(socket(&selector));
    let never = Cancellation::never();
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    sender.send_to(b"", socket.local_addr().unwrap()).unwrap();
    let (result, buffers) = drive(
        &mut driver,
        selector.socket_recvmsg(
            &socket,
            Vec::<Vec<u8>>::new(),
            SocketRecvFlags::default(),
            &never,
        ),
    );
    assert_eq!(result.unwrap().bytes, 0);
    assert!(buffers.is_empty());
    sender
        .send_to(b"next", socket.local_addr().unwrap())
        .unwrap();
    let (result, buffers) = drive(
        &mut driver,
        selector.socket_recvmsg(&socket, [vec![0; 4]], SocketRecvFlags::default(), &never),
    );
    assert_eq!(result.unwrap().bytes, 4);
    assert_eq!(buffers, [b"next".to_vec()]);
    drain(&selector, &mut driver);
}

struct InlineBuffers {
    // Vec has stable payload storage; the iovec itself moves with this owner before submission.
    bytes: Vec<u8>,
    slices: [std::io::IoSliceMut<'static>; 1],
}
impl InlineBuffers {
    fn new(mut bytes: Vec<u8>) -> Self {
        // SAFETY: The private Vec retains this allocation until drop. All byte access uses slices; the owner never reallocates or exposes the erased lifetime.
        let slice = unsafe {
            std::mem::transmute::<std::io::IoSliceMut<'_>, std::io::IoSliceMut<'static>>(
                std::io::IoSliceMut::new(&mut bytes),
            )
        };
        Self {
            bytes,
            slices: [slice],
        }
    }
}
// SAFETY: This owner retains one unchanged initialized range and a consistent native vector. The vector is inline, so completion backends must keep the sequence immobile after borrowing it.
unsafe impl IoBufSequence for InlineBuffers {
    fn buffer_count(&self) -> usize {
        1
    }
    fn buffer(&self, index: usize) -> &[u8] {
        &self.slices[index]
    }
    fn io_slices(&self) -> Option<&[std::io::IoSlice<'_>]> {
        // SAFETY: On Linux both standard types have the iovec ABI; this shared view cannot write payload or replace vectors.
        Some(unsafe { std::slice::from_raw_parts(self.slices.as_ptr().cast(), 1) })
    }
}
// SAFETY: Only this owner accesses its single writable range; its vector and payload are retained unchanged until retirement.
unsafe impl IoBufSequenceMut for InlineBuffers {
    fn buffer_mut(&mut self, index: usize) -> &mut [u8] {
        &mut self.slices[index]
    }
    fn io_slices_mut(&mut self) -> Option<&[std::io::IoSliceMut<'_>]> {
        Some(&self.slices)
    }
}

#[test]
fn inline_prepared_vectors_survive_submission_and_are_used_without_copying() {
    let (selector, mut driver) = pair(2);
    let (sender, receiver) = block_on(base_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let never = Cancellation::never();
    let (result, message) = drive(
        &mut driver,
        selector.socket_sendmsg(
            &sender,
            SocketMessage {
                buffers: InlineBuffers::new(b"inline".to_vec()),
                address: None,
            },
            SocketSendFlags::default(),
            &never,
        ),
    );
    assert_eq!(result.unwrap(), 6);
    assert_eq!(message.buffers.bytes.len(), 6);
    assert_eq!(message.buffers.buffer(0), b"inline");
    let (result, mut buffers) = drive(
        &mut driver,
        selector.socket_recvmsg(
            &receiver,
            InlineBuffers::new(vec![0; 6]),
            SocketRecvFlags::default(),
            &never,
        ),
    );
    assert_eq!(result.unwrap().bytes, 6);
    assert_eq!(buffers.buffer_mut(0), b"inline");
    block_on(selector.socket_close(sender)).unwrap();
    block_on(selector.socket_close(receiver)).unwrap();
    drain(&selector, &mut driver);
}

#[test]
fn submitted_message_keeps_payload_alive_after_waiter_drop_until_driver_retirement() {
    let (selector, mut driver) = pair(1);
    let socket = block_on(socket(&selector));
    let drops = Arc::new(AtomicUsize::new(0));
    let never = Cancellation::never();
    let mut operation = Box::pin(selector.socket_recvmsg(
        &socket,
        IoBuffersMut::new([Observed {
            bytes: vec![0],
            drops: Arc::clone(&drops),
        }]),
        SocketRecvFlags::default(),
        &never,
    ));
    assert!(block_on(poll_once(&mut operation)).is_none());
    driver.turn(Some(Duration::ZERO)).unwrap();
    assert_eq!(driver.reactor.as_ref().unwrap().in_flight.0.len(), 1);
    drop(operation);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drain(&selector, &mut driver);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

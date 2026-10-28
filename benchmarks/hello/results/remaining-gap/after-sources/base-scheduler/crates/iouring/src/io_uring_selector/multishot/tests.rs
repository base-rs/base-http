// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::super::tests::{drain, drive, pair};
use super::*;
use futures_lite::future::{block_on, poll_once};

use base_scheduler_selector::{SelectorDriver, SelectorWorker};

thread_local! {
    static BEFORE_RECEIVE_LISTEN: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

pub(super) fn before_receive_listen() {
    let hook = BEFORE_RECEIVE_LISTEN.with(|hook| hook.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

fn driven<F: Future>(worker: &mut crate::IoUringWorker, future: F) -> F::Output {
    let mut future = pin!(future);
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(output) = block_on(poll_once(future.as_mut())) {
            return output;
        }
        worker.turn(Some(Duration::from_millis(1))).unwrap();
        assert!(std::time::Instant::now() < deadline);
    }
}

fn receiver(
    selector: &IoUringSelector,
    socket: &NativeSocket,
    buffers: u16,
    buffer_size: usize,
) -> RecvMsgMultishot {
    block_on(selector.socket_recvmsg_multishot(
        socket,
        RecvMsgMultishotOptions {
            buffers,
            buffer_size,
        },
        SocketRecvFlags::default(),
        &Cancellation::never(),
    ))
    .unwrap()
}

#[test]
fn persistent_stream_reuses_one_submission_and_keeps_leases_alive_after_shutdown() {
    let (selector, mut driver) = pair(2);
    let (sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let mut receiving = receiver(&selector, &socket, 4, 128);
    let never = Cancellation::never();
    let mut saved = None;
    for index in 0..20 {
        let bytes = [index; 4];
        sender.try_send(&bytes, SocketSendFlags::default()).unwrap();
        let message = drive(&mut driver, receiving.recv(&never)).unwrap().unwrap();
        assert_eq!(message.payload(), &bytes);
        if index == 0 {
            saved = Some(message);
        }
    }
    assert_eq!(receiving.statistics().submissions, 1);
    assert_eq!(receiving.statistics().messages, 20);
    drive(&mut driver, receiving.close()).unwrap();
    drain(&selector, &mut driver);
    drop(receiving);
    drop(sender);
    drop(socket);
    drop(driver);
    drop(selector);
    let saved = saved.unwrap();
    assert_eq!(saved.payload(), &[0; 4]);
    drop(saved);
}

#[test]
fn empty_datagrams_truncation_and_exhaustion_preserve_message_boundaries() {
    let (selector, mut driver) = pair(2);
    let socket = block_on(async {
        let socket = selector
            .socket_open(
                SocketDomain::Ipv4,
                SocketType::Datagram,
                None,
                &Cancellation::never(),
            )
            .await
            .unwrap();
        selector
            .socket_bind(
                &socket,
                "127.0.0.1:0".parse().unwrap(),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        socket
    });
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut receiving = receiver(&selector, &socket, 1, 4);
    sender
        .send_to(b"abcdef", socket.local_addr().unwrap())
        .unwrap();
    let first = drive(&mut driver, receiving.recv(&Cancellation::never()))
        .unwrap()
        .unwrap();
    assert_eq!(first.payload(), b"abcd");
    assert!(first.truncated());
    assert_eq!(
        first.address().map(|address| address.to_socket_address()),
        Some(sender.local_addr().unwrap())
    );
    sender.send_to(b"", socket.local_addr().unwrap()).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while receiving.statistics().buffer_exhaustions == 0 {
        driver.turn(Some(Duration::from_millis(1))).unwrap();
        assert!(std::time::Instant::now() < deadline);
    }
    assert!(!driver.reactor.as_ref().unwrap().dormant.is_empty());
    drop(first);
    let second = drive(&mut driver, receiving.recv(&Cancellation::never()))
        .unwrap()
        .unwrap();
    assert!(second.payload().is_empty());
    assert!(!second.truncated());
    assert!(receiving.statistics().submissions >= 2);
    drop(second);
    drive(&mut driver, receiving.close()).unwrap();
    drain(&selector, &mut driver);
}

#[test]
fn shutdown_during_buffer_exhaustion_retires_without_a_transport_error() {
    let (selector, mut driver) = pair(2);
    let (sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let mut receiving = receiver(&selector, &socket, 1, 4);
    sender
        .try_send(b"abcdefgh", SocketSendFlags::default())
        .unwrap();
    driver.turn(Some(Duration::ZERO)).unwrap();
    {
        let reactor = driver.reactor.as_mut().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let timeout = types::Timespec::new().nsec(1_000_000);
        let arguments = types::SubmitArgs::new().timespec(&timeout);
        // Keep the real CQEs queued until shutdown: the command notification, one selected buffer, and terminal ENOBUFS. No buffer has been returned for rearming.
        while reactor.ring.completion().len() < 3 {
            match reactor.ring.submitter().submit_with_args(3, &arguments) {
                Ok(_) => {}
                Err(error) if error.raw_os_error() == Some(libc::ETIME) => {}
                Err(error) => panic!("receive did not complete: {error}"),
            }
            assert!(std::time::Instant::now() < deadline);
        }
    }
    selector.close();
    drive(&mut driver, receiving.close()).unwrap();
    assert_eq!(receiving.statistics().messages, 1);
    assert_eq!(receiving.statistics().buffer_exhaustions, 1);
    drain(&selector, &mut driver);
}

#[test]
fn cancellation_and_abandonment_retire_before_exclusive_socket_close() {
    for abandon in [false, true] {
        let (selector, mut driver) = pair(1);
        let (_sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
            &selector,
            SocketDomain::Ipv4,
        ));
        let mut receiving = receiver(&selector, &socket, 2, 8);
        driver.turn(Some(Duration::ZERO)).unwrap();
        if abandon {
            drop(receiving);
        } else {
            let token = Cancellation::new();
            let mut next = Box::pin(receiving.recv(&token));
            assert!(block_on(poll_once(&mut next)).is_none());
            token.cancel();
            assert!(matches!(
                drive(&mut driver, next),
                Err(OperationError::Cancelled(_))
            ));
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while selector.controllers[0]
            .capacity
            .state
            .load(Ordering::Relaxed)
            != 0
        {
            driver.turn(Some(Duration::from_millis(1))).unwrap();
            assert!(std::time::Instant::now() < deadline);
        }
        block_on(selector.socket_close(socket)).unwrap();
        drain(&selector, &mut driver);
    }
}

#[test]
fn construction_is_lazy_and_rejects_invalid_options_peeking_cancellation_and_closure() {
    let (selector, mut driver) = pair(1);
    let (_sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let never = Cancellation::never();
    let options = RecvMsgMultishotOptions::default();
    drop(selector.socket_recvmsg_multishot(&socket, options, SocketRecvFlags::default(), &never));
    assert_eq!(
        selector.controllers[0]
            .next_identifier
            .load(Ordering::Relaxed),
        0
    );
    for (buffers, buffer_size) in [
        (0, 1),
        (65535, 1),
        (1, 0),
        (1, usize::MAX),
        (1, i32::MAX as usize),
    ] {
        assert!(
            matches!(block_on(selector.socket_recvmsg_multishot(&socket, RecvMsgMultishotOptions { buffers, buffer_size }, SocketRecvFlags::default(), &never)), Err(OperationError::Io(error)) if error.kind() == io::ErrorKind::InvalidInput)
        );
    }
    assert!(
        block_on(selector.socket_recvmsg_multishot(
            &socket,
            options,
            SocketRecvFlags { peek: true },
            &never
        ))
        .is_err()
    );
    let token = Cancellation::new();
    token.cancel();
    assert!(matches!(
        block_on(selector.socket_recvmsg_multishot(
            &socket,
            options,
            SocketRecvFlags::default(),
            &token
        )),
        Err(OperationError::Cancelled(_))
    ));
    drain(&selector, &mut driver);
    assert!(
        block_on(selector.socket_recvmsg_multishot(
            &socket,
            options,
            SocketRecvFlags::default(),
            &never
        ))
        .is_err()
    );
}

#[test]
fn eof_follows_data_and_leases_support_owned_writes() {
    let (selector, mut driver) = pair(2);
    let (sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let mut receiving = receiver(&selector, &socket, 2, 128);
    sender
        .try_send(b"echo", SocketSendFlags::default())
        .unwrap();
    let message = drive(&mut driver, receiving.recv(&Cancellation::never()))
        .unwrap()
        .unwrap();
    let pointer = message.payload().as_ptr();
    let (result, returned) = drive(
        &mut driver,
        selector.socket_write(&socket, message.into_payload(), &Cancellation::never()),
    );
    assert_eq!(result.unwrap(), 4);
    assert_eq!(returned.as_ref().as_ptr(), pointer);
    let mut bytes = [0; 4];
    assert_eq!(
        drive(
            &mut driver,
            selector.socket_recv_poll(
                &sender,
                &mut bytes,
                SocketRecvFlags::default(),
                &Cancellation::never()
            )
        )
        .unwrap(),
        4
    );
    assert_eq!(&bytes, b"echo");
    drop(returned);
    drop(sender);
    assert!(
        drive(&mut driver, receiving.recv(&Cancellation::never()))
            .unwrap()
            .is_none()
    );
    drive(&mut driver, receiving.close()).unwrap();
    block_on(selector.socket_close(socket)).unwrap();
    drain(&selector, &mut driver);
}

#[test]
fn remote_return_rearms_the_original_pool_and_forgotten_receivers_drain_on_shutdown() {
    let (selector, mut driver) = pair(1);
    let (sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let mut receiving = receiver(&selector, &socket, 1, 4);
    sender
        .try_send(b"first", SocketSendFlags::default())
        .unwrap();
    let first = drive(&mut driver, receiving.recv(&Cancellation::never()))
        .unwrap()
        .unwrap();
    assert_eq!(first.payload(), b"firs");
    std::thread::spawn(move || drop(first)).join().unwrap();
    let second = drive(&mut driver, receiving.recv(&Cancellation::never()))
        .unwrap()
        .unwrap();
    assert_eq!(second.payload(), b"t");
    drop(second);
    std::mem::forget(receiving);
    drain(&selector, &mut driver);
    // A forgotten receiver cannot leave its socket held by a kernel request after shutdown.
    // SAFETY: No pointer escapes and the extra owner is released immediately after inspection.
    assert_eq!(Arc::strong_count(&unsafe { socket.retain_native() }), 2);
}

#[test]
fn unpublished_receivers_are_notified_when_the_driver_is_dropped() {
    for local in [false, true] {
        let (selector, driver) = pair(1);
        let (_sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
            &selector,
            SocketDomain::Ipv4,
        ));
        let owner: Box<dyn std::any::Any> = if local {
            Box::new(base_scheduler_selector::SelectorDriver::enter(driver))
        } else {
            Box::new(driver)
        };
        let mut receiving = receiver(&selector, &socket, 1, 8);
        drop(owner);
        assert!(block_on(receiving.recv(&Cancellation::never())).is_err());
        assert!(block_on(receiving.close()).is_ok());
    }
}

#[test]
fn pool_errors_and_driver_failure_notify_the_receiver() {
    use super::super::tests::Failures;
    for failure in ["register-buffer-ring", "unregister-buffer-ring", "wait"] {
        let (selector, mut driver) = pair(1);
        let (_sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
            &selector,
            SocketDomain::Ipv4,
        ));
        let mut receiving = receiver(&selector, &socket, 2, 8);
        if failure != "register-buffer-ring" {
            driver.turn(Some(Duration::ZERO)).unwrap();
        }
        let _failures = Failures::new([(failure, io::Error::from_raw_os_error(libc::EIO))]);
        match failure {
            "register-buffer-ring" => {
                assert!(drive(&mut driver, receiving.recv(&Cancellation::never())).is_err());
                assert!(drive(&mut driver, receiving.close()).is_ok());
            }
            "unregister-buffer-ring" => {
                assert_eq!(
                    drive(&mut driver, receiving.close())
                        .unwrap_err()
                        .raw_os_error(),
                    Some(libc::EIO)
                );
            }
            _ => {
                // Consume the initial remote-submission notification before reaching the injected idle wait.
                while driver.turn(Some(Duration::from_millis(1))).is_ok() {}
                assert!(block_on(receiving.recv(&Cancellation::never())).is_err());
                assert!(block_on(receiving.close()).is_ok());
            }
        }
        drop(_failures);
        drain(&selector, &mut driver);
    }
}

#[test]
fn dormant_close_and_shutdown_leave_held_leases_valid() {
    for shutdown in [false, true] {
        let (selector, mut driver) = pair(1);
        let (sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
            &selector,
            SocketDomain::Ipv4,
        ));
        let mut receiving = receiver(&selector, &socket, 1, 4);
        sender
            .try_send(b"abcdefgh", SocketSendFlags::default())
            .unwrap();
        let message = drive(&mut driver, receiving.recv(&Cancellation::never()))
            .unwrap()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while driver.reactor.as_ref().unwrap().dormant.is_empty() {
            driver.turn(Some(Duration::from_millis(1))).unwrap();
            assert!(std::time::Instant::now() < deadline);
        }
        if shutdown {
            drain(&selector, &mut driver);
            assert!(matches!(
                block_on(receiving.recv(&Cancellation::never())),
                Err(OperationError::Io(_))
            ));
        } else {
            drive(&mut driver, receiving.close()).unwrap();
            assert!(
                block_on(receiving.recv(&Cancellation::never()))
                    .unwrap()
                    .is_none()
            );
            drain(&selector, &mut driver);
        }
        assert_eq!(message.payload(), b"abcd");
    }
}

#[test]
fn selected_error_and_eof_buffers_are_returned_without_reading_a_header() {
    for result in [-libc::ECONNRESET, 0] {
        let (selector, mut driver) = pair(1);
        let (_sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
            &selector,
            SocketDomain::Ipv4,
        ));
        let mut receiving = receiver(&selector, &socket, 1, 8);
        let Command::Submit(request) = driver
            .reactor
            .as_ref()
            .unwrap()
            .incoming
            .try_recv()
            .unwrap()
        else {
            panic!("receive submission")
        };
        // Exercise valid terminal decoder outcomes without relying on whether this particular kernel selects a buffer for an error/EOF. This request is never submitted, and its payload intentionally contains no header.
        request.completion(result, 1); // IORING_CQE_F_BUFFER, selected buffer zero.
        request.finish(if result < 0 {
            Err(io::Error::from_raw_os_error(-result))
        } else {
            Ok(0)
        });
        if result < 0 {
            assert!(block_on(receiving.recv(&Cancellation::never())).is_err());
        } else {
            assert!(
                block_on(receiving.recv(&Cancellation::never()))
                    .unwrap()
                    .is_none()
            );
        }
        drain(&selector, &mut driver);
    }
}

#[test]
fn local_returns_publish_directly_and_only_exhaustion_queues_a_rearm() {
    use base_scheduler_selector::{SelectorDriver, SelectorWorker};
    for exhaust in [false, true] {
        let (selector, driver) = pair(1);
        let (sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
            &selector,
            SocketDomain::Ipv4,
        ));
        let mut worker = driver.enter();
        let mut receiving = receiver(&selector, &socket, if exhaust { 1 } else { 2 }, 4);
        sender
            .try_send(
                if exhaust { b"abcdefgh" } else { b"abcd" },
                SocketSendFlags::default(),
            )
            .unwrap();
        let first = driven(&mut worker, receiving.recv(&Cancellation::never()))
            .unwrap()
            .unwrap();
        if exhaust {
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while receiving.statistics().buffer_exhaustions == 0 {
                worker.turn(Some(Duration::from_millis(1))).unwrap();
                assert!(std::time::Instant::now() < deadline);
            }
        }
        drop(first);
        let command = crate::io_uring_worker::pop(&selector.controllers[0]);
        let held = if exhaust {
            assert!(matches!(command, Some(Command::Rearm(_))));
            selector.controllers[0].send(command.unwrap()).unwrap();
            let second = driven(&mut worker, receiving.recv(&Cancellation::never()))
                .unwrap()
                .unwrap();
            assert_eq!(second.payload(), b"efgh");
            Some(second)
        } else {
            assert!(command.is_none());
            // Reuse the entire pool repeatedly. Publishing directly must advance the provided-ring tail without ending the persistent submission.
            for _ in 0..16 {
                sender
                    .try_send(b"next", SocketSendFlags::default())
                    .unwrap();
                let message = driven(&mut worker, receiving.recv(&Cancellation::never()))
                    .unwrap()
                    .unwrap();
                assert_eq!(message.payload(), b"next");
                drop(message);
                assert!(crate::io_uring_worker::pop(&selector.controllers[0]).is_none());
            }
            assert_eq!(receiving.statistics().submissions, 1);
            None
        };
        driven(&mut worker, receiving.close()).unwrap();
        // A lease can return after retirement. Its late rearm notification must be harmless even though the original request has already been removed.
        drop(held);
        worker.turn(Some(Duration::ZERO)).unwrap();
        selector.close();
        while !worker.turn(Some(Duration::from_millis(1))).unwrap() {}
    }
}

#[test]
fn precancelled_receive_preserves_a_live_stream_and_admission_cancels_without_submission() {
    let (selector, mut driver) = pair(1);
    let (sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let mut receiving = receiver(&selector, &socket, 2, 8);
    let token = Cancellation::new();
    token.cancel();
    assert!(matches!(
        block_on(receiving.recv(&token)),
        Err(OperationError::Cancelled(_))
    ));
    let admission = Cancellation::new();
    let mut waiting = Box::pin(selector.socket_recvmsg_multishot(
        &socket,
        RecvMsgMultishotOptions::default(),
        SocketRecvFlags::default(),
        &admission,
    ));
    assert!(block_on(poll_once(&mut waiting)).is_none());
    admission.cancel();
    assert!(matches!(
        block_on(waiting),
        Err(OperationError::Cancelled(_))
    ));
    sender
        .try_send(b"live", SocketSendFlags::default())
        .unwrap();
    let message = drive(&mut driver, receiving.recv(&Cancellation::never()))
        .unwrap()
        .unwrap();
    assert_eq!(message.payload(), b"live");
    assert_eq!(receiving.statistics().submissions, 1);
    drive(&mut driver, receiving.close()).unwrap();
    drain(&selector, &mut driver);
}

#[test]
fn construction_failures_release_admission_and_close_races_notify_the_receiver() {
    use super::super::tests::{BEFORE_SYSCALL, Failures};
    for failure in ["allocate-buffer-ring", "socket-type", "identifier", "close"] {
        let (selector, mut driver) = pair(1);
        let (_sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
            &selector,
            SocketDomain::Ipv4,
        ));
        let _failures = Failures::new([(failure, io::Error::from_raw_os_error(libc::ENOMEM))]);
        if failure == "identifier" {
            selector.controllers[0]
                .next_identifier
                .store(CANCEL_TAG - 1, Ordering::Relaxed);
        }
        if failure == "close" {
            let controller = Arc::clone(&selector.controllers[0]);
            BEFORE_SYSCALL.with(|hook| {
                *hook.borrow_mut() = Some(("socket-type", Box::new(move || controller.close())))
            });
        }
        let result = block_on(selector.socket_recvmsg_multishot(
            &socket,
            RecvMsgMultishotOptions::default(),
            SocketRecvFlags::default(),
            &Cancellation::never(),
        ));
        if failure == "close" {
            let mut receiving = result.unwrap();
            assert!(
                matches!(block_on(receiving.recv(&Cancellation::never())), Err(OperationError::Io(error)) if error.kind() == io::ErrorKind::BrokenPipe)
            );
            assert!(block_on(receiving.close()).is_ok());
        } else {
            assert!(matches!(result, Err(OperationError::Io(_))));
        }
        assert_eq!(
            selector.controllers[0]
                .capacity
                .state
                .load(Ordering::Relaxed)
                & !CAPACITY_CLOSED,
            0
        );
        drop(_failures);
        drain(&selector, &mut driver);
    }
}

#[test]
fn cancellation_reports_retirement_failure() {
    let (selector, mut driver) = pair(1);
    let (_sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let mut receiving = receiver(&selector, &socket, 2, 8);
    driver.turn(Some(Duration::ZERO)).unwrap();
    let token = Cancellation::new();
    let mut next = Box::pin(receiving.recv(&token));
    assert!(block_on(poll_once(&mut next)).is_none());
    let _failures = super::super::tests::Failures::new([(
        "unregister-buffer-ring",
        io::Error::from_raw_os_error(libc::EIO),
    )]);
    token.cancel();
    assert!(
        matches!(drive(&mut driver, next), Err(OperationError::Io(error)) if error.raw_os_error() == Some(libc::EIO))
    );
    drop(_failures);
    drain(&selector, &mut driver);
}

#[test]
fn terminal_decoding_preserves_payload_before_rearm_and_accepts_eof_without_a_buffer() {
    let (selector, mut driver) = pair(1);
    let (_sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let controller = &selector.controllers[0];
    let pool = Pool::new(
        controller,
        RecvMsgMultishotOptions {
            buffers: 1,
            buffer_size: 8,
        },
        0,
    )
    .unwrap();
    let state = Arc::new(State::default());
    // SAFETY: A zeroed header with a name capacity is the recvmsg multishot ABI.
    let mut header: libc::msghdr = unsafe { std::mem::zeroed() };
    header.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as _;
    let request = Box::new(MultiRequest {
        identifier: 0,
        _permit: Some(block_on(controller.capacity.acquire(&Cancellation::never())).unwrap()),
        socket: Some(super::super::socket::retain_socket(&socket)),
        stream: true,
        pool: Arc::clone(&pool),
        state: Arc::clone(&state),
        header: UnsafeCell::new(header),
        cancelling: Cell::new(false),
        registered: Cell::new(false),
        eof: Cell::new(false),
        finished: false,
    });
    let overhead = 16 + std::mem::size_of::<libc::sockaddr_storage>();
    // The request is never published. Initialize a valid kernel completion layout to exercise the legitimate no-MORE outcome independently of kernel timing.
    // SAFETY: This private, unregistered pool has no kernel or lease references.
    unsafe {
        let pointer = pool.pointer(0);
        pointer.add(8).cast::<u32>().write_unaligned(4);
        std::ptr::copy_nonoverlapping(b"last".as_ptr(), pointer.add(overhead), 4);
    }
    request.completion((overhead + 4) as i32, 1); // BUFFER, without MORE.
    assert_eq!(request.retry((overhead + 4) as i32), Some(false));
    let message = state.received.lock().unwrap().messages.pop_front().unwrap();
    assert_eq!(message.payload(), b"last");
    drop(message);
    driver.turn(Some(Duration::ZERO)).unwrap();
    assert_eq!(request.retry((overhead + 4) as i32), Some(true));
    request.completion(0, 0); // EOF is also permitted without BUFFER.
    assert_eq!(request.retry(0), None);
    request.finish(Ok(0));
    drain(&selector, &mut driver);
}

#[test]
fn dropping_pending_dormant_and_prepared_requests_notifies_receivers() {
    for stage in ["pending", "dormant", "prepared"] {
        let (selector, mut driver) = pair(1);
        let (sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
            &selector,
            SocketDomain::Ipv4,
        ));
        let mut receiving = receiver(&selector, &socket, 1, 4);
        let held = if stage == "dormant" {
            sender
                .try_send(b"abcdefgh", SocketSendFlags::default())
                .unwrap();
            let message = drive(&mut driver, receiving.recv(&Cancellation::never()))
                .unwrap()
                .unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while driver.reactor.as_ref().unwrap().dormant.is_empty() {
                driver.turn(Some(Duration::from_millis(1))).unwrap();
                assert!(std::time::Instant::now() < deadline);
            }
            Some(message)
        } else {
            let reactor = driver.reactor.as_mut().unwrap();
            let Command::Submit(mut request) = reactor.incoming.try_recv().unwrap() else {
                panic!("receive submission")
            };
            if stage == "prepared" {
                // Registration can succeed before an SQE is published. Dropping that unfinished request must still notify its waiting consumer.
                request.prepare(&reactor.ring.submitter()).unwrap();
                drop(request);
            } else {
                reactor.pending.push_back(request);
            }
            None
        };
        drop(driver);
        assert!(matches!(
            block_on(receiving.recv(&Cancellation::never())),
            Err(OperationError::Io(_))
        ));
        if let Some(message) = held {
            assert_eq!(message.payload(), b"abcd");
        }
    }
}

#[test]
fn a_returned_buffer_before_terminal_collection_allows_immediate_resubmission() {
    let (selector, mut driver) = pair(1);
    let (sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let mut receiving = receiver(&selector, &socket, 1, 4);
    sender
        .try_send(b"abcd", SocketSendFlags::default())
        .unwrap();
    let first = drive(&mut driver, receiving.recv(&Cancellation::never()))
        .unwrap()
        .unwrap();
    assert_eq!(first.payload(), b"abcd");
    sender
        .try_send(b"efgh", SocketSendFlags::default())
        .unwrap();
    // Wait for the kernel's exhausted-pool completion without collecting it. Returning the lease now queues recycling ahead of completion collection.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while driver
        .reactor
        .as_mut()
        .unwrap()
        .ring
        .completion()
        .is_empty()
    {
        std::thread::yield_now();
        assert!(std::time::Instant::now() < deadline);
    }
    drop(first);
    let second = drive(&mut driver, receiving.recv(&Cancellation::never()))
        .unwrap()
        .unwrap();
    assert_eq!(second.payload(), b"efgh");
    drop(second);
    drive(&mut driver, receiving.close()).unwrap();
    drain(&selector, &mut driver);
}

#[test]
fn retained_leases_can_be_released_after_worker_thread_locals_are_destroyed() {
    thread_local! {
        static HELD: std::cell::RefCell<Option<(RecvMsgBuffer, Option<IoUringSelector>)>> = const { std::cell::RefCell::new(None) };
    }
    for keep_selector in [false, true] {
        std::thread::spawn(move || {
            // Initialize the lease holder first, so ROUTES is destroyed before the retained lease at thread exit.
            HELD.with(|held| assert!(held.borrow().is_none()));
            let (selector, driver) = pair(2);
            let (sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
                &selector,
                SocketDomain::Ipv4,
            ));
            let mut worker = driver.enter();
            let mut receiving = receiver(&selector, &socket, 2, 16);
            sender
                .try_send(b"retained", SocketSendFlags::default())
                .unwrap();
            let message = driven(&mut worker, receiving.recv(&Cancellation::never()))
                .unwrap()
                .unwrap();
            assert_eq!(message.payload(), b"retained");
            let payload = message.into_payload();
            let controller = payload.pool.controller.clone();
            driven(&mut worker, receiving.close()).unwrap();
            drop(receiving);
            selector.close();
            while !worker.turn(Some(Duration::from_millis(1))).unwrap() {}
            drop(worker);
            let retained_selector = if keep_selector {
                Some(selector)
            } else {
                drop(selector);
                None
            };
            assert_eq!(controller.strong_count() != 0, keep_selector);
            HELD.with(|held| *held.borrow_mut() = Some((payload, retained_selector)));
        })
        .join()
        .unwrap();
    }
}

#[test]
fn completion_between_queue_probe_and_listener_registration_is_not_lost() {
    let (selector, driver) = pair(2);
    let (sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let mut receiving = receiver(&selector, &socket, 2, 16);
    let driver = std::rc::Rc::new(std::cell::RefCell::new(driver));
    let publish = std::rc::Rc::clone(&driver);
    let state = Arc::clone(&receiving.state);
    sender
        .try_send(b"message", SocketSendFlags::default())
        .unwrap();
    BEFORE_RECEIVE_LISTEN.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            // No driver has processed this receive yet. Publish the real CQE after the first empty probe and before the listener exists.
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while state.received.lock().unwrap().messages.is_empty() {
                publish
                    .borrow_mut()
                    .turn(Some(Duration::from_millis(1)))
                    .unwrap();
                assert!(std::time::Instant::now() < deadline);
            }
        }));
    });
    let message = block_on(poll_once(receiving.recv(&Cancellation::never())))
        .expect("a CQE published before listener registration is ready on this poll")
        .unwrap()
        .unwrap();
    assert_eq!(message.payload(), b"message");
    assert!(BEFORE_RECEIVE_LISTEN.with(|hook| hook.borrow().is_none()));
    drop(message);
    drive(&mut driver.borrow_mut(), receiving.close()).unwrap();
    drain(&selector, &mut driver.borrow_mut());
}

#[test]
fn concurrent_completion_publication_preserves_receive_wakeups() {
    let (selector, driver) = pair(2);
    let driver = std::thread::spawn(move || driver.run().unwrap());
    let (sender, socket) = block_on(base_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let mut receiving = receiver(&selector, &socket, 8, 4);
    let never = Cancellation::never();
    for index in 0u32..10_000 {
        let bytes = index.to_ne_bytes();
        assert_eq!(
            sender.try_send(&bytes, SocketSendFlags::default()).unwrap(),
            bytes.len()
        );
        if index % 2 == 0 {
            std::thread::yield_now();
        }
        let message = block_on(receiving.recv(&never)).unwrap().unwrap();
        assert_eq!(message.payload(), bytes);
    }
    block_on(receiving.close()).unwrap();
    selector.close();
    driver.join().unwrap();
}

#[test]
fn a_completion_batch_preserves_every_queued_datagram() {
    let (selector, mut driver) = pair(2);
    let never = Cancellation::never();
    let socket =
        block_on(selector.socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never))
            .unwrap();
    block_on(selector.socket_bind(&socket, "127.0.0.1:0".parse().unwrap(), &never)).unwrap();
    let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut receiving = receiver(&selector, &socket, 4, 16);
    for payload in [b"first".as_slice(), b"second", b"third"] {
        peer.send_to(payload, socket.local_addr().unwrap()).unwrap();
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while receiving.statistics().messages != 3 {
        driver.turn(Some(Duration::from_millis(1))).unwrap();
        assert!(std::time::Instant::now() < deadline);
    }
    for payload in [b"first".as_slice(), b"second", b"third"] {
        let message = block_on(receiving.recv(&never)).unwrap().unwrap();
        assert_eq!(message.payload(), payload);
    }
    assert_eq!(receiving.statistics().submissions, 1);
    drive(&mut driver, receiving.close()).unwrap();
    drop(receiving);
    block_on(selector.socket_close(socket)).unwrap();
    drain(&selector, &mut driver);
}

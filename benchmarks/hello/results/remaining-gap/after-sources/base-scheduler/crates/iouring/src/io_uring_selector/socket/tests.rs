// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::super::tests::{Failures, drain, drive, pair};
use super::*;
use crate::Interest;
use futures_lite::future::{block_on, poll_once};

#[test]
fn transient_socket_failures_retry_and_preserve_the_owner() {
    let (selector, mut driver) = pair(1);
    let never = Cancellation::never();
    let socket =
        block_on(selector.socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never))
            .unwrap();
    block_on(selector.socket_bind(&socket, "127.0.0.1:0".parse().unwrap(), &never)).unwrap();
    let controller = selector.route();
    for kind in [io::ErrorKind::Interrupted, io::ErrorKind::WouldBlock] {
        let mut attempts = 0;
        let (result, buffer) = drive(
            &mut driver,
            retry_operation(
                &selector,
                &controller,
                &socket,
                vec![7],
                |buffer| {
                    attempts += 1;
                    let result = if attempts == 1 {
                        Err(kind.into())
                    } else {
                        Ok(1)
                    };
                    async move { (result, buffer) }
                },
                Interest::Writable,
                &never,
            ),
        );
        assert_eq!(result.unwrap(), 1);
        assert_eq!(attempts, 2);
        assert_eq!(buffer, [7]);
    }
    let token = Cancellation::new();
    let mut operation = Box::pin(retry_operation(
        &selector,
        &controller,
        &socket,
        vec![9],
        |buffer| async { (Err::<usize, _>(io::ErrorKind::WouldBlock.into()), buffer) },
        Interest::Readable,
        &token,
    ));
    assert!(block_on(poll_once(&mut operation)).is_none());
    token.cancel();
    let (result, buffer) = drive(&mut driver, operation);
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
    assert_eq!(buffer, [9]);
    drain(&selector, &mut driver);
}

#[test]
fn scalar_socket_requests_use_ring_opcodes_and_release_before_notification() {
    for write in [false, true] {
        let (selector, mut driver) = pair(2);
        let (sender, receiver) = block_on(base_scheduler_conformance::socket::stream_pair(
            &selector,
            SocketDomain::Ipv4,
        ));
        if write {
            retain_socket(&sender)
                .get_ref()
                .set_send_buffer_size(4096)
                .unwrap();
            retain_socket(&receiver)
                .get_ref()
                .set_recv_buffer_size(4096)
                .unwrap();
            let payload = [0x5a; 65536];
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            loop {
                match sender.try_send(&payload, SocketSendFlags::default()) {
                    Ok(_) => assert!(std::time::Instant::now() < deadline),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) => panic!("filling socket failed: {error}"),
                }
            }
        }
        let token = Cancellation::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        // TCP acknowledgements may briefly free capacity after filling. Only inspect a Send request once the owned write really suspends.
        let mut operation = loop {
            let mut operation: std::pin::Pin<Box<dyn Future<Output = BufferResult<Vec<u8>>>>> =
                if write {
                    Box::pin(selector.socket_write(&sender, vec![7], &token))
                } else {
                    Box::pin(selector.socket_read(&receiver, vec![7], &token))
                };
            match block_on(poll_once(&mut operation)) {
                None => break operation,
                Some((result, _)) => {
                    assert!(write);
                    assert!(result.unwrap() > 0);
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "peer did not apply backpressure"
            );
        };
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
            if write {
                opcode::Send::CODE
            } else {
                opcode::Recv::CODE
            }
        );
        token.cancel();
        assert!(block_on(poll_once(&mut operation)).is_none());
        assert!(matches!(
            driver
                .reactor
                .as_ref()
                .unwrap()
                .incoming
                .try_recv()
                .unwrap(),
            Command::Cancel(_)
        ));
        request.finish(Err(io::Error::from_raw_os_error(libc::ECANCELED)));
        let (result, buffer) = block_on(operation);
        assert!(matches!(result, Err(OperationError::Cancelled(_))));
        assert_eq!(buffer, [7]);
        block_on(selector.socket_close(sender)).unwrap();
        block_on(selector.socket_close(receiver)).unwrap();
        drain(&selector, &mut driver);
    }
}

#[test]
fn socket_retries_keep_the_original_ring_across_threads_and_nested_workers() {
    use base_scheduler_selector::{SelectorDriver, SelectorWorker};
    let (selector, mut drivers) =
        IoUringSelector::with_workers(IoUringOptions::default(), 2).unwrap();
    let mut other_worker = drivers.pop().unwrap().enter();
    let mut original_driver = drivers.pop().unwrap();
    let controller = Arc::clone(&selector.controllers[0]);
    let never = Cancellation::never();
    let socket =
        block_on(selector.socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never))
            .unwrap();
    block_on(selector.socket_bind(&socket, "127.0.0.1:0".parse().unwrap(), &never)).unwrap();
    let mut attempts = 0;
    let mut operation = Box::pin(retry_operation(
        &selector,
        &controller,
        &socket,
        vec![7],
        |buffer| {
            attempts += 1;
            let result = if attempts == 1 {
                Err(io::ErrorKind::WouldBlock.into())
            } else {
                Ok(1)
            };
            async move { (result, buffer) }
        },
        Interest::Writable,
        &never,
    ));
    assert!(block_on(poll_once(&mut operation)).is_none());
    assert_eq!(controller.capacity.state.load(Ordering::Acquire), 1);
    assert_eq!(
        selector.controllers[1]
            .capacity
            .state
            .load(Ordering::Acquire),
        0
    );
    let (result, buffer) = std::thread::scope(|scope| {
        let task = scope.spawn(move || block_on(operation));
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while controller.capacity.state.load(Ordering::Acquire) != 0 {
            assert!(
                !original_driver
                    .turn(Some(Duration::from_millis(1)))
                    .unwrap()
            );
            assert!(std::time::Instant::now() < deadline);
        }
        task.join().unwrap()
    });
    assert_eq!(result.unwrap(), 1);
    assert_eq!(buffer, [7]);
    assert_eq!(attempts, 2);
    selector.close();
    while !other_worker.turn(Some(Duration::from_millis(10))).unwrap() {}
    drain(&selector, &mut original_driver);
}

#[test]
fn socket_retry_readiness_cancels_and_closes_after_kernel_submission() {
    for close in [false, true] {
        let (selector, mut driver) = pair(1);
        let never = Cancellation::never();
        let socket =
            block_on(selector.socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never))
                .unwrap();
        block_on(selector.socket_bind(&socket, "127.0.0.1:0".parse().unwrap(), &never)).unwrap();
        let controller = selector.route();
        let token = Cancellation::new();
        let mut attempts = 0;
        let mut operation = Box::pin(retry_operation(
            &selector,
            &controller,
            &socket,
            vec![9],
            |buffer| {
                attempts += 1;
                async { (Err::<usize, _>(io::ErrorKind::WouldBlock.into()), buffer) }
            },
            Interest::Readable,
            &token,
        ));
        assert!(block_on(poll_once(&mut operation)).is_none());
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while driver.reactor.as_ref().unwrap().in_flight.0.is_empty() {
            assert!(!driver.turn(Some(Duration::ZERO)).unwrap());
            assert!(std::time::Instant::now() < deadline);
        }
        if close {
            selector.close();
        } else {
            token.cancel();
        }
        let (result, buffer) = drive(&mut driver, operation);
        let error = result.unwrap_err();
        if close {
            assert_eq!(error.raw_os_error(), Some(libc::ECANCELED));
        } else {
            assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        }
        assert_eq!(buffer, [9]);
        assert_eq!(attempts, 1);
        assert_eq!(
            controller.capacity.state.load(Ordering::Acquire) & !CAPACITY_CLOSED,
            0
        );
        drain(&selector, &mut driver);
    }
}

#[test]
fn ready_owned_writes_preserve_the_owner_without_consuming_ring_capacity() {
    let (selector, mut driver) = pair(1);
    let (sender, receiver) = block_on(base_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let never = Cancellation::never();
    let mut waiting = Box::pin(selector.socket_wait(&receiver, Interest::Readable, &never));
    assert!(block_on(poll_once(&mut waiting)).is_none());
    let identifier = selector.controllers[0]
        .next_identifier
        .load(Ordering::Relaxed);
    let mut source = Vec::with_capacity(64);
    source.extend_from_slice(b"ready");
    let pointer = source.as_ptr();
    let capacity = source.capacity();
    let (result, returned) = block_on(selector.socket_write(&sender, source, &never));
    assert_eq!(result.unwrap(), 5);
    assert_eq!(returned.as_ptr(), pointer);
    assert_eq!(returned.capacity(), capacity);
    assert_eq!(returned, b"ready");
    assert_eq!(
        selector.controllers[0]
            .next_identifier
            .load(Ordering::Relaxed),
        identifier
    );
    let mut received = [0; 5];
    assert_eq!(
        receiver
            .try_recv(&mut received, SocketRecvFlags::default())
            .unwrap(),
        5
    );
    assert_eq!(&received, b"ready");
    drop(waiting);
    drain(&selector, &mut driver);
}

#[test]
fn owned_write_fast_path_errors_and_ring_fallbacks_preserve_the_owner() {
    let (selector, mut driver) = pair(1);
    let (sender, receiver) = block_on(base_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let never = Cancellation::never();
    for kind in [
        io::ErrorKind::WouldBlock,
        io::ErrorKind::Interrupted,
        io::ErrorKind::PermissionDenied,
    ] {
        let mut source = Vec::with_capacity(32);
        source.extend_from_slice(b"test");
        let pointer = source.as_ptr();
        let capacity = source.capacity();
        let mut operation = Box::pin(selector.socket_write(&sender, source, &never));
        let initial = {
            let _failure = Failures::new([("socket-write", kind.into())]);
            block_on(poll_once(&mut operation))
        };
        let (result, returned) = if kind == io::ErrorKind::PermissionDenied {
            let output = initial.expect("fatal send failure must complete synchronously");
            assert_eq!(output.0.as_ref().unwrap_err().kind(), kind);
            output
        } else {
            assert!(initial.is_none());
            let output = drive(&mut driver, operation);
            assert_eq!(output.0.as_ref().unwrap(), &4);
            let mut received = [0; 4];
            assert_eq!(
                receiver
                    .try_recv(&mut received, SocketRecvFlags::default())
                    .unwrap(),
                4
            );
            assert_eq!(&received, b"test");
            output
        };
        assert_eq!(returned.as_ptr(), pointer);
        assert_eq!(returned.capacity(), capacity);
        assert_eq!(returned, b"test");
        assert_eq!(result.is_err(), kind == io::ErrorKind::PermissionDenied);
    }
    drain(&selector, &mut driver);
}

#[test]
fn owned_write_checks_precancellation_and_closure_before_sending() {
    let (selector, mut driver) = pair(1);
    let (sender, receiver) = block_on(base_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let token = Cancellation::new();
    token.cancel();
    let source = Vec::from(b"cancelled");
    let pointer = source.as_ptr();
    let (result, source) = block_on(selector.socket_write(&sender, source, &token));
    assert!(matches!(result, Err(OperationError::Cancelled(_))));
    assert_eq!(source.as_ptr(), pointer);
    selector.close();
    let (result, source) = block_on(selector.socket_write(&sender, source, &Cancellation::never()));
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(source.as_ptr(), pointer);
    assert_eq!(source, b"cancelled");
    let mut bytes = [0; 16];
    assert_eq!(
        receiver
            .try_recv(&mut bytes, SocketRecvFlags::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    drain(&selector, &mut driver);
}

#[test]
fn empty_owned_write_sends_one_udp_datagram_and_returns_the_owner() {
    let (selector, mut driver) = pair(1);
    let never = Cancellation::never();
    let sender =
        block_on(selector.socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never))
            .unwrap();
    let receiver =
        block_on(selector.socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never))
            .unwrap();
    for socket in [&sender, &receiver] {
        block_on(selector.socket_bind(socket, "127.0.0.1:0".parse().unwrap(), &never)).unwrap();
    }
    block_on(selector.socket_connect(&sender, receiver.local_addr().unwrap(), &never)).unwrap();
    let source = Vec::with_capacity(32);
    let pointer = source.as_ptr();
    let (result, returned) = block_on(selector.socket_write(&sender, source, &never));
    assert_eq!(result.unwrap(), 0);
    assert_eq!(returned.as_ptr(), pointer);
    assert_eq!(returned.capacity(), 32);
    let mut bytes = [0; 1];
    assert_eq!(
        receiver
            .try_recv(&mut bytes, SocketRecvFlags::default())
            .unwrap(),
        0
    );
    assert_eq!(
        receiver
            .try_recv(&mut bytes, SocketRecvFlags::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        selector.controllers[0]
            .next_identifier
            .load(Ordering::Relaxed),
        0
    );
    drain(&selector, &mut driver);
}

#[test]
fn abandoned_retry_wait_retains_the_owned_lane_until_poll_retirement() {
    let (selector, mut driver) = pair(1);
    let never = Cancellation::never();
    let socket =
        block_on(selector.socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never))
            .unwrap();
    block_on(selector.socket_bind(&socket, "127.0.0.1:0".parse().unwrap(), &never)).unwrap();
    let controller = selector.route();
    let mut state = crate::operation_state::OperationState::default();
    let owner = state.begin(vec![9]).unwrap();
    let mut operation = Box::pin(retry_operation(
        &selector,
        &controller,
        &socket,
        owner,
        |owner| async { (Err::<usize, _>(io::ErrorKind::WouldBlock.into()), owner) },
        Interest::Readable,
        &never,
    ));
    assert!(block_on(poll_once(operation.as_mut())).is_none());
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while driver.reactor.as_ref().unwrap().in_flight.0.is_empty() {
        driver.turn(Some(Duration::ZERO)).unwrap();
        assert!(std::time::Instant::now() < deadline);
    }
    drop(operation);
    let mut close = Box::pin(state.close());
    assert!(
        block_on(poll_once(close.as_mut())).is_none(),
        "raw readiness retains its socket until the original poll retires"
    );
    drive(&mut driver, close);
    block_on(selector.socket_close(socket)).unwrap();
    drain(&selector, &mut driver);
}

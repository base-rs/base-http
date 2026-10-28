// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::super::tests::{drain, pair};
use super::*;
use futures_lite::future::{block_on, poll_once};

#[test]
fn rejected_readiness_admission_returns_the_unchanged_owner() {
    for (payload, mode) in [false, true]
        .into_iter()
        .flat_map(|payload| (0..5).map(move |mode| (payload, mode)))
    {
        let (selector, mut driver) = pair(1);
        let (sender, receiver) = block_on(base_scheduler_conformance::socket::stream_pair(
            &selector,
            SocketDomain::Ipv4,
        ));
        let controller = Arc::clone(&selector.controllers[0]);
        let token = Cancellation::new();
        let held = matches!(mode, 2 | 3)
            .then(|| block_on(controller.capacity.acquire(&Cancellation::never())).unwrap());
        if mode == 0 {
            token.cancel();
        } else if mode == 1 {
            selector.close();
        } else if mode == 4 {
            controller
                .next_identifier
                .store(CANCEL_TAG - 2, Ordering::Relaxed);
        }
        let input = vec![7, 8, 9];
        let pointer = input.as_ptr();
        let mut waiting = Box::pin(async {
            if payload {
                let (result, input) = selector
                    .operation(
                        Arc::clone(&controller),
                        super::super::socket::retain_socket(&receiver),
                        input,
                        false,
                        |buffer| buffer.as_mut_ptr(),
                        &token,
                    )
                    .await;
                (result.map(drop).map_err(OperationError::from), input)
            } else {
                selector
                    .wait_ready_owned(
                        Arc::clone(&controller),
                        &receiver,
                        input,
                        Interest::Readable,
                        &token,
                    )
                    .await
            }
        });
        if held.is_some() {
            assert!(block_on(poll_once(waiting.as_mut())).is_none());
            if mode == 2 {
                token.cancel();
            } else {
                controller.close();
            }
        }
        let (result, input) = block_on(waiting);
        let error = result.unwrap_err();
        let expected = match mode {
            0 | 2 => io::ErrorKind::Interrupted,
            1 | 3 => io::ErrorKind::BrokenPipe,
            _ => io::ErrorKind::Other,
        };
        assert_eq!(error.kind(), expected);
        assert_eq!(input, [7, 8, 9]);
        assert_eq!(input.as_ptr(), pointer);
        drop(held);
        drop((sender, receiver));
        drain(&selector, &mut driver);
    }
}

#[test]
fn taking_a_readiness_reply_preserves_the_owned_input_until_request_disposal() {
    struct Input(Arc<AtomicBool>);
    impl Drop for Input {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let (selector, mut driver) = pair(1);
    let (sender, receiver) = block_on(base_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let dropped = Arc::new(AtomicBool::new(false));
    let token = Cancellation::never();
    let mut waiting = Box::pin(selector.wait_ready_owned(
        Arc::clone(&selector.controllers[0]),
        &receiver,
        Input(Arc::clone(&dropped)),
        Interest::Readable,
        &token,
    ));
    assert!(block_on(poll_once(waiting.as_mut())).is_none());
    let Command::Submit(request) = driver
        .reactor
        .as_ref()
        .unwrap()
        .incoming
        .try_recv()
        .unwrap()
    else {
        panic!("expected one readiness submission");
    };
    // Driver failure detaches notification from retained kernel ownership. Taking that notification must not dispose of the payload.
    let reply = request.take_reply().unwrap();
    assert!(request.take_reply().is_none());
    drop(reply);
    assert!(!dropped.load(Ordering::Acquire));
    // This request has not reached the kernel, so disposal is safe and releases the retained input.
    drop(request);
    assert!(dropped.load(Ordering::Acquire));
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(waiting))).is_err());
    block_on(selector.socket_close(receiver)).unwrap();
    block_on(selector.socket_close(sender)).unwrap();
    drain(&selector, &mut driver);
}

#[test]
fn unpublished_payload_requests_return_the_original_owner() {
    let (selector, mut driver) = pair(1);
    let (sender, receiver) = block_on(base_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let token = Cancellation::never();
    let input = vec![4, 5, 6];
    let pointer = input.as_ptr();
    let mut waiting = Box::pin(selector.socket_read(&receiver, input, &token));
    assert!(block_on(poll_once(waiting.as_mut())).is_none());
    let Command::Submit(request) = driver
        .reactor
        .as_ref()
        .unwrap()
        .incoming
        .try_recv()
        .unwrap()
    else {
        panic!("expected one payload submission");
    };
    drop(request);
    let (result, input) = block_on(waiting);
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(input, [4, 5, 6]);
    assert_eq!(input.as_ptr(), pointer);
    block_on(selector.socket_close(receiver)).unwrap();
    block_on(selector.socket_close(sender)).unwrap();
    drain(&selector, &mut driver);
}

#[test]
fn detaching_payload_failure_notification_preserves_ownership() {
    struct Input {
        bytes: Vec<u8>,
        dropped: Arc<AtomicBool>,
    }
    impl Drop for Input {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }
    impl AsRef<[u8]> for Input {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl AsMut<[u8]> for Input {
        fn as_mut(&mut self) -> &mut [u8] {
            &mut self.bytes
        }
    }
    // SAFETY: The vector exclusively owns its stable initialized allocation through request retirement.
    unsafe impl IoBuf for Input {}
    unsafe impl IoBufMut for Input {}
    let (selector, mut driver) = pair(1);
    let (sender, receiver) = block_on(base_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let dropped = Arc::new(AtomicBool::new(false));
    let token = Cancellation::never();
    let mut waiting = Box::pin(selector.socket_read(
        &receiver,
        Input {
            bytes: vec![0],
            dropped: Arc::clone(&dropped),
        },
        &token,
    ));
    assert!(block_on(poll_once(waiting.as_mut())).is_none());
    let Command::Submit(request) = driver
        .reactor
        .as_ref()
        .unwrap()
        .incoming
        .try_recv()
        .unwrap()
    else {
        panic!("expected one payload submission");
    };
    let reply = request.take_reply().unwrap();
    assert!(request.take_reply().is_none());
    drop(reply);
    assert!(!dropped.load(Ordering::Acquire));
    // Nothing was submitted to the kernel, so disposal safely releases the retained payload.
    drop(request);
    assert!(dropped.load(Ordering::Acquire));
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(waiting))).is_err());
    block_on(selector.socket_close(receiver)).unwrap();
    block_on(selector.socket_close(sender)).unwrap();
    drain(&selector, &mut driver);
}

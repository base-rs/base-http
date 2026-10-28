// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::super::tests::{drive, pair};
use super::*;
use futures_lite::future::{block_on, poll_once};
use std::time::Instant;

thread_local! {
    static SEND_ZERO: Cell<bool> = const { Cell::new(false) };
}

pub(super) fn send(operation: impl FnOnce() -> io::Result<usize>) -> io::Result<usize> {
    if SEND_ZERO.with(|value| value.replace(false)) {
        Ok(0)
    } else {
        operation()
    }
}

#[derive(Debug)]
struct Buffer {
    bytes: Box<[u8]>,
    drops: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
// SAFETY: The box owns stable initialized bytes until the complete owner is dropped.
unsafe impl IoBuf for Buffer {}
impl Drop for Buffer {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Release);
    }
}

fn buffer(length: usize, byte: u8, drops: &Arc<AtomicUsize>) -> Buffer {
    Buffer {
        bytes: vec![byte; length].into_boxed_slice(),
        drops: Arc::clone(drops),
    }
}

fn stream(selector: &IoUringSelector) -> (NativeSocket, NativeSocket) {
    block_on(base_scheduler_conformance::socket::stream_pair(
        selector,
        SocketDomain::Ipv4,
    ))
}

fn small_send_buffer(socket: &NativeSocket) {
    socket::retain_socket(socket)
        .get_ref()
        .set_send_buffer_size(4096)
        .unwrap();
}

fn drive_all<F: Future>(drivers: &mut [crate::IoUringDriver], future: F) -> F::Output {
    let mut future = pin!(future);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(result) = block_on(poll_once(future.as_mut())) {
            return result;
        }
        assert!(
            Instant::now() < deadline,
            "multi-ring writer test timed out"
        );
        for driver in &mut *drivers {
            driver.turn(Some(Duration::ZERO)).unwrap();
        }
    }
}

#[test]
fn immediate_writes_release_buffers_and_reuse_writer_without_allocating() {
    let (selector, mut driver) = pair(16);
    let (sender, receiver) = stream(&selector);
    let mut writer = selector.socket_stream_output::<Buffer>(&sender).unwrap();
    let never = Cancellation::never();
    let drops = Arc::new(AtomicUsize::new(0));
    for iteration in 0..8 {
        let bytes = buffer(16, iteration, &drops);
        let counts = allocation_counter::measure(|| {
            block_on(writer.submit(&selector, bytes, &never)).unwrap();
            block_on(writer.flush(&selector, &never)).unwrap();
        });
        assert_eq!(counts.count_total, 0);
        assert_eq!(drops.load(Ordering::Acquire), iteration as usize + 1);
        let (result, received) = drive(
            &mut driver,
            selector.socket_read(&receiver, vec![0; 16], &never),
        );
        assert_eq!(result.unwrap(), 16);
        assert_eq!(received, vec![iteration; 16]);
    }
    block_on(writer.submit(&selector, buffer(0, 0, &drops), &never)).unwrap();
    assert_eq!(drops.load(Ordering::Acquire), 9);
    block_on(writer.close(&selector)).unwrap();
    block_on(writer.close(&selector)).unwrap();
    assert!(block_on(writer.flush(&selector, &never)).is_err());
    drop(writer);
    block_on(selector.socket_close(sender)).unwrap();
    block_on(selector.socket_close(receiver)).unwrap();
}

#[test]
fn stream_type_scheduler_and_pre_cancellation_checks_preserve_new_buffers() {
    let (selector, _driver) = pair(8);
    let (other, _other_driver) = pair(8);
    let never = Cancellation::never();
    let datagram =
        block_on(selector.socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never))
            .unwrap();
    assert!(
        matches!(selector.socket_stream_output::<Vec<u8>>(&datagram), Err(error) if error.kind() == io::ErrorKind::InvalidInput)
    );
    let (sender, receiver) = stream(&selector);
    let mut writer = selector.socket_stream_output::<Vec<u8>>(&sender).unwrap();
    let token = Cancellation::new();
    token.cancel();
    let (error, bytes) = block_on(writer.submit(&selector, vec![1, 2], &token)).unwrap_err();
    assert!(matches!(error, OperationError::Cancelled(_)));
    assert_eq!(bytes, [1, 2]);
    let (error, bytes) = block_on(writer.submit(&other, vec![3], &never)).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(bytes, [3]);
    assert_eq!(
        block_on(writer.flush(&other, &never)).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        block_on(writer.close(&other)).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(!writer.closed);
    block_on(writer.close(&selector)).unwrap();
    let (error, bytes) = block_on(writer.submit(&selector, vec![4], &never)).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(bytes, [4]);
    drop(writer);
    block_on(selector.socket_close(sender)).unwrap();
    block_on(selector.socket_close(receiver)).unwrap();
    block_on(selector.socket_close(datagram)).unwrap();
}

#[test]
fn stalled_write_finishes_and_releases_its_owner_without_polling_a_writer_future() {
    let (selector, mut driver) = pair(16);
    let (sender, receiver) = stream(&selector);
    small_send_buffer(&sender);
    let mut writer = selector.socket_stream_output::<Buffer>(&sender).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let never = Cancellation::never();
    let length = 1024 * 1024;
    drive(
        &mut driver,
        writer.submit(&selector, buffer(length, 0x5a, &drops), &never),
    )
    .unwrap();
    assert!(
        writer.running.is_some(),
        "small send capacity must leave a pending write"
    );
    assert_eq!(drops.load(Ordering::Acquire), 0);
    let mut received = 0;
    let mut bytes = vec![0; 65536];
    let deadline = Instant::now() + Duration::from_secs(10);
    while received < length || drops.load(Ordering::Acquire) == 0 {
        assert!(Instant::now() < deadline);
        driver.turn(Some(Duration::ZERO)).unwrap();
        match receiver.try_recv(&mut bytes, SocketRecvFlags::default()) {
            Ok(count) => {
                assert!(count > 0);
                assert!(bytes[..count].iter().all(|byte| *byte == 0x5a));
                received += count;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("receive failed: {error}"),
        }
    }
    assert_eq!(received, length);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    block_on(writer.flush(&selector, &never)).unwrap();
    assert!(writer.cached.is_some());
    drive(
        &mut driver,
        writer.submit(&selector, buffer(length, 0x6b, &drops), &never),
    )
    .unwrap();
    drive(&mut driver, writer.close(&selector)).unwrap();
    assert_eq!(drops.load(Ordering::Acquire), 2);
    drop(writer);
    block_on(selector.socket_close(sender)).unwrap();
    block_on(selector.socket_close(receiver)).unwrap();
}

#[test]
fn cancelled_flush_does_not_cancel_accepted_output_but_submission_token_does() {
    for before_preparation in [true, false] {
        let (selector, mut driver) = pair(16);
        let (sender, receiver) = stream(&selector);
        small_send_buffer(&sender);
        let mut writer = selector.socket_stream_output::<Buffer>(&sender).unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let token = Cancellation::new();
        if before_preparation {
            let mut submission =
                Box::pin(writer.submit(&selector, buffer(4 * 1024 * 1024, 7, &drops), &token));
            assert!(block_on(poll_once(submission.as_mut())).is_none());
            drop(submission);
        } else {
            drive(
                &mut driver,
                writer.submit(&selector, buffer(4 * 1024 * 1024, 7, &drops), &token),
            )
            .unwrap();
        }
        assert!(writer.running.is_some());
        let wait = Cancellation::new();
        wait.cancel();
        assert!(matches!(
            block_on(writer.flush(&selector, &wait)),
            Err(OperationError::Cancelled(_))
        ));
        assert!(!token.is_cancelled());
        if !before_preparation {
            for _ in 0..16 {
                driver.turn(Some(Duration::ZERO)).unwrap();
            }
        }
        token.cancel();
        let deadline = Instant::now() + Duration::from_secs(5);
        while drops.load(Ordering::Acquire) == 0 {
            assert!(
                Instant::now() < deadline,
                "accepted cancellation did not progress"
            );
            driver.turn(Some(Duration::ZERO)).unwrap();
        }
        assert!(matches!(
            block_on(writer.flush(&selector, &Cancellation::never())),
            Err(OperationError::Cancelled(_))
        ));
        let (error, rejected) =
            block_on(writer.submit(&selector, buffer(1, 8, &drops), &Cancellation::never()))
                .unwrap_err();
        assert!(matches!(error, OperationError::Cancelled(_)));
        assert_eq!(rejected.as_ref(), [8]);
        drop(rejected);
        block_on(writer.close(&selector)).unwrap();
        drop(writer);
        block_on(selector.socket_close(sender)).unwrap();
        block_on(selector.socket_close(receiver)).unwrap();
    }
}

#[test]
fn next_submission_waits_and_cancelled_admission_does_not_touch_the_previous_write() {
    let (selector, mut driver) = pair(16);
    let (sender, receiver) = stream(&selector);
    small_send_buffer(&sender);
    let mut writer = selector.socket_stream_output::<Vec<u8>>(&sender).unwrap();
    let never = Cancellation::never();
    drive(
        &mut driver,
        writer.submit(&selector, vec![1; 4 * 1024 * 1024], &never),
    )
    .unwrap();
    let token = Cancellation::new();
    let mut next = Box::pin(writer.submit(&selector, vec![2; 16], &token));
    assert!(block_on(poll_once(&mut next)).is_none());
    token.cancel();
    let (error, bytes) = block_on(next).unwrap_err();
    assert!(matches!(error, OperationError::Cancelled(_)));
    assert_eq!(bytes, vec![2; 16]);
    assert!(writer.running.is_some());
    drive(&mut driver, writer.close(&selector)).unwrap();
    drop(writer);
    block_on(selector.socket_close(sender)).unwrap();
    block_on(selector.socket_close(receiver)).unwrap();
}

#[test]
fn direct_send_errors_are_accepted_then_reported_and_drop_retires_pending_output() {
    use super::super::tests::Failures;
    let (selector, mut driver) = pair(16);
    let (sender, receiver) = stream(&selector);
    let mut writer = selector.socket_stream_output::<Vec<u8>>(&sender).unwrap();
    let _failure = Failures::new([("stream-write", io::ErrorKind::BrokenPipe.into())]);
    block_on(writer.submit(&selector, vec![1], &Cancellation::never())).unwrap();
    for _ in 0..2 {
        assert_eq!(
            block_on(writer.flush(&selector, &Cancellation::never()))
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
    }
    block_on(writer.close(&selector)).unwrap();
    drop(writer);
    small_send_buffer(&sender);
    let drops = Arc::new(AtomicUsize::new(0));
    let mut writer = selector.socket_stream_output::<Buffer>(&sender).unwrap();
    drive(
        &mut driver,
        writer.submit(
            &selector,
            buffer(4 * 1024 * 1024, 2, &drops),
            &Cancellation::never(),
        ),
    )
    .unwrap();
    drop(writer);
    let deadline = Instant::now() + Duration::from_secs(5);
    while drops.load(Ordering::Acquire) == 0 {
        assert!(Instant::now() < deadline);
        driver.turn(Some(Duration::ZERO)).unwrap();
    }
    block_on(selector.socket_close(sender)).unwrap();
    block_on(selector.socket_close(receiver)).unwrap();
}

#[test]
fn immediate_zero_progress_and_identifier_exhaustion_become_terminal_write_errors() {
    use super::super::tests::Failures;
    let (selector, _driver) = pair(16);
    let (sender, receiver) = stream(&selector);
    let never = Cancellation::never();
    let mut writer = selector.socket_stream_output::<Vec<u8>>(&sender).unwrap();
    SEND_ZERO.with(|value| value.set(true));
    block_on(writer.submit(&selector, vec![1], &never)).unwrap();
    assert_eq!(
        block_on(writer.flush(&selector, &never))
            .unwrap_err()
            .kind(),
        io::ErrorKind::WriteZero
    );
    block_on(writer.close(&selector)).unwrap();
    drop(writer);
    let mut writer = selector.socket_stream_output::<Vec<u8>>(&sender).unwrap();
    selector.controllers[0]
        .next_identifier
        .store(CANCEL_TAG - 1, Ordering::Relaxed);
    let _failure = Failures::new([("stream-write", io::ErrorKind::WouldBlock.into())]);
    block_on(writer.submit(&selector, vec![2], &never)).unwrap();
    assert_eq!(
        block_on(writer.flush(&selector, &never))
            .unwrap_err()
            .kind(),
        io::ErrorKind::Other
    );
    block_on(writer.close(&selector)).unwrap();
    drop(writer);
    block_on(selector.socket_close(sender)).unwrap();
    block_on(selector.socket_close(receiver)).unwrap();
}

#[test]
fn socket_query_failure_and_closed_selector_reject_before_output() {
    use super::super::tests::Failures;
    let (selector, _driver) = pair(16);
    let (sender, receiver) = stream(&selector);
    {
        let _failure = Failures::new([("stream-socket-type", io::ErrorKind::Other.into())]);
        assert!(
            matches!(selector.socket_stream_output::<Vec<u8>>(&sender), Err(error) if error.kind() == io::ErrorKind::Other)
        );
    }
    let mut writer = selector.socket_stream_output::<Vec<u8>>(&sender).unwrap();
    selector.group.close();
    assert!(
        matches!(selector.socket_stream_output::<Vec<u8>>(&sender), Err(error) if error.kind() == io::ErrorKind::BrokenPipe)
    );
    let (error, bytes) =
        block_on(writer.submit(&selector, vec![1], &Cancellation::never())).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(bytes, [1]);
    block_on(writer.close(&selector)).unwrap();
    drop(writer);
    drop(sender);
    drop(receiver);
}

type RequestWithReply = (Box<WriteRequest<Vec<u8>>>, Arc<Shared<Vec<u8>>>);

// Model documented kernel completion outcomes before handing the request back to the real retirement machinery. No synthetic pointer is published to the kernel.
fn make_request(
    selector: &IoUringSelector,
    sender: &NativeSocket,
    token: Cancellation,
) -> RequestWithReply {
    let shared = Arc::new(Shared {
        result: Mutex::new(None),
        ready: AtomicBool::new(false),
        submitted: AtomicBool::new(false),
        waker: atomic_waker::AtomicWaker::new(),
    });
    let mut request = Box::new(WriteRequest::empty());
    request.identifier = selector.controllers[0].next_identifier().unwrap();
    request.socket = Some(socket::retain_socket(sender));
    request.buffer = Some(vec![1; 4]);
    request.length = 4;
    request.watch_cancellation(&selector.controllers[0], &token);
    request.reply.set(Some(Arc::clone(&shared)));
    (request, shared)
}

#[test]
fn delayed_cancellation_wakes_cannot_cancel_a_reused_request() {
    for retain_old_waker in [false, true] {
        let (selector, mut driver) = pair(16);
        let (sender, receiver) = stream(&selector);
        let token = Cancellation::new();
        let (request, completed) = make_request(&selector, &sender, token);
        let old_waker = retain_old_waker
            .then(|| Waker::from(Arc::clone(request.cancel_waker.as_ref().unwrap())));
        request.offset.set(request.length);
        request.finish(Ok(4));
        let Some(Completion::Retired(Ok(()), Some(mut request))) =
            completed.result.lock().unwrap().take()
        else {
            panic!("expected a reusable completed request")
        };
        let shared = Arc::clone(&completed);
        shared.ready.store(false, Ordering::Relaxed);
        request.identifier = selector.controllers[0].next_identifier().unwrap();
        request.socket = Some(socket::retain_socket(&sender));
        request.buffer = Some(vec![2; 4]);
        request.offset.set(0);
        request.reply.set(Some(Arc::clone(&shared)));
        request.announced = false;
        request.watch_cancellation(&selector.controllers[0], &Cancellation::new());
        selector.controllers[0].submit(request);
        // This notification belongs to the retired write even though its request storage has been reused.
        if let Some(old_waker) = old_waker {
            old_waker.wake_by_ref();
        }
        drive(&mut driver, async {
            poll_fn(|context| {
                shared.waker.register(context.waker());
                if shared.ready.load(Ordering::Acquire) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
        });
        assert!(matches!(
            shared.result.lock().unwrap().take(),
            Some(Completion::Retired(Ok(()), Some(_)))
        ));
        let mut bytes = [0; 4];
        let count = drive(
            &mut driver,
            selector.socket_recv_poll(
                &receiver,
                &mut bytes,
                SocketRecvFlags::default(),
                &Cancellation::never(),
            ),
        )
        .unwrap();
        assert_eq!(count, 4);
        assert_eq!(bytes, [2; 4]);
        block_on(selector.socket_close(sender)).unwrap();
        block_on(selector.socket_close(receiver)).unwrap();
    }
}

#[test]
fn completion_outcomes_preserve_short_write_order_and_retire_errors() {
    let (selector, driver) = pair(16);
    let (sender, receiver) = stream(&selector);
    for (result, expected) in [
        (0, io::ErrorKind::WriteZero),
        (-libc::EPIPE, io::ErrorKind::BrokenPipe),
        (1, io::ErrorKind::Interrupted),
    ] {
        let (request, shared) = make_request(&selector, &sender, Cancellation::never());
        request.completion(result, 0);
        if result == 1 {
            assert!(request.begin_cancellation());
        }
        assert!(request.retry(result).is_none());
        request.retire_completion(&driver.reactor.as_ref().unwrap().ring.submitter(), result);
        assert!(shared.ready.load(Ordering::Acquire));
        let Some(Completion::Retired(Err(error), Some(cached))) =
            shared.result.lock().unwrap().take()
        else {
            panic!("expected a retired failed request")
        };
        assert_eq!(error.kind(), expected);
        assert!(cached.buffer.is_none());
        assert!(cached.socket.is_none());
    }
    let (mut request, shared) = make_request(&selector, &sender, Cancellation::never());
    assert_eq!(request.entry().get_opcode() as u8, opcode::Send::CODE);
    request.submission_notification().unwrap().submitted();
    assert!(shared.submitted.load(Ordering::Acquire));
    request.completion(-libc::EINTR, 0);
    assert_eq!(request.retry(-libc::EINTR), Some(true));
    assert_eq!(request.entry().get_opcode() as u8, opcode::Send::CODE);
    assert!(
        request.submission_notification().is_none(),
        "retries do not acknowledge the same logical submission again"
    );
    request.completion(-libc::EAGAIN, 0);
    assert!(request.polling.get());
    assert_eq!(request.retry(-libc::EAGAIN), Some(true));
    assert_eq!(request.entry().get_opcode() as u8, opcode::PollAdd::CODE);
    request.completion(-libc::EINTR, 0);
    assert!(request.polling.get());
    request.completion(libc::POLLOUT as i32, 0);
    assert!(!request.polling.get());
    assert_eq!(request.offset.get(), 0);
    request.completion(2, 0);
    assert_eq!(request.offset.get(), 2);
    assert_eq!(request.retry(2), Some(true));
    request.completion(2, 0);
    assert!(request.retry(2).is_none());
    request.retire_completion(&driver.reactor.as_ref().unwrap().ring.submitter(), 2);
    let Some(Completion::Retired(Ok(()), Some(cached))) = shared.result.lock().unwrap().take()
    else {
        panic!("expected a completed request")
    };
    assert!(cached.buffer.is_none());
    drop(cached);
    block_on(selector.socket_close(sender)).unwrap();
    block_on(selector.socket_close(receiver)).unwrap();
}

#[test]
fn cancellation_racing_registration_prevents_submission() {
    let (selector, driver) = pair(16);
    let (sender, receiver) = stream(&selector);
    let token = Cancellation::new();
    let (mut request, shared) = make_request(&selector, &sender, token.clone());
    request.watch = Some(Box::pin(async move {
        token.cancel();
    }));
    assert_eq!(
        request
            .prepare(&driver.reactor.as_ref().unwrap().ring.submitter())
            .unwrap_err()
            .kind(),
        io::ErrorKind::Interrupted
    );
    request.finish(Err(io::ErrorKind::Interrupted.into()));
    assert!(matches!(
        shared.result.lock().unwrap().take(),
        Some(Completion::Retired(Err(OperationError::Cancelled(_)), _))
    ));
    block_on(selector.socket_close(sender)).unwrap();
    block_on(selector.socket_close(receiver)).unwrap();
}

#[test]
fn unpublished_commands_release_normally_and_unknown_kernel_access_reports_retirement_failure() {
    let (selector, _driver) = pair(16);
    let (sender, receiver) = stream(&selector);
    let (request, shared) = make_request(&selector, &sender, Cancellation::never());
    drop(request);
    assert!(matches!(
        shared.result.lock().unwrap().take(),
        Some(Completion::Retired(Err(_), None))
    ));

    let (request, shared) = make_request(&selector, &sender, Cancellation::never());
    let identifier = request.identifier;
    let mut writer = selector.socket_stream_output::<Vec<u8>>(&sender).unwrap();
    writer.shared = shared;
    writer.running = Some(RingBinding::new(&selector.controllers[0], identifier));
    let mut in_flight = InFlight::default();
    in_flight.0.insert(identifier, request);
    drop(in_flight);
    assert_eq!(
        block_on(writer.flush(&selector, &Cancellation::never()))
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        block_on(writer.close(&selector)).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        block_on(writer.close(&selector)).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    drop(writer);
    // Unknown access deliberately retains the first socket and buffer. Exclusive close is consequently unavailable for the leaked sender owner.
    drop(sender);
    block_on(selector.socket_close(receiver)).unwrap();
}

struct AfterDrop {
    bytes: Vec<u8>,
    action: Option<Box<dyn FnOnce() + Send>>,
}
impl AsRef<[u8]> for AfterDrop {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
// SAFETY: Vec owns a stable initialized allocation. The action runs only after backend access ends.
unsafe impl IoBuf for AfterDrop {}
impl Drop for AfterDrop {
    fn drop(&mut self) {
        if let Some(action) = self.action.take() {
            action();
        }
    }
}

#[test]
fn completion_callbacks_can_cancel_or_close_a_waiting_admission_before_its_first_byte() {
    for shutdown in [false, true] {
        let (selector, mut driver) = pair(16);
        let (sender, receiver) = stream(&selector);
        small_send_buffer(&sender);
        let mut writer = selector.socket_stream_output::<AfterDrop>(&sender).unwrap();
        let cancellation = Cancellation::new();
        let token = cancellation.clone();
        let group = Arc::clone(&selector.group);
        let finished = Arc::new(AtomicBool::new(false));
        let returned = Arc::clone(&finished);
        let first = AfterDrop {
            bytes: vec![1; 1024 * 1024],
            action: Some(Box::new(move || {
                if shutdown {
                    group.close();
                } else {
                    token.cancel();
                }
                returned.store(true, Ordering::Release);
            })),
        };
        drive(
            &mut driver,
            writer.submit(&selector, first, &Cancellation::never()),
        )
        .map_err(|(error, _)| error)
        .unwrap();
        let second = AfterDrop {
            bytes: vec![2; 8],
            action: None,
        };
        let mut next = Box::pin(writer.submit(&selector, second, &cancellation));
        assert!(block_on(poll_once(&mut next)).is_none());
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut incoming = vec![0; 65536];
        while !finished.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline);
            driver.turn(Some(Duration::ZERO)).unwrap();
            match receiver.try_recv(&mut incoming, SocketRecvFlags::default()) {
                Ok(count) => assert!(incoming[..count].iter().all(|byte| *byte == 1)),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("receive failed: {error}"),
            }
        }
        let Err((error, returned)) = block_on(next) else {
            panic!("the new write should be rejected")
        };
        assert_eq!(returned.as_ref(), [2; 8]);
        assert_eq!(
            error.kind(),
            if shutdown {
                io::ErrorKind::BrokenPipe
            } else {
                io::ErrorKind::Interrupted
            }
        );
        drive(&mut driver, writer.close(&selector)).unwrap();
        drop(writer);
        drop(sender);
        drop(receiver);
    }
}

#[test]
fn completed_writes_can_migrate_rings_while_reusing_the_request_allocation() {
    use super::super::tests::Failures;
    let (selector, mut drivers) = IoUringSelector::with_workers(
        IoUringOptions {
            entries: 16,
            ..Default::default()
        },
        2,
    )
    .unwrap();
    let (sender, receiver) = stream(&selector);
    let mut writer = selector.socket_stream_output::<Vec<u8>>(&sender).unwrap();
    let never = Cancellation::never();
    let mut previous_worker = None;
    let mut previous_request = None;
    for iteration in 0..8 {
        let _failure = Failures::new([("stream-write", io::ErrorKind::WouldBlock.into())]);
        drive_all(
            &mut drivers,
            writer.submit(&selector, vec![iteration; 16], &never),
        )
        .unwrap();
        let worker = writer.running.as_ref().unwrap().controller.worker;
        if let Some(previous) = previous_worker {
            assert_ne!(worker, previous);
        }
        previous_worker = Some(worker);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !writer.shared.ready.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline);
            for driver in &mut drivers {
                driver.turn(Some(Duration::ZERO)).unwrap();
            }
        }
        let mut incoming = [0; 16];
        let mut received = 0;
        while received < incoming.len() {
            assert!(Instant::now() < deadline);
            match receiver.try_recv(&mut incoming[received..], SocketRecvFlags::default()) {
                Ok(count) => {
                    assert!(count > 0);
                    received += count;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("receive failed: {error}"),
            }
        }
        assert_eq!(incoming, [iteration; 16]);
        block_on(writer.flush(&selector, &never)).unwrap();
        let address = writer.cached.as_ref().unwrap().as_ref() as *const WriteRequest<Vec<u8>>;
        if let Some(previous) = previous_request {
            assert_eq!(address, previous);
        }
        previous_request = Some(address);
    }
    // Completion was published before close; close must collect it without sending another cancellation.
    let _failure = Failures::new([("stream-write", io::ErrorKind::WouldBlock.into())]);
    drive_all(&mut drivers, writer.submit(&selector, vec![9; 16], &never)).unwrap();
    while !writer.shared.ready.load(Ordering::Acquire) {
        for driver in &mut drivers {
            driver.turn(Some(Duration::ZERO)).unwrap();
        }
    }
    block_on(writer.close(&selector)).unwrap();
    drop(writer);
    block_on(selector.socket_close(sender)).unwrap();
    block_on(selector.socket_close(receiver)).unwrap();
}

#[test]
fn raw_send_errors_preserve_os_details_and_completed_writers_can_be_dropped_without_flushing() {
    use super::super::tests::Failures;
    let (selector, mut driver) = pair(16);
    let (sender, receiver) = stream(&selector);
    let never = Cancellation::never();
    let mut writer = selector.socket_stream_output::<Vec<u8>>(&sender).unwrap();
    {
        let _failure = Failures::new([("stream-write", io::Error::from_raw_os_error(libc::EPIPE))]);
        block_on(writer.submit(&selector, vec![1], &never)).unwrap();
    }
    assert_eq!(
        block_on(writer.flush(&selector, &never))
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EPIPE)
    );
    block_on(writer.close(&selector)).unwrap();
    drop(writer);

    let drops = Arc::new(AtomicUsize::new(0));
    let mut writer = selector.socket_stream_output::<Buffer>(&sender).unwrap();
    let _failure = Failures::new([("stream-write", io::ErrorKind::WouldBlock.into())]);
    drive(
        &mut driver,
        writer.submit(&selector, buffer(16, 2, &drops), &never),
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !writer.shared.ready.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline);
        driver.turn(Some(Duration::ZERO)).unwrap();
    }
    assert_eq!(drops.load(Ordering::Acquire), 1);
    drop(writer);
    block_on(selector.socket_close(sender)).unwrap();
    block_on(selector.socket_close(receiver)).unwrap();
}

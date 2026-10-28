// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use base_scheduler_selector::{SelectorDriver, SelectorWorker};
use futures_lite::future::{block_on, poll_once};
use std::{pin::Pin, task::Context, time::Instant};

fn drive<F: Future>(
    worker: &mut crate::IoUringWorker,
    context: &mut Context<'_>,
    mut future: Pin<&mut F>,
) -> F::Output {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Poll::Ready(result) = future.as_mut().poll(context) {
            return result;
        }
        assert!(Instant::now() < deadline, "file monitor test timed out");
        worker.turn(Some(Duration::ZERO)).unwrap();
    }
}

#[test]
fn ordinary_and_submitted_monitor_operations_share_admission_and_retirement() {
    let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
    let mut worker = driver.enter();
    let waker = worker.waker();
    let mut context = Context::from_waker(&waker);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("file");
    std::fs::write(&path, b"abcdef").unwrap();
    let never = Cancellation::never();
    let file = block_on(selector.file_open(
        &path,
        FileOptions {
            write: true,
            ..FileOptions::READ
        },
        &never,
    ))
    .unwrap();
    macro_rules! check {
        ($ordinary:ident, $submit:ident $(, $offset:expr)?) => {{
            let mut monitor = selector.file_monitor(&file).unwrap();
            {
                let mut operation = pin!(selector.$ordinary(&mut monitor, vec![42], $($offset,)? &never));
                assert!(operation.as_mut().poll(&mut context).is_pending());
            }
            let (error, buffer) = block_on(selector.$submit(&mut monitor, vec![7], $($offset,)? &never)).err().unwrap();
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
            assert_eq!(buffer, [7]);
            drive(&mut worker, &mut context, pin!(selector.file_monitor_close(&mut monitor))).unwrap();

            let mut monitor = selector.file_monitor(&file).unwrap();
            let completion = block_on(selector.$submit(&mut monitor, vec![42], $($offset,)? &never)).unwrap();
            drop(completion);
            let (result, buffer) = block_on(selector.$ordinary(&mut monitor, vec![7], $($offset,)? &never));
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
            assert_eq!(buffer, [7]);
            {
                let mut close = pin!(selector.file_monitor_close(&mut monitor));
                assert!(close.as_mut().poll(&mut context).is_pending());
            }
            drive(&mut worker, &mut context, pin!(selector.file_monitor_close(&mut monitor))).unwrap();
            let (error, buffer) = block_on(selector.$submit(&mut monitor, vec![7], $($offset,)? &never)).err().unwrap();
            assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, [7]);
        }};
    }
    check!(file_monitor_read, file_monitor_read_submit);
    check!(file_monitor_write, file_monitor_write_submit);
    check!(file_monitor_read_at, file_monitor_read_at_submit, 0);
    check!(file_monitor_write_at, file_monitor_write_at_submit, 0);
    block_on(selector.file_close(file)).unwrap();
}

#[test]
fn dropping_an_unpolled_monitor_completion_queues_cancellation_without_close() {
    macro_rules! check {
        ($submit:ident $(, $offset:expr)?) => {{
            let (selector, mut driver) = super::super::tests::pair(1);
            let (_directory, file) = super::super::tests::file(&selector);
            let never = Cancellation::never();
            let mut monitor = selector.file_monitor(&file).unwrap();
            let completion = block_on(selector.$submit(&mut monitor, vec![42], $($offset,)? &never)).unwrap();
            let incoming = &driver.reactor.as_mut().unwrap().incoming;
            let request = match incoming.try_recv().unwrap() {
                Command::Submit(request) => request,
                _ => panic!("expected the accepted file request"),
            };
            let identifier = request.identifier();
            assert!(incoming.try_recv().is_err(), "admission must not cancel the accepted request");
            drop(completion);
            assert!(matches!(incoming.try_recv().unwrap(), Command::Cancel(cancelled) if cancelled == identifier));
            drop(request);
            drop(monitor);
            super::super::tests::drain(&selector, &mut driver);
        }};
    }
    check!(file_monitor_read_submit);
    check!(file_monitor_write_submit);
    check!(file_monitor_read_at_submit, 0);
    check!(file_monitor_write_at_submit, 0);
}

#[test]
fn write_all_reuses_one_request_type_across_short_transfers_and_logical_writes() {
    struct ShortWriter<'a> {
        selector: &'a IoUringSelector,
        monitor: IoUringFileRegistration,
    }
    impl FileWriter for ShortWriter<'_> {
        async fn write<B: IoBuf>(
            &mut self,
            buffer: B,
            cancellation: &Cancellation,
        ) -> BufferResult<B> {
            let length = buffer.as_ref().len().min(2);
            let (result, buffer) = self
                .selector
                .file_monitor_write(&mut self.monitor, buffer.slice(..length), cancellation)
                .await;
            (result, buffer.into_inner())
        }
        async fn write_at<B: IoBuf>(
            &mut self,
            buffer: B,
            offset: u64,
            cancellation: &Cancellation,
        ) -> BufferResult<B> {
            self.selector
                .file_monitor_write_at(&mut self.monitor, buffer, offset, cancellation)
                .await
        }
        async fn close(&mut self) -> io::Result<()> {
            self.selector.file_monitor_close(&mut self.monitor).await
        }
    }
    let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
    let mut worker = driver.enter();
    let waker = worker.waker();
    let mut context = Context::from_waker(&waker);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("file");
    std::fs::write(&path, b"").unwrap();
    let never = Cancellation::never();
    let file = block_on(selector.file_open(
        &path,
        FileOptions {
            write: true,
            ..FileOptions::READ
        },
        &never,
    ))
    .unwrap();
    let mut writer = ShortWriter {
        selector: &selector,
        monitor: selector.file_monitor(&file).unwrap(),
    };
    let mut buffer = Some(b"abcdef".to_vec());
    let mut cached = None;
    for iteration in 0..8 {
        let counts = allocation_counter::measure(|| {
            let (result, returned) = drive(
                &mut worker,
                &mut context,
                pin!(writer.write_all(buffer.take().unwrap(), &never)),
            );
            assert_eq!(result.unwrap(), 6);
            buffer = Some(returned);
        });
        if iteration >= 2 {
            assert_eq!(
                counts.count_total, 0,
                "short write_all replaced its cached request"
            );
        }
        let result = writer.monitor.shared.result.lock().unwrap();
        let Some(Completion::Cached(cached_request)) = result.as_ref() else {
            panic!("completed write retains request storage")
        };
        let current = cached_request
            .downcast_ref::<FileRequest<BufferSlice<BufferSlice<Vec<u8>>>>>()
            .unwrap() as *const _;
        if let Some(previous) = cached {
            assert_eq!(current, previous);
        }
        cached = Some(current);
    }
    assert_eq!(std::fs::read(path).unwrap(), b"abcdef".repeat(8));
    drive(&mut worker, &mut context, pin!(writer.close())).unwrap();
    drop(writer);
    block_on(selector.file_close(file)).unwrap();
}

#[test]
fn repeated_operations_reuse_request_storage_without_allocations() {
    let (selector, driver) = IoUringSelector::new(IoUringOptions {
        entries: 16,
        ..Default::default()
    })
    .unwrap();
    let mut worker = driver.enter();
    let waker = worker.waker();
    let mut context = Context::from_waker(&waker);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("file");
    std::fs::write(&path, b"abcdef").unwrap();
    let never = Cancellation::never();
    let file = block_on(selector.file_open(&path, FileOptions::READ, &never)).unwrap();
    let mut monitor = selector.file_monitor(&file).unwrap();
    let mut buffer = Some(vec![0; 6]);
    let mut address = None;
    for iteration in 0..34 {
        let counts = allocation_counter::measure(|| {
            let mut operation = pin!(selector.file_monitor_read_at(
                &mut monitor,
                buffer.take().unwrap(),
                0,
                &never
            ));
            let (result, returned) = drive(&mut worker, &mut context, operation.as_mut());
            assert_eq!(result.unwrap(), 6);
            buffer = Some(returned);
        });
        if iteration >= 2 {
            assert_eq!(counts.count_total, 0, "ordinary monitor reuse allocated");
        }
        let result = monitor.shared.result.lock().unwrap();
        let Some(Completion::Cached(cached_request)) = result.as_ref() else {
            panic!("completed read retains request storage")
        };
        let current = cached_request
            .downcast_ref::<FileRequest<Vec<u8>>>()
            .unwrap() as *const _;
        if let Some(previous) = address {
            assert_eq!(current, previous);
        }
        address = Some(current);
    }
    assert_eq!(buffer.unwrap(), b"abcdef");
    drive(
        &mut worker,
        &mut context,
        pin!(selector.file_monitor_close(&mut monitor)),
    )
    .unwrap();
    drop(monitor);
    block_on(selector.file_close(file)).unwrap();
}

#[test]
fn abandoned_operations_can_be_reused_and_forgotten_completions_can_be_closed() {
    let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
    let mut worker = driver.enter();
    let waker = worker.waker();
    let mut context = Context::from_waker(&waker);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("file");
    std::fs::write(&path, b"abcdef").unwrap();
    let never = Cancellation::never();
    let file = block_on(selector.file_open(&path, FileOptions::READ, &never)).unwrap();
    for forget in [false, true] {
        let mut monitor = selector.file_monitor(&file).unwrap();
        let mut operation =
            Box::pin(selector.file_monitor_read_at(&mut monitor, vec![0; 6], 0, &never));
        assert!(operation.as_mut().poll(&mut context).is_pending());
        if forget {
            std::mem::forget(operation);
        } else {
            drop(operation);
        }
        assert_eq!(
            io::Error::from(
                block_on(selector.file_monitor_read_at(&mut monitor, vec![0], 0, &never))
                    .0
                    .unwrap_err()
            )
            .kind(),
            io::ErrorKind::WouldBlock
        );
        while !monitor.shared.ready.load(Ordering::Acquire) {
            worker.turn(Some(Duration::ZERO)).unwrap();
        }
        if forget {
            let (result, _) =
                block_on(selector.file_monitor_read_at(&mut monitor, vec![0; 3], 0, &never));
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        } else {
            {
                let mut operation =
                    pin!(selector.file_monitor_read_at(&mut monitor, vec![0; 3], 0, &never));
                let (result, buffer) = drive(&mut worker, &mut context, operation.as_mut());
                assert_eq!(result.unwrap(), 3);
                assert_eq!(buffer, b"abc");
            }
        }
        drive(
            &mut worker,
            &mut context,
            pin!(selector.file_monitor_close(&mut monitor)),
        )
        .unwrap();
    }
    let mut monitor = selector.file_monitor(&file).unwrap();
    let mut operation =
        Box::pin(selector.file_monitor_read_at(&mut monitor, vec![0; 6], 0, &never));
    assert!(operation.as_mut().poll(&mut context).is_pending());
    std::mem::forget(operation);
    {
        let mut close = pin!(selector.file_monitor_close(&mut monitor));
        drive(&mut worker, &mut context, close.as_mut()).unwrap();
    }
    drop(monitor);
    block_on(selector.file_close(file)).unwrap();
}

#[test]
fn incompatible_and_closed_selectors_reject_monitor_operations() {
    let (first, _driver) = IoUringSelector::new(Default::default()).unwrap();
    let (second, _other) = IoUringSelector::new(Default::default()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("file");
    std::fs::write(&path, b"abcdef").unwrap();
    let never = Cancellation::never();
    let file = block_on(first.file_open(&path, FileOptions::READ, &never)).unwrap();
    let mut monitor = first.file_monitor(&file).unwrap();
    let (result, _) = block_on(second.file_monitor_read_at(&mut monitor, vec![0; 3], 0, &never));
    assert_eq!(
        io::Error::from(result.unwrap_err()).kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        block_on(second.file_monitor_close(&mut monitor))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    first.close();
    assert_eq!(
        io::Error::from(
            block_on(first.file_monitor_read_at(&mut monitor, vec![0], 0, &never))
                .0
                .unwrap_err()
        )
        .kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[test]
fn queued_requests_return_owners_but_unknown_kernel_retirement_never_does() {
    for submit in [false, true] {
        let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
        let mut worker = driver.enter();
        let waker = worker.waker();
        let mut context = Context::from_waker(&waker);
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("file");
        std::fs::write(&path, b"abcdef").unwrap();
        let never = Cancellation::never();
        let file = block_on(selector.file_open(&path, FileOptions::READ, &never)).unwrap();
        let mut monitor = selector.file_monitor(&file).unwrap();
        let mut operation =
            Box::pin(selector.file_monitor_read_at(&mut monitor, vec![42; 6], 0, &never));
        assert!(operation.as_mut().poll(&mut context).is_pending());
        if submit {
            worker.turn(Some(Duration::ZERO)).unwrap();
        }
        drop(worker);
        if submit {
            let panic =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(operation)));
            assert!(panic.is_err());
            for _ in 0..2 {
                assert_eq!(
                    block_on(selector.file_monitor_close(&mut monitor))
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::BrokenPipe
                );
            }
        } else {
            let (result, buffer) = block_on(operation);
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, [42; 6]);
            block_on(selector.file_monitor_close(&mut monitor)).unwrap();
            drop(monitor);
            let retained = retain_file(&file);
            assert_eq!(
                Arc::strong_count(&retained),
                2,
                "queued request released its retained file before completion"
            );
            drop(retained);
            drop(file);
        }
    }
}

#[test]
fn close_and_reuse_both_report_an_abandoned_driver_failure() {
    for reuse in [false, true] {
        let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
        let mut worker = driver.enter();
        let waker = worker.waker();
        let mut context = Context::from_waker(&waker);
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("file");
        std::fs::write(&path, b"abcdef").unwrap();
        let never = Cancellation::never();
        let file = block_on(selector.file_open(&path, FileOptions::READ, &never)).unwrap();
        let mut monitor = selector.file_monitor(&file).unwrap();
        let mut operation =
            Box::pin(selector.file_monitor_read_at(&mut monitor, vec![42; 6], 0, &never));
        assert!(operation.as_mut().poll(&mut context).is_pending());
        worker.turn(Some(Duration::ZERO)).unwrap();
        std::mem::forget(operation);
        drop(worker);
        if reuse {
            assert_eq!(
                block_on(selector.file_monitor_read_at(&mut monitor, vec![0], 0, &never))
                    .0
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
        }
        for _ in 0..2 {
            assert_eq!(
                block_on(selector.file_monitor_close(&mut monitor))
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
        }
    }
}

#[test]
fn admission_cancellation_and_identifier_exhaustion_return_the_buffer() {
    let (selector, _driver) = super::super::tests::pair(1);
    let (_directory, file) = super::super::tests::file(&selector);
    let never = Cancellation::never();
    let mut monitor = selector.file_monitor(&file).unwrap();
    let controller = &selector.controllers[0];
    let permit = block_on(controller.capacity.acquire(&never)).unwrap();
    let cancellation = Cancellation::new();
    {
        let mut operation =
            pin!(selector.file_monitor_read_at(&mut monitor, vec![42], 0, &cancellation));
        assert!(block_on(poll_once(operation.as_mut())).is_none());
        cancellation.cancel();
        let (result, buffer) = block_on(operation);
        assert!(matches!(result, Err(OperationError::Cancelled(_))));
        assert_eq!(buffer, [42]);
    }
    drop(permit);
    controller
        .next_identifier
        .store(CANCEL_TAG - 1, Ordering::Relaxed);
    let (result, buffer) =
        block_on(selector.file_monitor_read_at(&mut monitor, vec![43], 0, &never));
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Other);
    assert_eq!(buffer, [43]);
    block_on(selector.file_monitor_close(&mut monitor)).unwrap();
}

#[test]
fn rejected_abandoned_command_is_reclaimed_before_reuse_reports_shutdown() {
    let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
    let worker = driver.enter();
    let waker = worker.waker();
    let mut context = Context::from_waker(&waker);
    let (_directory, file) = super::super::tests::file(&selector);
    let never = Cancellation::never();
    let mut monitor = selector.file_monitor(&file).unwrap();
    let mut operation = Box::pin(selector.file_monitor_read_at(&mut monitor, vec![42], 0, &never));
    assert!(operation.as_mut().poll(&mut context).is_pending());
    drop(operation);
    drop(worker);
    let (result, buffer) =
        block_on(selector.file_monitor_read_at(&mut monitor, vec![43], 0, &never));
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(buffer, [43]);
    assert!(monitor.running.is_none());
    block_on(selector.file_monitor_close(&mut monitor)).unwrap();
}

struct ClosingBuffer {
    bytes: Vec<u8>,
    group: Arc<DriverGroup>,
}
impl AsRef<[u8]> for ClosingBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
// SAFETY: The Vec retains the same initialized allocation and range throughout ownership, without aliases.
unsafe impl IoBuf for ClosingBuffer {}
impl Drop for ClosingBuffer {
    fn drop(&mut self) {
        self.group.close();
    }
}

#[test]
fn disposal_of_an_abandoned_owner_can_close_admission_before_publication() {
    let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
    let mut worker = driver.enter();
    let waker = worker.waker();
    let mut context = Context::from_waker(&waker);
    let (_directory, file) = super::super::tests::file(&selector);
    let never = Cancellation::never();
    let mut monitor = selector.file_monitor(&file).unwrap();
    let buffer = ClosingBuffer {
        bytes: vec![42],
        group: Arc::clone(&selector.group),
    };
    let mut operation = Box::pin(selector.file_monitor_write_at(&mut monitor, buffer, 0, &never));
    assert!(operation.as_mut().poll(&mut context).is_pending());
    // Abandon collection; the queued request still owns its buffer until retirement.
    drop(operation);
    while !monitor.shared.ready.load(Ordering::Acquire) {
        worker.turn(Some(Duration::ZERO)).unwrap();
    }
    // Changing buffer type discards the cached owner after admission but before publishing the new command.
    let (result, buffer) =
        block_on(selector.file_monitor_write_at(&mut monitor, vec![43], 0, &never));
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(buffer, [43]);
    drive(
        &mut worker,
        &mut context,
        pin!(selector.file_monitor_close(&mut monitor)),
    )
    .unwrap();
}

#[test]
fn migration_invalidates_old_registrations_and_reuses_request_storage() {
    let (selector, mut drivers) = IoUringSelector::with_workers(Default::default(), 2).unwrap();
    let (_directory, file) = super::super::tests::file(&selector);
    let mut monitor = selector.file_monitor(&file).unwrap();
    let never = Cancellation::never();
    let wake = drivers[0].waker();
    let mut context = Context::from_waker(&wake);
    let mut buffer = Some(vec![0; 6]);
    let mut request_address = None;
    let mut previous_registration: Option<Arc<RegisteredFile>> = None;
    let mut retired_registration: Option<Weak<RegisteredFile>> = None;
    // Polling outside entered workers deliberately alternates routes between the two rings.
    for _ in 0..34 {
        {
            let mut operation = pin!(selector.file_monitor_read_at(
                &mut monitor,
                buffer.take().unwrap(),
                0,
                &never
            ));
            loop {
                if let Poll::Ready((result, returned)) = operation.as_mut().poll(&mut context) {
                    assert_eq!(result.unwrap(), 6);
                    buffer = Some(returned);
                    break;
                }
                for driver in &mut drivers {
                    driver.turn(Some(Duration::ZERO)).unwrap();
                }
            }
        }
        if let Some(retired) = retired_registration.take() {
            assert!(retired.upgrade().is_none());
        }
        let registration = monitor.files.values().next().unwrap();
        if let Some(previous) = previous_registration.replace(Arc::clone(registration)) {
            assert!(!Arc::ptr_eq(&previous, registration));
            assert!(previous.released());
            retired_registration = Some(Arc::downgrade(&previous));
        }
        let result = monitor.shared.result.lock().unwrap();
        let Some(Completion::Cached(cached_request)) = result.as_ref() else {
            panic!("completed read retains request storage")
        };
        let current = cached_request
            .downcast_ref::<FileRequest<Vec<u8>>>()
            .unwrap() as *const _;
        if let Some(previous) = request_address {
            assert_eq!(current, previous);
        }
        request_address = Some(current);
    }
    assert_eq!(monitor.files.values().count(), 1);
    for file in monitor.files.values() {
        assert_eq!(file.index().unwrap().0, 0);
    }
    {
        let mut close = pin!(selector.file_monitor_close(&mut monitor));
        loop {
            if let Poll::Ready(result) = close.as_mut().poll(&mut context) {
                result.unwrap();
                break;
            }
            for driver in &mut drivers {
                driver.turn(Some(Duration::ZERO)).unwrap();
            }
        }
    }
    drop(previous_registration);
    drop(monitor);
    block_on(selector.file_close(file)).unwrap();
}

#[test]
fn active_request_keeps_its_ring_when_polling_moves_to_another_worker() {
    let (selector, mut drivers) = IoUringSelector::with_workers(Default::default(), 2).unwrap();
    let (_directory, file) = super::super::tests::file(&selector);
    let mut monitor = selector.file_monitor(&file).unwrap();
    let never = Cancellation::never();
    let wake = drivers[0].waker();
    let mut context = Context::from_waker(&wake);
    let mut operation =
        Box::pin(selector.file_monitor_read_at(&mut monitor, vec![0; 6], 0, &never));
    assert!(operation.as_mut().poll(&mut context).is_pending());
    let mut worker = drivers.pop().unwrap().enter();
    assert!(operation.as_mut().poll(&mut context).is_pending());
    worker.turn(Some(Duration::ZERO)).unwrap();
    assert!(operation.as_mut().poll(&mut context).is_pending());
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        drivers[0].turn(Some(Duration::ZERO)).unwrap();
        if let Poll::Ready((result, buffer)) = operation.as_mut().poll(&mut context) {
            assert_eq!(result.unwrap(), 6);
            assert_eq!(buffer, b"abcdef");
            break;
        }
        assert!(Instant::now() < deadline);
    }
    drop(operation);
    assert_eq!(
        selector.controllers[1]
            .next_identifier
            .load(Ordering::Relaxed),
        0
    );
    {
        let mut operation =
            pin!(selector.file_monitor_read_at(&mut monitor, vec![0; 6], 0, &never));
        drive(&mut worker, &mut context, operation.as_mut())
            .0
            .unwrap();
    }
    assert_eq!(monitor.files.values().count(), 1);
    let mut close = pin!(selector.file_monitor_close(&mut monitor));
    loop {
        if let Poll::Ready(result) = close.as_mut().poll(&mut context) {
            result.unwrap();
            break;
        }
        drivers[0].turn(Some(Duration::ZERO)).unwrap();
        worker.turn(Some(Duration::ZERO)).unwrap();
    }
}

#[test]
fn unavailable_fixed_files_fall_back_without_changing_io_results() {
    for failure in [
        "disabled",
        "fixed-file-table",
        "fixed-file-register",
        "full",
    ] {
        let (selector, driver) = IoUringSelector::new(IoUringOptions {
            fixed_files: if failure == "disabled" { 0 } else { 1 },
            ..Default::default()
        })
        .unwrap();
        let mut worker = driver.enter();
        let wake = worker.waker();
        let mut context = Context::from_waker(&wake);
        let (_directory, file) = super::super::tests::file(&selector);
        let never = Cancellation::never();
        let _failures = super::super::tests::Failures::new(
            ["fixed-file-table", "fixed-file-register"]
                .contains(&failure)
                .then_some((failure, io::ErrorKind::PermissionDenied.into())),
        );
        let mut monitors = [
            selector.file_monitor(&file).unwrap(),
            selector.file_monitor(&file).unwrap(),
        ];
        for (index, monitor) in monitors.iter_mut().enumerate() {
            let mut operation =
                Box::pin(selector.file_monitor_read_at(monitor, vec![0; 6], 0, &never));
            let (result, buffer) = drive(&mut worker, &mut context, operation.as_mut());
            assert_eq!(result.unwrap(), 6);
            assert_eq!(buffer, b"abcdef");
            drop(operation);
            let cached = monitor.files.values().next().unwrap();
            let registered = (failure == "full" && index == 0)
                || (failure == "fixed-file-register" && index == 1);
            assert_eq!(cached.index().is_some(), registered);
        }
        for monitor in &mut monitors {
            drive(
                &mut worker,
                &mut context,
                pin!(selector.file_monitor_close(monitor)),
            )
            .unwrap();
        }
    }
}

#[test]
fn abandoned_close_preserves_release_and_retry_waits_for_every_registration() {
    let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let (_directory, file) = super::super::tests::file(&selector);
    let never = Cancellation::never();
    let mut monitor = selector.file_monitor(&file).unwrap();
    drive(
        &mut worker,
        &mut context,
        pin!(selector.file_monitor_read_at(&mut monitor, vec![0; 6], 0, &never)),
    )
    .0
    .unwrap();
    {
        let mut close = pin!(selector.file_monitor_close(&mut monitor));
        assert!(close.as_mut().poll(&mut context).is_pending());
        // Leaving this scope drops the wait; the next close must finish retirement.
    }
    drive(
        &mut worker,
        &mut context,
        pin!(selector.file_monitor_close(&mut monitor)),
    )
    .unwrap();
    // The released table slot is immediately reusable by another monitor.
    let mut next = selector.file_monitor(&file).unwrap();
    drive(
        &mut worker,
        &mut context,
        pin!(selector.file_monitor_read_at(&mut next, vec![0; 6], 0, &never)),
    )
    .0
    .unwrap();
    assert_eq!(next.files.values().next().unwrap().index().unwrap().0, 0);
    drive(
        &mut worker,
        &mut context,
        pin!(selector.file_monitor_close(&mut next)),
    )
    .unwrap();
}

#[test]
fn release_errors_and_driver_exit_remain_observable_on_repeated_close() {
    for exited in [false, true] {
        let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
        let mut worker = driver.enter();
        let wake = worker.waker();
        let mut context = Context::from_waker(&wake);
        let (_directory, file) = super::super::tests::file(&selector);
        let never = Cancellation::never();
        let mut monitor = selector.file_monitor(&file).unwrap();
        drive(
            &mut worker,
            &mut context,
            pin!(selector.file_monitor_read_at(&mut monitor, vec![0; 6], 0, &never)),
        )
        .0
        .unwrap();
        if exited {
            drop(worker);
            for _ in 0..2 {
                assert_eq!(
                    block_on(selector.file_monitor_close(&mut monitor))
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::BrokenPipe
                );
            }
        } else {
            let _failures = super::super::tests::Failures::new([(
                "fixed-file-release",
                io::ErrorKind::PermissionDenied.into(),
            )]);
            for _ in 0..2 {
                assert_eq!(
                    drive(
                        &mut worker,
                        &mut context,
                        pin!(selector.file_monitor_close(&mut monitor))
                    )
                    .unwrap_err()
                    .kind(),
                    io::ErrorKind::PermissionDenied
                );
            }
        }
    }
}

#[test]
fn failed_eviction_remains_observable_after_another_migration() {
    let (selector, mut drivers) = IoUringSelector::with_workers(Default::default(), 2).unwrap();
    let (_directory, file) = super::super::tests::file(&selector);
    let mut monitor = selector.file_monitor(&file).unwrap();
    let never = Cancellation::never();
    let wake = drivers[0].waker();
    let mut context = Context::from_waker(&wake);
    for iteration in 0..3 {
        let _failures = super::super::tests::Failures::new(
            (iteration == 1)
                .then_some(("fixed-file-release", io::ErrorKind::PermissionDenied.into())),
        );
        let mut operation =
            pin!(selector.file_monitor_read_at(&mut monitor, vec![0; 6], 0, &never));
        loop {
            if let Poll::Ready((result, buffer)) = operation.as_mut().poll(&mut context) {
                assert_eq!(result.unwrap(), 6);
                assert_eq!(buffer, b"abcdef");
                break;
            }
            for driver in &mut drivers {
                driver.turn(Some(Duration::ZERO)).unwrap();
            }
        }
    }
    assert!(monitor.retiring.iter().any(|file| !file.released()));
    for _ in 0..2 {
        let mut close = pin!(selector.file_monitor_close(&mut monitor));
        loop {
            if let Poll::Ready(result) = close.as_mut().poll(&mut context) {
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
                break;
            }
            for driver in &mut drivers {
                driver.turn(Some(Duration::ZERO)).unwrap();
            }
        }
    }
}

#[test]
fn close_waits_for_migration_release_on_the_original_ring_after_abandonment() {
    let (selector, mut drivers) = IoUringSelector::with_workers(Default::default(), 2).unwrap();
    let (_directory, file) = super::super::tests::file(&selector);
    let mut monitor = selector.file_monitor(&file).unwrap();
    let never = Cancellation::never();
    let wake = drivers[0].waker();
    let mut context = Context::from_waker(&wake);
    // External routing selects each ring in turn. Leave the first ring idle after migration.
    for driver in &mut drivers {
        super::super::tests::drive(
            driver,
            selector.file_monitor_read_at(&mut monitor, vec![0; 6], 0, &never),
        )
        .0
        .unwrap();
    }
    assert_eq!(monitor.retiring.len(), 1);
    assert!(!monitor.retiring[0].released());
    {
        let mut close = pin!(selector.file_monitor_close(&mut monitor));
        assert!(close.as_mut().poll(&mut context).is_pending());
        drivers[1].turn(Some(Duration::ZERO)).unwrap();
        assert!(close.as_mut().poll(&mut context).is_pending());
        // Leaving this scope drops the wait; the next close must finish retirement.
    }
    assert!(!monitor.retiring[0].released());
    {
        let mut close = pin!(selector.file_monitor_close(&mut monitor));
        assert!(close.as_mut().poll(&mut context).is_pending());
        drivers[0].turn(Some(Duration::ZERO)).unwrap();
        block_on(close).unwrap();
    }
    assert!(monitor.retiring[0].released());
    drop(monitor);
    block_on(selector.file_close(file)).unwrap();
}

#[test]
fn concurrent_completion_and_close_keep_independent_wakers_and_release_file_access() {
    struct Wakes(AtomicUsize);
    impl std::task::Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
    let mut worker = driver.enter();
    let (_directory, file) = super::super::tests::file(&selector);
    let never = Cancellation::never();
    let mut lane = selector.file_monitor(&file).unwrap();
    let mut completion = Box::pin(
        block_on(selector.file_monitor_read_at_submit(&mut lane, vec![0; 6], 0, &never)).unwrap(),
    );
    let completion_wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let completion_waker = std::task::Waker::from(Arc::clone(&completion_wakes));
    let mut completion_context = Context::from_waker(&completion_waker);
    assert!(
        completion
            .as_mut()
            .poll(&mut completion_context)
            .is_pending()
    );
    let close_wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let close_waker = std::task::Waker::from(Arc::clone(&close_wakes));
    let mut close_context = Context::from_waker(&close_waker);
    let mut close = Box::pin(selector.file_monitor_close(&mut lane));
    assert!(close.as_mut().poll(&mut close_context).is_pending());
    let deadline = Instant::now() + Duration::from_secs(5);
    while completion_wakes.0.load(Ordering::SeqCst) == 0
        || close_wakes.0.load(Ordering::SeqCst) == 0
    {
        worker.turn(Some(Duration::ZERO)).unwrap();
        assert!(
            Instant::now() < deadline,
            "both independent waiters must be woken"
        );
    }
    drive(&mut worker, &mut close_context, close.as_mut()).unwrap();
    drop(close);
    drop(lane);
    block_on(selector.file_close(file)).unwrap();
    let (result, buffer) = block_on(completion);
    // Close may have requested cancellation, but successful original completion still wins.
    if result.is_ok() {
        assert_eq!(buffer, b"abcdef");
    }
}

#[test]
fn dropping_a_cancelled_completion_releases_its_retired_owner_without_lane_reuse() {
    let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
    let mut worker = driver.enter();
    let waker = worker.waker();
    let mut context = Context::from_waker(&waker);
    let (_directory, file) = super::super::tests::file(&selector);
    let mut lane = selector.file_monitor(&file).unwrap();
    let cancellation = Cancellation::new();
    let pool = base_scheduler::HeapBufferPool::new(1, 6).unwrap();
    let buffer = pool.try_acquire().unwrap();
    let mut completion = Box::pin(
        block_on(selector.file_monitor_read_at_submit(&mut lane, buffer, 0, &cancellation))
            .unwrap(),
    );
    assert!(completion.as_mut().poll(&mut context).is_pending());
    cancellation.cancel();
    // Polling cancellation sends its request; it does not collect the original owner.
    assert!(completion.as_mut().poll(&mut context).is_pending());
    let deadline = Instant::now() + Duration::from_secs(5);
    while !lane.shared.ready.load(Ordering::Acquire) {
        worker.turn(Some(Duration::ZERO)).unwrap();
        assert!(Instant::now() < deadline);
    }
    assert!(pool.try_acquire().is_none());
    drop(completion);
    assert!(pool.try_acquire().is_some());
    drive(
        &mut worker,
        &mut context,
        pin!(selector.file_monitor_close(&mut lane)),
    )
    .unwrap();
    drop(lane);
    block_on(selector.file_close(file)).unwrap();
}

#[test]
fn synchronous_submission_rejection_preserves_the_original_completion_owner() {
    let (selector, _driver) = IoUringSelector::new(Default::default()).unwrap();
    let (_directory, file) = super::super::tests::file(&selector);
    let never = Cancellation::never();
    let mut lane = selector.file_monitor(&file).unwrap();
    let controller = Arc::clone(&selector.controllers[0]);
    super::super::tests::BEFORE_CAPACITY_ADMISSION.with(|hook| {
        // Admission wins its capacity race, but endpoint rejection finishes before submit returns the completion future.
        *hook.borrow_mut() = Some(Box::new(move || {
            controller.closed.store(true, Ordering::Release)
        }));
    });
    let buffer = vec![42; 6];
    let pointer = buffer.as_ptr();
    let completion =
        block_on(selector.file_monitor_read_at_submit(&mut lane, buffer, 0, &never)).unwrap();
    let (result, buffer) = block_on(completion);
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(buffer.as_ptr(), pointer);
    assert_eq!(buffer, [42; 6]);
    block_on(selector.file_monitor_close(&mut lane)).unwrap();
    drop(lane);
    block_on(selector.file_close(file)).unwrap();
}

#[test]
fn a_driver_failure_wakes_close_that_already_waits_for_retirement() {
    let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
    let mut worker = driver.enter();
    let waker = worker.waker();
    let mut context = Context::from_waker(&waker);
    let (_directory, file) = super::super::tests::file(&selector);
    let never = Cancellation::never();
    let mut monitor = selector.file_monitor(&file).unwrap();
    let completion =
        block_on(selector.file_monitor_read_at_submit(&mut monitor, vec![42], 0, &never)).unwrap();
    worker.turn(Some(Duration::ZERO)).unwrap();
    drop(completion);
    {
        let mut close = pin!(selector.file_monitor_close(&mut monitor));
        assert!(close.as_mut().poll(&mut context).is_pending());
        drop(worker);
        assert_eq!(
            block_on(close).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }
    assert_eq!(
        block_on(selector.file_monitor_close(&mut monitor))
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
}

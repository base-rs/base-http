// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate reusable bound file-resource cases using the existing file fixture and runner.
#[macro_export]
macro_rules! file_resource_tests {
    (run: $run:path $(,)?) => {
        $crate::__file_test_cases! { $run;
            bound_file_handles_preserve_cursor_offsets_and_ownership => |fixture, scheduler, file| {
                use $crate::__base_scheduler::{FileReader as _, FileReading as _, FileWriter as _, FileWriting as _};
                let never = $crate::__base_scheduler::Cancellation::never();
                let mut first = scheduler.file_reader(&file).unwrap();
                let mut second = scheduler.file_reader(&file).unwrap();
                let mut writer = scheduler.file_writer(&file).unwrap();
                let storage = ::std::vec![0; 2];
                let pointer = storage.as_ptr();
                let (result, storage) = first.read(storage, &never).await;
                assert_eq!(result.unwrap(), 2);
                assert_eq!(storage, b"ab");
                assert_eq!(storage.as_ptr(), pointer);
                let (result, storage) = second.read(storage, &never).await;
                assert_eq!(result.unwrap(), 2);
                assert_eq!(storage, b"cd");
                let (result, storage) = writer.write(storage, &never).await;
                assert_eq!(result.unwrap(), 2);
                assert_eq!(storage.as_ptr(), pointer);
                fixture.assert_contents(b"abcdcd");
                let (result, storage) = first.read_at(storage, 1, &never).await;
                assert_eq!(result.unwrap(), 2);
                assert_eq!(storage, b"bc");
                assert_eq!(writer.write_at(storage, 0, &never).await.0.unwrap(), 2);
                fixture.assert_contents(b"bccdcd");
                scheduler.file_seek(&file, ::std::io::SeekFrom::Start(0), &never).await.unwrap();
                assert_eq!(writer.write_all(::std::vec![42; 6], &never).await.0.unwrap(), 6);
                fixture.assert_contents(&[42; 6]);
                first.close().await.unwrap();
                second.close().await.unwrap();
                writer.close().await.unwrap();
                first.close().await.unwrap();
                writer.close().await.unwrap();
            },
            bound_file_handles_reject_cancelled_and_closed_operations => |fixture, scheduler, file| {
                use $crate::__base_scheduler::{FileReader as _, FileReading as _, FileWriter as _, FileWriting as _};
                let cancelled = $crate::__base_scheduler::Cancellation::new();
                cancelled.cancel();
                let never = $crate::__base_scheduler::Cancellation::never();
                let mut reader = scheduler.file_reader(&file).unwrap();
                let mut writer = scheduler.file_writer(&file).unwrap();
                for result in [
                    reader.read(::std::vec![42], &cancelled).await,
                    reader.read_at(::std::vec![42], 0, &cancelled).await,
                    writer.write(::std::vec![42], &cancelled).await,
                    writer.write_at(::std::vec![42], 0, &cancelled).await,
                    writer.write_all(::std::vec![42], &cancelled).await,
                ] {
                    assert!(matches!(result.0, Err($crate::__base_scheduler::OperationError::Cancelled(_))));
                    assert_eq!(result.1, [42]);
                }
                fixture.assert_contents(b"abcdef");
                reader.close().await.unwrap();
                writer.close().await.unwrap();
                assert_eq!(reader.read(::std::vec![42], &never).await.0.unwrap_err().kind(), ::std::io::ErrorKind::BrokenPipe);
                assert_eq!(writer.write(::std::vec![42], &never).await.0.unwrap_err().kind(), ::std::io::ErrorKind::BrokenPipe);
            },
            sequential_reader_writer => |fixture, scheduler, file| {
                $crate::file_resources::sequential(scheduler, &file).await;
            },
            independent_readers => |fixture, scheduler, file| {
                $crate::file_resources::independent(scheduler, &file).await;
            },
            resource_cancellation => |fixture, scheduler, file| {
                $crate::file_resources::cancellation(scheduler, &file).await;
            },
            resource_inflight_cancellation => |fixture, scheduler, file| {
                $crate::file_resources::inflight_cancellation(scheduler, &file).await;
            },
            pending_resource_retirement => |fixture, scheduler, file| {
                $crate::file_resources::pending_retirement(scheduler, &file).await;
            },
            resource_write_error => |fixture, scheduler, file| {
                use $crate::__base_scheduler::{FileOpen as _, FileWriting as _, FileWriter as _};
                let never = $crate::__base_scheduler::Cancellation::never();
                let read_only = scheduler.file_open(fixture.path(), $crate::__base_scheduler::FileOptions::READ, &never).await.unwrap();
                let mut writer = scheduler.file_writer(&read_only).unwrap();
                let (result, buffer) = writer.write(::std::vec![42], &never).await;
                assert!(result.is_err());
                assert_eq!(buffer, [42]);
                writer.close().await.unwrap();
                drop(writer);
                scheduler.file_close(read_only).await.unwrap();
            },
            abandoned_resource_operation => |fixture, scheduler, file| {
                $crate::file_resources::abandonment(scheduler, &file).await;
            },
        }
    };
}

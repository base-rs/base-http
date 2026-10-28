// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Portable submission, independently owned receive, and pipeline checks.
#[macro_export]
macro_rules! file_submission_tests {
    (run: $run:path $(,)?) => {
        $crate::__file_test_cases! { $run;
            bound_file_submissions_separate_acceptance_from_owner_collection => |fixture, scheduler, file| {
                use $crate::__base_scheduler::{FileReader as _, FileReading as _, FileWriter as _, FileWriting as _};
                let never = $crate::__base_scheduler::Cancellation::never();
                let mut reader = scheduler.file_reader(&file).unwrap();
                let mut writer = scheduler.file_writer(&file).unwrap();
                let storage = ::std::vec![0; 3];
                let pointer = storage.as_ptr();
                let completion = reader.read_submit(storage, &never).await.unwrap();
                let (result, storage) = completion.await;
                assert_eq!(result.unwrap(), 3);
                assert_eq!(storage, b"abc");
                assert_eq!(storage.as_ptr(), pointer);
                let (result, storage) = reader.read_at_submit(storage, 3, &never).await.unwrap().await;
                assert_eq!(result.unwrap(), 3);
                assert_eq!(storage, b"def");
                let (result, storage) = writer.write_at_submit(storage, 0, &never).await.unwrap().await;
                assert_eq!(result.unwrap(), 3);
                assert_eq!(storage.as_ptr(), pointer);
                scheduler.file_seek(&file, ::std::io::SeekFrom::Start(3), &never).await.unwrap();
                let (result, storage) = writer.write_submit(storage, &never).await.unwrap().await;
                assert_eq!(result.unwrap(), 3);
                assert_eq!(storage.as_ptr(), pointer);
                fixture.assert_contents(b"defdef");
                reader.close().await.unwrap();
                writer.close().await.unwrap();
            },
            submission_is_lazy_and_pre_cancelled_admission_returns_the_owner => |fixture, scheduler, file| {
                use $crate::__base_scheduler::{FileReader as _, FileReading as _, FileWriter as _, FileWriting as _};
                let mut reader = scheduler.file_reader(&file).unwrap();
                let mut writer = scheduler.file_writer(&file).unwrap();
                let never = $crate::__base_scheduler::Cancellation::never();
                let cancelled = $crate::__base_scheduler::Cancellation::new();
                cancelled.cancel();
                drop(reader.read_submit(::std::vec![0;3], &never));
                drop(writer.write_submit(::std::vec![0;3], &never));
                drop(reader.read_at_submit(::std::vec![0;3], 0, &never));
                drop(writer.write_at_submit(::std::vec![0;3], 0, &never));
                let (error, buffer) = reader.read_submit(::std::vec![7;3], &cancelled)
                    .await
                    .err()
                    .unwrap();
                assert!(matches!(
                    error,
                    $crate::__base_scheduler::OperationError::Cancelled(_)
                ));
                assert_eq!(buffer, [7; 3]);
                assert!(
                    writer.write_submit(::std::vec![7;3], &cancelled)
                        .await
                        .is_err()
                );
                assert!(
                    reader.read_at_submit(::std::vec![7;3], 0, &cancelled)
                        .await
                        .is_err()
                );
                assert!(
                    writer.write_at_submit(::std::vec![7;3], 0, &cancelled)
                        .await
                        .is_err()
                );
                fixture.assert_contents(b"abcdef");
                            reader.close().await.unwrap();
                writer.close().await.unwrap();
            },
            submitted_reads_and_writes_return_the_original_storage => |fixture, scheduler, file| {
                use $crate::__base_scheduler::{FileReader as _, FileReading as _, FileWriter as _, FileWriting as _};
                let mut reader = scheduler.file_reader(&file).unwrap();
                let mut writer = scheduler.file_writer(&file).unwrap();
                let never = $crate::__base_scheduler::Cancellation::never();
                let buffer = ::std::vec![0;3];
                let pointer = buffer.as_ptr();
                let completion = reader.read_submit(buffer, &never)
                    .await
                    .unwrap();
                let (result, buffer) = completion.await;
                assert_eq!(result.unwrap(), 3);
                assert_eq!(buffer, b"abc");
                assert_eq!(buffer.as_ptr(), pointer);
                let (result, buffer) = reader.read_at_submit(buffer, 3, &never)
                    .await
                    .unwrap()
                    .await;
                assert_eq!(result.unwrap(), 3);
                assert_eq!(buffer, b"def");
                let (result, buffer) = writer.write_at_submit(buffer, 0, &never)
                    .await
                    .unwrap()
                    .await;
                assert_eq!(result.unwrap(), 3);
                scheduler
                    .file_seek(&file, ::std::io::SeekFrom::Start(3), &never)
                    .await
                    .unwrap();
                let (result, _) = writer.write_submit(buffer, &never)
                    .await
                    .unwrap()
                    .await;
                assert_eq!(result.unwrap(), 3);
                fixture.assert_contents(b"defdef");
                for reading in [false, true] {
                    let result = if reading {
                        match reader.read_at_submit(::std::vec![0;1], u64::MAX, &never)
                            .await
                        {
                            Ok(completion) => completion.await.0,
                            Err((error, _)) => Err(error),
                        }
                    } else {
                        match writer.write_at_submit(::std::vec![0;1], u64::MAX, &never)
                            .await
                        {
                            Ok(completion) => completion.await.0,
                            Err((error, _)) => Err(error),
                        }
                    };
                    assert_eq!(
                        result.unwrap_err().kind(),
                        ::std::io::ErrorKind::InvalidInput
                    );
                }
                            reader.close().await.unwrap();
                writer.close().await.unwrap();
            },
            accepted_write_progresses_without_polling_its_completion => |fixture, scheduler, file| {
                use $crate::__base_scheduler::{FileReader as _, FileReading as _, FileWriter as _, FileWriting as _};
                let mut reader = scheduler.file_reader(&file).unwrap();
                let mut writer = scheduler.file_writer(&file).unwrap();
                let never = $crate::__base_scheduler::Cancellation::never();
                let completion = writer.write_at_submit(b"UVWXYZ".to_vec(), 0, &never)
                    .await
                    .unwrap();
                let deadline = ::std::time::Instant::now() + ::std::time::Duration::from_secs(5);
                loop {
                    let (result, buffer) = reader.read_at(::std::vec![0;6], 0, &never)
                        .await;
                    assert_eq!(result.unwrap(), 6);
                    if buffer == b"UVWXYZ" {
                        break;
                    }
                    assert!(
                        ::std::time::Instant::now() < deadline,
                        "accepted write never progressed"
                    );
                }
                let (result, _) = completion.await;
                assert_eq!(result.unwrap(), 6);
                            reader.close().await.unwrap();
                writer.close().await.unwrap();
            },
            close_retires_accepted_work_before_its_completion_is_collected => |fixture, scheduler, file| {
                use $crate::__base_scheduler::{FileWriter as _, FileWriting as _};
                let never = $crate::__base_scheduler::Cancellation::never();
                let mut writer = scheduler.file_writer(&file).unwrap();
                let buffer = b"UVWXYZ".to_vec();
                let pointer = buffer.as_ptr();
                let completion = writer.write_at_submit(buffer, 0, &never).await.unwrap();
                writer.close().await.unwrap();
                let (result, buffer) = completion.await;
                assert_eq!(result.unwrap(), 6);
                assert_eq!(buffer.as_ptr(), pointer);
                fixture.assert_contents(b"UVWXYZ");
            },
            dropping_an_unpolled_accepted_completion_releases_only_after_retirement => |fixture, scheduler, file| {
                use $crate::__base_scheduler::{FileReader as _, FileReading as _, FileWriter as _, FileWriting as _};
                let mut reader = scheduler.file_reader(&file).unwrap();
                let mut writer = scheduler.file_writer(&file).unwrap();
                let never = $crate::__base_scheduler::Cancellation::never();
                let pool = $crate::__base_scheduler::HeapBufferPool::new(1, 6).unwrap();
                let buffer = pool.try_acquire().unwrap();
                let completion = writer.write_at_submit(buffer, 0, &never)
                    .await
                    .unwrap();
                assert!(pool.try_acquire().is_none());
                drop(completion);
                let returned = pool.acquire(&never).await.unwrap();
                drop(returned);
                            reader.close().await.unwrap();
                writer.close().await.unwrap();
            },
        }
    };
}

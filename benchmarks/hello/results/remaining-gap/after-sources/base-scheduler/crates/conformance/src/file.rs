// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Assertions for independently implementable file capabilities. Fixtures supply regular files with exclusive test ownership and a cursor initially at zero unless documented otherwise.
use base_scheduler::{
    Cancellation, FileMetadata, FileReading, FileSeek, FileStat, FileSync, FileTruncate, FileType,
    FileWriting, IoBuf, OperationError, SyncMode,
};
use std::io::{self, SeekFrom};

pub(crate) fn cancelled<T>(result: Result<T, OperationError>) {
    assert!(
        matches!(result, Err(OperationError::Cancelled(_))),
        "expected cooperative cancellation"
    );
}

/// Check lazy, pre-cancelled, short, EOF, and owned-buffer reads of a known complete file.
pub async fn read<S: FileReading>(scheduler: &S, file: &S::File, expected: &[u8]) {
    let cancellation = Cancellation::new();
    cancellation.cancel();
    let buffer = vec![0; 2];
    let allocation = buffer.as_ptr() as usize;
    let (result, buffer) = crate::io::file_read(scheduler, file, buffer, &cancellation).await;
    cancelled(result);
    assert_eq!(buffer.as_ptr() as usize, allocation);
    let never = Cancellation::never();
    drop(crate::io::file_read(scheduler, file, vec![0; 2], &never));
    let mut actual = Vec::new();
    let mut buffer = buffer;
    loop {
        let (result, returned) = crate::io::file_read(scheduler, file, buffer, &never).await;
        let count = result.unwrap();
        assert_eq!(returned.as_ptr() as usize, allocation);
        assert!(count <= returned.len());
        actual.extend_from_slice(&returned[..count]);
        buffer = returned;
        assert!(actual.len() <= expected.len());
        if count == 0 {
            break;
        }
    }
    assert_eq!(actual, expected);
}

/// Write all supplied bytes, respecting short writes and returning the original allocation. The fixture verifies the resulting contents separately.
pub async fn write<S: FileWriting>(scheduler: &S, file: &S::File, bytes: &[u8]) {
    let cancellation = Cancellation::new();
    cancellation.cancel();
    let buffer = bytes.to_vec();
    let allocation = buffer.as_ptr() as usize;
    let (result, mut buffer) = crate::io::file_write(scheduler, file, buffer, &cancellation).await;
    cancelled(result);
    assert_eq!(buffer.as_ptr() as usize, allocation);
    let never = Cancellation::never();
    drop(crate::io::file_write(
        scheduler,
        file,
        bytes.to_vec(),
        &never,
    ));
    let mut offset = 0;
    while offset < bytes.len() {
        let (result, view) =
            crate::io::file_write(scheduler, file, buffer.slice(offset..), &never).await;
        let count = result.unwrap();
        assert!(count > 0 && count <= bytes.len() - offset);
        offset += count;
        buffer = view.into_inner();
        assert_eq!(buffer.as_ptr() as usize, allocation);
    }
}

/// Check positioned reads and range validation without assuming shared-cursor behavior. The fixture contains `expected` at `offset`.
pub async fn read_at<S: FileReading>(scheduler: &S, file: &S::File, offset: u64, expected: &[u8]) {
    let cancelled_token = Cancellation::new();
    cancelled_token.cancel();
    let (result, _) =
        crate::io::file_read_at(scheduler, file, vec![0; 2], u64::MAX, &cancelled_token).await;
    cancelled(result);
    let never = Cancellation::never();
    let buffer = vec![0; expected.len()];
    let allocation = buffer.as_ptr() as usize;
    let (result, mut buffer) =
        crate::io::file_read_at(scheduler, file, buffer, u64::MAX, &never).await;
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    assert_eq!(buffer.as_ptr() as usize, allocation);
    let mut read = 0;
    while read < expected.len() {
        let (result, view) = crate::io::file_read_at(
            scheduler,
            file,
            buffer.slice(read..),
            offset + read as u64,
            &never,
        )
        .await;
        let count = result.unwrap();
        assert!(count > 0 && count <= expected.len() - read);
        read += count;
        buffer = view.into_inner();
    }
    assert_eq!(buffer, expected);
    assert_eq!(buffer.as_ptr() as usize, allocation);
}

/// Check positioned writes and range validation. The fixture verifies resulting contents separately.
pub async fn write_at<S: FileWriting>(scheduler: &S, file: &S::File, offset: u64, bytes: &[u8]) {
    let cancellation = Cancellation::new();
    cancellation.cancel();
    let (result, _) =
        crate::io::file_write_at(scheduler, file, bytes.to_vec(), u64::MAX, &cancellation).await;
    cancelled(result);
    let never = Cancellation::never();
    let buffer = bytes.to_vec();
    let allocation = buffer.as_ptr() as usize;
    let (result, mut buffer) =
        crate::io::file_write_at(scheduler, file, buffer, u64::MAX, &never).await;
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    assert_eq!(buffer.as_ptr() as usize, allocation);
    let mut written = 0;
    while written < bytes.len() {
        let (result, view) = crate::io::file_write_at(
            scheduler,
            file,
            buffer.slice(written..),
            offset + written as u64,
            &never,
        )
        .await;
        let count = result.unwrap();
        assert!(count > 0 && count <= bytes.len() - written);
        written += count;
        buffer = view.into_inner();
    }
    assert_eq!(buffer.as_ptr() as usize, allocation);
}

/// Check cursor queries, relative/end seeks, laziness, and invalid absolute positions. Leaves the cursor at zero.
pub async fn seek<S: FileSeek>(scheduler: &S, file: &S::File, length: u64) {
    let never = Cancellation::never();
    let token = Cancellation::new();
    token.cancel();
    cancelled(scheduler.file_seek(file, SeekFrom::Start(1), &token).await);
    drop(scheduler.file_seek(file, SeekFrom::Start(1), &never));
    assert_eq!(
        scheduler
            .file_seek(file, SeekFrom::Current(0), &never)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        scheduler
            .file_seek(file, SeekFrom::End(0), &never)
            .await
            .unwrap(),
        length
    );
    assert_eq!(
        scheduler
            .file_seek(file, SeekFrom::Current(1), &never)
            .await
            .unwrap(),
        length + 1
    );
    assert_eq!(
        scheduler
            .file_seek(file, SeekFrom::Start(u64::MAX), &never)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        scheduler
            .file_seek(file, SeekFrom::Start(0), &never)
            .await
            .unwrap(),
        0
    );
}

/// Check optional metadata observations for a regular file of known length.
pub async fn metadata<S: FileStat>(scheduler: &S, file: &S::File, length: u64) {
    let token = Cancellation::new();
    token.cancel();
    cancelled(scheduler.file_stat(file, &token).await);
    let metadata = scheduler
        .file_stat(file, &Cancellation::never())
        .await
        .unwrap();
    assert!(metadata.size().is_none_or(|size| size == length));
    assert!(
        metadata
            .file_type()
            .is_none_or(|kind| kind == FileType::File)
    );
}

/// Check lazy and cancelled truncation, then resize to `length`. The fixture checks size and contents separately.
pub async fn truncate<S: FileTruncate>(scheduler: &S, file: &S::File, length: u64) {
    let token = Cancellation::new();
    token.cancel();
    cancelled(scheduler.file_truncate(file, u64::MAX, &token).await);
    let never = Cancellation::never();
    drop(scheduler.file_truncate(file, 0, &never));
    assert_eq!(
        scheduler
            .file_truncate(file, u64::MAX, &never)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    scheduler.file_truncate(file, length, &never).await.unwrap();
}

/// Check cancellation and both synchronization modes without asserting storage durability beyond the OS contract.
pub async fn sync<S: FileSync>(scheduler: &S, file: &S::File) {
    let token = Cancellation::new();
    token.cancel();
    for mode in [SyncMode::All, SyncMode::Data] {
        cancelled(scheduler.file_sync(file, mode, &token).await);
        scheduler
            .file_sync(file, mode, &Cancellation::never())
            .await
            .unwrap();
    }
}

/// Check a failed write preserves the original buffer owner on an error path supplied by the backend fixture.
pub async fn write_error<S: FileWriting>(scheduler: &S, file: &S::File) {
    let buffer = vec![7; 3];
    let allocation = buffer.as_ptr() as usize;
    let (result, buffer) =
        crate::io::file_write(scheduler, file, buffer, &Cancellation::never()).await;
    assert!(matches!(result, Err(OperationError::Io(_))));
    assert_eq!(buffer, vec![7; 3]);
    assert_eq!(buffer.as_ptr() as usize, allocation);
}

/// Check lazy and pre-cancelled file creation using a backend-supplied absence assertion. Returns the newly created read/write file; the fixture owns cleanup.
pub async fn create<S: base_scheduler::FileOpen>(
    scheduler: &S,
    path: &std::path::Path,
    assert_absent: impl Fn(),
) -> S::File {
    let options = base_scheduler::FileOptions {
        read: true,
        write: true,
        create_new: true,
        ..base_scheduler::FileOptions::CREATE
    };
    let never = Cancellation::never();
    drop(scheduler.file_open(path, options, &never));
    assert_absent();
    let token = Cancellation::new();
    token.cancel();
    cancelled(scheduler.file_open(path, options, &token).await);
    assert_absent();
    scheduler.file_open(path, options, &never).await.unwrap()
}

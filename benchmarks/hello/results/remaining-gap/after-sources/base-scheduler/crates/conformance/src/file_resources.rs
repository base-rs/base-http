// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{Cancellation, FileSeek};
use base_scheduler::{FileReader, FileReading, FileWriter, FileWriting};
use std::{
    future::{Future, poll_fn},
    io::SeekFrom,
    task::Poll,
};

pub async fn sequential<S: FileReading + FileWriting + FileSeek>(scheduler: &S, file: &S::File) {
    let never = Cancellation::never();
    let mut reader = scheduler.file_reader(file).unwrap();
    let mut writer = scheduler.file_writer(file).unwrap();
    let mut buffer = vec![0; 2];
    for _ in 0..3 {
        let result;
        (result, buffer) = reader.read_at(buffer, 1, &never).await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(buffer, b"bc");
    }
    // Independent reader and writer resources support changing owner types while sharing the underlying file cursor.
    let (result, buffer) = writer
        .write_at(Box::<[u8]>::from(&b"XY"[..]), 2, &never)
        .await;
    assert_eq!(result.unwrap(), 2);
    assert_eq!(&*buffer, b"XY");
    let (result, buffer) = reader.read_at(vec![0; 8], 0, &never).await;
    assert_eq!(result.unwrap(), 6);
    assert_eq!(&buffer[..6], b"abXYef");
    assert_eq!(reader.read_at(buffer, 6, &never).await.0.unwrap(), 0);
    scheduler
        .file_seek(file, SeekFrom::Start(0), &never)
        .await
        .unwrap();
    assert_eq!(reader.read(vec![0; 2], &never).await.1, b"ab");
    assert_eq!(writer.write(b"12".to_vec(), &never).await.0.unwrap(), 2);
    assert_eq!(reader.read(vec![0; 2], &never).await.1, b"ef");
    reader.close().await.unwrap();
    writer.close().await.unwrap();
    reader.close().await.unwrap();
    assert!(reader.read(vec![0; 2], &never).await.0.is_err());
}

pub async fn independent<S: FileReading + FileWriting>(scheduler: &S, file: &S::File) {
    let never = Cancellation::never();
    let mut first = scheduler.file_reader(file).unwrap();
    let mut second = scheduler.file_reader(file).unwrap();
    let (a, b) = futures_lite::future::zip(
        first.read_at(vec![0; 3], 0, &never),
        second.read_at(vec![0; 3], 3, &never),
    )
    .await;
    assert_eq!(a.0.unwrap(), 3);
    assert_eq!(a.1, b"abc");
    assert_eq!(b.0.unwrap(), 3);
    assert_eq!(b.1, b"def");
    first.close().await.unwrap();
    second.close().await.unwrap();
}

pub async fn cancellation<S: FileReading + FileWriting>(scheduler: &S, file: &S::File) {
    let mut reader = scheduler.file_reader(file).unwrap();
    let mut writer = scheduler.file_writer(file).unwrap();
    let cancelled = Cancellation::new();
    cancelled.cancel();
    let never = Cancellation::never();
    for error in [
        reader.read(vec![42], &cancelled).await.0.unwrap_err(),
        writer.write(vec![42], &cancelled).await.0.unwrap_err(),
        reader.read_at(vec![42], 0, &cancelled).await.0.unwrap_err(),
        writer
            .write_at(vec![42], 0, &cancelled)
            .await
            .0
            .unwrap_err(),
    ] {
        crate::file::cancelled::<()>(Err(error));
    }
    assert!(reader.read_at(vec![0], u64::MAX, &never).await.0.is_err());
    assert!(writer.write_at(vec![42], u64::MAX, &never).await.0.is_err());
    // Constructing and dropping an unpolled future changes neither the cursor nor resource admission.
    drop(writer.write(vec![42], &never));
    assert_eq!(reader.read_at(vec![0], 0, &never).await.1, b"a");
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}

pub async fn abandonment<S: FileReading + FileWriting>(scheduler: &S, file: &S::File) {
    let never = Cancellation::never();
    let mut reader = scheduler.file_reader(file).unwrap();
    {
        let mut operation = Box::pin(reader.read_at(vec![0; 6], 0, &never));
        // Either immediate completion or pending is valid; close must establish retirement in both cases.
        poll_fn(|context| {
            let _ = operation.as_mut().poll(context);
            Poll::Ready(())
        })
        .await;
    }
    reader.close().await.unwrap();
}

// Stop inside payload access, on the independent file worker or ring driver, so lifecycle assertions cannot accidentally test an already-completed operation.
struct GatedBuffer {
    bytes: Vec<u8>,
    started: Option<std::sync::mpsc::Sender<()>>,
    release: std::sync::mpsc::Receiver<()>,
}
impl AsRef<[u8]> for GatedBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsMut<[u8]> for GatedBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        if let Some(started) = self.started.take() {
            started.send(()).unwrap();
            self.release
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
        }
        &mut self.bytes
    }
}
// SAFETY: The Vec owns stable initialized storage, with exclusive access through the transferred owner. Gating never changes its allocation or range.
unsafe impl base_scheduler::IoBuf for GatedBuffer {}
unsafe impl base_scheduler::IoBufMut for GatedBuffer {}

pub async fn pending_retirement<S: FileReading + FileWriting>(scheduler: &S, file: &S::File) {
    let never = Cancellation::never();
    let mut reader = scheduler.file_reader(file).unwrap();
    let (started, wait_started) = std::sync::mpsc::channel();
    let (release, wait_release) = std::sync::mpsc::channel();
    let buffer = GatedBuffer {
        bytes: vec![0; 6],
        started: Some(started),
        release: wait_release,
    };
    let mut operation = Box::pin(reader.read_at(buffer, 0, &never));
    assert!(
        futures_lite::future::poll_once(operation.as_mut())
            .await
            .is_none()
    );
    wait_started
        .recv_timeout(std::time::Duration::from_secs(10))
        .unwrap();
    drop(operation);
    let error = reader.read_at(vec![0], 0, &never).await.0.unwrap_err();
    assert_eq!(
        std::io::Error::from(error).kind(),
        std::io::ErrorKind::WouldBlock
    );
    let mut close = Box::pin(reader.close());
    assert!(
        futures_lite::future::poll_once(close.as_mut())
            .await
            .is_none()
    );
    // Abandoning a pending cleanup does not reopen admission or establish retirement.
    drop(close);
    let (result, _) = reader.read_at(vec![0], 0, &never).await;
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::BrokenPipe);
    release.send(()).unwrap();
    reader.close().await.unwrap();
    // Dropping the retired resource here leaves the caller free to immediately close the original file.
}

pub async fn inflight_cancellation<S: FileReading + FileWriting>(scheduler: &S, file: &S::File) {
    let cancellation = Cancellation::new();
    let mut reader = scheduler.file_reader(file).unwrap();
    let (started, wait_started) = std::sync::mpsc::channel();
    let (release, wait_release) = std::sync::mpsc::channel();
    let buffer = GatedBuffer {
        bytes: vec![0; 6],
        started: Some(started),
        release: wait_release,
    };
    let allocation = buffer.as_ref().as_ptr() as usize;
    let mut operation = Box::pin(reader.read_at(buffer, 0, &cancellation));
    assert!(
        futures_lite::future::poll_once(operation.as_mut())
            .await
            .is_none()
    );
    wait_started
        .recv_timeout(std::time::Duration::from_secs(10))
        .unwrap();
    cancellation.cancel();
    release.send(()).unwrap();
    let (_, mut buffer) = operation.await;
    // Either completed progress or cancellation may win, but ownership and the original stable allocation must always return after retirement.
    assert_eq!(buffer.as_ref().as_ptr() as usize, allocation);
    assert_eq!(buffer.as_mut().len(), 6);
    reader.close().await.unwrap();
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::*;
use futures_lite::future::block_on;
use std::{io, sync::Mutex};

// Protocols can consume a bound read-only resource without requiring any scheduler operations.
struct ReadOnly<'a>(&'a Mutex<&'static [u8]>);
impl FileReader for ReadOnly<'_> {
    async fn read<B: IoBufMut>(
        &mut self,
        mut buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffer);
        }
        let mut remaining = self.0.lock().unwrap();
        let count = remaining.len().min(buffer.as_mut().len());
        buffer.as_mut()[..count].copy_from_slice(&remaining[..count]);
        *remaining = &remaining[count..];
        (Ok(count), buffer)
    }
    async fn read_at<B: IoBufMut>(
        &mut self,
        buffer: B,
        _: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.read(buffer, cancellation).await
    }
    async fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}
async fn read_chunk<R: FileReader>(
    reader: &mut R,
    buffer: Vec<u8>,
    cancellation: &Cancellation,
) -> BufferResult {
    reader.read(buffer, cancellation).await
}

#[test]
fn read_only_resource_reuses_owned_storage_without_allocating() {
    let file = Mutex::new(b"abc".as_slice());
    let mut reader = ReadOnly(&file);
    let token = Cancellation::new();
    let buffer = vec![0; 2];
    let pointer = buffer.as_ptr();
    drop(file.lock().unwrap());
    block_on(async {});
    let allocations = allocation_counter::measure(|| {
        let mut buffer = buffer;
        for _ in 0..16 {
            *file.lock().unwrap() = b"abc";
            let (result, returned) = block_on(read_chunk(&mut reader, buffer, &token));
            assert_eq!(result.unwrap(), 2);
            buffer = returned;
            assert_eq!(buffer, b"ab");
            assert_eq!(buffer.as_ptr(), pointer);
        }
    });
    assert_eq!(allocations.count_total, 0);
}

#[test]
fn read_only_resource_satisfies_a_read_only_consumer() {
    block_on(async {
        let file = Mutex::new(b"abc".as_slice());
        let mut reader = ReadOnly(&file);
        let buffer = vec![0; 2];
        let allocation = buffer.as_ptr();
        let never = Cancellation::never();
        let (result, buffer) = read_chunk(&mut reader, buffer, &never).await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(buffer, b"ab");
        let cancelled = Cancellation::new();
        cancelled.cancel();
        let (result, buffer) = read_chunk(&mut reader, buffer, &cancelled).await;
        assert!(matches!(result, Err(OperationError::Cancelled(_))));
        assert_eq!(buffer, b"ab");
        let (result, buffer) = read_chunk(&mut reader, buffer, &never).await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer, b"cb");
        let (result, buffer) = read_chunk(&mut reader, buffer, &never).await;
        assert_eq!(result.unwrap(), 0);
        assert_eq!(buffer.as_ptr(), allocation);
    });
}

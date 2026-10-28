// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Ordered output assertions shared by completion and readiness backends.
use crate::SocketTestScheduler;
use base_scheduler::*;
use futures_lite::future::{poll_once, zip};
use std::io;

/// Empty writes, sequential ordering, pre-cancelled admission, flush, and repeatable cleanup.
pub async fn ordering<S>(scheduler: &S, sender: &S::Socket, receiver: &S::Socket)
where
    S: SocketStreamWriting + SocketTestScheduler + Sync,
    S::Socket: 'static,
{
    let never = Cancellation::never();
    let mut writer = scheduler
        .socket_stream_writer_with_buffer::<Vec<u8>>(sender)
        .unwrap();
    drop(writer.submit(b"unpolled".to_vec(), &never));
    let cancellation = Cancellation::new();
    cancellation.cancel();
    let rejected = b"rejected".to_vec();
    let pointer = rejected.as_ptr();
    let (error, returned) = writer.submit(rejected, &cancellation).await.unwrap_err();
    crate::socket_resources::cancelled::<()>(Err(error));
    assert_eq!(returned.as_ptr(), pointer);
    assert_eq!(returned, b"rejected");
    let ((), ()) = zip(
        async {
            for value in [b"".as_slice(), b"header", b"", b"body", b"tail"] {
                writer
                    .submit(value.to_vec(), &never)
                    .await
                    .map_err(|(error, _)| error)
                    .unwrap();
            }
            writer.flush(&never).await.unwrap();
        },
        receive(scheduler, receiver, b"headerbodytail"),
    )
    .await;
    writer.close().await.unwrap();
    writer.close().await.unwrap();
    assert!(writer.flush(&never).await.is_err());
    let (error, returned) = writer.submit(b"closed".to_vec(), &never).await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(returned, b"closed");
}

/// An accepted large write and its partial retries progress without polling the writer. Its pooled owner is returned without calling flush.
pub async fn independent_progress<S>(scheduler: &S, sender: &S::Socket, receiver: &S::Socket)
where
    S: SocketStreamWriting + SocketTestScheduler + Sync,
    S::Socket: 'static,
{
    let never = Cancellation::never();
    for size in [8, 8 * 1024 * 1024] {
        let pool = HeapBufferPool::new(1, size).unwrap();
        let mut buffer = pool.acquire(&never).await.unwrap();
        buffer.as_mut().fill(0x5a);
        let mut writer = scheduler.socket_stream_writer_with_buffer(sender).unwrap();
        writer
            .submit(buffer, &never)
            .await
            .map_err(|(error, _)| error)
            .unwrap();

        // Cancelling a flush wait does not cancel accepted output.
        let cancellation = Cancellation::new();
        cancellation.cancel();
        let result = writer.flush(&cancellation).await;
        completed_or_cancelled(result);
        let mut incoming = vec![0; 64 * 1024];
        let mut received = 0;
        while received < pool.buffer_size() {
            let length = crate::io::socket_recv_poll(
                scheduler,
                receiver,
                &mut incoming,
                SocketRecvFlags::default(),
                &never,
            )
            .await
            .unwrap();
            assert!(length > 0);
            assert!(incoming[..length].iter().all(|byte| *byte == 0x5a));
            received += length;
        }
        assert_eq!(received, pool.buffer_size());
        // Waiting for pool capacity, rather than a writer method, also proves ownership is released autonomously.
        let returned = pool.acquire(&never).await.unwrap();
        drop(returned);
        writer.flush(&never).await.unwrap();
        writer.close().await.unwrap();
    }
}

/// Accepted output progresses independently while the reader receives the next request. Two shared slots bound the input/output pipeline without implicit receive-ahead.
pub async fn overlapped_read_write<S>(scheduler: &S, sender: &S::Socket, receiver: &S::Socket)
where
    S: SocketStreamWriting + SocketTestScheduler + Sync,
    S::Socket: 'static,
{
    let never = Cancellation::never();
    let pool = scheduler
        .buffer_pool(BufferPoolOptions::new(2, 8 * 1024 * 1024))
        .unwrap();
    let mut reader = scheduler
        .socket_stream_reader_with_pool(receiver, pool.clone())
        .unwrap();
    let mut writer = scheduler.socket_stream_writer(receiver).unwrap();
    let mut peer_reader = scheduler.socket_stream_reader(sender).unwrap();
    let mut peer_writer = scheduler.socket_stream_writer(sender).unwrap();

    peer_writer.write(b"a", &never).await.unwrap();
    let first = reader.read(&never).await.unwrap().unwrap();
    assert_eq!(first.as_ref(), b"a");
    drop(first);

    let mut response = pool.acquire(&never).await.unwrap();
    response.as_mut().fill(0x5a);
    writer
        .submit(response, &never)
        .await
        .map_err(|(error, _)| error)
        .unwrap();
    // Kernel buffering can complete the response immediately. Otherwise, abandoning this flush wait leaves accepted output running.
    let mut flush = Box::pin(writer.flush(&never));
    let _ = poll_once(flush.as_mut()).await.transpose().unwrap();
    drop(flush);

    peer_writer.write(b"b", &never).await.unwrap();
    let next = reader.read(&never).await.unwrap().unwrap();
    assert_eq!(next.as_ref(), b"b");
    drop(next);

    // Only the peer is polled while the response drains; no writer method is needed to advance or release its owner.
    let mut bytes = vec![0; 64 * 1024];
    let mut remaining = pool.buffer_size();
    while remaining > 0 {
        let count = peer_reader.read_into(&mut bytes, &never).await.unwrap();
        assert!(count > 0 && count <= remaining);
        assert!(bytes[..count].iter().all(|byte| *byte == 0x5a));
        remaining -= count;
    }
    let first = pool.acquire(&never).await.unwrap();
    let second = pool.acquire(&never).await.unwrap();
    assert!(pool.try_acquire().is_none());
    drop((first, second));

    writer.flush(&never).await.unwrap();
    reader.close().await.unwrap();
    writer.close().await.unwrap();
    peer_reader.close().await.unwrap();
    peer_writer.close().await.unwrap();
}

/// Abandoning a later admission wait leaves the first accepted write intact.
pub async fn abandoned_admission<S>(scheduler: &S, sender: &S::Socket, receiver: &S::Socket)
where
    S: SocketStreamWriting + SocketTestScheduler + Sync,
    S::Socket: 'static,
{
    let never = Cancellation::never();
    for size in [8, 8 * 1024 * 1024] {
        let expected = vec![0x33; size];
        let mut writer = scheduler.socket_stream_writer_with_buffer(sender).unwrap();
        writer
            .submit(expected.clone(), &never)
            .await
            .map_err(|(error, _)| error)
            .unwrap();
        let mut next = Box::pin(writer.submit(b"tail".to_vec(), &never));
        let accepted = poll_once(next.as_mut()).await;
        // A platform may have buffered the whole first write already. Account for the second admission if it completed synchronously.
        let appended = accepted
            .map(|result| {
                result.unwrap();
                b"tail".as_slice()
            })
            .unwrap_or_default();
        drop(next);
        receive(scheduler, receiver, &expected).await;
        receive(scheduler, receiver, appended).await;
        writer.flush(&never).await.unwrap();
        writer.close().await.unwrap();
    }
}

/// Cleanup can cancel blocked output without a peer consuming it, and releases retained pool storage.
pub async fn close_pending<S>(scheduler: &S, sender: &S::Socket)
where
    S: SocketStreamWriting + Sync,
    S::Socket: 'static,
{
    let never = Cancellation::never();
    let pool = HeapBufferPool::new(1, 8 * 1024 * 1024).unwrap();
    let mut writer = scheduler.socket_stream_writer_with_buffer(sender).unwrap();
    writer
        .submit(pool.acquire(&never).await.unwrap(), &never)
        .await
        .map_err(|(error, _)| error)
        .unwrap();
    drop(writer.close());
    writer.close().await.unwrap();
    writer.close().await.unwrap();
    assert!(pool.try_acquire().is_some());
}

async fn receive<S: SocketTestScheduler>(scheduler: &S, socket: &S::Socket, expected: &[u8]) {
    let mut buffer = vec![0; expected.len().min(64 * 1024)];
    let mut received = 0;
    while received < expected.len() {
        let capacity = buffer.len().min(expected.len() - received);
        let count = crate::io::socket_recv_poll(
            scheduler,
            socket,
            &mut buffer[..capacity],
            SocketRecvFlags::default(),
            &Cancellation::never(),
        )
        .await
        .unwrap();
        assert!(count > 0);
        assert_eq!(&buffer[..count], &expected[received..received + count]);
        received += count;
    }
}

fn completed_or_cancelled(result: OperationResult<()>) {
    assert!(matches!(result, Ok(()) | Err(OperationError::Cancelled(_))));
}

/// Borrowed scalar/vector I/O, owned input, and cancellation all reuse scheduler-bound state. The same input lane can alternate owned chunks and caller storage without copying already received payloads.
pub async fn bound_io<S>(scheduler: &S, sender: &S::Socket, receiver: &S::Socket)
where
    S: SocketStreamWriting + SocketStreamReading + SocketShutdown,
    S::Socket: 'static,
{
    use std::io::{IoSlice, IoSliceMut};
    let never = Cancellation::never();
    let mut writer = scheduler.socket_stream_writer(sender).unwrap();
    let pool = scheduler.buffer_pool(BufferPoolOptions::new(2, 8)).unwrap();
    let mut reader = scheduler
        .socket_stream_reader_with_pool(receiver, pool)
        .unwrap();
    let cancelled = Cancellation::new();
    let mut storage = [0; 8];
    {
        let mut reading = std::pin::pin!(reader.read_into(&mut storage, &cancelled));
        assert!(poll_once(reading.as_mut()).await.is_none());
        cancelled.cancel();
        crate::socket_resources::cancelled(reading.await);
    }
    assert_eq!(reader.read_into(&mut [], &never).await.unwrap(), 0);
    assert_eq!(reader.read_vectored(&mut [], &never).await.unwrap(), 0);
    assert_eq!(
        reader
            .read_vectored(&mut [IoSliceMut::new(&mut [])], &never)
            .await
            .unwrap(),
        0
    );
    crate::socket_resources::cancelled(reader.read_into(&mut [], &cancelled).await);
    crate::socket_resources::cancelled(reader.read_vectored(&mut [], &cancelled).await);
    let (count, ()) = zip(
        async {
            let mut received = 0;
            while received < 4 {
                let (first, second) = storage[received..4].split_at_mut(1);
                let n = reader
                    .read_vectored(
                        &mut [IoSliceMut::new(first), IoSliceMut::new(second)],
                        &never,
                    )
                    .await
                    .unwrap();
                assert!(n > 0);
                received += n;
            }
            received
        },
        async {
            let count = writer
                .write_vectored(&[IoSlice::new(b"ab"), IoSlice::new(b"cd")], &never)
                .await
                .unwrap();
            assert!(count > 0 && count <= 4);
            let pool = scheduler.buffer_pool(BufferPoolOptions::new(1, 4)).unwrap();
            let mut suffix = pool.acquire(&never).await.unwrap();
            suffix.as_mut()[..4 - count].copy_from_slice(&b"abcd"[count..]);
            suffix.truncate(4 - count);
            writer
                .submit(suffix, &never)
                .await
                .map_err(|(error, _)| error)
                .unwrap();
        },
    )
    .await;
    assert_eq!(count, 4);
    assert_eq!(&storage[..4], b"abcd");
    // Acceptance precedes the owned read. The default pooled reader and writer agree on owner representation.
    let pool = scheduler.buffer_pool(BufferPoolOptions::new(1, 1)).unwrap();
    let mut payload = pool.acquire(&never).await.unwrap();
    payload.as_mut()[0] = b'x';
    writer
        .submit(payload, &never)
        .await
        .map_err(|(e, _)| e)
        .unwrap();
    let chunk = reader.read(&never).await.unwrap().unwrap();
    assert_eq!(chunk.as_ref(), b"x");
    writer.write(b"y", &never).await.unwrap();
    assert_eq!(
        reader.read_into(&mut storage[..1], &never).await.unwrap(),
        1
    );
    assert_eq!(storage[0], b'y');
    writer.flush(&never).await.unwrap();
    writer.close().await.unwrap();
    drop(writer);
    scheduler
        .socket_shutdown(sender, std::net::Shutdown::Write)
        .await
        .unwrap();
    assert!(reader.read(&never).await.unwrap().is_none());
    assert_eq!(reader.read_into(&mut storage, &never).await.unwrap(), 0);
    reader.close().await.unwrap();
    drop(reader);
    assert_eq!(chunk.as_ref(), b"x");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flush_accepts_completion_or_cancellation_and_rejects_other_errors() {
        completed_or_cancelled(Ok(()));
        completed_or_cancelled(Err(Cancelled.into()));
        assert!(
            std::panic::catch_unwind(|| {
                completed_or_cancelled(
                    Err(io::Error::from(io::ErrorKind::PermissionDenied).into()),
                );
            })
            .is_err()
        );
    }
}

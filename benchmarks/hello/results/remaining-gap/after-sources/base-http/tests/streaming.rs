// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::body::{BodyError, BodyReader, ReadableBody};
use base_scheduler::Cancellation;
use futures_lite::future::block_on;
use std::{
    future::Future,
    pin::{Pin, pin},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

#[derive(Default)]
struct WakeCount(AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn poll<F: Future>(future: Pin<&mut F>, wakes: &Arc<WakeCount>) -> Poll<F::Output> {
    let waker = Waker::from(Arc::clone(wakes));
    future.poll(&mut Context::from_waker(&waker))
}
fn require_send<T: Send>(value: T) -> T {
    value
}

#[test]
fn writable_preserves_empty_chunks_and_declared_representation_length() {
    block_on(async {
        let cancellation = Cancellation::never();
        let (mut body, mut writer) = BodyReader::with_length(8, 3, 5);
        assert!(!body.is_ready());
        assert!(!body.is_empty());
        writer.write(Vec::new(), &cancellation).await.unwrap();
        writer
            .write(b"hello".to_vec(), &cancellation)
            .await
            .unwrap();
        assert!(body.is_ready());
        writer.close_write(None);
        drop(writer);
        assert_eq!(body.read(&cancellation).await.unwrap(), Some(Vec::new()));
        assert_eq!(
            body.read(&cancellation).await.unwrap(),
            Some(b"hello".to_vec())
        );
        assert!(body.is_ready());
        assert!(body.is_empty());
        assert_eq!(body.read(&cancellation).await.unwrap(), None);
        assert_eq!(body.length(), Some(5));
    });
}

#[test]
fn byte_capacity_blocks_until_a_consumer_releases_space() {
    let cancellation = Cancellation::never();
    let (mut body, mut writer) = BodyReader::bounded(4, 8);
    block_on(writer.write(b"abc".to_vec(), &cancellation)).unwrap();
    let wakes = Arc::new(WakeCount::default());
    let mut write = pin!(require_send(writer.write(b"de".to_vec(), &cancellation)));
    assert!(poll(write.as_mut(), &wakes).is_pending());
    assert_eq!(
        block_on(body.read(&cancellation)).unwrap(),
        Some(b"abc".to_vec())
    );
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(poll(write.as_mut(), &wakes), Poll::Ready(Ok(()))));
    assert_eq!(
        block_on(body.read(&cancellation)).unwrap(),
        Some(b"de".to_vec())
    );
}

#[test]
fn chunk_capacity_also_bounds_empty_chunks() {
    let cancellation = Cancellation::never();
    let (mut body, mut writer) = BodyReader::bounded(32, 1);
    block_on(writer.write(Vec::new(), &cancellation)).unwrap();
    let wakes = Arc::new(WakeCount::default());
    let mut write = pin!(writer.write(Vec::new(), &cancellation));
    assert!(poll(write.as_mut(), &wakes).is_pending());
    assert_eq!(
        block_on(body.read(&cancellation)).unwrap(),
        Some(Vec::new())
    );
    assert!(matches!(poll(write.as_mut(), &wakes), Poll::Ready(Ok(()))));
}

#[test]
fn oversized_chunks_are_rejected_without_poisoning_output() {
    block_on(async {
        let cancellation = Cancellation::never();
        let (mut body, mut writer) = BodyReader::bounded(2, 1);
        assert!(matches!(
            writer.write(vec![0; 3], &cancellation).await,
            Err(BodyError::ChunkTooLarge {
                length: 3,
                limit: 2
            })
        ));
        writer.write(vec![1; 2], &cancellation).await.unwrap();
        assert_eq!(body.read(&cancellation).await.unwrap(), Some(vec![1; 2]));
    });
}

#[test]
fn cancellation_wakes_a_pending_read_without_consuming_or_closing_input() {
    let cancellation = Cancellation::new();
    let (mut body, mut writer) = BodyReader::bounded(8, 1);
    let wakes = Arc::new(WakeCount::default());
    {
        let mut read = pin!(require_send(body.read(&cancellation)));
        assert!(poll(read.as_mut(), &wakes).is_pending());
        cancellation.cancel();
        assert!(wakes.0.load(Ordering::SeqCst) > 0);
        assert!(matches!(
            poll(read.as_mut(), &wakes),
            Poll::Ready(Err(BodyError::Cancelled))
        ));
    }
    block_on(writer.write(b"still ok".to_vec(), &Cancellation::never())).unwrap();
    assert_eq!(
        block_on(body.read(&Cancellation::never())).unwrap(),
        Some(b"still ok".to_vec())
    );
}

#[test]
fn cancelled_and_dropped_writes_never_enqueue_their_pending_chunk() {
    let cancellation = Cancellation::new();
    let (mut body, mut writer) = BodyReader::bounded(8, 1);
    block_on(writer.write(vec![1], &Cancellation::never())).unwrap();
    let wakes = Arc::new(WakeCount::default());
    {
        let mut write = pin!(writer.write(vec![2], &cancellation));
        assert!(poll(write.as_mut(), &wakes).is_pending());
        cancellation.cancel();
        assert!(matches!(
            poll(write.as_mut(), &wakes),
            Poll::Ready(Err(BodyError::Cancelled))
        ));
    }
    drop(writer.write(vec![3], &Cancellation::never()));
    assert_eq!(
        block_on(body.read(&Cancellation::never())).unwrap(),
        Some(vec![1])
    );
    writer.close_write(None);
    assert_eq!(block_on(body.read(&Cancellation::never())).unwrap(), None);
}

#[test]
fn precancelled_reads_preserve_already_queued_data() {
    let cancellation = Cancellation::new();
    cancellation.cancel();
    let (mut body, mut writer) = BodyReader::bounded(8, 1);
    block_on(writer.write(vec![1], &Cancellation::never())).unwrap();
    assert!(matches!(
        block_on(body.read(&cancellation)),
        Err(BodyError::Cancelled)
    ));
    assert_eq!(
        block_on(body.read(&Cancellation::never())).unwrap(),
        Some(vec![1])
    );
}

#[test]
fn unfinished_writer_drop_wakes_input_and_reports_failure() {
    let (mut body, writer) = BodyReader::<Vec<u8>>::bounded(8, 1);
    let cancellation = Cancellation::never();
    let wakes = Arc::new(WakeCount::default());
    let mut read = pin!(body.read(&cancellation));
    assert!(poll(read.as_mut(), &wakes).is_pending());
    drop(writer);
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        poll(read.as_mut(), &wakes),
        Poll::Ready(Err(BodyError::Abandoned))
    ));
}

#[test]
fn reader_abandonment_wakes_a_blocked_writer() {
    for drop_reader in [false, true] {
        let cancellation = Cancellation::never();
        let (mut body, mut writer) = BodyReader::bounded(8, 1);
        block_on(writer.write(vec![1], &cancellation)).unwrap();
        let wakes = Arc::new(WakeCount::default());
        let mut write = pin!(writer.write(vec![2], &cancellation));
        assert!(poll(write.as_mut(), &wakes).is_pending());
        if drop_reader {
            drop(body);
        } else {
            body.close(None);
        }
        assert!(wakes.0.load(Ordering::SeqCst) > 0);
        assert!(matches!(
            poll(write.as_mut(), &wakes),
            Poll::Ready(Err(BodyError::Abandoned))
        ));
    }
}

#[test]
fn explicit_failures_discard_queued_data_and_preserve_the_cause() {
    use std::{error::Error as _, io};
    let (mut body, mut writer) = BodyReader::bounded(8, 2);
    block_on(writer.write(vec![1], &Cancellation::never())).unwrap();
    writer.close_write(Some(BodyError::new(io::Error::other("producer failed"))));
    writer.close_write(None);
    drop(writer);
    assert!(body.is_ready());
    let error = block_on(body.read(&Cancellation::never())).unwrap_err();
    assert_eq!(error.source().unwrap().to_string(), "producer failed");
    assert!(matches!(
        block_on(body.read(&Cancellation::never())),
        Err(BodyError::Failed(_))
    ));
}

#[test]
fn independent_threads_make_progress_under_both_queue_limits() {
    let (mut body, mut writer) = BodyReader::bounded(2, 1);
    let producer = std::thread::spawn(move || {
        block_on(async {
            for n in 0..500u16 {
                writer
                    .write(n.to_be_bytes().to_vec(), &Cancellation::never())
                    .await
                    .unwrap();
            }
            writer.close_write(None);
        })
    });
    block_on(async {
        for n in 0..500u16 {
            assert_eq!(
                body.read(&Cancellation::never()).await.unwrap(),
                Some(n.to_be_bytes().to_vec())
            );
        }
        assert_eq!(body.read(&Cancellation::never()).await.unwrap(), None);
    });
    producer.join().unwrap();
}

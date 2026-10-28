// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Shared ownership and backpressure checks for backend-created pools.
use base_scheduler::*;
use futures_lite::future::poll_once;
use std::pin::pin;

pub async fn bounded<S: BufferPooling>(scheduler: &S) {
    let options = BufferPoolOptions {
        alignment: 4096,
        ..BufferPoolOptions::new(2, 257)
    };
    let pool = scheduler.buffer_pool(options).unwrap();
    assert_eq!(pool.capacity(), options.buffers);
    assert_eq!(pool.buffer_size(), options.buffer_size);
    assert!(pool.alignment() >= options.alignment);
    let mut first = pool.try_acquire().unwrap();
    assert_eq!(first.as_ref().as_ptr() as usize % options.alignment, 0);
    first.as_mut()[..6].copy_from_slice(b"abcdef");
    first.truncate(6);
    let view = first.freeze().slice(1..5);
    let other = pool.clone();
    let never = Cancellation::never();
    let second = other.acquire(&never).await.unwrap();
    assert_eq!(second.as_ref().len(), options.buffer_size);
    assert!(pool.try_acquire().is_none());
    let token = Cancellation::new();
    {
        let mut waiting = pin!(pool.acquire(&token));
        assert!(poll_once(waiting.as_mut()).await.is_none());
        token.cancel();
        assert!(waiting.await.is_err());
    }
    assert!(other.try_acquire().is_none());
    // Completed parser views remain valid after crossing a thread and dropping all public pool handles.
    std::thread::spawn(move || {
        assert_eq!(view.as_ref(), b"bcde");
        drop(view);
    })
    .join()
    .unwrap();
    // A portable pool's returned capacity is available from another thread, even while its other slot remains held.
    let acquiring = pool.clone();
    let first = std::thread::spawn(move || {
        acquiring
            .try_acquire()
            .expect("returned shared capacity is available on another thread")
    })
    .join()
    .unwrap();
    assert_eq!(first.as_ref().len(), options.buffer_size);
    drop((pool, other));
    assert_eq!(first.as_ref().len(), options.buffer_size);
    assert_eq!(second.as_ref().len(), options.buffer_size);
}

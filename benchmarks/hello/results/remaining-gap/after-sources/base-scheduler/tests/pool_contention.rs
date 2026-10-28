// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{Cancellation, HeapBufferPool};
use futures_lite::future::block_on;

#[test]
fn racing_exhaustion_and_returns_do_not_lose_capacity_or_notifications() {
    let pool = HeapBufferPool::new(2, 16).unwrap();
    let (done, finished) = std::sync::mpsc::channel();
    let mut handles = Vec::new();
    for value in 0..4 {
        let pool = pool.clone();
        let done = done.clone();
        handles.push(std::thread::spawn(move || {
            block_on(async {
                for _ in 0..if cfg!(miri) { 8 } else { 3000 } {
                    let mut buffer = pool.acquire(&Cancellation::never()).await.unwrap();
                    buffer.as_mut().fill(value);
                    std::thread::yield_now();
                    assert_eq!(buffer.as_ref(), [value; 16]);
                    let frozen = buffer.freeze();
                    assert_eq!(frozen.view(..4).as_ref(), [value; 4]);
                }
            });
            done.send(()).unwrap();
        }));
    }
    for _ in 0..4 {
        finished
            .recv_timeout(std::time::Duration::from_secs(30))
            .unwrap();
    }
    for handle in handles {
        handle.join().unwrap();
    }
    let first = pool.try_acquire().unwrap();
    let second = pool.try_acquire().unwrap();
    assert!(pool.try_acquire().is_none());
    drop((first, second));
}

#[test]
fn cancelling_one_waiter_preserves_returns_from_later_availability_words() {
    use futures_lite::future::poll_once;
    use std::pin::pin;

    block_on(async {
        for capacity in [65, 130] {
            let pool = HeapBufferPool::new(capacity, 16).unwrap();
            let mut held: Vec<_> = (0..capacity).map(|_| pool.try_acquire().unwrap()).collect();
            let cancellation = Cancellation::new();
            let never = Cancellation::never();
            let mut first = pin!(pool.acquire(&cancellation));
            let mut second = pin!(pool.acquire(&never));
            assert!(poll_once(first.as_mut()).await.is_none());
            assert!(poll_once(second.as_mut()).await.is_none());
            cancellation.cancel();
            assert!(first.await.is_err());
            let later = held.pop().unwrap();
            std::thread::spawn(move || drop(later)).join().unwrap();
            let returned = poll_once(second.as_mut()).await.unwrap().unwrap();
            assert_eq!(returned.as_ref(), [0; 16]);
            assert!(pool.try_acquire().is_none());
        }
    });
}

#[test]
fn abandoning_a_notified_waiter_preserves_other_waiters_capacity() {
    use futures_lite::future::poll_once;
    use std::pin::pin;

    block_on(async {
        let pool = HeapBufferPool::new(1, 16).unwrap();
        let held = pool.try_acquire().unwrap();
        let never = Cancellation::never();
        let mut second = pin!(pool.acquire(&never));
        {
            let mut first = pin!(pool.acquire(&never));
            assert!(poll_once(first.as_mut()).await.is_none());
            assert!(poll_once(second.as_mut()).await.is_none());
            drop(held);
        }
        assert!(poll_once(second.as_mut()).await.unwrap().is_ok());
    });
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! A latch releases every waiter once, asynchronous waits observe cancellation, and blocking waits wait for another thread.
use base_http_fixtures::Latch;
use base_scheduler::Cancellation;
use futures_lite::future::{block_on, poll_once};
use std::{pin::pin, thread};

#[test]
fn a_release_from_another_thread_wakes_waiters() {
    let latch = Latch::new();
    assert!(!latch.is_released());
    block_on(async {
        let never = Cancellation::never();
        let mut waiting = pin!(latch.wait(&never));
        // The wait is registered and pending before the release.
        assert!(poll_once(waiting.as_mut()).await.is_none());
        let releaser = thread::spawn({
            let latch = latch.clone();
            move || latch.release()
        });
        waiting.await.unwrap();
        releaser.join().unwrap();
    });
    assert!(latch.is_released());
    latch.wait_blocking();
}

#[test]
fn a_blocking_wait_returns_on_release_from_another_thread() {
    let latch = Latch::new();
    let waiter = thread::spawn({
        let latch = latch.clone();
        move || latch.wait_blocking()
    });
    latch.release();
    waiter.join().unwrap();
}

#[test]
fn a_cancelled_wait_fails() {
    let cancellation = Cancellation::new();
    cancellation.cancel();
    assert!(block_on(Latch::new().wait(&cancellation)).is_err());
}

#[test]
fn cancelling_a_pending_wait_fails_it() {
    block_on(async {
        let cancellation = Cancellation::new();
        let latch = Latch::new();
        let mut waiting = pin!(latch.wait(&cancellation));
        assert!(poll_once(waiting.as_mut()).await.is_none());
        let canceller = thread::spawn({
            let cancellation = cancellation.clone();
            move || cancellation.cancel()
        });
        assert!(waiting.await.is_err());
        canceller.join().unwrap();
    });
}

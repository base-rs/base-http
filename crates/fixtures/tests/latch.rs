// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! A latch releases every waiter once, asynchronous waits observe cancellation, and blocking waits wait for another thread.
use base_http_fixtures::Latch;
use base_scheduler::Cancellation;
use futures_lite::future::block_on;
use std::thread;

#[test]
fn a_release_from_another_thread_wakes_waiters() {
    let latch = Latch::new();
    assert!(!latch.is_released());
    let blocking = thread::spawn({
        let latch = latch.clone();
        move || latch.wait_blocking()
    });
    let releaser = thread::spawn({
        let latch = latch.clone();
        move || latch.release()
    });
    block_on(latch.wait(&Cancellation::never())).unwrap();
    blocking.join().unwrap();
    releaser.join().unwrap();
    assert!(latch.is_released());
    latch.wait_blocking();
}

#[test]
fn a_cancelled_wait_fails() {
    let cancellation = Cancellation::new();
    cancellation.cancel();
    assert!(block_on(Latch::new().wait(&cancellation)).is_err());
}

#[test]
fn cancelling_a_pending_wait_fails_it() {
    let cancellation = Cancellation::new();
    let canceller = thread::spawn({
        let cancellation = cancellation.clone();
        move || cancellation.cancel()
    });
    assert!(block_on(Latch::new().wait(&cancellation)).is_err());
    canceller.join().unwrap();
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use base_scheduler::{IoBuffers, IoBuffersMut};
use futures_lite::future::{block_on, poll_once};

#[test]
fn lazy_state_tracks_admission_and_retirement() {
    let mut state = OperationState::default();
    block_on(state.close());
    assert!(state.activity.is_none());
    let mut guard = state.begin(vec![1, 2, 3]).ok().unwrap();
    assert_eq!(guard.as_ref(), &[1, 2, 3]);
    guard.as_mut()[1] = 4;
    let (error, buffer) = state.begin(vec![5]).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(buffer, &[5]);
    let mut close = std::pin::pin!(state.close());
    assert!(block_on(poll_once(&mut close)).is_none());
    assert_eq!(guard.into_inner(), &[1, 4, 3]);
    block_on(close);
}

#[test]
fn guard_preserves_owned_vectors_and_prepared_ranges() {
    let mut state = OperationState::default();
    let buffers = IoBuffers::new([vec![1, 2], vec![3, 4]]);
    let guard = state.begin(buffers).ok().unwrap();
    assert_eq!(guard.buffer_count(), 2);
    assert_eq!(guard.buffer(1), &[3, 4]);
    let slices = guard.io_slices().unwrap();
    assert_eq!(&*slices[0], &[1, 2]);
    drop(guard);
    let buffers = IoBuffersMut::new([vec![1, 2], vec![3, 4]]);
    let mut guard = state.begin(buffers).ok().unwrap();
    guard.buffer_mut(1)[0] = 5;
    assert_eq!(&*guard.io_slices_mut().unwrap()[1], &[5, 4]);
    drop(guard);
    block_on(state.close());
}

#[test]
fn panicking_owner_still_publishes_retirement() {
    struct PanickingOwner;
    impl Drop for PanickingOwner {
        fn drop(&mut self) {
            panic!("injected owner destructor panic");
        }
    }
    let mut state = OperationState::default();
    let guard = state.begin(PanickingOwner).ok().unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(guard))).is_err());
    block_on(state.close());
}

#[test]
fn retired_results_do_not_block_the_next_operation_or_close() {
    let mut state = OperationState::default();
    let mut first = state.begin(vec![1]).ok().unwrap();
    first.retire();
    block_on(state.close());
    let second = state.begin(vec![2]).ok().unwrap();
    drop(first);
    assert!(state.begin(vec![3]).is_err());
    assert_eq!(second.into_inner(), &[2]);
    block_on(state.close());
}

#[test]
fn admission_observes_cross_thread_retirement_before_reusing_the_lane() {
    struct Owner(Arc<AtomicBool>);
    impl Drop for Owner {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let mut state = OperationState::default();
    for _ in 0..16 {
        let retired = Arc::new(AtomicBool::new(false));
        let guard = state.begin(Owner(Arc::clone(&retired))).ok().unwrap();
        let (error, returned) = state.begin(vec![1, 2, 3]).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(returned, [1, 2, 3]);
        let retiring = std::thread::spawn(move || drop(guard));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut value = returned;
        let next = loop {
            match state.begin(value) {
                Ok(guard) => break guard,
                Err((error, returned)) => {
                    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
                    assert!(
                        std::time::Instant::now() < deadline,
                        "operation did not retire"
                    );
                    value = returned;
                    std::thread::yield_now();
                }
            }
        };
        assert!(retired.load(Ordering::Acquire));
        assert!(state.begin(()).is_err());
        assert_eq!(next.into_inner(), [1, 2, 3]);
        retiring.join().unwrap();
        block_on(state.close());
    }
}

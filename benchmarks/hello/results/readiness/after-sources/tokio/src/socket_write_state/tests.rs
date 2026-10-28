// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use futures_lite::future::{block_on, poll_once};
use std::future::{Ready, ready};

#[test]
fn retained_errors_preserve_classification_and_first_failure() {
    let mut state = SocketWriteState::<Ready<Result<OperationResult<()>, TaskError>>>::default();
    state.fail(io::Error::from_raw_os_error(1).into());
    state.fail(base_scheduler::Cancelled.into());
    assert_eq!(state.check_open().unwrap_err().raw_os_error(), Some(1));
    assert_eq!(
        block_on(state.flush(&Cancellation::never()))
            .unwrap_err()
            .raw_os_error(),
        Some(1)
    );
    block_on(state.close()).unwrap();
    assert_eq!(
        state.check_open().unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );

    let mut state = SocketWriteState::<Ready<Result<OperationResult<()>, TaskError>>>::default();
    state.accept(ready(Err(TaskError::Cancelled)));
    let error = block_on(state.flush(&Cancellation::never())).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert_eq!(
        block_on(state.flush(&Cancellation::never()))
            .unwrap_err()
            .to_string(),
        error.to_string()
    );
    block_on(state.close()).unwrap();
}

#[test]
fn cancelled_wait_retains_pending_operation_and_close_signals_retirement() {
    block_on(async {
        let (sender, receiver) = futures_channel::oneshot::channel();
        let handle = Box::pin(async { receiver.await.unwrap() });
        let mut state = SocketWriteState::default();
        let stopping = state.cancellation();
        state.accept(handle);
        let cancellation = Cancellation::new();
        cancellation.cancel();
        assert_eq!(
            std::mem::discriminant(&state.flush(&cancellation).await.unwrap_err()),
            std::mem::discriminant(&OperationError::Cancelled(base_scheduler::Cancelled))
        );
        let mut closing = Box::pin(state.close());
        assert!(poll_once(closing.as_mut()).await.is_none());
        assert!(stopping.is_cancelled());
        drop(closing);
        sender
            .send(Ok(Err(base_scheduler::Cancelled.into())))
            .unwrap();
        state.close().await.unwrap();
        state.close().await.unwrap();
    });
}

#[test]
fn successful_completion_wins_cancelled_wait_and_task_failure_reports_cleanup_failure() {
    block_on(async {
        let cancellation = Cancellation::new();
        cancellation.cancel();
        let mut state = SocketWriteState::default();
        state.accept(ready(Ok(Ok(()))));
        state.flush(&cancellation).await.unwrap();
        assert!(!state.cancellation().is_cancelled());
        state.accept(ready(Err(TaskError::Cancelled)));
        assert_eq!(
            state.close().await.unwrap_err().kind(),
            io::ErrorKind::Other
        );
        state.close().await.unwrap();
    });
}

#[test]
fn drop_cancels_the_background_operation() {
    let state = SocketWriteState::<()>::default();
    let cancellation = state.cancellation();
    drop(state);
    assert!(cancellation.is_cancelled());
}

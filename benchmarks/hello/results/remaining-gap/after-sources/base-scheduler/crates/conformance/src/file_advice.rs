// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Advisory calls, validation, and admission/completion lifecycle assertions.

use crate::FileAdviceFixture;
use base_scheduler::{AdviceMode, FileAdvice};
use std::{
    future::{Future, poll_fn},
    pin::pin,
};

/// All modes and zero-length-to-EOF preserve native arguments and file state.
pub async fn modes<F: FileAdviceFixture>(fixture: &F) {
    let mut expected = fixture.observed();
    for mode in [
        AdviceMode::Normal,
        AdviceMode::Sequential,
        AdviceMode::Random,
        AdviceMode::WillNeed,
        AdviceMode::DontNeed,
        AdviceMode::NoReuse,
    ] {
        for length in [0, 4] {
            fixture
                .scheduler()
                .file_advise(fixture.file(), 1, length, mode)
                .await
                .unwrap();
            expected.push((1, length, mode));
            assert_eq!(
                fixture.observed(),
                expected,
                "completion returned before execution"
            );
            fixture.assert_state(b"abcdef", 3);
        }
    }
}

/// Unpolled completion and submission forms admit no work.
pub async fn lazy<F: FileAdviceFixture>(fixture: &F) {
    let expected = fixture.observed();
    drop(
        fixture
            .scheduler()
            .file_advise(fixture.file(), 1, 4, AdviceMode::Normal),
    );
    drop(
        fixture
            .scheduler()
            .file_advise_submit(fixture.file(), 1, 4, AdviceMode::Normal),
    );
    fixture.settle().await;
    assert_eq!(fixture.observed(), expected);
    fixture.assert_state(b"abcdef", 3);
}

/// Validation affects both forms; execution errors reach the completion form and are discarded by the submission form.
pub async fn errors<F: FileAdviceFixture>(fixture: &F) {
    for (offset, length) in [
        (i64::MAX as u64 + 1, 0),
        (i64::MAX as u64, 1),
        (1, u64::MAX),
    ] {
        let error = fixture
            .scheduler()
            .file_advise(fixture.file(), offset, length, AdviceMode::Normal)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        let error = fixture
            .scheduler()
            .file_advise_submit(fixture.file(), offset, length, AdviceMode::Normal)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }
    let error = fixture
        .scheduler()
        .file_advise(fixture.failing_file(), 0, 0, AdviceMode::Normal)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), fixture.error_kind());
    fixture
        .scheduler()
        .file_advise_submit(fixture.failing_file(), 0, 0, AdviceMode::Normal)
        .await
        .unwrap();
    fixture.settle().await;
    fixture.assert_state(b"abcdef", 3);
}

/// Accepted advice survives waiter abandonment; submission returns after admission even while execution is held.
pub async fn abandonment<F: FileAdviceFixture>(fixture: &F) {
    let mut expected = fixture.observed();
    let guard = fixture.hold();
    let mut operation = Box::pin(fixture.scheduler().file_advise(
        fixture.file(),
        1,
        4,
        AdviceMode::WillNeed,
    ));
    let mut accepted = pin!(fixture.accepted());
    poll_fn(|context| {
        assert!(
            operation.as_mut().poll(context).is_pending(),
            "completion returned before execution"
        );
        accepted.as_mut().poll(context)
    })
    .await;
    drop(operation);
    fixture.assert_retained();
    drop(guard);
    fixture.settle().await;
    expected.push((1, 4, AdviceMode::WillNeed));
    assert_eq!(fixture.observed(), expected);

    let guard = fixture.hold();
    fixture
        .scheduler()
        .file_advise_submit(fixture.file(), 2, 0, AdviceMode::DontNeed)
        .await
        .unwrap();
    fixture.assert_retained();
    assert_eq!(fixture.observed(), expected);
    drop(guard);
    fixture.settle().await;
    expected.push((2, 0, AdviceMode::DontNeed));
    assert_eq!(fixture.observed(), expected);
    fixture.assert_state(b"abcdef", 3);
}

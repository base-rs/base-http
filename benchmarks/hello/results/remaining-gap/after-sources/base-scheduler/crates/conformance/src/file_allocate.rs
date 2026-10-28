// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Linux-style reservation semantics, independent of the backend resource type.

use crate::FileAllocateFixture;
use base_scheduler::{AllocationMode, Cancellation, FileAllocate, OperationError};

const OFFSET: u64 = 65536;
const LENGTH: u64 = 4096;

/// Extending allocation preserves data/cursor, zero-fills exposed bytes, and never shrinks an existing file.
pub async fn extend<F: FileAllocateFixture>(fixture: &F) {
    let scheduler = fixture.scheduler();
    scheduler
        .file_allocate(
            fixture.file(),
            1,
            2,
            AllocationMode::Extend,
            &Cancellation::never(),
        )
        .await
        .unwrap();
    fixture.assert_state(b"abcdef", 3);
    fixture.assert_reserved(1, 2);
    scheduler
        .file_allocate(
            fixture.file(),
            OFFSET,
            LENGTH,
            AllocationMode::Extend,
            &Cancellation::never(),
        )
        .await
        .unwrap();
    let mut expected = vec![0; (OFFSET + LENGTH) as usize];
    expected[..6].copy_from_slice(b"abcdef");
    fixture.assert_state(&expected, 3);
    fixture.assert_reserved(OFFSET, LENGTH);
}

/// KeepSize reserves beyond EOF without changing data, length, or cursor.
pub async fn keep_size<F: FileAllocateFixture>(fixture: &F) {
    fixture
        .scheduler()
        .file_allocate(
            fixture.file(),
            OFFSET,
            LENGTH,
            AllocationMode::KeepSize,
            &Cancellation::never(),
        )
        .await
        .unwrap();
    fixture.assert_state(b"abcdef", 3);
    fixture.assert_reserved(OFFSET, LENGTH);
}

/// Unpolled and pre-cancelled allocation never reaches the allocation boundary.
pub async fn cancellation<F: FileAllocateFixture>(fixture: &F) {
    let calls = fixture.calls();
    let never = Cancellation::never();
    drop(fixture.scheduler().file_allocate(
        fixture.file(),
        OFFSET,
        LENGTH,
        AllocationMode::Extend,
        &never,
    ));
    assert_eq!(fixture.calls(), calls);
    let token = Cancellation::new();
    token.cancel();
    let result = fixture
        .scheduler()
        .file_allocate(
            fixture.file(),
            OFFSET,
            LENGTH,
            AllocationMode::Extend,
            &token,
        )
        .await;
    assert!(matches!(result, Err(OperationError::Cancelled(_))));
    assert_eq!(fixture.calls(), calls);
    fixture.assert_state(b"abcdef", 3);
}

/// Invalid ranges and OS execution errors are reported without hiding them behind success.
pub async fn errors<F: FileAllocateFixture>(fixture: &F) {
    for (offset, length) in [(0, 0), (i64::MAX as u64, 1), (u64::MAX, 1), (1, u64::MAX)] {
        let result = fixture
            .scheduler()
            .file_allocate(
                fixture.file(),
                offset,
                length,
                AllocationMode::Extend,
                &Cancellation::never(),
            )
            .await;
        assert!(
            matches!(result, Err(OperationError::Io(error)) if error.kind() == std::io::ErrorKind::InvalidInput)
        );
        fixture.assert_state(b"abcdef", 3);
    }
    let result = fixture
        .scheduler()
        .file_allocate(
            fixture.read_only_file(),
            OFFSET,
            LENGTH,
            AllocationMode::Extend,
            &Cancellation::never(),
        )
        .await;
    assert!(matches!(result, Err(OperationError::Io(_))));
    fixture.assert_state(b"abcdef", 3);
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Cancellation, OperationResult, ProcessWaitFlags, ProcessWaitResult};
use std::future::Future;

/// Wait for a child process to change state without blocking an executor worker.
pub trait ProcessWait: Send + Sync {
    /// Native process selector: `i32` on Unix and `u32` on Windows.
    ///
    /// Unix follows `waitpid`: positive selects one child, zero selects the current process group, `-1` selects any child, and values below `-1` select the process group with the corresponding positive ID. Each call reports one child, not the whole group. Windows supports individual process IDs only.
    type ProcessId: Copy + Send + 'static;

    /// Wait for one matching status, reaping a terminated Unix child. `None` means `nohang` found no reportable state change; no matching child is an OS error.
    ///
    /// Construction is lazy and pre-cancellation prevents waiting or reaping. Cancellation, dropping the future, and scheduler shutdown never terminate the process. Cancellation is best effort: a status already obtained wins concurrent cancellation. A started blocking fallback can wait until the child changes state and reap after cancellation or abandonment; scheduler shutdown must await it. Cancel and await the wait before assigning another waiter. Dropping alone does not establish retirement; use backend shutdown before assigning another reaper after abandonment. Forgetting a future can retain backend resources until process exit or scheduler shutdown.
    ///
    /// The caller must exclusively own waiting for the selected Unix children. Do not race another reaper, including `std::process::Child::wait` or `try_wait`. Waiting by ID does not update `Child`'s cached status; after a successful exit wait, do not use that `Child` for further waits or PID-based control. Drop it instead. On Windows, retain the original `Child` or another process handle through the wait so its numeric ID stays valid until this lazy operation opens its own handle. This operation does not close child stdin or drain output pipes; manage those explicitly. Tracing-specific state changes are outside this contract.
    fn process_wait(
        &self,
        process: Self::ProcessId,
        flags: ProcessWaitFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Option<ProcessWaitResult>>> + Send;
}

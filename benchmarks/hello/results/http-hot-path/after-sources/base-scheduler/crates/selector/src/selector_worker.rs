// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::{io, task::Waker, time::Duration};

/// An entered I/O driver owned exclusively by an executor worker.
///
/// Completion processing publishes results and wakes tasks; it must not poll application futures. The executor polls its ready queue after the entire bounded completion batch has been published. Other workers may steal published tasks immediately.
pub trait SelectorWorker {
    /// A wakeup which interrupts an event wait, including when waking races preparation to sleep. Calling it after driver retirement must be harmless.
    fn waker(&self) -> Waker;

    /// Stop admission and begin retiring accepted work without synchronously waiting.
    fn close(&mut self);

    /// Publish a bounded batch of completions and wait at most `timeout`. Zero is nonblocking; `None` permits an indefinite event wait. Check local and remote submissions before waiting. After publishing completions or otherwise waking tasks, return without waiting so the executor can poll those tasks; the executor need not interrupt a driver from its own thread. Return `true` when closed and drained. An error permanently stops the driver and must notify its waiters while preserving any storage still accessible to the OS.
    fn turn(&mut self, timeout: Option<Duration>) -> io::Result<bool>;
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::SelectorWorker;

/// A movable I/O driver which an executor can assign to one worker.
pub trait SelectorDriver: Send + 'static {
    /// The entered driver, including any thread-bound local routing state.
    type Worker: SelectorWorker;

    /// Consume the movable driver and enter its owning worker. The returned worker must prevent moving thread-bound routing state to another thread and restore routing when dropped, including during unwinding.
    fn enter(self) -> Self::Worker;
}

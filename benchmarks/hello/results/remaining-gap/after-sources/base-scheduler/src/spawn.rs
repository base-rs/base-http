// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{SpawnError, TaskError};
use std::future::Future;

/// The common spawning contract for schedulers and their handles.
pub trait Spawn {
    /// The runtime's awaitable task handle. Runtime adapters may use their own representation while preserving the ownership and result contract.
    type Handle<Output>: Future<Output = Result<Output, TaskError>> + Send + 'static
    where
        Output: Send + 'static;

    /// Register a task with this scheduler before making it runnable.
    ///
    /// The returned handle owns the execution lifetime. Dropping it requests destruction after any active poll returns; moving it transfers ownership. Await it explicitly for completion, or cancel cooperatively and then await. If the scheduler is closed, the future is dropped without being polled.
    fn spawn<FutureType>(
        &self,
        future: FutureType,
    ) -> Result<Self::Handle<FutureType::Output>, SpawnError>
    where
        FutureType: Future + Send + 'static,
        FutureType::Output: Send + 'static;
}

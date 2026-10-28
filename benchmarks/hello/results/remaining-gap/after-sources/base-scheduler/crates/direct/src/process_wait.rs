// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    DirectScheduler,
    process_io::{Process, ProcessId, operation_result},
};
use base_scheduler::{
    Cancellation, OperationResult, ProcessWait, ProcessWaitFlags, ProcessWaitResult,
};

impl ProcessWait for DirectScheduler {
    type ProcessId = ProcessId;
    async fn process_wait(
        &self,
        process: ProcessId,
        flags: ProcessWaitFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<Option<ProcessWaitResult>> {
        cancellation.check()?;
        let process = Process::open(process, flags)?;
        let (result, ()) = self
            .work
            .execute((), cancellation, move |_| process.wait(false))
            .await;
        match result {
            Err(base_scheduler::OperationError::Io(error)) => {
                operation_result(Err(error), cancellation)
            }
            result => result,
        }
    }
}

#[cfg(test)]
mod tests;

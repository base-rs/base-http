// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{AdviceMode, FileAdvice, FileOperation};
use std::io;

/// A file containing `abcdef` with cursor 3. Advice can be ignored by an OS: observe calls at the actual advice boundary instead of asserting cache residency or eviction.
pub trait FileAdviceFixture {
    type Scheduler: FileAdvice;
    /// Holds accepted work before executing advice. Destruction must release held work even during unwinding.
    type Guard;
    fn scheduler(&self) -> &Self::Scheduler;
    fn file(&self) -> &<Self::Scheduler as FileOperation>::File;
    fn failing_file(&self) -> &<Self::Scheduler as FileOperation>::File;
    fn error_kind(&self) -> io::ErrorKind;
    fn assert_state(&self, contents: &[u8], cursor: u64);
    fn observed(&self) -> Vec<(u64, u64, AdviceMode)>;
    fn hold(&self) -> Self::Guard;
    /// Wait until one operation has passed admission and is held before native access. Drive the operation future concurrently with this barrier; enforce a deadline.
    fn accepted(&self) -> impl Future<Output = ()>;
    /// Assert the backend retains access independently of the abandoned operation future.
    fn assert_retained(&self);
    /// Drive all accepted advice and cleanup to completion, including submitted operations whose execution errors are discarded.
    fn settle(&self) -> impl Future<Output = ()>;
}

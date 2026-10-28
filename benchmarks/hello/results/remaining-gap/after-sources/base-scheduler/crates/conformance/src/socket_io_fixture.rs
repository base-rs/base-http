// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::SocketIoGuard;
use base_scheduler::{Cancellation, OperationResult};

/// Backend-controlled access boundary for deterministic completion/cancellation and abandonment tests, supporting scalar buffers and message sequences alike.
///
/// `operation` submits I/O using the supplied owned input. `start` waits until backend access is stopped at a known boundary after cancellation can no longer prevent that attempt, and returns a guard controlling it independently of the waiting future. Fixtures enforce deadlines and guarantee successful progress once released. The fixture assertions verify input identity, results, retention, and eventual destruction.
pub trait SocketIoFixture {
    type Input: Send + 'static;
    type Output: Send + 'static;
    type Guard: SocketIoGuard;

    fn input(&self) -> Self::Input;
    fn assert_retained(&self);
    fn assert_released(&self);
    fn assert_completed(&self, output: &Self::Output, input: &Self::Input);
    fn operation(
        &self,
        input: Self::Input,
        cancellation: &Cancellation,
    ) -> impl Future<Output = (OperationResult<Self::Output>, Self::Input)> + Send;
    fn start(&self) -> impl Future<Output = Self::Guard>;
}

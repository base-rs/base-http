// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::Blocking;

/// Backend fixture for deterministic queue tests. Exclusive fixture ownership is required while these assertions run.
pub trait BlockingFixture {
    /// Service being tested.
    type Scheduler: Blocking;
    /// Holding every worker prevents queued jobs from starting. Dropping the guard releases them even during unwinding.
    type Guard;
    fn scheduler(&self) -> &Self::Scheduler;
    /// Hold every worker at an execution boundary, leaving exactly one admission slot available.
    fn hold_workers(&self) -> impl std::future::Future<Output = Self::Guard>;
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::Spawn;

/// An isolated task service. The runner drives its executor and enforces deadlines, including while awaiting task handles.
pub trait SpawnFixture {
    type Scheduler: Spawn;
    fn scheduler(&self) -> &Self::Scheduler;
    /// Return an owning check that observes the backend's task registry independently. The check runs inside the single task's first poll and must assert that registration already happened.
    fn registration_check(&self) -> impl FnOnce() + Send + 'static;
    /// Drive admitted ready work and handle-destruction requests to quiescence. Pending tasks stay alive. This is a backend test barrier, not a fixed number of yields or a wall-clock delay.
    fn settle(&self) -> impl Future<Output = ()>;
    /// Close task admission. Existing handles remain valid; subsequent spawn attempts must report `SchedulerClosed` without polling their inputs.
    fn close(&self);
}

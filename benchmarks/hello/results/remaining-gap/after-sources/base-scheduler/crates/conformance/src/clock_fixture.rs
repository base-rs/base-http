// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::Clock;
use std::time::Duration;

/// A fresh, controlled clock domain. Time stays fixed until `advance`; advancing wakes expired timers before returning. Use a backend's paused-clock or test timer driver, rather than wall-clock sleeps.
pub trait ClockFixture {
    type Scheduler: Clock;
    fn scheduler(&self) -> &Self::Scheduler;
    fn advance(&self, duration: Duration);
    /// Number of live timer registrations, observed independently of the futures under test. A fresh fixture has none.
    fn registrations(&self) -> usize;
}

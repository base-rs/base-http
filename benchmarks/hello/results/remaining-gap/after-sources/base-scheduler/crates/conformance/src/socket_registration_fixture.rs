// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{SocketDomain, SocketRegistration};

/// A backend endpoint accepting standard-library TCP resources. Run once per supported IP family. The suite additionally requires accept, scalar read/write, close, and endpoint lookup; individual helpers require only their own operations. The runner enforces a deadline covering native loopback setup, I/O, and cleanup.
pub trait SocketRegistrationFixture {
    type Scheduler: SocketRegistration;
    fn scheduler(&self) -> &Self::Scheduler;
    fn domain(&self) -> SocketDomain;
}

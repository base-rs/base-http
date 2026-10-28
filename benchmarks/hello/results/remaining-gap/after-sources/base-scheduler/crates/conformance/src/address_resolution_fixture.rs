// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{AddressInfo, AddressResolution, AddressResolveError};

/// A resolver with a controlled successful name and a controlled failure. Configure answers independently of the implementation under test: include duplicates, both families, scoped IPv6, and native type/protocol values where supported. No public DNS or host service database is required.
pub trait AddressResolutionFixture {
    type Scheduler: AddressResolution;
    fn scheduler(&self) -> &Self::Scheduler;
    /// A nonnumeric name resolving with default hints and port 80 to exactly `expected`, in order.
    fn name(&self) -> &str;
    /// A configured service name selecting the same port 80 result as the numeric service. No particular host service database is assumed.
    fn service_name(&self) -> &str;
    fn expected(&self) -> &[AddressInfo];
    fn failure_name(&self) -> &str;
    /// Compare category, native code, message, and any underlying system error against an independently configured failure.
    fn assert_error(&self, error: &AddressResolveError);
    /// Verify that the actual native/table lookup received the supplied family, type, protocol, and flags. Inspect the resolver boundary, including its native flag translation, independently of its returned answers.
    fn assert_hints(&self, hints: base_scheduler::AddressHints);
    /// Native/table lookup submissions, excluding argument validation. Observe the resolver boundary rather than calls to the async trait method.
    fn lookups(&self) -> usize;
}

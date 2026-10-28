// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{AddressHints, AddressInfoCollection, AddressService, Cancellation, OperationResult};
use std::future::Future;

/// Optional IP endpoint resolution, independent of socket creation and connection. Implementations may use a system resolver, an asynchronous resolver, or a deterministic table.
///
/// Generic consumers use borrowed views without prescribing the resolver's storage:
///
/// ```
/// use base_scheduler::{AddressHints, AddressInfoCollection, AddressInfoView, AddressResolution, AddressService, Cancellation, OperationResult, SocketAddress};
/// async fn first<S: AddressResolution>(scheduler: &S, cancellation: &Cancellation) -> OperationResult<Option<SocketAddress>> {
///     let result = scheduler.address_resolve(Some("example.com"), AddressService::Port(80), AddressHints::default(), cancellation).await?;
///     let address = result.entries().next().map(|entry| entry.address());
///     Ok(address)
/// }
/// ```
///
/// ```compile_fail
/// use base_scheduler::{AddressResolution, AddressHints, AddressService, Cancellation, SocketOperation};
/// async fn resolve<S: SocketOperation>(scheduler: &S) {
///     scheduler.address_resolve(Some("localhost"), AddressService::Port(80), AddressHints::default(), &Cancellation::never()).await;
/// }
/// ```
pub trait AddressResolution: Send + Sync {
    /// Owned result representation, with borrowed entries and on-demand portable accessors.
    type Addresses: AddressInfoCollection;

    /// Resolve a hostname and service to owned endpoints and socket creation parameters using typed resolver hints.
    ///
    /// Names contain no port. An absent name selects wildcard addresses with `passive`, or loopback without it. Empty names/services and embedded NUL bytes return `InvalidInput`. Numeric ports avoid named-service lookup. `numeric_host` prevents hostname lookup. Family, type, protocol, and address-configuration hints apply during resolution. With unspecified type/protocol the resolver can return multiple transports, including raw sockets. Preserve candidate ordering, duplicates, scope identifiers, and native type/protocol values. Native non-IP results are rejected with `InvalidData` because this capability returns IP endpoints. Successful results may be empty; no matching family or transport may instead be a resolver error, depending on the backend.
    ///
    /// Resolver failures are carried by `AddressResolveError` inside `OperationError::Io`, retaining a portable category and any native resolver code. Lookup and service databases are implementation-specific; there is no implicit connection policy, cache, canonical-name query, or IDNA normalization guarantee.
    ///
    /// The result owner may retain a native list or cached storage. Validate native entries before returning success; validation need not copy the list or eagerly normalize fields. Borrowed entries and iteration need no separate allocations, and native owners release their original list through the matching deallocator exactly once. Converting an entry to `SocketAddress` or `AddressInfo` is explicit and requires no heap allocation.
    ///
    /// Futures are lazy; pre-cancellation wins over validation. Cancellation can prevent admission or queued execution. Started blocking lookups return their actual result, and cancellation does not promise prompt completion. Dropping a future may abandon a running lookup, whose worker retains both inputs and its bounded capacity until completion. Await lookups before scheduler shutdown for graceful completion.
    fn address_resolve(
        &self,
        name: Option<&str>,
        service: AddressService<'_>,
        hints: AddressHints,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Self::Addresses>> + Send;
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Cancellation, Cancelled, ReceiveBuffer};
use std::future::Future;

/// A bounded, shareable source of exclusively writable, stable receive storage. Clones share the same capacity. Completed buffers and their frozen views retain storage independently of pool handles, receivers, and polling threads; the final owner makes that slot reusable.
///
/// Published receive buffers count against capacity, including while an accepted receive is idle. Use a separate pool for independently required scratch buffers or file pipelines. The scheduler's portable factory supplies a shared total budget; concrete backend experiments may document a different capacity distribution.
///
/// Acquisition returns a fully initialized `buffer_size()` prefix with at least `alignment()` alignment. Its previous contents are unspecified. Exhaustion waits with cancellation instead of growing the pool. Retained views and accepted I/O count against capacity until their access retires, including after future abandonment. Kernel registration and publication are implementation details; neither may grant concurrent access to a leased buffer.
pub trait BufferPool: Clone + Send + Sync + 'static {
    type Buffer: ReceiveBuffer;

    /// Total slot capacity, including leased and published buffers, rather than the number currently free. Worker-local caches are part of this total.
    fn capacity(&self) -> usize;
    fn buffer_size(&self) -> usize;
    fn alignment(&self) -> usize;

    /// Acquire immediately, or return `None` without waiting for capacity.
    fn try_acquire(&self) -> Option<Self::Buffer>;

    /// Pre-cancellation leaves capacity unchanged. Acquisition wins a subsequent cancellation race. Dropping or forgetting a pending acquisition must not release storage still accessible by another owner.
    fn acquire(
        &self,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<Self::Buffer, Cancelled>> + Send;
}

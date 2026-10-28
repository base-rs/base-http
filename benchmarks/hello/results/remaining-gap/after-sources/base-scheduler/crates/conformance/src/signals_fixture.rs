// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::Signals;
use std::{fmt::Debug, sync::Arc};

/// Isolated signal subscriptions and controlled delivery. The fixture owns an `Arc` endpoint so callbacks can safely test reentrant subscription. Signal identifiers used in tests must support equality and debugging; the production capability imposes no such bounds.
pub trait SignalsFixture {
    type Signal: Copy + Send + Eq + Debug + 'static;
    type Scheduler: Signals<Signal = Self::Signal> + 'static;
    fn scheduler(&self) -> &Arc<Self::Scheduler>;
    /// Two distinct supported identifiers.
    fn signals(&self) -> [Self::Signal; 2];
    fn invalid_signal(&self) -> Self::Signal;
    /// Deliver one isolated event and wait for dispatch and subscription retirement to quiesce. Emit calls are sequential; tests do not assume counts for coalesced OS bursts. Enforce a deadline and deliver in ordinary thread context. The in-progress-dispatch test drives this method on a scoped background thread.
    fn emit(&self, signal: Self::Signal) -> impl Future<Output = ()>;
}

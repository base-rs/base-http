// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{Cancellation, Cancelled};
use event_listener::{Event, Listener, listener};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

/// How long a blocking peer waits for a [`Latch`] before failing.
pub const LATCH_TIMEOUT: Duration = Duration::from_secs(10);

/// A one-shot synchronisation point between a test, its handler and its raw peers.
///
/// Clones share one state. Releasing it wakes every waiter, and later waits return immediately. Asynchronous waits observe cancellation; blocking waits, for peer threads, fail after [`LATCH_TIMEOUT`]. Use it instead of sleeping when one party must observe another's progress.
#[derive(Clone, Debug, Default)]
pub struct Latch(Arc<Inner>);

#[derive(Debug, Default)]
struct Inner {
    released: AtomicBool,
    event: Event,
}

impl Latch {
    /// A latch that has not been released.
    pub fn new() -> Self {
        Self::default()
    }

    /// Release every current and future waiter.
    pub fn release(&self) {
        self.0.released.store(true, Ordering::Release);
        self.0.event.notify(usize::MAX);
    }

    /// Whether the latch has been released.
    pub fn is_released(&self) -> bool {
        self.0.released.load(Ordering::Acquire)
    }

    /// Wait asynchronously for release, or fail when `cancellation` is cancelled first.
    pub async fn wait(&self, cancellation: &Cancellation) -> Result<(), Cancelled> {
        let mut cancelled = std::pin::pin!(cancellation.cancelled());
        loop {
            cancellation.check()?;
            // Register before examining the state, so a release in between still wakes this waiter.
            listener!(self.0.event => released);
            if self.is_released() {
                return Ok(());
            }
            futures_lite::future::or(released, cancelled.as_mut()).await;
        }
    }

    /// Block the current thread until release, panicking after [`LATCH_TIMEOUT`]. Use this from raw peer threads, never from a future.
    pub fn wait_blocking(&self) {
        let deadline = Instant::now() + LATCH_TIMEOUT;
        loop {
            let released = self.0.event.listen();
            if self.is_released() {
                return;
            }
            assert!(
                released.wait_deadline(deadline).is_some(),
                "latch was not released within {LATCH_TIMEOUT:?}"
            );
        }
    }
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::io_uring_selector::{Controller, DriverGroup, Reactor, SignalCompletion};
use base_scheduler_selector::SelectorDriver;
use std::{io, sync::Arc, time::Duration};

/// Exclusive ring driver. Submission handles may be used on other threads; only this owner processes the ring.
///
/// Call `turn` from an event loop, or move the driver into a dedicated thread and call `run`. Dropping the driver closes admission; any unretired kernel-accessible requests are deliberately retained on this exceptional path. Their waiting operation futures panic when polled because their owners cannot safely be returned. Orderly shutdown keeps driving until `turn` returns `true`.
pub struct IoUringDriver {
    pub(crate) reactor: Option<Reactor>,
    pub(crate) controller: Arc<Controller>,
    pub(crate) group: Arc<DriverGroup>,
    pub(crate) completion: Option<SignalCompletion>,
}
impl IoUringDriver {
    /// A waker for an event loop's root future. Waking it interrupts the driver's wait, including when the wake races preparation to sleep.
    pub fn waker(&self) -> std::task::Waker {
        std::task::Waker::from(Arc::clone(&self.controller))
    }
    /// Process a bounded batch and wait at most `timeout` for new events. `None` permits an indefinite event wait; zero is nonblocking. Returns `true` once closed and drained. A failure permanently stops this driver and closes admission.
    pub fn turn(&mut self, timeout: Option<Duration>) -> io::Result<bool> {
        let result = match self.reactor.as_mut() {
            Some(reactor) => reactor.turn(timeout),
            None => return Ok(true),
        };
        if !matches!(result, Ok(false)) {
            self.stop();
        }
        result
    }
    /// Drive until closed and drained, using the calling thread for event waits.
    pub fn run(mut self) -> io::Result<()> {
        while !self.turn(None)? {}
        Ok(())
    }
    pub(crate) fn stop(&mut self) {
        self.controller.close();
        self.group.close();
        // Retain unknown in-flight storage before waking shutdown or result waiters.
        drop(self.reactor.take());
        drop(self.completion.take());
    }
}
impl Drop for IoUringDriver {
    fn drop(&mut self) {
        self.stop();
    }
}

impl SelectorDriver for IoUringDriver {
    type Worker = crate::IoUringWorker;
    fn enter(self) -> Self::Worker {
        crate::IoUringWorker::new(self)
    }
}

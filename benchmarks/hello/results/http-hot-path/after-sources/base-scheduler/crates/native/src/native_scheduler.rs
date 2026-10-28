// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{NativeExecutorOptions, NativeOptions, NativeTask, ReadinessSelector};
use base_scheduler::{Spawn, SpawnError};
use base_scheduler_selector::{Selector, SelectorOptions};
use std::{future::Future, io};

/// Native task execution and timers composed with explicitly selected I/O capabilities.
///
/// The default selector uses readiness sockets and bounded synchronous I/O. Other selectors retain their own resource types and progress requirements. Only capabilities implemented by the selected type are forwarded.
pub struct NativeScheduler<S: Selector = ReadinessSelector> {
    selector: S,
    pub(crate) tasks: crate::task_service::TaskService,
}

impl NativeScheduler {
    /// Create native task workers and the default readiness selector.
    pub fn new() -> io::Result<Self> {
        Self::with_options(NativeOptions::default())
    }

    /// Configure task workers, timer capacity, and the default selector's blocking services.
    pub fn with_options(options: NativeOptions) -> io::Result<Self> {
        let (selector, drivers) = ReadinessSelector::with_workers(
            SelectorOptions {
                threads: options.threads,
                capacity: options.capacity,
            },
            options.workers,
        )?;
        Self::with_drivers(
            selector,
            drivers,
            NativeExecutorOptions {
                workers: options.workers,
                timer_capacity: options.timer_capacity,
            },
        )
    }
}

impl<S: Selector> NativeScheduler<S> {
    /// Compose an owned selector with native task execution and timers. `workers` and `timer_capacity` must be nonzero; I/O limits are configured when constructing the selector.
    ///
    /// The caller arranges the selector's driver and keeps it progressing through shutdown. If construction fails, the supplied selector is closed.
    pub fn with_selector(selector: S, options: NativeExecutorOptions) -> io::Result<Self> {
        let result = (|| {
            if options.workers == 0 || options.timer_capacity == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "worker count and timer capacity must be nonzero",
                ));
            }
            crate::task_service::TaskService::new(options.workers, options.timer_capacity)
        })();
        match result {
            Ok(tasks) => Ok(Self { selector, tasks }),
            Err(error) => {
                selector.close();
                Err(error)
            }
        }
    }

    /// Compose one independently owned I/O driver per native task worker. Drivers are entered and driven by those workers; no separate I/O thread is started. Root and external operations use the selector's documented routing policy.
    ///
    /// The number of drivers must equal `options.workers`. Failed construction closes the selector and drops unstarted drivers. Worker shutdown continues driving accepted I/O until it retires, even after task abandonment.
    pub fn with_drivers<D: base_scheduler_selector::SelectorDriver>(
        selector: S,
        drivers: Vec<D>,
        options: NativeExecutorOptions,
    ) -> io::Result<Self> {
        let result = if options.workers == 0
            || options.timer_capacity == 0
            || drivers.len() != options.workers
        {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "one driver per nonzero worker and nonzero timer capacity are required",
            ))
        } else {
            crate::task_service::TaskService::with_drivers(drivers, options.timer_capacity)
        };
        match result {
            Ok(tasks) => Ok(Self { selector, tasks }),
            Err(error) => {
                selector.close();
                Err(error)
            }
        }
    }

    /// Access the explicitly selected I/O capabilities.
    pub fn selector(&self) -> &S {
        &self.selector
    }

    /// Number of configured task workers.
    pub fn worker_count(&self) -> usize {
        self.tasks.shared.workers.len()
    }

    /// Poll a root future on this thread while workers execute spawned tasks. The root may borrow local state and need not be `Send`.
    ///
    /// # Panics
    ///
    /// Panics on a native task worker, where blocking would prevent task progress. Use `.await` inside tasks.
    pub fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        use event_listener::Listener;
        use std::{
            pin::pin,
            sync::Arc,
            task::{Context, Poll, Wake, Waker},
        };
        struct Notification(event_listener::Event);
        impl Wake for Notification {
            fn wake(self: Arc<Self>) {
                self.0.notify(1);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.notify(1);
            }
        }
        crate::worker::assert_outside_worker();
        let notification = Arc::new(Notification(event_listener::Event::new()));
        let waker = Waker::from(Arc::clone(&notification));
        let mut future = pin!(future);
        loop {
            let listener = notification.0.listen();
            match future.as_mut().poll(&mut Context::from_waker(&waker)) {
                Poll::Ready(output) => return output,
                Poll::Pending => listener.wait(),
            }
        }
    }

    /// Stop task, timer, and selector admission without waiting. Live task owners remain valid.
    pub fn close(&self) {
        self.selector.close();
        self.tasks.close();
    }

    /// Close admission and await selector, task, and timer retirement. Coordinate task cancellation and cleanup first; a task must not await shutdown of its own scheduler.
    pub async fn shutdown(&self) {
        self.close();
        self.selector.shutdown().await;
        self.tasks.drain().await;
    }
}
impl<S: Selector> Drop for NativeScheduler<S> {
    fn drop(&mut self) {
        self.close();
    }
}
impl<S: Selector> Spawn for NativeScheduler<S> {
    type Handle<Output: Send + 'static> = NativeTask<Output>;
    fn spawn<F>(&self, future: F) -> Result<Self::Handle<F::Output>, SpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.tasks.spawn(future)
    }
}
#[cfg(test)]
mod tests;

#[cfg(test)]
mod driver_tests;

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    NativeTask,
    native_task::TaskFuture,
    thread,
    worker::{self, WorkerState},
};
use base_scheduler::SpawnError;
use crossbeam_deque::Worker;
use event_listener::Event;
use std::{
    future::Future,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

pub(crate) struct TaskState {
    pub(crate) worker: AtomicUsize,
}
pub(crate) type Runnable = async_task::Runnable<TaskState>;

// Arming shares the runnable counter's modification order, so a waiter either sees quiescence or the last runnable sees this bit and notifies it. Keep the bit armed thereafter; drains are cold, and this avoids deregistration races between concurrent or abandoned waiters.
const NOTIFY_IDLE: usize = 1 << (usize::BITS - 1);

pub(crate) struct Shared {
    // Serialize registration with closure and distribute new tasks using a round-robin worker cursor. Task ownership itself needs only a count.
    admission: Mutex<usize>,
    pub(crate) timer_capacity: usize,
    pub(crate) timer_next: AtomicUsize,
    #[cfg(test)]
    pub(crate) paused: Mutex<Option<std::time::Instant>>,
    workers_remaining: AtomicUsize,
    pub(crate) workers: Vec<WorkerState>,
    closed: AtomicBool,
    pub(crate) idle_workers: AtomicUsize,
    // Task-future ownership can retire before its final runnable returns. Shutdown waits for both.
    remaining: AtomicUsize,
    outstanding: AtomicUsize,
    changed: Event,
}
pub(crate) struct TaskService {
    pub(crate) shared: Arc<Shared>,
}

// Retire after normal worker-local timer delivery and driver destruction; also balance partial startup and unwinding.
struct WorkerRegistration(Arc<Shared>);
impl WorkerRegistration {
    fn new(shared: &Arc<Shared>) -> Self {
        shared.workers_remaining.fetch_add(1, Ordering::AcqRel);
        Self(Arc::clone(shared))
    }
}
impl Drop for WorkerRegistration {
    fn drop(&mut self) {
        self.0.workers_remaining.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify(usize::MAX);
    }
}

pub(crate) struct TaskRegistration {
    shared: Arc<Shared>,
}
impl Drop for TaskRegistration {
    fn drop(&mut self) {
        self.shared.remaining.fetch_sub(1, Ordering::AcqRel);
        self.shared.changed.notify(usize::MAX);
        if self.shared.closed.load(Ordering::Acquire) {
            self.shared.wake_all();
        }
    }
}

impl Shared {
    pub(crate) fn new(queues: &[[Worker<Runnable>; 2]], timer_capacity: usize) -> Self {
        Self {
            admission: Mutex::new(0),
            timer_capacity,
            timer_next: AtomicUsize::new(0),
            #[cfg(test)]
            paused: Mutex::new(None),
            workers_remaining: AtomicUsize::new(0),
            workers: queues
                .iter()
                .map(|queues| WorkerState::new(queues.each_ref().map(Worker::stealer)))
                .collect(),
            closed: AtomicBool::new(false),
            idle_workers: AtomicUsize::new(0),
            remaining: AtomicUsize::new(0),
            outstanding: AtomicUsize::new(0),
            changed: Event::new(),
        }
    }

    pub(crate) fn has_runnables(&self) -> bool {
        self.outstanding.load(Ordering::Acquire) & !NOTIFY_IDLE != 0
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) fn send_timer(&self, owner: usize, command: crate::worker_timers::Command) {
        let rejected = {
            let _admission = self.admission.lock().unwrap();
            if self.is_closed() {
                Some(command)
            } else {
                self.workers[owner].timers.push(command);
                None
            }
        };
        if let Some(command) = rejected {
            crate::worker_timers::rejected(command);
        } else {
            self.workers[owner].wake();
        }
    }

    pub(crate) fn finished(&self) -> bool {
        self.closed.load(Ordering::Acquire)
            && self.remaining.load(Ordering::Acquire) == 0
            && !self.has_runnables()
    }
    pub(crate) fn wake_all(&self) {
        for worker in &self.workers {
            worker.wake();
        }
    }
    pub(crate) fn wake_idle(&self, preferred: usize) {
        std::sync::atomic::fence(Ordering::SeqCst);
        if self.idle_workers.load(Ordering::SeqCst) == 0 {
            return;
        }
        if let Some(worker) = self.workers.get(preferred) {
            if worker.wake() {
                return;
            }
        }
        for worker in &self.workers {
            if worker.wake() {
                return;
            }
        }
    }
    pub(crate) fn retire_runnable(&self) {
        if self.outstanding.fetch_sub(1, Ordering::AcqRel) == (NOTIFY_IDLE | 1) {
            // Draining waits for quiescence; intermediate retirements cannot satisfy that condition.
            self.changed.notify(usize::MAX);
        }
        if self.finished() {
            self.wake_all();
        }
    }
}
impl TaskService {
    pub(crate) fn new(threads: usize, timer_capacity: usize) -> io::Result<Self> {
        let queues: Vec<_> = (0..threads)
            .map(|_| std::array::from_fn(|_| Worker::new_fifo()))
            .collect();
        let service = Self {
            shared: Arc::new(Shared::new(&queues, timer_capacity)),
        };
        for (identifier, queue) in queues.into_iter().enumerate() {
            let shared = Arc::clone(&service.shared);
            let registration = WorkerRegistration::new(&shared);
            thread::start("base-native-task", move || {
                let _registration = registration;
                worker::run(shared, identifier, queue)
            })?;
        }
        Ok(service)
    }
    pub(crate) fn with_drivers<D: base_scheduler_selector::SelectorDriver>(
        drivers: Vec<D>,
        timer_capacity: usize,
    ) -> io::Result<Self> {
        let queues: Vec<_> = (0..drivers.len())
            .map(|_| std::array::from_fn(|_| Worker::new_fifo()))
            .collect();
        let service = Self {
            shared: Arc::new(Shared::new(&queues, timer_capacity)),
        };
        for (identifier, (queue, driver)) in queues.into_iter().zip(drivers).enumerate() {
            let shared = Arc::clone(&service.shared);
            let registration = WorkerRegistration::new(&shared);
            thread::start("base-native-task", move || {
                let _registration = registration;
                worker::run_driven(shared, identifier, queue, driver)
            })?;
        }
        Ok(service)
    }
    pub(crate) fn spawn<F>(&self, future: F) -> Result<NativeTask<F::Output>, SpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let mut admission = self.shared.admission.lock().unwrap();
        if self.shared.closed.load(Ordering::Acquire) {
            drop(admission);
            return Err(SpawnError::SchedulerClosed);
        }
        // Choose placement before the first poll can attach persistent I/O state. Later wakes follow the task's last polling worker, and idle workers can still steal it.
        let worker = *admission;
        *admission = (worker + 1) % self.shared.workers.len();
        // Publish ownership while admission is locked, before close can let the workers exit.
        self.shared.remaining.fetch_add(1, Ordering::Release);
        let registration = TaskRegistration {
            shared: Arc::clone(&self.shared),
        };
        let shared = Arc::clone(&self.shared);
        let (runnable, task) = async_task::Builder::new()
            .metadata(TaskState {
                worker: AtomicUsize::new(worker),
            })
            .spawn(
                move |_| TaskFuture::new(future, registration),
                move |runnable| {
                    shared.outstanding.fetch_add(1, Ordering::AcqRel);
                    worker::schedule(&shared, runnable);
                },
            );
        drop(admission);
        runnable.schedule();
        Ok(NativeTask(task))
    }
    pub(crate) fn close(&self) {
        {
            let _admission = self.shared.admission.lock().unwrap();
            self.shared.closed.store(true, Ordering::Release);
        }
        self.shared.wake_all();
    }
    pub(crate) async fn drain(&self) {
        self.wait(true).await;
    }
    pub(crate) async fn wait(&self, all: bool) {
        loop {
            event_listener::listener!(self.shared.changed => listener);
            self.shared
                .outstanding
                .fetch_or(NOTIFY_IDLE, Ordering::AcqRel);
            // Observe registration retirement first. After close, no new registrations can appear; its release also precedes the final runnable retirement. Reading in reverse order could combine pre-publication quiescence with post-completion registration state.
            if (!all
                || (self.shared.remaining.load(Ordering::Acquire) == 0
                    && (!self.shared.is_closed()
                        || self.shared.workers_remaining.load(Ordering::Acquire) == 0)))
                && !self.shared.has_runnables()
            {
                return;
            }
            listener.await;
        }
    }
}
impl Drop for TaskService {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests;

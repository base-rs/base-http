// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{DirectTask, thread};
use async_task::Runnable;
use base_scheduler::{SpawnError, TaskError};
use event_listener::Event;
use std::{
    collections::{HashSet, VecDeque},
    future::Future,
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll},
};

pub(crate) struct State {
    closed: bool,
    next: usize,
    pub(crate) registered: HashSet<usize>,
    queue: VecDeque<Runnable>,
    active: usize,
}
pub(crate) struct Shared {
    pub(crate) state: Mutex<State>,
    ready: Condvar,
    changed: Event,
}
pub(crate) struct TaskService {
    pub(crate) shared: Arc<Shared>,
}
struct Registration {
    shared: Arc<Shared>,
    id: usize,
}
impl Drop for Registration {
    fn drop(&mut self) {
        self.shared
            .state
            .lock()
            .unwrap()
            .registered
            .remove(&self.id);
        self.shared.ready.notify_all();
        self.shared.changed.notify(usize::MAX);
    }
}

// async-task aborts if its future or stored output destructor unwinds. Keep both user-owned values behind guards, including when the task is abandoned.
struct TaskFuture<F: Future> {
    future: Option<Pin<Box<F>>>,
    _registration: Registration,
}
impl<F: Future> Unpin for TaskFuture<F> {}
impl<F: Future> Future for TaskFuture<F> {
    type Output = TaskOutcome<F::Output>;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut result = match catch_unwind(AssertUnwindSafe(|| {
            this.future.as_mut().unwrap().as_mut().poll(context)
        })) {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(output)) => Ok(output),
            Err(payload) => Err(TaskError::Panicked(payload)),
        };
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(this.future.take()))) {
            crate::panic::discard(result);
            result = Err(TaskError::Panicked(payload));
        }
        Poll::Ready(TaskOutcome(Some(result)))
    }
}
impl<F: Future> Drop for TaskFuture<F> {
    fn drop(&mut self) {
        crate::panic::discard(self.future.take());
    }
}
pub(crate) struct TaskOutcome<T>(pub(crate) Option<Result<T, TaskError>>);
impl<T> Drop for TaskOutcome<T> {
    fn drop(&mut self) {
        crate::panic::discard(self.0.take());
    }
}

impl TaskService {
    // Start here because Spawn has no I/O-error channel for worker-start failure.
    pub(crate) fn new(threads: usize) -> io::Result<Self> {
        let service = Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    closed: false,
                    next: 0,
                    registered: HashSet::new(),
                    queue: VecDeque::new(),
                    active: 0,
                }),
                ready: Condvar::new(),
                changed: Event::new(),
            }),
        };
        for _ in 0..threads {
            let shared = Arc::clone(&service.shared);
            thread::start("base-direct-task", move || work(shared))?;
        }
        Ok(service)
    }

    pub(crate) fn spawn<F>(&self, future: F) -> Result<DirectTask<F::Output>, SpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let mut state = self.shared.state.lock().unwrap();
        if state.closed {
            // User destructors must run outside the task registry lock.
            drop(state);
            return Err(SpawnError::SchedulerClosed);
        }
        let Some(next) = state.next.checked_add(1) else {
            drop(state);
            return Err(SpawnError::IdentifiersExhausted);
        };
        let id = state.next;
        state.next = next;
        state.registered.insert(id);
        let registration = Registration {
            shared: Arc::clone(&self.shared),
            id,
        };
        let shared = Arc::clone(&self.shared);
        let (runnable, task) = async_task::spawn(
            TaskFuture {
                future: Some(Box::pin(future)),
                _registration: registration,
            },
            move |runnable| {
                shared.state.lock().unwrap().queue.push_back(runnable);
                shared.ready.notify_one();
            },
        );
        drop(state);
        runnable.schedule();
        Ok(DirectTask(task))
    }

    pub(crate) fn close(&self) {
        self.shared.state.lock().unwrap().closed = true;
        self.shared.ready.notify_all();
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.shared.state.lock().unwrap().closed
    }

    pub(crate) async fn drain(&self) {
        self.wait(true).await;
    }

    pub(crate) async fn wait(&self, all: bool) {
        loop {
            event_listener::listener!(self.shared.changed => listener);
            {
                let state = self.shared.state.lock().unwrap();
                if state.queue.is_empty()
                    && state.active == 0
                    && (!all || state.registered.is_empty())
                {
                    return;
                }
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

fn work(shared: Arc<Shared>) {
    loop {
        let runnable = {
            let mut state = shared.state.lock().unwrap();
            loop {
                if let Some(runnable) = state.queue.pop_front() {
                    state.active += 1;
                    break runnable;
                }
                if state.closed && state.registered.is_empty() {
                    return;
                }
                state = shared.ready.wait(state).unwrap();
            }
        };
        // A panicking user destructor must not kill a worker or strand drain waiters.
        crate::panic::contain(|| {
            runnable.run();
        });
        shared.state.lock().unwrap().active -= 1;
        shared.changed.notify(usize::MAX);
    }
}

#[cfg(test)]
mod tests;

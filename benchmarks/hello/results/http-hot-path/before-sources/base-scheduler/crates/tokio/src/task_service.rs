// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::TokioTask;
use base_scheduler::{SpawnError, TaskError};
use event_listener::Event;
use std::{
    collections::BTreeMap,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

#[cfg(not(test))]
type Entry = ();
#[cfg(test)]
type Entry = Arc<tests::Status>;

struct State {
    closed: bool,
    next: usize,
    registered: BTreeMap<usize, Entry>,
    #[cfg(test)]
    history: Vec<Entry>,
}
struct Shared {
    state: Mutex<State>,
    changed: Event,
}
pub(crate) struct TaskService {
    runtime: tokio::runtime::Handle,
    shared: Arc<Shared>,
}
struct Registration {
    shared: Arc<Shared>,
    id: usize,
}
impl Drop for Registration {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().unwrap();
        #[cfg(test)]
        tests::completed(&state, self.id);
        state.registered.remove(&self.id);
        drop(state);
        self.shared.changed.notify(usize::MAX);
    }
}

// Keep arbitrary user destructor panics inside the adapter, including abandoned futures and unconsumed outputs. Moving this wrapper never moves the pinned user future.
struct TaskFuture<F: Future> {
    future: Option<Pin<Box<F>>>,
    _registration: Registration,
}
impl<F: Future> Unpin for TaskFuture<F> {}
impl<F: Future> Future for TaskFuture<F> {
    type Output = TaskOutcome<F::Output>;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        #[cfg(test)]
        let _poll = tests::polling(&this._registration);
        #[cfg(test)]
        let waker = tests::task_waker(&_poll, context.waker());
        #[cfg(test)]
        let context = &mut Context::from_waker(&waker);
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
        Poll::Ready(TaskOutcome(
            Some(result),
            #[cfg(test)]
            tests::output(&this._registration),
        ))
    }
}
impl<F: Future> Drop for TaskFuture<F> {
    fn drop(&mut self) {
        crate::panic::discard(self.future.take());
    }
}
pub(crate) struct TaskOutcome<T>(
    pub(crate) Option<Result<T, TaskError>>,
    #[cfg(test)] Option<Arc<tests::Status>>,
);
impl<T> Drop for TaskOutcome<T> {
    fn drop(&mut self) {
        crate::panic::discard(self.0.take());
        #[cfg(test)]
        tests::retired(self.1.as_ref());
    }
}

impl TaskService {
    pub(crate) fn new(runtime: tokio::runtime::Handle) -> Self {
        Self {
            runtime,
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    closed: false,
                    next: 0,
                    registered: BTreeMap::new(),
                    #[cfg(test)]
                    history: Vec::new(),
                }),
                changed: Event::new(),
            }),
        }
    }
    pub(crate) fn spawn<F>(&self, future: F) -> Result<TokioTask<F::Output>, SpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let mut state = self.shared.state.lock().unwrap();
        if state.closed {
            drop(state);
            return Err(SpawnError::SchedulerClosed);
        }
        let Some(next) = state.next.checked_add(1) else {
            drop(state);
            return Err(SpawnError::IdentifiersExhausted);
        };
        let id = state.next;
        state.next = next;
        #[cfg(not(test))]
        state.registered.insert(id, ());
        #[cfg(test)]
        state.registered.insert(id, Entry::default());
        #[cfg(test)]
        let status = tests::record(&mut state, &self.shared, id);
        let registration = Registration {
            shared: Arc::clone(&self.shared),
            id,
        };
        // A stopped host may destroy the future synchronously inside spawn. Its registration must already be visible, and the registry lock must be released first.
        drop(state);
        let inner = self.runtime.spawn(TaskFuture {
            future: Some(Box::pin(future)),
            _registration: registration,
        });
        Ok(TokioTask {
            inner,
            #[cfg(test)]
            status,
        })
    }
    pub(crate) fn close(&self) {
        self.shared.state.lock().unwrap().closed = true;
    }
    pub(crate) async fn drain(&self) {
        loop {
            event_listener::listener!(self.shared.changed => listener);
            if self.shared.state.lock().unwrap().registered.is_empty() {
                return;
            }
            listener.await;
        }
    }
    #[cfg(test)]
    pub(crate) async fn wait(&self, all: bool) {
        loop {
            event_listener::listener!(self.shared.changed => listener);
            if tests::settled(&self.shared.state.lock().unwrap(), all) {
                return;
            }
            listener.await;
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;

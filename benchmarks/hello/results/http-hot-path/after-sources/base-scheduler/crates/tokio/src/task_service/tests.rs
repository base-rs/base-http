// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::{TokioScheduler, test_support};
use base_scheduler::{Cancellation, Spawn};
use base_scheduler_conformance::SpawnFixture;
use futures_lite::future::block_on;

use std::sync::atomic::{AtomicBool, Ordering};
#[derive(Default)]
pub(crate) struct Status {
    polled: AtomicBool,
    completed: AtomicBool,
    output: AtomicBool,
    retired: AtomicBool,
    waiting: AtomicBool,
    notified: AtomicBool,
    shared: Mutex<std::sync::Weak<Shared>>,
    active: AtomicBool,
    ready: AtomicBool,
    pub(crate) abandoned: AtomicBool,
}
pub(super) fn record(state: &mut State, shared: &Arc<Shared>, id: usize) -> Arc<Status> {
    let status = state.registered[&id].clone();
    *status.shared.lock().unwrap() = Arc::downgrade(shared);
    state.history.push(status.clone());
    status
}
pub(super) fn completed(state: &State, id: usize) {
    state.registered[&id]
        .completed
        .store(true, Ordering::SeqCst);
}
pub(super) fn settled(state: &State, all: bool) -> bool {
    let idle = state.registered.is_empty()
        || (!all
            && state.registered.values().all(|status| {
                status.polled.load(Ordering::SeqCst)
                    && !status.active.load(Ordering::SeqCst)
                    && !status.ready.load(Ordering::SeqCst)
                    // A finished poll can return before Tokio destroys the task future and notifies its join handle.
                    && !status.output.load(Ordering::SeqCst)
                    && !status.abandoned.load(Ordering::SeqCst)
            }));
    idle && state.history.iter().all(|status| {
        (!status.completed.load(Ordering::SeqCst)
            || !status.waiting.load(Ordering::SeqCst)
            || status.notified.load(Ordering::SeqCst))
            && (!status.abandoned.load(Ordering::SeqCst)
                || !status.output.load(Ordering::SeqCst)
                || status.retired.load(Ordering::SeqCst))
    })
}
pub(super) fn output(registration: &Registration) -> Option<Arc<Status>> {
    let status = registration.shared.state.lock().unwrap().registered[&registration.id].clone();
    status.output.store(true, Ordering::SeqCst);
    Some(status)
}
pub(super) fn retired(status: Option<&Arc<Status>>) {
    if let Some(status) = status {
        status.retired.store(true, Ordering::SeqCst);
        if let Some(shared) = status.shared.lock().unwrap().upgrade() {
            shared.changed.notify(usize::MAX);
        }
    }
}
pub(crate) fn abandon(status: &Status) {
    status.waiting.store(false, Ordering::SeqCst);
    status.abandoned.store(true, Ordering::SeqCst);
}
struct JoinWake {
    original: std::task::Waker,
    status: Arc<Status>,
}
impl std::task::Wake for JoinWake {
    fn wake(self: Arc<Self>) {
        self.original.wake_by_ref();
        self.status.notified.store(true, Ordering::SeqCst);
        if let Some(shared) = self.status.shared.lock().unwrap().upgrade() {
            shared.changed.notify(usize::MAX);
        }
    }
}
pub(crate) fn join_waker(status: &Arc<Status>, original: &std::task::Waker) -> std::task::Waker {
    status.waiting.store(true, Ordering::SeqCst);
    status.notified.store(false, Ordering::SeqCst);
    std::task::Waker::from(Arc::new(JoinWake {
        original: original.clone(),
        status: status.clone(),
    }))
}
pub(crate) fn joined<T>(status: &Status, poll: &Poll<T>) {
    if poll.is_ready() {
        status.waiting.store(false, Ordering::SeqCst);
    }
}
struct TaskWake {
    original: std::task::Waker,
    status: Arc<Status>,
}
impl std::task::Wake for TaskWake {
    fn wake(self: Arc<Self>) {
        self.status.ready.store(true, Ordering::SeqCst);
        self.original.wake_by_ref();
    }
}
pub(super) fn task_waker(poll: &PollGuard, original: &std::task::Waker) -> std::task::Waker {
    std::task::Waker::from(Arc::new(TaskWake {
        original: original.clone(),
        status: poll.status.clone(),
    }))
}
pub(super) struct PollGuard {
    shared: Arc<Shared>,
    status: Arc<Status>,
}
pub(super) fn polling(registration: &Registration) -> PollGuard {
    let status = registration.shared.state.lock().unwrap().registered[&registration.id].clone();
    status.active.store(true, Ordering::SeqCst);
    status.ready.store(false, Ordering::SeqCst);
    PollGuard {
        shared: registration.shared.clone(),
        status,
    }
}
impl Drop for PollGuard {
    fn drop(&mut self) {
        self.status.polled.store(true, Ordering::SeqCst);
        self.status.active.store(false, Ordering::SeqCst);
        self.shared.changed.notify(usize::MAX);
    }
}

impl SpawnFixture for TokioScheduler {
    type Scheduler = Self;
    fn scheduler(&self) -> &Self {
        self
    }
    fn registration_check(&self) -> impl FnOnce() + Send + 'static {
        let shared = Arc::clone(&self.tasks.shared);
        move || assert_eq!(shared.state.lock().unwrap().registered.len(), 1)
    }
    async fn settle(&self) {
        self.tasks.wait(false).await;
    }
    fn close(&self) {
        self.tasks.close();
    }
}
fn run(test: impl AsyncFnOnce(&TokioScheduler) + Send + 'static) {
    test_support::run(move || {
        let scheduler = test_support::scheduler();
        let result = catch_unwind(AssertUnwindSafe(|| block_on(test(&scheduler))));
        block_on(scheduler.shutdown());
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
mod conformance {
    base_scheduler_conformance::spawn_tests!(run: super::run);
}
mod dispatch {
    base_scheduler_conformance::spawn_dispatch_tests!(run: super::run);
}

#[test]
fn completed_poll_waits_for_registration_retirement_and_join_notification() {
    let status = Arc::new(Status::default());
    status.polled.store(true, Ordering::SeqCst);
    status.output.store(true, Ordering::SeqCst);
    status.waiting.store(true, Ordering::SeqCst);
    let mut state = State {
        closed: false,
        next: 1,
        registered: BTreeMap::from([(0, status.clone())]),
        history: vec![status.clone()],
    };
    assert!(!settled(&state, false));
    completed(&state, 0);
    state.registered.remove(&0);
    assert!(!settled(&state, false));
    status.notified.store(true, Ordering::SeqCst);
    assert!(settled(&state, false));
}

#[test]
fn identifier_exhaustion_rejects_without_polling() {
    let scheduler = test_support::scheduler();
    scheduler.tasks.shared.state.lock().unwrap().next = usize::MAX;
    assert!(matches!(
        scheduler.spawn(async {}),
        Err(SpawnError::IdentifiersExhausted)
    ));
    block_on(scheduler.shutdown());
}

#[test]
fn tasks_progress_without_occupying_syscall_workers_and_survive_admission_close() {
    test_support::run(|| {
        block_on(async {
            use base_scheduler::Blocking;
            let scheduler = test_support::scheduler();
            let token = Cancellation::new();
            let child = token.clone();
            let task = scheduler
                .spawn(async move {
                    child.cancelled().await;
                    42
                })
                .unwrap();
            scheduler.tasks.wait(false).await;
            let result = scheduler
                .spawn_blocking(|| 73, &Cancellation::never())
                .await
                .unwrap();
            assert_eq!(result.await.unwrap(), 73);
            scheduler.tasks.close();
            token.cancel();
            assert_eq!(task.await.unwrap(), 42);
            scheduler.shutdown().await;
        })
    });
}

#[test]
fn future_destructor_panic_is_reported_and_worker_stays_available() {
    struct Panics;
    impl Future for Panics {
        type Output = ();
        fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<()> {
            Poll::Ready(())
        }
    }
    impl Drop for Panics {
        fn drop(&mut self) {
            std::panic::panic_any(String::from("future destructor"));
        }
    }
    run(async |scheduler| {
        let Err(TaskError::Panicked(payload)) = scheduler.spawn(Panics).unwrap().await else {
            panic!("lost destructor panic");
        };
        assert_eq!(*payload.downcast::<String>().unwrap(), "future destructor");
        assert_eq!(scheduler.spawn(async { 17 }).unwrap().await.unwrap(), 17);
    });
}

#[test]
fn abandoned_task_destructor_panics_do_not_abort_or_stop_workers() {
    const CHILD: &str = "PROTOCOL_TOKIO_TASK_DROP_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "task_service::tests::abandoned_task_destructor_panics_do_not_abort_or_stop_workers",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    struct PanicsOnDrop;
    impl Drop for PanicsOnDrop {
        fn drop(&mut self) {
            panic!("abandoned destructor");
        }
    }
    struct Pending;
    impl Future for Pending {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }
    impl Drop for Pending {
        fn drop(&mut self) {
            // The payload also panics when destroyed: swallowing only the first unwind is insufficient.
            std::panic::panic_any(PanicsOnDrop);
        }
    }
    struct Ready;
    impl Future for Ready {
        type Output = PanicsOnDrop;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(PanicsOnDrop)
        }
    }
    impl Drop for Ready {
        fn drop(&mut self) {
            panic!("completed future destructor");
        }
    }
    run(async |scheduler| {
        let pending = scheduler.spawn(Pending).unwrap();
        scheduler.tasks.wait(false).await;
        drop(pending);
        scheduler.tasks.drain().await;

        let output = scheduler.spawn(async { PanicsOnDrop }).unwrap();
        scheduler.tasks.wait(false).await;
        drop(output);

        let panic_payload = scheduler
            .spawn(async { std::panic::panic_any(PanicsOnDrop) })
            .unwrap();
        scheduler.tasks.wait(false).await;
        drop(panic_payload);

        // Both the completed future and its unconsumed output panic during destruction.
        assert!(matches!(
            scheduler.spawn(Ready).unwrap().await,
            Err(TaskError::Panicked(_))
        ));
        assert_eq!(scheduler.spawn(async { 42 }).unwrap().await.unwrap(), 42);
    });
}

fn run_current_thread(test: impl AsyncFnOnce(&TokioScheduler) + Send + 'static) {
    test_support::run(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let scheduler = TokioScheduler::new(runtime.handle().clone());
        let result = catch_unwind(AssertUnwindSafe(|| runtime.block_on(test(&scheduler))));
        runtime.block_on(scheduler.shutdown());
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
mod current_thread {
    base_scheduler_conformance::spawn_tests!(run: super::run_current_thread);
}

#[test]
fn stopped_host_destroys_submissions_without_retaining_the_registry_lock() {
    test_support::run(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let scheduler = TokioScheduler::new(runtime.handle().clone());
        let task = scheduler.spawn(std::future::pending::<()>()).unwrap();
        drop(runtime);
        assert!(matches!(block_on(task), Err(TaskError::Cancelled)));
        let task = scheduler.spawn(std::future::pending::<()>()).unwrap();
        assert!(matches!(block_on(task), Err(TaskError::Cancelled)));
        block_on(scheduler.shutdown());
        assert!(
            scheduler
                .tasks
                .shared
                .state
                .lock()
                .unwrap()
                .registered
                .is_empty()
        );
    });
}

#[test]
fn shutdown_waits_for_cooperative_task_exit_and_leaves_host_running() {
    test_support::run(|| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let scheduler = TokioScheduler::new(runtime.handle().clone());
        block_on(async {
            let token = Cancellation::new();
            let child = token.clone();
            let task = scheduler
                .spawn(async move {
                    child.cancelled().await;
                    42
                })
                .unwrap();
            scheduler.tasks.wait(false).await;
            let mut shutdown = Box::pin(scheduler.shutdown());
            assert!(
                futures_lite::future::poll_once(&mut shutdown)
                    .await
                    .is_none()
            );
            assert!(matches!(
                scheduler.spawn(async {}),
                Err(SpawnError::SchedulerClosed)
            ));
            token.cancel();
            assert_eq!(task.await.unwrap(), 42);
            shutdown.await;
            assert_eq!(runtime.spawn(async { 73 }).await.unwrap(), 73);
        });
    });
}

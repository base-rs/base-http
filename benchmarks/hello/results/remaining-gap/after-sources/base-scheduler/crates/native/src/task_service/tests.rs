// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::{NativeScheduler, test_support};
use base_scheduler::{Cancellation, Spawn, TaskError};
use base_scheduler_conformance::SpawnFixture;
use futures_lite::future::block_on;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    task::{Context, Poll},
};

impl SpawnFixture for NativeScheduler {
    type Scheduler = Self;
    fn scheduler(&self) -> &Self {
        self
    }
    fn registration_check(&self) -> impl FnOnce() + Send + 'static {
        let shared = Arc::clone(&self.tasks.shared);
        move || assert_eq!(shared.remaining.load(Ordering::Acquire), 1)
    }
    async fn settle(&self) {
        self.tasks.wait(false).await;
    }
    fn close(&self) {
        self.tasks.close();
    }
}
fn run(test: impl AsyncFnOnce(&NativeScheduler) + Send + 'static) {
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

// Keep workers at their admission boundary so stealing cannot obscure initial placement.
fn queued_service(workers: usize) -> (TaskService, Vec<[Worker<Runnable>; 2]>) {
    let queues = (0..workers)
        .map(|_| std::array::from_fn(|_| Worker::new_fifo()))
        .collect::<Vec<_>>();
    let service = TaskService {
        shared: Arc::new(Shared::new(&queues, 8)),
    };
    (service, queues)
}

#[test]
fn initial_tasks_are_distributed_before_their_first_poll() {
    for workers in [1, 3] {
        let (service, _queues) = queued_service(workers);
        let tasks = (0..workers * 3)
            .map(|index| service.spawn(async move { index }).unwrap())
            .collect::<Vec<_>>();
        for (index, worker) in service.shared.workers.iter().enumerate() {
            for _ in 0..3 {
                let runnable = worker.incoming.steal().success().unwrap();
                assert_eq!(runnable.metadata().worker.load(Ordering::Relaxed), index);
                runnable.run();
                service.shared.retire_runnable();
            }
            assert!(worker.incoming.is_empty());
        }
        for (expected, task) in tasks.into_iter().enumerate() {
            assert_eq!(block_on(task).unwrap(), expected);
        }
        block_on(service.drain());
    }
}

#[test]
fn concurrent_spawners_share_initial_distribution() {
    let (service, _queues) = queued_service(3);
    let tasks = std::thread::scope(|scope| {
        (0..6)
            .map(|_| {
                scope.spawn(|| {
                    (0..4)
                        .map(|_| service.spawn(async { 42 }).unwrap())
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .flat_map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>()
    });
    for worker in &service.shared.workers {
        for _ in 0..8 {
            worker.incoming.steal().success().unwrap().run();
            service.shared.retire_runnable();
        }
        assert!(worker.incoming.is_empty());
    }
    for task in tasks {
        assert_eq!(block_on(task).unwrap(), 42);
    }
    block_on(service.drain());
}

#[test]
fn task_wakes_retain_placement_without_advancing_admission() {
    let (service, _queues) = queued_service(2);
    let mut polled = false;
    let task = service
        .spawn(std::future::poll_fn(move |context| {
            if polled {
                return Poll::Ready(42);
            }
            polled = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }))
        .unwrap();
    let first = &service.shared.workers[0].incoming;
    first.steal().success().unwrap().run();
    service.shared.retire_runnable();
    assert!(service.shared.workers[1].incoming.is_empty());
    first.steal().success().unwrap().run();
    service.shared.retire_runnable();
    assert_eq!(block_on(task).unwrap(), 42);

    let next = service.spawn(async { 73 }).unwrap();
    assert!(first.is_empty());
    service.shared.workers[1]
        .incoming
        .steal()
        .success()
        .unwrap()
        .run();
    service.shared.retire_runnable();
    assert_eq!(block_on(next).unwrap(), 73);
    block_on(service.drain());
}

#[test]
fn worker_startup_failures_are_reported() {
    for fail_after in [0, 1] {
        thread::FAIL_AFTER.set(Some(fail_after));
        assert!(
            NativeScheduler::with_options(crate::NativeOptions {
                workers: 2,
                threads: 2,
                capacity: 1,
                ..Default::default()
            })
            .is_err()
        );
    }
}

#[test]
fn rejected_future_destructors_can_close_admission_again() {
    struct CloseOnDrop(Arc<TaskService>);
    impl Drop for CloseOnDrop {
        fn drop(&mut self) {
            self.0.close();
        }
    }
    test_support::run(|| {
        let service = Arc::new(TaskService::new(1, 8).unwrap());
        service.close();
        let guard = CloseOnDrop(Arc::clone(&service));
        assert!(matches!(
            service.spawn(async move { drop(guard) }),
            Err(SpawnError::SchedulerClosed)
        ));
        block_on(service.drain());
    });
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
    const CHILD: &str = "CORE_NATIVE_TASK_DROP_CHILD";
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

#[test]
fn local_work_can_be_stolen_while_its_worker_is_busy() {
    test_support::run(|| {
        let scheduler = Arc::new(
            NativeScheduler::with_options(crate::NativeOptions {
                workers: 2,
                ..Default::default()
            })
            .unwrap(),
        );
        let inner = Arc::clone(&scheduler);
        let task = scheduler
            .spawn(async move {
                let (done, completion) = std::sync::mpsc::channel();
                // A full admission round places one child on this worker, regardless of where the parent was first polled. It must be stolen while this poll blocks.
                let children = (0..2)
                    .map(|_| {
                        let done = done.clone();
                        inner
                            .spawn(async move {
                                done.send(std::thread::current().id()).unwrap();
                                42
                            })
                            .unwrap()
                    })
                    .collect::<Vec<_>>();
                for _ in 0..2 {
                    let other_thread = completion
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                    assert_ne!(other_thread, std::thread::current().id());
                }
                for child in children {
                    assert_eq!(child.await.unwrap(), 42);
                }
            })
            .unwrap();
        block_on(task).unwrap();
        block_on(scheduler.shutdown());
    });
}

#[test]
fn busy_local_queue_regularly_services_external_submissions() {
    test_support::run(|| {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let scheduler = test_support::scheduler();
        let (started, start) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let observed = Arc::new(AtomicBool::new(false));
        let polls = Arc::new(AtomicUsize::new(0));
        let result = Arc::clone(&observed);
        let count = Arc::clone(&polls);
        let hot = scheduler
            .spawn(std::future::poll_fn(move |context| {
                let poll = count.fetch_add(1, Ordering::Relaxed);
                if poll == 0 {
                    started.send(()).unwrap();
                    released
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                }
                if result.load(Ordering::Acquire) || poll == 1000 {
                    return Poll::Ready(());
                }
                context.waker().wake_by_ref();
                Poll::Pending
            }))
            .unwrap();
        start
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let other = scheduler
            .spawn(async move {
                observed.store(true, Ordering::Release);
            })
            .unwrap();
        release.send(()).unwrap();
        block_on(hot).unwrap();
        assert!(
            polls.load(Ordering::Relaxed) < 1000,
            "external work was starved by local wakes"
        );
        block_on(other).unwrap();
        block_on(scheduler.shutdown());
    });
}

#[test]
fn submissions_from_another_native_worker_use_the_selected_scheduler() {
    test_support::run(|| {
        let first = test_support::scheduler();
        let second = Arc::new(test_support::scheduler());
        let other = Arc::clone(&second);
        let task = first
            .spawn(async move {
                let submitting = std::thread::current().id();
                assert_ne!(
                    other
                        .spawn(async { std::thread::current().id() })
                        .unwrap()
                        .await
                        .unwrap(),
                    submitting
                );
            })
            .unwrap();
        block_on(task).unwrap();
        block_on(first.shutdown());
        block_on(second.shutdown());
    });
}

#[test]
fn intermediate_runnable_retirement_does_not_wake_drain_waiters() {
    use std::task::{Wake, Waker};

    #[derive(Default)]
    struct WakeCount(AtomicUsize);
    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let (service, _queues) = queued_service(1);
    let tasks = (0..2)
        .map(|_| service.spawn(std::future::pending::<()>()).unwrap())
        .collect::<Vec<_>>();
    let wake = Arc::new(WakeCount::default());
    let waker = Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);
    let mut waiting = std::pin::pin!(service.wait(false));
    assert!(waiting.as_mut().poll(&mut context).is_pending());

    for remaining in [1, 0] {
        service.shared.workers[0]
            .incoming
            .steal()
            .success()
            .unwrap()
            .run();
        service.shared.retire_runnable();
        assert_eq!(wake.0.load(Ordering::Relaxed), usize::from(remaining == 0));
    }
    assert!(waiting.as_mut().poll(&mut context).is_ready());
    drop(tasks);
    for _ in 0..2 {
        service.shared.workers[0]
            .incoming
            .steal()
            .success()
            .unwrap()
            .run();
        service.shared.retire_runnable();
    }
    block_on(service.drain());
}

#[test]
fn quiescence_before_listening_and_concurrent_abandoned_waiters_preserve_progress() {
    for run_first in [false, true] {
        let (service, _queues) = queued_service(1);
        let task = service.spawn(std::future::pending::<()>()).unwrap();
        let run_one = || {
            service.shared.workers[0]
                .incoming
                .steal()
                .success()
                .unwrap()
                .run();
            service.shared.retire_runnable();
        };
        if run_first {
            run_one();
        }
        let mut first = Box::pin(service.wait(false));
        let mut second = Box::pin(service.wait(false));
        assert_eq!(
            block_on(futures_lite::future::poll_once(&mut first)).is_some(),
            run_first
        );
        assert_eq!(
            block_on(futures_lite::future::poll_once(&mut second)).is_some(),
            run_first
        );
        drop(first);
        if !run_first {
            run_one();
            assert!(block_on(futures_lite::future::poll_once(&mut second)).is_some());
        }
        assert!(!service.shared.has_runnables());
        drop(second);
        drop(task);
        run_one();
        service.close();
        assert!(service.shared.finished());
        block_on(service.drain());
    }
}

mod pinned_tasks {
    use super::*;
    use std::{cell::Cell, marker::PhantomPinned};

    struct PinnedFuture {
        address: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
        polls: Cell<usize>,
        panic_poll: bool,
        panic_drop: bool,
        _pin: PhantomPinned,
    }
    impl PinnedFuture {
        fn check_address(&self) {
            let address = self as *const Self as usize;
            match self
                .address
                .compare_exchange(0, address, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => {}
                Err(previous) => assert_eq!(previous, address, "pinned future moved"),
            }
        }
    }
    impl Future for PinnedFuture {
        type Output = usize;

        fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<usize> {
            self.check_address();
            assert!(!self.panic_poll, "poll panic");
            let polls = self.polls.get();
            self.polls.set(polls + 1);
            if polls == 0 {
                context.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(42)
            }
        }
    }
    impl Drop for PinnedFuture {
        fn drop(&mut self) {
            self.check_address();
            assert_eq!(self.drops.fetch_add(1, Ordering::SeqCst), 0);
            assert!(!self.panic_drop, "destructor panic");
        }
    }

    fn run_on_another_thread(service: &TaskService) {
        let shared = Arc::clone(&service.shared);
        let runnable = loop {
            match shared.workers[0].incoming.steal() {
                crossbeam_deque::Steal::Success(runnable) => break runnable,
                crossbeam_deque::Steal::Retry => continue,
                crossbeam_deque::Steal::Empty => panic!("expected a scheduled task"),
            }
        };
        std::thread::spawn(move || {
            runnable.run();
            shared.retire_runnable();
        })
        .join()
        .unwrap();
    }

    #[test]
    fn address_is_stable_through_thread_changes_completion_and_abandonment() {
        for abandon in [false, true] {
            for panic_drop in [false, true] {
                let (service, _queues) = queued_service(1);
                let drops = Arc::new(AtomicUsize::new(0));
                let mut task = Some(
                    service
                        .spawn(PinnedFuture {
                            address: Arc::new(AtomicUsize::new(0)),
                            drops: Arc::clone(&drops),
                            polls: Cell::new(0),
                            panic_poll: false,
                            panic_drop,
                            _pin: PhantomPinned,
                        })
                        .unwrap(),
                );
                run_on_another_thread(&service);
                assert_eq!(drops.load(Ordering::SeqCst), 0);
                if abandon {
                    drop(task.take());
                }
                run_on_another_thread(&service);
                if let Some(task) = task {
                    let result = block_on(task);
                    if panic_drop {
                        assert!(matches!(result, Err(TaskError::Panicked(_))));
                    } else {
                        assert_eq!(result.unwrap(), 42);
                    }
                }
                block_on(service.drain());
                assert_eq!(drops.load(Ordering::SeqCst), 1);
            }
        }
    }

    #[test]
    fn poll_and_destructor_panics_retire_the_future_once() {
        for panic_drop in [false, true] {
            let (service, _queues) = queued_service(1);
            let drops = Arc::new(AtomicUsize::new(0));
            let task = service
                .spawn(PinnedFuture {
                    address: Arc::new(AtomicUsize::new(0)),
                    drops: Arc::clone(&drops),
                    polls: Cell::new(0),
                    panic_poll: true,
                    panic_drop,
                    _pin: PhantomPinned,
                })
                .unwrap();
            run_on_another_thread(&service);
            assert!(matches!(block_on(task), Err(TaskError::Panicked(_))));
            block_on(service.drain());
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
    }
}

#[test]
fn independently_driven_selectors_also_report_worker_startup_failures() {
    for fail_after in [0, 1] {
        thread::FAIL_AFTER.set(Some(fail_after));
        assert!(TaskService::new(2, 8).is_err());
        thread::FAIL_AFTER.set(None);
    }
}

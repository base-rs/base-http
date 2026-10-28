// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{WorkerState, finish_idle_search, search_result, take_stolen, yield_on_retry};
use crate::task_service::{Runnable, TaskState};
use crossbeam_deque::{Steal, Worker};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
struct Interrupts(AtomicUsize);

impl std::task::Wake for Interrupts {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn local_worker_wake_clears_sleeping_without_interrupting_its_driver() {
    use std::{sync::Arc, task::Waker};
    let worker = worker_state();
    worker.thread.set(std::thread::current()).unwrap();
    let interrupts = Arc::new(Interrupts::default());
    worker
        .driver_waker
        .set(Waker::from(Arc::clone(&interrupts)))
        .unwrap();
    worker.sleeping.store(true, Ordering::SeqCst);

    assert!(worker.wake());
    assert!(!worker.sleeping.load(Ordering::SeqCst));
    assert_eq!(interrupts.0.load(Ordering::SeqCst), 0);
    assert!(!worker.wake());
}

#[test]
fn remote_wake_retains_the_unpark_token_and_interrupts_the_driver_once() {
    use std::{
        sync::{Arc, mpsc},
        task::Waker,
        thread,
        time::Duration,
    };
    let worker = Arc::new(worker_state());
    let interrupts = Arc::new(Interrupts::default());
    worker
        .driver_waker
        .set(Waker::from(Arc::clone(&interrupts)))
        .unwrap();
    let (ready, started) = mpsc::channel();
    let released = Arc::new(AtomicBool::new(false));
    let release = Arc::clone(&released);
    let (done, finished) = mpsc::channel();
    let target = Arc::clone(&worker);
    let handle = thread::spawn(move || {
        target.thread.set(thread::current()).unwrap();
        target.sleeping.store(true, Ordering::SeqCst);
        ready.send(()).unwrap();
        // Do not use a channel wait here: its implementation may consume Thread's unpark token.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !released.load(Ordering::Acquire) {
            assert!(std::time::Instant::now() < deadline);
            thread::yield_now();
        }
        // The test publishes the wake before park, so progress depends on retaining its token.
        thread::park_timeout(Duration::from_secs(30));
        done.send(()).unwrap();
    });
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(worker.wake());
    assert!(!worker.wake());
    assert!(!worker.sleeping.load(Ordering::SeqCst));
    assert_eq!(interrupts.0.load(Ordering::SeqCst), 1);
    release.store(true, Ordering::Release);
    finished.recv_timeout(Duration::from_secs(5)).unwrap();
    handle.join().unwrap();
}

fn worker_state() -> WorkerState {
    let queues: [Worker<Runnable>; 2] = std::array::from_fn(|_| Worker::new_fifo());
    WorkerState::new(queues.each_ref().map(Worker::stealer))
}

fn runnable() -> Runnable {
    let state = TaskState {
        worker: AtomicUsize::new(0),
    };
    let (runnable, _task) = async_task::Builder::new()
        .metadata(state)
        .spawn(|_| std::future::pending::<()>(), |_| {});
    runnable
}

#[test]
fn records_retry_separately_from_empty_and_success() {
    let mut retry = false;
    assert_eq!(take_stolen(Steal::<()>::Empty, &mut retry), None);
    assert!(!retry);

    assert_eq!(take_stolen(Steal::<()>::Retry, &mut retry), None);
    assert!(retry);

    assert_eq!(take_stolen(Steal::Success(()), &mut retry), Some(()));
    assert!(retry);
}

#[test]
fn search_reports_retry_only_when_a_queue_raced() {
    assert!(matches!(search_result(true), Steal::Retry));
    assert!(matches!(search_result(false), Steal::Empty));
}

#[test]
fn worker_yields_after_a_concurrent_steal_retry() {
    assert!(matches!(
        yield_on_retry(Steal::<crate::task_service::Runnable>::Retry),
        Steal::Retry
    ));
}

#[test]
fn idle_retry_restores_worker_sleep_state() {
    let worker = worker_state();
    let idle_workers = AtomicUsize::new(1);
    worker.sleeping.store(true, Ordering::SeqCst);

    assert!(
        finish_idle_search(
            &worker,
            &idle_workers,
            Steal::Retry,
            || panic!("retry results do not check whether the worker should park"),
            || panic!("retry results do not park"),
        )
        .is_none()
    );
    assert!(!worker.sleeping.load(Ordering::SeqCst));
    assert_eq!(idle_workers.load(Ordering::SeqCst), 0);
}

#[test]
fn idle_success_restores_worker_state_and_returns_the_runnable() {
    let worker = worker_state();
    let idle_workers = AtomicUsize::new(1);
    worker.sleeping.store(true, Ordering::SeqCst);

    let runnable = finish_idle_search(
        &worker,
        &idle_workers,
        Steal::Success(runnable()),
        || panic!("successful results do not check whether the worker should park"),
        || panic!("successful results do not park"),
    )
    .expect("successful steal was discarded");
    assert!(!worker.sleeping.load(Ordering::SeqCst));
    assert_eq!(idle_workers.load(Ordering::SeqCst), 0);
    drop(runnable);
}

#[test]
fn idle_empty_search_skips_park_after_shutdown() {
    let worker = worker_state();
    let idle_workers = AtomicUsize::new(1);
    worker.sleeping.store(true, Ordering::SeqCst);

    let result = finish_idle_search(
        &worker,
        &idle_workers,
        Steal::Empty,
        || false,
        || panic!("a shut down scheduler must not park its worker"),
    );

    assert!(result.is_none());
    assert!(!worker.sleeping.load(Ordering::SeqCst));
    assert_eq!(idle_workers.load(Ordering::SeqCst), 0);
}

#[test]
fn idle_empty_search_parks_while_scheduler_is_running() {
    let worker = worker_state();
    let idle_workers = AtomicUsize::new(1);
    let parked = AtomicBool::new(false);
    worker.sleeping.store(true, Ordering::SeqCst);

    let result = finish_idle_search(
        &worker,
        &idle_workers,
        Steal::Empty,
        || true,
        || parked.store(true, Ordering::SeqCst),
    );

    assert!(result.is_none());
    assert!(parked.load(Ordering::SeqCst));
    assert!(!worker.sleeping.load(Ordering::SeqCst));
    assert_eq!(idle_workers.load(Ordering::SeqCst), 0);
}

#[test]
fn batch_search_admits_incoming_tasks_before_local_work() {
    use super::{LocalWorker, search};
    use crate::task_service::{Shared, TaskService};
    use futures_lite::future::block_on;
    use std::sync::Arc;
    let queues = std::array::from_fn(|_| Worker::new_fifo());
    let shared = Arc::new(Shared::new(std::slice::from_ref(&queues), 8));
    let service = TaskService {
        shared: Arc::clone(&shared),
    };
    let [ready, current] = queues;
    let local = LocalWorker {
        identifier: 0,
        scheduler: Arc::downgrade(&shared),
        ready,
        current,
        timers: base_scheduler_timers::local::TimerQueue::with_capacity(8),
    };
    let task = service.spawn(async {}).unwrap();
    search(&shared, &local, 1).success().unwrap().run();
    shared.retire_runnable();
    block_on(task).unwrap();
    assert!(matches!(search(&shared, &local, 1), Steal::Empty));
}

#[test]
fn nested_admission_distributes_tasks_instead_of_inheriting_the_worker() {
    use super::{CURRENT_WORKER, LocalWorker};
    use crate::task_service::{Shared, TaskService};
    use futures_lite::future::block_on;
    use std::sync::Arc;

    struct WorkerContext;
    impl Drop for WorkerContext {
        fn drop(&mut self) {
            CURRENT_WORKER.with(|current| current.take());
        }
    }

    let mut queues = (0..3)
        .map(|_| std::array::from_fn(|_| Worker::new_fifo()))
        .collect::<Vec<_>>();
    let shared = Arc::new(Shared::new(&queues, 8));
    let service = TaskService {
        shared: Arc::clone(&shared),
    };
    let [ready, current] = queues.remove(0);
    CURRENT_WORKER.with(|context| {
        assert!(context.borrow().is_none());
        context.replace(Some(LocalWorker {
            identifier: 0,
            scheduler: Arc::downgrade(&shared),
            ready,
            current,
            timers: base_scheduler_timers::local::TimerQueue::with_capacity(8),
        }));
    });
    let context = WorkerContext;
    let tasks = (0..6)
        .map(|_| service.spawn(async { 42 }).unwrap())
        .collect::<Vec<_>>();
    for _ in 0..2 {
        let runnable =
            CURRENT_WORKER.with(|current| current.borrow().as_ref().unwrap().ready.pop().unwrap());
        runnable.run();
        shared.retire_runnable();
        for worker in &shared.workers[1..] {
            worker.incoming.steal().success().unwrap().run();
            shared.retire_runnable();
        }
    }
    assert!(
        shared
            .workers
            .iter()
            .all(|worker| worker.incoming.is_empty())
    );
    assert!(CURRENT_WORKER.with(|current| current.borrow().as_ref().unwrap().ready.is_empty()));
    drop(context);
    for task in tasks {
        assert_eq!(block_on(task).unwrap(), 42);
    }
    block_on(service.drain());
}

#[test]
fn idle_worker_steals_remote_wakes_when_preferred_worker_is_busy() {
    use super::{LocalWorker, search};
    use crate::task_service::{Shared, TaskService};
    use futures_lite::future::block_on;
    use std::sync::Arc;
    let mut queues: Vec<[Worker<Runnable>; 2]> = (0..2)
        .map(|_| std::array::from_fn(|_| Worker::new_fifo()))
        .collect();
    let shared = Arc::new(Shared::new(&queues, 8));
    let service = TaskService {
        shared: Arc::clone(&shared),
    };
    let [ready, current] = queues.remove(0);
    let local = LocalWorker {
        identifier: 0,
        scheduler: Arc::downgrade(&shared),
        ready,
        current,
        timers: base_scheduler_timers::local::TimerQueue::with_capacity(8),
    };
    let first = service.spawn(async {}).unwrap();
    shared.workers[0].incoming.steal().success().unwrap().run();
    shared.retire_runnable();
    block_on(first).unwrap();
    // The victim has no local work, but a real scheduled runnable has reached its inbox.
    let task = service.spawn(async {}).unwrap();
    search(&shared, &local, 1).success().unwrap().run();
    shared.retire_runnable();
    block_on(task).unwrap();
    assert!(matches!(search(&shared, &local, 1), Steal::Empty));
}

#[test]
fn idle_worker_can_steal_both_ready_generations() {
    use super::{LocalWorker, search};
    use crate::task_service::{Shared, TaskService};
    use futures_lite::future::block_on;
    use std::sync::Arc;
    let mut queues: Vec<[Worker<Runnable>; 2]> = (0..2)
        .map(|_| std::array::from_fn(|_| Worker::new_fifo()))
        .collect();
    let shared = Arc::new(Shared::new(&queues, 8));
    let service = TaskService {
        shared: Arc::clone(&shared),
    };
    let [ready, current] = queues.remove(0);
    let local = LocalWorker {
        identifier: 0,
        scheduler: Arc::downgrade(&shared),
        ready,
        current,
        timers: base_scheduler_timers::local::TimerQueue::with_capacity(8),
    };
    for queue in &queues[0] {
        let first = service.spawn(async {}).unwrap();
        shared.workers[0].incoming.steal().success().unwrap().run();
        shared.retire_runnable();
        block_on(first).unwrap();
        let task = service.spawn(async {}).unwrap();
        let runnable = shared.workers[1].incoming.steal().success().unwrap();
        queue.push(runnable);
        search(&shared, &local, 1).success().unwrap().run();
        shared.retire_runnable();
        block_on(task).unwrap();
    }
    assert!(matches!(search(&shared, &local, 1), Steal::Empty));
}

#[test]
fn idle_recheck_finds_work_published_after_a_quiescent_search() {
    quiescent_publication(false);
}

#[test]
fn publication_after_the_idle_recheck_retains_a_wake() {
    quiescent_publication(true);
}

fn quiescent_publication(after_recheck: bool) {
    use super::{LocalWorker, search};
    use crate::task_service::{Shared, TaskService};
    use futures_lite::future::block_on;
    use std::{
        sync::{Arc, mpsc},
        thread,
        time::{Duration, Instant},
    };

    let mut queues = (0..2)
        .map(|_| std::array::from_fn(|_| Worker::new_fifo()))
        .collect::<Vec<_>>();
    let shared = Arc::new(Shared::new(&queues, 8));
    let service = TaskService {
        shared: Arc::clone(&shared),
    };
    // Advance admission so the next task belongs to the other worker.
    let first = service.spawn(async {}).unwrap();
    shared.workers[0].incoming.steal().success().unwrap().run();
    shared.retire_runnable();
    block_on(first).unwrap();

    let [ready, current] = queues.remove(0);
    let local = LocalWorker {
        identifier: 0,
        scheduler: Arc::downgrade(&shared),
        ready,
        current,
        timers: base_scheduler_timers::local::TimerQueue::with_capacity(8),
    };
    let published = Arc::new(AtomicBool::new(false));
    let (searching, searched) = mpsc::channel();
    let (finished, completion) = mpsc::channel();
    let target = Arc::clone(&shared);
    let release = Arc::clone(&published);
    let handle = thread::spawn(move || {
        let worker = &target.workers[0];
        worker.thread.set(thread::current()).unwrap();
        assert!(matches!(search(&target, &local, 1), Steal::Empty));
        if after_recheck {
            worker.sleeping.store(true, Ordering::SeqCst);
            target.idle_workers.fetch_add(1, Ordering::SeqCst);
            std::sync::atomic::fence(Ordering::SeqCst);
            assert!(matches!(search(&target, &local, 1), Steal::Empty));
        }
        searching.send(()).unwrap();
        // Channel receives can consume Thread's unpark token. Keep it available for the actual park below.
        let deadline = Instant::now() + Duration::from_secs(5);
        while !release.load(Ordering::Acquire) {
            assert!(
                Instant::now() < deadline,
                "task publication deadline exceeded"
            );
            std::hint::spin_loop();
        }
        let result = if after_recheck {
            assert!(!worker.sleeping.load(Ordering::SeqCst));
            Steal::Empty
        } else {
            worker.sleeping.store(true, Ordering::SeqCst);
            target.idle_workers.fetch_add(1, Ordering::SeqCst);
            std::sync::atomic::fence(Ordering::SeqCst);
            search(&target, &local, 1)
        };
        let runnable = finish_idle_search(
            worker,
            &target.idle_workers,
            result,
            || true,
            || {
                assert!(
                    after_recheck,
                    "the idle recheck must find the published task"
                );
                thread::park_timeout(Duration::from_secs(30));
            },
        )
        .unwrap_or_else(|| search(&target, &local, 1).success().unwrap());
        assert!(!worker.sleeping.load(Ordering::SeqCst));
        assert_eq!(target.idle_workers.load(Ordering::SeqCst), 0);
        runnable.run();
        target.retire_runnable();
        finished.send(()).unwrap();
    });

    searched.recv_timeout(Duration::from_secs(5)).unwrap();
    let task = service.spawn(async { 42 }).unwrap();
    published.store(true, Ordering::Release);
    completion.recv_timeout(Duration::from_secs(5)).unwrap();
    handle.join().unwrap();
    assert_eq!(block_on(task).unwrap(), 42);
    block_on(service.drain());
}

#[test]
fn idle_wake_falls_back_when_the_preferred_worker_is_busy() {
    use crate::task_service::Shared;
    let queues = std::array::from_fn::<_, 2, _>(|_| std::array::from_fn(|_| Worker::new_fifo()));
    let shared = Shared::new(&queues, 8);
    let idle = &shared.workers[1];
    idle.thread.set(std::thread::current()).unwrap();
    idle.sleeping.store(true, Ordering::SeqCst);
    shared.idle_workers.store(1, Ordering::SeqCst);

    shared.wake_idle(0);

    assert!(!idle.sleeping.load(Ordering::SeqCst));
    assert_eq!(shared.idle_workers.load(Ordering::SeqCst), 1);
    // A concurrent wake may clear sleeping before the worker decrements the idle count. Repeating the scan in that interval must also be harmless.
    shared.wake_idle(0);
    assert!(!idle.sleeping.load(Ordering::SeqCst));

    // A ready batch can request help without preferring a worker.
    idle.sleeping.store(true, Ordering::SeqCst);
    shared.wake_idle(usize::MAX);
    assert!(!idle.sleeping.load(Ordering::SeqCst));
}

#[test]
fn idle_wake_prefers_the_requested_sleeping_worker() {
    use crate::task_service::Shared;
    let queues = std::array::from_fn::<_, 2, _>(|_| std::array::from_fn(|_| Worker::new_fifo()));
    let shared = Shared::new(&queues, 8);
    for worker in &shared.workers {
        worker.thread.set(std::thread::current()).unwrap();
        worker.sleeping.store(true, Ordering::SeqCst);
    }
    shared.idle_workers.store(2, Ordering::SeqCst);

    shared.wake_idle(1);

    assert!(shared.workers[0].sleeping.load(Ordering::SeqCst));
    assert!(!shared.workers[1].sleeping.load(Ordering::SeqCst));
    shared.wake_all();
}

#[test]
fn a_nonblocking_turn_without_a_driver_does_not_park() {
    super::drive(&mut None, Some(std::time::Duration::ZERO));
}

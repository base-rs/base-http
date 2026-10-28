// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::{DirectScheduler, test_support};
use base_scheduler::{Cancellation, Spawn};
use base_scheduler_conformance::SpawnFixture;
use futures_lite::future::block_on;

impl SpawnFixture for DirectScheduler {
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
fn run(test: impl AsyncFnOnce(&DirectScheduler) + Send + 'static) {
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
fn startup_and_identifier_exhaustion_are_reported_without_polling_inputs() {
    for fail_after in [0, 1] {
        thread::FAIL_AFTER.set(Some(fail_after));
        assert!(
            DirectScheduler::new(crate::DirectOptions {
                threads: 2,
                capacity: 1
            })
            .is_err()
        );
    }
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
    const CHILD: &str = "PROTOCOL_DIRECT_TASK_DROP_CHILD";
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

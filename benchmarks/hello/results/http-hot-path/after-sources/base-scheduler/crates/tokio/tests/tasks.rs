// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{Cancellation, Spawn, SpawnError};
use base_scheduler_tokio::TokioScheduler;

struct Dropped(Option<futures_channel::oneshot::Sender<()>>);
impl Drop for Dropped {
    fn drop(&mut self) {
        let _ = self.0.take().unwrap().send(());
    }
}

fn run(multithreaded: bool) {
    let (done, completed) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let result = std::panic::catch_unwind(|| {
            let mut builder = if multithreaded {
                tokio::runtime::Builder::new_multi_thread()
            } else {
                tokio::runtime::Builder::new_current_thread()
            };
            let runtime = builder.worker_threads(2).enable_all().build().unwrap();
            let scheduler = TokioScheduler::new(runtime.handle().clone());
            runtime.block_on(async {
                let (started, start) = futures_channel::oneshot::channel();
                let (dropped, drop_complete) = futures_channel::oneshot::channel();
                let captured = Dropped(Some(dropped));
                let task = scheduler
                    .spawn(async move {
                        let _captured = captured;
                        started.send(()).unwrap();
                        std::future::pending::<()>().await;
                    })
                    .unwrap();
                start.await.unwrap();
                drop(task);
                drop_complete.await.unwrap();

                let cancellation = Cancellation::new();
                let child = cancellation.clone();
                let (started, start) = futures_channel::oneshot::channel();
                let task = scheduler
                    .spawn(async move {
                        started.send(()).unwrap();
                        child.cancelled().await;
                        42
                    })
                    .unwrap();
                start.await.unwrap();
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
                cancellation.cancel();
                assert_eq!(task.await.unwrap(), 42);
                shutdown.await;
                assert_eq!(tokio::spawn(async { 73 }).await.unwrap(), 73);
            });
        });
        let _ = done.send(result);
    });
    if let Err(payload) = completed
        .recv_timeout(std::time::Duration::from_secs(15))
        .expect("task lifecycle test deadline exceeded")
    {
        std::panic::resume_unwind(payload);
    }
}

#[test]
fn current_thread_task_ownership_cancellation_and_shutdown() {
    run(false);
}

#[test]
fn multithreaded_task_ownership_cancellation_and_shutdown() {
    run(true);
}

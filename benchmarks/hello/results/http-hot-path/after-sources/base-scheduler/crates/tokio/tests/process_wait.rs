// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler_tokio::TokioScheduler;

fn run(test: impl AsyncFnOnce(&TokioScheduler) + Send + 'static) {
    let (done, completion) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = done.send(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let scheduler = TokioScheduler::new(runtime.handle().clone());
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    runtime.block_on(test(&scheduler))
                }));
                runtime.block_on(scheduler.shutdown());

                if let Err(payload) = outcome {
                    std::panic::resume_unwind(payload);
                }
            },
        )));
    });
    if let Err(payload) = completion
        .recv_timeout(std::time::Duration::from_secs(20))
        .expect("process-wait conformance deadline exceeded")
    {
        std::panic::resume_unwind(payload);
    }
}
base_scheduler_conformance::process_wait_tests!(run: run);

#[test]
fn pending_shutdown() {
    run(async |scheduler| {
        base_scheduler_conformance::process_wait::shutdown(
            scheduler,
            || scheduler.close(),
            scheduler.shutdown(),
        )
        .await;
    });
}
#[test]
fn dropped_and_forgotten_waits() {
    run(async |scheduler| {
        base_scheduler_conformance::process_wait::abandonment(scheduler, scheduler.shutdown())
            .await;
    });
}

#[cfg(unix)]
#[test]
fn suspended_wait_after_shutdown() {
    run(async |scheduler| {
        base_scheduler_conformance::process_wait::suspended_shutdown(
            scheduler,
            scheduler.shutdown(),
        )
        .await;
    });
}

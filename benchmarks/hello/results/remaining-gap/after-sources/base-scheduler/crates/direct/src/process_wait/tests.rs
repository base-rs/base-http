// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use futures_lite::future::block_on;

#[test]
fn started_wait_returns_status_after_cancellation() {
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    block_on(
        base_scheduler_conformance::process_wait::cancellation_with_admission(
            &scheduler,
            ProcessWaitFlags::default(),
            |pid| {
                let receiver = crate::process_io::tests::starting(pid);
                async move {
                    receiver
                        .recv_timeout(std::time::Duration::from_secs(10))
                        .unwrap();
                }
            },
        ),
    );
    block_on(scheduler.shutdown());
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::test_support;
use base_scheduler_conformance::ClockFixture;
use futures_lite::future::{block_on, poll_once};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

pub(super) struct Registration<'a>(&'a AtomicUsize);
impl<'a> Registration<'a> {
    pub(super) fn new(count: &'a AtomicUsize) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(count)
    }
}
impl Drop for Registration<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
struct Fixture {
    scheduler: TokioScheduler,
    runtime: tokio::runtime::Runtime,
}
impl ClockFixture for Fixture {
    type Scheduler = TokioScheduler;
    fn scheduler(&self) -> &TokioScheduler {
        &self.scheduler
    }
    fn advance(&self, duration: Duration) {
        self.runtime.block_on(async {
            tokio::time::advance(duration).await;
            tokio::task::yield_now().await;
        });
    }
    fn registrations(&self) -> usize {
        self.scheduler.timers.load(Ordering::SeqCst)
    }
}
fn run(test: impl AsyncFnOnce(&Fixture) + Send + 'static) {
    test_support::run(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap();
        let fixture = Fixture {
            scheduler: TokioScheduler::new(runtime.handle().clone()),
            runtime,
        };
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(test(&fixture))));
        block_on(fixture.scheduler.shutdown());
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
mod conformance {
    base_scheduler_conformance::clock_tests!(run: super::run);
}

#[test]
fn close_preserves_pending_timers_and_rejects_new_waits() {
    run(async |fixture| {
        let scheduler = &fixture.scheduler;
        let never = Cancellation::never();
        let cancellation = Cancellation::new();
        let mut wait = Box::pin(scheduler.sleep(Duration::from_secs(60), &never));
        let mut cancellable = Box::pin(scheduler.sleep(Duration::from_secs(60), &cancellation));
        let unpolled = scheduler.sleep(Duration::from_secs(1), &never);
        assert!(poll_once(&mut wait).await.is_none());
        assert!(poll_once(&mut cancellable).await.is_none());
        scheduler.close();
        assert!(poll_once(&mut wait).await.is_none());
        assert!(poll_once(&mut cancellable).await.is_none());
        cancellation.cancel();
        assert!(matches!(
            cancellable.await,
            Err(base_scheduler::OperationError::Cancelled(_))
        ));
        assert_eq!(
            unpolled.await.unwrap_err().kind(),
            std::io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            scheduler
                .sleep(Duration::from_secs(1), &never)
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::BrokenPipe
        );
        fixture.advance(Duration::from_secs(60));
        wait.await.unwrap();
    });
}

#[test]
fn real_timers_use_the_selected_host_on_both_runtime_types() {
    test_support::run(|| {
        for multithreaded in [false, true] {
            let mut builder = if multithreaded {
                tokio::runtime::Builder::new_multi_thread()
            } else {
                tokio::runtime::Builder::new_current_thread()
            };
            let runtime = builder.enable_all().build().unwrap();
            let scheduler = TokioScheduler::new(runtime.handle().clone());
            runtime.block_on(async {
                let started = scheduler.now();
                scheduler
                    .sleep(Duration::from_millis(1), &Cancellation::never())
                    .await
                    .unwrap();
                assert!(scheduler.now() >= started + Duration::from_millis(1));
                scheduler.shutdown().await;
            });
        }
    });
}

#[test]
fn another_ambient_clock_does_not_replace_the_selected_host_clock() {
    test_support::run(|| {
        let selected = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap();
        let ambient = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap();
        let scheduler = TokioScheduler::new(selected.handle().clone());
        let never = Cancellation::never();
        let start = scheduler.now();
        let mut sleep = Box::pin(scheduler.sleep(Duration::from_secs(60), &never));
        let scope = ambient.enter();
        assert!(block_on(poll_once(&mut sleep)).is_none());
        drop(scope);
        ambient.block_on(async {
            tokio::time::advance(Duration::from_secs(120)).await;
        });
        assert_eq!(scheduler.now(), start);
        assert!(block_on(poll_once(&mut sleep)).is_none());
        selected.block_on(async {
            tokio::time::advance(Duration::from_secs(60)).await;
            tokio::task::yield_now().await;
        });
        assert_eq!(scheduler.now(), start + Duration::from_secs(60));
        block_on(sleep).unwrap();
        block_on(scheduler.shutdown());
    });
}

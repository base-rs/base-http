// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use base_scheduler::TaskError;
use futures_lite::future::block_on;
use std::{sync::Arc, task::Poll};

fn scheduler(capacity: usize) -> NativeScheduler {
    NativeScheduler::with_options(NativeOptions {
        workers: 1,
        threads: 1,
        capacity,
        ..Default::default()
    })
    .unwrap()
}

#[test]
fn default_scheduler_and_borrowed_non_send_root_future_work() {
    let scheduler = NativeScheduler::new().unwrap();
    assert!(scheduler.worker_count() > 0);
    let value = std::rc::Rc::new(42);
    scheduler.block_on(async {
        assert_eq!(*value, 42);
        let mut first = true;
        std::future::poll_fn(|context| {
            if std::mem::take(&mut first) {
                context.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
        let mut first = true;
        std::future::poll_fn(|context| {
            if std::mem::take(&mut first) {
                // Exercise owned wake dispatch as well as wake_by_ref.
                #[allow(clippy::waker_clone_wake)]
                context.waker().clone().wake();
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
        assert_eq!(scheduler.spawn(async { 73 }).unwrap().await.unwrap(), 73);
        scheduler.shutdown().await;
    });
}

#[test]
fn zero_executor_workers_and_timer_slots_are_rejected() {
    for options in [
        NativeOptions {
            threads: 0,
            ..Default::default()
        },
        NativeOptions {
            workers: 0,
            ..Default::default()
        },
        NativeOptions {
            timer_capacity: 0,
            ..Default::default()
        },
    ] {
        assert_eq!(
            NativeScheduler::with_options(options).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}

#[test]
fn block_on_inside_a_worker_is_rejected_without_stopping_execution() {
    crate::test_support::run(|| {
        let scheduler = Arc::new(scheduler(1));
        let inner = Arc::clone(&scheduler);
        let task = scheduler
            .spawn(async move { inner.block_on(async {}) })
            .unwrap();
        assert!(matches!(block_on(task), Err(TaskError::Panicked(_))));
        assert_eq!(
            block_on(scheduler.spawn(async { 42 }).unwrap()).unwrap(),
            42
        );
        block_on(scheduler.shutdown());
    });
}

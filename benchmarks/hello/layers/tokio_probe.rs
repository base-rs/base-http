//! Disposable microbenchmark hooks over the pinned Tokio implementation.
use super::*;
use crate::io::{Interest, Ready};
use std::{
    future::Future,
    hint::black_box,
    task::{Context, Waker},
    time::Instant,
};

pub(crate) fn probe(kind: &str, iterations: u64, width: usize, waker: &Waker) -> (u64, usize) {
    if kind == "empty-poll" {
        return (driver::layer_empty(iterations), 0);
    }
    if kind == "events-driver" {
        return (driver::layer_events(iterations, width, waker), 0);
    }
    if kind == "dispatch" || kind == "dispatch-waiting" {
        return (
            driver::layer_dispatch(iterations, width, kind == "dispatch-waiting", waker),
            0,
        );
    }
    let records: Vec<_> = (0..width)
        .map(|_| Box::new(ScheduledIo::default()))
        .collect();
    let tokens: Vec<_> = records.iter().map(|record| record.token()).collect();
    let mut cx = Context::from_waker(waker);
    let elapsed;
    match kind {
        "readiness" => {
            let start = Instant::now();
            for _ in 0..iterations {
                for (record, &token) in records.iter().zip(&tokens) {
                    assert!(record.poll_readiness(&mut cx, Direction::Read).is_pending());
                    // SAFETY: each token refers to a live Box-owned record.
                    unsafe {
                        driver::dispatch_event(token, Ready::READABLE);
                    }
                    let event = match record.poll_readiness(&mut cx, Direction::Read) {
                        std::task::Poll::Ready(event) => event,
                        _ => panic!("not ready"),
                    };
                    record.clear_readiness(event);
                }
            }
            elapsed = start.elapsed().as_nanos() as u64;
        }
        "readiness-list" => {
            let start = Instant::now();
            for _ in 0..iterations {
                for (record, &token) in records.iter().zip(&tokens) {
                    let mut waiting = std::pin::pin!(record.readiness(Interest::READABLE));
                    assert!(waiting.as_mut().poll(&mut cx).is_pending());
                    unsafe {
                        driver::dispatch_event(token, Ready::READABLE);
                    }
                    let event = match waiting.as_mut().poll(&mut cx) {
                        std::task::Poll::Ready(event) => event,
                        _ => panic!("not ready"),
                    };
                    record.clear_readiness(event);
                }
            }
            elapsed = start.elapsed().as_nanos() as u64;
        }
        _ => panic!("unknown Tokio probe {kind}"),
    }
    black_box(&records);
    (elapsed, 0)
}

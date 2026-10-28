// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Controlled monotonic-clock assertions; no wall-clock timing assumptions.

use crate::{ClockFixture, probe::WakeProbe};
use base_scheduler::{Cancellation, Clock, OperationError, TimeoutError};
use futures_lite::future::poll_once;
use std::{
    future::Future,
    pin::pin,
    sync::{Arc, atomic::Ordering},
    task::{Context, Waker},
    time::Duration,
};

const STEP: Duration = Duration::from_secs(10);

/// Unpolled waits register nothing, and dropping a pending wait unregisters it.
pub async fn registration<F: ClockFixture>(fixture: &F) {
    let scheduler = fixture.scheduler();
    let never = Cancellation::never();
    assert_eq!(fixture.registrations(), 0);
    drop(scheduler.sleep_until(scheduler.now() + STEP, &never));
    drop(scheduler.sleep(STEP, &never));
    assert_eq!(fixture.registrations(), 0);
    let mut wait = Box::pin(scheduler.sleep_until(scheduler.now() + STEP, &never));
    assert!(poll_once(&mut wait).await.is_none());
    assert_eq!(fixture.registrations(), 1);
    drop(wait);
    assert_eq!(fixture.registrations(), 0);
}

/// Pre-cancellation wins even for elapsed deadlines and overflowing relative waits.
pub async fn pre_cancelled<F: ClockFixture>(fixture: &F) {
    let scheduler = fixture.scheduler();
    let token = Cancellation::new();
    token.cancel();
    for deadline in [scheduler.now(), scheduler.now() + STEP] {
        assert!(matches!(
            scheduler.sleep_until(deadline, &token).await,
            Err(OperationError::Cancelled(_))
        ));
    }
    for duration in [Duration::ZERO, STEP, Duration::MAX] {
        assert!(matches!(
            scheduler.sleep(duration, &token).await,
            Err(OperationError::Cancelled(_))
        ));
    }
    assert_eq!(fixture.registrations(), 0);
}

/// An absolute wait stays pending before its deadline, wakes on expiry, and releases its registration.
pub async fn deadline<F: ClockFixture>(fixture: &F) {
    let scheduler = fixture.scheduler();
    let start = scheduler.now();
    assert!(scheduler.now() >= start);
    let never = Cancellation::never();
    let mut wait = pin!(scheduler.sleep_until(start + STEP, &never));
    let probe = Arc::new(WakeProbe::default());
    let waker = Waker::from(Arc::clone(&probe));
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    fixture.advance(STEP / 2);
    assert!(scheduler.now() >= start + STEP / 2);
    assert!(poll_once(&mut wait).await.is_none());
    // Reinstall the observed waker after poll_once's temporary one.
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    probe.0.store(0, Ordering::SeqCst);
    fixture.advance(STEP / 2);
    assert!(
        probe.0.load(Ordering::SeqCst) > 0,
        "expiry did not wake its waiter"
    );
    wait.await.unwrap();
    assert!(scheduler.now() >= start + STEP);
    assert_eq!(fixture.registrations(), 0);
}

/// Relative time starts at first poll, using the supplied clock domain.
pub async fn relative<F: ClockFixture>(fixture: &F) {
    let scheduler = fixture.scheduler();
    let never = Cancellation::never();
    let mut wait = pin!(scheduler.sleep(STEP, &never));
    fixture.advance(STEP * 2);
    assert_eq!(fixture.registrations(), 0);
    assert!(poll_once(&mut wait).await.is_none());
    fixture.advance(STEP / 2);
    assert!(poll_once(&mut wait).await.is_none());
    fixture.advance(STEP / 2);
    wait.await.unwrap();
    assert_eq!(fixture.registrations(), 0);
    scheduler.sleep(Duration::ZERO, &never).await.unwrap();
    scheduler
        .sleep_until(scheduler.now(), &never)
        .await
        .unwrap();
    assert_eq!(fixture.registrations(), 0);
}

/// Pending cancellation wakes the waiter and removes its timer.
pub async fn cancellation<F: ClockFixture>(fixture: &F) {
    let scheduler = fixture.scheduler();
    let token = Cancellation::new();
    let mut wait = pin!(scheduler.sleep_until(scheduler.now() + STEP, &token));
    let probe = Arc::new(WakeProbe::default());
    let waker = Waker::from(Arc::clone(&probe));
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    probe.0.store(0, Ordering::SeqCst);
    token.cancel();
    assert!(
        probe.0.load(Ordering::SeqCst) > 0,
        "cancellation did not wake its waiter"
    );
    assert!(matches!(wait.await, Err(OperationError::Cancelled(_))));
    assert_eq!(fixture.registrations(), 0);
}

/// Once waiting, a deadline observed together with cancellation wins.
pub async fn expiry_wins<F: ClockFixture>(fixture: &F) {
    let scheduler = fixture.scheduler();
    let token = Cancellation::new();
    let mut wait = pin!(scheduler.sleep_until(scheduler.now() + STEP, &token));
    assert!(poll_once(&mut wait).await.is_none());
    fixture.advance(STEP);
    token.cancel();
    wait.await.unwrap();
    assert_eq!(fixture.registrations(), 0);
}

/// Overflow is an input error and registers no timer.
pub async fn overflow<F: ClockFixture>(fixture: &F) {
    assert!(
        fixture
            .scheduler()
            .now()
            .checked_add(Duration::MAX)
            .is_none()
    );
    let error = fixture
        .scheduler()
        .sleep(Duration::MAX, &Cancellation::never())
        .await
        .unwrap_err();
    assert!(
        matches!(error, OperationError::Io(error) if error.kind() == std::io::ErrorKind::InvalidInput)
    );
    assert_eq!(fixture.registrations(), 0);
}

/// Timeouts preserve operation results, use the supplied clock, and retire both waits.
pub async fn timeout<F: ClockFixture>(fixture: &F) {
    let scheduler = fixture.scheduler();
    assert_eq!(
        scheduler
            .timeout(Duration::MAX, std::future::ready(Err::<(), _>("closed")))
            .await
            .unwrap(),
        Err("closed")
    );
    assert_eq!(fixture.registrations(), 0);

    let mut wait = Box::pin(scheduler.timeout(STEP, std::future::pending::<()>()));
    // Time spent between construction and first poll does not shorten the wait.
    fixture.advance(STEP * 2);
    assert!(poll_once(&mut wait).await.is_none());
    assert_eq!(fixture.registrations(), 1);
    fixture.advance(STEP / 2);
    assert!(poll_once(&mut wait).await.is_none());
    fixture.advance(STEP / 2);
    assert!(matches!(wait.as_mut().await, Err(TimeoutError::Elapsed)));
    assert_eq!(fixture.registrations(), 0);

    let finish = Cancellation::new();
    let mut wait = Box::pin(scheduler.timeout(STEP, finish.cancelled()));
    assert!(poll_once(&mut wait).await.is_none());
    assert_eq!(fixture.registrations(), 1);
    // Both become ready before the next poll: operation completion wins.
    finish.cancel();
    fixture.advance(STEP);
    wait.as_mut().await.unwrap();
    assert_eq!(fixture.registrations(), 0);

    let mut wait = Box::pin(scheduler.timeout(STEP, std::future::pending::<()>()));
    assert!(poll_once(&mut wait).await.is_none());
    assert_eq!(fixture.registrations(), 1);
    drop(wait);
    assert_eq!(fixture.registrations(), 0);
}

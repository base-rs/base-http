// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::{NativeOptions, NativeScheduler, test_support};
use base_scheduler::Spawn;
use base_scheduler_conformance::ClockFixture;
use futures_lite::future::{block_on, poll_once};
use std::{
    sync::mpsc,
    task::{Context, Wake},
};

fn inspect(shared: &Arc<Shared>, owner: usize) -> usize {
    if worker::timers(shared, Some(owner), |_, _| ()).is_some() {
        turn(shared, owner);
        return worker::timers(shared, Some(owner), |_, timers| timers.len()).unwrap();
    }
    let (reply, receive) = mpsc::channel();
    shared.send_timer(owner, Command::Inspect(reply));
    receive.recv_timeout(Duration::from_secs(5)).unwrap()
}

impl ClockFixture for NativeScheduler {
    type Scheduler = Self;
    fn scheduler(&self) -> &Self {
        self
    }
    fn advance(&self, duration: Duration) {
        *self.tasks.shared.paused.lock().unwrap().as_mut().unwrap() += duration;
        self.registrations();
    }
    fn registrations(&self) -> usize {
        (0..self.worker_count())
            .map(|owner| inspect(&self.tasks.shared, owner))
            .sum()
    }
}

fn run(test: impl AsyncFnOnce(&NativeScheduler) + Send + 'static) {
    test_support::run(move || {
        let scheduler = test_support::scheduler();
        *scheduler.tasks.shared.paused.lock().unwrap() = Some(Instant::now());
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(test(&scheduler))));
        block_on(scheduler.shutdown());
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}

mod conformance {
    base_scheduler_conformance::clock_tests!(run: super::run);
}

mod worker_conformance {
    use super::*;
    macro_rules! case {
        ($($name:ident),* $(,)?) => {$(
            #[test]
            fn $name() {
                test_support::run(|| {
                    let scheduler = Arc::new(test_support::scheduler());
                    *scheduler.tasks.shared.paused.lock().unwrap() = Some(Instant::now());
                    let owned = Arc::clone(&scheduler);
                    let task = scheduler.spawn(async move {
                        base_scheduler_conformance::clock::$name(&*owned).await;
                    }).unwrap();
                    let result = block_on(task);
                    block_on(scheduler.shutdown());
                    result.unwrap();
                });
            }
        )*};
    }
    case!(
        registration,
        pre_cancelled,
        deadline,
        relative,
        cancellation,
        expiry_wins,
        overflow,
        timeout
    );
}

#[test]
fn worker_sleep_registration_repoll_and_drop_allocate_nothing() {
    test_support::run(|| {
        let scheduler = Arc::new(test_support::scheduler());
        let owned = Arc::clone(&scheduler);
        let task = scheduler
            .spawn(async move {
                let token = Cancellation::new();
                let deadline = owned.now() + Duration::from_secs(3600);
                let allocations = allocation_counter::measure(|| {
                    for _ in 0..32 {
                        let mut sleep = pin!(owned.sleep_until(deadline, &token));
                        for _ in 0..2 {
                            assert!(
                                sleep
                                    .as_mut()
                                    .poll(&mut Context::from_waker(Waker::noop()))
                                    .is_pending()
                            );
                        }
                    }
                });
                assert_eq!(allocations.count_total, 0, "{allocations:?}");
                assert_eq!(owned.registrations(), 0);
            })
            .unwrap();
        block_on(task).unwrap();
        block_on(scheduler.shutdown());
    });
}

#[test]
fn earlier_external_deadline_interrupts_worker_wait() {
    test_support::run(|| {
        let scheduler = test_support::scheduler();
        let never = Cancellation::never();
        let mut distant = Box::pin(scheduler.sleep(Duration::from_secs(3600), &never));
        assert!(block_on(poll_once(&mut distant)).is_none());
        assert_eq!(scheduler.registrations(), 1);
        let deadline = scheduler.now() + Duration::from_millis(10);
        scheduler
            .block_on(scheduler.sleep_until(deadline, &never))
            .unwrap();
        assert!(scheduler.now() >= deadline);
        drop(distant);
        assert_eq!(scheduler.registrations(), 0);
        block_on(scheduler.shutdown());
    });
}

#[test]
fn full_capacity_reclaims_cancelled_timers_and_shutdown_rejects_waits() {
    test_support::run(|| {
        let scheduler = NativeScheduler::with_options(NativeOptions {
            workers: 1,
            timer_capacity: 1,
            ..Default::default()
        })
        .unwrap();
        let never = Cancellation::never();
        let deadline = scheduler.now() + Duration::from_secs(3600);
        // The timer engine can round reservations up to its minimum batch size.
        let capacity =
            base_scheduler_timers::local::TimerQueue::<Waker>::with_capacity(1).capacity();
        let mut waits: Vec<_> = (0..capacity)
            .map(|_| Box::pin(scheduler.sleep_until(deadline, &never)))
            .collect();
        for wait in &mut waits {
            assert!(block_on(poll_once(wait)).is_none());
        }
        assert_eq!(scheduler.registrations(), capacity);
        assert_eq!(
            block_on(scheduler.sleep_until(deadline, &never))
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        drop(waits);
        assert_eq!(scheduler.registrations(), 0);
        let mut next = Box::pin(scheduler.sleep_until(deadline, &never));
        assert!(block_on(poll_once(&mut next)).is_none());
        assert_eq!(scheduler.registrations(), 1);
        scheduler.close();
        assert_eq!(
            block_on(next).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            block_on(scheduler.sleep_until(deadline, &never))
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        block_on(scheduler.shutdown());
    });
}

#[test]
fn dropping_unacknowledged_registration_releases_its_slot() {
    test_support::run(|| {
        let scheduler = test_support::scheduler();
        let (gate, started, release) = test_support::Gate::new();
        let task = scheduler
            .spawn(async move {
                gate.wait();
            })
            .unwrap();
        started.recv_timeout(Duration::from_secs(5)).unwrap();
        let never = Cancellation::never();
        let mut sleep = Box::pin(scheduler.sleep(Duration::from_secs(3600), &never));
        assert!(block_on(poll_once(&mut sleep)).is_none());
        drop(sleep);
        drop(release);
        block_on(task).unwrap();
        assert_eq!(scheduler.registrations(), 0);
        block_on(scheduler.shutdown());
    });
}

#[derive(Default)]
struct Probe(std::sync::atomic::AtomicUsize);
impl Wake for Probe {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn pending_registration_notifies_replacement_waker() {
    test_support::run(|| {
        let scheduler = test_support::scheduler();
        let (gate, started, release) = test_support::Gate::new();
        let task = scheduler
            .spawn(async move {
                gate.wait();
            })
            .unwrap();
        started.recv_timeout(Duration::from_secs(5)).unwrap();
        let never = Cancellation::never();
        let mut sleep = Box::pin(scheduler.sleep(Duration::from_secs(3600), &never));
        assert!(
            sleep
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let probe = Arc::new(Probe::default());
        let waker = Waker::from(Arc::clone(&probe));
        assert!(
            sleep
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(release);
        block_on(task).unwrap();
        assert_eq!(scheduler.registrations(), 1);
        assert!(probe.0.load(Ordering::SeqCst) > 0);
        // Consume acknowledgment and install the replacement on the actual timer.
        assert!(
            sleep
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(sleep);
        assert_eq!(scheduler.registrations(), 0);
        block_on(scheduler.shutdown());
    });
}

#[test]
fn moving_a_sleep_to_another_worker_updates_and_releases_its_original_queue() {
    test_support::run(|| {
        let scheduler = Arc::new(
            NativeScheduler::with_options(NativeOptions {
                workers: 2,
                ..Default::default()
            })
            .unwrap(),
        );
        let (gate, started, release) = test_support::Gate::new();
        let (send, receive) = oneshot::channel();
        let token = Cancellation::new();
        let cancel = token.clone();
        let owned = Arc::clone(&scheduler);
        let first = scheduler
            .spawn(async move {
                let owner = worker::timers(&owned.tasks.shared, None, |owner, _| owner).unwrap();
                let mut sleep =
                    Box::pin(async move { owned.sleep(Duration::from_secs(3600), &token).await });
                assert!(
                    sleep
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                assert!(send.send((owner, sleep)).is_ok());
                // Keep the original worker occupied so the next task must run elsewhere.
                gate.wait();
            })
            .unwrap();
        started.recv_timeout(Duration::from_secs(5)).unwrap();
        let (owner, mut sleep) = block_on(receive).unwrap();
        let owned = Arc::clone(&scheduler);
        let second = scheduler
            .spawn(async move {
                let current = worker::timers(&owned.tasks.shared, None, |owner, _| owner).unwrap();
                assert_ne!(current, owner);
                let probe = Waker::from(Arc::new(Probe::default()));
                assert!(
                    sleep
                        .as_mut()
                        .poll(&mut Context::from_waker(&probe))
                        .is_pending()
                );
                cancel.cancel();
                assert!(matches!(
                    sleep.await,
                    Err(base_scheduler::OperationError::Cancelled(_))
                ));
                assert!(!owned.tasks.shared.workers[owner].timers.is_empty());
            })
            .unwrap();
        block_on(second).unwrap();
        drop(release);
        block_on(first).unwrap();
        assert_eq!(scheduler.registrations(), 0);
        block_on(scheduler.shutdown());
    });
}

#[test]
fn calls_from_another_scheduler_use_the_explicitly_supplied_scheduler() {
    test_support::run(|| {
        let owner = Arc::new(test_support::scheduler());
        let other = test_support::scheduler();
        let owned = Arc::clone(&owner);
        let task = other
            .spawn(async move {
                owned
                    .sleep(Duration::from_millis(1), &Cancellation::never())
                    .await
                    .unwrap();
            })
            .unwrap();
        block_on(task).unwrap();
        block_on(other.shutdown());
        block_on(owner.shutdown());
    });
}

#[test]
fn timer_wakers_can_reenter_and_panic_without_stopping_workers() {
    test_support::run(|| {
        struct Reentrant(Arc<NativeScheduler>);
        impl Wake for Reentrant {
            fn wake(self: Arc<Self>) {
                // This uses the same queue and would fail if the wake retained a TLS borrow.
                let never = Cancellation::never();
                let mut sleep = pin!(self.0.sleep(Duration::from_secs(3600), &never));
                assert!(
                    sleep
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                panic!("timer waker panic");
            }
        }
        let scheduler = Arc::new(test_support::scheduler());
        let never = Cancellation::never();
        let mut sleep = Box::pin(scheduler.sleep(Duration::from_millis(10), &never));
        assert!(
            sleep
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(scheduler.registrations(), 1);
        let waker = Waker::from(Arc::new(Reentrant(Arc::clone(&scheduler))));
        assert!(
            sleep
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        block_on(sleep).unwrap();
        assert_eq!(
            block_on(scheduler.spawn(async { 42 }).unwrap()).unwrap(),
            42
        );
        block_on(scheduler.shutdown());
    });
}

#[test]
fn local_capacity_and_delayed_updates_obey_registration_ownership() {
    test_support::run(|| {
        let scheduler = Arc::new(
            NativeScheduler::with_options(NativeOptions {
                workers: 1,
                timer_capacity: 1,
                ..Default::default()
            })
            .unwrap(),
        );
        let owned = Arc::clone(&scheduler);
        let task = scheduler
            .spawn(async move {
                let shared = &owned.tasks.shared;
                let never = Cancellation::never();
                let deadline = owned.now() + Duration::from_secs(3600);
                let capacity = worker::timers(shared, None, |_, timers| timers.capacity()).unwrap();
                let mut waits: Vec<_> = (0..capacity)
                    .map(|_| Box::pin(owned.sleep_until(deadline, &never)))
                    .collect();
                for wait in &mut waits {
                    assert!(poll_once(wait).await.is_none());
                }
                assert_eq!(
                    owned
                        .sleep_until(deadline, &never)
                        .await
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::WouldBlock
                );
                drop(waits);
                // A migrated update can arrive after expiry but before the waiter releases its key.
                let (owner, key) = worker::timers(shared, None, |owner, timers| {
                    (
                        owner,
                        timers
                            .try_schedule(owned.now(), Waker::noop().clone())
                            .unwrap(),
                    )
                })
                .unwrap();
                let registration = registered(shared, owner, Ok(key)).unwrap();
                expire(shared, owner);
                let probe = Arc::new(Probe::default());
                dispatch(
                    shared,
                    owner,
                    Command::Update(key, Waker::from(Arc::clone(&probe))),
                );
                assert_eq!(probe.0.load(Ordering::SeqCst), 1);
                drop(registration);
                assert_eq!(owned.registrations(), 0);
                // An inbox command published just before idle must prevent an indefinite wait.
                let (reply, receive) = mpsc::channel();
                shared.send_timer(owner, Command::Inspect(reply));
                assert_eq!(timeout(shared, owner), Some(Duration::ZERO));
                turn(shared, owner);
                assert_eq!(receive.recv().unwrap(), 0);
            })
            .unwrap();
        block_on(task).unwrap();
        block_on(scheduler.shutdown());
    });
}

#[test]
fn closure_rejects_queued_registrations_and_notifies_late_updates() {
    test_support::run(|| {
        let scheduler = Arc::new(test_support::scheduler());
        let owned = Arc::clone(&scheduler);
        let registration = block_on(
            scheduler
                .spawn(async move {
                    let (owner, result) =
                        worker::timers(&owned.tasks.shared, None, |owner, timers| {
                            (
                                owner,
                                timers.try_schedule(
                                    owned.now() + Duration::from_secs(3600),
                                    Waker::noop().clone(),
                                ),
                            )
                        })
                        .unwrap();
                    registered(&owned.tasks.shared, owner, result).unwrap()
                })
                .unwrap(),
        )
        .unwrap();
        let (gate, started, release) = test_support::Gate::new();
        let task = scheduler
            .spawn(async move {
                gate.wait();
            })
            .unwrap();
        started.recv_timeout(Duration::from_secs(5)).unwrap();
        let (reply, receive) = oneshot::channel();
        scheduler.tasks.shared.send_timer(
            0,
            Command::Register {
                deadline: scheduler.now() + Duration::from_secs(3600),
                waker: Waker::noop().clone(),
                reply,
            },
        );
        scheduler.close();
        drop(release);
        block_on(task).unwrap();
        assert_eq!(
            block_on(receive).unwrap().err().unwrap().kind(),
            io::ErrorKind::BrokenPipe
        );
        block_on(scheduler.shutdown());
        let probe = Arc::new(Probe::default());
        dispatch(
            &scheduler.tasks.shared,
            registration.owner,
            Command::Update(registration.key, Waker::from(Arc::clone(&probe))),
        );
        assert_eq!(probe.0.load(Ordering::SeqCst), 1);
        drop(registration);
        assert_eq!(scheduler.registrations(), 0);
        // Registration helper handles closure between the public precheck and admission.
        assert_eq!(
            block_on(wait(
                &scheduler.tasks.shared,
                scheduler.now() + Duration::from_secs(1)
            ))
            .unwrap_err()
            .kind(),
            io::ErrorKind::BrokenPipe
        );
    });
}

#[test]
fn shutdown_waits_for_timer_callbacks_and_live_waits_can_outlast_workers() {
    test_support::run(|| {
        struct GatedWake(Arc<test_support::Gate>);
        impl Wake for GatedWake {
            fn wake(self: Arc<Self>) {
                self.0.wait();
            }
        }
        let scheduler = test_support::scheduler();
        let (gate, started, release) = test_support::Gate::new();
        let waker = Waker::from(Arc::new(GatedWake(gate)));
        let never = Cancellation::never();
        let mut sleep = Box::pin(scheduler.sleep(Duration::from_secs(3600), &never));
        assert!(
            sleep
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(scheduler.registrations(), 1);
        assert!(
            sleep
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(scheduler.registrations(), 1);
        scheduler.close();
        started.recv_timeout(Duration::from_secs(5)).unwrap();
        let mut shutdown = Box::pin(scheduler.shutdown());
        assert!(block_on(poll_once(&mut shutdown)).is_none());
        drop(release);
        block_on(shutdown);
        assert_eq!(
            block_on(sleep).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    });
}

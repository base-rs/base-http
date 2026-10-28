// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

mod support;
use base_scheduler::{Notify, Signals};
use base_scheduler_conformance::{SignalsFixture, signals};
use futures_lite::future::block_on;
use std::{
    collections::BTreeMap,
    io,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Clone, Copy, PartialEq)]
enum Fault {
    None,
    IgnoreFilter,
    DetachSubscription,
    AcceptInvalid,
}
type Entry = (Vec<u8>, Arc<dyn Notify<u8>>);
struct Scheduler {
    subscriptions: Mutex<BTreeMap<usize, Entry>>,
    retired: Mutex<Vec<Arc<dyn Notify<u8>>>>,
    next: AtomicUsize,
    fault: Fault,
}
struct Subscription {
    scheduler: Weak<Scheduler>,
    id: usize,
}
impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(scheduler) = self.scheduler.upgrade() {
            if scheduler.fault != Fault::DetachSubscription {
                scheduler.subscriptions.lock().unwrap().remove(&self.id);
            }
        }
    }
}
// A self handle is needed to create subscription guards, but guards do not retain the service.
struct Endpoint {
    scheduler: Arc<Scheduler>,
}
impl Signals for Endpoint {
    type Signal = u8;
    type Subscription = Subscription;
    fn signal_subscribe<T: Notify<u8> + 'static>(
        &self,
        signals: &[u8],
        target: T,
    ) -> io::Result<Subscription> {
        if signals.contains(&255) && self.scheduler.fault != Fault::AcceptInvalid {
            // Setup errors are synchronous, but failed setup may defer target cleanup.
            self.scheduler
                .retired
                .lock()
                .unwrap()
                .push(Arc::new(target));
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid signal",
            ));
        }
        let id = self.scheduler.next.fetch_add(1, Ordering::SeqCst);
        self.scheduler
            .subscriptions
            .lock()
            .unwrap()
            .insert(id, (signals.to_vec(), Arc::new(target)));
        Ok(Subscription {
            scheduler: Arc::downgrade(&self.scheduler),
            id,
        })
    }
}
struct Fixture {
    endpoint: Arc<Endpoint>,
}
impl Fixture {
    fn new(fault: Fault) -> Self {
        Self {
            endpoint: Arc::new(Endpoint {
                scheduler: Arc::new(Scheduler {
                    subscriptions: Mutex::default(),
                    retired: Mutex::default(),
                    next: AtomicUsize::new(0),
                    fault,
                }),
            }),
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.endpoint
            .scheduler
            .subscriptions
            .lock()
            .unwrap()
            .clear();
    }
}
impl SignalsFixture for Fixture {
    type Signal = u8;
    type Scheduler = Endpoint;
    fn scheduler(&self) -> &Arc<Endpoint> {
        &self.endpoint
    }
    fn signals(&self) -> [u8; 2] {
        [1, 2]
    }
    fn invalid_signal(&self) -> u8 {
        255
    }
    async fn emit(&self, signal: u8) {
        let targets = self
            .endpoint
            .scheduler
            .subscriptions
            .lock()
            .unwrap()
            .values()
            .filter(|(signals, _)| {
                signals.contains(&signal) || self.endpoint.scheduler.fault == Fault::IgnoreFilter
            })
            .map(|(_, target)| Arc::clone(target))
            .collect::<Vec<_>>();
        for target in targets {
            target.notify(signal);
        }
        self.endpoint.scheduler.retired.lock().unwrap().clear();
    }
}
fn run_case(test: impl AsyncFnOnce(&Fixture) + Send + 'static) {
    support::run(move || block_on(test(&Fixture::new(Fault::None))));
}
mod dispatch {
    base_scheduler_conformance::signals_dispatch_tests!(run: super::run_case);
}
mod generated {
    base_scheduler_conformance::signals_tests!(run: super::run_case);
}
#[test]
fn rejects_wrong_selection_detached_subscriptions_and_lost_setup_errors() {
    for fault in [
        Fault::IgnoreFilter,
        Fault::DetachSubscription,
        Fault::AcceptInvalid,
    ] {
        support::run(move || {
            let fixture = Fixture::new(fault);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                block_on(async {
                    match fault {
                        Fault::AcceptInvalid => signals::errors(&fixture).await,
                        _ => signals::subscriptions(&fixture).await,
                    }
                })
            }));
            assert!(
                result.is_err(),
                "signal suite accepted an incorrect backend"
            );
        });
    }
}

#[test]
fn rejects_target_retention_after_unsubscribed_dispatch_finishes() {
    support::run(|| {
        let fixture = Fixture::new(Fault::DetachSubscription);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(
                signals::in_progress(&fixture)
            )))
            .is_err()
        );
    });
}

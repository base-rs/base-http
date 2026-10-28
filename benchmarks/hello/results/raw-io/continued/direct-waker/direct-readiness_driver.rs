// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Interest, SelectorDriver, SelectorWorker};
use base_scheduler::Cancellation;
use polling::{Event, PollMode};
#[cfg(target_os = "macos")]
mod kqueue;
#[cfg(target_os = "macos")]
use kqueue::{Events, Poller};
#[cfg(not(target_os = "macos"))]
use polling::{Events, Poller};
use std::{
    cell::RefCell,
    collections::HashMap,
    future::poll_fn,
    io,
    marker::PhantomData,
    rc::Rc,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Poll, Wake, Waker},
    time::Duration,
};

type Native = async_io::Async<socket2::Socket>;

pub(crate) struct DriverGroup {
    closed: Cancellation,
    controllers: Vec<Arc<Controller>>,
    next: AtomicUsize,
}
impl DriverGroup {
    pub(crate) fn close(&self) {
        self.closed.cancel();
        for controller in &self.controllers {
            controller.close();
        }
    }
    #[cfg(all(test, target_os = "macos"))]
    pub(crate) fn fail_next_registration(self: &Arc<Self>, error: i32) {
        self.route().poller.failure.store(error, Ordering::Relaxed);
    }
    fn route(self: &Arc<Self>) -> Arc<Controller> {
        let local = ROUTES.with(|routes| {
            routes
                .borrow()
                .iter()
                .rev()
                .find(|(group, _)| group.ptr_eq(&Arc::downgrade(self)))
                .map(|(_, controller)| Arc::clone(controller))
        });
        local.unwrap_or_else(|| {
            Arc::clone(
                &self.controllers
                    [self.next.fetch_add(1, Ordering::Relaxed) % self.controllers.len()],
            )
        })
    }
}

struct Table {
    closed: bool,
    next: usize,
    registrations: HashMap<usize, Arc<Registration>>,
    retired: Vec<Arc<Registration>>,
}
struct Controller {
    poller: Poller,
    table: Mutex<Table>,
    needs_release: AtomicBool,
}
impl Controller {
    fn close(&self) {
        let registrations = {
            let mut table = self.table.lock().unwrap();
            table.closed = true;
            table.registrations.values().cloned().collect::<Vec<_>>()
        };
        // Closure wakes waits but cannot free pointers in a collected batch.
        for registration in registrations {
            registration.readable.wake();
            registration.writable.wake();
        }
        let _ = self.poller.notify();
    }

    fn release(&self) {
        if !self.needs_release.load(Ordering::Acquire) {
            return;
        }
        let retired = {
            let mut table = self.table.lock().unwrap();
            self.needs_release.store(false, Ordering::Release);
            std::mem::take(&mut table.retired)
        };
        // Registration/waker destruction must be outside the table lock.
        drop(retired);
    }

    fn stop(&self) {
        let (registrations, retired) = {
            let mut table = self.table.lock().unwrap();
            table.closed = true;
            (
                std::mem::take(&mut table.registrations),
                std::mem::take(&mut table.retired),
            )
        };
        // Only the sole driver calls this, after its final dispatch has ended.
        drop((registrations, retired));
    }
}
impl Wake for Controller {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.poller.notify();
    }
}

pub(crate) struct Direction {
    // Low bits are advisory readiness and readable EOF; upper bits identify
    // the notification. EOF inhibits short-read clearing, not WouldBlock clear.
    tick: AtomicUsize,
    #[cfg(target_os = "macos")]
    available: AtomicUsize,
    waker: Mutex<Option<Waker>>,
}
impl Direction {
    fn new() -> Self {
        Self {
            tick: AtomicUsize::new(0),
            #[cfg(target_os = "macos")]
            available: AtomicUsize::new(0),
            waker: Mutex::new(None),
        }
    }
    pub(crate) fn snapshot(&self) -> usize {
        self.tick.load(Ordering::Acquire)
    }
    pub(crate) fn publish(&self) {
        // Increment the generation before publishing readiness. A racing clear
        // can at most cause a spurious retry, never erase this notification.
        self.tick.fetch_add(4, Ordering::AcqRel);
        self.tick.fetch_or(1, Ordering::Release);
        self.wake();
    }
    #[cfg(target_os = "macos")]
    fn publish_readable(&self, available: usize, read_closed: bool) {
        // Publish the byte hint before advancing the event generation. A
        // concurrent reader either sees this hint or fails its generation clear.
        self.available.store(available, Ordering::Release);
        if read_closed {
            self.tick.fetch_or(2, Ordering::Release);
        }
        self.publish();
    }
    pub(crate) fn clear_after_read(&self, observed: usize, count: usize, capacity: usize) {
        #[cfg(target_os = "macos")]
        if count != 0
            && count < capacity
            && observed & 2 == 0
            && count >= self.available.load(Ordering::Acquire)
        {
            self.clear(observed);
        }
        // polling's other edge backends do not expose directional EOF here.
        // Retain advisory readiness until WouldBlock rather than lose EOF.
        #[cfg(not(target_os = "macos"))]
        let _ = (observed, count, capacity);
    }
    pub(crate) fn clear(&self, observed: usize) {
        // A WouldBlock must not erase an edge delivered during the syscall.
        let _ = self.tick.compare_exchange(
            observed,
            observed & !1,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
    fn wake(&self) {
        let waker = self.waker.lock().unwrap().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    pub(crate) async fn wait(&self, group: &DriverGroup) -> io::Result<()> {
        poll_fn(|cx| {
            if group.closed.is_cancelled() {
                return Poll::Ready(Err(closed()));
            }
            if self.snapshot() & 1 != 0 {
                return Poll::Ready(Ok(()));
            }
            // Comparison-only: one waiting task per direction, no listener guard.
            // Clone/drop/wake callbacks still execute outside the slot lock.
            let waker = cx.waker().clone();
            let previous = self.waker.lock().unwrap().replace(waker);
            drop(previous);
            if group.closed.is_cancelled() {
                Poll::Ready(Err(closed()))
            } else if self.snapshot() & 1 != 0 {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        })
        .await
    }
}
fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "readiness driver is closed")
}

pub(crate) struct Registration {
    owner: Weak<Controller>,
    key: usize,
    inactive: AtomicBool,
    readable: Direction,
    writable: Direction,
}
impl Registration {
    fn token(&self) -> usize {
        // Direct tokens require audited synchronous filter deletion. Other
        // polling backends retain safe numeric-key routing.
        #[cfg(any(target_os = "macos", target_os = "linux", target_os = "android"))]
        {
            (self as *const Self).expose_provenance()
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "android")))]
        {
            self.key
        }
    }
    pub(crate) fn direction(&self, interest: Interest) -> &Direction {
        match interest {
            Interest::Readable => &self.readable,
            Interest::Writable => &self.writable,
        }
    }
}

// Kept exclusively by NativeSocket aliases, before their native owner in drop
// order. Event delivery retains only Registration and never the native socket.
// This lets cross-thread exclusive close deregister synchronously, even while
// the driver is publishing a previously collected notification.
pub(crate) struct SocketRegistrations {
    socket: Weak<Native>,
    entries: Mutex<Vec<(Weak<DriverGroup>, Arc<Registration>)>>,
}
impl std::fmt::Debug for SocketRegistrations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SocketRegistrations")
            .finish_non_exhaustive()
    }
}
impl SocketRegistrations {
    pub(crate) fn new(socket: &Arc<Native>) -> Self {
        Self {
            socket: Arc::downgrade(socket),
            entries: Mutex::new(Vec::new()),
        }
    }
    pub(crate) fn existing(&self, group: &Arc<DriverGroup>) -> Option<Arc<Registration>> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .find(|(owner, _)| owner.as_ptr() == Arc::as_ptr(group))
            .map(|(_, registration)| Arc::clone(registration))
    }

    pub(crate) fn get(
        &self,
        group: &Arc<DriverGroup>,
        socket: &socket2::Socket,
    ) -> io::Result<Arc<Registration>> {
        let mut entries = self.entries.lock().unwrap();
        // Retired groups need no further notifications. Do not accumulate one
        // poller for every scheduler that has used this compatible socket.
        entries.retain(|(owner, registration)| {
            let live = owner
                .upgrade()
                .is_some_and(|owner| !owner.closed.is_cancelled());
            if !live {
                deregister(registration, socket);
            }
            live
        });
        if let Some((_, registration)) = entries
            .iter()
            .find(|(owner, _)| owner.ptr_eq(&Arc::downgrade(group)))
        {
            return Ok(Arc::clone(registration));
        }
        let owner = group.route();
        let mut table = owner.table.lock().unwrap();
        if table.closed {
            return Err(closed());
        }
        let key = table.next;
        // Stable diagnostic identities are separate from reusable pointer tokens.
        table.next = key
            .checked_add(1)
            .filter(|next| *next != usize::MAX)
            .ok_or_else(|| io::Error::other("readiness event keys exhausted"))?;
        let registration = Arc::new(Registration {
            owner: Arc::downgrade(&owner),
            key,
            inactive: AtomicBool::new(false),
            readable: Direction::new(),
            writable: Direction::new(),
        });
        // Publish ownership before the kernel can return this pointer. Failed
        // adds can have installed one filter, even when rollback also fails.
        table.registrations.insert(key, Arc::clone(&registration));
        let token = registration.token();
        // SAFETY: The controller retains this record until successful deletion
        // followed by a driver batch boundary, or until the driver stops.
        if let Err(error) = unsafe {
            owner
                .poller
                .add_with_mode(socket, Event::all(token), PollMode::Edge)
        } {
            registration.inactive.store(true, Ordering::Release);
            return Err(error);
        }
        drop(table);
        entries.push((Arc::downgrade(group), Arc::clone(&registration)));
        Ok(registration)
    }
}
fn deregister(registration: &Registration, socket: &socket2::Socket) {
    registration.inactive.store(true, Ordering::Release);
    let Some(owner) = registration.owner.upgrade() else {
        // Its driver and poller have already been destroyed.
        return;
    };
    // Never release a pointer which can still be returned by the OS. A failed
    // delete leaves its inactive record quarantined until the driver stops.
    if owner.poller.delete(socket).is_err() {
        return;
    }
    let notify = {
        let mut table = owner.table.lock().unwrap();
        if let Some(record) = table.registrations.remove(&registration.key) {
            table.retired.push(record);
            owner.needs_release.store(true, Ordering::Release);
        }
        table.retired.len() == 16
    };
    if notify {
        let _ = owner.poller.notify();
    }
}

impl Drop for SocketRegistrations {
    fn drop(&mut self) {
        let socket = self
            .socket
            .upgrade()
            .expect("native owner outlives registrations");
        for (_, registration) in self.entries.get_mut().unwrap().drain(..) {
            deregister(&registration, socket.get_ref());
        }
    }
}

struct DriverState {
    group: Arc<DriverGroup>,
    controller: Arc<Controller>,
    events: Events,
}

impl DriverState {
    fn dispatch(&self) {
        for event in self.events.iter() {
            #[cfg(target_os = "macos")]
            let (event, available, read_closed) =
                (event.readiness, event.available, event.read_closed);
            // SAFETY: Only add_with_mode installs these tokens. Active records
            // are controller-owned; successful deletion retires them only at
            // the next batch boundary. Failed adds/deletes remain quarantined.
            // Reentrant closure/retirement never releases a live batch's state.
            #[cfg(any(target_os = "macos", target_os = "linux", target_os = "android"))]
            let registration =
                unsafe { &*std::ptr::with_exposed_provenance::<Registration>(event.key) };
            #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "android")))]
            let registration = {
                let record = self
                    .controller
                    .table
                    .lock()
                    .unwrap()
                    .registrations
                    .get(&event.key)
                    .cloned();
                let Some(record) = record else {
                    continue;
                };
                record
            };
            if registration.inactive.load(Ordering::Acquire) {
                continue;
            }
            if event.readable {
                #[cfg(target_os = "macos")]
                registration
                    .readable
                    .publish_readable(available, read_closed);
                #[cfg(not(target_os = "macos"))]
                registration.readable.publish();
            }
            if event.writable {
                registration.writable.publish();
            }
        }
    }
}

/// A movable readiness driver for one executor worker.
///
/// Construct with `ReadinessSelector::with_workers`, then enter and drive it.
/// Dropping any driver closes the group's readiness admission and wakes waits.
/// On platforms without edge polling, this is an already-retired driver and
/// socket operations retain the standalone selector's independent progress.
pub struct ReadinessDriver {
    inner: Option<DriverState>,
}

pub(crate) fn create(
    workers: usize,
    closed: Cancellation,
) -> io::Result<(Option<Arc<DriverGroup>>, Vec<ReadinessDriver>)> {
    create_with(workers, closed, || {
        let poller = Poller::new()?;
        Ok(poller.supports_edge().then_some(poller))
    })
}
fn create_with(
    workers: usize,
    closed: Cancellation,
    mut factory: impl FnMut() -> io::Result<Option<Poller>>,
) -> io::Result<(Option<Arc<DriverGroup>>, Vec<ReadinessDriver>)> {
    let mut controllers = Vec::with_capacity(workers);
    for _ in 0..workers {
        let Some(poller) = factory()? else {
            return Ok((
                None,
                (0..workers)
                    .map(|_| ReadinessDriver { inner: None })
                    .collect(),
            ));
        };
        controllers.push(Arc::new(Controller {
            poller,
            table: Mutex::new(Table {
                closed: false,
                next: 0,
                registrations: HashMap::new(),
                retired: Vec::new(),
            }),
            needs_release: AtomicBool::new(false),
        }));
    }
    let group = Arc::new(DriverGroup {
        closed,
        controllers,
        next: AtomicUsize::new(0),
    });
    let drivers = group
        .controllers
        .iter()
        .map(|controller| ReadinessDriver {
            inner: Some(DriverState {
                group: Arc::clone(&group),
                controller: Arc::clone(controller),
                events: Events::new(),
            }),
        })
        .collect();
    Ok((Some(group), drivers))
}

thread_local! {
    static ROUTES: RefCell<Vec<(Weak<DriverGroup>, Arc<Controller>)>> = const { RefCell::new(Vec::new()) };
}

/// A thread-bound readiness worker, entered through `SelectorDriver`.
///
/// Drop restores nested routing. Neither operations nor task futures retain
/// thread-local borrows. Driver turns publish a bounded event batch without
/// polling application futures.
pub struct ReadinessWorker {
    driver: ReadinessDriver,
    thread: PhantomData<Rc<()>>,
}
impl SelectorDriver for ReadinessDriver {
    type Worker = ReadinessWorker;
    fn enter(self) -> Self::Worker {
        if let Some(inner) = &self.inner {
            ROUTES.with(|routes| {
                routes
                    .borrow_mut()
                    .push((Arc::downgrade(&inner.group), Arc::clone(&inner.controller)))
            });
        }
        ReadinessWorker {
            driver: self,
            thread: PhantomData,
        }
    }
}
impl SelectorWorker for ReadinessWorker {
    fn waker(&self) -> Waker {
        match &self.driver.inner {
            Some(inner) => Waker::from(Arc::clone(&inner.controller)),
            None => Waker::noop().clone(),
        }
    }
    fn close(&mut self) {
        if let Some(inner) = &self.driver.inner {
            inner.group.close();
        }
    }
    fn turn(&mut self, timeout: Option<Duration>) -> io::Result<bool> {
        let Some(inner) = &mut self.driver.inner else {
            return Ok(true);
        };
        // Forget the previous batch before reclaiming successfully deleted
        // registrations. Concurrent retirements stay owned until a later turn.
        inner.events.clear();
        inner.controller.release();
        if inner.group.closed.is_cancelled() {
            return Ok(true);
        }
        if let Err(error) = inner.controller.poller.wait(&mut inner.events, timeout) {
            if error.kind() == io::ErrorKind::Interrupted {
                return Ok(false);
            }
            inner.group.close();
            return Err(error);
        }
        inner.dispatch();
        Ok(inner.group.closed.is_cancelled())
    }
}
impl Drop for ReadinessWorker {
    fn drop(&mut self) {
        if let Some(inner) = &self.driver.inner {
            ROUTES.with(|routes| {
                routes
                    .borrow_mut()
                    .retain(|(_, owner)| !Arc::ptr_eq(owner, &inner.controller))
            });
        }
    }
}
impl Drop for ReadinessDriver {
    fn drop(&mut self) {
        if let Some(inner) = &mut self.inner {
            inner.group.close();
            inner.events.clear();
            inner.controller.stop();
        }
    }
}

#[cfg(test)]
mod tests;

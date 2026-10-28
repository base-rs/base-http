// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Diagnostic single-worker readiness driver. Used only in staged benchmark
//! copies. Socket operations/retirement must stay on that worker; migration and
//! concurrent same-direction waiters are deliberately outside this experiment.

use crate::{Interest, NativeSocket, SelectorDriver, SelectorWorker};
use polling::{Event, Events, PollMode, Poller};
use std::{
    cell::RefCell,
    collections::HashMap,
    future::poll_fn,
    io,
    marker::PhantomData,
    os::fd::AsRawFd,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
    },
    task::{Poll, Wake, Waker},
    time::Duration,
};

type Socket = async_io::Async<socket2::Socket>;
const READ: u8 = 1;
const WRITE: u8 = 2;

thread_local! {
    static CURRENT: RefCell<Option<Arc<Shared>>> = const { RefCell::new(None) };
}

pub(crate) struct Registration {
    socket: Arc<Socket>,
    ready: AtomicU8,
    readers: Mutex<Option<Waker>>,
    writers: Mutex<Option<Waker>>,
}

impl Registration {
    fn publish(&self, bit: u8) {
        self.ready.fetch_or(bit, Ordering::Release);
        let slot = if bit == READ {
            &self.readers
        } else {
            &self.writers
        };
        let wake = slot.lock().unwrap().take();
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}

#[derive(Default)]
struct Registrations {
    by_fd: HashMap<i32, usize>,
    // Never reuse event keys: an event already queued for a retired descriptor
    // must not become readiness for a new descriptor using the same number.
    entries: Vec<Option<Arc<Registration>>>,
}

struct Shared {
    poller: Poller,
    registrations: Mutex<Registrations>,
}

impl Wake for Shared {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.poller.notify();
    }
}

impl Shared {
    fn register(&self, socket: &NativeSocket) -> io::Result<Arc<Registration>> {
        let fd = socket.get_ref().as_raw_fd();
        let mut registrations = self.registrations.lock().unwrap();
        if let Some(key) = registrations.by_fd.get(&fd) {
            return Ok(registrations.entries[*key].as_ref().unwrap().clone());
        }
        let key = registrations.entries.len();
        let registered = Arc::new(Registration {
            socket: socket.inner.clone(),
            ready: AtomicU8::new(READ | WRITE),
            readers: Mutex::new(None),
            writers: Mutex::new(None),
        });
        // SAFETY: The registration retains its socket until retire/clear deletes
        // it from this poller. Each key is unique for this driver's lifetime.
        unsafe {
            self.poller
                .add_with_mode(socket.get_ref(), Event::all(key), PollMode::Edge)?;
        }
        registrations.by_fd.insert(fd, key);
        registrations.entries.push(Some(registered.clone()));
        Ok(registered)
    }

    fn retire(&self, socket: &NativeSocket) {
        let registered = {
            let mut registrations = self.registrations.lock().unwrap();
            registrations
                .by_fd
                .remove(&socket.get_ref().as_raw_fd())
                .and_then(|key| registrations.entries[key].take())
        };
        if let Some(registered) = registered {
            self.poller
                .delete(registered.socket.get_ref())
                .expect("delete before close");
            registered.publish(READ);
            registered.publish(WRITE);
        }
    }

    fn clear(&self) {
        let registrations = {
            let mut slots = self.registrations.lock().unwrap();
            std::mem::take(&mut *slots)
        };
        for registered in registrations.entries.into_iter().flatten() {
            let _ = self.poller.delete(registered.socket.get_ref());
            registered.publish(READ);
            registered.publish(WRITE);
        }
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        self.clear();
    }
}

/// Single-worker diagnostic driver using persistent edge readiness.
pub struct Driver(Arc<Shared>);

impl Driver {
    pub fn new() -> io::Result<Self> {
        Ok(Self(Arc::new(Shared {
            poller: Poller::new()?,
            registrations: Mutex::default(),
        })))
    }
}

pub struct Worker {
    shared: Arc<Shared>,
    events: Events,
    previous: Option<Arc<Shared>>,
    closed: bool,
    _local: PhantomData<Rc<()>>,
}

impl SelectorDriver for Driver {
    type Worker = Worker;
    fn enter(self) -> Worker {
        let previous = CURRENT.with(|current| current.replace(Some(self.0.clone())));
        Worker {
            shared: self.0,
            events: Events::new(),
            previous,
            closed: false,
            _local: PhantomData,
        }
    }
}

impl SelectorWorker for Worker {
    fn waker(&self) -> Waker {
        Waker::from(self.shared.clone())
    }
    fn close(&mut self) {
        self.closed = true;
        self.shared.clear();
    }
    fn turn(&mut self, timeout: Option<Duration>) -> io::Result<bool> {
        if self.closed {
            return Ok(true);
        }
        self.events.clear();
        self.shared.poller.wait(&mut self.events, timeout)?;
        for event in self.events.iter() {
            let registered = self
                .shared
                .registrations
                .lock()
                .unwrap()
                .entries
                .get(event.key)
                .and_then(Clone::clone);
            if let Some(registered) = registered {
                if event.readable {
                    registered.publish(READ);
                }
                if event.writable {
                    registered.publish(WRITE);
                }
            }
        }
        Ok(false)
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.shared.clear();
        CURRENT.with(|current| {
            current.replace(self.previous.take());
        });
    }
}

pub(crate) fn registration(socket: &NativeSocket) -> Option<io::Result<Arc<Registration>>> {
    CURRENT.with(|current| {
        current
            .borrow()
            .as_ref()
            .map(|shared| shared.register(socket))
    })
}

pub(crate) fn retire(socket: &NativeSocket) {
    CURRENT.with(|current| {
        if let Some(shared) = current.borrow().as_ref() {
            shared.retire(socket);
        }
    });
}

pub(crate) async fn io<T>(
    registration: Arc<Registration>,
    interest: Interest,
    mut operation: impl FnMut(&socket2::Socket) -> io::Result<T>,
) -> io::Result<T> {
    let bit = match interest {
        Interest::Readable => READ,
        Interest::Writable => WRITE,
    };
    let slot = if bit == READ {
        &registration.readers
    } else {
        &registration.writers
    };
    poll_fn(|context| {
        loop {
            if registration.ready.load(Ordering::Acquire) & bit != 0 {
                let result = operation(registration.socket.get_ref());
                if !matches!(&result, Err(error) if error.kind() == io::ErrorKind::WouldBlock) {
                    return Poll::Ready(result);
                }
                registration.ready.fetch_and(!bit, Ordering::AcqRel);
            }
            *slot.lock().unwrap() = Some(context.waker().clone());
            if registration.ready.load(Ordering::Acquire) & bit == 0 {
                return Poll::Pending;
            }
        }
    })
    .await
}

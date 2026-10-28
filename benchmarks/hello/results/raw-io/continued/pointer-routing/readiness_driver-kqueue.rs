// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Worker-owned macOS kqueue. Registrations use persistent EV_CLEAR filters;
//! EVFILT_USER interrupts idle workers without a pipe or a reactor thread.

use polling::{Event, PollMode};
use std::{
    io,
    mem::MaybeUninit,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    ptr,
    time::Duration,
};

const INTERRUPT: usize = usize::MAX;
const BATCH: usize = 1024;

#[cfg(test)]
thread_local! {
    pub(super) static INITIAL_FAILURE: std::cell::Cell<i32> = const { std::cell::Cell::new(0) };
}

pub(super) struct Poller {
    queue: OwnedFd,
    #[cfg(test)]
    pub(super) failure: std::sync::atomic::AtomicI32,
    #[cfg(test)]
    pub(super) partial_add_error: std::sync::atomic::AtomicI32,
}
impl Poller {
    pub(super) fn new() -> io::Result<Self> {
        Self::new_with(
            // SAFETY: kqueue creates a new descriptor with no borrowed storage.
            || unsafe { libc::kqueue() },
            // SAFETY: F_SETFD takes an integer and borrows this owned descriptor.
            |fd| unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) },
        )
    }
    fn new_with(open: impl FnOnce() -> i32, cloexec: impl FnOnce(i32) -> i32) -> io::Result<Self> {
        let fd = open();
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: This is the sole owner of the newly created descriptor.
        let queue = unsafe { OwnedFd::from_raw_fd(fd) };
        if cloexec(fd) < 0 {
            return Err(io::Error::last_os_error());
        }
        let poller = Self {
            queue,
            #[cfg(test)]
            failure: std::sync::atomic::AtomicI32::new(INITIAL_FAILURE.with(std::cell::Cell::take)),
            #[cfg(test)]
            partial_add_error: std::sync::atomic::AtomicI32::new(0),
        };
        poller.changes(&[event(
            INTERRUPT,
            libc::EVFILT_USER,
            libc::EV_ADD | libc::EV_CLEAR,
            0,
            INTERRUPT,
        )])?;
        Ok(poller)
    }
    // Resource-local fault injection exercises OS failure retirement without
    // changing production dispatch or affecting another test's descriptors.
    unsafe fn kevent(
        &self,
        queue: i32,
        changes: *const libc::kevent,
        nchanges: i32,
        events: *mut libc::kevent,
        nevents: i32,
        timeout: *const libc::timespec,
    ) -> i32 {
        #[cfg(test)]
        {
            let error = self.failure.swap(0, std::sync::atomic::Ordering::Relaxed);
            if error != 0 {
                // SAFETY: errno is thread-local on macOS.
                unsafe {
                    *libc::__error() = error;
                }
                return -1;
            }
        }
        // SAFETY: The caller provides storage valid for this synchronous call.
        unsafe { libc::kevent(queue, changes, nchanges, events, nevents, timeout) }
    }
    pub(super) fn supports_edge(&self) -> bool {
        true
    }

    /// The caller must delete the source before destroying its native owner.
    pub(super) unsafe fn add_with_mode(
        &self,
        socket: &socket2::Socket,
        interest: Event,
        mode: PollMode,
    ) -> io::Result<()> {
        debug_assert_eq!(mode, PollMode::Edge);
        let fd = socket.as_raw_fd() as usize;
        let flags = libc::EV_ADD | libc::EV_CLEAR;
        #[cfg(test)]
        {
            let error = self.partial_add_error.swap(0, std::sync::atomic::Ordering::Relaxed);
            if error != 0 {
                // Deterministically model one installed filter followed by an
                // add error and failed rollback, using a real readable source.
                self.changes(&[event(fd, libc::EVFILT_READ, flags, 0, interest.key)])?;
                self.failure.store(error, std::sync::atomic::Ordering::Relaxed);
                let _ = self.delete(socket);
                return Err(io::Error::from_raw_os_error(error));
            }
        }
        let result = self.changes(&[
            event(fd, libc::EVFILT_READ, flags, 0, interest.key),
            event(fd, libc::EVFILT_WRITE, flags, 0, interest.key),
        ]);
        if result.is_err() {
            // A receipt can report partial registration. Roll back both filters.
            let _ = self.delete(socket);
        }
        result
    }
    pub(super) fn delete(&self, socket: &socket2::Socket) -> io::Result<()> {
        let fd = socket.as_raw_fd() as usize;
        self.changes(&[
            event(fd, libc::EVFILT_READ, libc::EV_DELETE, 0, 0),
            event(fd, libc::EVFILT_WRITE, libc::EV_DELETE, 0, 0),
        ])
    }
    // Receipts prevent a successful registration change from consuming pending
    // readiness events. Fixed stack storage avoids per-change allocations.
    fn changes(&self, changes: &[libc::kevent]) -> io::Result<()> {
        let mut input = [event(0, 0, 0, 0, 0); 2];
        for (target, source) in input.iter_mut().zip(changes) {
            *target = *source;
            target.flags |= libc::EV_RECEIPT;
        }
        let mut receipts = [MaybeUninit::<libc::kevent>::uninit(); 2];
        let count = self.submit(&input[..changes.len()], &mut receipts)?;
        for (receipt, change) in receipts[..count].iter().zip(changes) {
            // SAFETY: The returned prefix was initialized by kevent.
            check_receipt(unsafe { receipt.assume_init_ref() }, change)?;
        }
        Ok(())
    }
    pub(super) fn notify(&self) -> io::Result<()> {
        let trigger = event(
            INTERRUPT,
            libc::EVFILT_USER,
            0,
            libc::NOTE_TRIGGER,
            INTERRUPT,
        );
        self.submit(&[trigger], &mut []).map(|_| ())
    }
    // Changes and interrupts are idempotent. Retry EINTR so a racing signal
    // cannot lose a worker wake or leave a registration only partly removed.
    fn submit(
        &self,
        changes: &[libc::kevent],
        events: &mut [MaybeUninit<libc::kevent>],
    ) -> io::Result<usize> {
        loop {
            // SAFETY: Both arrays live through this synchronous call. The
            // returned prefix is the only initialized part of the result array.
            let result = unsafe {
                self.kevent(
                    self.queue.as_raw_fd(),
                    changes.as_ptr(),
                    changes.len() as i32,
                    events.as_mut_ptr().cast(),
                    events.len() as i32,
                    &ZERO,
                )
            };
            if result >= 0 {
                return Ok(result as usize);
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }
    pub(super) fn wait(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<usize> {
        let timeout = timeout.map(|duration| libc::timespec {
            tv_sec: duration.as_secs().min(libc::time_t::MAX as u64) as libc::time_t,
            tv_nsec: duration.subsec_nanos() as libc::c_long,
        });
        // SAFETY: kevent writes only into this exclusively borrowed result
        // array, synchronously. No userspace pointers survive the call.
        let count = unsafe {
            self.kevent(
                self.queue.as_raw_fd(),
                ptr::null(),
                0,
                events.raw.as_mut_ptr().cast(),
                BATCH as i32,
                timeout.as_ref().map_or(ptr::null(), |value| value),
            )
        };
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        for raw in &events.raw[..count as usize] {
            // SAFETY: kevent initialized this returned prefix.
            let raw = unsafe { raw.assume_init_ref() };
            if raw.filter == libc::EVFILT_USER {
                continue;
            }
            let error = raw.flags & libc::EV_ERROR != 0;
            let mut ready = Event::none(raw.udata as usize);
            ready.readable = raw.filter == libc::EVFILT_READ || error;
            ready.writable = raw.filter == libc::EVFILT_WRITE || error;
            events.ready.push(ReadyEvent {
                readiness: ready,
                available: raw.data.max(0) as usize,
                read_closed: raw.filter == libc::EVFILT_READ && raw.flags & libc::EV_EOF != 0,
            });
        }
        Ok(events.ready.len())
    }
}
fn check_receipt(receipt: &libc::kevent, change: &libc::kevent) -> io::Result<()> {
    if receipt.flags & libc::EV_ERROR != 0 && receipt.data != 0 {
        // Absence establishes deletion. EPIPE is accepted only for adding a
        // closed write side; it cannot prove an old pointer was deregistered.
        let absent = receipt.data == libc::ENOENT as isize;
        let closed_add = receipt.data == libc::EPIPE as isize && change.flags & libc::EV_ADD != 0;
        if !absent && !closed_add {
            return Err(io::Error::from_raw_os_error(receipt.data as i32));
        }
    }
    Ok(())
}

const ZERO: libc::timespec = libc::timespec {
    tv_sec: 0,
    tv_nsec: 0,
};
fn event(ident: usize, filter: i16, flags: u16, fflags: u32, key: usize) -> libc::kevent {
    libc::kevent {
        ident,
        filter,
        flags,
        fflags,
        data: 0,
        udata: key as *mut libc::c_void,
    }
}

pub(super) struct Events {
    raw: Box<[MaybeUninit<libc::kevent>; BATCH]>,
    ready: Vec<ReadyEvent>,
}
#[derive(Clone, Copy)]
pub(super) struct ReadyEvent {
    pub(super) readiness: Event,
    pub(super) available: usize,
    pub(super) read_closed: bool,
}
// SAFETY: udata contains opaque address tokens. This array only transports
// them; the owning readiness driver controls all dereference and retirement.
// The raw result array has one owner and is accessed only by a synchronous wait.
unsafe impl Send for Events {}
impl Events {
    pub(super) fn new() -> Self {
        Self {
            raw: Box::new([MaybeUninit::uninit(); BATCH]),
            ready: Vec::with_capacity(BATCH),
        }
    }
    pub(super) fn clear(&mut self) {
        self.ready.clear();
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = ReadyEvent> + '_ {
        self.ready.iter().copied()
    }
}

#[cfg(test)]
mod tests;

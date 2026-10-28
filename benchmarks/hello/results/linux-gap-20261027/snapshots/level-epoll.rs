// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Worker-owned Linux epoll. One worker owns each wait loop and result buffer.

use polling::{Event, PollMode};
use std::{
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    ptr,
    time::Duration,
};
mod syscalls;

pub(super) const INTERRUPT: usize = usize::MAX;
const TIMER: usize = usize::MAX - 1;
const BATCH: usize = 1024;
const READ: u32 = (libc::EPOLLIN | libc::EPOLLPRI | libc::EPOLLERR | libc::EPOLLHUP) as u32;
const WRITE: u32 = (libc::EPOLLOUT | libc::EPOLLERR | libc::EPOLLHUP) as u32;

pub(super) struct Poller {
    backend: Backend,
}
enum Backend {
    Native(NativePoller),
    Compatible(polling::Poller),
}
impl Poller {
    pub(super) fn new() -> io::Result<Self> {
        let backend = match NativePoller::try_new()? {
            Some(native) => Backend::Native(native),
            // Choose compatibility before exposing any socket registrations. polling retains its optional timerfd and pipe-notifier fallbacks.
            None => Backend::Compatible(syscalls::compatible_new()?),
        };
        Ok(Self { backend })
    }
    pub(super) fn supports_edge(&self) -> bool {
        match &self.backend {
            Backend::Native(_) => true,
            Backend::Compatible(poller) => poller.supports_edge(),
        }
    }
    /// The socket must remain owned until synchronous deletion succeeds or this poller stops.
    pub(super) unsafe fn add_with_mode(
        &self,
        socket: &socket2::Socket,
        event: Event,
        mode: PollMode,
    ) -> io::Result<()> {
        match &self.backend {
            Backend::Native(poller) => {
                debug_assert_eq!(mode, PollMode::Edge);
                poller.change(
                    libc::EPOLL_CTL_ADD,
                    socket.as_raw_fd(),
                    event.key,
                    READ,
                )
            }
            // SAFETY: The caller retains the socket through deletion or poller shutdown.
            Backend::Compatible(poller) => unsafe { poller.add_with_mode(socket, event, mode) },
        }
    }
    pub(super) fn delete(&self, socket: &socket2::Socket) -> io::Result<()> {
        match &self.backend {
            Backend::Native(poller) => poller.change(libc::EPOLL_CTL_DEL, socket.as_raw_fd(), 0, 0),
            Backend::Compatible(poller) => poller.delete(socket),
        }
    }
    pub(super) fn notify(&self) -> io::Result<()> {
        match &self.backend {
            Backend::Native(poller) => poller.notify(),
            Backend::Compatible(poller) => poller.notify(),
        }
    }
    pub(super) fn clear_notification(&self) -> io::Result<()> {
        match &self.backend {
            Backend::Native(poller) => poller.clear_notification(),
            Backend::Compatible(_) => {
                unreachable!("polling filters its notification token before dispatch")
            }
        }
    }
    pub(super) fn wait(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<usize> {
        match &self.backend {
            Backend::Native(poller) => poller.wait(&mut events.raw, timeout),
            Backend::Compatible(poller) => {
                let compatible = events.compatible.get_or_insert_with(polling::Events::new);
                compatible.clear();
                let count = syscalls::compatible_wait(poller, compatible, timeout)?;
                // Normalize only the compatibility path; the native worker dispatches directly from its kernel result vector.
                events
                    .raw
                    .extend(compatible.iter().map(|event| libc::epoll_event {
                        events: (u32::from(event.readable) * libc::EPOLLIN as u32)
                            | (u32::from(event.writable) * libc::EPOLLOUT as u32),
                        u64: event.key as u64,
                    }));
                Ok(count)
            }
        }
    }
}
fn flags(event: Event) -> u32 {
    let mut flags = 0;
    if event.readable {
        flags |= READ;
    }
    if event.writable {
        flags |= WRITE;
    }
    flags
}

struct NativePoller {
    queue: OwnedFd,
    interrupt: OwnedFd,
    timer: OwnedFd,
}
impl NativePoller {
    fn try_new() -> io::Result<Option<Self>> {
        // SAFETY: The calls create new descriptors with no borrowed storage.
        let queue = owned(unsafe { syscalls::epoll_create1(libc::EPOLL_CLOEXEC) })?;
        let Ok(interrupt) =
            owned(unsafe { syscalls::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) })
        else {
            return Ok(None);
        };
        let Ok(timer) = owned(unsafe {
            syscalls::timerfd_create(
                libc::CLOCK_MONOTONIC,
                libc::TFD_CLOEXEC | libc::TFD_NONBLOCK,
            )
        }) else {
            // polling treats timerfd as optional, including when descriptor limits prevent creating it.
            return Ok(None);
        };
        let poller = Self {
            queue,
            interrupt,
            timer,
        };
        poller.change(
            libc::EPOLL_CTL_ADD,
            poller.interrupt.as_raw_fd(),
            INTERRUPT,
            libc::EPOLLIN as u32,
        )?;
        poller.change(
            libc::EPOLL_CTL_ADD,
            poller.timer.as_raw_fd(),
            TIMER,
            libc::EPOLLIN as u32,
        )?;
        Ok(Some(poller))
    }
    fn change(&self, operation: i32, descriptor: i32, key: usize, flags: u32) -> io::Result<()> {
        let mut event = libc::epoll_event {
            events: flags,
            u64: key as u64,
        };
        loop {
            // SAFETY: Descriptor owners and event storage live through this synchronous call.
            let result = unsafe {
                syscalls::epoll_ctl(self.queue.as_raw_fd(), operation, descriptor, &mut event)
            };
            if result >= 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }
    fn notify(&self) -> io::Result<()> {
        let value = 1u64;
        loop {
            // SAFETY: eventfd synchronously borrows exactly eight initialized bytes.
            let result = unsafe {
                syscalls::write(self.interrupt.as_raw_fd(), (&value as *const u64).cast(), 8)
            };
            if result >= 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            match error.kind() {
                io::ErrorKind::Interrupted => continue,
                // A saturated eventfd is already readable and retains the wake.
                io::ErrorKind::WouldBlock => return Ok(()),
                _ => return Err(error),
            }
        }
    }
    fn clear_notification(&self) -> io::Result<()> {
        let mut value = 0u64;
        loop {
            // SAFETY: The result has room for eventfd's eight-byte counter.
            let result = unsafe {
                syscalls::read(
                    self.interrupt.as_raw_fd(),
                    (&mut value as *mut u64).cast(),
                    8,
                )
            };
            if result >= 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            match error.kind() {
                io::ErrorKind::Interrupted => continue,
                io::ErrorKind::WouldBlock => return Ok(()),
                _ => return Err(error),
            }
        }
    }
    fn set_timeout(&self, duration: Duration) -> io::Result<()> {
        let setting = libc::itimerspec {
            it_interval: libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            it_value: libc::timespec {
                tv_sec: duration.as_secs().min(libc::time_t::MAX as u64) as libc::time_t,
                tv_nsec: duration.subsec_nanos() as libc::c_long,
            },
        };
        loop {
            // SAFETY: The kernel borrows the initialized setting only for this call; no old setting is requested.
            let result = unsafe {
                syscalls::timerfd_settime(self.timer.as_raw_fd(), 0, &setting, ptr::null_mut())
            };
            if result >= 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }
    fn wait(
        &self,
        events: &mut Vec<libc::epoll_event>,
        timeout: Option<Duration>,
    ) -> io::Result<usize> {
        events.reserve(BATCH);
        let length = events.len();
        let timed = timeout.is_some_and(|duration| !duration.is_zero());
        if let Some(duration) = timeout.filter(|_| timed) {
            self.set_timeout(duration)?;
        }
        let millis = if timeout == Some(Duration::ZERO) {
            0
        } else {
            -1
        };
        // SAFETY: epoll writes exclusively into BATCH reserved tail entries. The null signal mask preserves the worker's existing mask.
        let count = unsafe {
            syscalls::epoll_pwait(
                self.queue.as_raw_fd(),
                events.as_mut_ptr().add(length),
                BATCH as i32,
                millis,
                ptr::null(),
            )
        };
        // Capture errno before timer cleanup can replace it.
        let result = if count < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(count as usize)
        };
        if timed {
            // Disarming resets timerfd's expiration count as well as cancelling the timer. Always do this after an interrupted/failed wait too; a cleanup failure is terminal, even if the wait itself was interrupted.
            self.set_timeout(Duration::ZERO)?;
        }
        let count = result?;
        // SAFETY: Only the returned prefix was initialized by the synchronous wait; errors leave the existing prefix unchanged.
        unsafe { events.set_len(length + count) };
        Ok(count)
    }
}
fn owned(descriptor: i32) -> io::Result<OwnedFd> {
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: The caller transfers sole ownership of a newly created descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(descriptor) })
}

pub(super) struct Events {
    raw: Vec<libc::epoll_event>,
    compatible: Option<polling::Events>,
}
impl Events {
    pub(super) fn new() -> Self {
        Self {
            raw: Vec::with_capacity(BATCH),
            compatible: None,
        }
    }
    pub(super) fn clear(&mut self) {
        self.raw.clear();
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = Event> + '_ {
        // Timer expirations end a timed turn; disarm already reset their counter. Interrupts are drained during the same dispatch pass, before pointer decoding.
        self.raw
            .iter()
            .filter(|event| event.u64 as usize != TIMER)
            .map(|event| {
                Event::new(
                    event.u64 as usize,
                    event.events & READ != 0,
                    event.events & WRITE != 0,
                )
            })
    }
}

#[cfg(test)]
mod tests;

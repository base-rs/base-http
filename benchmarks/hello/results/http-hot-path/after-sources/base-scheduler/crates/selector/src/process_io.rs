// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{Cancellation, Cancelled, ProcessWaitFlags, ProcessWaitResult};
use std::{io, process::ExitStatus};

#[cfg(unix)]
pub(crate) type ProcessId = libc::pid_t;
#[cfg(windows)]
pub(crate) type ProcessId = u32;

pub(crate) struct Process {
    pid: ProcessId,
    flags: ProcessWaitFlags,
    #[cfg(windows)]
    handle: std::os::windows::io::OwnedHandle,
}

impl Process {
    pub(crate) fn open(pid: ProcessId, flags: ProcessWaitFlags) -> io::Result<Self> {
        #[cfg(unix)]
        if pid == ProcessId::MIN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "process group ID cannot be represented",
            ));
        }
        #[cfg(windows)]
        let handle = {
            use std::os::windows::io::FromRawHandle;
            use windows_sys::Win32::System::Threading::{
                OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE,
            };
            if flags.untraced || flags.continued {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "Windows does not report stopped or continued process states",
                ));
            }
            // SAFETY: OpenProcess accepts a numeric ID and returns a newly owned handle. It receives no userspace pointers.
            let handle = unsafe {
                OpenProcess(
                    PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                    0,
                    pid,
                )
            };
            if handle.is_null() {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: A successful OpenProcess call transfers ownership of this handle to us.
            unsafe { std::os::windows::io::OwnedHandle::from_raw_handle(handle) }
        };
        Ok(Self {
            pid,
            flags,
            #[cfg(windows)]
            handle,
        })
    }

    #[cfg(unix)]
    pub(crate) fn try_wait(&self) -> io::Result<Option<ProcessWaitResult>> {
        self.wait(true)
    }

    pub(crate) fn wait(&self, nonblocking: bool) -> io::Result<Option<ProcessWaitResult>> {
        #[cfg(test)]
        tests::before_wait(self.pid, nonblocking || self.flags.nohang);
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            let mut options = if nonblocking || self.flags.nohang {
                libc::WNOHANG
            } else {
                0
            };
            if self.flags.untraced {
                options |= libc::WUNTRACED;
            }
            if self.flags.continued {
                options |= libc::WCONTINUED;
            }
            let mut status = 0;
            // SAFETY: status is a live writable integer for this synchronous call. The OS does not retain its address.
            let pid = syscall("waitpid", || unsafe {
                libc::waitpid(self.pid, &mut status, options)
            })?;
            if pid == 0 {
                Ok(None)
            } else {
                Ok(Some(ProcessWaitResult {
                    pid: pid as u32,
                    status: ExitStatus::from_raw(status),
                }))
            }
        }
        #[cfg(windows)]
        {
            use std::os::windows::{io::AsRawHandle, process::ExitStatusExt};
            use windows_sys::Win32::{
                Foundation::{WAIT_OBJECT_0, WAIT_TIMEOUT},
                System::Threading::{GetExitCodeProcess, WaitForSingleObject},
            };
            // SAFETY: The owned process handle remains live throughout this synchronous wait.
            match windows_call("wait", || unsafe {
                WaitForSingleObject(
                    self.handle.as_raw_handle(),
                    if nonblocking || self.flags.nohang {
                        0
                    } else {
                        u32::MAX
                    },
                )
            }) {
                WAIT_OBJECT_0 => {
                    let mut status = 0;
                    // SAFETY: The handle is live and status is writable for this synchronous call.
                    if windows_call("exit-code", || unsafe {
                        GetExitCodeProcess(self.handle.as_raw_handle(), &mut status) as u32
                    }) == 0
                    {
                        return Err(io::Error::last_os_error());
                    }
                    // 259 is a valid exit code once the handle is signaled.
                    Ok(Some(ProcessWaitResult {
                        pid: self.pid,
                        status: ExitStatus::from_raw(status),
                    }))
                }
                WAIT_TIMEOUT => Ok(None),
                _ => Err(io::Error::last_os_error()),
            }
        }
    }
}

pub(crate) fn operation_result<T>(
    result: io::Result<T>,
    cancellation: &Cancellation,
) -> base_scheduler::OperationResult<T> {
    match result {
        Err(error) if error.kind() == io::ErrorKind::Interrupted && cancellation.is_cancelled() => {
            Err(Cancelled.into())
        }
        result => result.map_err(Into::into),
    }
}

#[cfg(unix)]
impl Process {
    // Only individual exit waits fit pidfd/kqueue notification. Other selectors and state changes use the pool fallback.
    pub(crate) fn notification(&self) -> io::Result<Option<std::os::fd::OwnedFd>> {
        if self.pid <= 0 || self.flags.untraced || self.flags.continued {
            return Ok(None);
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::FromRawFd;
            // SAFETY: pidfd_open accepts integers and returns a newly owned descriptor, without accessing userspace pointers.
            match syscall("pidfd", || unsafe {
                libc::syscall(libc::SYS_pidfd_open, self.pid, 0) as i32
            }) {
                // SAFETY: A successful pidfd_open call returns a new descriptor owned by this call.
                Ok(fd) => Ok(Some(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) })),
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::ENOSYS | libc::EINVAL | libc::ENODEV | libc::ESRCH)
                    ) =>
                {
                    Ok(None)
                }
                Err(error) => Err(error),
            }
        }
        #[cfg(any(target_os = "macos", target_os = "freebsd"))]
        {
            use std::os::fd::{AsRawFd, FromRawFd};
            // SAFETY: kqueue creates a new descriptor without accessing userspace pointers.
            let fd = syscall("kqueue", || unsafe { libc::kqueue() })?;
            // SAFETY: A successful kqueue call returns a new descriptor owned by this call.
            let fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) };
            // SAFETY: fcntl accesses only the live descriptor and integer flags.
            syscall("cloexec", || unsafe {
                libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC)
            })?;
            let event = libc::kevent {
                ident: self.pid as _,
                filter: libc::EVFILT_PROC,
                flags: libc::EV_ADD | libc::EV_ENABLE | libc::EV_ONESHOT,
                fflags: libc::NOTE_EXIT,
                data: 0,
                udata: std::ptr::null_mut(),
            };
            // SAFETY: event remains live throughout registration. No output or retained userspace pointer is supplied.
            let result = syscall("kevent", || unsafe {
                libc::kevent(
                    fd.as_raw_fd(),
                    &event,
                    1,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null(),
                )
            });
            if let Err(error) = result {
                // Exit may race registration before the child becomes reapable. The pool bridges this short transition.
                if error.raw_os_error() == Some(libc::ESRCH) {
                    return Ok(None);
                }
                return Err(error);
            }
            Ok(Some(fd))
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "freebsd")))]
        {
            Ok(None)
        }
    }
}

// A small syscall boundary lets tests exercise OS failures and readiness/reaping races without exhausting process-wide resources.
#[cfg(unix)]
fn syscall(name: &'static str, operation: impl FnOnce() -> i32) -> io::Result<i32> {
    #[cfg(test)]
    if let Some(result) = tests::injected(name) {
        return result;
    }
    #[cfg(not(test))]
    let _ = name;
    let result = operation();
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

#[cfg(test)]
pub(crate) mod tests;

#[cfg(windows)]
fn windows_call(name: &'static str, operation: impl FnOnce() -> u32) -> u32 {
    #[cfg(test)]
    if let Some(result) = tests::windows_injected(name) {
        return result;
    }
    #[cfg(not(test))]
    let _ = name;
    operation()
}

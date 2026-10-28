// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use std::{cell::UnsafeCell, os::unix::process::ExitStatusExt, process::ExitStatus};

type WaitResult = io::Result<Option<ProcessWaitResult>>;

// The driver owns this stable allocation until the original WAITID CQE. Neither dropping the waiter nor a cancellation CQE can free the kernel's output destination.
struct WaitRequest {
    identifier: u64,
    _permit: Permit,
    process: i32,
    flags: ProcessWaitFlags,
    info: UnsafeCell<libc::siginfo_t>,
    reply: Cell<Option<oneshot::Sender<WaitResult>>>,
    cancelling: Cell<bool>,
}

// SAFETY: The SIGCHLD result contains only scalar values. Its stable allocation moves by ownership between queues; only the owning driver publishes it, and userspace reads it only after terminal kernel completion. No siginfo union pointer is followed.
unsafe impl Send for WaitRequest {}

impl PendingRequest for WaitRequest {
    fn identifier(&self) -> u64 {
        self.identifier
    }
    fn entry(&mut self) -> squeue::Entry {
        let (kind, id) = match self.process {
            -1 => (libc::P_ALL, 0),
            0 => (libc::P_PGID, 0),
            pid if pid < -1 => (libc::P_PGID, pid.unsigned_abs()),
            pid => (libc::P_PID, pid as u32),
        };
        let mut options = libc::WEXITED;
        if self.flags.nohang {
            options |= libc::WNOHANG;
        }
        if self.flags.untraced {
            options |= libc::WSTOPPED;
        }
        if self.flags.continued {
            options |= libc::WCONTINUED;
        }
        opcode::WaitId::new(kind, id, options)
            .infop(self.info.get())
            .build()
            .user_data(self.identifier)
    }
    fn entry_may_wake(&self) -> bool {
        false
    }
    fn finish(self: Box<Self>, result: io::Result<usize>) {
        let result = result.and_then(|_| {
            // SAFETY: Only terminal original completion or rejection reaches finish. No kernel access remains, and this request exclusively owns the initialized siginfo storage.
            let info = unsafe { &*self.info.get() };
            status(info)
        });
        let reply = self
            .reply
            .take()
            .expect("unfinished process wait retains its sender");
        drop(self);
        discard(reply.send(result));
    }
    fn begin_cancellation(&self) -> bool {
        !self.cancelling.replace(true)
    }
    fn take_reply(&self) -> Option<Box<dyn Send>> {
        self.reply
            .take()
            .map(|reply| Box::new(reply) as Box<dyn Send>)
    }
}

impl Drop for WaitRequest {
    fn drop(&mut self) {
        if let Some(reply) = self.reply.take() {
            discard(reply.send(Err(io::ErrorKind::BrokenPipe.into())));
        }
    }
}

fn status(info: &libc::siginfo_t) -> WaitResult {
    // Older kernels leave stale child fields on WNOHANG without an event, but always clear si_signo. Check that sentinel before interpreting any child fields.
    if info.si_signo == 0 {
        return Ok(None);
    }
    // SAFETY: A completed WAITID with a signal reports the SIGCHLD fields. No union pointer is followed.
    child_status(unsafe { info.si_pid() }, info.si_code, unsafe {
        info.si_status()
    })
}

fn child_status(pid: i32, code: i32, value: i32) -> WaitResult {
    let raw = match code {
        libc::CLD_EXITED => value << 8,
        libc::CLD_KILLED => value,
        libc::CLD_DUMPED => value | 0x80,
        libc::CLD_STOPPED | libc::CLD_TRAPPED => (value << 8) | 0x7f,
        libc::CLD_CONTINUED => 0xffff,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected WAITID child status",
            ));
        }
    };
    Ok(Some(ProcessWaitResult {
        pid: pid as u32,
        status: ExitStatus::from_raw(raw),
    }))
}

impl ProcessWait for IoUringSelector {
    type ProcessId = i32;
    async fn process_wait(
        &self,
        process: i32,
        flags: ProcessWaitFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<Option<ProcessWaitResult>> {
        cancellation.check()?;
        self.check_open()?;
        if process == i32::MIN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "process group ID cannot be represented",
            )
            .into());
        }
        let controller = self.route();
        let supported = controller.waitid_supported;
        #[cfg(test)]
        let supported = supported && !tests::FORCE_FALLBACK.get();
        if !supported {
            return self
                .fallback
                .process_wait(process, flags, cancellation)
                .await;
        }
        let permit = controller
            .capacity
            .acquire(cancellation)
            .await
            .map_err(|error| operation_result::<()>(Err(error), cancellation).unwrap_err())?;
        let identifier = controller.next_identifier()?;
        let (reply, result) = oneshot::channel();
        let request = WaitRequest {
            identifier,
            _permit: permit,
            process,
            flags,
            // SAFETY: An all-zero siginfo_t is valid initialization, including the no-status si_pid sentinel.
            info: UnsafeCell::new(unsafe { std::mem::zeroed() }),
            reply: Cell::new(Some(reply)),
            cancelling: Cell::new(false),
        };
        let result = Self::submit(controller, Box::new(request), result, cancellation).await;
        // Opcode probing is authoritative. Permission errors and ECHILD are real errors, not reasons to retry with another reaper.
        operation_result(result, cancellation)
    }
}

#[cfg(test)]
mod tests;

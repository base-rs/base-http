// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;

#[cfg(unix)]
use std::{cell::RefCell, collections::VecDeque};
#[cfg(unix)]
thread_local! {
    static INJECTIONS: RefCell<VecDeque<(&'static str, io::Result<i32>)>> = const { RefCell::new(VecDeque::new()) };
}
#[cfg(unix)]
pub(crate) fn injected(name: &'static str) -> Option<io::Result<i32>> {
    INJECTIONS.with(|state| {
        let mut state = state.borrow_mut();
        if state.front().is_some_and(|(expected, _)| *expected == name) {
            state.pop_front().map(|(_, result)| result)
        } else {
            None
        }
    })
}
#[cfg(unix)]
pub(crate) struct Injection;
#[cfg(unix)]
impl Injection {
    pub(crate) fn new(values: impl IntoIterator<Item = (&'static str, io::Result<i32>)>) -> Self {
        INJECTIONS.with(|state| *state.borrow_mut() = values.into_iter().collect());
        Self
    }
}
#[cfg(unix)]
impl Drop for Injection {
    fn drop(&mut self) {
        INJECTIONS.with(|state| state.borrow_mut().clear());
    }
}

#[test]
fn cancellation_and_io_errors_are_distinct() {
    let cancellation = Cancellation::new();
    cancellation.cancel();
    let result = operation_result::<()>(Err(io::ErrorKind::Interrupted.into()), &cancellation);
    assert_eq!(
        std::mem::discriminant(&result.unwrap_err()),
        std::mem::discriminant(&base_scheduler::OperationError::from(Cancelled))
    );
    assert_eq!(
        operation_result::<()>(Err(io::ErrorKind::PermissionDenied.into()), &cancellation)
            .unwrap_err()
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        operation_result::<()>(
            Err(io::ErrorKind::Interrupted.into()),
            &Cancellation::never()
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::Interrupted
    );
    operation_result(Ok(()), &cancellation).unwrap();
}

#[cfg(unix)]
#[test]
fn interrupted_wait_preserves_the_os_error() {
    let process =
        Process::open(std::process::id() as ProcessId, ProcessWaitFlags::default()).unwrap();
    let _injection = Injection::new([("waitpid", Err(io::Error::from_raw_os_error(libc::EINTR)))]);
    assert_eq!(
        process.wait(false).unwrap_err().raw_os_error(),
        Some(libc::EINTR)
    );
    assert_eq!(
        process.wait(true).unwrap_err().raw_os_error(),
        Some(libc::ECHILD)
    );
    assert!(Process::open(ProcessId::MIN, ProcessWaitFlags::default()).is_err());
}

#[cfg(windows)]
#[test]
fn windows_requires_a_process_and_supported_state_flags() {
    assert!(Process::open(0, ProcessWaitFlags::default()).is_err());
    for flags in [
        ProcessWaitFlags {
            untraced: true,
            ..Default::default()
        },
        ProcessWaitFlags {
            continued: true,
            ..Default::default()
        },
    ] {
        assert_eq!(
            Process::open(std::process::id(), flags)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::Unsupported
        );
    }
    let process = Process::open(
        std::process::id(),
        ProcessWaitFlags {
            nohang: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(process.wait(true).unwrap(), None);
    assert_eq!(process.wait(false).unwrap(), None);
}

static STARTED: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<ProcessId, std::sync::mpsc::Sender<()>>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));
pub(crate) fn starting(pid: ProcessId) -> std::sync::mpsc::Receiver<()> {
    let (sender, receiver) = std::sync::mpsc::channel();
    STARTED.lock().unwrap().insert(pid, sender);
    receiver
}
pub(crate) fn before_wait(pid: ProcessId, nonblocking: bool) {
    if !nonblocking {
        if let Some(sender) = STARTED.lock().unwrap().remove(&pid) {
            sender.send(()).unwrap();
        }
    }
}

#[cfg(windows)]
thread_local! {
    static WINDOWS_RESULTS: std::cell::RefCell<std::collections::VecDeque<(&'static str, u32)>> = const { std::cell::RefCell::new(std::collections::VecDeque::new()) };
}
#[cfg(windows)]
pub(super) fn windows_injected(name: &'static str) -> Option<u32> {
    WINDOWS_RESULTS.with(|state| {
        let mut state = state.borrow_mut();
        if state.front().is_some_and(|(expected, _)| *expected == name) {
            state.pop_front().map(|(_, value)| value)
        } else {
            None
        }
    })
}
#[cfg(windows)]
#[test]
fn windows_wait_and_exit_code_failures_are_reported() {
    use windows_sys::Win32::Foundation::{WAIT_FAILED, WAIT_OBJECT_0};
    let process = Process::open(std::process::id(), ProcessWaitFlags::default()).unwrap();
    WINDOWS_RESULTS
        .with(|state| *state.borrow_mut() = [("wait", WAIT_FAILED)].into_iter().collect());
    assert!(process.wait(true).is_err());
    WINDOWS_RESULTS.with(|state| {
        *state.borrow_mut() = [("wait", WAIT_OBJECT_0), ("exit-code", 0)]
            .into_iter()
            .collect()
    });
    assert!(process.wait(true).is_err());
}

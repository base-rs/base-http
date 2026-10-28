// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Child-process wait assertions. The generated suite runs broad Unix selectors in an isolated subprocess so they cannot reap another test's children.

use base_scheduler::{
    Cancellation, OperationError, ProcessWait, ProcessWaitFlags, ProcessWaitResult,
};
use futures_lite::future::poll_once;
use std::{
    fmt::Debug,
    io::Read,
    process::{Child, Command, Stdio},
};

struct ChildProcess {
    child: Child,
    reaped: bool,
}
impl ChildProcess {
    fn command(code: u32) -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "process_child_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env("CORE_SCHEDULER_PROCESS_EXIT", code.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::null());
        command
    }
    fn spawn(code: u32) -> Self {
        Self {
            child: Self::command(code).spawn().unwrap(),
            reaped: false,
        }
    }
    fn id<S: ProcessWait>(&self) -> S::ProcessId
    where
        S::ProcessId: TryFrom<u32>,
        <S::ProcessId as TryFrom<u32>>::Error: Debug,
    {
        self.child.id().try_into().unwrap()
    }
    fn release(&mut self) {
        drop(self.child.stdin.take());
    }
    fn result(&mut self, result: Option<ProcessWaitResult>) -> ProcessWaitResult {
        let result = result.unwrap();
        self.reaped = true;
        assert_eq!(result.pid, self.child.id());
        result
    }
}
impl Drop for ChildProcess {
    fn drop(&mut self) {
        if !self.reaped {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// Child fixture invoked by `process_wait_tests!`; it exits only after the parent releases stdin.
#[doc(hidden)]
pub fn child() {
    let mut input = Vec::new();
    std::io::stdin().read_to_end(&mut input).unwrap();
    let code = std::env::var("CORE_SCHEDULER_PROCESS_EXIT")
        .unwrap()
        .parse()
        .unwrap();
    // Use C runtime exit so its atexit callbacks, including coverage recording on Windows, run before termination.
    // SAFETY: This isolated child fixture has finished reading stdin and intentionally terminates with the requested status.
    unsafe { libc::exit(code) };
}

/// Individual exit status and an already-available result.
pub async fn exit<S: ProcessWait>(scheduler: &S)
where
    S::ProcessId: TryFrom<u32>,
    <S::ProcessId as TryFrom<u32>>::Error: Debug,
{
    let mut child = ChildProcess::spawn(23);
    child.release();
    let result = scheduler
        .process_wait(
            child.id::<S>(),
            ProcessWaitFlags::default(),
            &Cancellation::never(),
        )
        .await
        .unwrap();
    assert_eq!(child.result(result).status.code(), Some(23));
    #[cfg(windows)]
    {
        let mut child = ChildProcess::spawn(259);
        child.release();
        let result = scheduler
            .process_wait(
                child.id::<S>(),
                ProcessWaitFlags::default(),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        assert_eq!(child.result(result).status.code(), Some(259));
    }
}

/// Windows rejects invalid process IDs and stopped/continued status requests before waiting.
#[cfg(windows)]
pub async fn validation<S: ProcessWait>(scheduler: &S)
where
    S::ProcessId: TryFrom<u32>,
    <S::ProcessId as TryFrom<u32>>::Error: Debug,
{
    let error = scheduler
        .process_wait(
            0.try_into().unwrap(),
            ProcessWaitFlags::default(),
            &Cancellation::never(),
        )
        .await
        .unwrap_err();
    assert!(error.raw_os_error().is_some());
    for flags in [
        ProcessWaitFlags {
            nohang: true,
            untraced: true,
            ..Default::default()
        },
        ProcessWaitFlags {
            nohang: true,
            continued: true,
            ..Default::default()
        },
    ] {
        let error = scheduler
            .process_wait(
                std::process::id().try_into().unwrap(),
                flags,
                &Cancellation::never(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
    }
}

/// Nonblocking waits, lazy construction, and pre-cancellation do not reap a running child.
pub async fn lazy<S: ProcessWait>(scheduler: &S)
where
    S::ProcessId: TryFrom<u32>,
    <S::ProcessId as TryFrom<u32>>::Error: Debug,
{
    let mut child = ChildProcess::spawn(17);
    let cancellation = Cancellation::new();
    drop(scheduler.process_wait(child.id::<S>(), ProcessWaitFlags::default(), &cancellation));
    assert_eq!(
        scheduler
            .process_wait(
                child.id::<S>(),
                ProcessWaitFlags {
                    nohang: true,
                    ..Default::default()
                },
                &Cancellation::never()
            )
            .await
            .unwrap(),
        None
    );
    cancellation.cancel();
    child.release();
    #[cfg(unix)]
    {
        // Observe termination without reaping, so pre-cancellation is tested even when a status is already available.
        let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::uninit();
        // SAFETY: The caller owns this live child's wait status and supplies valid output storage for this synchronous call. WNOWAIT leaves reaping to the scheduler.
        assert_eq!(
            unsafe {
                libc::waitid(
                    libc::P_PID,
                    child.child.id() as _,
                    info.as_mut_ptr(),
                    libc::WEXITED | libc::WNOWAIT,
                )
            },
            0
        );
    }
    #[cfg(windows)]
    child.child.wait().unwrap();
    let error = scheduler
        .process_wait(child.id::<S>(), ProcessWaitFlags::default(), &cancellation)
        .await
        .unwrap_err();
    assert_eq!(
        std::mem::discriminant(&error),
        std::mem::discriminant(&OperationError::from(base_scheduler::Cancelled))
    );
    let result = scheduler
        .process_wait(
            child.id::<S>(),
            ProcessWaitFlags {
                nohang: true,
                ..Default::default()
            },
            &Cancellation::never(),
        )
        .await
        .unwrap();
    assert_eq!(child.result(result).status.code(), Some(17));
}

/// Cancellation never kills the child; a started blocking wait may still collect its exit status.
pub async fn cancellation<S: ProcessWait>(scheduler: &S)
where
    S::ProcessId: TryFrom<u32>,
    <S::ProcessId as TryFrom<u32>>::Error: Debug,
{
    cancellation_with_admission(scheduler, ProcessWaitFlags::default(), |_| {
        std::future::ready(())
    })
    .await;
}

/// A controlled backend can establish the blocking-call admission boundary before cancellation, verifying that successful status wins.
pub async fn cancellation_with_admission<S: ProcessWait, F: Future<Output = ()>>(
    scheduler: &S,
    flags: ProcessWaitFlags,
    started: impl FnOnce(S::ProcessId) -> F,
) where
    S::ProcessId: TryFrom<u32>,
    <S::ProcessId as TryFrom<u32>>::Error: Debug,
{
    let mut child = ChildProcess::spawn(31);
    let started = started(child.id::<S>());
    let cancellation = Cancellation::new();
    let mut wait = Box::pin(scheduler.process_wait(child.id::<S>(), flags, &cancellation));
    assert!(poll_once(wait.as_mut()).await.is_none());
    started.await;
    cancellation.cancel();
    child.release();
    match wait.await {
        Ok(result) => {
            assert_eq!(child.result(result).status.code(), Some(31));
            return;
        }
        Err(error) => assert_eq!(
            std::mem::discriminant(&error),
            std::mem::discriminant(&OperationError::from(base_scheduler::Cancelled))
        ),
    }
    let result = scheduler
        .process_wait(
            child.id::<S>(),
            ProcessWaitFlags::default(),
            &Cancellation::never(),
        )
        .await
        .unwrap();
    assert_eq!(child.result(result).status.code(), Some(31));
}

/// Run broad-selector coverage in a fresh test process, where no other tests have children.
#[cfg(unix)]
pub fn isolated() {
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "process_group_fixture",
            "--ignored",
            "--nocapture",
        ])
        .status()
        .unwrap();
    assert!(status.success());
}

/// Unix selectors and native stopped/continued/signal status. Invoke only in an isolated test subprocess.
#[cfg(unix)]
pub async fn groups<S: ProcessWait<ProcessId = i32>>(scheduler: &S) {
    use std::os::unix::process::{CommandExt, ExitStatusExt};
    for selector in [-1, 0] {
        let mut child = ChildProcess::spawn(19);
        child.release();
        let result = scheduler
            .process_wait(
                selector,
                ProcessWaitFlags::default(),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        assert_eq!(child.result(result).status.code(), Some(19));
    }
    let mut command = ChildProcess::command(29);
    command.process_group(0);
    let mut child = ChildProcess {
        child: command.spawn().unwrap(),
        reaped: false,
    };
    child.release();
    let result = scheduler
        .process_wait(
            -(child.child.id() as i32),
            ProcessWaitFlags::default(),
            &Cancellation::never(),
        )
        .await
        .unwrap();
    assert_eq!(child.result(result).status.code(), Some(29));
    let error = scheduler
        .process_wait(-1, ProcessWaitFlags::default(), &Cancellation::never())
        .await
        .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::ECHILD));
    assert_eq!(
        scheduler
            .process_wait(
                i32::MIN,
                ProcessWaitFlags::default(),
                &Cancellation::never()
            )
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidInput
    );

    let mut child = ChildProcess::spawn(37);
    let pid = child.child.id() as i32;
    // SAFETY: This is our live, unreaped child, retained exclusively by this isolated fixture.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGSTOP) }, 0);
    let stopped = scheduler
        .process_wait(
            pid,
            ProcessWaitFlags {
                untraced: true,
                ..Default::default()
            },
            &Cancellation::never(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stopped.status.stopped_signal(), Some(libc::SIGSTOP));
    // SAFETY: The child is stopped and still owned by this fixture.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGCONT) }, 0);
    let continued = scheduler
        .process_wait(
            pid,
            ProcessWaitFlags {
                continued: true,
                ..Default::default()
            },
            &Cancellation::never(),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(continued.status.continued());
    child.release();
    let result = scheduler
        .process_wait(pid, ProcessWaitFlags::default(), &Cancellation::never())
        .await
        .unwrap();
    assert_eq!(child.result(result).status.code(), Some(37));

    let mut child = ChildProcess::spawn(0);
    child.child.kill().unwrap();
    let result = scheduler
        .process_wait(
            child.id::<S>(),
            ProcessWaitFlags::default(),
            &Cancellation::never(),
        )
        .await
        .unwrap();
    assert_eq!(child.result(result).status.signal(), Some(libc::SIGKILL));
}

/// Closure prevents new waits; accepted blocking waits may still collect a successful status before shutdown retires them.
pub async fn shutdown<S: ProcessWait>(
    scheduler: &S,
    close: impl FnOnce(),
    shutdown: impl Future<Output = ()>,
) where
    S::ProcessId: TryFrom<u32>,
    <S::ProcessId as TryFrom<u32>>::Error: Debug,
{
    let mut child = ChildProcess::spawn(41);
    let never = Cancellation::never();
    let mut wait =
        Box::pin(scheduler.process_wait(child.id::<S>(), ProcessWaitFlags::default(), &never));
    assert!(poll_once(wait.as_mut()).await.is_none());
    close();
    child.release();
    if let Ok(result) = wait.await {
        assert_eq!(child.result(result).status.code(), Some(41));
    }
    shutdown.await;
    assert!(
        scheduler
            .process_wait(child.id::<S>(), ProcessWaitFlags::default(), &never)
            .await
            .is_err()
    );
    #[cfg(unix)]
    if !child.reaped {
        let status = child.child.wait().unwrap();
        child.reaped = true;
        assert_eq!(status.code(), Some(41));
    }
    #[cfg(windows)]
    {
        // Windows process handles remain waitable after another handle collects the status.
        assert_eq!(child.child.wait().unwrap().code(), Some(41));
        child.reaped = true;
    }
}

/// Abandonment never frees backend state while a worker or kernel can use it. A started fallback can reap even after its result is abandoned.
pub async fn abandonment<S: ProcessWait>(scheduler: &S, shutdown: impl Future<Output = ()>)
where
    S::ProcessId: TryFrom<u32>,
    <S::ProcessId as TryFrom<u32>>::Error: Debug,
{
    let mut first = ChildProcess::spawn(43);
    let mut second = ChildProcess::spawn(47);
    let never = Cancellation::never();
    let mut dropped =
        Box::pin(scheduler.process_wait(first.id::<S>(), ProcessWaitFlags::default(), &never));
    assert!(poll_once(dropped.as_mut()).await.is_none());
    drop(dropped);
    let mut forgotten =
        Box::pin(scheduler.process_wait(second.id::<S>(), ProcessWaitFlags::default(), &never));
    assert!(poll_once(forgotten.as_mut()).await.is_none());
    std::mem::forget(forgotten);
    first.release();
    second.release();
    shutdown.await;
    for (child, code) in [(&mut first, 43), (&mut second, 47)] {
        // Backend access has retired. A dropped result may have forfeited status; never send a signal through the stale Child in that case.
        child.reaped = true;
        #[cfg(unix)]
        match child.child.wait() {
            Ok(status) => assert_eq!(status.code(), Some(code)),
            Err(error) => assert_eq!(error.raw_os_error(), Some(libc::ECHILD)),
        }
        #[cfg(windows)]
        assert_eq!(child.child.wait().unwrap().code(), Some(code));
    }
}

/// A suspended native notification cannot initiate a reap when polled after completed shutdown. Only use where the backend provides cancellable native process notifications.
#[cfg(unix)]
pub async fn suspended_shutdown<S: ProcessWait>(scheduler: &S, shutdown: impl Future<Output = ()>)
where
    S::ProcessId: TryFrom<u32>,
    <S::ProcessId as TryFrom<u32>>::Error: Debug,
{
    let mut child = ChildProcess::spawn(53);
    let never = Cancellation::never();
    let mut wait =
        Box::pin(scheduler.process_wait(child.id::<S>(), ProcessWaitFlags::default(), &never));
    assert!(poll_once(wait.as_mut()).await.is_none());
    shutdown.await;
    child.release();
    assert!(wait.await.is_err());
    let status = child.child.wait().unwrap();
    child.reaped = true;
    assert_eq!(status.code(), Some(53));
}

/// A fixture abandoned before submission cleans up its child without involving a scheduler.
pub fn fixture_cleanup() {
    drop(ChildProcess::spawn(0));
}

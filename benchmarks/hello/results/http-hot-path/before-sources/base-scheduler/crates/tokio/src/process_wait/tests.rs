// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#[cfg(unix)]
use super::*;
#[cfg(unix)]
use crate::process_io::tests::Injection;
#[cfg(unix)]
use futures_lite::future::poll_once;

#[cfg(unix)]
#[test]
fn interruptions_and_notification_reaping_races_use_the_pool() {
    run(async |scheduler| {
        let pid = std::process::id() as i32;
        let _injection =
            Injection::new([("waitpid", Err(io::Error::from_raw_os_error(libc::EINTR)))]);
        assert_eq!(
            scheduler
                .process_wait(
                    pid,
                    ProcessWaitFlags {
                        nohang: true,
                        ..Default::default()
                    },
                    &Cancellation::never()
                )
                .await
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ECHILD)
        );
        drop(_injection);
        for result in [Ok(0), Err(io::Error::from_raw_os_error(libc::EINTR))] {
            use std::{io::Write, os::fd::IntoRawFd, os::unix::net::UnixStream};
            let (notification, mut sender) = UnixStream::pair().unwrap();
            notification.set_nonblocking(true).unwrap();
            sender.write_all(b"ready").unwrap();
            let mut calls = vec![("waitpid", Ok(0))];
            #[cfg(target_os = "linux")]
            calls.push(("pidfd", Ok(notification.into_raw_fd())));
            #[cfg(any(target_os = "macos", target_os = "freebsd"))]
            {
                calls.push(("kqueue", Ok(notification.into_raw_fd())));
                calls.push(("kevent", Ok(0)));
            }
            calls.push(("waitpid", result));
            let _injection = Injection::new(calls);
            // A readiness event is allowed to precede reapability. Neither None nor EINTR may turn into an invalid successful result.
            assert_eq!(
                scheduler
                    .process_wait(pid, ProcessWaitFlags::default(), &Cancellation::never())
                    .await
                    .unwrap_err()
                    .raw_os_error(),
                Some(libc::ECHILD)
            );
        }
    });
}

#[cfg(unix)]
struct ChildGuard(std::process::Child, bool);
#[cfg(unix)]
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if !self.1 {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

#[cfg(unix)]
#[test]
fn cancelling_a_running_pool_fallback_does_not_kill_the_child() {
    run(async |scheduler| {
        let mut child = ChildGuard(
            std::process::Command::new("/bin/cat")
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::null())
                .spawn()
                .unwrap(),
            false,
        );
        let cancellation = Cancellation::new();
        let mut wait = Box::pin(scheduler.process_wait(
            child.0.id() as i32,
            ProcessWaitFlags {
                untraced: true,
                ..Default::default()
            },
            &cancellation,
        ));
        assert!(poll_once(wait.as_mut()).await.is_none());
        cancellation.cancel();
        drop(child.0.stdin.take());
        match wait.await {
            Ok(result) => {
                child.1 = true;
                assert_eq!(result.unwrap().status.code(), Some(0));
            }
            Err(error) => assert_eq!(
                std::mem::discriminant(&error),
                std::mem::discriminant(&base_scheduler::OperationError::from(
                    base_scheduler::Cancelled
                ))
            ),
        }
    });
}

#[cfg(unix)]
fn run(test: impl AsyncFnOnce(&TokioScheduler)) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let scheduler = TokioScheduler::new(runtime.handle().clone());
    runtime.block_on(test(&scheduler));
    runtime.block_on(scheduler.shutdown());
}

#[cfg(unix)]
thread_local! {
    pub(super) static REGISTRATION_ERROR: std::cell::Cell<Option<io::Error>> = const { std::cell::Cell::new(None) };
}
#[cfg(unix)]
#[test]
fn notification_and_reactor_registration_failures_survive() {
    run(async |scheduler| {
        let pid = std::process::id() as i32;
        #[cfg(target_os = "linux")]
        let name = "pidfd";
        #[cfg(any(target_os = "macos", target_os = "freebsd"))]
        let name = "kqueue";
        let _injection = Injection::new([
            ("waitpid", Ok(0)),
            (name, Err(io::ErrorKind::PermissionDenied.into())),
        ]);
        assert_eq!(
            scheduler
                .process_wait(pid, ProcessWaitFlags::default(), &Cancellation::never())
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        drop(_injection);
        let _injection = Injection::new([("waitpid", Ok(0))]);
        REGISTRATION_ERROR.set(Some(io::ErrorKind::OutOfMemory.into()));
        assert_eq!(
            scheduler
                .process_wait(pid, ProcessWaitFlags::default(), &Cancellation::never())
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::OutOfMemory
        );
    });
}

#[cfg(unix)]
#[test]
fn started_fallback_returns_status_after_cancellation() {
    run(async |scheduler| {
        base_scheduler_conformance::process_wait::cancellation_with_admission(
            scheduler,
            ProcessWaitFlags {
                untraced: true,
                ..Default::default()
            },
            |pid| {
                let receiver = crate::process_io::tests::starting(pid);
                async move {
                    receiver
                        .recv_timeout(std::time::Duration::from_secs(10))
                        .unwrap();
                }
            },
        )
        .await;
    });
}

#[cfg(windows)]
#[test]
fn started_windows_wait_returns_status_after_cancellation() {
    use super::*;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let scheduler = TokioScheduler::new(runtime.handle().clone());
    runtime.block_on(
        base_scheduler_conformance::process_wait::cancellation_with_admission(
            &scheduler,
            ProcessWaitFlags::default(),
            |pid| {
                let receiver = crate::process_io::tests::starting(pid);
                async move {
                    receiver
                        .recv_timeout(std::time::Duration::from_secs(10))
                        .unwrap();
                }
            },
        ),
    );
    runtime.block_on(scheduler.shutdown());
}

#[cfg(windows)]
#[test]
fn windows_pool_start_failure_is_reported() {
    use super::*;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut scheduler = TokioScheduler::new(runtime.handle().clone());
    scheduler.work.fail_start_at = Some(0);
    let error = runtime
        .block_on(scheduler.process_wait(
            std::process::id(),
            ProcessWaitFlags::default(),
            &Cancellation::never(),
        ))
        .unwrap_err();
    assert_eq!(error.to_string(), "injected thread startup failure");
    runtime.block_on(scheduler.shutdown());
}

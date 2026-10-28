// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use std::sync::atomic::Ordering;

#[test]
fn construction_errors_are_reported_before_admission() {
    fn fail() -> i32 {
        // SAFETY: This intentionally invalid descriptor sets thread-local EBADF.
        unsafe { libc::close(-1) }
    }
    assert!(Poller::new_with(fail, |_| unreachable!()).is_err());
    assert!(
        Poller::new_with(
            || {
                // SAFETY: kqueue creates a new owned descriptor.
                unsafe { libc::kqueue() }
            },
            |_| fail()
        )
        .is_err()
    );
}

#[test]
fn changes_use_receipts_and_roll_back_after_failure() {
    let poller = Poller::new().unwrap();
    let socket: socket2::Socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap().into();
    poller.failure.store(libc::ENOMEM, Ordering::Relaxed);
    // SAFETY: All successful registrations are removed before socket destruction.
    assert_eq!(
        unsafe { poller.add_with_mode(&socket, Event::all(1), PollMode::Edge) }
            .unwrap_err()
            .raw_os_error(),
        Some(libc::ENOMEM)
    );
    // A failed add has not reserved the source; it can be registered again.
    // Interrupted registration changes retry without publishing an error.
    poller.failure.store(libc::EINTR, Ordering::Relaxed);
    unsafe { poller.add_with_mode(&socket, Event::all(2), PollMode::Edge) }.unwrap();
    poller.delete(&socket).unwrap();
    poller.delete(&socket).unwrap();
    let error = poller
        .changes(&[event(1, -128, libc::EV_ADD, 0, 3)])
        .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
}

#[test]
fn waits_and_interrupts_report_os_errors_without_losing_ownership() {
    let poller = Poller::new().unwrap();
    let mut events = Events::new();
    poller.failure.store(libc::EINTR, Ordering::Relaxed);
    assert_eq!(
        poller
            .wait(&mut events, Some(Duration::ZERO))
            .unwrap_err()
            .kind(),
        io::ErrorKind::Interrupted
    );
    poller.failure.store(libc::EBADF, Ordering::Relaxed);
    assert_eq!(
        poller.notify().unwrap_err().raw_os_error(),
        Some(libc::EBADF)
    );
    poller.failure.store(libc::EINTR, Ordering::Relaxed);
    poller.notify().unwrap();
    assert_eq!(poller.wait(&mut events, None).unwrap(), 0);
    events.clear();
    assert_eq!(
        poller
            .wait(&mut events, Some(Duration::from_nanos(1)))
            .unwrap(),
        0
    );
}

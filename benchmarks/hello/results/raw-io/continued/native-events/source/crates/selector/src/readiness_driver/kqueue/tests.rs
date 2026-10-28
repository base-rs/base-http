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
    assert_eq!(poller.wait(&mut events, None).unwrap(), 1);
    assert!(events.iter().next().is_none());
    events.clear();
    assert_eq!(
        poller
            .wait(&mut events, Some(Duration::from_nanos(1)))
            .unwrap(),
        0
    );
}

#[test]
fn collected_events_survive_wait_errors_and_append_until_clear() {
    let poller = Poller::new().unwrap();
    let receiver = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let address = receiver.local_addr().unwrap();
    let socket: socket2::Socket = receiver.into();
    // SAFETY: The token is opaque in this poller test, and deletion precedes
    // destruction of the native socket.
    unsafe { poller.add_with_mode(&socket, Event::all(42), PollMode::Edge) }.unwrap();
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    sender.send_to(b"native", address).unwrap();
    let mut events = Events::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let readable = loop {
        poller
            .wait(&mut events, Some(Duration::from_millis(10)))
            .unwrap();
        if let Some(readable) = events.iter().find(|event| event.readiness.readable) {
            break readable;
        }
        assert!(std::time::Instant::now() < deadline, "read event missing");
    };
    assert_eq!(readable.readiness.key, 42);
    assert_eq!(readable.available, 6);
    assert!(!readable.read_closed);
    let collected = events.iter().count();

    poller.failure.store(libc::EINTR, Ordering::Relaxed);
    assert!(poller.wait(&mut events, Some(Duration::ZERO)).is_err());
    assert_eq!(events.iter().count(), collected);
    poller.notify().unwrap();
    assert!(poller.wait(&mut events, None).unwrap() > 0);
    assert!(events.iter().any(|event| event.readiness.readable
        && event.readiness.key == 42
        && event.available == 6));
    events.clear();
    assert!(events.iter().next().is_none());
    poller.delete(&socket).unwrap();
}

#[test]
fn receipt_errors_distinguish_closed_additions_from_failed_deletions() {
    for flags in [libc::EV_ADD, libc::EV_DELETE] {
        let change = event(1, libc::EVFILT_WRITE, flags, 0, 1);
        for error in [0, libc::ENOENT, libc::EPIPE, libc::ENOMEM] {
            let mut receipt = event(1, libc::EVFILT_WRITE, libc::EV_ERROR, 0, 1);
            receipt.data = error as isize;
            let result = check_receipt(&receipt, &change);
            let accepted = error == 0
                || error == libc::ENOENT
                || (error == libc::EPIPE && flags == libc::EV_ADD);
            assert_eq!(result.is_ok(), accepted);
            if !accepted {
                assert_eq!(result.unwrap_err().raw_os_error(), Some(error));
            }
        }
    }
}

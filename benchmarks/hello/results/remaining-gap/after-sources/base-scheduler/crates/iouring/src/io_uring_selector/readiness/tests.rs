// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::super::tests::{drain, drive, pair};
use super::*;
use crate::Interest;
use futures_lite::future::{block_on, poll_once};

fn streams(selector: &IoUringSelector) -> (NativeSocket, NativeSocket) {
    block_on(base_scheduler_conformance::socket::stream_pair(
        selector,
        SocketDomain::Ipv4,
    ))
}

fn submitted(driver: &IoUringDriver) -> Box<dyn PendingRequest> {
    match driver
        .reactor
        .as_ref()
        .unwrap()
        .incoming
        .try_recv()
        .unwrap()
    {
        Command::Submit(request) => request,
        _ => panic!("expected readiness submission"),
    }
}

#[test]
fn ready_waits_and_cancelled_waits_retire_before_returning() {
    let (selector, mut driver) = pair(1);
    let (sender, receiver) = streams(&selector);
    drive(
        &mut driver,
        selector.socket_wait(&sender, Interest::Writable, &Cancellation::never()),
    )
    .unwrap();
    let token = Cancellation::new();
    let mut waiting = Box::pin(selector.socket_wait(&receiver, Interest::Readable, &token));
    assert!(block_on(poll_once(&mut waiting)).is_none());
    driver.turn(Some(Duration::ZERO)).unwrap();
    token.cancel();
    assert!(matches!(
        drive(&mut driver, waiting),
        Err(OperationError::Cancelled(_))
    ));
    block_on(selector.socket_close(receiver)).unwrap();
    block_on(selector.socket_close(sender)).unwrap();
    drain(&selector, &mut driver);
}

#[test]
fn readiness_errors_and_unpublished_shutdown_release_the_socket() {
    for discard_request in [false, true] {
        let (selector, mut driver) = pair(1);
        let (sender, receiver) = streams(&selector);
        let never = Cancellation::never();
        let mut waiting = Box::pin(selector.socket_wait(&receiver, Interest::Readable, &never));
        assert!(block_on(poll_once(&mut waiting)).is_none());
        let request = submitted(&driver);
        if discard_request {
            drop(request);
        } else {
            request.finish(Err(io::ErrorKind::PermissionDenied.into()));
        }
        let Err(OperationError::Io(error)) = block_on(waiting) else {
            panic!("expected I/O error")
        };
        assert_eq!(
            error.kind(),
            if discard_request {
                io::ErrorKind::BrokenPipe
            } else {
                io::ErrorKind::PermissionDenied
            }
        );
        block_on(selector.socket_close(receiver)).unwrap();
        block_on(selector.socket_close(sender)).unwrap();
        drain(&selector, &mut driver);
    }
}

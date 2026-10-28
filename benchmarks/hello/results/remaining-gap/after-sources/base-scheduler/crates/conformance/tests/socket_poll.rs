// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#[path = "support/socket_model.rs"]
mod socket_model;
use base_scheduler::*;
use futures_lite::future::block_on;
use socket_model::{SocketModel, socket_factories};
use std::io;

struct IgnoresCancellation(bool);
impl SocketOperation for IgnoresCancellation {
    type Socket = ();
}

impl SocketModel for IgnoresCancellation {
    fn recv(&self, _: &(), _: &mut [u8], _: &Cancellation) -> OperationResult<usize> {
        unreachable!("pre-cancelled send must already reject this backend")
    }
    fn send(&self, _: &(), buffer: &[u8], _: &Cancellation) -> OperationResult<usize> {
        if self.0 {
            Err(io::Error::from(io::ErrorKind::PermissionDenied).into())
        } else {
            Ok(buffer.len())
        }
    }
}
socket_factories!(IgnoresCancellation);

#[test]
fn borrowed_suite_rejects_success_and_io_error_when_pre_cancelled() {
    for io_error in [false, true] {
        let result = std::panic::catch_unwind(|| {
            block_on(
                base_scheduler_conformance::socket_poll::scalar_pre_cancelled(
                    &IgnoresCancellation(io_error),
                    &(),
                    &(),
                ),
            );
        });
        assert!(
            result.is_err(),
            "non-cancellation result passed the cancellation assertion"
        );
    }
}

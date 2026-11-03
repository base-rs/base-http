// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use base_http::{InterimHandler, InterimResponses, Request, Response, Status, body::EmptyBody};
use std::{convert::Infallible, io::Write, net::TcpStream};

/// Blocks its worker in every call until released, so the server cannot observe cancellation.
struct Blocking {
    started: Latch,
    release: Latch,
}

impl Handler for Blocking {
    type Error = Infallible;
    type Body<B: ReadableBody> = EmptyBody;

    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        _: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<Response<EmptyBody>, Infallible> {
        self.started.release();
        self.release.wait_blocking();
        Ok(Response::new(Status::OK, EmptyBody))
    }
}

// With one worker, which the handler blocks, Native strands the scheduler clock's timers behind that worker, so only the join's own-thread deadline can fail it on time. This guards the temporary workaround until base-rs/base-scheduler#93 is fixed.
#[test]
fn a_server_occupying_the_only_worker_fails_its_join() {
    let (started, release) = (Latch::new(), Latch::new());
    let context = ServerContext::new()
        .handler(Blocking {
            started: started.clone(),
            release: release.clone(),
        })
        .teardown_timeout(Duration::from_millis(50));
    shared_native(1, |scheduler| {
        let failure = catch_unwind(AssertUnwindSafe(|| {
            context.run_spawned_on(scheduler, async |context| {
                let mut stream = TcpStream::connect(context.address()).unwrap();
                stream
                    .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    .unwrap();
                started.wait_blocking();
                stream
            })
        }))
        .expect_err("a stalled join fails");
        // Let the handler return, so the scheduler can shut down.
        release.release();
        let message = failure.downcast_ref::<String>().unwrap();
        assert!(
            message.contains("did not stop within 50ms"),
            "unexpected failure: {message}"
        );
    });
}

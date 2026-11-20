// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Request bodies that outlive their handler: the connection waits for them
//! before the next request, and takes its input back from them when it is
//! cancelled or its handler fails.

mod support;

use base_http::{
    InterimHandler, InterimResponses, Request, Response, Status,
    body::{BodyFrame, BufferedBody, ReadableBody},
};
use base_http_server::{Error, Handler};
use base_scheduler::{Cancellation, Clock, SocketAccept, SocketClose};
use futures_lite::future::block_on;
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{
    any::Any,
    future::{Future, poll_fn},
    io::{self, BufRead, BufReader, ErrorKind, Read, Write},
    net::TcpStream,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Poll, Waker},
    time::Duration,
};
use support::{connect, head, listener, scheduler};

type Work<'h> = Pin<Box<dyn Future<Output = ()> + Send + 'h>>;

/// When deferred work may run, released by the peer.
#[derive(Default)]
struct Gate {
    released: AtomicBool,
    finished: AtomicBool,
    waker: Mutex<Option<Waker>>,
}
impl Gate {
    fn wake(&self) {
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
    /// Keep a peer connected until the connection has finished, so its close
    /// cannot race what the test observes.
    fn wait_finished(&self) {
        while !self.finished.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.wake();
    }
}

/// Work holding request bodies beyond their handler's call.
struct Deferred<'h> {
    queued: Mutex<Vec<Work<'h>>>,
    gate: Arc<Gate>,
}
impl<'h> Deferred<'h> {
    fn push(&self, work: Work<'h>) {
        self.queued.lock().unwrap().push(work);
        self.gate.wake();
    }
    /// Run released work until the connection has finished and no work remains.
    async fn run(&self) {
        let mut running = FuturesUnordered::new();
        poll_fn(|cx| {
            *self.gate.waker.lock().unwrap() = Some(cx.waker().clone());
            if self.gate.released.load(Ordering::Acquire) {
                running.extend(self.queued.lock().unwrap().drain(..));
                while let Poll::Ready(Some(())) = running.poll_next_unpin(cx) {}
            }
            let idle = running.is_empty() && self.queued.lock().unwrap().is_empty();
            if self.gate.finished.load(Ordering::Acquire) && idle {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    /// Respond at once and read the body later.
    Respond,
    /// Read the body to its end, then respond and keep holding it.
    ReadFirst,
    /// Fail after passing the body on.
    Fail,
    /// Respond, then drop the body unread.
    Ignore,
}

/// How deferred work's reads of a request body ended.
#[derive(Debug, PartialEq)]
enum Outcome {
    Ended(Vec<u8>),
    Closed(Vec<u8>),
    Failed(String),
}

async fn read_all<B: ReadableBody>(body: &mut B) -> Outcome {
    let mut data = Vec::new();
    loop {
        match body.read(&Cancellation::never()).await {
            Ok(BodyFrame::Data { data: chunk, .. }) => data.extend_from_slice(chunk.as_ref()),
            Ok(BodyFrame::Headers { .. }) => {}
            Ok(BodyFrame::End) => return Outcome::Ended(data),
            Err(error) => {
                let closed = (&error as &dyn Any)
                    .downcast_ref::<base_http1::Error>()
                    .is_some_and(|error| matches!(error, base_http1::Error::Closed));
                return if closed {
                    Outcome::Closed(data)
                } else {
                    Outcome::Failed(error.to_string())
                };
            }
        }
    }
}

struct Defer<'q, 'h> {
    deferred: &'q Deferred<'h>,
    mode: Mode,
    reads: Arc<Mutex<Vec<Outcome>>>,
}
impl<'h> Handler<'h> for Defer<'_, 'h> {
    type Error = io::Error;
    type Body<B: ReadableBody + 'h> = BufferedBody<Vec<u8>>;
    async fn call<B: ReadableBody + 'h, I: InterimHandler>(
        &self,
        request: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<Response<BufferedBody<Vec<u8>>>, io::Error> {
        let mut body = request.body;
        if self.mode == Mode::Ignore {
            self.deferred.push(Box::pin(async move { drop(body) }));
            return Ok(Response::new(
                Status::OK,
                BufferedBody::from(b"ok".to_vec()),
            ));
        }
        let mut first = None;
        if self.mode == Mode::ReadFirst {
            first = Some(read_all(&mut body).await);
        }
        let reads = self.reads.clone();
        self.deferred.push(Box::pin(async move {
            if let Some(first) = first {
                reads.lock().unwrap().push(first);
            }
            let read = read_all(&mut body).await;
            reads.lock().unwrap().push(read);
        }));
        match self.mode {
            Mode::Fail => Err(io::Error::other("handler failed")),
            _ => Ok(Response::new(
                Status::OK,
                BufferedBody::from(b"ok".to_vec()),
            )),
        }
    }
}

/// Serve one connection with deferred work, failing if it does not finish.
fn run<T: Send + 'static>(
    mode: Mode,
    released: bool,
    peer: impl FnOnce(BufReader<TcpStream>, Cancellation, Arc<Gate>) -> T + Send + 'static,
) -> (Result<u64, Error>, T, Vec<Outcome>) {
    let scheduler = scheduler();
    let gate = Arc::new(Gate::default());
    if released {
        gate.release();
    }
    let reads = Arc::new(Mutex::new(Vec::new()));
    let (served, peer) = block_on(async {
        let (listener, address) = listener(&scheduler).await;
        let cancellation = Cancellation::new();
        let peer_cancellation = cancellation.clone();
        let peer_gate = gate.clone();
        let peer = std::thread::spawn(move || peer(connect(address), peer_cancellation, peer_gate));
        let (socket, _) = scheduler
            .socket_accept(&listener, &cancellation)
            .await
            .unwrap();
        scheduler.socket_close(listener).await.unwrap();
        let deferred = Deferred {
            queued: Mutex::new(Vec::new()),
            gate: gate.clone(),
        };
        let handler = Defer {
            deferred: &deferred,
            mode,
            reads: reads.clone(),
        };
        let serve = async {
            let served = base_http_server::serve_connection(
                &scheduler,
                socket,
                &handler,
                &Default::default(),
                &cancellation,
            )
            .await;
            gate.finished.store(true, Ordering::Release);
            gate.wake();
            served
        };
        let (served, ()) = scheduler
            .timeout(
                Duration::from_secs(10),
                futures_lite::future::zip(serve, deferred.run()),
            )
            .await
            .expect("the connection and its deferred work finish");
        (served, peer.join().unwrap())
    });
    block_on(scheduler.shutdown());
    let reads = std::mem::take(&mut *reads.lock().unwrap());
    (served, peer, reads)
}

fn response(peer: &mut BufReader<TcpStream>) -> u16 {
    let head = head(peer);
    let mut body = vec![0; head.get("content-length").unwrap().parse().unwrap()];
    peer.read_exact(&mut body).unwrap();
    head.status
}

/// Whether no input arrives within a short wait.
fn quiet(peer: &mut BufReader<TcpStream>) -> bool {
    peer.get_ref()
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    let quiet = matches!(peer.fill_buf(), Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut));
    peer.get_ref()
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    quiet
}

#[test]
fn a_body_held_after_its_handler_delays_the_next_request_until_it_ends() {
    let (served, (first, waited, second), reads) = run(
        Mode::Respond,
        false,
        |mut peer, _, gate| {
            peer.get_mut()
            .write_all(b"POST /1 HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhelloGET /2 HTTP/1.1\r\nHost: x\r\n\r\n")
            .unwrap();
            let first = response(&mut peer);
            let waited = quiet(&mut peer);
            gate.release();
            let second = response(&mut peer);
            peer.get_mut().shutdown(std::net::Shutdown::Write).unwrap();
            (first, waited, second)
        },
    );
    assert_eq!((first, second), (200, 200));
    assert!(waited, "the second request waited for the first body");
    assert_eq!(served.unwrap(), 2);
    assert_eq!(reads[0], Outcome::Ended(b"hello".to_vec()));
}

#[test]
fn a_held_body_that_has_ended_does_not_delay_the_next_request() {
    let (served, (first, second), reads) = run(Mode::ReadFirst, false, |mut peer, _, gate| {
        peer.get_mut()
            .write_all(b"POST /1 HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhelloGET /2 HTTP/1.1\r\nHost: x\r\n\r\n")
            .unwrap();
        let first = response(&mut peer);
        let second = response(&mut peer);
        peer.get_mut().shutdown(std::net::Shutdown::Write).unwrap();
        gate.release();
        (first, second)
    });
    assert_eq!((first, second), (200, 200));
    assert_eq!(served.unwrap(), 2);
    // Reads after the end need no input, which the next request then held.
    assert_eq!(reads[0], Outcome::Ended(b"hello".to_vec()));
    assert_eq!(reads[1], Outcome::Ended(Vec::new()));
}

#[test]
fn a_closing_connection_waits_for_a_held_body_before_retiring_input() {
    let (served, status, reads) = run(Mode::Respond, false, |mut peer, _, gate| {
        peer.get_mut()
            .write_all(
                b"POST / HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Length: 5\r\n\r\nhel",
            )
            .unwrap();
        let status = response(&mut peer);
        gate.release();
        // The body is still being read after the response, as a forwarded
        // upload would be.
        std::thread::sleep(Duration::from_millis(50));
        peer.get_mut().write_all(b"lo").unwrap();
        status
    });
    assert_eq!(status, 200);
    assert_eq!(served.unwrap(), 1);
    assert_eq!(reads[0], Outcome::Ended(b"hello".to_vec()));
}

#[test]
fn cancellation_takes_input_back_from_a_held_body() {
    let (served, status, reads) = run(Mode::Respond, false, |mut peer, cancellation, gate| {
        peer.get_mut()
            .write_all(b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\n\r\nhello")
            .unwrap();
        let status = response(&mut peer);
        // The body has not been read at all; shutdown must not wait for it.
        cancellation.cancel();
        gate.release();
        gate.wait_finished();
        status
    });
    assert_eq!(status, 200);
    assert!(matches!(served, Err(Error::Cancelled)), "{served:?}");
    assert_eq!(reads[0], Outcome::Closed(Vec::new()));
}

#[test]
fn cancellation_stops_a_held_body_read_in_progress() {
    let (served, status, reads) = run(Mode::Respond, true, |mut peer, cancellation, gate| {
        peer.get_mut()
            .write_all(b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\n\r\nhello")
            .unwrap();
        let status = response(&mut peer);
        // Deferred work is waiting for the rest of the body.
        std::thread::sleep(Duration::from_millis(50));
        cancellation.cancel();
        gate.wait_finished();
        status
    });
    assert_eq!(status, 200);
    assert!(matches!(served, Err(Error::Cancelled)), "{served:?}");
    assert_eq!(reads[0], Outcome::Closed(b"hello".to_vec()));
}

#[test]
fn a_failed_handler_takes_input_back_from_a_body_it_passed_on() {
    let (served, (status, closed), reads) = run(Mode::Fail, false, |mut peer, _, gate| {
        peer.get_mut()
            .write_all(b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\n\r\nhello")
            .unwrap();
        let status = response(&mut peer);
        let closed = peer.fill_buf().unwrap().is_empty();
        gate.release();
        (status, closed)
    });
    assert_eq!(status, 500);
    assert!(closed, "the connection closed after the failure");
    assert!(matches!(served, Err(Error::Handler(_))), "{served:?}");
    assert_eq!(reads[0], Outcome::Closed(Vec::new()));
}

fn assert_send<F: Future + Send>(future: F) -> F {
    future
}

/// A pass-through middleware layer.
struct Layer<H>(H);
impl<'h, H: Handler<'h>> Handler<'h> for Layer<H> {
    type Error = H::Error;
    type Body<B: ReadableBody + 'h> = H::Body<B>;
    async fn call<B: ReadableBody + 'h, I: InterimHandler>(
        &self,
        request: Request<B>,
        interim: &mut InterimResponses<I>,
        cancellation: &Cancellation,
    ) -> Result<Response<H::Body<B>>, H::Error> {
        self.0.call(request, interim, cancellation).await
    }
}

/// Awaiting a layered handler bound to one scope, with a borrowed body, directly
/// inside a `Send` future is
/// rejected by rustc as "not general enough"; `call_send` is accepted.
#[test]
fn call_send_awaits_a_scope_bound_handler_in_a_send_future() {
    // The borrowed body must outlive the handler's scope.
    let mut local = BufferedBody::from(b"borrowed".to_vec());
    let gate = Arc::new(Gate::default());
    let deferred = Deferred {
        queued: Mutex::new(Vec::new()),
        gate,
    };
    let handler = Layer(Layer(Defer {
        deferred: &deferred,
        mode: Mode::Respond,
        reads: Arc::default(),
    }));
    let status = block_on(assert_send(async {
        let mut interim = InterimResponses::new(|_: base_http::ResponseMetadata| async {
            Ok::<_, io::Error>(())
        });
        let request = Request::new(base_http::Method::POST, "/", &mut local);
        let response =
            base_http_server::call_send(&handler, request, &mut interim, &Cancellation::never())
                .await
                .unwrap();
        response.status
    }));
    assert_eq!(status, Status::OK);
    assert_eq!(deferred.queued.lock().unwrap().len(), 1);
}

#[test]
fn a_body_dropped_unread_after_its_response_closes_the_connection() {
    let (served, (status, closed), reads) = run(Mode::Ignore, true, |mut peer, _, _| {
        peer.get_mut()
            .write_all(b"POST /1 HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhelloGET /2 HTTP/1.1\r\nHost: x\r\n\r\n")
            .unwrap();
        let status = response(&mut peer);
        let closed = peer.fill_buf().unwrap().is_empty();
        (status, closed)
    });
    assert_eq!(status, 200);
    assert!(closed, "unread input is not drained for the next request");
    assert_eq!(served.unwrap(), 1);
    assert!(reads.is_empty());
}

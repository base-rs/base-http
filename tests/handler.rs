// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! The shared request interface: middleware generic over the context wraps a
//! server's handler and a client's alike.

use base_http::{
    ClientContext, Fields, Handler, HandlerContext, InterimRelay, Method, Peer, Request,
    RequestOptions, Response, ResponseMetadata, ServerContext, Status,
    body::{BodyFrame, BufferedBody, Chunk, ReadableBody},
};
use base_scheduler::Cancellation;
use futures_lite::future::{block_on, zip};
use std::{
    convert::Infallible,
    io,
    sync::{Arc, Mutex},
};

/// Echoes the request body, and its fields as response fields, for any context.
struct Echo;
impl<'h, C: HandlerContext> Handler<'h, C> for Echo {
    type Error = io::Error;
    type Body<B: ReadableBody + 'h> = B;
    async fn call<B: ReadableBody + 'h>(
        &self,
        request: Request<B>,
        _: &mut C,
        cancellation: &Cancellation,
    ) -> Result<Response<B>, io::Error> {
        if cancellation.is_cancelled() {
            return Err(io::Error::other("cancelled"));
        }
        let (metadata, body) = request.into_parts();
        let mut response = Response::new(Status::OK, body);
        response.fields = metadata.fields;
        Ok(response)
    }
}

/// Adds its name to the request on the way in and the response on the way out.
struct Tagged<H>(&'static str, H);
impl<'h, C: HandlerContext, H: Handler<'h, C>> Handler<'h, C> for Tagged<H> {
    type Error = H::Error;
    type Body<B: ReadableBody + 'h> = H::Body<B>;
    async fn call<B: ReadableBody + 'h>(
        &self,
        mut request: Request<B>,
        context: &mut C,
        cancellation: &Cancellation,
    ) -> Result<Response<H::Body<B>>, H::Error> {
        request.fields.add("x-in", self.0).unwrap();
        let mut response = self.1.call(request, context, cancellation).await?;
        response.fields.add("x-out", self.0).unwrap();
        Ok(response)
    }
}

/// Answers `/answered` itself, with a body of its own, without its delegate.
struct Answering<H>(H);
enum Either<A> {
    Delegated(A),
    Answered(BufferedBody<Vec<u8>>),
}
impl<A: ReadableBody> ReadableBody for Either<A> {
    type Data = Vec<u8>;
    type Error = io::Error;
    async fn read(&mut self, cancellation: &Cancellation) -> Result<BodyFrame<Vec<u8>>, io::Error> {
        match self {
            Self::Delegated(body) => body
                .read(cancellation)
                .await
                .map(|frame| frame.map_data(Chunk::into_vec))
                .map_err(io::Error::other),
            Self::Answered(body) => body.read(cancellation).await.map_err(io::Error::other),
        }
    }
    fn close(&mut self, error: Option<io::Error>) {
        match self {
            Self::Delegated(body) => body.close(None),
            Self::Answered(body) => body.close(None),
        }
        drop(error);
    }
}
impl<'h, C: HandlerContext, H: Handler<'h, C>> Handler<'h, C> for Answering<H> {
    type Error = H::Error;
    type Body<B: ReadableBody + 'h> = Either<H::Body<B>>;
    async fn call<B: ReadableBody + 'h>(
        &self,
        request: Request<B>,
        context: &mut C,
        cancellation: &Cancellation,
    ) -> Result<Response<Either<H::Body<B>>>, H::Error> {
        if request.path.as_deref() == Some("/answered") {
            let body = BufferedBody::from(b"answered".to_vec());
            return Ok(Response::new(Status::OK, Either::Answered(body)));
        }
        Ok(self
            .0
            .call(request, context, cancellation)
            .await?
            .map_body(Either::Delegated))
    }
}

fn read_all<B: ReadableBody>(body: &mut B) -> Vec<u8> {
    let mut data = Vec::new();
    while let BodyFrame::Data { data: chunk, .. } =
        block_on(body.read(&Cancellation::never())).unwrap()
    {
        data.extend_from_slice(chunk.as_ref());
    }
    data
}

/// Gives its delegate a cancellation of its own, already cancelled.
struct Narrowing<H>(H);
impl<'h, C: HandlerContext, H: Handler<'h, C>> Handler<'h, C> for Narrowing<H> {
    type Error = H::Error;
    type Body<B: ReadableBody + 'h> = H::Body<B>;
    async fn call<B: ReadableBody + 'h>(
        &self,
        request: Request<B>,
        context: &mut C,
        cancellation: &Cancellation,
    ) -> Result<Response<H::Body<B>>, H::Error> {
        let narrower = cancellation.child();
        narrower.cancel();
        self.0.call(request, context, &narrower).await
    }
}

/// Sends 103 Early Hints through whichever context it is given.
struct Hinting<H>(H);
impl<'h, C: HandlerContext, H: Handler<'h, C>> Handler<'h, C> for Hinting<H> {
    type Error = H::Error;
    type Body<B: ReadableBody + 'h> = H::Body<B>;
    async fn call<B: ReadableBody + 'h>(
        &self,
        request: Request<B>,
        context: &mut C,
        cancellation: &Cancellation,
    ) -> Result<Response<H::Body<B>>, H::Error> {
        context
            .interim()
            .send_interim_response(ResponseMetadata::new(Status::EARLY_HINTS), cancellation)
            .await
            .unwrap();
        self.0.call(request, context, cancellation).await
    }
}

/// A server-only middleware: it reads the connection's peer.
struct PeerTagged<H>(H);
impl<'h, H: Handler<'h, ServerContext>> Handler<'h, ServerContext> for PeerTagged<H> {
    type Error = H::Error;
    type Body<B: ReadableBody + 'h> = H::Body<B>;
    async fn call<B: ReadableBody + 'h>(
        &self,
        request: Request<B>,
        context: &mut ServerContext,
        cancellation: &Cancellation,
    ) -> Result<Response<H::Body<B>>, H::Error> {
        let peer = context.peer().clone();
        let mut response = self.0.call(request, context, cancellation).await?;
        let address = peer.address().unwrap().to_string();
        response.fields.add("x-peer", address.as_str()).unwrap();
        Ok(response)
    }
}

fn server_context() -> (ServerContext, InterimRelay) {
    let relay = InterimRelay::new();
    (ServerContext::new(Arc::default(), relay.sender()), relay)
}

fn client_context() -> ClientContext<impl base_http::InterimHandler> {
    ClientContext::new(|_: ResponseMetadata| async { Ok::<_, Infallible>(()) })
}

fn values(fields: &Fields, name: &str) -> Vec<String> {
    fields
        .values(name)
        .map(|value| String::from_utf8(value.to_vec()).unwrap())
        .collect()
}

#[test]
fn the_same_middleware_wraps_a_server_handler_and_a_client_and_applies_in_order() {
    let stack = Tagged("outer", Tagged("inner", Echo));
    let never = Cancellation::never();
    let request = || Request::new(Method::POST, "/", BufferedBody::from(b"hello".to_vec()));
    let (mut server, _relay) = server_context();
    let served = block_on(stack.call(request(), &mut server, &never)).unwrap();
    let mut client = client_context();
    let called = block_on(stack.call(request(), &mut client, &never)).unwrap();
    for response in [served, called] {
        // Requests pass the outer layer first; responses leave the inner first.
        assert_eq!(values(&response.fields, "x-in"), ["outer", "inner"]);
        assert_eq!(values(&response.fields, "x-out"), ["inner", "outer"]);
    }
}

#[test]
fn a_layer_answers_without_calling_its_delegate() {
    let stack = Answering(Tagged("delegate", Echo));
    let (mut context, _relay) = server_context();
    let never = Cancellation::never();
    let mut answered = block_on(stack.call(
        Request::new(
            Method::GET,
            "/answered",
            BufferedBody::from(b"unread".to_vec()),
        ),
        &mut context,
        &never,
    ))
    .unwrap();
    assert!(values(&answered.fields, "x-out").is_empty());
    assert_eq!(read_all(&mut answered.body), b"answered");
    let mut delegated = block_on(stack.call(
        Request::new(Method::POST, "/", BufferedBody::from(b"echoed".to_vec())),
        &mut context,
        &never,
    ))
    .unwrap();
    assert_eq!(values(&delegated.fields, "x-out"), ["delegate"]);
    assert_eq!(read_all(&mut delegated.body), b"echoed");
    answered.body.close(None);
    delegated.body.close(None);
}

#[test]
fn a_layer_gives_its_delegate_a_narrower_cancellation() {
    let stack = Narrowing(Echo);
    let mut context = client_context();
    let outer = Cancellation::new();
    let refused = block_on(stack.call(
        Request::new(Method::GET, "/", BufferedBody::<Vec<u8>>::default()),
        &mut context,
        &outer,
    ));
    assert!(refused.is_err());
    assert!(!outer.is_cancelled());
}

#[test]
fn a_borrowed_request_body_passes_through_layers_without_copying() {
    let stack = Tagged("outer", Tagged("inner", Echo));
    let mut local = BufferedBody::from(b"borrowed".to_vec());
    let address = &local as *const BufferedBody;
    let mut context = client_context();
    let response = block_on(stack.call(
        Request::new(Method::POST, "/", &mut local),
        &mut context,
        &Cancellation::never(),
    ))
    .unwrap();
    assert!(std::ptr::eq(&*response.body, address));
}

#[test]
fn generic_middleware_sends_interim_responses_through_either_context() {
    let stack = Hinting(Echo);
    let never = Cancellation::never();
    let request = || Request::new(Method::GET, "/", BufferedBody::<Vec<u8>>::default());

    let observed = Arc::new(Mutex::new(Vec::new()));
    let seen = observed.clone();
    let mut client = ClientContext::new(move |response: ResponseMetadata| {
        let seen = seen.clone();
        async move {
            seen.lock().unwrap().push(response.status);
            Ok::<_, Infallible>(())
        }
    });
    block_on(stack.call(request(), &mut client, &never)).unwrap();
    assert_eq!(*observed.lock().unwrap(), [Status::EARLY_HINTS]);
    assert_eq!(client.options, RequestOptions::default());

    let (mut server, relay) = server_context();
    let (response, written) = block_on(zip(stack.call(request(), &mut server, &never), async {
        let written = relay.next().await;
        relay.complete(Ok::<_, Infallible>(()));
        written
    }));
    response.unwrap();
    assert_eq!(written.status, Status::EARLY_HINTS);
}

#[test]
fn a_server_middleware_reads_the_peer() {
    let peer = Peer::new(
        Some("192.0.2.1:4433".parse().unwrap()),
        Some("198.51.100.2:443".parse().unwrap()),
    )
    .encrypted(Some(b"http/1.1"));
    assert_eq!(
        peer.local_address(),
        Some("198.51.100.2:443".parse().unwrap())
    );
    assert!(peer.is_encrypted());
    assert_eq!(peer.application_protocol(), Some(&b"http/1.1"[..]));
    assert!(!peer.is_early_data());
    let relay = InterimRelay::new();
    let mut context = ServerContext::new(Arc::new(peer), relay.sender());
    let response = block_on(PeerTagged(Echo).call(
        Request::new(Method::GET, "/", BufferedBody::<Vec<u8>>::default()),
        &mut context,
        &Cancellation::never(),
    ))
    .unwrap();
    assert_eq!(values(&response.fields, "x-peer"), ["192.0.2.1:4433"]);
    let plain = Peer::new(None, None);
    assert!(!plain.is_encrypted());
    assert_eq!(plain.application_protocol(), None);
    assert_eq!(plain, Peer::default());
}

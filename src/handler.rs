// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! One request interface for servers, clients and the middleware between them.
//!
//! A [`Handler`] takes a request, a mutable context and cancellation, and
//! produces a response, as `protocol-http`'s `call(request)` does. A server
//! calls its application with a [`ServerContext`]; a client is called with a
//! [`ClientContext`]. A middleware is a handler that holds another and calls
//! through to it. Middleware compose by wrapping, with static dispatch: a stack
//! is a nested type, and no layer boxes, requires `'static` or spawns a task.
//!
//! ```rust
//! use base_http::{
//!     Handler, HandlerContext, Request, Response, Status,
//!     body::{BufferedBody, ReadableBody},
//! };
//! use base_scheduler::Cancellation;
//! use std::convert::Infallible;
//!
//! /// Adds a field to every response, for a server or a client alike.
//! struct Tagged<H>(H);
//! impl<'h, C: HandlerContext, H: Handler<'h, C>> Handler<'h, C> for Tagged<H> {
//!     type Error = H::Error;
//!     type Body<B: ReadableBody + 'h> = H::Body<B>;
//!     async fn call<B: ReadableBody + 'h>(
//!         &self,
//!         request: Request<B>,
//!         context: &mut C,
//!         cancellation: &Cancellation,
//!     ) -> Result<Response<H::Body<B>>, H::Error> {
//!         let mut response = self.0.call(request, context, cancellation).await?;
//!         response.fields.add("x-tagged", "yes").unwrap();
//!         Ok(response)
//!     }
//! }
//! ```
use crate::{
    InterimHandler, InterimResponses, Peer, Request, Response, body::ReadableBody,
    interim::InterimSender,
};
use base_scheduler::Cancellation;
use std::{error::Error, future::Future, sync::Arc};

/// Produce a response for a request, given a context of type `C`.
///
/// Bodies and contexts stay generic, so a layer passes a body through without
/// copying, and a returned body may retain the request body. A request body
/// lives for `'scope`, such as a server's borrow of its scheduler, so a handler
/// can pass it to work that outlives the call, such as a client forwarding it
/// upstream.
///
/// A middleware written for any context implements `Handler<'h, C>` for every
/// `C: HandlerContext`, so it wraps a server application or a client alike. One
/// that needs a side's own data, such as the [`Peer`], is written for that
/// side's context, so placing it on the other side does not compile.
///
/// A middleware for one side cannot be placed on the other:
///
/// ```rust,compile_fail,E0277
/// use base_http::{
///     ClientContext, Handler, InterimHandler, Request, Response, ServerContext,
///     body::ReadableBody,
/// };
/// use base_scheduler::Cancellation;
///
/// /// Reads the server's peer, so it is written for `ServerContext` only.
/// struct PeerLogged<H>(H);
/// impl<'h, H: Handler<'h, ServerContext>> Handler<'h, ServerContext> for PeerLogged<H> {
///     type Error = H::Error;
///     type Body<B: ReadableBody + 'h> = H::Body<B>;
///     async fn call<B: ReadableBody + 'h>(
///         &self,
///         request: Request<B>,
///         context: &mut ServerContext,
///         cancellation: &Cancellation,
///     ) -> Result<Response<H::Body<B>>, H::Error> {
///         println!("{:?}", context.peer().address());
///         self.0.call(request, context, cancellation).await
///     }
/// }
///
/// fn client_side<'h, I: InterimHandler, H: Handler<'h, ClientContext<I>>>(_: &H) {}
///
/// fn place<'h, I: InterimHandler, H: Handler<'h, ClientContext<I>>>(client: H) {
///     client_side::<I, _>(&PeerLogged(client));
/// }
/// ```
///
/// Generic code awaiting a handler bound to one `'scope` inside its own `Send`
/// future may be rejected by rustc with "implementation of `Handler` is not
/// general enough"; awaiting [`call_send`] instead avoids it.
pub trait Handler<'scope, C: HandlerContext>: Sync {
    type Error: Error + Send + Sync + 'static;
    type Body<B: ReadableBody + 'scope>: ReadableBody;

    fn call<B: ReadableBody + 'scope>(
        &self,
        request: Request<B>,
        context: &mut C,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<Response<Self::Body<B>>, Self::Error>> + Send;
}

/// What every context gives a handler: the exchange's interim-response hook.
/// A server's hook sends informational responses to the peer, such as 103
/// Early Hints; a client's observes those the upstream sends.
pub trait HandlerContext: Send {
    type Interim: InterimHandler;
    fn interim(&mut self) -> &mut InterimResponses<Self::Interim>;
}

/// Call `handler` through a future whose type states `Send`, for generic code
/// that awaits a handler bound to one `'scope` within its own `Send` future,
/// which rustc may otherwise reject as "not general enough".
pub fn call_send<'scope, C, H, B>(
    handler: &H,
    request: Request<B>,
    context: &mut C,
    cancellation: &Cancellation,
) -> impl Future<Output = Result<Response<H::Body<B>>, H::Error>> + Send
where
    C: HandlerContext,
    H: Handler<'scope, C>,
    B: ReadableBody + 'scope,
{
    handler.call(request, context, cancellation)
}

/// The context a server passes to its handler for one exchange: the
/// connection's [`Peer`] and the hook for sending interim responses, such as
/// 103 Early Hints, which the server writes.
///
/// It borrows nothing from the server, so a handler's response cannot depend
/// on it, and handlers need not name each protocol engine's writer.
pub struct ServerContext {
    peer: Arc<Peer>,
    interim: InterimResponses<InterimSender>,
}

impl ServerContext {
    pub fn new(peer: Arc<Peer>, interim: InterimSender) -> Self {
        Self {
            peer,
            interim: InterimResponses::new(interim),
        }
    }

    /// The connection the request arrived on.
    pub fn peer(&self) -> &Arc<Peer> {
        &self.peer
    }
}

impl HandlerContext for ServerContext {
    type Interim = InterimSender;
    fn interim(&mut self) -> &mut InterimResponses<InterimSender> {
        &mut self.interim
    }
}

/// The context an application passes when calling a client: the observer of
/// interim responses the upstream sends, and options for this request.
pub struct ClientContext<I> {
    interim: InterimResponses<I>,
    pub options: RequestOptions,
}

impl<I: InterimHandler> ClientContext<I> {
    pub fn new(observer: I) -> Self {
        Self {
            interim: InterimResponses::new(observer),
            options: RequestOptions::default(),
        }
    }
}

impl<I: InterimHandler> HandlerContext for ClientContext<I> {
    type Interim = I;
    fn interim(&mut self) -> &mut InterimResponses<I> {
        &mut self.interim
    }
}

/// Per-request client options. None exist yet; options such as whether a
/// request may be sent as early data arrive with the transports that need them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RequestOptions {}

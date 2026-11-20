# Middleware

Accepted 2026-11-20; not yet implemented. Decisions are recorded in their sections; the [open questions](#open-questions) remain undecided.

## Shared request interface

### Requirement

`base-http` defines one request interface, generic over a context type: a call that takes a request, a mutable reference to the context and cancellation, and produces a response. Each side has its own context, carrying exactly the per-exchange data that side has:

- `ServerContext`, which a server passes to its handler, carries the downstream [`Peer`](load-balancer.md#connection-information-and-the-proxy-protocol) and the interim-response writer the handler uses to send informational responses.
- `ClientContext`, which an application passes when calling a client, carries the interim-response observer the client calls as informational responses arrive, and per-request options, such as whether the request may be sent in early data, whether the automatic resend after 425 applies, and retry overrides.

In both, the caller supplies the interim hook and the callee calls it, so a proxy relays upstream informational responses, such as 100 Continue, by giving the upstream client's context the downstream writer as its observer. Cancellation stays a separate argument, as every Base operation takes it. Server handlers implement the interface for `ServerContext`, and `base-http-client` implements it for `ClientContext`. The client's current `call` requires more of a request body than the interface's `B: ReadableBody` allows, a lifetime outliving the client's driver scope and data convertible into `Vec<u8>`, and an implementation cannot add bounds the trait method does not declare. How the client meets the interface, for example through its direct writer path, which forwards borrowed bodies frame by frame as the [duplex design](duplex.md#direct-writable-client-experiment) describes, or by relaxing those bounds, is settled before the client implements it, and is an [open question](#open-questions) until then; the client keeps its existing `call` meanwhile. Per-exchange data added later goes into the relevant context, without changing the interface's signature.

A middleware is an implementation of the interface that holds a delegate implementing it, for the same context, and calls through to it, transforming the request on the way in and the response on the way out, or answering without calling its delegate. Middleware compose by wrapping, with static dispatch: a stack is a nested type, and no layer requires boxing, a `'static` bound or a task. Request and response bodies stay generic, so a layer passes bodies through without copying, and a returned body may borrow the request body, as the server's handler contract allows now. A middleware that only transforms requests and responses is generic over the context type, so the same layer wraps a server application or a client. A middleware that needs one side's data, such as the `Peer` or the client's request options, is written for that side's context, so placing it on the other side is a compile-time error. A small trait implemented by both contexts gives generic middleware access to the interim hook where it needs one.

The interface lives in `base-http`, which depends on no protocol crate, client, server or runtime, so middleware depending only on `base-http` works with any of them, as the [proposal](proposal.md#3-crate-structure) intends.

The interface is the current `base_http_server::Handler`, moved into `base-http` and made generic over the context, as `Handler<C>`, keeping its associated `Error` and `Body<B>` types, with the signature `call(request, &mut C, &Cancellation)`; `base-http-server` re-exports it. The interim-hook type, a generic parameter of `call` now, becomes a parameter of each context type. A middleware forwards the context to its delegate, changing only what it handles, and can give its delegate a narrower cancellation. Decided on 2026-11-20, after first considering one shared context with optional fields for each side.

### Rationale

Async::HTTP composes applications and clients through protocol-http's [`Middleware`](https://github.com/socketry/protocol-http/blob/6b9b3d84f4e6261068fac7b52e567d70eb3909e5/lib/protocol/http/middleware.rb#L23-L54), which holds a `delegate` and forwards `call(request)`. Because server applications and `Async::HTTP::Client` both answer `call(request)`, one middleware can sit in front of either; Falcon's [proxy](https://github.com/socketry/falcon/blob/5107b0713fd78dbe8b08ee123391b1f4bcffdf12/lib/falcon/middleware/proxy.rb#L32-L79) is a middleware whose delegates, per host, are clients. Base has no such abstraction: its only request-to-response trait is the server's `Handler`, and the client's `call` is an inherent method with a different signature. The user decided on 2026-11-20 to follow the protocol-http model, so that the load balancer's request and response transformations are middleware layers around each upstream group's client. Separate context types, decided the same day, serve the user's goals of runtime efficiency followed by simplicity: each side carries only the data it has, with no optional fields to check or ignore, dispatch stays static, and the context's type states which side a layer is for, while middleware generic over the context keeps the reuse across sides that protocol-http's model provides.

### Scope

The request interface in `base-http`, its implementations in `base-http-server` and `base-http-client`, and middleware built on it, including the load balancer's layers. When the interface is implemented, the unrelated `ServerContext` test type in [`base-http-fixtures`](fixtures.md) is renamed, for example to `ServerFixture`, so the two are not confused.

### Validation

Inspect `cargo tree -e normal` of `base-http` for protocol, client, server and runtime dependencies. Tests: one middleware generic over the context wraps a server handler and, unchanged, a client, and observes the same requests and responses; a middleware written for `ServerContext` reads the `Peer`, and one written for `ClientContext` sets a request option the client then honours; a compile-fail test places a `ServerContext` middleware around a client; a proxy relays an upstream 100 Continue downstream through the contexts' interim hooks; a middleware forwards the context and gives its delegate a narrower cancellation; a stack of two layers applies them in order; a layer answers without calling its delegate; a layer passes a borrowed request body through to a returned response without copying. Inspect the interface and a stacked type for boxing and `'static` bounds.

## Load balancer layers

### Requirement

The load balancer's request and response transformations are middleware wrapping each upstream group's client, written for `ClientContext` where they need its request options, such as turning off the automatic 425 resend for forwarded requests. A layer knows the group's configured HTTP version, so it can adapt a request to that version and adapt the response back. Each layer is self-contained, with its own dependencies, so the proxy core and groups that do not use a layer do not depend on it.

The first layer translates WebSocket handshakes across versions, in `base-load-balancer`: forwarding an extended CONNECT to an HTTP/1 upstream, it generates `Sec-WebSocket-Key` and verifies the upstream's `Sec-WebSocket-Accept`, not forwarding the accept downstream; forwarding an HTTP/1 upgrade to an HTTP/2 or HTTP/3 upstream, it computes `Sec-WebSocket-Accept` from the client's key for the downstream switch and does not forward the key ([RFC 6455 §4](https://www.rfc-editor.org/rfc/rfc6455.html#section-4), [RFC 8441 §5](https://www.rfc-editor.org/rfc/rfc8441.html#section-5)). Without the layer, the [proxy](load-balancer.md#proxy-forwarding) forwards a protocol switch only when both sides use the same mechanism, and otherwise responds 502.

### Rationale

The user decided on 2026-11-20 that translation should not live in the proxy core, but in a layered, self-contained mechanism of the load balancer. Wrapping the group's client places the layer after the policy object has chosen the group, where the upstream's version is known, and keeps SHA-1, base64 and randomness out of `base-http-proxy`.

### Scope

Layers in `base-load-balancer`, and the proxy's behavior without them.

### Validation

With the layer, WebSocket echo passes through the load balancer for every pair of versions against independent WebSocket servers, an upstream sending a wrong `Sec-WebSocket-Accept` is rejected, and an upstream declining the upgrade is answered with 502. Without it, a same-mechanism WebSocket is forwarded and a cross-version one receives 502. Inspect `cargo tree -e normal` of `base-http-proxy` for the layer's dependencies.

## Open questions

- How `base-http-client` implements the interface given its body bounds, which also decides whether a layer around a client passes a borrowed request body through without copying.
- `ClientContext`'s exact request options, and how the client reports its errors through the interface's associated error type.
- An adapter for middleware stacks composed at runtime from configuration, which static nesting cannot express, such as an enum or boxed layer.
- Whether optional adapters to other middleware interfaces, such as Tower's `Service`, are provided.

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Endpoint-scoped clients with explicit, supervised protocol progress.
#![doc = include_str!("../readme.md")]
#![forbid(unsafe_code)]

mod body;
mod connection;
mod driver;
mod error;
mod outgoing;
mod reuse;
mod sink;
mod writable;
pub use writable::{PendingResponse, RequestBodyWriter, StreamingResponseBody, WritableRequest};

pub use body::ResponseBody;
pub use connection::Connection;
pub use driver::Driver;
pub use error::Error;

use base_http::{Authority, InterimHandler, Method, Request, Response, Scheme, body::ReadableBody};
use base_pool::MultiplexPool;
use base_scheduler::{
    Cancellation, SocketClose, SocketConnect, SocketOpen, SocketSetOption, SocketStreamReading,
    SocketStreamWriting, TcpNoDelay,
};
use driver::{CancelOnDrop, Operation, Pending, Queue, checked};
use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
};

/// A resolved HTTP endpoint. Its HTTP policy is independent of TLS identity
/// and the address used to establish the underlying transport.
#[derive(Clone, Debug)]
pub struct Endpoint {
    pub address: SocketAddr,
    pub authority: Authority,
    #[cfg(feature = "rustls")]
    pub(crate) tls: Option<Tls>,
}
#[cfg(feature = "rustls")]
#[derive(Clone, Debug)]
pub(crate) struct Tls {
    config: Arc<rustls::ClientConfig>,
    server_name: String,
}
impl Endpoint {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            authority: address.to_string().into(),
            #[cfg(feature = "rustls")]
            tls: None,
        }
    }
    /// Enable HTTPS with explicit certificate trust and server identity. The
    /// HTTP implementation supplies the ALPN offers when connecting.
    #[cfg(feature = "rustls")]
    pub fn with_tls(
        mut self,
        config: Arc<rustls::ClientConfig>,
        server_name: impl Into<String>,
    ) -> Self {
        self.tls = Some(Tls {
            config,
            server_name: server_name.into(),
        });
        self
    }
    pub fn scheme(&self) -> &'static str {
        #[cfg(feature = "rustls")]
        if self.tls.is_some() {
            return "https";
        }
        "http"
    }
    // The scheme constant, so requests need not convert the text.
    pub(crate) fn request_scheme(&self) -> Scheme {
        #[cfg(feature = "rustls")]
        if self.tls.is_some() {
            return Scheme::HTTPS;
        }
        Scheme::HTTP
    }
}

#[derive(Clone, Debug)]
pub struct Options {
    pub pool: base_pool::Options,
    pub http1: base_http1::Options,
    /// HTTP engine selection, shared by plain and encrypted establishment.
    pub protocols: base_http_protocol::HttpProtocols,
    /// Bound accepted calls, including queued, uploading and unread responses.
    pub max_requests: usize,
    pub response_buffer_bytes: usize,
    pub response_buffer_frames: usize,
    /// Disable Nagle's algorithm on each TCP connection, so small writes such
    /// as streamed frames are not held back waiting for acknowledgement. This
    /// is a best-effort latency hint: a socket that rejects it remains usable.
    pub tcp_nodelay: bool,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            pool: base_pool::Options::default(),
            http1: base_http1::Options::default(),
            protocols: base_http_protocol::HttpProtocols::default(),
            max_requests: 1024,
            response_buffer_bytes: 64 * 1024,
            response_buffer_frames: 16,
            tcp_nodelay: true,
        }
    }
}
impl Options {
    pub fn validate(&self) -> Result<(), Error> {
        self.pool.validate()?;
        self.http1.validate()?;
        if self.max_requests == 0
            || self.response_buffer_frames == 0
            || self.response_buffer_bytes < self.http1.read_size.max(self.http1.max_head_bytes)
        {
            return Err(Error::InvalidOptions);
        }
        Ok(())
    }
}

/// Durable remote-server interface. Clones share connection admission and
/// supervision. `call` submits pull bodies; `open` lends direct HTTP/1 directions.
/// Neither operation chooses a process-global scheduler.
pub struct Client<'scope> {
    endpoint: Endpoint,
    queue: Arc<Queue<'scope>>,
    pool: MultiplexPool<Connection<'scope>>,
    context: Arc<driver::Context<'scope>>,
}
impl<'scope> Client<'scope> {
    /// Construct without dialing. Poll the returned driver concurrently with
    /// calls. Bodies may borrow for the driver's scope; spawning requires owned
    /// `Send + 'static` captures, as with any scheduler task.
    pub fn new<S>(
        scheduler: &'scope S,
        endpoint: Endpoint,
        options: Options,
    ) -> Result<(Self, Driver<'scope>), Error>
    where
        S: SocketOpen
            + SocketConnect
            + SocketClose
            + SocketSetOption<TcpNoDelay>
            + SocketStreamReading
            + SocketStreamWriting
            + 'scope,
        S::Socket: 'static,
    {
        options.validate()?;
        let driver::ClientParts {
            queue,
            pool,
            driver,
            context,
        } = driver::make(scheduler, endpoint.clone(), options);
        Ok((
            Self {
                endpoint,
                queue,
                pool,
                context,
            },
            driver,
        ))
    }
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
    pub fn pool_stats(&self) -> base_pool::Stats {
        self.pool.stats()
    }
    /// Reject new submissions, finish accepted calls, then retire connections.
    /// Cancel the driver's lifetime signal to force active calls down.
    pub fn begin_close(&self) {
        self.queue.begin_close();
    }

    /// Return final response metadata promptly. The driver continues upload
    /// independently of response consumption. No automatic retries occur.
    pub async fn call<B>(
        &self,
        request: Request<B>,
        cancellation: &Cancellation,
    ) -> Result<Response<ResponseBody>, Error>
    where
        B: ReadableBody + 'scope,
        B::Data: Into<Vec<u8>>,
    {
        self.call_with_interim(
            request,
            |_: base_http::ResponseMetadata| async { Ok::<_, Infallible>(()) },
            cancellation,
        )
        .await
    }
    /// Observe informational responses in order, without accumulating a log.
    pub async fn call_with_interim<B, I>(
        &self,
        mut request: Request<B>,
        interim: I,
        cancellation: &Cancellation,
    ) -> Result<Response<ResponseBody>, Error>
    where
        B: ReadableBody + 'scope,
        B::Data: Into<Vec<u8>>,
        I: InterimHandler + 'scope,
    {
        if cancellation.is_cancelled() {
            request.body.close(None);
            return Err(Error::Cancelled);
        }
        if request.method == Method::CONNECT
            || request.protocol.is_some()
            || request.fields.contains("upgrade")
        {
            request.body.close(None);
            return Err(Error::Unsupported("managed client tunnels and upgrades"));
        }
        if request
            .scheme
            .as_deref()
            .is_some_and(|scheme| scheme != self.endpoint.scheme())
        {
            request.body.close(None);
            return Err(Error::Unsupported(
                "request scheme does not match the endpoint",
            ));
        }
        request
            .scheme
            .get_or_insert_with(|| self.endpoint.request_scheme());
        request
            .authority
            .get_or_insert_with(|| self.endpoint.authority.clone());
        let local = Cancellation::new();
        let mut guard = CancelOnDrop(local.clone(), true);
        let (reply, response) = futures_channel::oneshot::channel();
        let pending = match Pending::new(self.queue.clone()) {
            Ok(pending) => pending,
            Err(error) => {
                request.body.close(None);
                return Err(error);
            }
        };
        let operation = Operation {
            request: request.map_body(|body| outgoing::OutgoingBody::new(body, local.clone())),
            interim: Box::new(interim),
            reply: Some(reply),
            cancellation: local,
            lifetime: cancellation.clone(),
            _pending: Some(pending),
        };
        // A queue closed since admission drops the operation and its reply sender, so the response reports `Closed`, as for calls queued when the driver stops.
        let _ = self.queue.push(operation);
        let response = checked(
            async { response.await.map_err(|_| Error::Closed)? },
            cancellation,
        )
        .await?;
        guard.1 = false;
        Ok(response)
    }
}
impl Clone for Client<'_> {
    fn clone(&self) -> Self {
        self.queue.handles.fetch_add(1, Ordering::Relaxed);
        Self {
            endpoint: self.endpoint.clone(),
            queue: self.queue.clone(),
            pool: self.pool.clone(),
            context: self.context.clone(),
        }
    }
}
impl Drop for Client<'_> {
    fn drop(&mut self) {
        if self.queue.handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.queue.begin_close();
        }
    }
}

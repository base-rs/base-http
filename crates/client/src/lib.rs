// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Endpoint-scoped clients with explicit, supervised protocol progress.
#![doc = include_str!("../readme.md")]
#![forbid(unsafe_code)]

mod call;
mod connection;
mod driver;
mod error;
mod observer;
mod reuse;
mod sink;
mod writable;
pub use writable::{PendingResponse, RequestBodyWriter, StreamingResponseBody, WritableRequest};

use connection::Connection;
pub use driver::Driver;
pub use error::Error;

use base_http::{Authority, Scheme};
use base_pool::MultiplexPool;
use base_scheduler::{
    Placement, SocketClose, SocketConnect, SocketOpen, SocketSetOption, SocketStreamReading,
    SocketStreamWriting, TcpNoDelay,
};
use driver::Queue;
use std::{
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
    /// Bound accepted requests, including those still uploading and unread
    /// responses.
    pub max_requests: usize,
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
            tcp_nodelay: true,
        }
    }
}
impl Options {
    pub fn validate(&self) -> Result<(), Error> {
        self.pool.validate()?;
        self.http1.validate()?;
        if self.max_requests == 0 {
            return Err(Error::InvalidOptions);
        }
        Ok(())
    }
}

/// Durable remote-server interface. Clones share connection admission and
/// supervision. `call` sends a request with a body to pull from; `open` lends
/// direct HTTP/1 directions for writing one imperatively. Both run in the
/// calling task.
/// Neither operation chooses a process-global scheduler.
///
/// `'scope` is the scheduler borrow, and it also bounds the request bodies and interim observers that calls queue for the driver. The client and its driver share that queue, so neither can outlive the scheduler. The type is invariant over `'scope`: shortening it would let a call queue a body that borrows shorter-lived data for a driver that keeps running after the data is gone.
///
/// To keep a client for longer, give the scheduler a longer life. Create it at the application root and store the client in a struct generic over `'scope`, or use a scheduler that lives for `'static`, whose `Client<'static>` can be stored anywhere.
pub struct Client<'scope> {
    endpoint: Endpoint,
    queue: Arc<Queue<'scope>>,
    pool: MultiplexPool<Connection<'scope>>,
    context: Arc<driver::Context<'scope>>,
}
impl<'scope> Client<'scope> {
    /// Construct without dialing. Poll the returned driver concurrently with
    /// calls. Bodies may borrow for the driver's scope.
    ///
    /// `placement` decides where each connection's driver runs: the scheduler,
    /// `&scheduler`, supervises them within the returned [`Driver`]; a scope
    /// of the scheduler's scoped spawn runs each as its own task of that scope,
    /// so connections progress on more than one worker. Calls on established
    /// connections run in their calling tasks either way, and the driver still
    /// owns admission, cancellation and the joined retirement of every
    /// connection.
    pub fn new<P, S>(
        placement: P,
        endpoint: Endpoint,
        options: Options,
    ) -> Result<(Self, Driver<'scope>), Error>
    where
        P: Placement<'scope, Scheduler = S> + 'scope,
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
        } = driver::make(placement, endpoint.clone(), options);
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

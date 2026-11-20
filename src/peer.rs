// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! What a server knows about the connection a request arrived on.
//!
//! A [`Peer`] reaches a handler through its [`ServerContext`](crate::ServerContext),
//! not through the request, so forwarding a request cannot carry it upstream.
//! It is named after `protocol-http`'s `Peer`.
use std::net::SocketAddr;

/// Connection information for the requests a server receives on one connection.
///
/// Fields are private so later details, such as those a PROXY protocol header
/// or QUIC supplies, can be added without breaking callers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Peer {
    address: Option<SocketAddr>,
    local_address: Option<SocketAddr>,
    encrypted: bool,
    application_protocol: Option<Box<[u8]>>,
}

impl Peer {
    /// A peer at `address`, received on `local_address`. Either is `None` when
    /// the transport has no IP address, such as a Unix socket, or reported none.
    pub fn new(address: Option<SocketAddr>, local_address: Option<SocketAddr>) -> Self {
        Self {
            address,
            local_address,
            ..Self::default()
        }
    }

    /// The same peer over an encrypted transport, which negotiated
    /// `application_protocol`, such as `http/1.1`, when the peer offered one.
    pub fn encrypted(mut self, application_protocol: Option<&[u8]>) -> Self {
        self.encrypted = true;
        self.application_protocol = application_protocol.map(Box::from);
        self
    }

    /// The remote address of the connection.
    pub fn address(&self) -> Option<SocketAddr> {
        self.address
    }

    /// The local address the connection was received on.
    pub fn local_address(&self) -> Option<SocketAddr> {
        self.local_address
    }

    /// Whether the connection is encrypted, such as by TLS.
    pub fn is_encrypted(&self) -> bool {
        self.encrypted
    }

    /// The application protocol an encrypted connection negotiated, such as
    /// `h2` or `http/1.1`.
    pub fn application_protocol(&self) -> Option<&[u8]> {
        self.application_protocol.as_deref()
    }

    /// Whether the request arrived as TLS early data, which a replay could
    /// repeat. No current transport accepts early data, so this is false.
    pub fn is_early_data(&self) -> bool {
        false
    }
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::{fmt, io, net::SocketAddr, sync::Arc};

/// A client failure. A response body that fails with a client error reports it as the source of a `BodyError`, or as the original `BodyError` when a request body failed.
///
/// `Negotiation`, `Pool`, `Protocol` and `Body` wrap another layer's error transparently: they display it and report its source. `Connect` and `Io` describe the failure and report the I/O error as their source. A cause chain therefore never repeats a message.
#[derive(Clone, Debug)]
pub enum Error {
    /// Opening the transport to `address` failed, including the TLS handshake and server verification, so no request was sent. The source can also be a local failure, such as exhausted file descriptors or an invalid TLS server name. Cancellation while connecting, including a caller's deadline, is `Cancelled`. A rejected protocol selection is `Negotiation`, which also precedes any request. With TLS 1.3, a server that rejects the client after the handshake is reported by the first exchange instead.
    Connect {
        address: SocketAddr,
        source: Arc<io::Error>,
    },
    Negotiation(base_http_protocol::NegotiationError),
    Closed,
    Busy,
    Cancelled,
    Abandoned,
    InvalidOptions,
    Unsupported(&'static str),
    Pool(base_pool::Error),
    Protocol(Arc<base_http1::Error>),
    Io(Arc<io::Error>),
    Body(base_http::body::BodyError),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect { address, .. } => write!(f, "connecting to {address} failed"),
            Self::Negotiation(error) => error.fmt(f),
            Self::Closed => f.write_str("HTTP client is closed"),
            Self::Busy => f.write_str("HTTP client request limit reached"),
            Self::Cancelled => f.write_str("HTTP client request cancelled"),
            Self::Abandoned => f.write_str("HTTP client response abandoned"),
            Self::InvalidOptions => f.write_str("invalid HTTP client limits"),
            Self::Unsupported(reason) => write!(f, "unsupported HTTP client operation: {reason}"),
            Self::Pool(error) => error.fmt(f),
            Self::Protocol(error) => error.fmt(f),
            Self::Io(_) => f.write_str("HTTP client I/O failed"),
            Self::Body(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect { source, .. } => Some(source.as_ref()),
            Self::Negotiation(error) => error.source(),
            Self::Pool(error) => error.source(),
            Self::Protocol(error) => error.source(),
            Self::Io(error) => Some(error.as_ref()),
            Self::Body(error) => error.source(),
            _ => None,
        }
    }
}
impl Error {
    /// Classify a transport establishment failure, preserving cancellation.
    pub(crate) fn connect(address: SocketAddr, error: base_scheduler::OperationError) -> Self {
        match error {
            base_scheduler::OperationError::Cancelled(_) => Self::Cancelled,
            base_scheduler::OperationError::Io(error) => Self::Connect {
                address,
                source: Arc::new(error),
            },
        }
    }
}

impl Error {
    /// Classify a failure of a caller's body or interim observer. A relayed client body failure is kept as is, so it is reported once.
    pub(crate) fn body<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        let mut error = Some(error);
        if let Some(Some(Self::Body(body))) =
            (&mut error as &mut dyn std::any::Any).downcast_mut::<Option<Self>>()
        {
            return Self::Body(body.clone());
        }
        Self::Body(base_http::body::BodyError::new(error.unwrap()))
    }
}

impl From<base_pool::Error> for Error {
    fn from(error: base_pool::Error) -> Self {
        match error {
            base_pool::Error::Cancelled => Self::Cancelled,
            base_pool::Error::Closed => Self::Closed,
            error => Self::Pool(error),
        }
    }
}
impl From<base_http1::Error> for Error {
    fn from(error: base_http1::Error) -> Self {
        match error {
            base_http1::Error::Cancelled => Self::Cancelled,
            error => Self::Protocol(Arc::new(error)),
        }
    }
}
impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(Arc::new(error))
    }
}
impl From<base_scheduler::OperationError> for Error {
    fn from(error: base_scheduler::OperationError) -> Self {
        base_http1::Error::from(error).into()
    }
}
impl From<base_http::body::BodyError> for Error {
    fn from(error: base_http::body::BodyError) -> Self {
        Self::Body(error)
    }
}

impl From<base_http_protocol::NegotiationError> for Error {
    fn from(error: base_http_protocol::NegotiationError) -> Self {
        Self::Negotiation(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chain(error: &dyn std::error::Error) -> Vec<String> {
        let mut messages = vec![error.to_string()];
        let mut source = error.source();
        while let Some(error) = source {
            messages.push(error.to_string());
            source = error.source();
        }
        messages
    }

    fn refused() -> io::Error {
        io::Error::from(io::ErrorKind::ConnectionRefused)
    }

    #[test]
    fn cause_chains_never_repeat_a_message() {
        let errors = [
            Error::from(base_http1::Error::Io(refused())),
            Error::from(refused()),
            Error::from(base_http::body::BodyError::new(refused())),
            Error::from(base_http::body::BodyError::new(base_http1::Error::Io(
                refused(),
            ))),
            // A request body relaying another body's failure.
            Error::from(base_http::body::BodyError::new(
                base_http::body::BodyError::new(refused()),
            )),
            // A failed interim response observer.
            Error::from(base_http1::Error::Body(Box::new(
                base_http::InterimError::Handler(Error::from(base_http::body::BodyError::new(
                    refused(),
                ))),
            ))),
            // A client body relaying another client's failed body.
            Error::body(Error::body(refused())),
            Error::Pool(base_pool::Error::WaitersFull),
            Error::connect("127.0.0.1:9".parse().unwrap(), refused().into()),
            Error::from(base_http_protocol::NegotiationError::Missing),
        ];
        for error in &errors {
            let messages = chain(error);
            let unique: std::collections::HashSet<_> = messages.iter().collect();
            assert_eq!(
                unique.len(),
                messages.len(),
                "{error:?} repeats {messages:?}"
            );
        }
    }

    #[test]
    fn connect_errors_name_the_address() {
        let error = Error::connect("10.0.0.5:9292".parse().unwrap(), refused().into());
        assert_eq!(
            chain(&error),
            ["connecting to 10.0.0.5:9292 failed", "connection refused"]
        );
        let cancelled = base_scheduler::OperationError::Cancelled(base_scheduler::Cancelled);
        assert!(matches!(
            Error::connect("10.0.0.5:9292".parse().unwrap(), cancelled),
            Error::Cancelled
        ));
    }

    #[test]
    fn protocol_errors_are_transparent() {
        let error = Error::from(base_http1::Error::Io(refused()));
        assert_eq!(
            chain(&error),
            ["HTTP/1 connection I/O failed", "connection refused"]
        );
    }
}

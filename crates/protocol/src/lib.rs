// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![doc = include_str!("../readme.md")]
#![forbid(unsafe_code)]

use base_endpoint::stream::{ReadableStream, WritableStream};
use base_http1::{RequestReader, RequestWriter, ResponseReader, ResponseWriter};
use std::fmt;

/// Enabled HTTP engines and their establishment policy, shared by plain and
/// encrypted establishment, so the identifiers offered and those accepted
/// always agree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpProtocols {
    /// The enabled engines, most preferred first. Application-protocol offers
    /// follow this order, and only these engines are negotiated.
    pub enabled: Vec<HttpProtocol>,
    /// The engine used on a plain TCP connection, which has no negotiation.
    pub plaintext: Plaintext,
    /// Reject an encrypted peer that did not negotiate an application protocol.
    /// Otherwise such a peer uses HTTP/1, when it is enabled. Unknown or
    /// disabled negotiated protocols are rejected regardless of this setting.
    pub require_alpn: bool,
}
impl Default for HttpProtocols {
    fn default() -> Self {
        Self {
            enabled: vec![HttpProtocol::Http1],
            plaintext: Plaintext::Http1,
            require_alpn: false,
        }
    }
}
impl HttpProtocols {
    /// Opaque application-protocol names supplied to the security provider, in
    /// preference order.
    pub fn alpn_protocols(&self) -> Vec<Vec<u8>> {
        self.enabled
            .iter()
            .map(|protocol| protocol.alpn().to_vec())
            .collect()
    }
    /// Select the engine for a plain TCP connection.
    pub fn plaintext(&self) -> Result<HttpProtocol, NegotiationError> {
        let protocol = match self.plaintext {
            Plaintext::Http1 => HttpProtocol::Http1,
        };
        self.enabled(protocol)
    }
    /// Select the engine after an authenticated transport reports negotiation.
    pub fn negotiated(&self, selected: Option<&[u8]>) -> Result<HttpProtocol, NegotiationError> {
        match selected {
            Some(name) => self
                .enabled
                .iter()
                .copied()
                .find(|protocol| protocol.alpn() == name)
                .ok_or_else(|| NegotiationError::Unsupported(name.to_vec())),
            None if !self.require_alpn => self.enabled(HttpProtocol::Http1),
            None => Err(NegotiationError::Missing),
        }
    }
    fn enabled(&self, protocol: HttpProtocol) -> Result<HttpProtocol, NegotiationError> {
        if self.enabled.contains(&protocol) {
            Ok(protocol)
        } else {
            Err(NegotiationError::Disabled(protocol))
        }
    }
}

/// How a plain TCP connection, which negotiates nothing, chooses its engine.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Plaintext {
    /// Use HTTP/1.
    #[default]
    Http1,
}

/// An HTTP engine: a wire protocol implementation selected per connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HttpProtocol {
    /// HTTP/1 framing over a reliable plaintext byte stream.
    Http1,
}
impl HttpProtocol {
    /// The engine's application-protocol identifier.
    pub fn alpn(self) -> &'static [u8] {
        match self {
            Self::Http1 => b"http/1.1",
        }
    }
    /// Construct a client connection of this engine over an established
    /// stream.
    pub fn client<R: ReadableStream, W: WritableStream>(
        self,
        reader: R,
        writer: W,
        http1: base_http1::Options,
    ) -> Result<ClientConnection<R, W>, ProtocolError> {
        match self {
            Self::Http1 => Ok(ClientConnection::Http1(
                ResponseReader::new(reader, http1)?,
                RequestWriter::new(writer),
            )),
        }
    }
    /// Construct a server connection of this engine over an established
    /// stream.
    pub fn server<R: ReadableStream, W: WritableStream>(
        self,
        reader: R,
        writer: W,
        http1: base_http1::Options,
    ) -> Result<ServerConnection<R, W>, ProtocolError> {
        match self {
            Self::Http1 => Ok(ServerConnection::Http1(
                RequestReader::new(reader, http1)?,
                ResponseWriter::new(writer),
            )),
        }
    }
}

/// A selected client connection, one variant for each engine compiled in. The
/// client dispatches on it, so adding an engine adds a variant.
pub enum ClientConnection<R, W> {
    Http1(ResponseReader<R>, RequestWriter<W>),
}

/// A selected server connection, one variant for each engine compiled in. The
/// server dispatches on it, so adding an engine adds a variant.
pub enum ServerConnection<R, W> {
    Http1(RequestReader<R>, ResponseWriter<W>),
}

/// A failure of an engine, naming the engine that produced it. Transparent: it
/// displays the engine's error and reports that error's source.
#[derive(Debug)]
pub enum ProtocolError {
    Http1(base_http1::Error),
}
impl ProtocolError {
    /// The engine that failed.
    pub fn protocol(&self) -> HttpProtocol {
        match self {
            Self::Http1(_) => HttpProtocol::Http1,
        }
    }
}
impl From<base_http1::Error> for ProtocolError {
    fn from(error: base_http1::Error) -> Self {
        Self::Http1(error)
    }
}
impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http1(error) => error.fmt(formatter),
        }
    }
}
impl std::error::Error for ProtocolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Http1(error) => error.source(),
        }
    }
}

/// Negotiated evidence did not select an enabled HTTP engine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NegotiationError {
    /// The caller required a negotiated identifier but none was supplied.
    Missing,
    /// The peer selected an unsupported identifier. Retain its opaque bytes.
    Unsupported(Vec<u8>),
    /// The engine the connection requires is not enabled.
    Disabled(HttpProtocol),
}
impl fmt::Display for NegotiationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => formatter.write_str("HTTP application protocol was not negotiated"),
            Self::Disabled(protocol) => {
                let name = protocol.alpn().escape_ascii();
                write!(
                    formatter,
                    "HTTP application protocol \"{name}\" is not enabled"
                )
            }
            Self::Unsupported(name) => {
                let name = name.escape_ascii();
                write!(
                    formatter,
                    "unsupported HTTP application protocol: \"{name}\""
                )
            }
        }
    }
}
impl std::error::Error for NegotiationError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn http1_disabled() -> HttpProtocols {
        HttpProtocols {
            enabled: Vec::new(),
            ..HttpProtocols::default()
        }
    }

    #[test]
    fn offers_follow_the_enabled_engines_in_order() {
        assert_eq!(
            HttpProtocols::default().alpn_protocols(),
            vec![b"http/1.1".to_vec()]
        );
        assert!(http1_disabled().alpn_protocols().is_empty());
    }

    #[test]
    fn unknown_or_disabled_identifiers_are_rejected() {
        for required in [false, true] {
            let protocols = HttpProtocols {
                require_alpn: required,
                ..HttpProtocols::default()
            };
            for name in [b"h2".as_slice(), b"h3", b"http/1.1\0", b"\xff"] {
                assert_eq!(
                    protocols.negotiated(Some(name)),
                    Err(NegotiationError::Unsupported(name.to_vec()))
                );
            }
            assert_eq!(
                protocols.negotiated(Some(b"http/1.1")),
                Ok(HttpProtocol::Http1)
            );
            assert_eq!(protocols.plaintext(), Ok(HttpProtocol::Http1));
        }
        let disabled = http1_disabled();
        assert_eq!(
            disabled.negotiated(Some(b"http/1.1")),
            Err(NegotiationError::Unsupported(b"http/1.1".to_vec()))
        );
        assert_eq!(
            disabled.plaintext(),
            Err(NegotiationError::Disabled(HttpProtocol::Http1))
        );
    }

    #[test]
    fn a_missing_identifier_selects_http1_only_when_allowed_and_enabled() {
        assert_eq!(
            HttpProtocols::default().negotiated(None),
            Ok(HttpProtocol::Http1)
        );
        let required = HttpProtocols {
            require_alpn: true,
            ..HttpProtocols::default()
        };
        assert_eq!(required.negotiated(None), Err(NegotiationError::Missing));
        assert_eq!(
            http1_disabled().negotiated(None),
            Err(NegotiationError::Disabled(HttpProtocol::Http1))
        );
    }

    #[test]
    fn negotiation_errors_describe_the_selection() {
        assert_eq!(
            NegotiationError::Missing.to_string(),
            "HTTP application protocol was not negotiated"
        );
        assert_eq!(
            NegotiationError::Unsupported(b"h2".to_vec()).to_string(),
            "unsupported HTTP application protocol: \"h2\""
        );
        assert_eq!(
            NegotiationError::Unsupported(b"\xff\"".to_vec()).to_string(),
            r#"unsupported HTTP application protocol: "\xff\"""#
        );
        assert_eq!(
            NegotiationError::Disabled(HttpProtocol::Http1).to_string(),
            "HTTP application protocol \"http/1.1\" is not enabled"
        );
    }

    #[test]
    fn protocol_errors_name_their_engine_and_are_transparent() {
        let io = std::io::Error::other("reset");
        let error = ProtocolError::from(base_http1::Error::Io(io));
        assert_eq!(error.protocol(), HttpProtocol::Http1);
        assert_eq!(error.to_string(), "HTTP/1 connection I/O failed");
        assert_eq!(
            std::error::Error::source(&error).unwrap().to_string(),
            "reset"
        );
    }
}

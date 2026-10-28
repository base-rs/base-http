// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![doc = include_str!("../readme.md")]
#![forbid(unsafe_code)]

use base_endpoint::stream::{ReadableStream, WritableStream};
use base_http1::{RequestReader, RequestWriter, ResponseReader, ResponseWriter};
use std::fmt;

/// Enabled HTTP implementations and their establishment policy.
#[derive(Clone, Copy, Debug, Default)]
pub struct HttpProtocols {
    /// Reject an encrypted peer that did not negotiate an application protocol.
    /// Unknown negotiated protocols are rejected regardless of this setting.
    pub require_alpn: bool,
}
impl HttpProtocols {
    /// Opaque application-protocol names supplied to the security provider.
    pub fn alpn_protocols(&self) -> Vec<Vec<u8>> {
        vec![b"http/1.1".to_vec()]
    }
    /// The initial explicit policy for a plain TCP stream.
    pub fn plaintext(&self) -> HttpProtocol {
        HttpProtocol::Http1
    }
    /// Select the engine after an authenticated transport reports negotiation.
    pub fn negotiated(&self, selected: Option<&[u8]>) -> Result<HttpProtocol, NegotiationError> {
        match selected {
            Some(b"http/1.1") => Ok(HttpProtocol::Http1),
            None if !self.require_alpn => Ok(HttpProtocol::Http1),
            None => Err(NegotiationError::Missing),
            Some(name) => Err(NegotiationError::Unsupported(name.to_vec())),
        }
    }
}

/// An enabled wire engine selected before exchange admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpProtocol {
    /// HTTP/1 framing over a reliable plaintext byte stream.
    Http1,
}
impl HttpProtocol {
    /// Construct matching client codec directions over an established stream.
    pub fn client<R: ReadableStream, W: WritableStream>(
        self,
        reader: R,
        writer: W,
        options: base_http1::Options,
    ) -> Result<(ResponseReader<R>, RequestWriter<W>), base_http1::Error> {
        match self {
            Self::Http1 => Ok((
                ResponseReader::new(reader, options)?,
                RequestWriter::new(writer),
            )),
        }
    }
    /// Construct matching server codec directions over an established stream.
    pub fn server<R: ReadableStream, W: WritableStream>(
        self,
        reader: R,
        writer: W,
        options: base_http1::Options,
    ) -> Result<(RequestReader<R>, ResponseWriter<W>), base_http1::Error> {
        match self {
            Self::Http1 => Ok((
                RequestReader::new(reader, options)?,
                ResponseWriter::new(writer),
            )),
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
}
impl fmt::Display for NegotiationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => formatter.write_str("HTTP application protocol was not negotiated"),
            Self::Unsupported(name) => {
                write!(formatter, "unsupported HTTP application protocol: {name:?}")
            }
        }
    }
}
impl std::error::Error for NegotiationError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_identifiers_never_fall_back_to_http1() {
        for required in [false, true] {
            let protocols = HttpProtocols {
                require_alpn: required,
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
            assert_eq!(protocols.plaintext(), HttpProtocol::Http1);
        }
    }
}

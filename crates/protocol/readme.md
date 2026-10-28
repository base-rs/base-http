# Base HTTP Protocol

Experimental protocol selection shared by the application client and server, following Async::HTTP's protocol factories. `HttpProtocols` supplies application-protocol offers and selects an enabled engine from explicit plaintext policy or an opaque negotiated identifier. `HttpProtocol` constructs the corresponding client/server codec pair.

The initial engine is HTTP/1, advertising `http/1.1`. Missing ALPN is accepted for older HTTPS peers unless `require_alpn` is enabled. An unknown negotiated identifier is always rejected. TLS configuration, handshakes, encryption and transport ownership remain outside this crate. `base-http1` only consumes plaintext stream capabilities.

HTTP/2 and QUIC/HTTP/3 are future engines. TCP and QUIC require distinct establishment paths; advertising `h3` over a TLS/TCP connection cannot select QUIC. The existing semantic `base_http::Protocol` describes application upgrades and is unrelated to this configuration.

# Base HTTP Protocol

Experimental protocol selection shared by the application client and server, following Async::HTTP's protocol factories. `HttpProtocols` lists the enabled engines in preference order, from which it supplies application-protocol offers, and selects an enabled engine from its `Plaintext` policy or an opaque negotiated identifier, so offers and selection always agree. `HttpProtocol` constructs the selected connection, a `ClientConnection` or `ServerConnection`, closed enums with one variant for each engine compiled in, on which the client and server dispatch. `ProtocolError` reports an engine's failure and names the engine.

The initial engine is HTTP/1, advertising `http/1.1`. Missing ALPN is accepted for older HTTPS peers unless `require_alpn` is enabled, and only when HTTP/1 is enabled. An unknown or disabled negotiated identifier is always rejected, and a plaintext policy whose engine is disabled refuses the connection. TLS configuration, handshakes, encryption and transport ownership remain outside this crate. `base-http1` only consumes plaintext stream capabilities.

HTTP/2 and QUIC/HTTP/3 are future engines. TCP and QUIC require distinct establishment paths; advertising `h3` over a TLS/TCP connection cannot select QUIC. The existing semantic `base_http::Protocol` describes application upgrades and is unrelated to this configuration.

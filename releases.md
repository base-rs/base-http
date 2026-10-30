# Releases

## v0.1.0

- First release of `base-http`, `base-http1`, `base-http-protocol`, `base-http-client` and `base-http-server`. `base-http` defines runtime-independent HTTP semantics: requests and responses, methods, statuses, versions, request targets, ordered fields with header interpretation and combination policies, interim responses, and buffered and streaming bodies. Constructing messages and mapping bodies performs no I/O.
- `base-http1` implements native HTTP/1.0 and HTTP/1.1 framing over Base scheduler streams. It parses heads with a hand-written parser and handles fixed-length, chunked and close-delimited bodies, trailers and interim responses. The server side also manages `101` upgrades and CONNECT. `base-http-protocol` selects the protocol for a connection, including through ALPN.
- `base-http-client` provides a cloneable client with an explicit driver that pools HTTP/1 connections through `base-pool`, over plain TCP or, with the `rustls` feature, TLS. `base-http-server` serves connections and listeners with a connection limit, over plain TCP or, with the `rustls` feature, TLS.
- Errors report each cause once: an error either describes its cause in its message or returns it from `source`. A connection that fails to open is `base_http_client::Error::Connect`, which carries the endpoint address.
- Requires Rust 1.87, `base-scheduler 0.3`, `base-pool 0.3` and `base-endpoint 0.1`.

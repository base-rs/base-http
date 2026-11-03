// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! The shared protocol cases, run between this client and the Base server for each HTTP version and transport.
use base_http::Version;
use base_http_fixtures::{Context, ServerContext, protocol::App};
use base_scheduler_native::NativeScheduler;

fn serve(app: App, version: Version) -> ServerContext<App> {
    ServerContext::new().handler(app).version(version)
}

fn http11(app: App, test: impl AsyncFnOnce(&Context<'_, NativeScheduler, App>)) {
    serve(app, Version::Http11).run(test);
}

mod http11 {
    base_http_fixtures::protocol_tests!(run: super::http11);
    base_http_fixtures::protocol_trailer_tests!(run: super::http11);
}

// HTTP/1.0 cannot send trailers or interim responses; its interim case expects only the final response, and its trailer cases expect the trailers to be dropped.
fn http10(app: App, test: impl AsyncFnOnce(&Context<'_, NativeScheduler, App>)) {
    serve(app, Version::Http10).run(test);
}

mod http10 {
    base_http_fixtures::protocol_tests!(run: super::http10);
    base_http_fixtures::protocol_trailer_tests!(run: super::http10);
}

#[cfg(feature = "rustls")]
fn https(app: App, test: impl AsyncFnOnce(&Context<'_, NativeScheduler, App>)) {
    serve(app, Version::Http11).tls().run(test);
}

#[cfg(feature = "rustls")]
mod https {
    base_http_fixtures::protocol_tests!(run: super::https);
    base_http_fixtures::protocol_trailer_tests!(run: super::https);
}

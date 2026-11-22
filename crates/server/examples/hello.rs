// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::{
    Request, Response, Status,
    body::{BufferedBody, ReadableBody},
};
use base_http_server::{Endpoint, Handler, Options, Server, ServerContext};
use base_scheduler::{Cancellation, Placement, SocketAccept, SocketLocalAddress, SpawnScoped};
use base_scheduler_native::{NativeOptions, NativeScheduler};
use futures_lite::future::block_on;
use std::{convert::Infallible, net::SocketAddr};

struct Hello {
    close: bool,
}
impl<'h> Handler<'h, ServerContext> for Hello {
    type Error = Infallible;
    type Body<B: ReadableBody + 'h> = BufferedBody<&'static [u8]>;

    async fn call<B: ReadableBody + 'h>(
        &self,
        _: Request<B>,
        _: &mut ServerContext,
        _: &Cancellation,
    ) -> Result<Response<Self::Body<B>>, Infallible> {
        let mut response = Response::new(
            Status::OK,
            BufferedBody::from(b"Hello from Base HTTP!\n".as_slice()),
        );
        response.fields.add("Content-Type", "text/plain").unwrap();
        if self.close {
            response.fields.add("Connection", "close").unwrap();
        }
        Ok(response)
    }
}

// Serve `handler` until the process ends. `placement` decides where connections run: the scheduler polls them within this future, and a scope runs each as its own task.
async fn serve<'p, P>(
    placement: P,
    handler: &'p Hello,
    options: &'p Options,
    once: bool,
) -> Result<(), Box<dyn std::error::Error>>
where
    P: Placement<'p, Scheduler = NativeScheduler>,
{
    let scheduler = placement.scheduler();
    let address: SocketAddr = std::env::var("BASE_HTTP_ADDRESS")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_owned())
        .parse()?;
    let cancellation = Cancellation::never();
    let endpoint = Endpoint::bind(scheduler, address).await?;
    eprintln!(
        "Listening on http://{}",
        scheduler.socket_local_address(endpoint.listener())?
    );
    if once {
        let (socket, _) = scheduler
            .socket_accept(endpoint.listener(), &cancellation)
            .await?;
        // Stop listening before serving the one connection.
        drop(endpoint);
        base_http_server::serve_connection(scheduler, socket, handler, options, &cancellation)
            .await?;
    } else {
        Server::new(handler, endpoint, options)
            .run(placement, &cancellation)
            .await?;
    }
    Ok(())
}

// `--spawned` runs each connection as its own task, on both workers; otherwise connections interleave on whichever worker polls the server.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scheduler = NativeScheduler::with_options(NativeOptions {
        workers: 2,
        ..Default::default()
    })?;
    let once = std::env::args().any(|arg| arg == "--once");
    // Spawned connections borrow the handler and options, so they outlive the scope.
    let handler = Hello { close: once };
    let options = Options::default();
    let result = if std::env::args().any(|arg| arg == "--spawned") {
        scheduler.scope(async |scope| serve(scope, &handler, &options, once).await)
    } else {
        block_on(serve(&scheduler, &handler, &options, once))
    };
    block_on(scheduler.shutdown());
    result
}

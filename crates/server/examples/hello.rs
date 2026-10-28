// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::{
    InterimHandler, InterimResponses, Request, Response, Status,
    body::{BufferedBody, ReadableBody},
};
use base_http_server::{Handler, Options};
use base_scheduler::{
    Cancellation, SocketAccept, SocketBind, SocketClose, SocketListen, SocketLocalAddress,
    SocketOpen, SocketType,
};
use base_scheduler_direct::{DirectOptions, DirectScheduler};
use futures_lite::future::block_on;
use std::{convert::Infallible, net::SocketAddr};

struct Hello {
    close: bool,
}
impl Handler for Hello {
    type Error = Infallible;
    type Body<B: ReadableBody> = BufferedBody<&'static [u8]>;

    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        _: Request<B>,
        _: &mut InterimResponses<I>,
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scheduler = DirectScheduler::new(DirectOptions {
        threads: 2,
        capacity: 256,
    })?;
    let result = block_on(async {
        let address: SocketAddr = std::env::var("BASE_HTTP_ADDRESS")
            .unwrap_or_else(|_| "127.0.0.1:8080".to_owned())
            .parse()?;
        let once = std::env::args().any(|arg| arg == "--once");
        let handler = Hello { close: once };
        let cancellation = Cancellation::never();
        let listener = scheduler
            .socket_open(address.into(), SocketType::Stream, None, &cancellation)
            .await?;
        scheduler
            .socket_bind(&listener, address, &cancellation)
            .await?;
        scheduler
            .socket_listen(&listener, 128, &cancellation)
            .await?;
        eprintln!(
            "Listening on http://{}",
            scheduler.socket_local_address(&listener)?
        );
        if once {
            let (socket, _) = scheduler.socket_accept(&listener, &cancellation).await?;
            scheduler.socket_close(listener).await?;
            base_http_server::serve_connection(
                &scheduler,
                socket,
                &handler,
                &Default::default(),
                &cancellation,
            )
            .await?;
        } else {
            base_http_server::serve(
                &scheduler,
                listener,
                &handler,
                &Options::default(),
                &cancellation,
            )
            .await?;
        }
        Ok(())
    });
    block_on(scheduler.shutdown());
    result
}

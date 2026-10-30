// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Run with `cargo run --example client_server` from base-http-server.
//! Starts one local HTTP/1 connection, echoes a POST body, then joins cleanup.
use base_http::{
    InterimHandler, InterimResponses, Method, Request, Response, ResponseMetadata, Status,
    body::{BodyFrame, BufferedBody, ReadableBody},
};
use base_http_server::Handler;
use base_http1::{RequestWriter, ResponseReader};
use base_scheduler::{
    Cancellation, SocketAccept, SocketBind, SocketClose, SocketConnect, SocketListen,
    SocketLocalAddress, SocketOpen, SocketStreamReading, SocketStreamWriting, SocketType,
};
use base_scheduler_direct::{DirectOptions, DirectScheduler};
use futures_lite::future::{block_on, zip};
use std::{convert::Infallible, future::Future, net::SocketAddr};

type DemoResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Echo;

impl Handler for Echo {
    type Error = Infallible;
    type Body<B: ReadableBody> = B;

    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        request: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<Response<B>, Infallible> {
        // The response streams directly from the incoming request body.
        Ok(Response::new(Status::OK, request.body))
    }
}

// Failure cancels sibling work; successful completion leaves it running.
// Callers use zip so every operation is awaited through cleanup.
async fn supervise<T, E>(
    work: impl Future<Output = Result<T, E>>,
    cancellation: &Cancellation,
) -> Result<T, E> {
    let result = work.await;
    if result.is_err() {
        cancellation.cancel();
    }
    result
}

async fn client(
    scheduler: &DirectScheduler,
    address: SocketAddr,
    cancellation: &Cancellation,
) -> DemoResult<()> {
    let socket = scheduler
        .socket_open(address.into(), SocketType::Stream, None, cancellation)
        .await?;

    let result = async {
        scheduler
            .socket_connect(&socket, address, cancellation)
            .await?;
        let mut output = RequestWriter::new(scheduler.socket_stream_writer(&socket)?);
        let mut input =
            ResponseReader::new(scheduler.socket_stream_reader(&socket)?, Default::default())?;

        let exchange = async {
            let mut request = Request::new(
                Method::POST,
                "/echo",
                BufferedBody::from(b"Hello from Base HTTP!\n".as_slice()),
            );
            request.authority = Some(address.to_string().into());
            request.fields.add("Content-Type", "text/plain")?;
            // This demonstration makes one exchange and then exits.
            request.persistent = Some(false);
            let metadata = request.metadata.clone();

            let send = output.write_request(&mut request, cancellation);
            let receive = async {
                let mut interim = InterimResponses::new(|metadata: ResponseMetadata| async move {
                    println!("Interim response: {}", metadata.status);
                    Ok::<_, Infallible>(())
                });
                let mut response = input
                    .read_response(&metadata, &mut interim, cancellation)
                    .await?;
                let mut headers = base_http::HttpHeaders::new(response.fields.clone());
                // read_response returns when the final response metadata is ready.
                println!("Response: {}", response.status);
                let mut bytes = Vec::new();
                loop {
                    let frame = response.read(cancellation).await?;
                    let end = frame.is_end();
                    match frame {
                        BodyFrame::Data { data, .. } => bytes.extend_from_slice(&data),
                        BodyFrame::Headers { fields, .. } => headers.append_trailers(fields)?,
                        BodyFrame::End => {}
                    }
                    if end {
                        break;
                    }
                }
                Ok::<_, base_http1::Error>(bytes)
            };

            // Keep uploading while the response metadata and body are consumed.
            let (sent, received) = zip(
                supervise(send, cancellation),
                supervise(receive, cancellation),
            )
            .await;
            sent?;
            let bytes = received?;
            print!("Body: {}", String::from_utf8_lossy(&bytes));
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        }
        .await;

        // Always retire both lanes, including when the exchange failed.
        let (output_closed, input_closed) = zip(output.close(), input.close()).await;
        exchange?;
        output_closed?;
        input_closed?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;

    // The lane handles have been dropped before exclusive socket closure.
    let closed = scheduler.socket_close(socket).await;
    result?;
    closed?;
    Ok(())
}

async fn run(scheduler: &DirectScheduler) -> DemoResult<()> {
    let cancellation = Cancellation::new();
    let address: SocketAddr = "127.0.0.1:0".parse()?;
    let listener = scheduler
        .socket_open(address.into(), SocketType::Stream, None, &cancellation)
        .await?;

    let result = async {
        scheduler
            .socket_bind(&listener, address, &cancellation)
            .await?;
        scheduler.socket_listen(&listener, 1, &cancellation).await?;
        let address = scheduler.socket_local_address(&listener)?;
        println!("Listening on http://{address}");

        let server = async {
            let (socket, _) = scheduler.socket_accept(&listener, &cancellation).await?;
            base_http_server::serve_connection(
                scheduler,
                socket,
                &Echo,
                &Default::default(),
                &cancellation,
            )
            .await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        };
        let (served, received) = zip(
            supervise(server, &cancellation),
            supervise(client(scheduler, address, &cancellation), &cancellation),
        )
        .await;
        received?;
        served?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;

    let closed = scheduler.socket_close(listener).await;
    result?;
    closed?;
    Ok(())
}

fn main() -> DemoResult<()> {
    let scheduler = DirectScheduler::new(DirectOptions {
        threads: 4,
        capacity: 64,
    })?;
    let result = block_on(run(&scheduler));
    block_on(scheduler.shutdown());
    result
}

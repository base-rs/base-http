// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::{
    InterimHandler, InterimResponses, Request, Response, Status,
    body::{BufferedBody, ReadableBody},
};
use base_http_server::Handler;
use base_scheduler::{
    Cancellation, SocketAccept, SocketClose, SocketRegistration, SocketSetOption,
    SocketStreamReading, SocketStreamWriting, Spawn, TcpNoDelay,
};
use base_scheduler_direct::{DirectOptions, DirectScheduler};
use base_scheduler_native::{NativeOptions, NativeScheduler};
use base_scheduler_tokio::TokioScheduler;
use futures_util::{
    future::{Either, select},
    stream::{FuturesUnordered, StreamExt},
};
use http_body_util::Full;
use hyper::{body::Bytes, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use std::{convert::Infallible, io::Write, net::TcpListener, pin::pin, sync::Arc};

mod raw;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const BODY: &[u8] = b"Hello, world!";
const MAX_CONNECTIONS: usize = 1024;

struct Hello;
impl Handler for Hello {
    type Error = Infallible;
    type Body<B: ReadableBody> = BufferedBody<&'static [u8]>;

    #[allow(clippy::manual_async_fn)] // Drop the unused request at the service-call boundary.
    fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        _: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> impl std::future::Future<Output = std::result::Result<Response<Self::Body<B>>, Infallible>> + Send
    {
        // Match Hyper's service closure: the unused request is dropped before
        // returning a future, which constructs a fresh response when polled.
        async {
            let mut response = Response::new(Status::OK, BufferedBody::from(BODY));
            response.fields.add("Content-Type", "text/plain").unwrap();
            Ok(response)
        }
    }
}

fn ready(listener: &TcpListener) -> Result<()> {
    println!("{}", listener.local_addr()?);
    std::io::stdout().flush()?;
    Ok(())
}

fn connection_finished<T, E: std::fmt::Display>(result: std::result::Result<T, E>) {
    if let Err(error) = result {
        // Load generators may close a socket with an in-flight final request.
        // Peer failures do not stop admission, as in base_http_server::serve.
        eprintln!("connection: {error}");
    }
}

// Every variant has one application worker and one FuturesUnordered supervisor.
// Connections are polled in that supervisor, matching base_http_server::serve.
// This small accept loop additionally sets TCP_NODELAY on every accepted socket.
async fn base_server<S>(scheduler: &S) -> Result<()>
where
    S: SocketRegistration
        + SocketAccept
        + SocketSetOption<TcpNoDelay>
        + SocketStreamReading
        + SocketStreamWriting
        + SocketClose,
    S::Socket: 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0")?;
    ready(&listener)?;
    let listener = scheduler.register_listener(listener)?;
    let cancellation = Cancellation::never();
    let options = base_http1::Options::default();
    let mut connections = FuturesUnordered::new();
    loop {
        if connections.len() == MAX_CONNECTIONS {
            connection_finished(connections.next().await.unwrap());
            continue;
        }
        let accepted = {
            let mut accept = pin!(scheduler.socket_accept(&listener, &cancellation));
            loop {
                let completed = pin!(async {
                    if connections.is_empty() {
                        std::future::pending().await
                    } else {
                        connections.next().await.unwrap()
                    }
                });
                match select(accept.as_mut(), completed).await {
                    Either::Left((accepted, _)) => break accepted,
                    Either::Right((completed, _)) => {
                        connection_finished(completed);
                    }
                }
            }
        };
        let (socket, _) = accepted?;
        scheduler
            .socket_set_option(&socket, TcpNoDelay, true, &cancellation)
            .await?;
        connections.push(base_http_server::serve_connection(
            scheduler,
            socket,
            &Hello,
            &options,
            &cancellation,
        ));
    }
}

async fn hyper_server() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    ready(&listener)?;
    listener.set_nonblocking(true)?;
    let listener = tokio::net::TcpListener::from_std(listener)?;
    let mut builder = http1::Builder::new();
    // Base currently emits no automatic Date field. Match the response work.
    builder.auto_date_header(false);
    let mut connections = FuturesUnordered::new();
    loop {
        if connections.len() == MAX_CONNECTIONS {
            connection_finished(connections.next().await.unwrap());
            continue;
        }
        let accepted = {
            let mut accept = pin!(listener.accept());
            loop {
                let completed = pin!(async {
                    if connections.is_empty() {
                        std::future::pending().await
                    } else {
                        connections.next().await.unwrap()
                    }
                });
                match select(accept.as_mut(), completed).await {
                    Either::Left((accepted, _)) => break accepted,
                    Either::Right((completed, _)) => {
                        connection_finished(completed);
                    }
                }
            }
        };
        let (socket, _) = accepted?;
        socket.set_nodelay(true)?;
        connections.push(builder.serve_connection(
            TokioIo::new(socket),
            service_fn(|_: hyper::Request<hyper::body::Incoming>| async {
                let mut response = hyper::Response::new(Full::new(Bytes::from_static(BODY)));
                response.headers_mut().insert(
                    hyper::header::CONTENT_TYPE,
                    hyper::header::HeaderValue::from_static("text/plain"),
                );
                Ok::<_, Infallible>(response)
            }),
        ));
    }
}

fn main() -> Result<()> {
    match std::env::args().nth(1).as_deref() {
        Some(backend @ ("base-native" | "base-native-background" | "base-native-raw")) => {
            let scheduler = Arc::new(if backend == "base-native-background" {
                NativeScheduler::with_selector(
                    base_scheduler_native::ReadinessSelector::with_options(
                        base_scheduler_selector::SelectorOptions { threads: 1, capacity: MAX_CONNECTIONS },
                    )?,
                    base_scheduler_native::NativeExecutorOptions { workers: 1, ..Default::default() },
                )?
            } else {
                NativeScheduler::with_options(NativeOptions {
                    workers: 1, threads: 1, capacity: MAX_CONNECTIONS,
                    ..Default::default()
                })?
            });
            let shared = scheduler.clone();
            let raw = backend == "base-native-raw";
            let server = scheduler.spawn(async move {
                if raw {
                    raw::base_server(shared.as_ref()).await
                } else {
                    base_server(shared.as_ref()).await
                }
            })?;
            let result = scheduler
                .block_on(server)
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            scheduler.block_on(scheduler.shutdown());
            result
        }
        #[cfg(target_os = "linux")]
        Some(backend @ ("base-iouring" | "base-iouring-raw")) => {
            use base_scheduler_iouring::{IoUringOptions, IoUringSelector};
            let (selector, drivers) = IoUringSelector::with_workers(IoUringOptions {
                capacity: MAX_CONNECTIONS,
                fallback: base_scheduler_selector::SelectorOptions { threads: 1, capacity: MAX_CONNECTIONS },
                ..Default::default()
            }, 1)?;
            let scheduler = Arc::new(NativeScheduler::with_drivers(selector, drivers,
                base_scheduler_native::NativeExecutorOptions { workers: 1, ..Default::default() })?);
            let shared = scheduler.clone();
            let raw = backend.ends_with("-raw");
            let server = scheduler.spawn(async move {
                if raw { raw::base_server(shared.as_ref()).await }
                else { base_server(shared.as_ref()).await }
            })?;
            let result = scheduler.block_on(server)
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            scheduler.block_on(scheduler.shutdown());
            result
        }
        Some("base-direct") => {
            let scheduler = DirectScheduler::new(DirectOptions {
                threads: 1,
                capacity: MAX_CONNECTIONS,
            })?;
            let result = futures_lite::future::block_on(base_server(&scheduler));
            futures_lite::future::block_on(scheduler.shutdown());
            result
        }
        Some(backend @ ("base-tokio" | "hyper-tokio" | "base-tokio-raw" | "tokio-raw" | "tokio-task-raw")) => {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(async {
                if backend == "tokio-task-raw" {
                    tokio::spawn(raw::tokio_server()).await?
                } else if backend == "tokio-raw" {
                    raw::tokio_server().await
                } else if backend == "hyper-tokio" {
                    hyper_server().await
                } else {
                    let scheduler = TokioScheduler::new(tokio::runtime::Handle::current());
                    let result = if backend == "base-tokio-raw" {
                        raw::base_server(&scheduler).await
                    } else {
                        base_server(&scheduler).await
                    };
                    scheduler.shutdown().await;
                    result
                }
            })
        }
        _ => Err(
            "usage: base-http-hello-benchmark base-native|base-native-background|base-iouring|base-iouring-raw|base-direct|base-tokio|hyper-tokio|base-native-raw|base-tokio-raw|tokio-raw|tokio-task-raw"
                .into(),
        ),
    }
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#[cfg(target_os = "linux")]
use base_scheduler::{SocketRecvFlags, SocketRecvMessage};

use crate::Result;
use base_scheduler::{
    BufferPooling, Cancellation, IoBuf, SocketAccept, SocketRegistration, SocketSetOption,
    SocketStreamReader, SocketStreamReading, SocketStreamWriter, SocketStreamWriting, Spawn,
    TcpNoDelay,
};
use base_scheduler_tokio::TokioScheduler;
use std::{
    io::{self, Write},
    net::TcpListener,
    sync::Arc,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const BUFFER_SIZE: usize = 65536;

fn ready(listener: &TcpListener) {
    println!("{}", listener.local_addr().expect("listener address"));
    io::stdout().flush().expect("report listener address");
}

async fn tokio_server(listener: TcpListener, connections: usize) {
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    let listener =
        tokio::net::TcpListener::from_std(listener).expect("Tokio listener registration");
    let mut tasks = Vec::with_capacity(connections);
    for _ in 0..connections {
        let (mut socket, _) = listener.accept().await.expect("accept connection");
        socket.set_nodelay(true).expect("TCP_NODELAY");
        tasks.push(tokio::spawn(async move {
            let mut buffer = vec![0; BUFFER_SIZE];
            loop {
                let count = socket.read(&mut buffer).await.expect("echo read");
                if count == 0 {
                    return;
                }
                socket
                    .write_all(&buffer[..count])
                    .await
                    .expect("echo write");
            }
        }));
    }
    for task in tasks {
        task.await.expect("echo task failed");
    }
}

async fn echo<S: SocketStreamReading + SocketStreamWriting>(scheduler: &S, socket: S::Socket)
where
    S::Socket: 'static,
{
    let never = Cancellation::never();
    let mut reader = scheduler
        .socket_stream_reader(&socket)
        .expect("stream reader");
    let mut writer = scheduler
        .socket_stream_writer(&socket)
        .expect("stream writer");
    let mut buffer = vec![0; BUFFER_SIZE];
    loop {
        let (result, returned) = reader.read_owned(buffer, &never).await;
        buffer = returned;
        let count = result.expect("echo operation");
        if count == 0 {
            return;
        }
        let mut sent = 0;
        while sent < count {
            let (result, returned) = writer.write_owned(buffer.slice(sent..count), &never).await;
            buffer = returned.into_inner();
            let count = result.expect("echo operation");
            assert!(count != 0, "echo write made zero progress");
            sent += count;
        }
    }
}

// The historical polling and monitor modes now share the bound borrowed fast path.
async fn echo_poll<S: SocketStreamReading + SocketStreamWriting>(scheduler: &S, socket: S::Socket)
where
    S::Socket: 'static,
{
    echo_monitor(scheduler, socket).await;
}

async fn echo_monitor<S>(scheduler: &S, socket: S::Socket)
where
    S: SocketStreamReading + SocketStreamWriting,
    S::Socket: 'static,
{
    let never = Cancellation::never();
    let mut reader = scheduler
        .socket_stream_reader(&socket)
        .expect("stream reader");
    let mut writer = scheduler
        .socket_stream_writer(&socket)
        .expect("stream writer");
    let mut buffer = vec![0; BUFFER_SIZE];
    loop {
        let count = reader
            .read_into(&mut buffer, &never)
            .await
            .expect("stream read");
        if count == 0 {
            break;
        }
        let mut sent = 0;
        while sent < count {
            let written = writer
                .write(&buffer[sent..count], &never)
                .await
                .expect("stream write");
            assert!(written != 0, "stream write made zero progress");
            sent += written;
        }
    }
    reader.close().await.expect("retire stream reader");
    writer.close().await.expect("retire stream writer");
}

#[cfg(target_os = "linux")]
async fn multishot_server(
    scheduler: Arc<base_scheduler_native::NativeScheduler<base_scheduler_iouring::IoUringSelector>>,
    listener: TcpListener,
    connections: usize,
) {
    let listener = scheduler
        .register_listener(listener)
        .expect("register listener");
    let never = Cancellation::never();
    let mut tasks = Vec::with_capacity(connections);
    for _ in 0..connections {
        let (socket, _) = scheduler
            .socket_accept(&listener, &never)
            .await
            .expect("accept connection");
        scheduler
            .socket_set_option(&socket, TcpNoDelay, true, &never)
            .await
            .expect("TCP_NODELAY");
        let shared = Arc::clone(&scheduler);
        tasks.push(
            scheduler
                .spawn(async move {
                    let never = Cancellation::never();
                    let mut receiving = shared
                        .selector()
                        .socket_recvmsg_multishot(
                            &socket,
                            base_scheduler_iouring::RecvMsgMultishotOptions::default(),
                            SocketRecvFlags::default(),
                            &never,
                        )
                        .await
                        .expect("start multishot receive");
                    let mut writer = shared.socket_stream_writer(&socket).expect("stream writer");
                    while let Some(message) =
                        receiving.recv(&never).await.expect("multishot receive")
                    {
                        let mut bytes = message.payload();
                        while !bytes.is_empty() {
                            let sent = writer.write(bytes, &never).await.expect("echo write");
                            assert_ne!(sent, 0, "echo write made zero progress");
                            bytes = &bytes[sent..];
                        }
                        // Return this lease to the backend's fixed receive pool.
                        drop(message);
                    }
                    receiving.close().await.expect("retire multishot receive");
                })
                .expect("spawn echo task"),
        );
    }
    for task in tasks {
        task.await.expect("echo task failed");
    }
}

async fn scheduler_server<S>(
    scheduler: Arc<S>,
    listener: TcpListener,
    connections: usize,
    poll: bool,
    monitors: bool,
) where
    S: SocketRegistration
        + SocketAccept
        + SocketStreamWriting
        + SocketStreamReading
        + BufferPooling
        + SocketSetOption<TcpNoDelay>
        + Spawn
        + Send
        + Sync
        + 'static,
    S::Socket: 'static,
{
    let listener = scheduler
        .register_listener(listener)
        .expect("scheduler listener registration");
    let mut tasks = Vec::with_capacity(connections);
    let never = Cancellation::never();
    for _ in 0..connections {
        let (socket, _) = scheduler
            .socket_accept(&listener, &never)
            .await
            .expect("accept connection");
        scheduler
            .socket_set_option(&socket, TcpNoDelay, true, &never)
            .await
            .expect("TCP_NODELAY");
        let shared = Arc::clone(&scheduler);
        let task = if monitors {
            scheduler.spawn(async move { echo_monitor(shared.as_ref(), socket).await })
        } else if poll {
            scheduler.spawn(async move { echo_poll(shared.as_ref(), socket).await })
        } else {
            scheduler.spawn(async move { echo(shared.as_ref(), socket).await })
        };
        tasks.push(task.expect("spawn echo task"));
    }
    for task in tasks {
        task.await.expect("echo task failed");
    }
}

pub(crate) fn run(backend: &str, workers: usize, connections: usize) -> Result<()> {
    let listener = TcpListener::bind(std::env::var("BASE_HTTP_LISTEN").unwrap_or_else(|_| "127.0.0.1:0".into())).expect("bind echo listener");
    match backend {
        "tokio" => {
            let runtime = crate::runtime(workers);
            ready(&listener);
            runtime.block_on(tokio_server(listener, connections));
        }
        "tokio-adapter" | "tokio-adapter-poll" | "tokio-adapter-monitor" => {
            let runtime = crate::runtime(workers);
            let scheduler = Arc::new(TokioScheduler::new(runtime.handle().clone()));
            ready(&listener);
            runtime.block_on(scheduler_server(
                Arc::clone(&scheduler),
                listener,
                connections,
                backend == "tokio-adapter-poll",
                backend == "tokio-adapter-monitor",
            ));
            runtime.block_on(scheduler.shutdown());
        }
        #[cfg(target_os = "linux")]
        "native-poll" => {
            use base_scheduler_native::{NativeOptions, NativeScheduler};
            let scheduler = Arc::new(
                NativeScheduler::with_options(NativeOptions {
                    workers,
                    ..Default::default()
                })
                .expect("Native epoll setup"),
            );
            ready(&listener);
            scheduler.block_on(scheduler_server(
                Arc::clone(&scheduler),
                listener,
                connections,
                true,
                false,
            ));
            scheduler.block_on(scheduler.shutdown());
        }
        #[cfg(target_os = "linux")]
        "iouring" | "iouring-poll" | "iouring-multishot" | "iouring-monitor" => {
            use base_scheduler_iouring::{IoUringOptions, IoUringSelector};
            use base_scheduler_native::{NativeExecutorOptions, NativeScheduler};
            let (selector, drivers) = IoUringSelector::with_workers(
                IoUringOptions {
                    capacity: connections.saturating_mul(2).max(256),
                    ..Default::default()
                },
                workers,
            )
            .expect("io_uring setup");
            let scheduler = Arc::new(
                NativeScheduler::with_drivers(
                    selector,
                    drivers,
                    NativeExecutorOptions {
                        workers,
                        ..Default::default()
                    },
                )
                .expect("Native runtime setup"),
            );
            ready(&listener);
            if backend == "iouring-multishot" {
                scheduler.block_on(multishot_server(
                    Arc::clone(&scheduler),
                    listener,
                    connections,
                ));
            } else {
                scheduler.block_on(scheduler_server(
                    Arc::clone(&scheduler),
                    listener,
                    connections,
                    backend == "iouring-poll",
                    backend == "iouring-monitor",
                ));
            }
            scheduler.block_on(scheduler.shutdown());
        }
        _ => return Err("unknown backend (iouring requires Linux)".into()),
    }
    Ok(())
}

#[cfg(test)]
mod tests;

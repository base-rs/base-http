// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Diagnostic controls: one read of arbitrary bytes, then one fixed response write.
//! These deliberately do not implement HTTP request framing or connection policy.

use super::{MAX_CONNECTIONS, Result, connection_finished, ready};
use base_scheduler::{
    Cancellation, SocketAccept, SocketClose, SocketRegistration, SocketSetOption,
    SocketStreamReader, SocketStreamReading, SocketStreamWriter, SocketStreamWriting, Spawn,
    TcpNoDelay,
};
use futures_util::{
    future::{Either, select},
    stream::{FuturesUnordered, StreamExt},
};
use std::{io, net::TcpListener, pin::pin, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const READ_SIZE: usize = 16 * 1024;
const RESPONSE: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 13\r\n\r\nHello, world!";

fn check_write(written: usize) -> io::Result<()> {
    if written == RESPONSE.len() {
        Ok(())
    } else {
        // Preserve the control's single-write boundary instead of retrying.
        Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "raw control requires a complete response in one write",
        ))
    }
}

async fn base_connection<S, const OWNED_RECEIVE: bool>(
    scheduler: &S,
    socket: S::Socket,
    cancellation: &Cancellation,
) -> Result<()>
where
    S: SocketStreamReading + SocketStreamWriting + SocketClose,
    S::Socket: 'static,
{
    let result: Result<()> = async {
        let mut reader = scheduler.socket_stream_reader(&socket)?;
        let mut writer = match scheduler.socket_stream_writer(&socket) {
            Ok(writer) => writer,
            Err(error) => {
                reader.close().await?;
                return Err(error.into());
            }
        };
        let result: Result<()> = async {
            // Keep identical storage in both variants: only the receive API changes.
            let mut buffer = vec![0; READ_SIZE];
            loop {
                let count = if OWNED_RECEIVE {
                    let (result, returned) = reader.read_owned(buffer, cancellation).await;
                    buffer = returned;
                    result?
                } else {
                    reader.read_into(&mut buffer, cancellation).await?
                };
                if count == 0 {
                    return Ok(());
                }
                check_write(writer.write(RESPONSE, cancellation).await?)?;
            }
        }
        .await;
        let flushed = if result.is_ok() {
            writer.flush(cancellation).await
        } else {
            Ok(())
        };
        let reader_closed = reader.close().await;
        let writer_closed = writer.close().await;
        result?;
        flushed?;
        reader_closed?;
        writer_closed?;
        Ok(())
    }
    .await;
    let closed = scheduler.socket_close(socket).await;
    result?;
    closed?;
    Ok(())
}

pub async fn base_server<S, const OWNED_RECEIVE: bool>(scheduler: &S) -> Result<()>
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
                    Either::Right((completed, _)) => connection_finished(completed),
                }
            }
        };
        let (socket, _) = accepted?;
        scheduler
            .socket_set_option(&socket, TcpNoDelay, true, &cancellation)
            .await?;
        connections.push(base_connection::<_, OWNED_RECEIVE>(
            scheduler,
            socket,
            &cancellation,
        ));
    }
}

fn task_finished(result: std::result::Result<Result<()>, base_scheduler::TaskError>) {
    match result {
        Ok(result) => connection_finished(result),
        Err(error) => connection_finished(Err::<(), _>(error)),
    }
}

fn spawn_base_connection<S, const OWNED_RECEIVE: bool>(
    scheduler: &Arc<S>,
    socket: S::Socket,
) -> std::result::Result<S::Handle<Result<()>>, base_scheduler::SpawnError>
where
    S: SocketStreamReading + SocketStreamWriting + SocketClose + Spawn + 'static,
    S::Socket: 'static,
{
    let shared = scheduler.clone();
    scheduler.spawn(async move {
        let cancellation = Cancellation::never();
        base_connection::<_, OWNED_RECEIVE>(shared.as_ref(), socket, &cancellation).await
    })
}

pub fn base_task_server<S, const OWNED_RECEIVE: bool>(scheduler: Arc<S>) -> impl std::future::Future<Output = Result<()>> + Send
where
    S: SocketRegistration
        + SocketAccept
        + SocketSetOption<TcpNoDelay>
        + SocketStreamReading
        + SocketStreamWriting
        + SocketClose
        + Spawn
        + 'static,
    S::Socket: 'static,
{
    async move {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    ready(&listener)?;
    let listener = scheduler.register_listener(listener)?;
    let cancellation = Cancellation::never();
    let mut connections = FuturesUnordered::new();
    loop {
        if connections.len() == MAX_CONNECTIONS {
            task_finished(connections.next().await.unwrap());
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
                    Either::Right((completed, _)) => task_finished(completed),
                }
            }
        };
        let (socket, _) = accepted?;
        scheduler
            .socket_set_option(&socket, TcpNoDelay, true, &cancellation)
            .await?;
        let connection = spawn_base_connection::<_, OWNED_RECEIVE>(&scheduler, socket)?;
        // Retain every task handle; Base dropping a handle requests cancellation.
        connections.push(connection);
    }
}
}

pub async fn tokio_server<const CONNECTION_TASKS: bool>() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    ready(&listener)?;
    listener.set_nonblocking(true)?;
    let listener = tokio::net::TcpListener::from_std(listener)?;
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
                    Either::Right((completed, _)) => connection_finished(completed),
                }
            }
        };
        let (socket, _) = accepted?;
        socket.set_nodelay(true)?;
        connections.push(async move {
            if CONNECTION_TASKS {
                tokio::spawn(tokio_connection(socket)).await?
            } else {
                tokio_connection(socket).await
            }
        });
    }
}

async fn tokio_connection(mut socket: tokio::net::TcpStream) -> Result<()> {
    // Match Base's initialized, reused heap buffer in every topology.
    let mut buffer = vec![0; READ_SIZE];
    loop {
        if socket.read(&mut buffer).await? == 0 {
            return Ok(());
        }
        check_write(socket.write(RESPONSE).await?)?;
    }
}

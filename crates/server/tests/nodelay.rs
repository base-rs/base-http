// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

// Real TCP connections served through a scheduler that reads back each socket's TCP_NODELAY before closing it.
use base_http::{
    InterimHandler, InterimResponses, Request, Response, Status,
    body::{BufferedBody, ReadableBody},
};
use base_http_server::{Handler, Options};
use base_scheduler::{
    BufferPool, BufferPoolOptions, BufferPooling, Cancellation, IoBuf, OperationResult,
    SocketAccept, SocketAddress, SocketClose, SocketGetOption, SocketOperation, SocketSetOption,
    SocketStreamReader, SocketStreamReading, SocketStreamWriter, SocketStreamWriting, TcpNoDelay,
};
use base_scheduler_direct::{DirectScheduler, DirectSocket};
use futures_lite::future::block_on;
use std::{
    future::Future,
    io::{self, Read, Write},
    sync::Mutex,
};

mod support;
use support::*;

struct Recording<'a> {
    inner: &'a DirectScheduler,
    nodelay: Mutex<Vec<bool>>,
}
impl SocketOperation for Recording<'_> {
    type Socket = DirectSocket;
}
impl BufferPooling for Recording<'_> {
    type Pool = <DirectScheduler as BufferPooling>::Pool;
    fn buffer_pool(&self, options: BufferPoolOptions) -> io::Result<Self::Pool> {
        self.inner.buffer_pool(options)
    }
}
impl SocketAccept for Recording<'_> {
    fn socket_accept(
        &self,
        socket: &DirectSocket,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<(DirectSocket, SocketAddress)>> + Send {
        self.inner.socket_accept(socket, cancellation)
    }
}
impl SocketSetOption<TcpNoDelay> for Recording<'_> {
    fn socket_set_option(
        &self,
        socket: &DirectSocket,
        option: TcpNoDelay,
        value: bool,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send {
        self.inner
            .socket_set_option(socket, option, value, cancellation)
    }
}
impl SocketClose for Recording<'_> {
    async fn socket_close(&self, socket: DirectSocket) -> io::Result<()> {
        let nodelay = self
            .inner
            .socket_get_option(&socket, TcpNoDelay, &Cancellation::never())
            .await
            .unwrap();
        self.nodelay.lock().unwrap().push(nodelay);
        self.inner.socket_close(socket).await
    }
    fn socket_close_submit(
        &self,
        socket: DirectSocket,
    ) -> impl Future<Output = io::Result<()>> + Send {
        self.inner.socket_close_submit(socket)
    }
}
impl<'a> SocketStreamReading for Recording<'a> {
    fn socket_stream_reader<'scheduler>(
        &'scheduler self,
        socket: &DirectSocket,
    ) -> io::Result<
        impl SocketStreamReader<Buffer = <Self::Pool as BufferPool>::Buffer>
        + 'scheduler
        + use<'scheduler, 'a>,
    > {
        self.inner.socket_stream_reader(socket)
    }
    fn socket_stream_reader_with_pool<'scheduler>(
        &'scheduler self,
        socket: &DirectSocket,
        pool: Self::Pool,
    ) -> io::Result<
        impl SocketStreamReader<Buffer = <Self::Pool as BufferPool>::Buffer>
        + 'scheduler
        + use<'scheduler, 'a>,
    > {
        self.inner.socket_stream_reader_with_pool(socket, pool)
    }
}
impl<'a> SocketStreamWriting for Recording<'a> {
    fn socket_stream_writer<'scheduler>(
        &'scheduler self,
        socket: &DirectSocket,
    ) -> io::Result<
        impl SocketStreamWriter<Buffer = <Self::Pool as BufferPool>::Buffer>
        + 'scheduler
        + use<'scheduler, 'a>,
    > {
        self.inner.socket_stream_writer(socket)
    }
    fn socket_stream_writer_with_buffer<'scheduler, B: IoBuf>(
        &'scheduler self,
        socket: &DirectSocket,
    ) -> io::Result<impl SocketStreamWriter<Buffer = B> + 'scheduler + use<'scheduler, 'a, B>> {
        self.inner.socket_stream_writer_with_buffer::<B>(socket)
    }
}

struct Hello;
impl Handler for Hello {
    type Error = io::Error;
    type Body<B: ReadableBody> = BufferedBody;
    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        _: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<Response<BufferedBody>, io::Error> {
        Ok(Response::new(
            Status::OK,
            BufferedBody::from(b"hello".to_vec()),
        ))
    }
}

// Serve one connection, returning TCP_NODELAY for each closed socket: the accepted connection, then the listener.
fn served_nodelay(options: Options) -> Vec<bool> {
    let scheduler = scheduler();
    let recording = Recording {
        inner: &scheduler,
        nodelay: Mutex::default(),
    };
    block_on(async {
        let (listener, address) = listener(&scheduler).await;
        let cancellation = Cancellation::new();
        let peer = {
            let cancellation = cancellation.clone();
            std::thread::spawn(move || {
                let mut peer = connect(address);
                peer.get_mut()
                    .write_all(b"GET / HTTP/1.1\r\nHost: example\r\nConnection: close\r\n\r\n")
                    .unwrap();
                let mut response = Vec::new();
                peer.read_to_end(&mut response).unwrap();
                cancellation.cancel();
                response
            })
        };
        let statistics =
            base_http_server::serve(&recording, listener, &Hello, &options, &cancellation)
                .await
                .unwrap();
        assert_eq!((statistics.connections, statistics.requests), (1, 1));
        assert!(peer.join().unwrap().ends_with(b"\r\n\r\nhello"));
    });
    block_on(scheduler.shutdown());
    recording.nodelay.into_inner().unwrap()
}

#[test]
fn accepted_connections_disable_nagle_unless_configured_otherwise() {
    assert_eq!(served_nodelay(Options::default()), [true, false]);
    let options = Options {
        tcp_nodelay: false,
        ..Default::default()
    };
    assert_eq!(served_nodelay(options), [false, false]);
}

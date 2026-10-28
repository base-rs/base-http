// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{NativeScheduler, Selector};
use base_scheduler::*;
use std::{
    io,
    io::SeekFrom,
    net::{Shutdown, TcpListener, TcpStream},
    path::Path,
};

impl<S: Selector + FileOperation> FileOperation for NativeScheduler<S> {
    type File = S::File;
}

impl<S: Selector + FileOpen> FileOpen for NativeScheduler<S> {
    async fn file_open(
        &self,
        path: &Path,
        options: FileOptions,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::File> {
        self.selector().file_open(path, options, cancellation).await
    }
}

impl<S: Selector + FileClose> FileClose for NativeScheduler<S> {
    async fn file_close(&self, file: Self::File) -> std::io::Result<()> {
        self.selector().file_close(file).await
    }
    async fn file_close_submit(&self, file: Self::File) -> std::io::Result<()> {
        self.selector().file_close_submit(file).await
    }
}

impl<S: Selector + FileSeek> FileSeek for NativeScheduler<S> {
    async fn file_seek(
        &self,
        file: &Self::File,
        position: SeekFrom,
        cancellation: &Cancellation,
    ) -> OperationResult<u64> {
        self.selector()
            .file_seek(file, position, cancellation)
            .await
    }
}

impl<S: Selector + FileTruncate> FileTruncate for NativeScheduler<S> {
    async fn file_truncate(
        &self,
        file: &Self::File,
        length: u64,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.selector()
            .file_truncate(file, length, cancellation)
            .await
    }
}

impl<S: Selector + FileSync> FileSync for NativeScheduler<S> {
    async fn file_sync(
        &self,
        file: &Self::File,
        mode: SyncMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.selector().file_sync(file, mode, cancellation).await
    }
}

impl<S: Selector + FileStat> FileStat for NativeScheduler<S> {
    type Metadata = S::Metadata;
    async fn file_stat(
        &self,
        file: &Self::File,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Metadata> {
        self.selector().file_stat(file, cancellation).await
    }
}

impl<S: Selector + FileAllocate> FileAllocate for NativeScheduler<S> {
    async fn file_allocate(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        mode: AllocationMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.selector()
            .file_allocate(file, offset, length, mode, cancellation)
            .await
    }
}

impl<S: Selector + FileAdvice> FileAdvice for NativeScheduler<S> {
    async fn file_advise(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        advice: AdviceMode,
    ) -> io::Result<()> {
        self.selector()
            .file_advise(file, offset, length, advice)
            .await
    }
    async fn file_advise_submit(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        advice: AdviceMode,
    ) -> io::Result<()> {
        self.selector()
            .file_advise_submit(file, offset, length, advice)
            .await
    }
}

impl<S: Selector + SocketOperation> SocketOperation for NativeScheduler<S> {
    type Socket = S::Socket;
}

impl<S: Selector + SocketOpen> SocketOpen for NativeScheduler<S> {
    async fn socket_open(
        &self,
        domain: SocketDomain,
        kind: SocketType,
        protocol: Option<SocketProtocol>,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Socket> {
        self.selector()
            .socket_open(domain, kind, protocol, cancellation)
            .await
    }
}

impl<S: Selector + SocketBind> SocketBind for NativeScheduler<S> {
    async fn socket_bind(
        &self,
        socket: &Self::Socket,
        address: SocketAddress,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.selector()
            .socket_bind(socket, address, cancellation)
            .await
    }
}

impl<S: Selector + SocketListen> SocketListen for NativeScheduler<S> {
    async fn socket_listen(
        &self,
        socket: &Self::Socket,
        backlog: i32,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.selector()
            .socket_listen(socket, backlog, cancellation)
            .await
    }
}

impl<S: Selector + SocketConnect> SocketConnect for NativeScheduler<S> {
    async fn socket_connect(
        &self,
        socket: &Self::Socket,
        address: SocketAddress,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.selector()
            .socket_connect(socket, address, cancellation)
            .await
    }
}

impl<S: Selector + SocketAccept> SocketAccept for NativeScheduler<S> {
    async fn socket_accept(
        &self,
        socket: &Self::Socket,
        cancellation: &Cancellation,
    ) -> OperationResult<(Self::Socket, SocketAddress)> {
        self.selector().socket_accept(socket, cancellation).await
    }
}

impl<S: Selector + SocketLocalAddress> SocketLocalAddress for NativeScheduler<S> {
    fn socket_local_address(&self, socket: &Self::Socket) -> io::Result<SocketAddress> {
        self.selector().socket_local_address(socket)
    }
}

impl<S: Selector + SocketPeerAddress> SocketPeerAddress for NativeScheduler<S> {
    fn socket_peer_address(&self, socket: &Self::Socket) -> io::Result<SocketAddress> {
        self.selector().socket_peer_address(socket)
    }
}

impl<S: Selector + SocketClose> SocketClose for NativeScheduler<S> {
    async fn socket_close(&self, socket: Self::Socket) -> io::Result<()> {
        self.selector().socket_close(socket).await
    }
    async fn socket_close_submit(&self, socket: Self::Socket) -> io::Result<()> {
        self.selector().socket_close_submit(socket).await
    }
}

impl<S: Selector + SocketShutdown> SocketShutdown for NativeScheduler<S> {
    async fn socket_shutdown(&self, socket: &Self::Socket, how: Shutdown) -> io::Result<()> {
        self.selector().socket_shutdown(socket, how).await
    }
    async fn socket_shutdown_submit(&self, socket: &Self::Socket, how: Shutdown) -> io::Result<()> {
        self.selector().socket_shutdown_submit(socket, how).await
    }
}

impl<S: Selector + SocketRegistration> SocketRegistration for NativeScheduler<S> {
    fn register_socket(&self, socket: TcpStream) -> io::Result<Self::Socket> {
        self.selector().register_socket(socket)
    }
    fn register_listener(&self, listener: TcpListener) -> io::Result<Self::Socket> {
        self.selector().register_listener(listener)
    }
}

impl<S: Selector + SocketGetOption<O>, O: SocketOption> SocketGetOption<O> for NativeScheduler<S> {
    async fn socket_get_option(
        &self,
        socket: &Self::Socket,
        option: O,
        cancellation: &Cancellation,
    ) -> OperationResult<O::Value> {
        self.selector()
            .socket_get_option(socket, option, cancellation)
            .await
    }
}

impl<S: Selector + SocketSetOption<O>, O: SocketOption> SocketSetOption<O> for NativeScheduler<S> {
    async fn socket_set_option(
        &self,
        socket: &Self::Socket,
        option: O,
        value: O::Value,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.selector()
            .socket_set_option(socket, option, value, cancellation)
            .await
    }
}

impl<S: Selector + AddressResolution> AddressResolution for NativeScheduler<S> {
    type Addresses = S::Addresses;
    async fn address_resolve(
        &self,
        name: Option<&str>,
        service: AddressService<'_>,
        hints: AddressHints,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Addresses> {
        self.selector()
            .address_resolve(name, service, hints, cancellation)
            .await
    }
}

impl<S: Selector + Blocking> Blocking for NativeScheduler<S> {
    type Handle<Output: Send + 'static> = S::Handle<Output>;
    async fn spawn_blocking<F, Output>(
        &self,
        operation: F,
        cancellation: &Cancellation,
    ) -> Result<Self::Handle<Output>, BlockingError>
    where
        F: FnOnce() -> Output + Send + 'static,
        Output: Send + 'static,
    {
        self.selector()
            .spawn_blocking(operation, cancellation)
            .await
    }
}

impl<S: Selector + Signals> Signals for NativeScheduler<S> {
    type Signal = S::Signal;
    type Subscription = S::Subscription;
    fn signal_subscribe<Target>(
        &self,
        signals: &[Self::Signal],
        target: Target,
    ) -> io::Result<Self::Subscription>
    where
        Target: Notify<Self::Signal> + 'static,
    {
        self.selector().signal_subscribe(signals, target)
    }
}

impl<S: Selector + BufferPooling> BufferPooling for NativeScheduler<S> {
    type Pool = S::Pool;
    fn buffer_pool(&self, options: BufferPoolOptions) -> io::Result<Self::Pool> {
        self.selector().buffer_pool(options)
    }
}

impl<S: Selector + SocketStreamReading> SocketStreamReading for NativeScheduler<S>
where
    S::Socket: 'static,
{
    fn socket_stream_reader<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<impl SocketStreamReader<Buffer = <Self::Pool as BufferPool>::Buffer> + 'scheduler>
    where
        Self::Socket: 'static,
    {
        self.selector().socket_stream_reader(socket)
    }

    fn socket_stream_reader_with_pool<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
        pool: Self::Pool,
    ) -> io::Result<impl SocketStreamReader<Buffer = <Self::Pool as BufferPool>::Buffer> + 'scheduler>
    where
        Self::Socket: 'static,
    {
        self.selector().socket_stream_reader_with_pool(socket, pool)
    }
}

impl<S: Selector + SocketPacketReading> SocketPacketReading for NativeScheduler<S>
where
    S::Socket: 'static,
{
    fn socket_packet_reader<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<
        impl SocketPacketReader<
            Message: SocketRecvMessageMut<Payload = <Self::Pool as BufferPool>::Buffer>,
        > + 'scheduler,
    >
    where
        Self::Socket: 'static,
    {
        self.selector().socket_packet_reader(socket)
    }

    fn socket_packet_reader_with_pool<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
        pool: Self::Pool,
    ) -> io::Result<
        impl SocketPacketReader<
            Message: SocketRecvMessageMut<Payload = <Self::Pool as BufferPool>::Buffer>,
        > + 'scheduler,
    >
    where
        Self::Socket: 'static,
    {
        self.selector().socket_packet_reader_with_pool(socket, pool)
    }
}

impl<S: Selector + SocketPacketWriting> SocketPacketWriting for NativeScheduler<S>
where
    S::Socket: 'static,
{
    fn socket_packet_writer<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<impl SocketPacketWriter + 'scheduler>
    where
        Self::Socket: 'static,
    {
        self.selector().socket_packet_writer(socket)
    }
}

impl<S: Selector + FileReading> FileReading for NativeScheduler<S> {
    type Reader<'scheduler>
        = S::Reader<'scheduler>
    where
        Self: 'scheduler;
    fn file_reader(&self, file: &Self::File) -> io::Result<Self::Reader<'_>> {
        self.selector().file_reader(file)
    }
}

impl<S: Selector + FileWriting> FileWriting for NativeScheduler<S> {
    type Writer<'scheduler>
        = S::Writer<'scheduler>
    where
        Self: 'scheduler;
    fn file_writer(&self, file: &Self::File) -> io::Result<Self::Writer<'_>> {
        self.selector().file_writer(file)
    }
}

impl<S: Selector + SocketStreamWriting> SocketStreamWriting for NativeScheduler<S> {
    fn socket_stream_writer<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<impl SocketStreamWriter<Buffer = <Self::Pool as BufferPool>::Buffer> + 'scheduler>
    where
        Self::Socket: 'static,
    {
        self.selector().socket_stream_writer(socket)
    }

    fn socket_stream_writer_with_buffer<'scheduler, B: IoBuf>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<impl SocketStreamWriter<Buffer = B> + 'scheduler>
    where
        Self::Socket: 'static,
    {
        self.selector().socket_stream_writer_with_buffer(socket)
    }
}

impl<S: Selector + ProcessWait> ProcessWait for NativeScheduler<S> {
    type ProcessId = S::ProcessId;
    async fn process_wait(
        &self,
        process: Self::ProcessId,
        flags: ProcessWaitFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<Option<ProcessWaitResult>> {
        self.selector()
            .process_wait(process, flags, cancellation)
            .await
    }
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::IoUringSelector;
use base_scheduler::*;
use base_scheduler_selector::{
    BlockingTask, NativeFile, NativeSocket, NativeSubscription, SystemAddresses, SystemFileMetadata,
};
use std::{
    io,
    io::SeekFrom,
    net::{Shutdown, TcpListener, TcpStream},
    path::Path,
};

impl FileOperation for IoUringSelector {
    type File = NativeFile;
}

impl FileOpen for IoUringSelector {
    async fn file_open(
        &self,
        path: &Path,
        options: FileOptions,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::File> {
        self.fallback.file_open(path, options, cancellation).await
    }
}

impl FileClose for IoUringSelector {
    async fn file_close(&self, file: Self::File) -> std::io::Result<()> {
        self.fallback.file_close(file).await
    }
    async fn file_close_submit(&self, file: Self::File) -> std::io::Result<()> {
        self.fallback.file_close_submit(file).await
    }
}

impl FileSeek for IoUringSelector {
    async fn file_seek(
        &self,
        file: &Self::File,
        position: SeekFrom,
        cancellation: &Cancellation,
    ) -> OperationResult<u64> {
        self.fallback.file_seek(file, position, cancellation).await
    }
}

impl FileTruncate for IoUringSelector {
    async fn file_truncate(
        &self,
        file: &Self::File,
        length: u64,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.fallback
            .file_truncate(file, length, cancellation)
            .await
    }
}

impl FileSync for IoUringSelector {
    async fn file_sync(
        &self,
        file: &Self::File,
        mode: SyncMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.fallback.file_sync(file, mode, cancellation).await
    }
}

impl FileStat for IoUringSelector {
    type Metadata = SystemFileMetadata;
    async fn file_stat(
        &self,
        file: &Self::File,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Metadata> {
        self.fallback.file_stat(file, cancellation).await
    }
}

impl FileAllocate for IoUringSelector {
    async fn file_allocate(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        mode: AllocationMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.fallback
            .file_allocate(file, offset, length, mode, cancellation)
            .await
    }
}

impl FileAdvice for IoUringSelector {
    async fn file_advise(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        advice: AdviceMode,
    ) -> io::Result<()> {
        self.fallback
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
        self.fallback
            .file_advise_submit(file, offset, length, advice)
            .await
    }
}

impl SocketOperation for IoUringSelector {
    type Socket = NativeSocket;
}

impl SocketOpen for IoUringSelector {
    async fn socket_open(
        &self,
        domain: SocketDomain,
        kind: SocketType,
        protocol: Option<SocketProtocol>,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Socket> {
        self.fallback
            .socket_open(domain, kind, protocol, cancellation)
            .await
    }
}

impl SocketBind for IoUringSelector {
    async fn socket_bind(
        &self,
        socket: &Self::Socket,
        address: SocketAddress,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.fallback
            .socket_bind(socket, address, cancellation)
            .await
    }
}

impl SocketListen for IoUringSelector {
    async fn socket_listen(
        &self,
        socket: &Self::Socket,
        backlog: i32,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.fallback
            .socket_listen(socket, backlog, cancellation)
            .await
    }
}

impl SocketConnect for IoUringSelector {
    async fn socket_connect(
        &self,
        socket: &Self::Socket,
        address: SocketAddress,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.fallback
            .socket_connect(socket, address, cancellation)
            .await
    }
}

impl SocketAccept for IoUringSelector {
    async fn socket_accept(
        &self,
        socket: &Self::Socket,
        cancellation: &Cancellation,
    ) -> OperationResult<(Self::Socket, SocketAddress)> {
        self.fallback.socket_accept(socket, cancellation).await
    }
}

impl SocketLocalAddress for IoUringSelector {
    fn socket_local_address(&self, socket: &Self::Socket) -> io::Result<SocketAddress> {
        self.fallback.socket_local_address(socket)
    }
}

impl SocketPeerAddress for IoUringSelector {
    fn socket_peer_address(&self, socket: &Self::Socket) -> io::Result<SocketAddress> {
        self.fallback.socket_peer_address(socket)
    }
}

impl SocketClose for IoUringSelector {
    async fn socket_close(&self, socket: Self::Socket) -> io::Result<()> {
        self.fallback.socket_close(socket).await
    }
    async fn socket_close_submit(&self, socket: Self::Socket) -> io::Result<()> {
        self.fallback.socket_close_submit(socket).await
    }
}

impl SocketShutdown for IoUringSelector {
    async fn socket_shutdown(&self, socket: &Self::Socket, how: Shutdown) -> io::Result<()> {
        self.fallback.socket_shutdown(socket, how).await
    }
    async fn socket_shutdown_submit(&self, socket: &Self::Socket, how: Shutdown) -> io::Result<()> {
        self.fallback.socket_shutdown_submit(socket, how).await
    }
}

impl SocketRegistration for IoUringSelector {
    fn register_socket(&self, socket: TcpStream) -> io::Result<Self::Socket> {
        self.fallback.register_socket(socket)
    }
    fn register_listener(&self, listener: TcpListener) -> io::Result<Self::Socket> {
        self.fallback.register_listener(listener)
    }
}

impl<O: SocketOption> SocketGetOption<O> for IoUringSelector
where
    base_scheduler_selector::ReadinessSelector: SocketGetOption<O>,
{
    async fn socket_get_option(
        &self,
        socket: &Self::Socket,
        option: O,
        cancellation: &Cancellation,
    ) -> OperationResult<O::Value> {
        self.fallback
            .socket_get_option(socket, option, cancellation)
            .await
    }
}

impl<O: SocketOption> SocketSetOption<O> for IoUringSelector
where
    base_scheduler_selector::ReadinessSelector: SocketSetOption<O>,
{
    async fn socket_set_option(
        &self,
        socket: &Self::Socket,
        option: O,
        value: O::Value,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.fallback
            .socket_set_option(socket, option, value, cancellation)
            .await
    }
}

impl AddressResolution for IoUringSelector {
    type Addresses = SystemAddresses;
    async fn address_resolve(
        &self,
        name: Option<&str>,
        service: AddressService<'_>,
        hints: AddressHints,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Addresses> {
        self.fallback
            .address_resolve(name, service, hints, cancellation)
            .await
    }
}

impl Blocking for IoUringSelector {
    type Handle<Output: Send + 'static> = BlockingTask<Output>;
    async fn spawn_blocking<F, Output>(
        &self,
        operation: F,
        cancellation: &Cancellation,
    ) -> Result<Self::Handle<Output>, BlockingError>
    where
        F: FnOnce() -> Output + Send + 'static,
        Output: Send + 'static,
    {
        self.fallback.spawn_blocking(operation, cancellation).await
    }
}

impl Signals for IoUringSelector {
    type Signal = i32;
    type Subscription = NativeSubscription;
    fn signal_subscribe<Target>(
        &self,
        signals: &[Self::Signal],
        target: Target,
    ) -> io::Result<Self::Subscription>
    where
        Target: Notify<Self::Signal> + 'static,
    {
        self.fallback.signal_subscribe(signals, target)
    }
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#[path = "support/socket_model.rs"]
mod socket_model;
mod support;
use base_scheduler::*;
use base_scheduler_conformance::{
    SocketOptionFixture, SocketRegistrationFixture, socket_options, socket_registration,
};
use futures_lite::future::block_on;
use socket_model::{SocketModel, socket_factories};
use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

// A test-only, synchronous native adapter. These exchanges arrange traffic before reads; this is not a production scheduler or a certificate for direct.
#[derive(Clone, Copy, PartialEq)]
enum Fault {
    None,
    IgnoreCancellation,
    ConstantGetter,
    IgnoreSetter,
    LoseError,
    RejectStream,
    WrongPeer,
}
struct Native {
    calls: AtomicUsize,
    fault: Fault,
}
impl Native {
    fn new(fault: Fault) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            fault,
        }
    }
}
#[derive(Clone)]
enum Socket {
    Stream(Arc<Mutex<TcpStream>>),
    Listener(Arc<TcpListener>),
    Failing,
}
impl Socket {
    fn stream(&self) -> io::Result<&Mutex<TcpStream>> {
        match self {
            Self::Stream(stream) => Ok(stream),
            _ => Err(io::Error::new(io::ErrorKind::NotConnected, "not a stream")),
        }
    }
}
impl SocketOperation for Native {
    type Socket = Socket;
}
impl SocketGetOption<TcpNoDelay> for Native {
    async fn socket_get_option(
        &self,
        socket: &Socket,
        _: TcpNoDelay,
        cancellation: &Cancellation,
    ) -> OperationResult<bool> {
        if self.fault != Fault::IgnoreCancellation {
            cancellation.check()?;
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fault == Fault::ConstantGetter
            || (self.fault == Fault::LoseError && matches!(socket, Socket::Failing))
        {
            return Ok(false);
        }
        Ok(socket.stream()?.lock().unwrap().nodelay()?)
    }
}
impl SocketSetOption<TcpNoDelay> for Native {
    async fn socket_set_option(
        &self,
        socket: &Socket,
        _: TcpNoDelay,
        value: bool,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        if self.fault != Fault::IgnoreCancellation {
            cancellation.check()?;
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fault == Fault::IgnoreSetter
            || (self.fault == Fault::LoseError && matches!(socket, Socket::Failing))
        {
            return Ok(());
        }
        socket.stream()?.lock().unwrap().set_nodelay(value)?;
        Ok(())
    }
}
impl SocketRegistration for Native {
    fn register_socket(&self, socket: TcpStream) -> io::Result<Socket> {
        if self.fault == Fault::RejectStream {
            return Ok(Socket::Failing);
        }
        Ok(Socket::Stream(Arc::new(Mutex::new(socket))))
    }
    fn register_listener(&self, listener: TcpListener) -> io::Result<Socket> {
        Ok(Socket::Listener(Arc::new(listener)))
    }
}

impl SocketModel for Native {
    fn recv(
        &self,
        socket: &Socket,
        buffer: &mut [u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        cancellation.check()?;
        Ok(socket.stream()?.lock().unwrap().read(buffer)?)
    }
    fn send(
        &self,
        socket: &Socket,
        buffer: &[u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        cancellation.check()?;
        Ok(socket.stream()?.lock().unwrap().write(buffer)?)
    }
}
socket_factories!(Native);
impl SocketAccept for Native {
    async fn socket_accept(
        &self,
        socket: &Socket,
        cancellation: &Cancellation,
    ) -> OperationResult<(Socket, SocketAddress)> {
        cancellation.check()?;
        let Socket::Listener(listener) = socket else {
            return Err(io::Error::other("not a listener").into());
        };
        let (stream, address) = listener.accept()?;
        Ok((Socket::Stream(Arc::new(Mutex::new(stream))), address))
    }
}
impl SocketClose for Native {
    async fn socket_close(&self, socket: Socket) -> io::Result<()> {
        drop(socket);
        Ok(())
    }
    async fn socket_close_submit(&self, socket: Socket) -> io::Result<()> {
        drop(socket);
        Ok(())
    }
}
impl SocketLocalAddress for Native {
    fn socket_local_address(&self, socket: &Socket) -> io::Result<SocketAddress> {
        match socket {
            Socket::Stream(stream) => stream.lock().unwrap().local_addr(),
            Socket::Listener(listener) => listener.local_addr(),
            _ => Err(io::Error::other("invalid socket")),
        }
    }
}
impl SocketPeerAddress for Native {
    fn socket_peer_address(&self, socket: &Socket) -> io::Result<SocketAddress> {
        if self.fault == Fault::WrongPeer {
            return self.socket_local_address(socket);
        }
        socket.stream()?.lock().unwrap().peer_addr()
    }
}
trait Calls: SocketOperation<Socket = Socket> {
    fn calls(&self) -> usize;
}
impl Calls for Native {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}
struct GetterOnly(Native);
impl From<Native> for GetterOnly {
    fn from(native: Native) -> Self {
        Self(native)
    }
}
impl SocketOperation for GetterOnly {
    type Socket = Socket;
}
impl Calls for GetterOnly {
    fn calls(&self) -> usize {
        self.0.calls()
    }
}
impl SocketGetOption<TcpNoDelay> for GetterOnly {
    async fn socket_get_option(
        &self,
        socket: &Socket,
        option: TcpNoDelay,
        token: &Cancellation,
    ) -> OperationResult<bool> {
        self.0.socket_get_option(socket, option, token).await
    }
}
struct SetterOnly(Native);
impl From<Native> for SetterOnly {
    fn from(native: Native) -> Self {
        Self(native)
    }
}
impl SocketOperation for SetterOnly {
    type Socket = Socket;
}
impl Calls for SetterOnly {
    fn calls(&self) -> usize {
        self.0.calls()
    }
}
impl SocketSetOption<TcpNoDelay> for SetterOnly {
    async fn socket_set_option(
        &self,
        socket: &Socket,
        option: TcpNoDelay,
        value: bool,
        token: &Cancellation,
    ) -> OperationResult<()> {
        self.0.socket_set_option(socket, option, value, token).await
    }
}
struct OptionsFixture<S> {
    scheduler: S,
    socket: Socket,
    failing: Socket,
    _peer: TcpStream,
}
impl<S: Calls + From<Native>> OptionsFixture<S> {
    fn new(fault: Fault) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (stream, _) = listener.accept().unwrap();
        Self {
            scheduler: Native::new(fault).into(),
            socket: Socket::Stream(Arc::new(Mutex::new(stream))),
            failing: Socket::Failing,
            _peer: peer,
        }
    }
}
impl<S: Calls> SocketOptionFixture for OptionsFixture<S> {
    type Scheduler = S;
    type Option = TcpNoDelay;
    fn scheduler(&self) -> &S {
        &self.scheduler
    }
    fn socket(&self) -> &Socket {
        &self.socket
    }
    fn failing_socket(&self) -> &Socket {
        &self.failing
    }
    fn error_kind(&self) -> io::ErrorKind {
        io::ErrorKind::NotConnected
    }
    fn option(&self) -> TcpNoDelay {
        TcpNoDelay
    }
    fn values(&self) -> [bool; 2] {
        [false, true]
    }
    fn set_native(&self, value: bool) {
        self.socket
            .stream()
            .unwrap()
            .lock()
            .unwrap()
            .set_nodelay(value)
            .unwrap();
    }
    fn get_native(&self) -> bool {
        self.socket
            .stream()
            .unwrap()
            .lock()
            .unwrap()
            .nodelay()
            .unwrap()
    }
    fn calls(&self) -> usize {
        self.scheduler.calls()
    }
}
fn run_get(test: impl AsyncFnOnce(&OptionsFixture<GetterOnly>) + Send + 'static) {
    support::run(move || block_on(test(&OptionsFixture::new(Fault::None))));
}
fn run_set(test: impl AsyncFnOnce(&OptionsFixture<SetterOnly>) + Send + 'static) {
    support::run(move || block_on(test(&OptionsFixture::new(Fault::None))));
}
mod getter_only {
    base_scheduler_conformance::socket_get_option_tests!(run: super::run_get);
}
mod setter_only {
    base_scheduler_conformance::socket_set_option_tests!(run: super::run_set);
}
struct RegistrationFixture {
    scheduler: Native,
    domain: SocketDomain,
}
impl SocketRegistrationFixture for RegistrationFixture {
    type Scheduler = Native;
    fn scheduler(&self) -> &Native {
        &self.scheduler
    }
    fn domain(&self) -> SocketDomain {
        self.domain
    }
}
fn run_ipv4(test: impl AsyncFnOnce(&RegistrationFixture) + Send + 'static) {
    support::run(move || {
        block_on(test(&RegistrationFixture {
            scheduler: Native::new(Fault::None),
            domain: SocketDomain::Ipv4,
        }))
    });
}
fn run_ipv6(test: impl AsyncFnOnce(&RegistrationFixture) + Send + 'static) {
    support::run(move || {
        block_on(test(&RegistrationFixture {
            scheduler: Native::new(Fault::None),
            domain: SocketDomain::Ipv6,
        }))
    });
}
mod ipv4 {
    base_scheduler_conformance::socket_registration_tests!(run: super::run_ipv4);
}
mod ipv6 {
    base_scheduler_conformance::socket_registration_tests!(run: super::run_ipv6);
}
#[test]
fn rejects_socket_extension_contract_violations() {
    for fault in [
        Fault::IgnoreCancellation,
        Fault::ConstantGetter,
        Fault::IgnoreSetter,
        Fault::LoseError,
        Fault::RejectStream,
        Fault::WrongPeer,
    ] {
        support::run(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                block_on(async {
                    match fault {
                        Fault::RejectStream | Fault::WrongPeer => {
                            socket_registration::stream(&Native::new(fault), SocketDomain::Ipv4)
                                .await
                        }
                        Fault::ConstantGetter => {
                            socket_options::get(&OptionsFixture::<Native>::new(fault)).await
                        }
                        Fault::IgnoreSetter => {
                            socket_options::set(&OptionsFixture::<Native>::new(fault)).await
                        }
                        Fault::LoseError => {
                            socket_options::get(&OptionsFixture::<Native>::new(fault)).await
                        }
                        _ => {
                            socket_options::get_cancellation(&OptionsFixture::<Native>::new(fault))
                                .await
                        }
                    }
                })
            }));
            assert!(
                result.is_err(),
                "socket extension suite accepted an incorrect backend"
            );
        });
    }
}

#[test]
fn rejects_pre_cancelled_setter_side_effects() {
    support::run(|| {
        let fixture = OptionsFixture::<Native>::new(Fault::IgnoreCancellation);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(
                socket_options::set_cancellation(&fixture)
            )))
            .is_err()
        );
    });
}

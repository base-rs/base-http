// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{
    Clock, SocketAccept, SocketBind, SocketClose, SocketConnect, SocketListen, SocketLocalAddress,
    SocketOpen, SocketOperation, SocketSetOption, SocketStreamReading, SocketStreamWriting,
    TcpNoDelay,
};
use base_scheduler_native::{NativeOptions, NativeScheduler};
use futures_lite::future::block_on;
use std::{
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    sync::Arc,
};

/// The scheduler capabilities a fixture needs to listen, serve, connect and bound its teardown.
///
/// Every Base scheduler backend provides them; the blanket implementation makes this an alias.
pub trait Scheduler:
    SocketOpen
    + SocketBind
    + SocketListen
    + SocketLocalAddress
    + SocketAccept
    + SocketConnect
    + SocketClose
    + SocketSetOption<TcpNoDelay>
    + SocketStreamReading
    + SocketStreamWriting
    + Clock
    + SocketOperation<Socket: 'static>
{
}

impl<S> Scheduler for S where
    S: SocketOpen
        + SocketBind
        + SocketListen
        + SocketLocalAddress
        + SocketAccept
        + SocketConnect
        + SocketClose
        + SocketSetOption<TcpNoDelay>
        + SocketStreamReading
        + SocketStreamWriting
        + Clock
        + SocketOperation<Socket: 'static>
{
}

/// Run `work` with a fresh two-worker Native scheduler, then shut it down, also when `work` panics, before propagating the panic.
pub fn native<T>(work: impl FnOnce(&NativeScheduler) -> T) -> T {
    shared_native(|scheduler| work(scheduler))
}

/// Run `work` with a fresh two-worker Native scheduler shared through an [`Arc`], as [`ServerContext::spawn`](crate::ServerContext::spawn) needs, then shut it down as [`native`] does.
pub fn shared_native<T>(work: impl FnOnce(&Arc<NativeScheduler>) -> T) -> T {
    let scheduler = Arc::new(
        NativeScheduler::with_options(NativeOptions {
            workers: 2,
            ..Default::default()
        })
        .expect("construct a Native scheduler"),
    );
    let result = catch_unwind(AssertUnwindSafe(|| work(&scheduler)));
    block_on(scheduler.shutdown());
    result.unwrap_or_else(|panic| resume_unwind(panic))
}

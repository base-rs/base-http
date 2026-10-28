// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// A fixture-owned backend access boundary, independent of the I/O waiter.
///
/// `release` lets an accepted attempt finish; `retired` confirms that no backend access remains and the input has been destroyed. Destruction must also release the attempt if an assertion panics. Fixtures enforce deadlines on waits.
pub trait SocketIoGuard {
    fn release(&mut self);
    fn retired(&mut self) -> impl Future<Output = ()>;
}

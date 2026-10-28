// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Shared resource type for socket operations provided by a scheduler. Implement this trait on the scheduler; all socket capabilities use this one associated type. No concrete OS descriptor or shared-ownership wrapper is required.
///
/// IP socket operations through the chosen scheduler. Each backend supplies one descriptor type for unconnected, listening, and connected sockets. Operations have the same state requirements as their system calls; invalid transitions report OS errors.
///
/// Creation, binding, listening, and connecting are separate operations. All operation futures are lazy. Pre-cancellation prevents a syscall; successful progress wins a cancellation race. Cancelling or abandoning a pending connect does not roll back kernel connection state: discard that socket before trying another connection. Completion backends retain owned buffers until the original operation completes.
///
/// File and socket resources remain distinct. There is no common descriptor trait, protocol buffering, or automatic close coordination. Stream and packet I/O belongs to scheduler-bound reader and writer capabilities. Non-IP families remain an extension.
///
/// Implementations document resource compatibility, including affinity to particular scheduler instances. Successive operations may use different compatible instances. Incompatible pairings must be rejected with `std::io::ErrorKind::InvalidInput` before submission when the type system cannot reject them. Rebinding or conversion is an explicit backend operation. Bound reader and writer handles fix their compatible scheduler at construction; per-operation calls do not rebind them.
///
/// Selecting a socket resource type does not supply reads:
///
/// ```compile_fail,E0599
/// use base_scheduler::{Cancellation, SocketOperation};
/// async fn read<S: SocketOperation>(scheduler: &S, socket: &S::Socket) {
///     scheduler.socket_stream_reader(socket);
/// }
/// ```
pub trait SocketOperation: Send + Sync {
    /// The socket resource accepted by this scheduler's socket operations.
    type Socket: Send + Sync;
}

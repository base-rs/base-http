// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Shared resource type for file operations provided by a scheduler.
///
/// Implement this trait on the scheduler, not the descriptor. All file operation traits use this one associated type, allowing generic consumers to combine capabilities without resource-type equality bounds. Implementations choose their own representation; no OS descriptor or shared-ownership wrapper is required by the interface.
///
/// A submitted operation must retain every resource and buffer it can still access, even if its waiting future is dropped. Applications finish outstanding work before explicitly closing a resource; close never cancels or joins other operations.
///
/// Implementations document resource compatibility, including affinity to particular scheduler instances. Successive operations may use different compatible instances. Incompatible pairings must be rejected with `std::io::ErrorKind::InvalidInput` before submission when the type system cannot reject them. Rebinding or conversion is an explicit backend operation. Bound reader and writer handles fix their compatible scheduler at construction; per-operation calls do not rebind them.
///
/// Selecting a resource type does not supply any operations:
///
/// ```compile_fail,E0599
/// use base_scheduler::{Cancellation, FileOperation};
/// async fn read<S: FileOperation>(scheduler: &S, file: &S::File) {
///     scheduler.file_reader(file);
/// }
/// ```
pub trait FileOperation: Send + Sync {
    /// The file resource accepted by this scheduler's file operations.
    type File: Send + Sync;
}

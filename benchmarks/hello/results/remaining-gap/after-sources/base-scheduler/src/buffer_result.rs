// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// The operation result and original owned buffer, returned on success, error, or cancellation.
///
/// Reads fill the initialized buffer length without resizing it. Only the first successful byte count contains new data; writes may be partial. Abandoning the future forfeits the buffer, which the backend must retain until it can no longer access it.
pub type BufferResult<B = Vec<u8>> = (crate::OperationResult<usize>, B);

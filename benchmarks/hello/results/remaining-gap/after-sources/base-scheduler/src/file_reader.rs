// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{BufferResult, Cancellation, IoBufMut};
use std::{future::Future, io};

/// Scheduler-bound, sequential access to a file's readable direction. Construct through [`FileReading`](crate::FileReading). Separate handles permit overlapping operations, but shared-cursor operations have no ordering guarantee across handles.
pub trait FileReader: Send {
    /// Read once at the file's shared OS cursor, returning a possibly short count and the original owner on every result. Cancellation cannot roll back transferred bytes or cursor changes.
    fn read<B: IoBufMut>(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;

    /// Read once at an explicit offset. Offsets must fit in `i64`. Unix implementations leave the shared cursor unchanged; Windows implementations may use `seek_read`, which changes it. The handle maintains no private cursor.
    fn read_at<B: IoBufMut>(
        &mut self,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;

    /// Await admission separately from collecting the original owner. Accepted work can complete independently of polling the returned future. The fallback completes the ordinary operation before returning; backends can admit independently progressing work earlier. Pre-cancellation rejects admission and returns the unchanged owner.
    fn read_submit<B: IoBufMut>(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
    ) -> impl Future<
        Output = Result<
            impl Future<Output = BufferResult<B>> + Send + 'static,
            (crate::OperationError, B),
        >,
    > + Send {
        async move {
            if let Err(error) = cancellation.check() {
                return Err((error.into(), buffer));
            }
            let result = self.read(buffer, cancellation).await;
            Ok(async move { result })
        }
    }

    /// Await admission separately from collecting the original owner. Accepted work can complete independently of polling the returned future. The fallback completes the ordinary operation before returning; backends can admit independently progressing work earlier. Pre-cancellation rejects admission and returns the unchanged owner.
    fn read_at_submit<B: IoBufMut>(
        &mut self,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> impl Future<
        Output = Result<
            impl Future<Output = BufferResult<B>> + Send + 'static,
            (crate::OperationError, B),
        >,
    > + Send {
        async move {
            if let Err(error) = cancellation.check() {
                return Err((error.into(), buffer));
            }
            let result = self.read_at(buffer, offset, cancellation).await;
            Ok(async move { result })
        }
    }

    /// Stop admission and await accepted operation retirement without cancellation. Repeated close is safe; dropping a pending close is not a retirement barrier. This does not close the underlying file. Drop the retired handle before exclusive file close.
    fn close(&mut self) -> impl Future<Output = io::Result<()>> + Send;
}

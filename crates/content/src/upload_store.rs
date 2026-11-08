// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::Cancellation;
use base_web_content::UploadHead;
use std::{error::Error, fmt, future::Future};

/// Storage for uploads, which [`read_content`](crate::read_content) streams each upload into as it arrives.
///
/// Each upload is created, written in pieces, and then finished, which gives the handle the declared struct holds in its [`Upload`](base_web_content::Upload). An upload that fails, or whose content stops arriving, is aborted, and a finished upload whose content is refused is removed. [`FileStore`](crate::FileStore) stores uploads in files.
///
/// Uploads are stored before the whole body has been validated, so a store must not make an upload visible to anything else until the application accepts it.
pub trait UploadStore: Send {
    /// What the declared struct holds for a stored upload, such as a path.
    type Handle: Send;
    /// An upload being stored.
    type Upload: Send;
    /// Why a store operation failed.
    type Error: Error + Send + Sync + 'static;

    /// Begin storing an upload. When this fails, nothing of the upload is stored.
    ///
    /// The head's filename and media type are what the client declared, and must not choose where the upload is stored.
    fn create(
        &mut self,
        head: &UploadHead,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<Self::Upload, Self::Error>> + Send;

    /// Store some of an upload's content, after what was stored before. When this fails, the caller must abort the upload, as [`read_content`](crate::read_content) does.
    fn write(
        &mut self,
        upload: &mut Self::Upload,
        content: &[u8],
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Complete an upload whose content has all been written, returning its handle. When this fails, the caller must abort the upload, as [`read_content`](crate::read_content) does.
    fn finish(
        &mut self,
        upload: &mut Self::Upload,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<Self::Handle, Self::Error>> + Send;

    /// Remove whatever was stored of an upload that was not finished, or whose finish failed. It is not cancellable, since it cleans up after a failure.
    fn abort(
        &mut self,
        upload: Self::Upload,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Remove a finished upload. It is not cancellable, since it cleans up after a failure.
    fn remove(
        &mut self,
        handle: Self::Handle,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A store for declarations without uploads, which refuses any upload.
///
/// A declaration without uploads accepts handles of every type, so it can be read with this store, whose handle is `()`. If the declaration does have uploads of `Upload<()>`, an upload fails to store with [`NoUploadsError`].
#[derive(Clone, Copy, Debug, Default)]
pub struct NoUploads;

/// An upload reached [`NoUploads`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NoUploadsError;

impl fmt::Display for NoUploadsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("no store was given for uploads")
    }
}

impl Error for NoUploadsError {}

impl UploadStore for NoUploads {
    type Handle = ();
    type Upload = ();
    type Error = NoUploadsError;

    async fn create(&mut self, _: &UploadHead, _: &Cancellation) -> Result<(), NoUploadsError> {
        Err(NoUploadsError)
    }

    async fn write(
        &mut self,
        _: &mut (),
        _: &[u8],
        _: &Cancellation,
    ) -> Result<(), NoUploadsError> {
        Err(NoUploadsError)
    }

    async fn finish(&mut self, _: &mut (), _: &Cancellation) -> Result<(), NoUploadsError> {
        Err(NoUploadsError)
    }

    async fn abort(&mut self, _: ()) -> Result<(), NoUploadsError> {
        Ok(())
    }

    async fn remove(&mut self, _: ()) -> Result<(), NoUploadsError> {
        Ok(())
    }
}

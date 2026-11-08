// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::UploadStore;
use base_scheduler::{Blocking, Cancellation};
use base_web_content::{FieldPath, UploadHead};
use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

/// The content an upload gathers before it is written, so that small pieces do not each need a blocking job.
const WRITE_SIZE: usize = 64 * 1024;

/// Stores each upload in a new file, at a path the application chooses, as `protocol-multipart`'s `Readable#save` does.
///
/// `destination` is given the declared field's path, such as `avatar` or `photos[2]`, and returns the file's path; it is never given the filename or media type the client declared, which must not choose where an upload is stored. Each file is created exclusively, so creating it fails if anything is at the path, including a symbolic link, and on Unix with permissions 0600 unless [`Self::with_permissions`] chooses others; the process's umask may remove more. A path that already exists fails the upload, and the read, with [`io::ErrorKind::AlreadyExists`], so `destination` should return a path that is new, such as one with a random or unique name in a directory only the server can write.
///
/// When writing or finishing an upload fails, or the upload is aborted, its file is removed; a file that is already gone counts as removed. The store only removes files it created, by the paths it created them at, so the directory should not be one that others can write. Files are closed when they finish, but not synchronized to storage. File operations run through the scheduler's [`Blocking`] capability, so that they do not block an async worker, and content is gathered into writes of up to 64 KiB; only when the scheduler refuses to admit blocking work, such as after cancellation, is a file closed on the current thread.
///
/// The handle of a stored upload is its path.
pub struct FileStore<'s, S, D> {
    scheduler: &'s S,
    destination: D,
    #[cfg(unix)]
    permissions: u32,
}

impl<'s, S, D> FileStore<'s, S, D>
where
    S: Blocking + Sync,
    D: FnMut(&FieldPath) -> PathBuf + Send,
{
    /// A store creating files on `scheduler`'s blocking threads, at the paths `destination` gives.
    pub fn new(scheduler: &'s S, destination: D) -> Self {
        Self {
            scheduler,
            destination,
            #[cfg(unix)]
            permissions: 0o600,
        }
    }

    /// Create files with these permissions, as for `open(2)`'s `mode`, rather than 0600.
    #[cfg(unix)]
    pub fn with_permissions(mut self, permissions: u32) -> Self {
        self.permissions = permissions;
        self
    }
}

impl<S, D> fmt::Debug for FileStore<'_, S, D> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("FileStore").finish_non_exhaustive()
    }
}

/// An upload being stored in a file by [`FileStore`].
pub struct FileUpload {
    path: PathBuf,
    /// The open file, until the upload finishes or a write fails.
    file: Option<File>,
    /// Content not yet written.
    pending: Vec<u8>,
}

impl FileUpload {
    /// The path the upload is stored at.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Shows the path, which the application chose, and the size of the content not yet written.
impl fmt::Debug for FileUpload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileUpload")
            .field("path", &self.path)
            .field("pending", &self.pending.len())
            .finish_non_exhaustive()
    }
}

impl<S, D> UploadStore for FileStore<'_, S, D>
where
    S: Blocking + Sync,
    D: FnMut(&FieldPath) -> PathBuf + Send,
{
    type Handle = PathBuf;
    type Upload = FileUpload;
    type Error = io::Error;

    async fn create(
        &mut self,
        head: &UploadHead,
        cancellation: &Cancellation,
    ) -> io::Result<FileUpload> {
        let path = (self.destination)(head.path());
        let mut options = OpenOptions::new();
        // SECURITY: `create_new` is `O_CREAT | O_EXCL`, which fails if anything exists at the path, including a symbolic link, so an upload never writes through a link or into a file the server did not just create.
        options.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, self.permissions);
        let opened = path.clone();
        let file = blocking(self.scheduler, move || options.open(opened), cancellation).await?;
        Ok(FileUpload {
            path,
            file: Some(file),
            pending: Vec::new(),
        })
    }

    async fn write(
        &mut self,
        upload: &mut FileUpload,
        content: &[u8],
        cancellation: &Cancellation,
    ) -> io::Result<()> {
        upload.pending.extend_from_slice(content);
        if upload.pending.len() < WRITE_SIZE {
            return Ok(());
        }
        flush(self.scheduler, upload, cancellation).await
    }

    async fn finish(
        &mut self,
        upload: &mut FileUpload,
        cancellation: &Cancellation,
    ) -> io::Result<PathBuf> {
        if !upload.pending.is_empty() {
            flush(self.scheduler, upload, cancellation).await?;
        }
        // Close the file on a blocking thread, since closing can wait for the file system.
        let file = upload.file.take();
        let close = move || {
            drop(file);
            Ok(())
        };
        blocking(self.scheduler, close, cancellation).await?;
        Ok(upload.path.clone())
    }

    async fn abort(&mut self, upload: FileUpload) -> io::Result<()> {
        let FileUpload { path, file, .. } = upload;
        let abort = move || {
            drop(file);
            remove(&path)
        };
        blocking(self.scheduler, abort, &Cancellation::never()).await
    }

    async fn remove(&mut self, handle: PathBuf) -> io::Result<()> {
        blocking(
            self.scheduler,
            move || remove(&handle),
            &Cancellation::never(),
        )
        .await
    }
}

/// Run file operations on one of `scheduler`'s blocking threads, admitted under `cancellation`.
async fn blocking<S: Blocking + Sync, T: Send + 'static>(
    scheduler: &S,
    operation: impl FnOnce() -> io::Result<T> + Send + 'static,
    cancellation: &Cancellation,
) -> io::Result<T> {
    let handle = scheduler
        .spawn_blocking(operation, cancellation)
        .await
        .map_err(io::Error::other)?;
    // The task error's panic payload cannot be shared between threads, so only its description is kept.
    handle
        .await
        .map_err(|error| io::Error::other(error.to_string()))?
}

/// Write what the upload has gathered, leaving its file closed when this fails.
async fn flush<S: Blocking + Sync>(
    scheduler: &S,
    upload: &mut FileUpload,
    cancellation: &Cancellation,
) -> io::Result<()> {
    // An upload whose write failed has lost its file, and one that finished has closed it.
    let mut file = upload.file.take().ok_or_else(closed)?;
    let pending = std::mem::take(&mut upload.pending);
    let path = upload.path.clone();
    let (file, mut pending) = blocking(
        scheduler,
        move || {
            write_all(&mut file, &pending, &path)?;
            Ok((file, pending))
        },
        cancellation,
    )
    .await?;
    pending.clear();
    upload.file = Some(file);
    upload.pending = pending;
    Ok(())
}

/// The error for writing to an upload that failed or finished.
fn closed() -> io::Error {
    io::Error::other("the upload's file is closed, after it failed or finished")
}

/// Write all of `content` to the file at `path`.
fn write_all(file: &mut File, content: &[u8], path: &Path) -> io::Result<()> {
    #[cfg(test)]
    {
        if faults::check(path, faults::Operation::Panic).is_err() {
            panic!("injected panic");
        }
        faults::check(path, faults::Operation::Write)?;
    }
    #[cfg(not(test))]
    let _ = path;
    file.write_all(content)
}

/// Remove the file at `path`, which counts as removed if it is already gone.
// SECURITY: Removal is by path, so something that replaced the file at that path after it was created would be removed instead. Replacing it needs write access to the application's upload directory, which the store's documentation requires only the server to have; with that access, the replacement could be removed or overwritten directly anyway.
fn remove(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    faults::check(path, faults::Operation::Remove)?;
    match fs::remove_file(path) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

/// Failures injected into file operations by path, for tests of failures the file system cannot be made to produce reliably.
#[cfg(test)]
pub(crate) mod faults {
    use std::{
        io,
        path::{Path, PathBuf},
        sync::Mutex,
    };

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Operation {
        Write,
        Remove,
        /// Panic while writing, as a blocking job that fails.
        Panic,
    }

    static FAULTS: Mutex<Vec<(PathBuf, Operation)>> = Mutex::new(Vec::new());

    /// Fail the next `operation` on `path`.
    pub(crate) fn inject(path: &Path, operation: Operation) {
        FAULTS.lock().unwrap().push((path.to_owned(), operation));
    }

    /// The injected failure of `operation` on `path`, if any, which is then used up.
    pub(crate) fn check(path: &Path, operation: Operation) -> io::Result<()> {
        let mut faults = FAULTS.lock().unwrap();
        let Some(position) = faults
            .iter()
            .position(|(faulty, injected)| faulty == path && *injected == operation)
        else {
            return Ok(());
        };
        faults.remove(position);
        Err(io::Error::other("injected failure"))
    }
}

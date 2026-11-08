# Base HTTP Content

Reads a request's body into a structure declared with [`base-web-content`](https://github.com/base-rs/base-web-content)'s `#[derive(Content)]`, doing the I/O that `base-web-content` leaves to the server: the body is read as it arrives, a `Content-Length` larger than the declaration could accept is refused before any of the body is read, uploads are streamed to storage, and `FileStore` saves them to files safely. It depends only on `base-http` requests and bodies, so a handler can use it with any server.

## Usage

Declare the content, choose where uploads go, and read the request:

```rust
use base_http::{Method, Request, body::BufferedBody};
use base_http_content::{FileStore, ReadErrorKind, read_content};
use base_scheduler::Cancellation;
use base_web_content::{Content, FieldPath, Rejection, Upload};
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};

#[derive(Content)]
struct Profile {
    #[content(size_limit = 256)]
    name: String,
    #[content(accept("image/png", "image/jpeg"), size_limit = 1024 * 1024)]
    avatar: Option<Upload<PathBuf>>,
}

let scheduler = base_scheduler_native::NativeScheduler::new()?;
let uploads = tempfile::tempdir()?;

// A request as a server would give it to a handler.
let body = b"--XyZ\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\nAda\r\n--XyZ\r\nContent-Disposition: form-data; name=\"avatar\"; filename=\"me.png\"\r\nContent-Type: image/png\r\n\r\n\x89PNG\r\n--XyZ--\r\n";
let mut request = Request::new(Method::POST, "/profile", BufferedBody::from(body.to_vec()));
request.fields.add("Content-Type", "multipart/form-data; boundary=XyZ")?;

// Each upload goes to a new file the server names, never to the filename the client sent.
let next = AtomicU64::new(0);
let mut store = FileStore::new(&scheduler, |_: &FieldPath| {
    uploads.path().join(format!("upload-{}", next.fetch_add(1, Ordering::Relaxed)))
});
let cancellation = Cancellation::never();
let read = read_content::<Profile, _, _>(&mut request, &mut store, &cancellation);
match futures_lite::future::block_on(read) {
    Ok(accepted) => {
        let profile = accepted.content;
        let avatar = profile.avatar.expect("an avatar was sent");
        assert_eq!(profile.name, "Ada");
        assert_eq!(std::fs::read(avatar.handle())?, b"\x89PNG");
    }
    // Refused content: show the form again with its violations, or respond with the status.
    Err(error) => match error.kind {
        ReadErrorKind::Rejected(Rejection::Violations(violations)) => panic!("{} invalid fields", violations.len()),
        _ => panic!("respond with {}: {error}", error.status().as_u16()),
    },
}
futures_lite::future::block_on(scheduler.shutdown());
# Ok::<(), Box<dyn std::error::Error>>(())
```

`read_content` refuses a request whose length, from its body or `Content-Length`, exceeds the declaration's `body_size_limit` with 413 before reading any of its body, and a request without a known length as soon as its body exceeds it. Each failure maps to one status, from `ReadError::status`: 413 for a length over the limit, the content error's own 400, 413, or 415, 422 for violations, 400 for a malformed `Content-Type` or a body that fails, and 500 for an upload that fails to store.

When the content is refused, or reading or storing fails, the upload still arriving is aborted and every upload stored so far is removed before the error is returned, and the body is closed when reading stopped before its end. Cleanup is asynchronous, so dropping the read before it completes skips it: stop a read by cancelling its `Cancellation` instead. Accepted content's uploads belong to the application, as do `Accepted::discarded`, uploads that later ones for the same field replaced.

Uploads can go anywhere that implements `UploadStore`. `FileStore` creates each file exclusively, failing if anything, including a symbolic link, is at the path, with permissions 0600 on Unix unless `with_permissions` chooses others, and removes the file when writing it fails or the upload is aborted. Its destination function is given the declared field's path, never the submitted filename or media type. File operations run on the scheduler's blocking threads. A declaration without uploads can be read with `NoUploads`.

See the [design requirements](../../design/content.md) for the behaviour this crate guarantees.

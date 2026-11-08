# Request content

## Requirement

`base-http-content`, a package in this workspace, must read a request's body into a structure declared with `base-web-content`'s `#[derive(Content)]`, doing the I/O that `base-web-content` leaves to the server layer: reading the body, the early `Content-Length` check, streaming uploads to storage, and saving uploads to files safely.

Placement and dependencies:

- The package must depend on `base-http` for requests and bodies, `base-web-content` and `base-web-media-type` for parsing, and `base-scheduler` for cancellation and blocking work. It must not depend on `base-http1`, `base-http-server`, `base-endpoint`, a concrete scheduler, or an async runtime, so that a handler or middleware can use it with any `ReadableBody`, and an HTTP/1-only server does not pull in form parsing.
- No other package in this workspace may depend on it, and `base-web-content` must remain free of I/O.

Reading a body:

- The media type must come from the request's single `Content-Type` field. Without one, `base-web-content` refuses the content as missing a media type. Two or more `Content-Type` fields, or one that is not a valid media type, must be refused with 400 Bad Request without reading the body.
- When the request's length, from its body or its declared `Content-Length`, exceeds the largest body the declaration could need in its format, `ContentParser::body_size_limit`, the request must be refused with 413 Content Too Large before any of the body is read. A length equal to the maximum must be read. A request without a known length, such as a chunked one, must be read and is refused by the parser's running total as soon as it exceeds the maximum.
- The body must be read as it arrives, frame by frame, with each frame given to `ContentParser` before the next is read. The input retained between frames must not exceed the parser's `minimum_buffer_size` plus one frame, so that the whole body is never buffered; a frame must be parsed in place when no earlier input is retained. After the body's last frame, what remains must be given to `parse_end`. Trailers are ignored.
- The result must not depend on how the body is divided into frames, except that an upload's content may reach storage in different pieces.

Uploads and cleanup:

- An upload's content must be streamed to an application-supplied `UploadStore` as the parser reports it, and must not be buffered by the reader beyond the parser's input.
- `ContentParser::finish` must be called however reading ends: after a fatal content error, a body read failure, a storage failure, or cancellation. When the content is refused, or reading or storage fails, the reader must abort the upload whose content was still arriving and remove every upload `Rejected` returns, before returning the error. Cleanup must not be cancellable, and its failures must be returned with the error without replacing its cause.
- The content is accepted exactly when `finish` accepts it. A body that fails after the content is complete, in a multipart epilogue, therefore still yields the content. Accepted content is returned with its `discarded` handles, which belong to the application.
- When reading stops before the body ends, the body must be closed with `ReadableBody::close`, so that the connection is not reused with unread input.
- Dropping the read before it completes skips cleanup, which is asynchronous, and leaves everything it stored in place. `base-http-server` drops a handler's future when the server's cancellation fires, at shutdown, so a read in progress then leaves everything it stored, and a handler still running leaves the uploads it accepted. A client cannot make the server drop a handler, since its disconnection reaches the reader as a body failure, which is cleaned up, but a client that stalls keeps its read in progress until shutdown. This must be documented, with the advice to stage uploads in a directory the application clears when it starts and to move accepted uploads out of it, and that a handler stops a read itself by cancelling a child of its cancellation.

Errors:

- Each failure must map to one status: 413 for a length over the maximum, the `ContentError`'s own status (400, 413, or 415) for a fatal content error, 422 Unprocessable Content for violations, 400 for a malformed `Content-Type` or a failure to read the body, and 500 for a storage failure. A read the server cancelled is a body or storage failure too; its status matters little, since there is usually no one to respond to. Errors and their `Display` and `Debug` output must not include submitted content, filenames, or media types, following `base-web-content`'s `design/errors.md`.

Saving uploads to files:

- `FileStore` must create each file exclusively, failing if anything, including a symbolic link, exists at the path, with permissions 0600 on Unix unless the application chooses other permission bits; the setuid, setgid, and sticky bits must never be set.
- The destination must be chosen by the application, from the declared field's path. It must never be derived from the submitted filename or media type, which the store must not pass to the application's destination function.
- When an upload is aborted, which the reader does whenever writing or finishing it fails, the store must remove the file it created for it. The store must only remove files at the paths it created them at.
- File operations must run through the scheduler's `Blocking` capability, so that they do not block an async worker. The one exception is a file that is dropped where it is when the scheduler refuses to admit the blocking work that would have used it, such as after cancellation, which closes it on the current thread.

## Rationale

These decisions were made with the user on 2026-11-08, when `base-web-content`'s scope (`base-web-content/design/scope.md`) placed reading request bodies, the early `Content-Length` check, and saving uploads in the server layer so that `base-web-content` stays sans-I/O. The user delegated the remaining design to this implementation.

A separate package, rather than a feature of `base-http-server`, follows the [proposal](proposal.md#3-crate-structure): application middleware depends on `base-http` and need not pull in a server, protocol codec, or TLS implementation. The reader needs only a `Request` and a `ReadableBody`, so it works with any server or test body. A package in this workspace keeps the dependency direction from HTTP to the sans-I/O web crates, and is released with the other HTTP packages.

The Ruby equivalent is split between `utopia`'s `Controller::Base#parse_body` (`utopia` 3.0.7, `lib/utopia/controller/base.rb`), which feeds the request body to `protocol-content`'s parser and always closes the input with any error, and `protocol-multipart`'s `Readable#save` (0.7.0, `lib/protocol/multipart/readable.rb`), which opens the destination with `WRONLY | CREAT | EXCL` and permissions 0600 by default, and unlinks the file it created when copying fails. The file store follows `save`; closing a body that was not read to its end follows `parse_body`. `utopia` maps a client-side argument error to 422 (`lib/utopia/http.rb`), which violations follow.

The scheduler's `FileOptions` (`base-scheduler` 0.4) can create a file exclusively but cannot set its permissions, and the scheduler has no capability to remove a file, so the file store uses `std::fs` through `Blocking::spawn_blocking`, which is how the scheduler's own file operations run on its native backends. Moving to scheduler file operations would need those capabilities first.

Cleanup is explicit and asynchronous rather than done in `Drop`, as Base resources are elsewhere: `base-scheduler`'s close operations are explicit, and `base-web-html`'s `design/streaming.md` forbids implicit work when a writer is dropped. A drop guard would also have to block a worker to remove files. `base-http-server` stops polling a handler once its cancellation fires (`cancellable` around `Handler::call` in `crates/server/src/lib.rs`), and timed draining is future policy there, so leftovers at shutdown are left to the application, as temporary upload directories commonly are; letting handlers finish their cleanup during shutdown would be a change to the server. Accepting content once `finish` accepts it, even after a later body failure, follows `parse_body`, which returns once the parser has read the content and closes the input without reading the rest; it also means accepted content, whose uploads cannot be removed generically once inside the declared struct, is never refused after the fact.

## Scope

The `base-http-content` package: its reader functions and options, `UploadStore` and its implementations, and its error type.

## Validation

Inspect `cargo tree -e normal -p base-http-content` for the dependencies above, and the other packages' manifests for no dependency on it.

Tests, on a real scheduler for the file store:

- `Content-Length` at the maximum is read, and one byte over is refused with 413 before any frame is read; a body of unknown length over the maximum, within every field's limits, is refused while parsing as over the body's maximum, with 413.
- The read future is `Send`, and the reader works within a `base-http-server` handler over a real connection, where a client that stops sending in the middle of an upload leaves no file behind.
- Every multipart and URL-encoded test body gives the same content and stored bytes when divided into two frames at every position, and when delivered one byte per frame.
- A missing, repeated, or malformed `Content-Type` is refused without reading the body.
- The upload in progress is aborted and every stored upload removed after a fatal content error, after violations, after a body read failure, after a storage failure in each `UploadStore` operation, and after cancellation; cleanup failures are returned with the cause; the body is closed when reading stops early; a body failure in a multipart epilogue still yields the content.
- `FileStore` refuses an existing file and a symbolic link at the destination, without changing either; creates files with mode 0600, and with the mode the application chooses, on Unix; removes the partial file when a write, the final write, or the upload is aborted, and after an injected failure of each; and never receives the submitted filename.
- 100% region coverage with `cargo bake test:coverage --all-targets true`.

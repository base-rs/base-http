// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{ReadError, ReadErrorKind, UploadStore};
use base_http::{
    Fields, Request,
    body::{BodyFrame, ReadableBody},
};
use base_scheduler::Cancellation;
use base_web_content::{
    Accepted, ContentError, ContentEvent, ContentHandles, ContentParser, ParameterRules, Rejection,
};
use base_web_media_type::MediaType;

/// Options for [`read_content_with`], which may gain more: start from [`ReadOptions::default`] and set those you need.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct ReadOptions {
    /// The rules for reading multipart part names and filenames, rather than those the media type gives, as [`ContentParser::with_parameter_rules`] describes.
    pub parameter_rules: Option<ParameterRules>,
}

/// Read a request's content into the struct `T` declares, streaming its uploads to `store`, with the default [`ReadOptions`].
///
/// The format is chosen by the request's `Content-Type`. A request whose length, from its body or its `Content-Length`, exceeds the largest body `T` could need in that format is refused with [`ReadErrorKind::ContentTooLarge`] before any of its body is read; a request without a known length is refused as soon as its body exceeds it. The body is read as it arrives, retaining only what the parser has not yet consumed, and each upload is streamed to `store` as it arrives.
///
/// When the content is refused, or reading or storing fails, the upload still arriving is aborted and every upload stored so far is removed, without cancellation, before the error is returned; any failures to remove them are in [`ReadError::cleanup`]. When reading stops before the body's end, the body is closed. The content is accepted exactly when [`ContentParser::finish`] accepts it, so a body that fails after the content is complete, in a multipart epilogue, still yields it. Accepted content holds its uploads' handles, which belong to the application, as do those in [`Accepted::discarded`], which later uploads for the same fields replaced; remove those with [`UploadStore::remove`] if they are not wanted.
///
/// Cleanup is asynchronous, so dropping the returned future before it completes skips it, and leaves everything it stored in place. To stop a read yourself, give it a child of the handler's cancellation, from [`Cancellation::child`], and cancel that: the body's read and the store's operations observe it, and the future, awaited, then cleans up. `base-http-server` drops a handler's future when the server's cancellation fires, at shutdown, so a read in progress then leaves everything it stored, and a handler still running leaves the uploads it accepted. Stage uploads in a directory the application clears when it starts, and move accepted uploads out of it. A client cannot make the server drop a handler, since its disconnection reaches the read as a body failure, which is cleaned up, but a client that stalls keeps its read in progress until shutdown.
pub async fn read_content<T, B, S>(
    request: &mut Request<B>,
    store: &mut S,
    cancellation: &Cancellation,
) -> Result<Accepted<T, S::Handle>, ReadError<B::Error, S::Error>>
where
    T: ContentHandles<S::Handle>,
    B: ReadableBody,
    S: UploadStore,
{
    read_content_with(request, store, &ReadOptions::default(), cancellation).await
}

/// Read a request's content as [`read_content`] does, with `options`.
pub async fn read_content_with<T, B, S>(
    request: &mut Request<B>,
    store: &mut S,
    options: &ReadOptions,
    cancellation: &Cancellation,
) -> Result<Accepted<T, S::Handle>, ReadError<B::Error, S::Error>>
where
    T: ContentHandles<S::Handle>,
    B: ReadableBody,
    S: UploadStore,
{
    let parser = match parser(request, options) {
        Ok(parser) => parser,
        Err(kind) => {
            request.body.close(None);
            return Err(ReadError::new(kind));
        }
    };
    let mut reader = Reader {
        parser,
        store,
        upload: None,
    };
    let stopped = reader.read_body(&mut request.body, cancellation).await;
    let Reader {
        parser,
        store,
        upload,
    } = reader;
    if stopped.is_err() {
        request.body.close(None);
    }
    let mut cleanup = Vec::new();
    if let Some(upload) = upload {
        if let Err(error) = store.abort(upload).await {
            cleanup.push(error);
        }
    }
    // An upload in progress leaves the content incomplete, so accepted content never follows an abort.
    let rejected = match parser.finish() {
        Ok(accepted) => return Ok(accepted),
        Err(rejected) => rejected,
    };
    for handle in rejected.handles {
        if let Err(error) = store.remove(handle).await {
            cleanup.push(error);
        }
    }
    // A body or store failure leaves the content incomplete, which the parser refuses as a consequence; the failure is the cause.
    let kind = match stopped {
        Err(Stop::Body(error)) => ReadErrorKind::Body(error),
        Err(Stop::Store(error)) => ReadErrorKind::Store(error),
        Ok(()) | Err(Stop::Content) => ReadErrorKind::Rejected(rejected.reason),
    };
    Err(ReadError { kind, cleanup })
}

/// The parser for the request's media type, once the request's length, if known, is within the largest body the declaration could need.
fn parser<T, H, B, S>(
    request: &Request<B>,
    options: &ReadOptions,
) -> Result<ContentParser<T, H>, ReadErrorKind<B::Error, S>>
where
    T: ContentHandles<H>,
    B: ReadableBody,
{
    let media_type = media_type(&request.fields).ok_or(ReadErrorKind::InvalidMediaType)?;
    let mut parser = ContentParser::<T, H>::new(media_type.as_ref())
        .map_err(|error| ReadErrorKind::Rejected(Rejection::Error(error)))?;
    if let Some(rules) = options.parameter_rules {
        parser = parser.with_parameter_rules(rules);
    }
    let limit = parser.body_size_limit();
    match request.length() {
        Some(length) if length > limit as u64 => Err(ReadErrorKind::ContentTooLarge {
            length,
            limit: limit as u64,
        }),
        _ => Ok(parser),
    }
}

/// The media type of the single `Content-Type` field, `Some(None)` without one, or `None` when there are several or it is not a valid media type.
fn media_type(fields: &Fields) -> Option<Option<MediaType>> {
    let mut values = fields.values("content-type");
    let Some(value) = values.next() else {
        return Some(None);
    };
    if values.next().is_some() {
        return None;
    }
    let text = std::str::from_utf8(value).ok()?;
    MediaType::parse(text).ok().map(Some)
}

/// Why reading stopped early.
enum Stop<B, S> {
    /// The parser reported a fatal error, which `finish` reports again.
    Content,
    Body(B),
    Store(S),
}

/// A parser's fatal error, which `finish` reports with the handles to remove.
fn content<B, S>(_: ContentError) -> Stop<B, S> {
    Stop::Content
}

/// What one parser event leaves the reader to do.
enum Step {
    Continue,
    Incomplete,
    End,
}

struct Reader<'s, T, S: UploadStore> {
    parser: ContentParser<T, S::Handle>,
    store: &'s mut S,
    /// The upload whose content is arriving.
    upload: Option<S::Upload>,
}

impl<T, S: UploadStore> Reader<'_, T, S> {
    /// Read the body to its end, giving each frame to the parser as it arrives, and only retaining what the parser has not consumed.
    async fn read_body<B: ReadableBody>(
        &mut self,
        body: &mut B,
        cancellation: &Cancellation,
    ) -> Result<(), Stop<B::Error, S::Error>> {
        let mut retained = Vec::new();
        loop {
            let frame = body.read(cancellation).await.map_err(Stop::Body)?;
            let end = frame.is_end();
            if let BodyFrame::Data { data, .. } = frame {
                let data = data.as_ref();
                if retained.is_empty() {
                    let consumed = self.parse(data, false, cancellation).await?;
                    retained.extend_from_slice(&data[consumed..]);
                } else {
                    retained.extend_from_slice(data);
                    let consumed = self.parse(&retained, false, cancellation).await?;
                    retained.drain(..consumed);
                }
            }
            if end {
                return self.parse(&retained, true, cancellation).await.map(drop);
            }
        }
    }

    /// Parse `input`, returning how much was consumed: while more may follow, until the parser needs more, or once the body has ended, with `end`, until the parser reports the content's end.
    async fn parse<B>(
        &mut self,
        input: &[u8],
        end: bool,
        cancellation: &Cancellation,
    ) -> Result<usize, Stop<B, S::Error>> {
        let mut offset = 0;
        loop {
            let rest = &input[offset..];
            let parsed = match end {
                false => self.parser.parse(rest),
                true => self.parser.parse_end(rest),
            }
            .map_err(content)?;
            offset += parsed.consumed;
            match self
                .handle(parsed.event, cancellation)
                .await
                .map_err(Stop::Store)?
            {
                Step::Continue => {}
                // A multipart body's epilogue may follow its end, and is parsed until the input is consumed; once the body has ended, `finish` checks what remains.
                Step::End if !end && offset < input.len() => {}
                // `parse_end` never reports `Incomplete`.
                Step::Incomplete | Step::End => return Ok(offset),
            }
        }
    }

    /// Store an upload as the parser reports it.
    async fn handle(
        &mut self,
        event: ContentEvent<'_>,
        cancellation: &Cancellation,
    ) -> Result<Step, S::Error> {
        match event {
            ContentEvent::Upload(head) => {
                self.upload = Some(self.store.create(&head, cancellation).await?);
            }
            ContentEvent::UploadContent(content) => {
                let upload = self.upload.as_mut().expect("content follows its upload");
                self.store.write(upload, content, cancellation).await?;
            }
            ContentEvent::UploadEnd => {
                let upload = self.upload.as_mut().expect("an upload ends after it began");
                let handle = self.store.finish(upload, cancellation).await?;
                self.upload = None;
                self.parser.store(handle);
            }
            ContentEvent::End => return Ok(Step::End),
            ContentEvent::Incomplete => return Ok(Step::Incomplete),
        }
        Ok(Step::Continue)
    }
}

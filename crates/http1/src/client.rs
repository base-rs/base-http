// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    Error, Exchange, IncomingBody, Options, RequestReader, ResponseWriter, input, output, wire,
};
use base_endpoint::stream::{ReadableStream, WritableStream};
use base_http::{
    InterimHandler, InterimResponses, Method, Protocol, Request, RequestMetadata, Response,
    ResponseMetadata, Status, Version,
    body::{BodyFrame, ReadableBody},
};
use base_scheduler::Cancellation;
use std::io;

/// Sequential client output. Drive this lane concurrently with `ResponseReader`
/// so an upstream response can arrive before request production completes.
/// No dialing, pooling, detached task or Expect/Continue wait policy is hidden.
pub struct RequestWriter<W> {
    output: ResponseWriter<W>,
    body: Option<output::BodyEncoder>,
    persistent: bool,
    buffer: Vec<u8>,
}

impl<W: WritableStream> RequestWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            output: ResponseWriter::new(writer),
            body: None,
            persistent: false,
            buffer: Vec::new(),
        }
    }
    /// Whether this output lane permits another request. This does not check
    /// completion of the independent response lane.
    pub fn is_reusable(&self) -> bool {
        self.output.is_usable()
    }

    /// Send one request. `true` permits another output message; the owner must
    /// also finish the matching response before reusing the HTTP/1 connection.
    /// The body is closed on return, including error and cancellation.
    pub async fn write_request<B: ReadableBody>(
        &mut self,
        request: &mut Request<B>,
        cancellation: &Cancellation,
    ) -> Result<bool, Error> {
        let result = self.write_inner(request, cancellation).await;
        request.body.close(None);
        result
    }

    async fn write_inner<B: ReadableBody>(
        &mut self,
        request: &mut Request<B>,
        cancellation: &Cancellation,
    ) -> Result<bool, Error> {
        cancellation.check()?;
        let (framing, persistent) = request_plan(
            &mut self.buffer,
            &request.metadata,
            request.body.length(),
            request.body.may_have_trailers(),
        )?;
        self.output.begin()?;
        self.output.write_all(&self.buffer, cancellation).await?;
        self.output.flush(cancellation).await?;
        self.output
            .write_body(&mut request.body, framing, false, cancellation)
            .await?;
        self.output.completed(persistent);
        Ok(persistent)
    }

    /// Begin imperative output. A missing content length uses chunked HTTP/1.1
    /// framing; declared fixed lengths are checked at every write and at EOF.
    /// After failure or cancellation this lane must be retired.
    pub async fn begin_request(
        &mut self,
        metadata: &RequestMetadata,
        cancellation: &Cancellation,
    ) -> Result<(), Error> {
        cancellation.check()?;
        let (framing, persistent) =
            request_plan(&mut self.buffer, metadata, metadata.content_length, false)?;
        self.output.begin()?;
        self.output.write_all(&self.buffer, cancellation).await?;
        self.output.flush(cancellation).await?;
        // Activate the body only once the whole head is sent, so frames cannot follow a failed or dropped head.
        self.persistent = persistent;
        self.body = Some(output::BodyEncoder::new(framing, false));
        Ok(())
    }

    /// Directly encode and flush one frame. Taking the encoder before awaiting
    /// makes a dropped, partially completed write permanently non-resumable.
    pub async fn write_frame<D: AsRef<[u8]>>(
        &mut self,
        frame: BodyFrame<D>,
        cancellation: &Cancellation,
    ) -> Result<(), Error> {
        cancellation.check()?;
        let mut body = self
            .body
            .take()
            .ok_or(Error::Invalid("request output is not active"))?;
        let end = frame.is_end();
        let flush = !matches!(&frame, BodyFrame::End) || body.end_requires_output();
        body.write_frame(&mut self.output, frame, cancellation)
            .await?;
        if flush {
            self.output.flush(cancellation).await?;
        }
        if end {
            self.output.completed(self.persistent);
        } else {
            self.body = Some(body);
        }
        Ok(())
    }

    /// Flush pending transport output. A failure retires the lane; accepted
    /// bytes must not be replayed after an interrupted flush.
    pub async fn flush(&mut self, cancellation: &Cancellation) -> Result<(), Error> {
        let result = self.output.flush(cancellation).await;
        if result.is_err() {
            self.body = None;
            self.output.completed(false);
        }
        result
    }

    pub async fn close(&mut self) -> io::Result<()> {
        self.output.close().await
    }
}

fn request_plan(
    bytes: &mut Vec<u8>,
    head: &RequestMetadata,
    length: Option<u64>,
    may_have_trailers: bool,
) -> Result<(output::Framing, bool), Error> {
    let version = head.version.unwrap_or(Version::Http11);
    let label = match version {
        Version::Http10 => "HTTP/1.0",
        Version::Http11 => "HTTP/1.1",
        _ => return Err(Error::Invalid("unsupported request version")),
    };
    let fields = &head.fields;
    let host = wire::singleton(fields, "host")?;
    if let Some(host) = host {
        wire::authority(
            std::str::from_utf8(host).map_err(|_| Error::Invalid("invalid host"))?,
            false,
        )?;
    }
    let add_host = if let Some(authority) = &head.authority {
        wire::authority(authority, false)?;
        if let Some(host) = host {
            if !host.eq_ignore_ascii_case(authority.as_bytes()) {
                return Err(Error::Invalid("authority differs from Host"));
            }
            None
        } else {
            Some(authority.as_str())
        }
    } else {
        None
    };
    if version == Version::Http11 && host.is_none() && add_host.is_none() {
        return Err(Error::Invalid("missing host"));
    }
    let upgrade = if let Some(protocol) = &head.protocol {
        if version != Version::Http11 {
            return Err(Error::Invalid("upgrade requires HTTP/1.1"));
        }
        if let Some(offer) = wire::singleton(fields, "upgrade")? {
            if !offer.eq_ignore_ascii_case(protocol.as_str().as_bytes()) {
                return Err(Error::Invalid("conflicting upgrade protocol"));
            }
            None
        } else {
            Some(protocol.as_str())
        }
    } else {
        None
    };
    wire::connection_options(fields)?;
    let add_upgrade = head.protocol.is_some() && !wire::has_token(fields, "connection", b"upgrade");
    if version == Version::Http11
        && (add_upgrade || wire::has_token(fields, "connection", b"upgrade"))
        && !fields.contains("upgrade")
        && upgrade.is_none()
    {
        return Err(Error::Invalid("missing upgrade protocol"));
    }
    let connection = head
        .persistent
        .map(|p| if p { "keep-alive" } else { "close" })
        .filter(|value| !wire::has_token(fields, "connection", value.as_bytes()));
    let target = if head.method == Method::CONNECT {
        if head.path.is_some() {
            return Err(Error::Invalid("CONNECT cannot have a path"));
        }
        head.authority
            .as_deref()
            .ok_or(Error::Invalid("CONNECT requires authority"))?
    } else {
        head.path.as_deref().unwrap_or("/")
    };
    wire::request_target(&head.method, target)?;
    let (raw_length, raw_chunked) = wire::framing(fields)?;
    let declared = wire::declared_length(head.content_length, raw_length)?;
    if declared.is_some() && raw_chunked {
        return Err(Error::Invalid("content-length with transfer-encoding"));
    }
    if raw_chunked && version != Version::Http11 {
        return Err(Error::Invalid("HTTP/1.0 transfer-encoding"));
    }
    if let (Some(length), Some(declared)) = (length, declared) {
        if length != declared {
            return Err(Error::LengthMismatch);
        }
    }
    let chunked = raw_chunked || may_have_trailers || fields.contains("trailer");
    let framing = if chunked {
        if version != Version::Http11 {
            return Err(Error::Invalid("trailers require HTTP/1.1 chunked framing"));
        }
        output::Framing::Chunked(declared.or(length))
    } else if let Some(length) = declared.or(length) {
        output::Framing::Fixed(length)
    } else if version == Version::Http11 {
        output::Framing::Chunked(None)
    } else {
        return Err(Error::Invalid("HTTP/1.0 request requires a body length"));
    };
    let chunked = matches!(framing, output::Framing::Chunked(_));
    let persistent = head.persistent != Some(false)
        && !wire::has_token(fields, "connection", b"close")
        && (version == Version::Http11
            || head.persistent == Some(true)
            || wire::has_token(fields, "connection", b"keep-alive"));
    wire::output_fields(fields.pairs())?;
    // Validate first, then encode directly into the connection's reusable buffer.
    bytes.clear();
    use std::io::Write;
    // Writing into a `Vec` cannot fail.
    write!(bytes, "{} {target} {label}\r\n", head.method.as_str()).expect("writing to a Vec");
    for field in fields.pairs() {
        if chunked && field.name().eq_ignore_ascii_case("content-length") {
            continue;
        }
        bytes.extend_from_slice(field.name().as_bytes());
        bytes.extend_from_slice(b": ");
        bytes.extend_from_slice(field.value());
        bytes.extend_from_slice(b"\r\n");
    }
    for (name, value) in [
        ("Host", add_host),
        ("Upgrade", upgrade),
        ("Connection", add_upgrade.then_some("upgrade")),
        ("Connection", connection),
        (
            "Transfer-Encoding",
            (chunked && !raw_chunked).then_some("chunked"),
        ),
    ] {
        if let Some(value) = value {
            write!(bytes, "{name}: {value}\r\n").expect("writing to a Vec");
        }
    }
    if let output::Framing::Fixed(length) = framing {
        if raw_length.is_none()
            && (declared.is_some()
                || length != 0
                || matches!(head.method.as_str(), "POST" | "PUT" | "PATCH"))
        {
            write!(bytes, "Content-Length: {length}\r\n").expect("writing to a Vec");
        }
    }
    bytes.extend_from_slice(b"\r\n");
    Ok((framing, persistent))
}

/// Sequential client input sharing the native request body's chunk decoder.
/// The matching request head supplies HEAD/CONNECT semantics. This reader does
/// not queue outstanding requests; the owner pairs each response in wire order.
/// Switch responses expose raw input only; coordinating the outgoing switch
/// and managed bidirectional client bodies is not implemented here.
pub struct ResponseReader<R> {
    input: RequestReader<R>,
    persistent: bool,
}

impl<R: ReadableStream> ResponseReader<R> {
    pub fn new(reader: R, options: Options) -> Result<Self, Error> {
        Ok(Self {
            input: RequestReader::new(reader, options)?,
            persistent: true,
        })
    }
    pub fn is_reusable(&self) -> bool {
        self.persistent && self.input.is_reusable()
    }

    /// Deliver interim heads in order, then return the final response. `101`
    /// and successful CONNECT yield raw input and permanently end HTTP reuse.
    /// The connection owner supervises the independent request-writing lane.
    pub async fn read_response<I: InterimHandler>(
        &mut self,
        request: &RequestMetadata,
        interim: &mut InterimResponses<I>,
        cancellation: &Cancellation,
    ) -> Result<Response<IncomingBody<'_, R>>, Error> {
        cancellation.check()?;
        if !self.is_reusable() {
            return Err(Error::Invalid("response input is not reusable"));
        }
        let max_fields = self.input.options.max_fields;
        let head = loop {
            self.input.poisoned = true;
            let mut head = self
                .input
                .read_section(cancellation, "message head", |bytes| {
                    wire::response(bytes, max_fields)
                })
                .await?
                .ok_or(Error::Truncated)?;
            if head.status.is_final() {
                interim.finish();
                break head;
            }
            if head.fields.contains("content-length")
                || head.fields.contains("transfer-encoding")
                || head.version != Some(Version::Http11)
            {
                return Err(Error::Invalid("invalid interim framing"));
            }
            head.fields = wire::application_fields(head.fields)?.0;
            interim
                .send_interim_response(head, cancellation)
                .await
                .map_err(|error| {
                    if cancellation.is_cancelled() {
                        Error::Cancelled
                    } else {
                        Error::Body(Box::new(error))
                    }
                })?;
        };
        self.response_body(request, head)
    }

    fn response_body(
        &mut self,
        request: &RequestMetadata,
        mut head: ResponseMetadata,
    ) -> Result<Response<IncomingBody<'_, R>>, Error> {
        let connect = request.method == Method::CONNECT && head.status.is_success();
        let upgrade = head.status == Status::SWITCHING_PROTOCOLS;
        let tunnel = connect || upgrade;
        // CONNECT framing fields are ignored by recipients (RFC 9112 6.3).
        let (length, chunked) = if connect {
            (None, false)
        } else {
            wire::framing(&head.fields)?
        };
        if chunked && head.version != Some(Version::Http11) {
            return Err(Error::Invalid("HTTP/1.0 transfer-encoding"));
        }
        if upgrade {
            if length.is_some()
                || chunked
                || !wire::has_token(&head.fields, "connection", b"upgrade")
            {
                return Err(Error::Invalid("invalid upgrade response"));
            }
            let selected = wire::singleton(&head.fields, "upgrade")?
                .ok_or(Error::Invalid("missing selected protocol"))?;
            let selected = std::str::from_utf8(selected)
                .map_err(|_| Error::Invalid("invalid selected protocol"))?;
            let offered = request
                .protocol
                .as_ref()
                .is_some_and(|p| p.as_str().eq_ignore_ascii_case(selected))
                || (wire::has_token(&request.fields, "connection", b"upgrade")
                    && wire::has_token(&request.fields, "upgrade", selected.as_bytes()));
            if !offered {
                return Err(Error::Invalid("upgrade protocol was not offered"));
            }
            head.protocol = Some(
                Protocol::new(selected)
                    .map_err(|_| Error::Invalid("unsupported selected protocol"))?,
            );
        }
        let absent = !tunnel
            && (request.method == Method::HEAD
                || head.status == Status::NO_CONTENT
                || head.status == Status::NOT_MODIFIED);
        if head.status == Status::NO_CONTENT && !connect && (length.is_some() || chunked) {
            return Err(Error::Invalid("framing fields on 204"));
        }
        let raw = tunnel || (!absent && !chunked && length.is_none());
        let framing = if absent || raw {
            input::Framing::Fixed(0)
        } else if chunked {
            input::Framing::Chunked {
                remaining: 0,
                delimiter: false,
            }
        } else {
            input::Framing::Fixed(length.unwrap())
        };
        let body_length = if raw {
            None
        } else if absent && head.status == Status::NO_CONTENT {
            Some(0)
        } else {
            length
        };
        let response_persistent = !wire::has_token(&head.fields, "connection", b"close")
            && (head.version == Some(Version::Http11)
                || wire::has_token(&head.fields, "connection", b"keep-alive"));
        head.content_length = length;
        let (fields, trailer_options) = wire::application_fields(head.fields)?;
        head.fields = fields;
        let exchange = Exchange::new(request, trailer_options);
        let request_persistent = exchange.is_persistent();
        self.persistent = request_persistent && response_persistent && !raw;
        if tunnel {
            exchange.switch();
        }
        self.input.previous = Some(exchange.clone());
        self.input.poisoned = false;
        let forbid_content = head.status == Status::RESET_CONTENT && !tunnel;
        let body = IncomingBody::new(
            &mut self.input,
            exchange,
            framing,
            body_length,
            raw,
            forbid_content,
        );
        Ok(Response::from_parts(head, body))
    }

    pub async fn close(&mut self) -> io::Result<()> {
        self.input.close().await
    }
}

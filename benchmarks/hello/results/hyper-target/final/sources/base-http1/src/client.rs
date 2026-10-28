// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    Error, Exchange, IncomingBody, Options, RequestReader, ResponseWriter, input, output, wire,
};
use base_http::{
    Fields, InterimHandler, InterimResponses, Method, Protocol, Request, RequestHead, Response,
    ResponseHead, Status, Version, body::ReadableBody,
};
use base_scheduler::{Cancellation, SocketStreamReader, SocketStreamWriter};
use std::io;

/// Sequential client output. Drive this lane concurrently with `ResponseReader`
/// so an upstream response can arrive before request production completes.
/// No dialing, pooling, detached task or Expect/Continue wait policy is hidden.
pub struct RequestWriter<W> {
    output: ResponseWriter<W>,
}

impl<W: SocketStreamWriter> RequestWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            output: ResponseWriter::new(writer),
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
        let (bytes, framing, persistent) = request_plan(&request.head, request.body.length())?;
        self.output.begin()?;
        self.output.write_all(&bytes, cancellation).await?;
        self.output.flush(cancellation).await?;
        self.output
            .write_body(
                &mut request.body,
                &mut request.head.fields,
                framing,
                false,
                cancellation,
            )
            .await?;
        self.output.completed(persistent);
        Ok(persistent)
    }

    pub async fn close(&mut self) -> io::Result<()> {
        self.output.close().await
    }
}

fn request_plan(
    head: &RequestHead,
    length: Option<u64>,
) -> Result<(Vec<u8>, output::Framing, bool), Error> {
    let version = head.version.unwrap_or(Version::Http11);
    let label = match version {
        Version::Http10 => "HTTP/1.0",
        Version::Http11 => "HTTP/1.1",
        _ => return Err(Error::Invalid("unsupported request version")),
    };
    let mut fields =
        Fields::try_from_pairs(head.fields.headers().iter().map(|f| (f.name(), f.value())))?;
    output::validate_trailers(&head.fields)?;
    if let Some(authority) = &head.authority {
        if let Some(host) = wire::singleton(&fields, "host")? {
            if !host.eq_ignore_ascii_case(authority.as_bytes()) {
                return Err(Error::Invalid("authority differs from Host"));
            }
        } else {
            fields.add("Host", authority)?;
        }
    }
    if let Some(protocol) = &head.protocol {
        if version != Version::Http11 {
            return Err(Error::Invalid("upgrade requires HTTP/1.1"));
        }
        if let Some(offer) = wire::singleton(&fields, "upgrade")? {
            if !offer.eq_ignore_ascii_case(protocol.as_str().as_bytes()) {
                return Err(Error::Invalid("conflicting upgrade protocol"));
            }
        } else {
            fields.add("Upgrade", protocol.as_str())?;
        }
        output::connection_option(&mut fields, "upgrade")?;
    }
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
    // Validate before serializing the start line; otherwise an embedded CRLF
    // could turn a caller-supplied target into extra wire fields.
    if target.is_empty() || !target.is_ascii() || target.bytes().any(|b| b <= 32 || b == 127) {
        return Err(Error::Invalid("invalid request target"));
    }
    let (declared, chunked) = wire::framing(&fields)?;
    if chunked && version != Version::Http11 {
        return Err(Error::Invalid("HTTP/1.0 transfer-encoding"));
    }
    if let (Some(length), Some(declared)) = (length, declared) {
        if length != declared {
            return Err(Error::LengthMismatch);
        }
    }
    let framing = if chunked || head.fields.has_trailers() || fields.contains("trailer") {
        if declared.is_some() || version != Version::Http11 {
            return Err(Error::Invalid("trailers require HTTP/1.1 chunked framing"));
        }
        if !chunked {
            fields.add("Transfer-Encoding", "chunked")?;
        }
        if !head.fields.trailers().is_empty() && !fields.contains("trailer") {
            fields.add(
                "Trailer",
                head.fields
                    .trailers()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>()
                    .join(", "),
            )?;
        }
        output::Framing::Chunked
    } else if let Some(length) = declared.or(length) {
        if declared.is_none()
            && (length != 0 || matches!(head.method.as_str(), "POST" | "PUT" | "PATCH"))
        {
            fields.add("Content-Length", length.to_string())?;
        }
        output::Framing::Fixed(length)
    } else if version == Version::Http11 {
        fields.add("Transfer-Encoding", "chunked")?;
        output::Framing::Chunked
    } else {
        return Err(Error::Invalid("HTTP/1.0 request requires a body length"));
    };
    let mut bytes = format!("{} {target} {label}\r\n", head.method.as_str()).into_bytes();
    wire::append_fields(&mut bytes, fields.headers());
    bytes.extend_from_slice(b"\r\n");
    // Apply the same request-target and Host validation in both directions.
    let mut headers = vec![httparse::EMPTY_HEADER; fields.len()];
    let mut parsed = httparse::Request::new(&mut headers);
    if parsed.parse(&bytes) != Ok(httparse::Status::Complete(bytes.len())) {
        return Err(Error::Invalid("invalid request head"));
    }
    let prepared = wire::request(&parsed)?;
    let persistent = Exchange::new(prepared).is_persistent();
    Ok((bytes, framing, persistent))
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

impl<R: SocketStreamReader> ResponseReader<R> {
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
        request: &RequestHead,
        interim: &mut InterimResponses<I>,
        cancellation: &Cancellation,
    ) -> Result<Response<IncomingBody<'_, R>>, Error> {
        cancellation.check()?;
        if !self.is_reusable() {
            return Err(Error::Invalid("response input is not reusable"));
        }
        let max_fields = self.input.options.max_fields;
        let head = loop {
            let bytes = self
                .input
                .read_head(cancellation)
                .await?
                .ok_or(Error::Truncated)?;
            let mut local = [std::mem::MaybeUninit::uninit(); 100];
            let mut overflow;
            let headers = if max_fields <= local.len() {
                &mut local[..max_fields]
            } else {
                overflow = vec![std::mem::MaybeUninit::uninit(); max_fields];
                &mut overflow
            };
            let mut parsed = httparse::Response::new(&mut []);
            match httparse::ParserConfig::default().parse_response_with_uninit_headers(
                &mut parsed,
                bytes,
                headers,
            ) {
                Ok(httparse::Status::Complete(n)) if n == bytes.len() => {}
                Err(httparse::Error::TooManyHeaders) => return Err(Error::Limit("field count")),
                _ => return Err(Error::Invalid("invalid response head")),
            }
            let mut head = ResponseHead::new(
                Status::new(parsed.code.ok_or(Error::Invalid("missing status"))?)
                    .map_err(|_| Error::Invalid("invalid status"))?,
            );
            head.version = Some(match parsed.version {
                Some(0) => Version::Http10,
                Some(1) => Version::Http11,
                _ => return Err(Error::Invalid("unsupported response version")),
            });
            head.fields = wire::fields(parsed.headers)?;
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
        request: &RequestHead,
        mut head: ResponseHead,
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
        let exchange = Exchange::new(request.clone());
        let request_persistent = exchange.is_persistent();
        let response_persistent = !wire::has_token(&head.fields, "connection", b"close")
            && (head.version == Some(Version::Http11)
                || wire::has_token(&head.fields, "connection", b"keep-alive"));
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

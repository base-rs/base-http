// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Error, Exchange, wire};
use base_http::{InterimHandler, Response, ResponseHead, Status, Version, body::ReadableBody};
use base_scheduler::{Cancellation, SocketStreamWriter};
use std::{
    future::{Future, poll_fn},
    io,
    pin::pin,
    task::Poll,
};

async fn wait<F: Future>(future: F, cancellation: &Cancellation) -> Result<F::Output, Error> {
    if cancellation.is_never() {
        return Ok(future.await);
    }
    let mut future = pin!(future);
    let mut cancelled = pin!(cancellation.cancelled());
    poll_fn(|cx| {
        if cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(Error::Cancelled));
        }
        future.as_mut().poll(cx).map(Ok)
    })
    .await
}

// Bound copied output independently of the number or size of body chunks.
const OUTPUT_BATCH_LIMIT: usize = 16 * 1024;

async fn send_all<W: SocketStreamWriter>(
    writer: &mut W,
    mut bytes: &[u8],
    cancellation: &Cancellation,
) -> Result<(), Error> {
    while !bytes.is_empty() {
        let n = writer.write(bytes, cancellation).await?;
        if n == 0 {
            return Err(
                io::Error::new(io::ErrorKind::WriteZero, "HTTP output made no progress").into(),
            );
        }
        bytes = &bytes[n..];
    }
    Ok(())
}

/// The response lane. A partially sent or abandoned response permanently
/// poisons admission. The owner must still await `close` to retire the backend.
pub struct ResponseWriter<W> {
    writer: W,
    pending: Vec<u8>,
    usable: bool,
    continue_sent: bool,
}

impl<W: SocketStreamWriter> ResponseWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            pending: Vec::new(),
            usable: true,
            continue_sent: false,
        }
    }
    pub fn is_usable(&self) -> bool {
        self.usable
    }
    pub fn interim(&mut self, version: Version) -> InterimWriter<'_, W> {
        InterimWriter {
            output: self,
            version,
        }
    }

    /// Best-effort rejection for invalid input, followed by connection closure.
    pub async fn reject(
        &mut self,
        status: Status,
        cancellation: &Cancellation,
    ) -> Result<(), Error> {
        cancellation.check()?;
        if !self.usable {
            return Err(Error::Invalid("response output is not reusable"));
        }
        let mut head = ResponseHead::new(status);
        head.version = Some(Version::Http11);
        head.fields.add("Content-Length", "0")?;
        head.fields.add("Connection", "close")?;
        self.usable = false;
        self.write_head(&head, cancellation).await?;
        self.flush(cancellation).await?;
        Ok(())
    }

    pub(crate) async fn write_all(
        &mut self,
        bytes: &[u8],
        cancellation: &Cancellation,
    ) -> Result<(), Error> {
        cancellation.check()?;
        if bytes.len() > OUTPUT_BATCH_LIMIT - self.pending.len() {
            self.send_pending(cancellation).await?;
        }
        if bytes.len() >= OUTPUT_BATCH_LIMIT {
            // Large chunks retain their existing owner and are not copied.
            return send_all(&mut self.writer, bytes, cancellation).await;
        }
        self.reserve(bytes.len());
        self.pending.extend_from_slice(bytes);
        Ok(())
    }

    fn reserve(&mut self, additional: usize) {
        let required = self.pending.len() + additional;
        if required > self.pending.capacity() {
            let capacity = required
                .max(self.pending.capacity().saturating_mul(2))
                .clamp(256, OUTPUT_BATCH_LIMIT);
            self.pending.reserve_exact(capacity - self.pending.len());
        }
    }

    async fn send_pending(&mut self, cancellation: &Cancellation) -> Result<(), Error> {
        // The lane is already poisoned while sending. On error/cancellation the
        // retained prefix must never be retried by a subsequent message or close.
        send_all(&mut self.writer, &self.pending, cancellation).await?;
        self.pending.clear();
        Ok(())
    }

    async fn write_head(
        &mut self,
        head: &ResponseHead,
        cancellation: &Cancellation,
    ) -> Result<(), Error> {
        self.write_start_line(head.status, head.version, cancellation)
            .await?;
        self.write_fields(head.fields.headers(), cancellation)
            .await?;
        self.write_all(b"\r\n", cancellation).await
    }

    async fn write_start_line(
        &mut self,
        status: Status,
        version: Option<Version>,
        cancellation: &Cancellation,
    ) -> Result<(), Error> {
        let version = match version {
            Some(Version::Http10) => b"HTTP/1.0 ",
            Some(Version::Http11) => b"HTTP/1.1 ",
            _ => return Err(Error::Invalid("unsupported response version")),
        };
        let status_code = status.as_u16();
        let code = [
            b'0' + (status_code / 100) as u8,
            b'0' + (status_code / 10 % 10) as u8,
            b'0' + (status_code % 10) as u8,
            b' ',
        ];
        let reason = status.description().unwrap_or("").as_bytes();
        let size = version.len() + code.len() + reason.len() + 2;
        if size <= OUTPUT_BATCH_LIMIT - self.pending.len() {
            self.reserve(size);
            self.pending.extend_from_slice(version);
            self.pending.extend_from_slice(&code);
            self.pending.extend_from_slice(reason);
            self.pending.extend_from_slice(b"\r\n");
            return Ok(());
        }
        self.write_all(version, cancellation).await?;
        self.write_all(&code, cancellation).await?;
        self.write_all(reason, cancellation).await?;
        self.write_all(b"\r\n", cancellation).await
    }

    async fn write_fields(
        &mut self,
        fields: &[base_http::Field],
        cancellation: &Cancellation,
    ) -> Result<(), Error> {
        for field in fields {
            self.write_field(field.name(), field.value(), cancellation)
                .await?;
        }
        Ok(())
    }

    async fn write_field(
        &mut self,
        name: &str,
        value: &[u8],
        cancellation: &Cancellation,
    ) -> Result<(), Error> {
        let size = name.len().saturating_add(value.len()).saturating_add(4);
        if size <= OUTPUT_BATCH_LIMIT - self.pending.len() {
            self.reserve(size);
            self.pending.extend_from_slice(name.as_bytes());
            self.pending.extend_from_slice(b": ");
            self.pending.extend_from_slice(value);
            self.pending.extend_from_slice(b"\r\n");
            return Ok(());
        }
        self.write_all(name.as_bytes(), cancellation).await?;
        self.write_all(b": ", cancellation).await?;
        self.write_all(value, cancellation).await?;
        self.write_all(b"\r\n", cancellation).await
    }

    async fn write_plan(
        &mut self,
        plan: &Plan<'_>,
        cancellation: &Cancellation,
    ) -> Result<(), Error> {
        self.write_start_line(plan.response.status, Some(plan.version), cancellation)
            .await?;
        for field in plan.response.fields.headers() {
            if plan.upgrade.is_some() && field.name().eq_ignore_ascii_case("upgrade") {
                continue;
            }
            self.write_field(field.name(), field.value(), cancellation)
                .await?;
        }
        if let Some(protocol) = plan.upgrade {
            self.write_field("Upgrade", protocol, cancellation).await?;
        }
        if plan.connection_upgrade {
            self.write_field("Connection", b"upgrade", cancellation)
                .await?;
        }
        if let Some(length) = plan.content_length {
            let mut digits = [0; 20];
            let bytes = decimal(length, &mut digits);
            self.write_field("Content-Length", bytes, cancellation)
                .await?;
        }
        if plan.chunked {
            self.write_field("Transfer-Encoding", b"chunked", cancellation)
                .await?;
        }
        if plan.declare_trailers {
            self.write_all(b"Trailer: ", cancellation).await?;
            for (index, field) in plan.response.fields.trailers().iter().enumerate() {
                if index != 0 {
                    self.write_all(b", ", cancellation).await?;
                }
                self.write_all(field.name().as_bytes(), cancellation)
                    .await?;
            }
            self.write_all(b"\r\n", cancellation).await?;
        }
        if let Some(option) = plan.connection {
            self.write_field("Connection", option, cancellation).await?;
        }
        self.write_all(b"\r\n", cancellation).await
    }

    /// Write one final response. `true` means output permits persistence; the
    /// connection owner must also finish input and join concurrent work before
    /// admitting another request. Discarded input disables persistence.
    pub async fn write_response<B: ReadableBody>(
        &mut self,
        exchange: &Exchange,
        response: &mut Response<B>,
        cancellation: &Cancellation,
    ) -> Result<bool, Error> {
        let result = self
            .write_response_inner(exchange, response, cancellation)
            .await;
        response.body.close(None);
        result
    }

    async fn write_response_inner<B: ReadableBody>(
        &mut self,
        exchange: &Exchange,
        response: &mut Response<B>,
        cancellation: &Cancellation,
    ) -> Result<bool, Error> {
        cancellation.check()?;
        if !self.usable {
            return Err(Error::Invalid("response output is not reusable"));
        }
        let (framing, persistent, tunnel) = {
            let plan = plan(exchange, &response.head, response.body.length())?;
            if plan.tunnel
                && response.head.status == Status::SWITCHING_PROTOCOLS
                && !self.continue_sent
                && exchange.expects_continue()
            {
                self.interim(Version::Http11)
                    .on_interim_response(ResponseHead::new(Status::CONTINUE), cancellation)
                    .await?;
            }
            self.usable = false;
            self.write_plan(&plan, cancellation).await?;
            if plan.tunnel {
                // Commit the handshake before the first potentially blocking input read.
                self.flush(cancellation).await?;
                exchange.switch();
            }
            (plan.framing, plan.persistent, plan.tunnel)
        };
        self.write_body(
            &mut response.body,
            &mut response.head.fields,
            framing,
            !tunnel && response.head.status == Status::RESET_CONTENT,
            cancellation,
        )
        .await?;
        self.usable = persistent;
        self.continue_sent = false;
        Ok(persistent)
    }

    pub(crate) fn begin(&mut self) -> Result<(), Error> {
        if !self.usable {
            return Err(Error::Invalid("output is not reusable"));
        }
        self.usable = false;
        Ok(())
    }

    pub(crate) fn completed(&mut self, persistent: bool) {
        self.usable = persistent;
    }

    pub(crate) async fn flush(&mut self, cancellation: &Cancellation) -> Result<(), Error> {
        self.send_pending(cancellation).await?;
        Ok(self.writer.flush(cancellation).await?)
    }

    pub(crate) async fn write_body<B: ReadableBody>(
        &mut self,
        body: &mut B,
        fields: &mut base_http::Fields,
        framing: Framing,
        forbid_content: bool,
        cancellation: &Cancellation,
    ) -> Result<(), Error> {
        if !matches!(framing, Framing::Absent) {
            let mut total = 0u64;
            loop {
                if !body.is_ready() {
                    self.flush(cancellation).await?;
                }
                // Readiness is only a hint: buffered input can contain incomplete
                // chunk framing. Flush on actual suspension, retaining the same
                // read future so partial reads and cancellation stay intact.
                let mut read = pin!(wait(body.read(cancellation), cancellation));
                let ready = poll_fn(|cx| Poll::Ready(read.as_mut().poll(cx))).await;
                let chunk = match ready {
                    Poll::Ready(result) => result,
                    Poll::Pending => {
                        self.flush(cancellation).await?;
                        read.await
                    }
                }?
                .map_err(|e| {
                    if cancellation.is_cancelled() {
                        Error::Cancelled
                    } else {
                        Error::Body(Box::new(e))
                    }
                })?;
                let Some(chunk) = chunk else {
                    break;
                };
                let bytes = chunk.as_ref();
                if bytes.is_empty() {
                    continue;
                }
                total = total
                    .checked_add(bytes.len() as u64)
                    .ok_or(Error::LengthMismatch)?;
                if forbid_content && total != 0 {
                    return Err(Error::Invalid("content on 205"));
                }
                if let Framing::Fixed(length) = framing {
                    if total > length {
                        return Err(Error::LengthMismatch);
                    }
                }
                if matches!(framing, Framing::Chunked) {
                    self.write_all(format!("{:x}\r\n", bytes.len()).as_bytes(), cancellation)
                        .await?;
                    self.write_all(bytes, cancellation).await?;
                    self.write_all(b"\r\n", cancellation).await?;
                } else {
                    self.write_all(bytes, cancellation).await?;
                }
            }
            if let Framing::Fixed(length) = framing {
                if total != length {
                    return Err(Error::LengthMismatch);
                }
            }
            body.finish(fields).map_err(|e| Error::Body(Box::new(e)))?;
            if matches!(framing, Framing::Chunked) {
                validate_trailers(fields)?;
                self.write_all(b"0\r\n", cancellation).await?;
                self.write_fields(fields.trailers(), cancellation).await?;
                self.write_all(b"\r\n", cancellation).await?;
            } else if !fields.trailers().is_empty() {
                return Err(Error::Invalid("trailers require chunked framing"));
            }
        }
        self.flush(cancellation).await?;
        Ok(())
    }

    pub async fn close(&mut self) -> io::Result<()> {
        self.usable = false;
        self.pending.clear();
        self.writer.close().await
    }
}

/// Statically dispatched interim-response hook borrowing the output lane.
pub struct InterimWriter<'a, W> {
    output: &'a mut ResponseWriter<W>,
    version: Version,
}

impl<W: SocketStreamWriter> InterimHandler for InterimWriter<'_, W> {
    type Error = Error;
    async fn on_interim_response(
        &mut self,
        mut response: ResponseHead,
        cancellation: &Cancellation,
    ) -> Result<(), Error> {
        cancellation.check()?;
        if !self.output.usable {
            return Err(Error::Invalid("response output is not reusable"));
        }
        if !response.status.is_informational() || response.status == Status::SWITCHING_PROTOCOLS {
            return Err(Error::Invalid("invalid interim status"));
        }
        if self.version != Version::Http11 {
            return Err(Error::Invalid("HTTP/1.0 interim response"));
        }
        if response.fields.contains("content-length")
            || response.fields.contains("transfer-encoding")
            || response.fields.has_trailers()
            || response.protocol.is_some()
        {
            return Err(Error::Invalid("invalid interim framing"));
        }
        response.version = Some(self.version);
        self.output.usable = false;
        self.output.write_head(&response, cancellation).await?;
        self.output.flush(cancellation).await?;
        if response.status == Status::CONTINUE {
            self.output.continue_sent = true;
        }
        self.output.usable = true;
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Framing {
    Absent,
    Fixed(u64),
    Chunked,
    Raw,
}
// Borrow semantic fields and describe only the framing additions. This avoids
// constructing/cloning a second semantic message just to serialize a wire head.
struct Plan<'a> {
    response: &'a ResponseHead,
    version: Version,
    framing: Framing,
    persistent: bool,
    tunnel: bool,
    content_length: Option<u64>,
    chunked: bool,
    declare_trailers: bool,
    upgrade: Option<&'a [u8]>,
    connection_upgrade: bool,
    connection: Option<&'static [u8]>,
}

fn plan<'a>(
    exchange: &Exchange,
    response: &'a ResponseHead,
    length: Option<u64>,
) -> Result<Plan<'a>, Error> {
    if !response.status.is_final() {
        return Err(Error::Invalid("final response required"));
    }
    let fields = &response.fields;
    let has_trailers = fields.has_trailers();
    validate_trailers(fields)?;
    let version = exchange.version();
    if response.version.is_some_and(|v| v != version) {
        return Err(Error::Invalid("response version differs from request"));
    }
    // All framing/connection lookups inspect the initial section only.
    let contains = |name: &str| {
        fields
            .headers()
            .iter()
            .any(|field| field.name().eq_ignore_ascii_case(name))
    };
    let (declared, chunked) = wire::framing(fields)?;
    let connect = exchange.is_connect() && response.status.is_success();
    let upgrade = response.status == Status::SWITCHING_PROTOCOLS;
    let tunnel = connect || upgrade;
    let status_bodyless =
        response.status == Status::NO_CONTENT || response.status == Status::NOT_MODIFIED;
    let absent = !tunnel && (exchange.is_head() || status_bodyless);
    let mut persistent = exchange.is_persistent()
        && !exchange.is_abandoned()
        && !wire::has_token(fields, "connection", b"close")
        && !tunnel;
    let mut content_length = None;
    let mut generated_chunked = false;
    let mut declare_trailers = false;
    let mut selected_upgrade = None;
    let mut connection_upgrade = false;
    let mut connection = None;
    let framing;
    if tunnel {
        if !exchange.is_complete() {
            return Err(Error::Invalid("switch with unread request body"));
        }
        if declared.is_some() || chunked || has_trailers {
            return Err(Error::Invalid("framing fields on tunnel response"));
        }
        if upgrade {
            if version != Version::Http11 || !exchange.has_upgrade() {
                return Err(Error::Invalid("unsolicited upgrade"));
            }
            let selected = response
                .protocol
                .as_ref()
                .map(|p| p.as_str().as_bytes())
                .or(wire::singleton(fields, "upgrade")?)
                .ok_or(Error::Invalid("missing selected protocol"))?;
            if !wire::token(selected) || !exchange.accepts_upgrade(selected) {
                return Err(Error::Invalid("upgrade protocol was not offered"));
            }
            selected_upgrade = Some(selected);
            connection_upgrade = !wire::has_token(fields, "connection", b"upgrade");
        } else if response.protocol.is_some() {
            return Err(Error::Invalid("protocol on traditional CONNECT"));
        }
        framing = Framing::Raw;
    } else {
        if response.protocol.is_some() {
            return Err(Error::Invalid("protocol without upgrade status"));
        }
        if contains("upgrade") {
            connection_upgrade = !wire::has_token(fields, "connection", b"upgrade");
        }
        if chunked && version != Version::Http11 {
            return Err(Error::Invalid("HTTP/1.0 transfer-encoding"));
        }
        if absent {
            if chunked || has_trailers {
                return Err(Error::Invalid(
                    "trailers or transfer-encoding on bodyless response",
                ));
            }
            if response.status == Status::NO_CONTENT {
                if declared.is_some() {
                    return Err(Error::Invalid("content-length on 204"));
                }
            } else if declared.is_none() {
                content_length = length;
            }
            framing = Framing::Absent;
        } else {
            if let (Some(actual), Some(declared)) = (length, declared) {
                if actual != declared {
                    return Err(Error::LengthMismatch);
                }
            }
            if response.status == Status::RESET_CONTENT
                && (length.is_some_and(|n| n != 0) || declared.is_some_and(|n| n != 0))
            {
                return Err(Error::Invalid("content on 205"));
            }
            if response.status == Status::RESET_CONTENT
                && declared.is_none()
                && !chunked
                && !has_trailers
                && !contains("trailer")
            {
                content_length = Some(0);
                framing = Framing::Fixed(0);
            } else if chunked || has_trailers || contains("trailer") {
                if declared.is_some() || version != Version::Http11 {
                    return Err(Error::Invalid("trailers require HTTP/1.1 chunked framing"));
                }
                generated_chunked = !chunked;
                declare_trailers = !fields.trailers().is_empty() && !contains("trailer");
                framing = Framing::Chunked;
            } else if let Some(length) = declared.or(length) {
                if declared.is_none() {
                    content_length = Some(length);
                }
                framing = Framing::Fixed(length);
            } else if version == Version::Http11 {
                generated_chunked = true;
                framing = Framing::Chunked;
            } else {
                persistent = false;
                framing = Framing::Raw;
            }
        }
        if !persistent {
            if !wire::has_token(fields, "connection", b"close") {
                connection = Some(&b"close"[..]);
            }
        } else if version == Version::Http10
            && !wire::has_token(fields, "connection", b"keep-alive")
        {
            connection = Some(&b"keep-alive"[..]);
        }
    }
    Ok(Plan {
        response,
        version,
        framing,
        persistent,
        tunnel,
        content_length,
        chunked: generated_chunked,
        declare_trailers,
        upgrade: selected_upgrade,
        connection_upgrade,
        connection,
    })
}

fn decimal(mut value: u64, digits: &mut [u8; 20]) -> &[u8] {
    let mut start = digits.len();
    loop {
        start -= 1;
        digits[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            return &digits[start..];
        }
    }
}

pub(crate) fn connection_option(fields: &mut base_http::Fields, value: &str) -> Result<(), Error> {
    if !wire::has_token(fields, "connection", value.as_bytes()) {
        fields.add("Connection", value)?;
    }
    Ok(())
}

pub(crate) fn validate_trailers(fields: &base_http::Fields) -> Result<(), Error> {
    let mut checked = base_http::Fields::new();
    checked.mark_trailers();
    for field in fields.trailers() {
        checked.append(field.clone())?;
    }
    Ok(())
}

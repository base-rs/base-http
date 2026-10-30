// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Error, Options, wire};
use base_endpoint::stream::ReadableStream;
use base_http::{
    Fields, Method, Request, RequestMetadata, Version,
    body::{BodyFrame, ReadableBody},
};
use base_scheduler::Cancellation;
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Default)]
struct ExchangeState {
    upgrade: Option<Box<[u8]>>,
    trailer_options: Vec<Vec<u8>>,
    complete: AtomicBool,
    tunnel: AtomicBool,
    abandoned: AtomicBool,
}

#[derive(Clone, Copy)]
struct ExchangeFacts {
    version: Version,
    head: bool,
    connect: bool,
    persistent: bool,
    expect_continue: bool,
}

/// Captured framing facts and, when input can still change, shared state. A successful switch changes incoming body reads to raw data without releasing transport.
#[derive(Clone)]
pub struct Exchange {
    facts: ExchangeFacts,
    state: Option<Arc<ExchangeState>>,
}

pub(crate) fn request_persistent(head: &RequestMetadata) -> bool {
    head.persistent != Some(false)
        && !wire::has_token(&head.fields, "connection", b"close")
        && (head.version.unwrap_or(Version::Http11) == Version::Http11
            || head.persistent == Some(true)
            || wire::has_token(&head.fields, "connection", b"keep-alive"))
}

impl Exchange {
    pub(crate) fn new(head: &RequestMetadata, trailer_options: Vec<Vec<u8>>) -> Self {
        // Retain only protocol decisions. Ordinary fields and target strings belong solely to the application request; mutations cannot change already established framing. Trailer exclusions retain only the bytes needed for later protocol processing. The client selects upgrades from its own request, so its exchange retains no upgrade offers.
        Self {
            facts: ExchangeFacts {
                version: head.version.unwrap_or(Version::Http11),
                head: head.method == Method::HEAD,
                connect: head.method == Method::CONNECT,
                persistent: request_persistent(head),
                expect_continue: wire::has_token(&head.fields, "expect", b"100-continue"),
            },
            // Client response completion depends on incoming response framing,
            // independently of the matching outgoing request's body length.
            state: Some(Arc::new(ExchangeState {
                trailer_options,
                ..Default::default()
            })),
        }
    }
    fn from_request(head: &RequestMetadata, framing: wire::RequestFraming) -> Self {
        let facts = ExchangeFacts {
            version: head.version.unwrap_or(Version::Http11),
            head: head.method == Method::HEAD,
            connect: head.method == Method::CONNECT,
            persistent: framing.persistent,
            expect_continue: framing.expect_continue,
        };
        // Completed ordinary server input cannot become active or abandoned.
        // CONNECT and offered upgrades can become raw even when initially empty.
        let mutable = facts.connect
            || framing.upgrade.is_some()
            || framing.chunked
            || framing.length.is_some_and(|n| n != 0);
        Self {
            facts,
            state: mutable.then(|| {
                Arc::new(ExchangeState {
                    upgrade: framing.upgrade,
                    trailer_options: framing.trailer_options,
                    ..Default::default()
                })
            }),
        }
    }

    fn filter_trailers(&self, fields: &mut Fields) {
        let state = self
            .state
            .as_ref()
            .expect("chunked input retains shared state");
        fields.retain(|field| {
            !state
                .trailer_options
                .iter()
                .any(|option| field.name().as_bytes().eq_ignore_ascii_case(option))
        });
    }

    pub fn version(&self) -> Version {
        self.facts.version
    }
    pub fn is_head(&self) -> bool {
        self.facts.head
    }
    pub fn is_connect(&self) -> bool {
        self.facts.connect
    }
    pub fn expects_continue(&self) -> bool {
        self.facts.expect_continue
    }
    pub(crate) fn has_upgrade(&self) -> bool {
        self.state
            .as_ref()
            .is_some_and(|state| state.upgrade.is_some())
    }
    pub(crate) fn accepts_upgrade(&self, protocol: &[u8]) -> bool {
        self.state
            .as_ref()
            .and_then(|state| state.upgrade.as_ref())
            .is_some_and(|offers| {
                offers
                    .split(|byte| *byte == b',')
                    .any(|offer| wire::trim(offer).eq_ignore_ascii_case(protocol))
            })
    }
    pub fn is_complete(&self) -> bool {
        self.state
            .as_ref()
            .is_none_or(|state| state.complete.load(Ordering::Acquire))
    }
    pub fn is_tunnel(&self) -> bool {
        self.state
            .as_ref()
            .is_some_and(|state| state.tunnel.load(Ordering::Acquire))
    }
    /// Whether the application discarded input before reaching its boundary.
    pub fn is_abandoned(&self) -> bool {
        self.state
            .as_ref()
            .is_some_and(|state| state.abandoned.load(Ordering::Acquire))
    }
    pub fn is_persistent(&self) -> bool {
        self.facts.persistent
    }
    pub(crate) fn switch(&self) {
        self.state
            .as_ref()
            .expect("switches retain shared state")
            .tunnel
            .store(true, Ordering::Release);
    }
    fn set_complete(&self, complete: bool) {
        if let Some(state) = &self.state {
            state.complete.store(complete, Ordering::Release);
        }
    }
    fn abandon(&self) {
        // Input without shared state is complete from the start, so it is never abandoned.
        self.state
            .as_ref()
            .expect("incomplete input retains shared state")
            .abandoned
            .store(true, Ordering::Release);
    }
}

/// Sequential request admission. Input read-ahead survives body completion and
/// protocol switches. Dropped/failed partial reads prohibit further admission.
pub struct RequestReader<R> {
    reader: R,
    buffer: Vec<u8>,
    start: usize,
    end: usize,
    pub(crate) options: Options,
    pub(crate) poisoned: bool,
    pub(crate) previous: Option<Exchange>,
}

impl<R: ReadableStream> RequestReader<R> {
    pub fn new(reader: R, options: Options) -> Result<Self, Error> {
        options.validate()?;
        Ok(Self {
            reader,
            buffer: vec![0; options.read_size],
            start: 0,
            end: 0,
            options,
            poisoned: false,
            previous: None,
        })
    }

    pub fn is_reusable(&self) -> bool {
        !self.poisoned
            && self
                .previous
                .as_ref()
                .is_none_or(|x| x.is_complete() && !x.is_tunnel() && !x.is_abandoned())
    }

    fn available(&self) -> &[u8] {
        &self.buffer[self.start..self.end]
    }

    async fn fill(&mut self, cancellation: &Cancellation) -> Result<bool, Error> {
        if self.start != 0 {
            self.buffer.copy_within(self.start..self.end, 0);
            self.end -= self.start;
            self.start = 0;
        }
        // Neither the buffer nor its initial `read_size` allocation can exceed `isize::MAX`, so this cannot overflow.
        let limit = self.end + self.options.read_size;
        if self.buffer.len() < limit {
            // Keep initialized spare capacity across reads. A pending/cancelled
            // read never publishes it: only a successful count advances end.
            self.buffer.resize(limit, 0);
        }
        let n = self
            .reader
            .read_into(&mut self.buffer[self.end..limit], cancellation)
            .await?;
        self.end += n;
        Ok(n != 0)
    }

    fn take(&mut self, n: usize) -> Vec<u8> {
        let data = self.available()[..n].to_vec();
        self.start += n;
        data
    }

    /// Parse a response head or trailer section with `parse`, which returns
    /// the section's length once it is complete. As for requests, the first
    /// read is parsed directly, and an incomplete section is parsed again only
    /// once its end arrives. `Ok(None)` means the input ended cleanly first.
    pub(crate) async fn read_section<T>(
        &mut self,
        cancellation: &Cancellation,
        limit: &'static str,
        parse: impl Fn(&[u8]) -> Result<Option<(T, usize)>, Error>,
    ) -> Result<Option<T>, Error> {
        let max_head_bytes = self.options.max_head_bytes;
        let mut scanned = None;
        loop {
            let available = self.available();
            let window = &available[..available.len().min(max_head_bytes)];
            // A trailer section can be only the empty line.
            let terminated = window.starts_with(b"\r\n")
                || scanned.is_none_or(|scanned: usize| {
                    window[scanned..].windows(4).any(|w| w == b"\r\n\r\n")
                });
            if terminated && !window.is_empty() {
                if let Some((section, n)) = parse(window)? {
                    self.start += n;
                    return Ok(Some(section));
                }
            }
            if available.len() >= max_head_bytes {
                return Err(Error::Limit(limit));
            }
            if !available.is_empty() {
                scanned = Some(available.len().saturating_sub(3));
            }
            if !self.fill(cancellation).await? {
                if self.available().is_empty() {
                    return Ok(None);
                }
                return Err(Error::Truncated);
            }
        }
    }

    /// Clean EOF between requests returns `None`; partial heads are errors.
    pub async fn read_request(
        &mut self,
        cancellation: &Cancellation,
    ) -> Result<Option<(Request<IncomingBody<'_, R>>, Exchange)>, Error> {
        cancellation.check()?;
        if !self.is_reusable() {
            return Err(Error::Invalid("request input is not reusable"));
        }
        // Parse the first input available, which is usually a complete head.
        // A head that is still incomplete is parsed again only once its
        // terminator arrives, so fragmented input costs linear work. The loop stays in this function so the parsed metadata
        // is not moved through another future's output.
        self.poisoned = true;
        let max_head_bytes = self.options.max_head_bytes;
        let mut scanned = None;
        let (head, facts) = loop {
            let available = self.available();
            let window = &available[..available.len().min(max_head_bytes)];
            let terminated = scanned.is_none_or(|scanned: usize| {
                window[scanned..].windows(4).any(|w| w == b"\r\n\r\n")
            });
            if terminated && !window.is_empty() {
                if let Some((head, facts, n)) = wire::request(window, self.options.max_fields)? {
                    self.start += n;
                    break (head, facts);
                }
            }
            if available.len() >= max_head_bytes {
                return Err(Error::Limit("message head"));
            }
            if !available.is_empty() {
                scanned = Some(available.len().saturating_sub(3));
            }
            if !self.fill(cancellation).await? {
                if self.available().is_empty() {
                    self.poisoned = false;
                    return Ok(None);
                }
                return Err(Error::Truncated);
            }
        };
        let length = facts.length;
        let chunked = facts.chunked;
        if chunked && head.version != Some(Version::Http11) {
            return Err(Error::Invalid("HTTP/1.0 transfer-encoding"));
        }
        let framing = if chunked {
            Framing::Chunked {
                remaining: 0,
                delimiter: false,
            }
        } else {
            Framing::Fixed(length.unwrap_or(0))
        };
        let exchange = Exchange::from_request(&head, facts);
        self.previous = Some(exchange.clone());
        self.poisoned = false;
        let body = IncomingBody::new(
            self,
            exchange.clone(),
            framing,
            if chunked {
                None
            } else {
                Some(length.unwrap_or(0))
            },
            false,
            false,
        );
        Ok(Some((Request::from_parts(head, body), exchange)))
    }

    /// Establish backend retirement even after cancellation or parse failure.
    pub async fn close(&mut self) -> io::Result<()> {
        self.poisoned = true;
        self.reader.close().await
    }

    async fn line(&mut self, limit: usize, cancellation: &Cancellation) -> Result<Vec<u8>, Error> {
        loop {
            if let Some(end) = self.available().windows(2).position(|w| w == b"\r\n") {
                if end + 2 > limit {
                    return Err(Error::Limit("line length"));
                }
                return Ok(self.take(end + 2));
            }
            if self.available().len() >= limit {
                return Err(Error::Limit("line length"));
            }
            if !self.fill(cancellation).await? {
                return Err(Error::Truncated);
            }
        }
    }

    async fn trailers(&mut self, cancellation: &Cancellation) -> Result<Fields, Error> {
        let max_fields = self.options.max_fields;
        let fields = self
            .read_section(cancellation, "trailer section", |bytes| {
                wire::trailers(bytes, max_fields)
            })
            .await?
            .ok_or(Error::Truncated)?;
        crate::output::validate_trailer_fields(fields.pairs())?;
        Ok(fields)
    }
}

pub(crate) enum Framing {
    Fixed(u64),
    Chunked { remaining: u64, delimiter: bool },
}

/// A pull body borrowing the connection's input lane. Returning it as the
/// response body provides streaming echo and managed duplex tunnels without
/// boxing, queueing, spawning, or transferring socket ownership.
pub struct IncomingBody<'a, R> {
    input: &'a mut RequestReader<R>,
    exchange: Exchange,
    framing: Framing,
    length: Option<u64>,
    complete: bool,
    closed: bool,
    raw: bool,
    forbid_content: bool,
}

impl<'a, R: ReadableStream> IncomingBody<'a, R> {
    pub(crate) fn new(
        input: &'a mut RequestReader<R>,
        exchange: Exchange,
        framing: Framing,
        length: Option<u64>,
        raw: bool,
        forbid_content: bool,
    ) -> Self {
        let complete = !raw && matches!(framing, Framing::Fixed(0));
        exchange.set_complete(complete);
        Self {
            input,
            exchange,
            framing,
            length,
            complete,
            closed: false,
            raw,
            forbid_content,
        }
    }
    async fn read_inner(
        &mut self,
        cancellation: &Cancellation,
    ) -> Result<BodyFrame<Vec<u8>>, Error> {
        if self.exchange.is_tunnel() && !self.raw {
            self.raw = true;
            self.complete = false;
        }
        if self.complete || self.closed {
            return Ok(BodyFrame::End);
        }
        loop {
            if self.raw {
                if self.input.available().is_empty() && !self.input.fill(cancellation).await? {
                    self.complete = true;
                    return Ok(BodyFrame::End);
                }
                return Ok(BodyFrame::Data {
                    data: self.input.take(self.input.available().len()),
                    end: false,
                });
            }
            match &mut self.framing {
                Framing::Fixed(remaining) => {
                    if self.input.available().is_empty() && !self.input.fill(cancellation).await? {
                        return Err(Error::Truncated);
                    }
                    let n = (*remaining).min(self.input.available().len() as u64) as usize;
                    *remaining -= n as u64;
                    let data = self.input.take(n);
                    if *remaining == 0 {
                        self.complete = true;
                    }
                    return Ok(BodyFrame::Data {
                        data,
                        end: self.complete,
                    });
                }
                Framing::Chunked {
                    remaining,
                    delimiter,
                } => {
                    if *remaining != 0 {
                        if self.input.available().is_empty()
                            && !self.input.fill(cancellation).await?
                        {
                            return Err(Error::Truncated);
                        }
                        let n = (*remaining).min(self.input.available().len() as u64) as usize;
                        *remaining -= n as u64;
                        return Ok(BodyFrame::Data {
                            data: self.input.take(n),
                            end: false,
                        });
                    }
                    if *delimiter {
                        while self.input.available().len() < 2 {
                            if !self.input.fill(cancellation).await? {
                                return Err(Error::Truncated);
                            }
                        }
                        if !self.input.available().starts_with(b"\r\n") {
                            return Err(Error::Invalid("missing chunk delimiter"));
                        }
                        self.input.start += 2;
                    }
                    let line = self
                        .input
                        .line(self.input.options.max_chunk_line_bytes, cancellation)
                        .await?;
                    let size = chunk_size(&line)?;
                    if size == 0 {
                        let mut fields = self.input.trailers(cancellation).await?;
                        self.exchange.filter_trailers(&mut fields);
                        self.complete = true;
                        return Ok(BodyFrame::Headers { fields, end: true });
                    }
                    *remaining = size;
                    *delimiter = true;
                }
            }
        }
    }
}

impl<R: ReadableStream> ReadableBody for IncomingBody<'_, R> {
    type Data = Vec<u8>;
    type Error = Error;
    fn may_have_trailers(&self) -> bool {
        !self.raw && !self.exchange.is_tunnel() && matches!(self.framing, Framing::Chunked { .. })
    }
    fn is_ready(&self) -> bool {
        !self.input.available().is_empty() || self.complete || self.closed
    }
    fn is_empty(&self) -> bool {
        (self.complete && !self.exchange.is_tunnel()) || self.closed
    }
    fn length(&self) -> Option<u64> {
        if self.exchange.is_tunnel() {
            None
        } else {
            self.length
        }
    }
    fn close(&mut self, _: Option<Error>) {
        if !self.complete {
            self.exchange.abandon();
        }
        self.closed = true;
    }
    async fn read(&mut self, cancellation: &Cancellation) -> Result<BodyFrame<Vec<u8>>, Error> {
        cancellation.check()?;
        if self.input.poisoned {
            return Err(Error::Invalid("previous body read failed or was abandoned"));
        }
        self.input.poisoned = true;
        let result = self.read_inner(cancellation).await.and_then(|chunk| {
            if self.forbid_content
                && matches!(&chunk, BodyFrame::Data { data, .. } if !data.is_empty())
            {
                Err(Error::Invalid("content on 205"))
            } else {
                Ok(chunk)
            }
        });
        if result.is_ok() {
            self.input.poisoned = false;
            self.exchange.set_complete(self.complete);
        }
        result
    }
}

impl<R> Drop for IncomingBody<'_, R> {
    fn drop(&mut self) {
        if !self.complete {
            self.exchange.abandon();
        }
    }
}

// Validate extension grammar rather than trusting ignored bytes after ';'.
// A line ends at its first CRLF, and this grammar rejects any other CR or LF.
fn chunk_size(line: &[u8]) -> Result<u64, Error> {
    let bytes = &line[..line.len() - 2];
    let end = bytes
        .iter()
        .position(|b| !b.is_ascii_hexdigit())
        .unwrap_or(bytes.len());
    if end == 0 {
        return Err(Error::Invalid("invalid chunk size"));
    }
    let size = u64::from_str_radix(std::str::from_utf8(&bytes[..end]).unwrap(), 16)
        .map_err(|_| Error::Invalid("chunk size overflow"))?;
    let mut rest = &bytes[end..];
    while !rest.is_empty() {
        rest = wire::trim(rest);
        if rest.first() != Some(&b';') {
            return Err(Error::Invalid("invalid chunk extension"));
        }
        rest = &rest[1..];
        rest = wire::trim(rest);
        let end = rest
            .iter()
            .position(|b| !wire::token(&[*b]))
            .unwrap_or(rest.len());
        if end == 0 {
            return Err(Error::Invalid("missing chunk extension name"));
        }
        rest = wire::trim(&rest[end..]);
        if rest.first() == Some(&b'=') {
            rest = wire::trim(&rest[1..]);
            if rest.first() == Some(&b'"') {
                rest = &rest[1..];
                loop {
                    let b = *rest
                        .first()
                        .ok_or(Error::Invalid("unterminated chunk extension"))?;
                    rest = &rest[1..];
                    if b == b'"' {
                        break;
                    }
                    if b == b'\\' {
                        let b = *rest
                            .first()
                            .ok_or(Error::Invalid("invalid quoted escape"))?;
                        rest = &rest[1..];
                        if b < 32 && b != b'\t' || b == 127 {
                            return Err(Error::Invalid("invalid quoted escape"));
                        }
                    } else if b < 32 && b != b'\t' || b == 127 {
                        return Err(Error::Invalid("invalid chunk extension"));
                    }
                }
            } else {
                let end = rest
                    .iter()
                    .position(|b| !wire::token(&[*b]))
                    .unwrap_or(rest.len());
                if end == 0 {
                    return Err(Error::Invalid("missing chunk extension value"));
                }
                rest = &rest[end..];
            }
        }
    }
    Ok(size)
}

#[cfg(test)]
mod tests {
    use super::chunk_size;

    #[test]
    fn chunk_lines_reject_stray_line_controls() {
        assert_eq!(chunk_size(b"1a;name=\"v\\\"\";flag\r\n").unwrap(), 26);
        for line in [
            &b"1\r;x\r\n"[..],
            b"1\n\r\n",
            b"1;x\r\r\n",
            b"1;x=\r\r\n",
            b"1;x=\"\r\"\r\n",
            b"1;x=\"\\\n\"\r\n",
            b"1;x\0\r\n",
        ] {
            assert!(chunk_size(line).is_err(), "{line:?}");
        }
    }

    #[test]
    fn chunk_lines_accept_token_extension_values_and_reject_malformed_ones() {
        assert_eq!(chunk_size(b"ff ; name = value ; flag\r\n").unwrap(), 255);
        for (line, reason) in [
            (&b"11111111111111111\r\n"[..], "chunk size overflow"),
            (b"1;x=\"open\r\n", "unterminated chunk extension"),
            (b"1;x=\"\\\r\n", "invalid quoted escape"),
        ] {
            assert!(
                matches!(chunk_size(line), Err(crate::Error::Invalid(r)) if r == reason),
                "{line:?}"
            );
        }
    }
}

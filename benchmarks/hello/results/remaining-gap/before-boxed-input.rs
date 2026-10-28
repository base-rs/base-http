// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Error, Options, wire};
use base_http::{Fields, Method, Request, RequestHead, Version, body::ReadableBody};
use base_scheduler::{Cancellation, SocketStreamReader};
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

pub(crate) fn request_persistent(head: &RequestHead) -> bool {
    !wire::has_token(&head.fields, "connection", b"close")
        && (head.version.unwrap_or(Version::Http11) == Version::Http11
            || wire::has_token(&head.fields, "connection", b"keep-alive"))
}

impl Exchange {
    pub(crate) fn new(head: &RequestHead) -> Self {
        // Retain only protocol decisions. Ordinary fields and target strings
        // belong solely to the application request; mutations cannot change
        // already established framing. Upgrade offers alone need retained bytes.
        let upgrade = wire::has_token(&head.fields, "connection", b"upgrade").then(|| {
            let mut offers = Vec::new();
            for field in head
                .fields
                .headers()
                .iter()
                .filter(|field| field.name().eq_ignore_ascii_case("upgrade"))
            {
                if !offers.is_empty() {
                    offers.push(b',');
                }
                offers.extend_from_slice(field.value());
            }
            offers.into_boxed_slice()
        });
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
                upgrade,
                ..Default::default()
            })),
        }
    }
    fn from_request(head: &RequestHead, framing: wire::RequestFraming) -> Self {
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
                    ..Default::default()
                })
            }),
        }
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
        if let Some(state) = &self.state {
            state.abandoned.store(true, Ordering::Release);
        }
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

impl<R: SocketStreamReader> RequestReader<R> {
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
        let limit = self
            .end
            .checked_add(self.options.read_size)
            .ok_or(Error::Limit("receive buffer"))?;
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

    pub(crate) async fn read_head(
        &mut self,
        cancellation: &Cancellation,
    ) -> Result<Option<&[u8]>, Error> {
        self.poisoned = true;
        let mut scanned = 0;
        loop {
            if let Some(end) = self.available()[scanned..]
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .map(|p| scanned + p + 4)
            {
                if end > self.options.max_head_bytes {
                    return Err(Error::Limit("message head"));
                }
                wire::strict_lines(&self.available()[..end])?;
                if matches!(self.available().first(), Some(b'\r' | b'\n' | b' ' | b'\t')) {
                    return Err(Error::Invalid("invalid start line prefix"));
                }
                let start = self.start;
                self.start += end;
                return Ok(Some(&self.buffer[start..start + end]));
            }
            if self.available().len() >= self.options.max_head_bytes {
                return Err(Error::Limit("message head"));
            }
            scanned = self.available().len().saturating_sub(3);
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
        let max_fields = self.options.max_fields;
        let Some(bytes) = self.read_head(cancellation).await? else {
            self.poisoned = false;
            return Ok(None);
        };
        let mut local = [std::mem::MaybeUninit::uninit(); 100];
        let mut overflow;
        let headers = if max_fields <= local.len() {
            &mut local[..max_fields]
        } else {
            overflow = vec![std::mem::MaybeUninit::uninit(); max_fields];
            &mut overflow
        };
        let mut parsed = httparse::Request::new(&mut []);
        match parsed.parse_with_uninit_headers(bytes, headers) {
            Ok(httparse::Status::Complete(n)) if n == bytes.len() => {}
            Err(httparse::Error::TooManyHeaders) => return Err(Error::Limit("field count")),
            _ => return Err(Error::Invalid("invalid request head")),
        }
        let (head, facts) = wire::request(&parsed)?;
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
        let mut fields = Fields::new();
        fields.mark_trailers();
        let mut size = 0usize;
        loop {
            let line = self
                .line(self.options.max_head_bytes - size, cancellation)
                .await?;
            size += line.len();
            if line == b"\r\n" {
                return Ok(fields);
            }
            if fields.len() >= self.options.max_fields {
                return Err(Error::Limit("trailer count"));
            }
            wire::strict_lines(&line)?;
            let colon = line
                .iter()
                .position(|b| *b == b':')
                .ok_or(Error::Invalid("invalid trailer"))?;
            let name = std::str::from_utf8(&line[..colon])
                .map_err(|_| Error::Invalid("invalid trailer name"))?;
            fields.add(name, wire::trim(&line[colon + 1..line.len() - 2]))?;
        }
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
    trailers: Option<Fields>,
    forbid_content: bool,
}

impl<'a, R: SocketStreamReader> IncomingBody<'a, R> {
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
            trailers: None,
            forbid_content,
        }
    }
    async fn read_inner(&mut self, cancellation: &Cancellation) -> Result<Option<Vec<u8>>, Error> {
        if self.exchange.is_tunnel() && !self.raw {
            self.raw = true;
            self.complete = false;
        }
        if self.complete || self.closed {
            return Ok(None);
        }
        loop {
            if self.raw {
                if self.input.available().is_empty() && !self.input.fill(cancellation).await? {
                    self.complete = true;
                    return Ok(None);
                }
                return Ok(Some(self.input.take(self.input.available().len())));
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
                    return Ok(Some(data));
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
                        return Ok(Some(self.input.take(n)));
                    }
                    if *delimiter {
                        let line = self.input.line(2, cancellation).await?;
                        if line != b"\r\n" {
                            return Err(Error::Invalid("missing chunk delimiter"));
                        }
                    }
                    let line = self
                        .input
                        .line(self.input.options.max_chunk_line_bytes, cancellation)
                        .await?;
                    let size = chunk_size(&line)?;
                    if size == 0 {
                        self.trailers = Some(self.input.trailers(cancellation).await?);
                        self.complete = true;
                        return Ok(None);
                    }
                    *remaining = size;
                    *delimiter = true;
                }
            }
        }
    }
}

impl<R: SocketStreamReader> ReadableBody for IncomingBody<'_, R> {
    type Data = Vec<u8>;
    type Error = Error;
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
    fn finish(&mut self, fields: &mut Fields) -> Result<(), Error> {
        if !self.complete {
            return Err(Error::Invalid("body has not reached EOF"));
        }
        if let Some(trailers) = self.trailers.take() {
            fields.mark_trailers();
            for field in trailers.pairs() {
                fields.append(field.clone())?;
            }
        }
        Ok(())
    }
    async fn read(&mut self, cancellation: &Cancellation) -> Result<Option<Vec<u8>>, Error> {
        cancellation.check()?;
        if self.input.poisoned {
            return Err(Error::Invalid("previous body read failed or was abandoned"));
        }
        self.input.poisoned = true;
        let result = self.read_inner(cancellation).await.and_then(|chunk| {
            if self.forbid_content && chunk.as_ref().is_some_and(|bytes| !bytes.is_empty()) {
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
fn chunk_size(line: &[u8]) -> Result<u64, Error> {
    wire::strict_lines(line)?;
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

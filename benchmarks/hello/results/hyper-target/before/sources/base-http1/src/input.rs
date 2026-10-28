// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Error, Options, wire};
use base_http::{Fields, Request, RequestHead, Version, body::ReadableBody};
use base_scheduler::{Cancellation, SocketStreamReader};
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Default)]
struct State {
    complete: AtomicBool,
    tunnel: AtomicBool,
    abandoned: AtomicBool,
}

/// Per-request framing context shared with the output direction. A successful
/// switch changes incoming body reads to raw data without releasing transport.
#[derive(Clone)]
pub struct Exchange {
    context: Arc<Context>,
}

struct Context {
    head: RequestHead,
    state: State,
}

impl Exchange {
    pub(crate) fn new(head: RequestHead) -> Self {
        Self {
            context: Arc::new(Context {
                head,
                state: State::default(),
            }),
        }
    }
    pub fn head(&self) -> &RequestHead {
        &self.context.head
    }
    pub fn is_complete(&self) -> bool {
        self.context.state.complete.load(Ordering::Acquire)
    }
    pub fn is_tunnel(&self) -> bool {
        self.context.state.tunnel.load(Ordering::Acquire)
    }
    /// Whether the application discarded input before reaching its boundary.
    pub fn is_abandoned(&self) -> bool {
        self.context.state.abandoned.load(Ordering::Acquire)
    }
    pub fn is_persistent(&self) -> bool {
        !wire::has_token(&self.context.head.fields, "connection", b"close")
            && (self.context.head.version.unwrap_or(Version::Http11) == Version::Http11
                || wire::has_token(&self.context.head.fields, "connection", b"keep-alive"))
    }
    pub(crate) fn switch(&self) {
        self.context.state.tunnel.store(true, Ordering::Release);
    }
}

/// Sequential request admission. Input read-ahead survives body completion and
/// protocol switches. Dropped/failed partial reads prohibit further admission.
pub struct RequestReader<R> {
    reader: R,
    buffer: Vec<u8>,
    scratch: Vec<u8>,
    start: usize,
    pub(crate) options: Options,
    pub(crate) poisoned: bool,
    pub(crate) previous: Option<Exchange>,
}

impl<R: SocketStreamReader> RequestReader<R> {
    pub fn new(reader: R, options: Options) -> Result<Self, Error> {
        options.validate()?;
        Ok(Self {
            reader,
            buffer: Vec::new(),
            scratch: vec![0; options.read_size],
            start: 0,
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
        &self.buffer[self.start..]
    }

    async fn fill(&mut self, cancellation: &Cancellation) -> Result<bool, Error> {
        if self.start != 0 {
            self.buffer.drain(..self.start);
            self.start = 0;
        }
        let n = self
            .reader
            .read_into(&mut self.scratch, cancellation)
            .await?;
        self.buffer.extend_from_slice(&self.scratch[..n]);
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
    ) -> Result<Option<Vec<u8>>, Error> {
        self.poisoned = true;
        loop {
            if let Some(end) = self
                .available()
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .map(|p| p + 4)
            {
                if end > self.options.max_head_bytes {
                    return Err(Error::Limit("message head"));
                }
                wire::strict_lines(&self.available()[..end])?;
                if matches!(self.available().first(), Some(b'\r' | b'\n' | b' ' | b'\t')) {
                    return Err(Error::Invalid("invalid start line prefix"));
                }
                return Ok(Some(self.take(end)));
            }
            if self.available().len() >= self.options.max_head_bytes {
                return Err(Error::Limit("message head"));
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
        let Some(bytes) = self.read_head(cancellation).await? else {
            self.poisoned = false;
            return Ok(None);
        };
        let mut local = [std::mem::MaybeUninit::uninit(); 100];
        let mut overflow;
        let headers = if self.options.max_fields <= local.len() {
            &mut local[..self.options.max_fields]
        } else {
            overflow = vec![std::mem::MaybeUninit::uninit(); self.options.max_fields];
            &mut overflow
        };
        let mut parsed = httparse::Request::new(&mut []);
        match parsed.parse_with_uninit_headers(&bytes, headers) {
            Ok(httparse::Status::Complete(n)) if n == bytes.len() => {}
            Err(httparse::Error::TooManyHeaders) => return Err(Error::Limit("field count")),
            _ => return Err(Error::Invalid("invalid request head")),
        }
        let head = wire::request(&parsed)?;
        let (length, chunked) = wire::framing(&head.fields)?;
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
        let exchange = Exchange::new(head.clone());
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
        exchange
            .context
            .state
            .complete
            .store(complete, Ordering::Release);
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
            self.exchange
                .context
                .state
                .abandoned
                .store(true, Ordering::Release);
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
            self.exchange
                .context
                .state
                .complete
                .store(self.complete, Ordering::Release);
        }
        result
    }
}

impl<R> Drop for IncomingBody<'_, R> {
    fn drop(&mut self) {
        if !self.complete {
            self.exchange
                .context
                .state
                .abandoned
                .store(true, Ordering::Release);
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

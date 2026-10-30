// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! The pipelines used before the single-pass parser: line checks, httparse,
//! then field classification for heads, and a line-by-line reader for trailer
//! sections. Tests and fuzz targets compare the parser with them.

use crate::Error;
use crate::wire::*;
use base_http::{Fields, Method, Protocol, RequestMetadata, ResponseMetadata, Status, Version};
use std::fmt::Debug;

/// Reject lenient line endings and whitespace before a field name. This check
/// complements httparse, which intentionally accepts some interoperable forms.
fn strict_lines(bytes: &[u8]) -> Result<(), Error> {
    // Every LF must follow a CR and every CR must precede an LF. Comparing each
    // adjacent pair without short-circuiting lets the compiler vectorize the
    // scan; heads are bounded by the input limits.
    let unpaired = bytes
        .iter()
        .zip(&bytes[1.min(bytes.len())..])
        .fold(false, |unpaired, (previous, byte)| {
            unpaired | ((*previous == b'\r') != (*byte == b'\n'))
        });
    if unpaired || bytes.first() == Some(&b'\n') || bytes.last() == Some(&b'\r') {
        return Err(Error::Invalid("expected CRLF"));
    }
    Ok(())
}

/// httparse only accepts token names and values without controls other than HTAB, which `Fields` always accepts.
fn add(fields: &mut Fields, field: &httparse::Header<'_>) {
    fields
        .add(field.name, field.value)
        .expect("httparse accepted a valid field");
}

fn fields(headers: &[httparse::Header<'_>]) -> Fields {
    let mut fields = Fields::new();
    for field in headers {
        add(&mut fields, field);
    }
    fields
}

/// httparse only parses HTTP/1.0 and HTTP/1.1.
fn version(minor: Option<u8>) -> Version {
    match minor {
        Some(0) => Version::Http10,
        Some(1) => Version::Http11,
        _ => unreachable!("httparse accepted an unsupported HTTP version"),
    }
}

/// Received values may contain SP, HTAB, VCHAR and obs-text (RFC 9110, section
/// 5.5), the bytes httparse accepts in heads.
fn field_value(value: &[u8]) -> bool {
    value.iter().fold(true, |valid, b| {
        valid & (*b == b'\t' || *b >= b' ' && *b != 0x7f)
    })
}

pub(crate) fn read(
    bytes: &[u8],
    max_fields: usize,
) -> Result<(RequestMetadata, RequestFraming), Error> {
    strict_lines(bytes)?;
    if matches!(bytes.first(), Some(b'\r' | b'\n' | b' ' | b'\t')) {
        return Err(Error::Invalid("invalid start line prefix"));
    }
    let mut headers = vec![std::mem::MaybeUninit::uninit(); max_fields];
    let mut parsed = httparse::Request::new(&mut []);
    match parsed.parse_with_uninit_headers(bytes, &mut headers) {
        Ok(httparse::Status::Complete(n)) if n == bytes.len() => {}
        Err(httparse::Error::TooManyHeaders) => return Err(Error::Limit("field count")),
        _ => return Err(Error::Invalid("invalid request head")),
    }
    request(&parsed)
}

fn request(parsed: &httparse::Request<'_, '_>) -> Result<(RequestMetadata, RequestFraming), Error> {
    // A complete httparse request always has a method token, a target and a version.
    let method = Method::new(parsed.method.expect("complete request has a method"))
        .expect("httparse accepted a method token");
    let target = parsed.path.expect("complete request has a target");
    request_target(&method, target)?;
    let version = version(parsed.version);
    let mut head = RequestMetadata::new(method);
    head.version = Some(version);
    let mut host = None;
    let mut length = None;
    let mut transfer = None;
    let mut close = false;
    let mut keep_alive = false;
    let mut connection_upgrade = false;
    let mut expect_continue = false;
    let mut first_upgrade = None;
    let mut upgrade_count = 0;
    let mut options: Vec<Vec<u8>> = Vec::new();
    for field in parsed.headers.iter() {
        // httparse has already applied the field name and value syntax. Wire
        // controls and Host are captured here instead of being stored and then
        // removed from the application fields.
        match request_control(field.name.as_bytes()) {
            Some(Control::Host) => set_singleton(&mut host, field.value)?,
            Some(Control::ContentLength) => set_singleton(&mut length, field.value)?,
            Some(Control::TransferEncoding) => set_singleton(&mut transfer, field.value)?,
            Some(Control::Connection) => {
                for option in field.value.split(|b| *b == b',').map(trim) {
                    close |= option.eq_ignore_ascii_case(b"close");
                    keep_alive |= option.eq_ignore_ascii_case(b"keep-alive");
                    connection_upgrade |= option.eq_ignore_ascii_case(b"upgrade");
                    // RFC 9110 list recipients ignore empty elements within parser limits.
                    if !option.is_empty() {
                        if !token(option) {
                            return Err(Error::Invalid("invalid connection option"));
                        }
                        options.push(option.to_vec());
                    }
                }
            }
            Some(Control::Upgrade) => {
                first_upgrade.get_or_insert(field.value);
                upgrade_count += 1;
            }
            Some(Control::Removed) => {}
            None => {
                if field.name.eq_ignore_ascii_case("expect") {
                    expect_continue |= field
                        .value
                        .split(|b| *b == b',')
                        .any(|v| trim(v).eq_ignore_ascii_case(b"100-continue"));
                }
                add(&mut head.fields, field);
            }
        }
    }
    // Connection may nominate fields before or after itself.
    if !options.is_empty() {
        head.fields.retain(|field| {
            !options
                .iter()
                .any(|option| field.name().as_bytes().eq_ignore_ascii_case(option))
        });
    }
    let host = host
        .map(|v| std::str::from_utf8(v).map_err(|_| Error::Invalid("invalid host")))
        .transpose()?;
    if version == Version::Http11 && host.is_none() {
        return Err(Error::Invalid("missing host"));
    }
    if let Some(host) = host {
        authority(host, false)?;
        head.authority = Some(host.into());
    }
    // `request_target` has already checked the target form, including the CONNECT authority, the scheme and the absolute-form authority.
    if head.method == Method::CONNECT {
        head.authority = Some(target.into());
    } else if target.starts_with('/') || target == "*" {
        head.path = Some(target.into());
    } else {
        let (scheme, rest) = target
            .split_once("://")
            .expect("request_target accepted an absolute-form target");
        let end = rest.find(['/', '?']).unwrap_or(rest.len());
        head.authority = Some(rest[..end].into());
        head.scheme = Some(scheme.into());
        let suffix = &rest[end..];
        head.path = Some(if suffix.is_empty() {
            "/".into()
        } else if suffix.starts_with('?') {
            format!("/{suffix}").into()
        } else {
            suffix.into()
        });
    }
    let upgrade = if connection_upgrade {
        if version == Version::Http11 {
            let first = first_upgrade.ok_or(Error::Invalid("missing upgrade protocol"))?;
            if upgrade_count == 1 {
                if let Ok(offer) = std::str::from_utf8(trim(first)) {
                    head.protocol = Protocol::new(offer).ok();
                }
            }
        }
        let mut offers = Vec::new();
        for field in parsed
            .headers
            .iter()
            .filter(|f| f.name.eq_ignore_ascii_case("upgrade"))
        {
            if !offers.is_empty() {
                offers.push(b',');
            }
            offers.extend_from_slice(field.value);
        }
        Some(offers.into_boxed_slice())
    } else {
        None
    };
    let (length, chunked) = parse_framing(length, transfer)?;
    head.content_length = length;
    let framing = RequestFraming {
        length,
        chunked,
        persistent: !close && (version == Version::Http11 || keep_alive),
        expect_continue,
        trailer_options: options,
        upgrade,
    };
    Ok((head, framing))
}

/// The old reader cut the head at the first CRLF CRLF before parsing it.
fn terminator(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|p| p + 4)
}

/// The old response reader: cut at the terminator, check line endings and the
/// start line prefix, then parse with httparse.
fn response(bytes: &[u8], max_fields: usize) -> Result<Option<(ResponseMetadata, usize)>, Error> {
    let Some(end) = terminator(bytes) else {
        return Ok(None);
    };
    let bytes = &bytes[..end];
    strict_lines(bytes)?;
    if matches!(bytes.first(), Some(b'\r' | b'\n' | b' ' | b'\t')) {
        return Err(Error::Invalid("invalid start line prefix"));
    }
    let mut headers = vec![std::mem::MaybeUninit::uninit(); max_fields];
    let mut parsed = httparse::Response::new(&mut []);
    match httparse::ParserConfig::default().parse_response_with_uninit_headers(
        &mut parsed,
        bytes,
        &mut headers,
    ) {
        Ok(httparse::Status::Complete(n)) if n == bytes.len() => {}
        Err(httparse::Error::TooManyHeaders) => return Err(Error::Limit("field count")),
        _ => return Err(Error::Invalid("invalid response head")),
    }
    let mut head = ResponseMetadata::new(
        Status::new(parsed.code.expect("complete response has a status"))
            .map_err(|_| Error::Invalid("invalid status"))?,
    );
    head.version = Some(version(parsed.version));
    head.fields = fields(parsed.headers);
    Ok(Some((head, end)))
}

/// The old trailer reader, which read and checked one line at a time.
fn trailers(bytes: &[u8], max_fields: usize) -> Result<Option<(Fields, usize)>, Error> {
    let mut fields = Fields::new();
    let mut at = 0;
    loop {
        let Some(end) = bytes[at..].windows(2).position(|w| w == b"\r\n") else {
            return Ok(None);
        };
        let line = &bytes[at..at + end + 2];
        at += end + 2;
        if line == b"\r\n" {
            return Ok(Some((fields, at)));
        }
        if fields.len() >= max_fields {
            return Err(Error::Limit("trailer count"));
        }
        strict_lines(line)?;
        let colon = line
            .iter()
            .position(|b| *b == b':')
            .ok_or(Error::Invalid("invalid trailer"))?;
        let name = std::str::from_utf8(&line[..colon])
            .map_err(|_| Error::Invalid("invalid trailer name"))?;
        let value = trim(&line[colon + 1..line.len() - 2]);
        if !field_value(value) {
            return Err(Error::Invalid("invalid trailer value"));
        }
        fields.add(name, value)?;
    }
}

/// A prefix of a valid section must not be rejected early. Completing a
/// rejected prefix with a terminator cannot show this, since that completion
/// is invalid for a prefix ending inside a token.
fn incomplete_before(section: &[u8], incomplete: impl Fn(&[u8]) -> bool) {
    for end in 0..section.len() {
        assert!(
            incomplete(&section[..end]),
            "{:?} rejected as a prefix",
            &section[..end]
        );
    }
}

type Section<T> = fn(&[u8], usize) -> Result<Option<(T, usize)>, Error>;

/// Check that a section parser and the previous reader agree on `bytes`. Both
/// must accept with the same result and length, or both reject; the parser
/// may also reject input the old reader was still waiting on, but only input
/// that completing the section leaves invalid.
fn compare_section<T: Debug>(bytes: &[u8], max_fields: usize, parser: Section<T>, old: Section<T>) {
    match (parser(bytes, max_fields), old(bytes, max_fields)) {
        (Ok(Some((section, n))), Ok(Some((expected, end)))) => {
            assert_eq!(n, end, "{bytes:?}");
            assert_eq!(format!("{section:?}"), format!("{expected:?}"), "{bytes:?}");
            incomplete_before(&bytes[..end], |prefix| {
                matches!(parser(prefix, max_fields), Ok(None))
            });
        }
        (Ok(None), Ok(None)) | (Err(_), Err(_)) => {}
        (Err(error), Ok(None)) => {
            let mut completed = bytes.to_vec();
            completed.extend_from_slice(b"\r\n\r\n");
            assert!(
                old(&completed, max_fields).is_err(),
                "{bytes:?} rejected early as {error:?}"
            );
        }
        (parsed, expected) => panic!(
            "{bytes:?}: parser {:?}, previous {:?}",
            parsed.map(|p| p.map(|(_, n)| n)),
            expected.map(|p| p.map(|(_, n)| n))
        ),
    }
}

pub(crate) fn compare_response(bytes: &[u8], max_fields: usize) {
    compare_section(bytes, max_fields, crate::wire::response, response);
}

pub(crate) fn compare_trailers(bytes: &[u8], max_fields: usize) {
    compare_section(bytes, max_fields, crate::wire::trailers, trailers);
}

/// Check that the parser and the previous pipeline agree on `bytes`.
pub(crate) fn compare(bytes: &[u8], max_fields: usize) {
    compare_request(bytes, max_fields, crate::wire::request);
}

type RequestParser =
    fn(&[u8], usize) -> Result<Option<(RequestMetadata, RequestFraming, usize)>, Error>;

fn compare_request(bytes: &[u8], max_fields: usize, parser: RequestParser) {
    let parsed = parser(bytes, max_fields);
    let Some(end) = terminator(bytes) else {
        // Without a terminator the old reader waited for more input. The
        // parser may reject a prefix early, but only one no completion of
        // the head could make valid.
        if let Err(error) = parsed {
            let mut completed = bytes.to_vec();
            completed.extend_from_slice(b"\r\n\r\n");
            assert!(
                read(&completed, max_fields).is_err(),
                "{bytes:?} rejected early as {error:?}"
            );
        }
        return;
    };
    match (parsed, read(&bytes[..end], max_fields)) {
        (Ok(Some((head, framing, n))), Ok((expected_head, expected_framing))) => {
            assert_eq!(n, end, "{bytes:?}");
            assert_eq!(
                format!("{head:?}"),
                format!("{expected_head:?}"),
                "{bytes:?}"
            );
            assert_eq!(framing, expected_framing, "{bytes:?}");
            incomplete_before(&bytes[..end], |prefix| {
                matches!(parser(prefix, max_fields), Ok(None))
            });
        }
        (Err(_), Err(_)) => {}
        (parsed, expected) => panic!(
            "{bytes:?}: parser {:?}, pipeline {:?}",
            parsed.map(|p| p.map(|(_, _, n)| n)),
            expected.map(|_| ())
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        compare as compare_with, compare_request, compare_response, compare_section,
        compare_trailers, read, strict_lines,
    };
    use crate::{Error, wire};
    use base_http::{Fields, Method, RequestMetadata};

    const SEEDS: &[&[u8]] = &[
        b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n",
        b"POST /upload?x=1 HTTP/1.1\r\nHost: a:80\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n",
        b"PUT /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\nTrailer: x\r\n\r\n",
        b"GET http://Example.COM:8080/p?q HTTP/1.1\r\nHost: example.com:8080\r\n\r\n",
        b"GET https://example.com HTTP/1.1\r\nHost: example.com\r\n\r\n",
        b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n",
        b"OPTIONS * HTTP/1.0\r\n\r\n",
        b"GET /chat HTTP/1.1\r\nHost: h\r\nConnection: keep-alive, Upgrade, x-drop\r\nUpgrade: websocket\r\nX-Drop: 1\r\nUpgrade: h2c\r\n\r\n",
        b"BREW /pot HTTP/1.1\r\nhOsT: [::1]:80\r\nX-Empty:\r\nX-Spaces: \t a b \t\r\nX-Obs: \x80\xff\r\n\r\n",
        b"GET / HTTP/1.1\r\nHost: h\r\nKeep-Alive: 5\r\nTE: trailers\r\nProxy-Connection: x\r\nCookie: a=b; c=d\r\n\r\n",
        b"GET /a/long/path/beyond/the/inline/limit?with=query HTTP/1.1\r\nHost: a.long.host.name.beyond.inline:65535\r\nUser-Agent: Mozilla/5.0 (X11; Linux x86_64)\r\n\r\n",
        b"GET / HTTP/1.1\r\nHost: h\r\nCookie: session=4f1c2a9e8b7d6c5e4f3a2b1c0d9e8f7a; theme=dark; _ga=GA1.1.1234567890\r\nAccess-Control-Request-Headers: x\r\nAccept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8 \t\r\n\r\n",
    ];

    fn original_strict_lines(bytes: &[u8]) -> bool {
        bytes.iter().enumerate().all(|(i, b)| {
            !((*b == b'\n' && (i == 0 || bytes[i - 1] != b'\r'))
                || (*b == b'\r' && bytes.get(i + 1) != Some(&b'\n')))
        })
    }

    #[test]
    fn strict_lines_matches_pairwise_definition() {
        let alphabet = *b"a\r\n";
        for length in 0..=8 {
            for mut index in 0..alphabet.len().pow(length) {
                let bytes: Vec<u8> = (0..length)
                    .map(|_| {
                        let byte = alphabet[index % alphabet.len()];
                        index /= alphabet.len();
                        byte
                    })
                    .collect();
                assert_eq!(
                    strict_lines(&bytes).is_ok(),
                    original_strict_lines(&bytes),
                    "{bytes:?}"
                );
            }
        }
    }

    const ALPHABET: &[u8] = b"\r\n \t:\x00\x01\x7f\x80\xffAa0#\\/,;=@[]%?*";

    const MAX_FIELDS: usize = 5;

    fn compare(bytes: &[u8]) {
        compare_with(bytes, MAX_FIELDS);
        // Input usually continues past the head, which moves its last lines
        // from the word and byte tails onto the block scan.
        let mut pipelined = bytes.to_vec();
        pipelined.extend_from_slice(b"GET /next HTTP/1.1\r\nHost: h\r\n\r\n");
        compare_with(&pipelined, MAX_FIELDS);
    }

    /// Heads that intermediaries have historically framed differently, plus
    /// other invalid forms. Each must be rejected, as the previous pipeline did.
    #[test]
    fn ambiguous_and_invalid_heads_are_rejected() {
        let rejected: &[&[u8]] = &[
            // Line endings and folding.
            b"GET / HTTP/1.1\nHost: h\n\n",
            b"GET / HTTP/1.1\r\nHost: h\n\r\n",
            b"GET / HTTP/1.1\r\nHost: h\r\nX: a\rb\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: h\r\nX: a\nb\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: h\r\nX: a\r\n b\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: h\r\nX: a\r\n\tb\r\n\r\n",
            b"GET / HTTP/1.1\r\n Host: h\r\n\r\n",
            b"\r\nGET / HTTP/1.1\r\nHost: h\r\n\r\n",
            b" GET / HTTP/1.1\r\nHost: h\r\n\r\n",
            // Field names.
            b"GET / HTTP/1.1\r\nHost: h\r\nContent-Length : 5\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: h\r\nContent-Length\t: 5\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: h\r\n: v\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: h\r\nX\x00Y: v\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: h\r\nX(Y): v\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: h\r\nX\r\n\r\n",
            // Field values.
            b"GET / HTTP/1.1\r\nHost: h\r\nX: a\x00b\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: h\r\nX: a\x01b\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: h\r\nX: a\x7fb\r\n\r\n",
            // Message framing.
            b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 5, 5\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: +5\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: -1\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 0x5\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 5 5\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length:\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 99999999999999999999\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: gzip\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: gzip, chunked\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked, chunked\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: xchunked\r\n\r\n",
            // Host.
            b"GET / HTTP/1.1\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: a\r\nHost: a\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: a, b\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: a b\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: a/b\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: user@a\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: a:99999\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: a:\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost:\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: \xc3\xa9\r\n\r\n",
            // Request line.
            b"GET  / HTTP/1.1\r\nHost: h\r\n\r\n",
            b"GET /  HTTP/1.1\r\nHost: h\r\n\r\n",
            b"GET\t/ HTTP/1.1\r\nHost: h\r\n\r\n",
            b"GET / HTTP/1.1 \r\nHost: h\r\n\r\n",
            b"GET / HTTP/1.2\r\nHost: h\r\n\r\n",
            b"GET / HTTP/2.0\r\nHost: h\r\n\r\n",
            b"GET / http/1.1\r\nHost: h\r\n\r\n",
            b"GET /\r\nHost: h\r\n\r\n",
            b"G(T / HTTP/1.1\r\nHost: h\r\n\r\n",
            b"GET /a#b HTTP/1.1\r\nHost: h\r\n\r\n",
            b"GET /a\\b HTTP/1.1\r\nHost: h\r\n\r\n",
            b"GET /\xc3\xa9 HTTP/1.1\r\nHost: h\r\n\r\n",
            b"GET ftp://h/ HTTP/1.1\r\nHost: h\r\n\r\n",
            b"GET h/ HTTP/1.1\r\nHost: h\r\n\r\n",
            b"GET * HTTP/1.1\r\nHost: h\r\n\r\n",
            b"CONNECT h HTTP/1.1\r\nHost: h\r\n\r\n",
            b"CONNECT /h HTTP/1.1\r\nHost: h\r\n\r\n",
        ];
        for bytes in rejected {
            assert!(wire::request(bytes, MAX_FIELDS).is_err(), "{bytes:?}");
            assert!(read(bytes, MAX_FIELDS).is_err(), "{bytes:?}");
        }
        let accepted: &[&[u8]] = &[
            b"GET / HTTP/1.0\r\n\r\n",
            b"get / HTTP/1.1\r\nhost: h\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: \t5 \t\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: CHUNKED\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: h\r\nX: \x80\xff\t\x7e\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: h\r\nX:\r\n\r\n",
        ];
        for bytes in accepted {
            compare(bytes);
            assert!(read(bytes, MAX_FIELDS).is_ok(), "{bytes:?}");
        }
    }

    #[test]
    fn field_count_limit_applies_at_the_boundary() {
        let head = |count: usize| {
            let mut head = b"GET / HTTP/1.1\r\nHost: h\r\n".to_vec();
            for _ in 1..count {
                head.extend_from_slice(b"X: v\r\n");
            }
            head.extend_from_slice(b"\r\n");
            head
        };
        assert!(
            wire::request(&head(MAX_FIELDS), MAX_FIELDS)
                .unwrap()
                .is_some()
        );
        assert!(matches!(
            wire::request(&head(MAX_FIELDS + 1), MAX_FIELDS),
            Err(crate::Error::Limit("field count"))
        ));
        assert!(matches!(
            read(&head(MAX_FIELDS + 1), MAX_FIELDS),
            Err(crate::Error::Limit("field count"))
        ));
    }

    #[test]
    fn response_and_trailer_field_limits_apply_at_the_boundary() {
        let lines = |count: usize| {
            let mut lines = b"X: v\r\n".repeat(count);
            lines.extend_from_slice(b"\r\n");
            lines
        };
        let response = |count: usize| [b"HTTP/1.1 200 OK\r\n".as_slice(), &lines(count)].concat();
        for count in [MAX_FIELDS, MAX_FIELDS + 1] {
            compare_response(&response(count), MAX_FIELDS);
            compare_trailers(&lines(count), MAX_FIELDS);
        }
        let (head, end) = super::response(&response(MAX_FIELDS), MAX_FIELDS)
            .unwrap()
            .unwrap();
        assert_eq!(head.fields.len(), MAX_FIELDS);
        assert_eq!(end, response(MAX_FIELDS).len());
        assert!(matches!(
            super::response(&response(MAX_FIELDS + 1), MAX_FIELDS),
            Err(Error::Limit("field count"))
        ));
        let (fields, end) = super::trailers(&lines(MAX_FIELDS), MAX_FIELDS)
            .unwrap()
            .unwrap();
        assert_eq!(fields.len(), MAX_FIELDS);
        assert_eq!(end, lines(MAX_FIELDS).len());
        assert!(matches!(
            super::trailers(&lines(MAX_FIELDS + 1), MAX_FIELDS),
            Err(Error::Limit("trailer count"))
        ));
    }

    /// Run a comparison expected to detect a divergent parser and return its panic message.
    fn divergence(compare: impl FnOnce() + std::panic::UnwindSafe) -> String {
        let payload =
            std::panic::catch_unwind(compare).expect_err("the oracle accepted a divergent parser");
        payload
            .downcast_ref::<String>()
            .cloned()
            .expect("the oracle reports a formatted message")
    }

    const TRAILER: &[u8] = b"X: v\r\n\r\n";

    #[test]
    fn section_comparison_detects_divergent_parsers() {
        // Accepting only a complete section rejects its valid prefixes.
        let message = divergence(|| {
            compare_section::<Fields>(
                TRAILER,
                MAX_FIELDS,
                |bytes, max_fields| match wire::trailers(bytes, max_fields) {
                    Ok(None) => Err(Error::Invalid("early rejection")),
                    result => result,
                },
                super::trailers,
            )
        });
        assert!(message.contains("rejected as a prefix"), "{message}");
        let message = divergence(|| {
            compare_section::<Fields>(
                b"X\r\n\r\n",
                MAX_FIELDS,
                |_, _| Ok(Some((Fields::new(), 3))),
                super::trailers,
            )
        });
        assert!(
            message.contains("parser Ok(Some(3)), previous Err("),
            "{message}"
        );
        let message = divergence(|| {
            compare_section::<Fields>(TRAILER, MAX_FIELDS, |_, _| Ok(None), super::trailers)
        });
        assert!(
            message.contains("parser Ok(None), previous Ok(Some(8))"),
            "{message}"
        );
    }

    fn accept_everything(
        bytes: &[u8],
        _: usize,
    ) -> Result<Option<(RequestMetadata, wire::RequestFraming, usize)>, Error> {
        let framing = wire::RequestFraming {
            length: None,
            chunked: false,
            persistent: true,
            expect_continue: false,
            trailer_options: Vec::new(),
            upgrade: None,
        };
        Ok(Some((
            RequestMetadata::new(Method::GET),
            framing,
            bytes.len(),
        )))
    }

    #[test]
    fn request_comparison_detects_divergent_parsers() {
        let head = SEEDS[0];
        // Accepting only a complete head rejects its valid prefixes.
        let message = divergence(|| {
            compare_request(head, MAX_FIELDS, |bytes, max_fields| {
                match wire::request(bytes, max_fields) {
                    Ok(None) => Err(Error::Invalid("early rejection")),
                    result => result,
                }
            })
        });
        assert!(message.contains("rejected as a prefix"), "{message}");
        let message = divergence(|| {
            compare_request(b"GET / HTTP/1.1\r\n\r\n", MAX_FIELDS, accept_everything)
        });
        assert!(
            message.contains("parser Ok(Some(18)), pipeline Err("),
            "{message}"
        );
        let message = divergence(|| compare_request(head, MAX_FIELDS, |_, _| Ok(None)));
        assert!(
            message.contains("parser Ok(None), pipeline Ok(())"),
            "{message}"
        );
    }

    const RESPONSES: &[&[u8]] = &[
        b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n",
        b"HTTP/1.0 404 Not Found\r\n\r\n",
        b"HTTP/1.1 204\r\nX-Empty:\r\n\r\n",
        b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n",
        b"HTTP/1.1 100 \r\n\r\n",
        b"HTTP/1.1 503 Service \t Unavailable \x80\xff\r\nRetry-After: 5\r\nX-Spaces: \t a b \t\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nSet-Cookie: session=4f1c2a9e8b7d6c5e4f3a2b1c0d9e8f7a; Path=/; HttpOnly\r\nAccess-Control-Allow-Headers: x\r\nTransfer-Encoding: chunked\r\n\r\n",
    ];

    const TRAILERS: &[&[u8]] = &[
        b"\r\n",
        b"X-Checksum: abc\r\n\r\n",
        b"X-A: 1\r\nx-b:\r\nX-Spaces: \t a b \t\r\nX-Obs: \x80\xff\r\n\r\n",
        b"Server-Timing: total;dur=123.4, db;dur=56.7, cache;desc=\"hit\"\r\nAccess-Control-Request-Headers: x\r\n\r\n",
    ];

    fn mutate_once(seeds: &[&[u8]], compare: impl Fn(&[u8])) {
        for seed in seeds {
            compare(seed);
            for at in 0..=seed.len() {
                compare(&seed[..at]);
                for byte in ALPHABET {
                    let mut inserted = seed.to_vec();
                    inserted.insert(at, *byte);
                    compare(&inserted);
                    if at < seed.len() {
                        let mut replaced = seed.to_vec();
                        replaced[at] = *byte;
                        compare(&replaced);
                    }
                }
                if at < seed.len() {
                    let mut removed = seed.to_vec();
                    removed.remove(at);
                    compare(&removed);
                }
            }
        }
    }

    fn mutate_randomly(seeds: &[&[u8]], cases: usize, compare: impl Fn(&[u8])) {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move |bound: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % bound as u64) as usize
        };
        for _ in 0..cases {
            let mut bytes = seeds[next(seeds.len())].to_vec();
            for _ in 0..1 + next(4) {
                let at = next(bytes.len() + 1);
                let byte = ALPHABET[next(ALPHABET.len())];
                match next(3) {
                    0 if at < bytes.len() => bytes[at] = byte,
                    1 if at < bytes.len() => {
                        bytes.remove(at);
                    }
                    _ => bytes.insert(at, byte),
                }
            }
            compare(&bytes);
        }
    }

    /// Compare a section alone and followed by further input, which moves its
    /// last lines from the word and byte tails onto the block scan.
    fn with_and_without_more(bytes: &[u8], compare: impl Fn(&[u8], usize), more: &[u8]) {
        compare(bytes, MAX_FIELDS);
        let mut continued = bytes.to_vec();
        continued.extend_from_slice(more);
        compare(&continued, MAX_FIELDS);
    }

    fn compare_responses(bytes: &[u8]) {
        with_and_without_more(bytes, compare_response, b"chunk data");
    }

    fn compare_trailer_sections(bytes: &[u8]) {
        with_and_without_more(
            bytes,
            compare_trailers,
            b"GET /next HTTP/1.1\r\nHost: h\r\n\r\n",
        );
    }

    #[test]
    fn parser_matches_the_previous_pipeline_on_single_mutations() {
        mutate_once(SEEDS, compare);
        mutate_once(RESPONSES, compare_responses);
        mutate_once(TRAILERS, compare_trailer_sections);
    }

    #[test]
    fn parser_matches_the_previous_pipeline_on_random_mutations() {
        mutate_randomly(SEEDS, 200_000, compare);
        mutate_randomly(RESPONSES, 100_000, compare_responses);
        mutate_randomly(TRAILERS, 100_000, compare_trailer_sections);
    }
}

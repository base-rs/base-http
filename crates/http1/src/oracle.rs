// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! The request head pipeline used before the single-pass parser: line checks,
//! httparse, then field classification. Tests compare the parser with it.

use crate::Error;
use crate::wire::*;
use base_http::{Method, Protocol, RequestMetadata, Version};

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
    let method = match parsed.method.ok_or(Error::Invalid("missing method"))? {
        "GET" => Method::GET,
        "HEAD" => Method::HEAD,
        "POST" => Method::POST,
        "PUT" => Method::PUT,
        "DELETE" => Method::DELETE,
        "CONNECT" => Method::CONNECT,
        "OPTIONS" => Method::OPTIONS,
        "TRACE" => Method::TRACE,
        "PATCH" => Method::PATCH,
        "QUERY" => Method::QUERY,
        other => Method::new(other).map_err(|_| Error::Invalid("invalid method"))?,
    };
    let target = parsed.path.ok_or(Error::Invalid("missing target"))?;
    request_target(&method, target)?;
    let version = match parsed.version {
        Some(0) => Version::Http10,
        Some(1) => Version::Http11,
        _ => return Err(Error::Invalid("unsupported HTTP version")),
    };
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
                head.fields.add(field.name, field.value)?;
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
    if head.method == Method::CONNECT {
        authority(target, true)?;
        head.authority = Some(target.into());
    } else if target.starts_with('/') || (target == "*" && head.method == Method::OPTIONS) {
        head.path = Some(target.into());
    } else if let Some((scheme, rest)) = target.split_once("://") {
        if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
            return Err(Error::Invalid("unsupported target scheme"));
        }
        let end = rest.find(['/', '?']).unwrap_or(rest.len());
        authority(&rest[..end], false)?;
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
    } else {
        return Err(Error::Invalid("invalid request target form"));
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

/// Check that the parser and the previous pipeline agree on `bytes`.
pub(crate) fn compare(bytes: &[u8], max_fields: usize) {
    let parsed = crate::wire::request(bytes, max_fields);
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
    use super::{compare as compare_with, read};
    use crate::wire;

    const SEEDS: &[&[u8]] = &[
        b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n",
        b"POST /upload?x=1 HTTP/1.1\r\nHost: a:80\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n",
        b"PUT /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\nTrailer: x\r\n\r\n",
        b"GET http://Example.COM:8080/p?q HTTP/1.1\r\nHost: example.com:8080\r\n\r\n",
        b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n",
        b"OPTIONS * HTTP/1.0\r\n\r\n",
        b"GET /chat HTTP/1.1\r\nHost: h\r\nConnection: keep-alive, Upgrade, x-drop\r\nUpgrade: websocket\r\nX-Drop: 1\r\nUpgrade: h2c\r\n\r\n",
        b"BREW /pot HTTP/1.1\r\nhOsT: [::1]:80\r\nX-Empty:\r\nX-Spaces: \t a b \t\r\nX-Obs: \x80\xff\r\n\r\n",
        b"GET / HTTP/1.1\r\nHost: h\r\nKeep-Alive: 5\r\nTE: trailers\r\nProxy-Connection: x\r\nCookie: a=b; c=d\r\n\r\n",
        b"GET /a/long/path/beyond/the/inline/limit?with=query HTTP/1.1\r\nHost: a.long.host.name.beyond.inline:65535\r\nUser-Agent: Mozilla/5.0 (X11; Linux x86_64)\r\n\r\n",
        b"GET / HTTP/1.1\r\nHost: h\r\nCookie: session=4f1c2a9e8b7d6c5e4f3a2b1c0d9e8f7a; theme=dark; _ga=GA1.1.1234567890\r\nAccess-Control-Request-Headers: x\r\nAccept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8 \t\r\n\r\n",
    ];

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
    fn parser_matches_the_previous_pipeline_on_single_mutations() {
        for seed in SEEDS {
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

    #[test]
    fn parser_matches_the_previous_pipeline_on_random_mutations() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move |bound: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % bound as u64) as usize
        };
        for _ in 0..200_000 {
            let mut bytes = SEEDS[next(SEEDS.len())].to_vec();
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
}

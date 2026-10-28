// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Error;
use base_http::{Fields, Method, Protocol, RequestHead, Version};

pub(crate) fn token(bytes: &[u8]) -> bool {
    !bytes.is_empty()
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(b))
}

pub(crate) fn has_token(fields: &Fields, name: &str, token: &[u8]) -> bool {
    fields
        .headers()
        .iter()
        .filter(|f| f.name().eq_ignore_ascii_case(name))
        .flat_map(|f| f.value().split(|b| *b == b','))
        .any(|part| trim(part).eq_ignore_ascii_case(token))
}

pub(crate) fn trim(mut value: &[u8]) -> &[u8] {
    while value.first().is_some_and(|b| matches!(b, b' ' | b'\t')) {
        value = &value[1..];
    }
    while value.last().is_some_and(|b| matches!(b, b' ' | b'\t')) {
        value = &value[..value.len() - 1];
    }
    value
}

pub(crate) fn singleton<'a>(fields: &'a Fields, name: &str) -> Result<Option<&'a [u8]>, Error> {
    let mut values = fields
        .headers()
        .iter()
        .filter(|f| f.name().eq_ignore_ascii_case(name));
    let value = values.next().map(|f| trim(f.value()));
    if values.next().is_some() {
        return Err(Error::Invalid("duplicate singleton field"));
    }
    Ok(value)
}

pub(crate) fn framing(fields: &Fields) -> Result<(Option<u64>, bool), Error> {
    parse_framing(
        singleton(fields, "content-length")?,
        singleton(fields, "transfer-encoding")?,
    )
}

fn parse_framing(
    length: Option<&[u8]>,
    transfer: Option<&[u8]>,
) -> Result<(Option<u64>, bool), Error> {
    let length = length
        .map(|bytes| {
            if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
                return Err(Error::Invalid("invalid content-length"));
            }
            std::str::from_utf8(bytes)
                .unwrap()
                .parse::<u64>()
                .map_err(|_| Error::Invalid("content-length overflow"))
        })
        .transpose()?;
    if transfer.is_some() && length.is_some() {
        return Err(Error::Invalid("content-length with transfer-encoding"));
    }
    if transfer.is_some_and(|v| !v.eq_ignore_ascii_case(b"chunked")) {
        return Err(Error::Invalid("unsupported transfer coding"));
    }
    Ok((length, transfer.is_some()))
}

/// Reject lenient line endings and whitespace before a field name. This check
/// complements httparse, which intentionally accepts some interoperable forms.
pub(crate) fn strict_lines(bytes: &[u8]) -> Result<(), Error> {
    for (i, b) in bytes.iter().enumerate() {
        if (*b == b'\n' && (i == 0 || bytes[i - 1] != b'\r'))
            || (*b == b'\r' && bytes.get(i + 1) != Some(&b'\n'))
        {
            return Err(Error::Invalid("expected CRLF"));
        }
    }
    Ok(())
}

pub(crate) fn fields(headers: &[httparse::Header<'_>]) -> Result<Fields, Error> {
    let mut fields = Fields::new();
    for field in headers {
        fields.add(field.name, field.value)?;
    }
    Ok(fields)
}

fn authority(value: &str, require_port: bool) -> Result<(), Error> {
    if value.is_empty()
        || !value.is_ascii()
        || value
            .bytes()
            .any(|b| b <= 32 || b == 127 || b"/?#@\\,".contains(&b))
    {
        return Err(Error::Invalid("invalid authority"));
    }
    let port = if let Some(value) = value.strip_prefix('[') {
        let (host, rest) = value
            .split_once(']')
            .ok_or(Error::Invalid("invalid IPv6 authority"))?;
        host.parse::<std::net::Ipv6Addr>()
            .map_err(|_| Error::Invalid("invalid IPv6 authority"))?;
        if rest.is_empty() {
            None
        } else {
            Some(
                rest.strip_prefix(':')
                    .ok_or(Error::Invalid("invalid port"))?,
            )
        }
    } else {
        let (host, port) = value
            .split_once(':')
            .map_or((value, None), |(h, p)| (h, Some(p)));
        if !host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-._~%!$&'()*+;=".contains(&b))
        {
            return Err(Error::Invalid("invalid authority host"));
        }
        if host.is_empty() {
            return Err(Error::Invalid("missing authority host"));
        }
        port
    };
    if let Some(port) = port {
        if port.is_empty()
            || !port.bytes().all(|b| b.is_ascii_digit())
            || port.parse::<u16>().is_err()
        {
            return Err(Error::Invalid("invalid port"));
        }
    } else if require_port {
        return Err(Error::Invalid("CONNECT requires a port"));
    }
    Ok(())
}

pub(crate) struct RequestFraming {
    pub length: Option<u64>,
    pub chunked: bool,
    pub persistent: bool,
    pub expect_continue: bool,
    pub upgrade: Option<Box<[u8]>>,
}

fn set_singleton<'a>(slot: &mut Option<&'a [u8]>, value: &'a [u8]) -> Result<(), Error> {
    if slot.replace(trim(value)).is_some() {
        return Err(Error::Invalid("duplicate singleton field"));
    }
    Ok(())
}

pub(crate) fn request(
    parsed: &httparse::Request<'_, '_>,
) -> Result<(RequestHead, RequestFraming), Error> {
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
    if target.contains('#') || target.contains('\\') {
        return Err(Error::Invalid("invalid request target"));
    }
    let version = match parsed.version {
        Some(0) => Version::Http10,
        Some(1) => Version::Http11,
        _ => return Err(Error::Invalid("unsupported HTTP version")),
    };
    let mut head = RequestHead::new(method);
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
    for field in parsed.headers.iter() {
        head.fields.add(field.name, field.value)?;
        let name = field.name;
        if name.eq_ignore_ascii_case("host") {
            set_singleton(&mut host, field.value)?;
        } else if name.eq_ignore_ascii_case("content-length") {
            set_singleton(&mut length, field.value)?;
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            set_singleton(&mut transfer, field.value)?;
        } else if name.eq_ignore_ascii_case("connection") {
            for option in field.value.split(|b| *b == b',').map(trim) {
                close |= option.eq_ignore_ascii_case(b"close");
                keep_alive |= option.eq_ignore_ascii_case(b"keep-alive");
                connection_upgrade |= option.eq_ignore_ascii_case(b"upgrade");
            }
        } else if name.eq_ignore_ascii_case("expect") {
            expect_continue |= field
                .value
                .split(|b| *b == b',')
                .any(|v| trim(v).eq_ignore_ascii_case(b"100-continue"));
        } else if name.eq_ignore_ascii_case("upgrade") {
            first_upgrade.get_or_insert(field.value);
            upgrade_count += 1;
        }
    }
    let host = host
        .map(|v| std::str::from_utf8(v).map_err(|_| Error::Invalid("invalid host")))
        .transpose()?;
    if version == Version::Http11 && host.is_none() {
        return Err(Error::Invalid("missing host"));
    }
    if let Some(host) = host {
        authority(host, false)?;
        head.authority = Some(host.to_owned());
    }
    if head.method == Method::CONNECT {
        authority(target, true)?;
        head.authority = Some(target.to_owned());
    } else if target.starts_with('/') || (target == "*" && head.method == Method::OPTIONS) {
        head.path = Some(target.to_owned());
    } else if let Some((scheme, rest)) = target.split_once("://") {
        if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
            return Err(Error::Invalid("unsupported target scheme"));
        }
        let end = rest.find(['/', '?']).unwrap_or(rest.len());
        authority(&rest[..end], false)?;
        head.authority = Some(rest[..end].to_owned());
        head.scheme = Some(scheme.to_owned());
        let suffix = &rest[end..];
        head.path = Some(if suffix.is_empty() {
            "/".to_owned()
        } else if suffix.starts_with('?') {
            format!("/{suffix}")
        } else {
            suffix.to_owned()
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
    let framing = RequestFraming {
        length,
        chunked,
        persistent: !close && (version == Version::Http11 || keep_alive),
        expect_continue,
        upgrade,
    };
    Ok((head, framing))
}

pub(crate) fn append_fields(bytes: &mut Vec<u8>, fields: &[base_http::Field]) {
    for field in fields {
        bytes.extend_from_slice(field.name().as_bytes());
        bytes.extend_from_slice(b": ");
        bytes.extend_from_slice(field.value());
        bytes.extend_from_slice(b"\r\n");
    }
}

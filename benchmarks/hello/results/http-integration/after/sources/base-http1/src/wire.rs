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
    let length = singleton(fields, "content-length")?
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
    let transfer = singleton(fields, "transfer-encoding")?;
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

pub(crate) fn request(parsed: &httparse::Request<'_, '_>) -> Result<RequestHead, Error> {
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
    head.fields = fields(parsed.headers)?;
    let host = singleton(&head.fields, "host")?
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
    if version == Version::Http11 && has_token(&head.fields, "connection", b"upgrade") {
        let mut offers = head
            .fields
            .headers()
            .iter()
            .filter(|f| f.name().eq_ignore_ascii_case("upgrade"));
        let offer = offers
            .next()
            .ok_or(Error::Invalid("missing upgrade protocol"))?
            .value();
        // Preserve all raw offers. The semantic shorthand represents one token.
        if let (Ok(offer), None) = (std::str::from_utf8(trim(offer)), offers.next()) {
            head.protocol = Protocol::new(offer).ok();
        }
    }
    Ok(head)
}

pub(crate) fn append_fields(bytes: &mut Vec<u8>, fields: &[base_http::Field]) {
    for field in fields {
        bytes.extend_from_slice(field.name().as_bytes());
        bytes.extend_from_slice(b": ");
        bytes.extend_from_slice(field.value());
        bytes.extend_from_slice(b"\r\n");
    }
}

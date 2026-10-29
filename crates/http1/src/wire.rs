// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Error, parse};
use base_http::{Fields, Method, Protocol, RequestMetadata, Version};
use smallvec::SmallVec;

pub(crate) fn token(bytes: &[u8]) -> bool {
    !bytes.is_empty()
        && bytes.iter().all(|b| {
            b.is_ascii_alphanumeric()
                || matches!(
                    b,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

pub(crate) fn has_token(fields: &Fields, name: &str, token: &[u8]) -> bool {
    fields
        .pairs()
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
        .pairs()
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

pub(crate) fn parse_framing(
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

pub(crate) fn fields(headers: &[httparse::Header<'_>]) -> Result<Fields, Error> {
    let mut fields = Fields::new();
    for field in headers {
        fields.add(field.name, field.value)?;
    }
    Ok(fields)
}

/// Received values may contain SP, HTAB, VCHAR and obs-text (RFC 9110, section
/// 5.5), the bytes httparse accepts in heads.
pub(crate) fn field_value(value: &[u8]) -> bool {
    value.iter().fold(true, |valid, b| {
        valid & (*b == b'\t' || *b >= b' ' && *b != 0x7f)
    })
}

/// CR, LF and NUL change how HTTP/1 text is framed or interpreted, so no sent
/// value may contain them (RFC 9110, section 5.5). Other bytes pass through.
pub(crate) fn output_fields(fields: &[base_http::Field]) -> Result<(), Error> {
    if fields.iter().any(base_http::Field::has_dangerous_bytes) {
        return Err(Error::Invalid("field value contains CR, LF or NUL"));
    }
    Ok(())
}

/// Bytes permitted in a registered name: unreserved, sub-delims except comma,
/// and percent-encoding.
const REG_NAME: [bool; 256] = {
    let mut table = [false; 256];
    let mut byte = 0;
    while byte < 256 {
        let b = byte as u8;
        table[byte] = b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'-' | b'.'
                    | b'_'
                    | b'~'
                    | b'%'
                    | b'!'
                    | b'$'
                    | b'&'
                    | b'\''
                    | b'('
                    | b')'
                    | b'*'
                    | b'+'
                    | b';'
                    | b'='
            );
        byte += 1;
    }
    table
};

/// Accept the common `reg-name[:port]` form. Returns false when the value is
/// invalid or uses another form, leaving the complete validation to select the
/// error. The scans do not short-circuit, so they compile without a branch per
/// byte.
#[inline]
fn simple_authority(value: &[u8], require_port: bool) -> bool {
    let (host, port) = match value.iter().rposition(|b| *b == b':') {
        Some(colon) => (&value[..colon], Some(&value[colon + 1..])),
        None => (value, None),
    };
    if host.is_empty()
        || !host
            .iter()
            .fold(true, |valid, b| valid & REG_NAME[usize::from(*b)])
    {
        return false;
    }
    match port {
        None => !require_port,
        Some(port) => {
            if port.is_empty() || port.len() > 5 {
                return false;
            }
            let (number, digits) = port.iter().fold((0u32, true), |(number, digits), b| {
                (
                    number * 10 + u32::from(b.wrapping_sub(b'0')),
                    digits & b.is_ascii_digit(),
                )
            });
            digits && number <= u32::from(u16::MAX)
        }
    }
}

pub(crate) fn authority(value: &str, require_port: bool) -> Result<(), Error> {
    if simple_authority(value.as_bytes(), require_port) {
        return Ok(());
    }
    if value.is_empty()
        || !value.is_ascii()
        || value
            .bytes()
            .any(|b| b <= 32 || b == 127 || matches!(b, b'/' | b'?' | b'#' | b'@' | b'\\' | b','))
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
        if !host.bytes().all(|b| REG_NAME[usize::from(b)]) {
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

/// Validate the four request-target forms without allocating semantic metadata.
/// Used by both incoming parsing and outgoing encoding.
pub(crate) fn request_target(method: &Method, target: &str) -> Result<(), Error> {
    if target.is_empty()
        || !target.is_ascii()
        || target
            .bytes()
            .any(|b| b <= 32 || b == 127 || matches!(b, b'#' | b'\\'))
    {
        return Err(Error::Invalid("invalid request target"));
    }
    target_form(method, target)
}

/// Check which of the four request-target forms a target with valid bytes
/// takes. The parser has already checked received bytes.
pub(crate) fn target_form(method: &Method, target: &str) -> Result<(), Error> {
    if *method == Method::CONNECT {
        authority(target, true)
    } else if target.starts_with('/') || (target == "*" && *method == Method::OPTIONS) {
        Ok(())
    } else if let Some((scheme, rest)) = target.split_once("://") {
        if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
            return Err(Error::Invalid("unsupported target scheme"));
        }
        let end = rest.find(['/', '?']).unwrap_or(rest.len());
        authority(&rest[..end], false)
    } else {
        Err(Error::Invalid("invalid request target form"))
    }
}

/// Validate Connection's token syntax without constructing a filtered message.
pub(crate) fn connection_options(fields: &Fields) -> Result<(), Error> {
    for value in fields.values("connection") {
        for option in value
            .split(|b| *b == b',')
            .map(trim)
            .filter(|v| !v.is_empty())
        {
            if !token(option) {
                return Err(Error::Invalid("invalid connection option"));
            }
        }
    }
    Ok(())
}

pub(crate) struct ResponseFraming<'a> {
    pub length: Option<u64>,
    pub chunked: bool,
    pub close: bool,
    pub keep_alive: bool,
    pub connection_upgrade: bool,
    pub trailer: bool,
    pub upgrade: Option<&'a [u8]>,
    pub duplicate_upgrade: bool,
}

pub(crate) fn response_framing(fields: &Fields) -> Result<ResponseFraming<'_>, Error> {
    let mut length = None;
    let mut transfer = None;
    let mut facts = ResponseFraming {
        length: None,
        chunked: false,
        close: false,
        keep_alive: false,
        connection_upgrade: false,
        trailer: false,
        upgrade: None,
        duplicate_upgrade: false,
    };
    for field in fields.pairs() {
        let name = field.name();
        if name.eq_ignore_ascii_case("content-length") {
            set_singleton(&mut length, field.value())?;
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            set_singleton(&mut transfer, field.value())?;
        } else if name.eq_ignore_ascii_case("connection") {
            for option in field.value().split(|b| *b == b',').map(trim) {
                facts.close |= option.eq_ignore_ascii_case(b"close");
                facts.keep_alive |= option.eq_ignore_ascii_case(b"keep-alive");
                facts.connection_upgrade |= option.eq_ignore_ascii_case(b"upgrade");
            }
        } else if name.eq_ignore_ascii_case("trailer") {
            facts.trailer = true;
        } else if name.eq_ignore_ascii_case("upgrade") {
            facts.duplicate_upgrade |= facts.upgrade.replace(trim(field.value())).is_some();
        }
    }
    (facts.length, facts.chunked) = parse_framing(length, transfer)?;
    Ok(facts)
}

#[derive(Debug, PartialEq)]
pub(crate) struct RequestFraming {
    pub length: Option<u64>,
    pub chunked: bool,
    pub persistent: bool,
    pub expect_continue: bool,
    pub trailer_options: Vec<Vec<u8>>,
    pub upgrade: Option<Box<[u8]>>,
}

pub(crate) fn set_singleton<'a>(slot: &mut Option<&'a [u8]>, value: &'a [u8]) -> Result<(), Error> {
    if slot.replace(trim(value)).is_some() {
        return Err(Error::Invalid("duplicate singleton field"));
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(crate) enum Control {
    Host,
    ContentLength,
    TransferEncoding,
    Connection,
    Upgrade,
    Removed,
}

/// Classify request fields consumed by HTTP/1 framing rather than retained as
/// application fields.
pub(crate) fn request_control(name: &[u8]) -> Option<Control> {
    let is = |control: &[u8]| name.eq_ignore_ascii_case(control);
    match name.len() {
        2 if is(b"te") => Some(Control::Removed),
        4 if is(b"host") => Some(Control::Host),
        7 if is(b"upgrade") => Some(Control::Upgrade),
        10 if is(b"connection") => Some(Control::Connection),
        10 if is(b"keep-alive") => Some(Control::Removed),
        14 if is(b"content-length") => Some(Control::ContentLength),
        16 if is(b"proxy-connection") => Some(Control::Removed),
        17 if is(b"transfer-encoding") => Some(Control::TransferEncoding),
        _ => None,
    }
}

/// Parse a request head from the start of `bytes`, returning the metadata,
/// framing facts and head length. `Ok(None)` means the head is incomplete.
pub(crate) fn request(
    bytes: &[u8],
    max_fields: usize,
) -> Result<Option<(RequestMetadata, RequestFraming, usize)>, Error> {
    let Some((line, at)) = parse::request_line(bytes)? else {
        return Ok(None);
    };
    let method = match line.method {
        b"GET" => Method::GET,
        b"HEAD" => Method::HEAD,
        b"POST" => Method::POST,
        b"PUT" => Method::PUT,
        b"DELETE" => Method::DELETE,
        b"CONNECT" => Method::CONNECT,
        b"OPTIONS" => Method::OPTIONS,
        b"TRACE" => Method::TRACE,
        b"PATCH" => Method::PATCH,
        b"QUERY" => Method::QUERY,
        // The parser accepts only token bytes, which are ASCII.
        other => Method::new(std::str::from_utf8(other).unwrap())
            .map_err(|_| Error::Invalid("invalid method"))?,
    };
    // Validate every line before interpreting any, recording where each
    // field is, so that one UTF-8 check covers the target and all names.
    let mut lines = parse::Lines::new(bytes, at, max_fields);
    let mut fields: SmallVec<[parse::Field; 16]> = SmallVec::new();
    let length_of_head = loop {
        match lines.next()? {
            parse::Line::Field(field) => fields.push(field),
            parse::Line::End(n) => break n,
            parse::Line::Incomplete => return Ok(None),
        }
    };
    // obs-text that is not UTF-8 leaves each piece to be converted separately.
    let text = std::str::from_utf8(&bytes[..length_of_head]).ok();
    let target = line.target.ascii(text, bytes);
    target_form(&method, target)?;
    let version = line.version;
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
    let mut offers = Vec::new();
    for field in &fields {
        let name = field.name.of(bytes);
        let value = field.value.of(bytes);
        // The parser has already applied the field name and value syntax. Wire
        // controls and Host are captured here instead of being stored and then
        // removed from the application fields.
        match request_control(name) {
            Some(Control::Host) => {
                if host.replace(field.value).is_some() {
                    return Err(Error::Invalid("duplicate singleton field"));
                }
            }
            Some(Control::ContentLength) => set_singleton(&mut length, value)?,
            Some(Control::TransferEncoding) => set_singleton(&mut transfer, value)?,
            Some(Control::Connection) => {
                for option in value.split(|b| *b == b',').map(trim) {
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
                first_upgrade.get_or_insert(value);
                upgrade_count += 1;
                if !offers.is_empty() {
                    offers.push(b',');
                }
                offers.extend_from_slice(value);
            }
            Some(Control::Removed) => {}
            None => {
                if name.eq_ignore_ascii_case(b"expect") {
                    expect_continue |= value
                        .split(|b| *b == b',')
                        .any(|v| trim(v).eq_ignore_ascii_case(b"100-continue"));
                }
                head.fields.add(field.name.ascii(text, bytes), value)?;
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
        .map(|host| host.text(text, bytes).ok_or(Error::Invalid("invalid host")))
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
    Ok(Some((head, framing, length_of_head)))
}

/// Remove wire controls from a response after their values have been validated and captured.
/// Scan Connection first, so nominated pairs are removed regardless of order. Requests classify
/// the same controls, plus Host, with `request_control` while their fields are read.
pub(crate) fn application_fields(mut fields: Fields) -> Result<(Fields, Vec<Vec<u8>>), Error> {
    let options: Vec<Vec<u8>> = fields
        .values("connection")
        .flat_map(|value| value.split(|b| *b == b','))
        .map(trim)
        // RFC 9110 list recipients ignore empty elements within parser limits.
        .filter(|option| !option.is_empty())
        .map(|option| {
            if !token(option) {
                return Err(Error::Invalid("invalid connection option"));
            }
            Ok(option.to_vec())
        })
        .collect::<Result<_, _>>()?;
    fields.retain(|field| {
        let name = field.name();
        !([
            "connection",
            "keep-alive",
            "proxy-connection",
            "transfer-encoding",
            "content-length",
            "upgrade",
            "te",
        ]
        .iter()
        .any(|control| name.eq_ignore_ascii_case(control))
            || options
                .iter()
                .any(|option| name.as_bytes().eq_ignore_ascii_case(option)))
    });
    Ok((fields, options))
}

pub(crate) fn declared_length(typed: Option<u64>, raw: Option<u64>) -> Result<Option<u64>, Error> {
    if typed.is_some() && raw.is_some() && typed != raw {
        return Err(Error::Invalid("conflicting content-length"));
    }
    Ok(typed.or(raw))
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn simple_authority_only_accepts_valid_authorities() {
        for (value, require_port) in [
            ("example", false),
            ("example:80", false),
            ("127.0.0.1:65535", true),
            ("example:0", false),
            ("example:65535", false),
            ("example:00080", false),
            ("example.", false),
            ("a-b.c_d~e%20!$&'()*+;=", false),
        ] {
            assert!(simple_authority(value.as_bytes(), require_port), "{value}");
            assert!(authority(value, require_port).is_ok(), "{value}");
        }
        for (value, require_port) in [
            ("", false),
            ("example", true),
            ("example:", false),
            ("example:65536", false),
            ("example:123456", false),
            ("example:8a", false),
            ("example:80:80", false),
            ("exa mple", false),
            ("user@example", false),
            ("example,other", false),
            ("example/path", false),
            ("[::1]:80", false),
            (":80", false),
            ("ex\u{e9}mple", false),
        ] {
            assert!(!simple_authority(value.as_bytes(), require_port), "{value}");
        }
        assert!(!simple_authority(b"example:000080", false));
        assert!(authority("example:000080", false).is_ok());
        assert!(authority("[::1]:80", true).is_ok());
    }
}

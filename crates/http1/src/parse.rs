// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! A single-pass HTTP/1 head parser. Each byte is classified once, so callers
//! receive validated start-line elements and field lines without a separate
//! line-ending, name or value scan. See `design/http1-parser.md` for the
//! accepted grammar.

use crate::Error;
use base_http::Version;

macro_rules! table {
    (|$b:ident| $accept:expr) => {{
        let mut table = [false; 256];
        let mut byte = 0;
        while byte < 256 {
            let $b = byte as u8;
            table[byte] = $accept;
            byte += 1;
        }
        table
    }};
}

/// RFC 9110, section 5.6.2.
const TOKEN: [bool; 256] = table!(|b| {
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
});

/// Visible ASCII except the fragment and backslash delimiters, which no
/// request target form permits.
const TARGET: [bool; 256] = table!(|b| b > b' ' && b < 0x7f && b != b'#' && b != b'\\');

/// HTAB, SP, VCHAR and obs-text (RFC 9110, section 5.5).
const VALUE: [bool; 256] = table!(|b| b == b'\t' || (b >= b' ' && b != 0x7f));

const ONES: u64 = u64::from_ne_bytes([0x01; 8]);
const HIGH: u64 = u64::from_ne_bytes([0x80; 8]);

/// Mark controls and DEL in a little-endian word with the high bit of their
/// byte. A borrow can also mark bytes above a marked byte, so only the lowest
/// mark is exact: it locates the first control.
#[inline]
fn controls(word: u64) -> u64 {
    let below = word.wrapping_sub(ONES * 0x20) & !word;
    let del = word ^ (ONES * 0x7f);
    let del = del.wrapping_sub(ONES) & !del;
    (below | del) & HIGH
}

/// Bit `i` is set when `block[i]` is a control or DEL. The fold has no
/// branch, so the compiler vectorizes it and builds the mask with one
/// instruction where the target has one.
#[inline]
fn controls_in(block: &[u8; 16]) -> u32 {
    block.iter().enumerate().fold(0, |mask, (i, b)| {
        mask | (u32::from((*b < b' ') | (*b == 0x7f)) << i)
    })
}

/// Bit `i` is set when `block[i]` is not an ASCII letter, digit or `-`, which
/// covers nearly every field name byte seen in practice.
#[inline]
fn unusual_in(block: &[u8; 16]) -> u32 {
    block.iter().enumerate().fold(0, |mask, (i, b)| {
        let letter = (b | 0x20).wrapping_sub(b'a') < 26;
        let digit = b.wrapping_sub(b'0') < 10;
        mask | (u32::from(!(letter | digit | (*b == b'-'))) << i)
    })
}

#[inline]
fn span(bytes: &[u8], mut at: usize, accept: &[bool; 256]) -> usize {
    while bytes.get(at).is_some_and(|b| accept[usize::from(*b)]) {
        at += 1;
    }
    at
}

/// The position of a parsed element in the head.
#[derive(Clone, Copy)]
pub(crate) struct Span {
    start: usize,
    end: usize,
}

impl Span {
    #[inline]
    pub(crate) fn of(self, bytes: &[u8]) -> &[u8] {
        &bytes[self.start..self.end]
    }

    /// The element as text, for a target or field name, which the parser
    /// accepts only as ASCII. `head` is the head as text when it is UTF-8.
    /// Every span starts and ends next to an ASCII byte or the end of the
    /// head, so slicing the text cannot split a character.
    #[inline]
    pub(crate) fn ascii<'a>(self, head: Option<&'a str>, bytes: &'a [u8]) -> &'a str {
        match head {
            Some(head) => &head[self.start..self.end],
            None => std::str::from_utf8(self.of(bytes)).unwrap(),
        }
    }

    /// The element as text if it is UTF-8, for a field value.
    #[inline]
    pub(crate) fn text<'a>(self, head: Option<&'a str>, bytes: &'a [u8]) -> Option<&'a str> {
        match head {
            Some(head) => Some(&head[self.start..self.end]),
            None => std::str::from_utf8(self.of(bytes)).ok(),
        }
    }
}

pub(crate) struct RequestLine<'a> {
    pub method: &'a [u8],
    pub target: Span,
    pub version: Version,
}

/// Parse `method SP request-target SP HTTP-version CRLF`. `Ok(None)` means
/// the input ended first; invalid bytes are reported as soon as they are seen.
pub(crate) fn request_line(bytes: &[u8]) -> Result<Option<(RequestLine<'_>, usize)>, Error> {
    if matches!(bytes.first(), Some(b'\r' | b'\n' | b' ' | b'\t')) {
        return Err(Error::Invalid("invalid start line prefix"));
    }
    let method = span(bytes, 0, &TOKEN);
    match bytes.get(method) {
        None => return Ok(None),
        Some(b' ') if method > 0 => {}
        Some(_) => return Err(Error::Invalid("invalid request head")),
    }
    let start = method + 1;
    let target = span(bytes, start, &TARGET);
    match bytes.get(target) {
        None => return Ok(None),
        Some(b' ') if target > start => {}
        Some(_) => return Err(Error::Invalid("invalid request target")),
    }
    let version = match bytes.get(target + 1..target + 11) {
        Some(b"HTTP/1.1\r\n") => Version::Http11,
        Some(b"HTTP/1.0\r\n") => Version::Http10,
        None if [b"HTTP/1.1\r\n", b"HTTP/1.0\r\n"]
            .iter()
            .any(|version| version.starts_with(&bytes[target + 1..])) =>
        {
            return Ok(None);
        }
        _ => return Err(Error::Invalid("invalid request head")),
    };
    let line = RequestLine {
        method: &bytes[..method],
        target: Span { start, end: target },
        version,
    };
    Ok(Some((line, target + 11)))
}

pub(crate) struct StatusLine {
    pub version: Version,
    pub code: u16,
}

/// Parse `HTTP-version SP 3DIGIT [ SP reason-phrase ] CRLF`. The reason phrase
/// is checked and discarded. `Ok(None)` means the input ended first.
pub(crate) fn status_line(bytes: &[u8]) -> Result<Option<(StatusLine, usize)>, Error> {
    if matches!(bytes.first(), Some(b'\r' | b'\n' | b' ' | b'\t')) {
        return Err(Error::Invalid("invalid start line prefix"));
    }
    let version = match bytes.get(..9) {
        Some(b"HTTP/1.1 ") => Version::Http11,
        Some(b"HTTP/1.0 ") => Version::Http10,
        None if [b"HTTP/1.1 ", b"HTTP/1.0 "]
            .iter()
            .any(|version| version.starts_with(bytes)) =>
        {
            return Ok(None);
        }
        _ => return Err(Error::Invalid("invalid response head")),
    };
    let mut code = 0;
    for at in 9..12 {
        match bytes.get(at) {
            None => return Ok(None),
            Some(digit @ b'0'..=b'9') => code = code * 10 + u16::from(digit - b'0'),
            Some(_) => return Err(Error::Invalid("invalid response head")),
        }
    }
    let at = match bytes.get(12) {
        None => return Ok(None),
        Some(b' ') => span(bytes, 13, &VALUE),
        Some(_) => 12,
    };
    match bytes.get(at) {
        None => return Ok(None),
        Some(b'\r') => {}
        Some(b'\n') => return Err(Error::Invalid("expected CRLF")),
        Some(_) => return Err(Error::Invalid("invalid response head")),
    }
    match bytes.get(at + 1) {
        None => return Ok(None),
        Some(b'\n') => {}
        Some(_) => return Err(Error::Invalid("expected CRLF")),
    }
    Ok(Some((StatusLine { version, code }, at + 2)))
}

/// A field name, which is a token and so ASCII, and its trimmed value.
#[derive(Clone, Copy)]
pub(crate) struct Field {
    pub name: Span,
    pub value: Span,
}

pub(crate) enum Line {
    Field(Field),
    /// The empty line ending the section, with the section's total length.
    End(usize),
    Incomplete,
}

/// Field lines following a start line, up to and including the empty line.
pub(crate) struct Lines<'a> {
    bytes: &'a [u8],
    at: usize,
    remaining: usize,
}

impl<'a> Lines<'a> {
    pub(crate) fn new(bytes: &'a [u8], at: usize, max_fields: usize) -> Self {
        Self {
            bytes,
            at,
            remaining: max_fields,
        }
    }

    /// Parse `field-name ":" OWS field-value OWS CRLF`, or the final CRLF.
    pub(crate) fn next(&mut self) -> Result<Line, Error> {
        let bytes = self.bytes;
        let start = self.at;
        match bytes.get(start) {
            None => return Ok(Line::Incomplete),
            Some(b'\r') => {
                return match bytes.get(start + 1) {
                    None => Ok(Line::Incomplete),
                    Some(b'\n') => Ok(Line::End(start + 2)),
                    Some(_) => Err(Error::Invalid("expected CRLF")),
                };
            }
            Some(_) => {}
        }
        if self.remaining == 0 {
            return Err(Error::Limit("field count"));
        }
        let mut colon = start;
        while let Some(block) = bytes.get(colon..colon + 16) {
            let mask = unusual_in(block.try_into().unwrap());
            if mask == 0 {
                colon += 16;
                continue;
            }
            colon += mask.trailing_zeros() as usize;
            break;
        }
        let colon = span(bytes, colon, &TOKEN);
        match bytes.get(colon) {
            None => return Ok(Line::Incomplete),
            Some(b':') if colon > start => {}
            Some(b'\n') => return Err(Error::Invalid("expected CRLF")),
            Some(_) => return Err(Error::Invalid("invalid field name")),
        }
        let mut at = colon + 1;
        while matches!(bytes.get(at), Some(b' ' | b'\t')) {
            at += 1;
        }
        let value = at;
        loop {
            // Jump to the next control, which is usually the CR ending the
            // line, 16 bytes at a time and then a word at a time near the
            // end of the input.
            if let Some(block) = bytes.get(at..at + 16) {
                let mask = controls_in(block.try_into().unwrap());
                if mask == 0 {
                    at += 16;
                    continue;
                }
                at += mask.trailing_zeros() as usize;
            } else if let Some(word) = bytes.get(at..at + 8) {
                let marks = controls(u64::from_le_bytes(word.try_into().unwrap()));
                if marks == 0 {
                    at += 8;
                    continue;
                }
                at += marks.trailing_zeros() as usize / 8;
            }
            match bytes.get(at) {
                None => return Ok(Line::Incomplete),
                Some(b) if VALUE[usize::from(*b)] => at += 1,
                Some(b'\r') => break,
                Some(b'\n') => return Err(Error::Invalid("expected CRLF")),
                Some(_) => return Err(Error::Invalid("invalid field value")),
            }
        }
        match bytes.get(at + 1) {
            None => return Ok(Line::Incomplete),
            Some(b'\n') => {}
            Some(_) => return Err(Error::Invalid("expected CRLF")),
        }
        let mut end = at;
        while end > value && matches!(bytes[end - 1], b' ' | b'\t') {
            end -= 1;
        }
        self.at = at + 2;
        self.remaining -= 1;
        Ok(Line::Field(Field {
            name: Span { start, end: colon },
            value: Span { start: value, end },
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_masks_classify_every_byte_in_every_position() {
        for byte in 0..=255u8 {
            let control = byte < 0x20 || byte == 0x7f;
            let usual = byte.is_ascii_alphanumeric() || byte == b'-';
            for position in 0..16 {
                let mut block = [b'a'; 16];
                block[position] = byte;
                let bit = 1 << position;
                assert_eq!(
                    controls_in(&block) == bit,
                    control,
                    "{byte:#x} at {position}"
                );
                assert_eq!(controls_in(&block) & !bit, 0);
                assert_eq!(unusual_in(&block) == bit, !usual, "{byte:#x} at {position}");
                assert_eq!(unusual_in(&block) & !bit, 0);
            }
        }
    }

    #[test]
    fn controls_locate_the_first_control_byte() {
        // Two chosen bytes in every pair of positions, over fills near each
        // boundary the borrow can cross.
        let interesting = [
            0u8, 0x01, 0x09, 0x0d, 0x1f, 0x20, 0x21, 0x7e, 0x7f, 0x80, 0xa0, 0xff,
        ];
        for first in 0..8 {
            for second in first..8 {
                for a in interesting {
                    for b in interesting {
                        for fill in [0x20u8, 0x7e, 0x80, 0xa0, 0xff] {
                            let mut word = [fill; 8];
                            word[first] = a;
                            word[second] = b;
                            let expected = word.iter().position(|b| *b < 0x20 || *b == 0x7f);
                            let marks = controls(u64::from_le_bytes(word));
                            let found = (marks != 0).then(|| marks.trailing_zeros() as usize / 8);
                            assert_eq!(found, expected, "{word:?}");
                        }
                    }
                }
            }
        }
    }
}

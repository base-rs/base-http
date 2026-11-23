// Released under the MIT License.
// Copyright, 2013-2015, by Ilya Grigorik.
// Copyright, 2014-2015, by Kaoru Maeda.
// Copyright, 2015, by Tamir Duberstein.
// Copyright, 2016, by George Ulmer.
// Copyright, 2018, by Byron Formwalt.
// Copyright, 2018, by Kenichi Nakamura.
// Copyright, 2018, by Tiago Cardoso.
// Copyright, 2018-2026, by Samuel Williams.
// Copyright, 2019, by Jingyi Chen.
// Copyright, 2020, by Justin Mazzocchi.
// Copyright, 2024, by Maruth Goyal.
// Copyright, 2024, by Nathan Froyd.

//! Prefixed integers, [RFC 7541 §5.1](https://www.rfc-editor.org/rfc/rfc7541#section-5.1).

use crate::DecodeError;

/// The most continuation bytes an integer may use. Five carry 35 bits, enough for any value up to [`VALUE_LIMIT`] with a few non-minimal zero bytes, while bounding the work for each integer.
const CONTINUATION_LIMIT: usize = 5;

/// The largest value an integer may have. Every HPACK integer, an index, a string length or a table size, is checked against a smaller configured bound afterwards.
const VALUE_LIMIT: u64 = u32::MAX as u64;

/// Appends `value` with a `prefix_bits` prefix, the remaining high bits of the first byte taken from `pattern`.
pub(crate) fn encode(output: &mut Vec<u8>, pattern: u8, prefix_bits: u32, value: usize) {
    let limit = (1usize << prefix_bits) - 1;
    if value < limit {
        output.push(pattern | value as u8);
        return;
    }
    output.push(pattern | limit as u8);
    let mut rest = value - limit;
    while rest >= 0x80 {
        output.push(rest as u8 | 0x80);
        rest >>= 7;
    }
    output.push(rest as u8);
}

/// Decodes an integer with a `prefix_bits` prefix from the start of `input`, returning its value and the number of bytes it used, or `None` if `input` ends first.
///
/// Fails with [`DecodeError::IntegerOverflow`] if the integer uses more than [`CONTINUATION_LIMIT`] continuation bytes or exceeds [`VALUE_LIMIT`].
pub(crate) fn decode(input: &[u8], prefix_bits: u32) -> Result<Option<(usize, usize)>, DecodeError> {
    let Some(&first) = input.first() else {
        return Ok(None);
    };
    let limit = ((1u16 << prefix_bits) - 1) as u8;
    let prefix = first & limit;
    if prefix < limit {
        return Ok(Some((usize::from(prefix), 1)));
    }
    let mut value = u64::from(prefix);
    for (position, &byte) in input[1..].iter().take(CONTINUATION_LIMIT).enumerate() {
        value += u64::from(byte & 0x7f) << (7 * position);
        if byte & 0x80 == 0 {
            if value > VALUE_LIMIT {
                return Err(DecodeError::IntegerOverflow);
            }
            return Ok(Some((value as usize, position + 2)));
        }
    }
    if input.len() > CONTINUATION_LIMIT {
        Err(DecodeError::IntegerOverflow)
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests;

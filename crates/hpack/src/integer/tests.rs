// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;

fn encoded(prefix_bits: u32, value: usize) -> Vec<u8> {
    let mut output = Vec::new();
    encode(&mut output, 0, prefix_bits, value);
    output
}

#[test]
fn encodes_the_rfc_examples() {
    // RFC 7541 C.1.1 to C.1.3, also `protocol-hpack`'s `write_integer` tests.
    assert_eq!(encoded(5, 10), [10]);
    assert_eq!(encoded(5, 1337), [31, 128 + 26, 10]);
    assert_eq!(encoded(8, 42), [42]);
}

#[test]
fn decodes_the_rfc_examples() {
    assert_eq!(decode(&[10], 5), Ok(Some((10, 1))));
    assert_eq!(decode(&[31, 128 + 26, 10], 5), Ok(Some((1337, 3))));
    assert_eq!(decode(&[42], 8), Ok(Some((42, 1))));
}

#[test]
fn keeps_the_pattern_bits_and_ignores_them_when_decoding() {
    let mut output = Vec::new();
    encode(&mut output, 0xe0, 5, 1337);
    assert_eq!(output, [0xe0 | 31, 128 + 26, 10]);
    assert_eq!(decode(&output, 5), Ok(Some((1337, 3))));
}

#[test]
fn round_trips_boundaries() {
    for prefix_bits in 1..=8 {
        let limit = (1usize << prefix_bits) - 1;
        for value in [0, 1, limit - 1, limit, limit + 1, limit + 127, limit + 128, 1 << 14, 1 << 21, 1 << 28, u32::MAX as usize] {
            let bytes = encoded(prefix_bits, value);
            assert_eq!(decode(&bytes, prefix_bits), Ok(Some((value, bytes.len()))), "{prefix_bits} {value}");
            for end in 0..bytes.len() {
                assert_eq!(decode(&bytes[..end], prefix_bits), Ok(None), "{prefix_bits} {value} {end}");
            }
        }
    }
}

#[test]
fn reads_only_its_own_bytes() {
    assert_eq!(decode(&[31, 128 + 26, 10, 0xff], 5), Ok(Some((1337, 3))));
}

#[test]
fn accepts_non_minimal_encodings_within_the_bound() {
    assert_eq!(decode(&[31, 0x80, 0x80, 0x80, 0x80, 0x00], 5), Ok(Some((31, 6))));
}

#[test]
fn rejects_integers_longer_than_the_bound() {
    // `protocol-hpack` reads continuation bytes without limit.
    assert_eq!(decode(&[31, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00], 5), Err(DecodeError::IntegerOverflow));
    assert_eq!(decode(&[31, 0x80, 0x80, 0x80, 0x80, 0x80], 5), Err(DecodeError::IntegerOverflow));
}

#[test]
fn rejects_values_beyond_the_bound() {
    let bytes = encoded(5, u32::MAX as usize + 1);
    assert_eq!(bytes.len(), 6);
    assert_eq!(decode(&bytes, 5), Err(DecodeError::IntegerOverflow));
}

#[test]
fn reports_truncated_continuations_as_incomplete() {
    // `protocol-hpack` reads `0xff 0x80` as index 126.
    assert_eq!(decode(&[0xff, 0x80], 7), Ok(None));
    assert_eq!(decode(&[], 7), Ok(None));
}

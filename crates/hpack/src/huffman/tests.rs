// Released under the MIT License.
// Copyright, 2014, by Kaoru Maeda.
// Copyright, 2015, by Ilya Grigorik.
// Copyright, 2015, by Tamir Duberstein.
// Copyright, 2018-2026, by Samuel Williams.
// Copyright, 2024, by Nathan Froyd.

use super::*;
use std::collections::HashMap;

/// RFC 7541 Appendix C examples, as `protocol-hpack`'s Huffman tests use them.
const EXAMPLES: [(&str, &str); 3] = [
    ("www.example.com", "f1e3c2e5f23a6ba0ab90f4ff"),
    ("no-cache", "a8eb10649cbf"),
    ("Mon, 21 Oct 2013 20:13:21 GMT", "d07abe941054d444a8200595040b8166e082a62d1bff"),
];

fn hex(text: &str) -> Vec<u8> {
    (0..text.len()).step_by(2).map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap()).collect()
}

fn encoded(bytes: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    encode(bytes, &mut output);
    assert_eq!(output.len(), encoded_len(bytes));
    output
}

fn decoded(bytes: &[u8]) -> Result<Vec<u8>, DecodeError> {
    let mut output = Vec::new();
    decode(bytes, &mut output).map(|()| output)
}

/// Decodes by walking the code bit by bit, independently of the four-bit machine.
fn reference_decode(symbols: &HashMap<(u32, u8), usize>, input: &[u8]) -> Result<Vec<u8>, DecodeError> {
    let mut output = Vec::new();
    let mut code = 0u32;
    let mut length = 0u8;
    for &byte in input {
        for bit in (0..8).rev() {
            code = (code << 1) | u32::from((byte >> bit) & 1);
            length += 1;
            if let Some(&symbol) = symbols.get(&(code, length)) {
                if symbol == END_OF_STRING {
                    return Err(DecodeError::HuffmanEndOfString);
                }
                output.push(symbol as u8);
                code = 0;
                length = 0;
            }
        }
    }
    if length <= 7 && code == (1 << length) - 1 { Ok(output) } else { Err(DecodeError::HuffmanPadding) }
}

#[test]
fn encodes_the_rfc_examples() {
    for (plain, coded) in EXAMPLES {
        assert_eq!(encoded(plain.as_bytes()), hex(coded), "{plain}");
    }
}

#[test]
fn decodes_the_rfc_examples() {
    for (plain, coded) in EXAMPLES {
        assert_eq!(decoded(&hex(coded)).unwrap(), plain.as_bytes(), "{plain}");
    }
}

#[test]
fn round_trips_strings() {
    for string in [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:16.0) Gecko/20100101 Firefox/16.0",
        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "http://www.craigslist.org/about/sites/",
        "cl_b=AB2BKbsl4hGM7M4nH5PYWghTM5A; cl_def_lang=en; cl_def_hp=shoals",
        "image/png,image/*;q=0.8,*/*;q=0.5",
        "BX=c99r6jp89a7no&b=3&s=q4; localization=en-us%3Bus%3Bus",
        "UTF-8でエンコードした日本語文字列",
        "",
    ] {
        assert_eq!(decoded(&encoded(string.as_bytes())).unwrap(), string.as_bytes());
    }
}

#[test]
fn round_trips_every_two_byte_sequence() {
    for first in 0..=255u8 {
        for second in 0..=255u8 {
            let string = [first, second];
            assert_eq!(decoded(&encoded(&string)).unwrap(), string);
        }
    }
}

#[test]
fn every_symbol_round_trips_alone_and_after_others() {
    let all: Vec<u8> = (0..=255).collect();
    assert_eq!(decoded(&encoded(&all)).unwrap(), all);
    for byte in 0..=255u8 {
        assert_eq!(decoded(&encoded(&[byte])).unwrap(), [byte]);
    }
}

#[test]
fn rejects_input_shorter_than_expected() {
    let coded = hex(EXAMPLES[0].1);
    assert_eq!(decoded(&coded[..coded.len() - 1]), Err(DecodeError::HuffmanPadding));
}

#[test]
fn rejects_padding_that_is_not_all_ones() {
    assert_eq!(decoded(&hex("f1e3c2e5f23a6ba0ab90f4fe")), Err(DecodeError::HuffmanPadding));
}

#[test]
fn rejects_padding_longer_than_seven_bits() {
    assert_eq!(decoded(&hex("e7cf9bebe89b6fb16fa9b6ffff")), Err(DecodeError::HuffmanPadding));
    // A whole byte of padding after a symbol ending on a byte boundary: `0` is `00000`, three ones then pad it.
    assert_eq!(decoded(&[0b0000_0111, 0xff]), Err(DecodeError::HuffmanPadding));
    assert_eq!(decoded(&[0b0000_0111]).unwrap(), b"0");
}

#[test]
fn rejects_the_end_of_string_symbol() {
    // `a`, `b`, then the 30 one bits of end-of-string.
    assert_eq!(decoded(&hex("1c7fffffffff")), Err(DecodeError::HuffmanEndOfString));
}

#[test]
fn the_machine_matches_a_bitwise_decoder() {
    let symbols: HashMap<(u32, u8), usize> = CODES.iter().enumerate().map(|(symbol, &code)| (code, symbol)).collect();
    // Every one- and two-byte input, valid or not, decodes alike, including the errors.
    for first in 0..=255u8 {
        assert_eq!(decoded(&[first]), reference_decode(&symbols, &[first]), "{first:02x}");
        for second in 0..=255u8 {
            let input = [first, second];
            assert_eq!(decoded(&input), reference_decode(&symbols, &input), "{first:02x}{second:02x}");
        }
    }
}

#[test]
fn the_machine_is_built_at_compile_time_and_at_run_time_alike() {
    // Building it at run time also measures the builder's coverage, which compile-time evaluation does not.
    let built = machine();
    for state in 0..256 {
        assert_eq!(built.accepting[state], MACHINE.accepting[state]);
        for nibble in 0..16 {
            let (left, right) = (built.transitions[state][nibble], MACHINE.transitions[state][nibble]);
            assert_eq!((left.state, left.symbol, left.flags), (right.state, right.symbol, right.flags));
        }
    }
    assert_eq!(MACHINE.accepting.iter().filter(|&&accepting| accepting).count(), 8);
}

#[test]
fn encoded_length_counts_padding() {
    assert_eq!(encoded_len(b""), 0);
    // `0` has five bits, padded to one byte; `00` has ten, padded to two.
    assert_eq!(encoded_len(b"0"), 1);
    assert_eq!(encoded_len(b"00"), 2);
    // The longest codes, thirty bits, take four bytes alone.
    assert_eq!(encoded_len(b"\n"), 4);
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Encoder behaviour, including `protocol-hpack`'s compressor and header representation tests.

mod support;

use base_http_hpack::{DEFAULT_TABLE_SIZE, Decoder, DecoderOptions, Encoder, EncoderOptions, HuffmanUse, Indexing, STATIC_TABLE, TableUse};
use support::*;

fn encoder(huffman: HuffmanUse, tables: TableUse) -> Encoder {
    Encoder::new(EncoderOptions {
        huffman,
        tables,
        ..EncoderOptions::default()
    })
}

fn one(encoder: &mut Encoder, name: &str, value: &str, indexing: Indexing) -> Vec<u8> {
    encode(encoder, &[field(name, value)], indexing)
}

#[test]
fn uses_the_indexed_representation_for_exact_matches() {
    // `protocol-hpack`'s "should handle indexed representation".
    let mut encoder = Encoder::default();
    assert_eq!(one(&mut encoder, ":status", "304", Indexing::Incremental), [0x80 | 11]);
    assert_eq!(one(&mut encoder, ":status", "304", Indexing::NotIndexed), [0x80 | 11]);
    // Every static entry, with or without a value.
    for (position, (name, value)) in STATIC_TABLE.iter().enumerate() {
        assert_eq!(one(&mut encoder, name, value, Indexing::Incremental), [0x80 | (position as u8 + 1)], "{name}");
    }
    assert!(encoder.table().is_empty());
}

#[test]
fn uses_each_literal_representation() {
    // `protocol-hpack`'s literal header tests: the pattern of the first byte and the name's index.
    let cases = [
        (Indexing::Incremental, 0x40, 0x3f),
        (Indexing::NotIndexed, 0x00, 0x0f),
        (Indexing::NeverIndexed, 0x10, 0x0f),
    ];
    for (indexing, pattern, prefix) in cases {
        let mut encoder = encoder(HuffmanUse::Never, TableUse::All);
        let indexed = one(&mut encoder, ":status", "my-value", indexing);
        assert_eq!(indexed[0] & !prefix, pattern, "{indexing:?}");
        assert_eq!(indexed[0] & prefix, 8, "{indexing:?}");
        assert_eq!(&indexed[1..], b"\x08my-value");
        let literal = one(&mut encoder, "x-custom", "my-value", indexing);
        assert_eq!(literal, [&[pattern][..], b"\x08x-custom\x08my-value"].concat(), "{indexing:?}");
        let (fields, _) = decode_detailed(&mut Decoder::default(), &[indexed, literal].concat()).unwrap();
        assert_eq!(fields, [(field(":status", "my-value"), Some(8), indexing), (field("x-custom", "my-value"), None, indexing)]);
        assert_eq!(encoder.table().len(), if indexing == Indexing::Incremental { 2 } else { 0 });
    }
}

#[test]
fn refers_to_dynamic_entries() {
    let mut encoder = encoder(HuffmanUse::Never, TableUse::All);
    one(&mut encoder, "x-custom", "a", Indexing::Incremental);
    one(&mut encoder, "x-custom", "b", Indexing::Incremental);
    // The newest exact match, then the newest name.
    assert_eq!(one(&mut encoder, "x-custom", "a", Indexing::NeverIndexed), b"\x1f\x2f\x01a");
    assert_eq!(one(&mut encoder, "x-custom", "a", Indexing::Incremental), [0x80 | 63]);
    assert_eq!(one(&mut encoder, "x-custom", "c", Indexing::NotIndexed), b"\x0f\x2f\x01c");
    // A static name is preferred over a dynamic name.
    one(&mut encoder, "cookie", "a", Indexing::Incremental);
    assert_eq!(one(&mut encoder, "cookie", "z", Indexing::NotIndexed), b"\x0f\x11\x01z");
}

#[test]
fn never_indexes_sensitive_fields() {
    let mut encoder = Encoder::default();
    // Not even an exact static match is used, so the field reaches every hop as a never-indexed literal.
    let block = one(&mut encoder, ":method", "GET", Indexing::NeverIndexed);
    assert_eq!(block, b"\x12\x03GET");
    let block = one(&mut encoder, "authorization", "secret", Indexing::NeverIndexed);
    assert_eq!(block[0], 0x10 | 0x0f);
    assert!(encoder.table().is_empty());
    let (fields, _) = decode_detailed(&mut Decoder::default(), &block).unwrap();
    assert_eq!(fields, [(field("authorization", "secret"), Some(23), Indexing::NeverIndexed)]);
}

#[test]
fn table_use_limits_the_references() {
    let mut none = encoder(HuffmanUse::Never, TableUse::None);
    assert_eq!(one(&mut none, ":method", "GET", Indexing::Incremental), b"\x00\x07:method\x03GET");
    let mut only_static = encoder(HuffmanUse::Never, TableUse::Static);
    assert_eq!(one(&mut only_static, ":method", "GET", Indexing::Incremental), [0x82]);
    assert_eq!(one(&mut only_static, "x-custom", "a", Indexing::Incremental), b"\x00\x08x-custom\x01a");
    assert_eq!(one(&mut only_static, "x-custom", "a", Indexing::Incremental), b"\x00\x08x-custom\x01a");
    assert!(none.table().is_empty() && only_static.table().is_empty());
}

#[test]
fn does_not_index_a_field_larger_than_the_table() {
    // `protocol-hpack` would insert it, emptying both tables.
    let mut encoder = encoder(HuffmanUse::Never, TableUse::All);
    one(&mut encoder, "x-small", "a", Indexing::Incremental);
    let large = "v".repeat(DEFAULT_TABLE_SIZE);
    let block = one(&mut encoder, "x-large", &large, Indexing::Incremental);
    assert_eq!(block[0], 0x00);
    assert_eq!(table(encoder.table().iter()), [field("x-small", "a")]);
    // An entry of exactly the capacity is indexed.
    let exact = "v".repeat(DEFAULT_TABLE_SIZE - 32 - 7);
    assert_eq!(one(&mut encoder, "x-exact", &exact, Indexing::Incremental)[0], 0x40);
    assert_eq!(table(encoder.table().iter()), [field("x-exact", &exact)]);
}

#[test]
fn writes_strings_with_and_without_huffman() {
    // `protocol-hpack`'s `write_string` tests.
    let strings = ["abcdefghij", "éáűőúöüó€", &"éáűőúöüó€".repeat(100)];
    for (huffman, flag) in [(HuffmanUse::Always, 0x80), (HuffmanUse::Never, 0x00)] {
        for string in strings {
            let mut encoder = encoder(huffman, TableUse::None);
            let block = one(&mut encoder, "x", string, Indexing::NotIndexed);
            // The value follows the literal name `x`.
            assert_eq!(block[3] & 0x80, flag, "{huffman:?} {string}");
            assert_eq!(decode(&mut Decoder::default(), &block).unwrap(), [field("x", string)]);
        }
    }
}

#[test]
fn chooses_the_shorter_string() {
    // `protocol-hpack`'s "choosing shorter representation", preferring the plain string at equal length.
    for (string, flag) in [("日本語", 0x00), ("200", 0x80), ("xq", 0x00)] {
        let mut encoder = encoder(HuffmanUse::Shorter, TableUse::None);
        let block = one(&mut encoder, "x", string, Indexing::NotIndexed);
        assert_eq!(block[3] & 0x80, flag, "{string}");
    }
}

#[test]
fn signals_a_smaller_table_in_the_first_block() {
    let mut encoder = Encoder::new(EncoderOptions {
        table_size_limit: 512,
        ..EncoderOptions::default()
    });
    assert_eq!(encoder.table().capacity(), 512);
    assert_eq!(encode(&mut encoder, &[], Indexing::Incremental), [0x3f, 0xe1, 0x03]);
    assert_eq!(encode(&mut encoder, &[], Indexing::Incremental), [0u8; 0]);
    // A larger decoder limit is capped.
    encoder.set_table_size_limit(65536);
    assert_eq!(encoder.table().capacity(), 512);
    assert_eq!(encode(&mut encoder, &[], Indexing::Incremental), [0u8; 0]);
}

#[test]
fn signals_the_smallest_and_final_sizes() {
    let mut encoder = Encoder::new(EncoderOptions {
        table_size_limit: 65536,
        ..EncoderOptions::default()
    });
    one(&mut encoder, "x-name", "value", Indexing::Incremental);
    // Lowering evicts at once; raising again signals both, smallest first, as RFC 7541 §4.2 requires.
    encoder.set_table_size_limit(0);
    assert!(encoder.table().is_empty());
    encoder.set_table_size_limit(100);
    encoder.set_table_size_limit(8192);
    let block = one(&mut encoder, ":method", "GET", Indexing::Incremental);
    assert_eq!(block, [0x20, 0x3f, 0xe1, 0x3f, 0x82]);
    // One change signals one update, and an unchanged limit none.
    encoder.set_table_size_limit(1024);
    encoder.set_table_size_limit(1024);
    assert_eq!(encode(&mut encoder, &[], Indexing::Incremental), [0x3f, 0xe1, 0x07]);
    encoder.set_table_size_limit(2048);
    assert_eq!(encode(&mut encoder, &[], Indexing::Incremental), [0x3f, 0xe1, 0x0f]);
    // A change undone before the next block still signals the smaller size.
    encoder.set_table_size_limit(1000);
    encoder.set_table_size_limit(2048);
    let block = encode(&mut encoder, &[], Indexing::Incremental);
    assert_eq!(block, [0x3f, 0xc9, 0x07, 0x3f, 0xe1, 0x0f]);
    let mut decoder = Decoder::new(DecoderOptions {
        table_size_limit: 65536,
        ..DecoderOptions::default()
    });
    decode(&mut decoder, &block).unwrap();
    assert_eq!(decoder.table().capacity(), 2048);
}

#[test]
fn encoders_are_independent_and_cloneable() {
    let mut encoder = Encoder::default();
    one(&mut encoder, "x-name", "value", Indexing::Incremental);
    let mut clone = encoder.clone();
    assert_eq!(one(&mut clone, "x-name", "value", Indexing::Incremental), [0xbe]);
    encoder.set_table_size_limit(0);
    assert_eq!(clone.table().len(), 1);
    assert!(format!("{encoder:?}").contains("Encoder"));
    let mut output = Vec::new();
    assert!(format!("{:?}", encoder.begin_block(&mut output)).contains("HeaderBlockEncoder"));
}

#[test]
fn appends_to_reused_output() {
    let mut encoder = Encoder::default();
    let mut output = b"frame header".to_vec();
    encoder.begin_block(&mut output).encode(b":method", b"GET", Indexing::Incremental);
    assert_eq!(output, b"frame header\x82");
}

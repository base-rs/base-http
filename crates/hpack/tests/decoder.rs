// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Decoder behaviour, including `protocol-hpack`'s decompressor tests and every decoding error.

mod support;

use base_http_hpack::{DecodeError, Decoder, DecoderOptions, Encoder, EncoderOptions, Indexing, SectionLimit, SectionSummary};
use support::*;

fn decoder_with(options: DecoderOptions) -> Decoder {
    Decoder::new(options)
}

fn literal(pattern: u8, name: &str, value: &str) -> Vec<u8> {
    let mut block = vec![pattern, name.len() as u8];
    block.extend_from_slice(name.as_bytes());
    block.push(value.len() as u8);
    block.extend_from_slice(value.as_bytes());
    block
}

#[test]
fn reports_each_representation() {
    let mut decoder = Decoder::default();
    let mut block = vec![0x82]; // Indexed `:method: GET`.
    block.extend(literal(0x40, "x-incremental", "a")); // Incremental, literal name.
    block.extend([0x44, 1, b'/']); // Incremental, static name `:path`.
    block.extend([0x0f, 0x0d, 1, b'7']); // Not indexed, static name 28, `content-length`.
    block.extend(literal(0x10, "x-secret", "s")); // Never indexed, literal name.
    block.extend([0x1f, 0x08, 1, b'c']); // Never indexed, static name 23, `authorization`.
    block.push(0xbf); // Indexed dynamic entry 63: `x-incremental: a`, now second newest.
    block.extend([0x7f, 0x00, 1, b'b']); // Incremental, dynamic name 63.
    let (fields, summary) = decode_detailed(&mut decoder, &block).unwrap();
    assert_eq!(
        fields,
        [
            (field(":method", "GET"), Some(2), Indexing::Incremental),
            (field("x-incremental", "a"), None, Indexing::Incremental),
            (field(":path", "/"), Some(4), Indexing::Incremental),
            (field("content-length", "7"), Some(28), Indexing::NotIndexed),
            (field("x-secret", "s"), None, Indexing::NeverIndexed),
            (field("authorization", "c"), Some(23), Indexing::NeverIndexed),
            (field("x-incremental", "a"), None, Indexing::Incremental),
            (field("x-incremental", "b"), None, Indexing::Incremental),
        ]
    );
    assert_eq!(summary.field_count(), 8);
    assert_eq!(summary.limit_exceeded(), None);
    assert_eq!(table(decoder.table().iter()), [field("x-incremental", "b"), field(":path", "/"), field("x-incremental", "a")]);
}

#[test]
fn counts_the_section_size_as_rfc_9113_defines_it() {
    let mut decoder = Decoder::default();
    let (_, summary) = decode_detailed(&mut decoder, &[0x82, 0x84]).unwrap();
    assert_eq!(summary.size(), (7 + 3 + 32) + (5 + 1 + 32));
    assert_eq!(decoder.section(), SectionSummary::default());
}

#[test]
fn reports_the_section_while_decoding() {
    let mut decoder = Decoder::default();
    decoder.decode(&[0x82, 0x84], |_| {}).unwrap();
    assert_eq!(decoder.section().field_count(), 2);
    decoder.decode(&[0x86], |_| {}).unwrap();
    assert_eq!(decoder.section().field_count(), 3);
    assert_eq!(decoder.finish().unwrap().field_count(), 3);
    assert_eq!(decoder.section().field_count(), 0);
}

#[test]
fn an_empty_block_has_an_empty_section() {
    let mut decoder = Decoder::default();
    assert_eq!(decoder.decode_block(&[], |_| panic!("no fields")), Ok(SectionSummary::default()));
}

#[test]
fn decodes_fragments_split_anywhere() {
    let mut encoder = Encoder::default();
    let fields = [
        field(":method", "GET"),
        field(":path", "/a/long/path/that/is/huffman/coded?with=query"),
        field("x-literal-name", "plain\u{1}value"),
        field("cookie", &"c".repeat(300)),
        field("x-literal-name", "plain\u{1}value"),
    ];
    let mut blocks = vec![encode(&mut encoder, &fields, Indexing::Incremental)];
    blocks.push(encode(&mut encoder, &fields, Indexing::NeverIndexed));
    let whole = {
        let mut decoder = Decoder::default();
        blocks.iter().map(|block| decode(&mut decoder, block).unwrap()).collect::<Vec<_>>()
    };
    assert_eq!(whole, [fields.to_vec(), fields.to_vec()]);
    // Split each block once at every position, and also into single bytes.
    for split in 0..blocks[0].len() {
        let mut decoder = Decoder::default();
        for (block, expected) in blocks.iter().zip(&whole) {
            let split = split.min(block.len());
            let mut decoded = Vec::new();
            decoder.decode(&block[..split], |field| decoded.push(owned(field))).unwrap();
            decoder.decode(&block[split..], |field| decoded.push(owned(field))).unwrap();
            decoder.finish().unwrap();
            assert_eq!(&decoded, expected, "split at {split}");
        }
    }
    let mut decoder = Decoder::default();
    for (block, expected) in blocks.iter().zip(&whole) {
        let mut decoded = Vec::new();
        for byte in block {
            decoder.decode(std::slice::from_ref(byte), |field| decoded.push(owned(field))).unwrap();
        }
        decoder.finish().unwrap();
        assert_eq!(&decoded, expected);
    }
}

#[test]
fn an_incremental_field_may_name_the_entry_it_evicts() {
    // RFC 7541 §4.4. The table holds one entry of 32 + 6 + 6 = 44; the new entry needs 32 + 6 + 20 bytes.
    let mut decoder = decoder_with(DecoderOptions { table_size_limit: 60, initial_table_size: 60, ..DecoderOptions::default() });
    let mut block = literal(0x40, "x-name", "first!");
    block.extend([0x7e, 20]); // Incremental, dynamic name 62.
    block.extend(b"a twenty byte value!");
    assert_eq!(decode(&mut decoder, &block).unwrap(), [field("x-name", "first!"), field("x-name", "a twenty byte value!")]);
    assert_eq!(table(decoder.table().iter()), [field("x-name", "a twenty byte value!")]);
}

#[test]
fn an_incremental_field_larger_than_the_table_empties_it() {
    let mut decoder = decoder_with(DecoderOptions { table_size_limit: 60, initial_table_size: 60, ..DecoderOptions::default() });
    let mut block = literal(0x40, "x-name", "first!");
    // Incremental, dynamic name 62, with a value too large for the table: the name must survive emptying it.
    block.extend([0x7e, 40]);
    block.extend([b'v'; 40]);
    block.extend(literal(0x40, "x-larger-than-the-table", &"w".repeat(40)));
    let decoded = decode(&mut decoder, &block).unwrap();
    assert_eq!(decoded[1], field("x-name", &"v".repeat(40)));
    assert_eq!(decoded[2], field("x-larger-than-the-table", &"w".repeat(40)));
    assert!(decoder.table().is_empty());
}

#[test]
fn keeps_decoding_past_the_section_size_limit() {
    let mut decoder = decoder_with(DecoderOptions { section_size_limit: 100, ..DecoderOptions::default() });
    // Two fields fit, of 42 and 38 bytes; the third, of 48, does not, and a later small one is not delivered either.
    let mut block = vec![0x82, 0x84];
    block.extend(literal(0x40, "x-later", "value-used-later"));
    block.push(0x86);
    let (fields, summary) = decode_detailed(&mut decoder, &block).unwrap();
    assert_eq!(fields.len(), 2);
    assert_eq!(summary.limit_exceeded(), Some(SectionLimit::Size));
    assert_eq!(summary.field_count(), 4);
    assert_eq!(summary.size(), 42 + 38 + (7 + 16 + 32) + (7 + 4 + 32));
    // The undelivered field still entered the dynamic table, so the next block can refer to it.
    assert_eq!(decode(&mut decoder, &[0xbe]).unwrap(), [field("x-later", "value-used-later")]);
}

#[test]
fn keeps_decoding_past_the_field_count_limit() {
    let mut decoder = decoder_with(DecoderOptions { field_count_limit: 2, ..DecoderOptions::default() });
    let mut block = vec![0x82, 0x84];
    block.extend(literal(0x40, "x-later", "value"));
    let (fields, summary) = decode_detailed(&mut decoder, &block).unwrap();
    assert_eq!(fields.len(), 2);
    assert_eq!(summary.limit_exceeded(), Some(SectionLimit::FieldCount));
    assert_eq!(decode(&mut decoder, &[0xbe]).unwrap(), [field("x-later", "value")]);
    // Each block has its own section.
    assert_eq!(decoder.decode_block(&[0x82], |_| {}).unwrap().limit_exceeded(), None);
}

#[test]
fn the_first_limit_exceeded_is_reported() {
    let mut decoder = decoder_with(DecoderOptions {
        field_count_limit: 1,
        section_size_limit: 50,
        ..DecoderOptions::default()
    });
    let (_, summary) = decode_detailed(&mut decoder, &[0x82, 0x82, 0x82]).unwrap();
    assert_eq!(summary.limit_exceeded(), Some(SectionLimit::FieldCount));
    let mut block = literal(0x40, "a", &"b".repeat(30));
    block.push(0x82);
    let (_, summary) = decode_detailed(&mut decoder, &block).unwrap();
    assert_eq!(summary.limit_exceeded(), Some(SectionLimit::Size));
}

#[test]
fn applies_table_size_updates() {
    let mut decoder = Decoder::default();
    decode(&mut decoder, &literal(0x40, "x-name", "value")).unwrap();
    assert_eq!(decode(&mut decoder, &[0x20, 0x3f, 0xe1, 0x1f, 0x82]).unwrap(), [field(":method", "GET")]);
    assert!(decoder.table().is_empty());
    assert_eq!(decoder.table().capacity(), 4096);
}

#[test]
fn accepts_a_block_of_only_table_size_updates() {
    // `protocol-hpack` rejects this as a "trailing table size update", though RFC 7541 permits it.
    let mut decoder = Decoder::default();
    assert_eq!(decode(&mut decoder, &[0x3f, 0xe1, 0x01]).unwrap(), []);
    assert_eq!(decoder.table().capacity(), 256);
}

#[test]
fn rejects_a_table_size_update_over_the_limit() {
    // `protocol-hpack`'s "should reject table size update if exceed limit".
    let mut decoder = decoder_with(DecoderOptions { table_size_limit: 256, initial_table_size: 256, ..DecoderOptions::default() });
    assert_eq!(decoder.table_size_limit(), 256);
    let mut encoder = Encoder::new(EncoderOptions {
        initial_table_size: 256,
        table_size_limit: 512,
        ..EncoderOptions::default()
    });
    encoder.set_table_size_limit(512);
    assert_eq!(decode(&mut decoder, &encode(&mut encoder, &[], Indexing::Incremental)), Err(DecodeError::TableSizeTooLarge { size: 512, limit: 256 }));
}

#[test]
fn rejects_a_table_size_update_after_a_field() {
    let mut decoder = Decoder::default();
    assert_eq!(decode(&mut decoder, &[0x82, 0x20]), Err(DecodeError::MisplacedTableSizeUpdate));
}

#[test]
fn requires_a_table_size_update_after_the_limit_is_lowered() {
    let mut decoder = Decoder::default();
    decoder.set_table_size_limit(100);
    assert_eq!(decoder.table().capacity(), 4096);
    assert_eq!(decode(&mut decoder, &[0x82]), Err(DecodeError::MissingTableSizeUpdate));
    assert_eq!(decode(&mut decoder, &literal(0x00, "x", "y")), Err(DecodeError::MissingTableSizeUpdate));
    // An empty block cannot omit it either.
    assert_eq!(decode(&mut decoder, &[]), Err(DecodeError::MissingTableSizeUpdate));
    // An update above the smallest limit does not satisfy it.
    decoder.set_table_size_limit(4096);
    assert_eq!(decode(&mut decoder, &[0x3f, 0xe1, 0x1f, 0x82]), Err(DecodeError::MissingTableSizeUpdate));
    // The smallest, then the final limit, does.
    assert_eq!(decode(&mut decoder, &[0x3f, 0x45, 0x3f, 0xe1, 0x1f, 0x82]).unwrap(), [field(":method", "GET")]);
    assert_eq!(decoder.table().capacity(), 4096);
    // Once satisfied, later blocks need none.
    assert_eq!(decode(&mut decoder, &[0x82]).unwrap(), [field(":method", "GET")]);
}

#[test]
fn raising_the_limit_requires_no_update() {
    let mut decoder = Decoder::default();
    decoder.set_table_size_limit(65536);
    assert_eq!(decode(&mut decoder, &[0x82]).unwrap(), [field(":method", "GET")]);
    assert_eq!(decoder.table().capacity(), 4096);
    // Lowering it to the capacity in use requires none either.
    decoder.set_table_size_limit(4096);
    assert_eq!(decode(&mut decoder, &[0x82]).unwrap(), [field(":method", "GET")]);
}

#[test]
fn rejects_index_zero() {
    // `protocol-hpack`'s "should raise when decoding indexed representation with index zero".
    assert_eq!(decode(&mut Decoder::default(), &[0x80]), Err(DecodeError::InvalidIndex { index: 0 }));
}

#[test]
fn rejects_indices_beyond_the_dynamic_table() {
    // `protocol-hpack`'s "raises an error if the index is out of bounds".
    let mut decoder = Decoder::default();
    assert_eq!(decode(&mut decoder, &[0xbe]), Err(DecodeError::InvalidIndex { index: 62 }));
    assert_eq!(decode(&mut decoder, &[0x7e, 0x00]), Err(DecodeError::InvalidIndex { index: 62 }));
    assert_eq!(decode(&mut decoder, &[0x0f, 0x2f, 0x00]), Err(DecodeError::InvalidIndex { index: 62 }));
    decode(&mut decoder, &literal(0x40, "x", "y")).unwrap();
    assert_eq!(decode(&mut decoder, &[0xbe]).unwrap(), [field("x", "y")]);
    assert_eq!(decode(&mut decoder, &[0xbf]), Err(DecodeError::InvalidIndex { index: 63 }));
    assert_eq!(decode(&mut decoder, &[0xff, 0x80, 0x08]), Err(DecodeError::InvalidIndex { index: 1151 }));
}

#[test]
fn rejects_a_truncated_integer() {
    // `protocol-hpack` reads `0xff 0x80` as index 126.
    let mut decoder = Decoder::default();
    assert_eq!(decode(&mut decoder, &[0xff, 0x80]), Err(DecodeError::Truncated));
    assert_eq!(decode(&mut decoder, &[0x82]).unwrap(), [field(":method", "GET")]);
}

#[test]
fn rejects_every_truncation_of_a_block() {
    let mut encoder = Encoder::default();
    encoder.set_table_size_limit(1000);
    let block = encode(
        &mut encoder,
        &[field(":method", "GET"), field("x-literal", "value"), field("cookie", "huffman-coded-cookie"), field("x-literal", "value")],
        Indexing::Incremental,
    );
    let mut boundaries = 0;
    for end in 1..block.len() {
        let mut decoder = Decoder::default();
        decoder.set_table_size_limit(1000);
        let mut delivered = 0;
        decoder.decode(&block[..end], |_| delivered += 1).unwrap();
        // A field is delivered only once its representation is complete, and a block may end only between representations.
        assert!(delivered < 4, "{end}");
        match decoder.finish() {
            Ok(summary) => {
                assert_eq!(summary.field_count(), delivered, "{end}");
                boundaries += 1;
            }
            Err(error) => assert_eq!(error, DecodeError::Truncated, "{end}"),
        }
    }
    // After the table size update and each of the first three fields.
    assert_eq!(boundaries, 4);
}

#[test]
fn rejects_an_integer_over_its_bound() {
    let mut decoder = Decoder::default();
    assert_eq!(decode(&mut decoder, &[0xff, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]), Err(DecodeError::IntegerOverflow));
    assert_eq!(decode(&mut decoder, &[0x3f, 0xff, 0xff, 0xff, 0xff, 0x7f]), Err(DecodeError::IntegerOverflow));
    // A string length, as each part of a literal.
    assert_eq!(decode(&mut decoder, &[0x40, 0x7f, 0xff, 0xff, 0xff, 0xff, 0x7f]), Err(DecodeError::IntegerOverflow));
    assert_eq!(decode(&mut decoder, &[0x41, 0x7f, 0xff, 0xff, 0xff, 0xff, 0x7f]), Err(DecodeError::IntegerOverflow));
}

#[test]
fn rejects_a_string_over_its_limit_before_it_arrives() {
    let mut decoder = decoder_with(DecoderOptions { string_length_limit: 4, ..DecoderOptions::default() });
    assert_eq!(decode(&mut decoder, &literal(0x00, "four", "four")).unwrap(), [field("four", "four")]);
    // Only the length has arrived, and it is already too long.
    assert_eq!(decoder.decode(&[0x00, 0x05], |_| {}), Err(DecodeError::StringTooLong { length: 5, limit: 4 }));
    assert_eq!(decoder.decode(&[0x01, 0x7f, 0x80, 0x01], |_| {}), Err(DecodeError::StringTooLong { length: 255, limit: 4 }));
}

#[test]
fn rejects_invalid_huffman_strings() {
    let mut decoder = Decoder::default();
    // A name with the end-of-string symbol, then a value with bad padding.
    let end_of_string = [0x40, 0x86, 0x1c, 0x7f, 0xff, 0xff, 0xff, 0xff, 0x00];
    assert_eq!(decode(&mut decoder, &end_of_string), Err(DecodeError::HuffmanEndOfString));
    let padding = [0x41, 0x82, 0x00, 0x00];
    assert_eq!(decode(&mut decoder, &padding), Err(DecodeError::HuffmanPadding));
    // Neither changed the table.
    assert!(decoder.table().is_empty());
}

#[test]
fn an_error_ends_the_block() {
    let mut decoder = Decoder::default();
    // A pending incomplete representation, then an error in the next fragment.
    decoder.decode(&[0x82, 0x40, 0x05, b'a'], |_| {}).unwrap();
    assert_eq!(decoder.section().field_count(), 1);
    assert_eq!(decoder.decode(&[b'b', b'c', b'd', b'e', 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], |_| {}), Err(DecodeError::IntegerOverflow));
    assert_eq!(decoder.section(), SectionSummary::default());
    // The next block starts afresh, without the earlier bytes.
    assert_eq!(decode(&mut decoder, &[0x82]).unwrap(), [field(":method", "GET")]);
}

#[test]
fn errors_describe_themselves() {
    let errors = [
        (DecodeError::Truncated, "header block ended within a representation"),
        (DecodeError::IntegerOverflow, "integer exceeds the HPACK integer bound"),
        (DecodeError::InvalidIndex { index: 70 }, "invalid table index 70"),
        (DecodeError::StringTooLong { length: 5, limit: 4 }, "string length 5 exceeds limit 4"),
        (DecodeError::HuffmanEndOfString, "Huffman coded string contains the end-of-string symbol"),
        (DecodeError::HuffmanPadding, "Huffman coded string has invalid padding"),
        (DecodeError::TableSizeTooLarge { size: 512, limit: 256 }, "dynamic table size 512 exceeds limit 256"),
        (DecodeError::MisplacedTableSizeUpdate, "dynamic table size update after a field representation"),
        (DecodeError::MissingTableSizeUpdate, "header block does not begin with the required dynamic table size update"),
    ];
    for (error, message) in errors {
        assert_eq!(error.to_string(), message);
        let error: Box<dyn std::error::Error> = Box::new(error);
        assert!(error.source().is_none());
    }
}

#[test]
fn decoders_are_independent_and_cloneable() {
    let mut decoder = Decoder::default();
    decode(&mut decoder, &literal(0x40, "x", "y")).unwrap();
    let mut clone = decoder.clone();
    decode(&mut decoder, &[0x20]).unwrap();
    assert!(decoder.table().is_empty());
    assert_eq!(decode(&mut clone, &[0xbe]).unwrap(), [field("x", "y")]);
    assert!(format!("{clone:?}").contains("Decoder"));
}

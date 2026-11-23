// Released under the MIT License.
// Copyright, 2013-2015, by Ilya Grigorik.
// Copyright, 2014, by Kaoru Maeda.
// Copyright, 2015, by Tamir Duberstein.
// Copyright, 2018, by Byron Formwalt.
// Copyright, 2018, by Kenichi Nakamura.
// Copyright, 2018, by Tiago Cardoso.
// Copyright, 2018-2026, by Samuel Williams.
// Copyright, 2019, by Jingyi Chen.

//! Round trips through the encoder and decoder, `protocol-hpack`'s sequence tests, and property tests against `fluke-hpack` as a differential oracle.

mod support;

use base_http_hpack::{DecodeError, Decoder, DecoderOptions, Encoder, EncoderOptions, HuffmanUse, Indexing, STATIC_TABLE, TableUse};
use support::*;

fn sequence() -> Vec<Vec<Field>> {
    json(&fixture("protocol-hpack/sequence1.json"))
        .as_array()
        .unwrap()
        .iter()
        .map(|headers| {
            headers
                .as_array()
                .unwrap()
                .iter()
                .map(|pair| field(pair[0].as_str().unwrap(), pair[1].as_str().unwrap()))
                .collect()
        })
        .collect()
}

/// `protocol-hpack`'s compressor `#encode` tests: each block is encoded with the given table size limit and decoded, and both tables agree.
fn encode_sequence(mut table_size: impl FnMut() -> usize) -> Decoder {
    let mut encoder = Encoder::new(EncoderOptions {
        table_size_limit: 65536,
        ..EncoderOptions::default()
    });
    let mut decoder = Decoder::new(DecoderOptions {
        table_size_limit: 65536,
        ..DecoderOptions::default()
    });
    for headers in sequence() {
        encoder.set_table_size_limit(table_size());
        let block = encode(&mut encoder, &headers, Indexing::Incremental);
        assert_eq!(decode(&mut decoder, &block).unwrap(), headers);
        assert_eq!(table(decoder.table().iter()), table(encoder.table().iter()));
        assert_eq!(decoder.table().capacity(), encoder.table().capacity());
    }
    decoder
}

#[test]
fn encodes_the_sequence_with_a_small_table() {
    assert_eq!(encode_sequence(|| 512).table().capacity(), 512);
}

#[test]
fn encodes_the_sequence_with_the_default_table() {
    assert_eq!(encode_sequence(|| 4096).table().capacity(), 4096);
}

#[test]
fn encodes_the_sequence_with_a_large_table() {
    assert_eq!(encode_sequence(|| 65536).table().capacity(), 65536);
}

#[test]
fn encodes_the_sequence_with_random_table_sizes() {
    let mut random = Random::new(1);
    encode_sequence(|| 128 + random.below(65536 - 128));
}

fn random_name(random: &mut Random) -> Vec<u8> {
    match random.below(5) {
        0 | 1 => STATIC_TABLE[random.below(STATIC_TABLE.len())].0.as_bytes().to_vec(),
        2 => ["x-request-id", "x-forwarded-for", "user-agent", "cookie", "x-trace"][random.below(5)].as_bytes().to_vec(),
        3 => (0..1 + random.below(20)).map(|_| b'a' + random.below(26) as u8).collect(),
        _ => random.bytes(12),
    }
}

fn random_value(random: &mut Random) -> Vec<u8> {
    match random.below(6) {
        0 => STATIC_TABLE[random.below(STATIC_TABLE.len())].1.as_bytes().to_vec(),
        1 => b"value".to_vec(),
        2 | 3 => (0..random.below(60)).map(|_| b' ' + random.below(95) as u8).collect(),
        4 => random.bytes(300),
        _ => random.bytes(2000),
    }
}

fn random_fields(random: &mut Random) -> Vec<(Field, Indexing)> {
    (0..random.below(12))
        .map(|_| {
            let indexing = match random.below(6) {
                0 => Indexing::NotIndexed,
                1 => Indexing::NeverIndexed,
                _ => Indexing::Incremental,
            };
            ((random_name(random), random_value(random)), indexing)
        })
        .collect()
}

/// Splits `block` into up to four fragments at random positions.
fn fragments<'a>(random: &mut Random, block: &'a [u8]) -> Vec<&'a [u8]> {
    let mut splits: Vec<usize> = (0..random.below(4)).map(|_| random.below(block.len() + 1)).collect();
    splits.sort();
    let mut fragments = Vec::new();
    let mut start = 0;
    for split in splits {
        fragments.push(&block[start..split]);
        start = split;
    }
    fragments.push(&block[start..]);
    fragments
}

#[test]
fn random_fields_round_trip_in_random_fragments() {
    const LIMITS: [usize; 6] = [0, 64, 100, 512, 4096, 16384];
    for seed in 0..100 {
        let mut random = Random::new(seed);
        let mut encoder = Encoder::new(EncoderOptions {
            table_size_limit: [0, 256, 4096, 65536][random.below(4)],
            huffman: [HuffmanUse::Never, HuffmanUse::Always, HuffmanUse::Shorter][random.below(3)],
            tables: [TableUse::None, TableUse::Static, TableUse::All][random.below(3)],
            ..EncoderOptions::default()
        });
        let mut decoder = Decoder::new(DecoderOptions {
            section_size_limit: usize::MAX,
            field_count_limit: usize::MAX,
            ..DecoderOptions::default()
        });
        let mut oracle = fluke_hpack::Decoder::new();
        for round in 0..20 {
            // A SETTINGS_HEADER_TABLE_SIZE change, sometimes lowered and raised again, reaching both sides before the next block.
            for _ in 0..random.below(3) {
                let limit = LIMITS[random.below(LIMITS.len())];
                encoder.set_table_size_limit(limit);
                decoder.set_table_size_limit(limit);
            }
            let fields = random_fields(&mut random);
            let mut block = Vec::new();
            let mut fields_encoder = encoder.begin_block(&mut block);
            for ((name, value), indexing) in &fields {
                fields_encoder.encode(name, value, *indexing);
            }

            let context = format!("seed {seed} round {round}");
            let mut decoded = Vec::new();
            for fragment in fragments(&mut random, &block) {
                decoder.decode(fragment, |field| decoded.push((owned(field), field.indexing()))).unwrap();
            }
            assert_eq!(decoder.finish().unwrap().field_count(), fields.len(), "{context}");
            for ((sent, indexing), (received, received_indexing)) in fields.iter().zip(&decoded) {
                assert_eq!(sent, received, "{context}");
                // Sensitivity always survives; a field without indexing may arrive through a table reference.
                assert_eq!(*indexing == Indexing::NeverIndexed, *received_indexing == Indexing::NeverIndexed, "{context}");
            }
            assert_eq!(table(decoder.table().iter()), table(encoder.table().iter()), "{context}");
            assert_eq!(decoder.table().capacity(), encoder.table().capacity(), "{context}");

            // `fluke-hpack` rejects a block of only table size updates, which RFC 7541 permits.
            if !fields.is_empty() {
                let oracle_fields = oracle.decode(&block).unwrap_or_else(|error| panic!("{context}: {error:?}"));
                assert_eq!(oracle_fields, fields.iter().map(|(field, _)| field.clone()).collect::<Vec<_>>(), "{context}");
            } else if !block.is_empty() {
                oracle.decode(&[&block[..], &[0x82]].concat()).unwrap();
            }
        }
    }
}

#[test]
fn decodes_what_fluke_hpack_encodes() {
    for seed in 0..100 {
        let mut random = Random::new(seed);
        let mut oracle = fluke_hpack::Encoder::new();
        let mut decoder = Decoder::new(DecoderOptions {
            section_size_limit: usize::MAX,
            field_count_limit: usize::MAX,
            ..DecoderOptions::default()
        });
        for round in 0..20 {
            let fields: Vec<Field> = random_fields(&mut random).into_iter().map(|(field, _)| field).collect();
            let block = oracle.encode(fields.iter().map(|(name, value)| (&name[..], &value[..])));
            assert_eq!(decode(&mut decoder, &block).unwrap(), fields, "seed {seed} round {round}");
        }
    }
}

/// Decodes `blocks` whole and in random fragments, which must agree on every field and outcome.
fn decode_both_ways(random: &mut Random, blocks: &[Vec<u8>], options: &DecoderOptions) -> Vec<Result<usize, DecodeError>> {
    let mut whole = Decoder::new(options.clone());
    let mut split = Decoder::new(options.clone());
    let mut outcomes = Vec::new();
    for block in blocks {
        let mut whole_fields = Vec::new();
        let whole_result = whole.decode_block(block, |field| whole_fields.push((owned(field), field.static_index(), field.indexing())));
        let mut split_fields = Vec::new();
        let mut split_result = Ok(());
        for fragment in fragments(random, block) {
            split_result = split.decode(fragment, |field| split_fields.push((owned(field), field.static_index(), field.indexing())));
            if split_result.is_err() {
                break;
            }
        }
        let split_result = split_result.and_then(|()| split.finish());
        assert_eq!(whole_fields, split_fields, "{block:02x?}");
        assert_eq!(whole_result, split_result, "{block:02x?}");
        assert_eq!(table(whole.table().iter()), table(split.table().iter()));
        outcomes.push(whole_result.map(|summary| summary.field_count()));
    }
    outcomes
}

#[test]
fn arbitrary_input_decodes_alike_whole_and_in_fragments() {
    let options = DecoderOptions {
        table_size_limit: 256,
        initial_table_size: 256,
        section_size_limit: 200,
        field_count_limit: 4,
        string_length_limit: 40,
    };
    let mut accepted = 0;
    let mut rejected = 0;
    for seed in 0..3000 {
        let mut random = Random::new(seed);
        let blocks: Vec<Vec<u8>> = (0..3)
            .map(|_| {
                // Mostly small representations, so that valid blocks are common.
                (0..random.below(12))
                    .map(|_| match random.below(8) {
                        0 => 0x80 | random.below(70) as u8,
                        1 => 0x40 | random.below(4) as u8,
                        2 => 0x20 | random.below(32) as u8,
                        3 => random.below(8) as u8,
                        _ => random.next() as u8,
                    })
                    .collect()
            })
            .collect();
        for outcome in decode_both_ways(&mut random, &blocks, &options) {
            match outcome {
                Ok(_) => accepted += 1,
                Err(_) => rejected += 1,
            }
        }
    }
    assert!(accepted > 1000 && rejected > 1000, "{accepted} accepted, {rejected} rejected");
}

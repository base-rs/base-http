// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! The RFC 7541 Appendix C examples, as `protocol-hpack`'s `test/protocol/hpack/rfc7541.rb` runs them.

mod support;

use base_http_hpack::{Decoder, DecoderOptions, Encoder, EncoderOptions, HuffmanUse, Indexing};
use serde_json::Value;
use support::*;

struct Stream {
    wire: Vec<u8>,
    emitted: Vec<Field>,
    table: Vec<Field>,
    table_size: usize,
}

struct Example {
    title: String,
    table_size: usize,
    huffman: HuffmanUse,
    decompressor_only: bool,
    streams: Vec<Stream>,
}

fn pairs(value: &Value) -> Vec<Field> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|pair| field(pair[0].as_str().unwrap(), pair[1].as_str().unwrap()))
        .collect()
}

fn examples() -> Vec<Example> {
    let examples: Vec<Example> = json_files(&fixture("protocol-hpack/rfc7541"))
        .iter()
        .map(|path| {
            let fixture = json(path);
            Example {
                title: fixture["title"].as_str().unwrap().to_owned(),
                table_size: fixture["table_size"].as_u64().unwrap() as usize,
                huffman: match fixture["huffman"].as_str().unwrap() {
                    "never" => HuffmanUse::Never,
                    "always" => HuffmanUse::Always,
                    other => panic!("unknown Huffman mode {other}"),
                },
                decompressor_only: fixture.get("only").is_some_and(|only| only == "decompressor"),
                streams: fixture["streams"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|stream| Stream {
                        wire: hex(stream["wire"].as_str().unwrap()),
                        emitted: pairs(&stream["emitted"]),
                        table: pairs(&stream["table"]),
                        table_size: stream["table_size"].as_u64().unwrap() as usize,
                    })
                    .collect(),
            }
        })
        .collect();
    assert_eq!(examples.len(), 5);
    examples
}

#[test]
fn decoder_emits_the_fields_and_keeps_the_table() {
    for example in examples() {
        let mut decoder = Decoder::new(DecoderOptions {
            table_size_limit: example.table_size,
            initial_table_size: example.table_size,
            ..DecoderOptions::default()
        });
        for (nth, stream) in example.streams.iter().enumerate() {
            let context = format!("{} request {}", example.title, nth + 1);
            assert_eq!(decode(&mut decoder, &stream.wire).unwrap(), stream.emitted, "{context}");
            assert_eq!(table(decoder.table().iter()), stream.table, "{context}");
            assert_eq!(decoder.table().size(), stream.table_size, "{context}");
        }
    }
}

#[test]
fn encoder_emits_the_bytes_and_keeps_the_table() {
    let mut encoded = 0;
    for example in examples().into_iter().filter(|example| !example.decompressor_only) {
        let mut encoder = Encoder::new(EncoderOptions {
            initial_table_size: example.table_size,
            table_size_limit: example.table_size,
            huffman: example.huffman,
            ..EncoderOptions::default()
        });
        for (nth, stream) in example.streams.iter().enumerate() {
            let context = format!("{} request {}", example.title, nth + 1);
            assert_eq!(encode(&mut encoder, &stream.emitted, Indexing::Incremental), stream.wire, "{context}");
            assert_eq!(table(encoder.table().iter()), stream.table, "{context}");
            assert_eq!(encoder.table().size(), stream.table_size, "{context}");
        }
        encoded += 1;
    }
    assert_eq!(encoded, 4);
}

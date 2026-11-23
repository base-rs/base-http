// Released under the MIT License.
// Copyright, 2014, by Kaoru Maeda.
// Copyright, 2014-2016, by Ilya Grigorik.
// Copyright, 2015, by Tamir Duberstein.
// Copyright, 2016, by Kien Nguyen Trung.
// Copyright, 2018-2026, by Samuel Williams.

//! The hpack-test-case corpus, as `protocol-hpack`'s `test/protocol/hpack/external.rb` runs it: every implementation's encoded stories are decoded, and every raw story is encoded with each encoder configuration and decoded again, by this decoder and by `fluke-hpack`.

mod support;

use base_http_hpack::{
    Decoder, DecoderOptions, Encoder, EncoderOptions, HuffmanUse, Indexing, TableUse,
};
use serde_json::Value;
use std::path::Path;
use support::*;

/// The encoded directories `external.rb` lists that exist at the imported revision, and those it does not list.
const ENCODED: [&str; 15] = [
    "go-hpack",
    "haskell-http2-linear",
    "haskell-http2-linear-huffman",
    "haskell-http2-naive",
    "haskell-http2-naive-huffman",
    "haskell-http2-static",
    "haskell-http2-static-huffman",
    "nghttp2",
    "nghttp2-16384-4096",
    "nghttp2-change-table-size",
    "node-http2-hpack",
    "python-hpack",
    "swift-nio-hpack-huffman",
    "swift-nio-hpack-plain-text",
    "raw-data",
];

struct Case {
    header_table_size: Option<usize>,
    wire: Vec<u8>,
    headers: Vec<Field>,
}

fn cases(path: &Path) -> Vec<Case> {
    let story = json(path);
    story["cases"]
        .as_array()
        .unwrap()
        .iter()
        .map(|case| Case {
            // swift-nio's stories write `null` for an unchanged size.
            header_table_size: case
                .get("header_table_size")
                .and_then(Value::as_u64)
                .map(|size| size as usize),
            wire: case
                .get("wire")
                .map_or_else(Vec::new, |wire| hex(wire.as_str().unwrap())),
            headers: case["headers"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|header| {
                    header
                        .as_object()
                        .unwrap()
                        .iter()
                        .map(|(name, value)| field(name, value.as_str().unwrap()))
                })
                .collect(),
        })
        .collect()
}

/// Decodes one implementation's story, applying each acknowledged `header_table_size` before its case.
fn decode_story(path: &Path) {
    let mut decoder = Decoder::new(DecoderOptions {
        section_size_limit: usize::MAX,
        field_count_limit: usize::MAX,
        ..DecoderOptions::default()
    });
    for (seqno, case) in cases(path).iter().enumerate() {
        if let Some(size) = case.header_table_size {
            decoder.set_table_size_limit(size);
        }
        let decoded = decode(&mut decoder, &case.wire)
            .unwrap_or_else(|error| panic!("{} case {seqno}: {error}", path.display()));
        assert_eq!(decoded, case.headers, "{} case {seqno}", path.display());
    }
}

/// Encodes a raw story with one configuration, decoding every block with this decoder and with `fluke-hpack`.
fn encode_story(
    path: &Path,
    tables: TableUse,
    huffman: HuffmanUse,
    table_size_limit: usize,
) -> usize {
    let mut encoder = Encoder::new(EncoderOptions {
        table_size_limit,
        huffman,
        tables,
        ..EncoderOptions::default()
    });
    let mut decoder = Decoder::new(DecoderOptions {
        section_size_limit: usize::MAX,
        field_count_limit: usize::MAX,
        ..DecoderOptions::default()
    });
    let mut oracle = fluke_hpack::Decoder::new();
    let mut bytes = 0;
    for (seqno, case) in cases(path).iter().enumerate() {
        let context = format!(
            "{} case {seqno} with {tables:?}, {huffman:?} and {table_size_limit}",
            path.display()
        );
        let block = encode(&mut encoder, &case.headers, Indexing::Incremental);
        assert_eq!(
            decode(&mut decoder, &block).unwrap(),
            case.headers,
            "{context}"
        );
        assert_eq!(
            table(decoder.table().iter()),
            table(encoder.table().iter()),
            "{context}"
        );
        let oracle_fields: Vec<Field> = oracle
            .decode(&block)
            .unwrap_or_else(|error| panic!("{context}: {error:?}"));
        assert_eq!(oracle_fields, case.headers, "{context}");
        bytes += block.len();
    }
    bytes
}

fn stories(directory: &str) -> Vec<std::path::PathBuf> {
    json_files(&fixture("hpack-test-case").join(directory))
}

#[test]
fn imports_four_stories_of_every_directory() {
    for directory in ENCODED {
        assert_eq!(stories(directory).len(), 4, "{directory}");
    }
}

#[test]
fn decodes_every_implementations_stories() {
    for directory in ENCODED.iter().filter(|&&directory| directory != "raw-data") {
        for path in stories(directory) {
            decode_story(&path);
        }
    }
}

#[test]
fn encodes_raw_stories_with_every_configuration() {
    for path in stories("raw-data") {
        for tables in [TableUse::None, TableUse::Static, TableUse::All] {
            let mut sizes = Vec::new();
            for huffman in [HuffmanUse::Never, HuffmanUse::Always, HuffmanUse::Shorter] {
                for table_size_limit in [4096, 512] {
                    sizes.push(encode_story(&path, tables, huffman, table_size_limit));
                }
            }
            // Choosing the shorter string is never longer than either fixed choice.
            assert!(
                sizes[4] <= sizes[0] && sizes[4] <= sizes[2],
                "{} {tables:?} {sizes:?}",
                path.display()
            );
        }
    }
}

/// Runs the whole corpus from a checkout, as `tests/fixtures/readme.md` describes.
#[test]
#[ignore = "needs a checkout of hpack-test-case named by HPACK_TEST_CASE; see tests/fixtures/readme.md"]
fn full_corpus() {
    let root = std::env::var_os("HPACK_TEST_CASE")
        .expect("HPACK_TEST_CASE names a checkout of hpack-test-case");
    let root = Path::new(&root);
    let mut decoded = 0;
    for directory in ENCODED.iter().filter(|&&directory| directory != "raw-data") {
        for path in json_files(&root.join(directory)) {
            decode_story(&path);
            decoded += 1;
        }
    }
    let mut encoded = 0;
    for path in json_files(&root.join("raw-data")) {
        for tables in [TableUse::None, TableUse::Static, TableUse::All] {
            for huffman in [HuffmanUse::Never, HuffmanUse::Always, HuffmanUse::Shorter] {
                encode_story(&path, tables, huffman, 4096);
            }
        }
        encoded += 1;
    }
    println!("decoded {decoded} stories and encoded {encoded}");
    assert!(decoded >= 14 * 31 && encoded >= 32);
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![allow(dead_code, reason = "each test target uses a different part of the support module")]

use base_http_hpack::{DecodeError, DecodedField, Decoder, Encoder, Indexing, SectionSummary};
use std::path::{Path, PathBuf};

pub type Field = (Vec<u8>, Vec<u8>);

pub fn fixture(path: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(path)
}

pub fn json(path: &Path) -> serde_json::Value {
    let text = std::fs::read_to_string(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    serde_json::from_str(&text).unwrap()
}

/// Every `*.json` file in `directory`, in name order.
pub fn json_files(directory: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("{}: {error}", directory.display()))
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "json"))
        .collect();
    files.sort();
    files
}

/// Decodes hexadecimal text, ignoring whitespace.
pub fn hex(text: &str) -> Vec<u8> {
    let digits: Vec<u8> = text.bytes().filter(|byte| !byte.is_ascii_whitespace()).collect();
    digits
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

pub fn field(name: &str, value: &str) -> Field {
    (name.as_bytes().to_vec(), value.as_bytes().to_vec())
}

pub fn owned(field: DecodedField<'_>) -> Field {
    (field.name().to_vec(), field.value().to_vec())
}

/// Decodes a whole block, collecting its delivered fields.
pub fn decode(decoder: &mut Decoder, block: &[u8]) -> Result<Vec<Field>, DecodeError> {
    let mut fields = Vec::new();
    decoder.decode_block(block, |field| fields.push(owned(field)))?;
    Ok(fields)
}

/// Decodes a whole block, collecting every delivered field with its static index and indexing.
pub fn decode_detailed(decoder: &mut Decoder, block: &[u8]) -> Result<(Vec<(Field, Option<usize>, Indexing)>, SectionSummary), DecodeError> {
    let mut fields = Vec::new();
    let summary = decoder.decode_block(block, |field| fields.push((owned(field), field.static_index(), field.indexing())))?;
    Ok((fields, summary))
}

/// Encodes one block of fields, all with the same indexing.
pub fn encode(encoder: &mut Encoder, fields: &[Field], indexing: Indexing) -> Vec<u8> {
    let mut block = Vec::new();
    let mut fields_encoder = encoder.begin_block(&mut block);
    for (name, value) in fields {
        fields_encoder.encode(name, value, indexing);
    }
    block
}

pub fn table(entries: impl Iterator<Item = (impl AsRef<[u8]>, impl AsRef<[u8]>)>) -> Vec<Field> {
    entries.map(|(name, value)| (name.as_ref().to_vec(), value.as_ref().to_vec())).collect()
}

/// A small deterministic generator for property tests, so failures reproduce.
pub struct Random(u64);

impl Random {
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
    }

    pub fn next(&mut self) -> u64 {
        // xorshift64*.
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    pub fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    pub fn chance(&mut self, numerator: usize, denominator: usize) -> bool {
        self.below(denominator) < numerator
    }

    /// Fewer than `bound` random bytes.
    pub fn bytes(&mut self, bound: usize) -> Vec<u8> {
        let length = self.below(bound);
        (0..length).map(|_| self.next() as u8).collect()
    }
}

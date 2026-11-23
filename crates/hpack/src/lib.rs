// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![doc = include_str!("../readme.md")]
#![forbid(unsafe_code)]

mod decode_error;
mod decoder;
mod dynamic_table;
mod encoder;
mod huffman;
mod indexing;
mod integer;
mod static_table;

pub use decode_error::DecodeError;
pub use decoder::{DecodedField, Decoder, DecoderOptions, SectionLimit, SectionSummary};
pub use dynamic_table::DynamicTable;
pub use encoder::{Encoder, EncoderOptions, HeaderBlockEncoder, HuffmanUse, TableUse};
pub use indexing::Indexing;
pub use static_table::STATIC_TABLE;

/// The dynamic table size both endpoints assume before any update: HTTP/2's initial `SETTINGS_HEADER_TABLE_SIZE`, RFC 9113 §6.5.2.
pub const DEFAULT_TABLE_SIZE: usize = 4096;

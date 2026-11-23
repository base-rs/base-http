// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::{error::Error, fmt};

/// A header block that cannot be decoded.
///
/// Every variant leaves the decoder's dynamic table possibly different from the encoder's, so HTTP/2 treats each as a `COMPRESSION_ERROR` connection error, as RFC 9113 §4.3 requires. Exceeding the field section limits is not an error: see [`SectionSummary`](crate::SectionSummary).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// The block ended within a representation.
    Truncated,
    /// An integer used more continuation bytes, or had a larger value, than any HPACK integer needs.
    IntegerOverflow,
    /// An index was zero or beyond the end of the dynamic table.
    InvalidIndex { index: usize },
    /// A string literal's encoded length exceeded [`DecoderOptions::string_length_limit`](crate::DecoderOptions::string_length_limit).
    StringTooLong { length: usize, limit: usize },
    /// A Huffman coded string contained the end-of-string symbol.
    HuffmanEndOfString,
    /// A Huffman coded string ended with padding longer than seven bits, or with bits that are not a prefix of the end-of-string code.
    HuffmanPadding,
    /// A dynamic table size update exceeded the decoder's table size limit.
    TableSizeTooLarge { size: usize, limit: usize },
    /// A dynamic table size update followed a field representation in the same block.
    MisplacedTableSizeUpdate,
    /// The decoder's table size limit was lowered below the table's capacity, and the next block did not begin with a dynamic table size update within the new limit, as RFC 7541 §4.2 requires.
    MissingTableSizeUpdate,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => formatter.write_str("header block ended within a representation"),
            Self::IntegerOverflow => formatter.write_str("integer exceeds the HPACK integer bound"),
            Self::InvalidIndex { index } => write!(formatter, "invalid table index {index}"),
            Self::StringTooLong { length, limit } => write!(formatter, "string length {length} exceeds limit {limit}"),
            Self::HuffmanEndOfString => formatter.write_str("Huffman coded string contains the end-of-string symbol"),
            Self::HuffmanPadding => formatter.write_str("Huffman coded string has invalid padding"),
            Self::TableSizeTooLarge { size, limit } => write!(formatter, "dynamic table size {size} exceeds limit {limit}"),
            Self::MisplacedTableSizeUpdate => formatter.write_str("dynamic table size update after a field representation"),
            Self::MissingTableSizeUpdate => formatter.write_str("header block does not begin with the required dynamic table size update"),
        }
    }
}

impl Error for DecodeError {}

// Released under the MIT License.
// Copyright, 2018-2026, by Samuel Williams.
// Copyright, 2024, by Maruth Goyal.
// Copyright, 2024, by Nathan Froyd.

//! The decoder, after `protocol-hpack`'s `Decompressor` and the decoding half of its `Context`.

use crate::{
    DEFAULT_TABLE_SIZE, DecodeError, Indexing,
    dynamic_table::{DynamicTable, ENTRY_OVERHEAD, EntryName},
    huffman, integer,
    static_table::{STATIC_TABLE, STATIC_TABLE_LEN},
};
use std::ops::Range;

/// The decoder's configuration. Every limit is finite by default, with HTTP/2's defaults from the Base HTTP/2 requirements.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecoderOptions {
    /// The largest dynamic table size an update may set, such as the `SETTINGS_HEADER_TABLE_SIZE` this endpoint advertised in HTTP/2. A limit below [`initial_table_size`](Self::initial_table_size) requires the first block to begin with an update within it.
    ///
    /// HTTP/2 applies a local limit only once the peer acknowledges it, so an HTTP/2 decoder starts with [`DEFAULT_TABLE_SIZE`] and applies a configured limit with [`Decoder::set_table_size_limit`] on acknowledgment.
    pub table_size_limit: usize,
    /// The dynamic table's capacity before any update, agreed outside HPACK: [`DEFAULT_TABLE_SIZE`] for HTTP/2, whose initial `SETTINGS_HEADER_TABLE_SIZE` it is.
    pub initial_table_size: usize,
    /// The largest field section, in RFC 9113 §6.5.2's measure: each field's name and value lengths plus 32. HTTP/2's `SETTINGS_MAX_HEADER_LIST_SIZE`.
    pub section_size_limit: usize,
    /// The most fields in a section.
    pub field_count_limit: usize,
    /// The longest string literal, in encoded bytes. A Huffman coded string decodes to at most eight fifths of this. The default equals HTTP/2's default header block limit, so that the block limit, not this, bounds ordinary strings.
    pub string_length_limit: usize,
}

impl Default for DecoderOptions {
    fn default() -> Self {
        Self {
            table_size_limit: DEFAULT_TABLE_SIZE,
            initial_table_size: DEFAULT_TABLE_SIZE,
            section_size_limit: 32 * 1024,
            field_count_limit: 100,
            string_length_limit: 64 * 1024,
        }
    }
}

/// A field section limit that a header block exceeded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SectionLimit {
    /// [`DecoderOptions::section_size_limit`].
    Size,
    /// [`DecoderOptions::field_count_limit`].
    FieldCount,
}

/// The size of a field section, decoded so far or in full.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SectionSummary {
    size: usize,
    field_count: usize,
    limit_exceeded: Option<SectionLimit>,
}

impl SectionSummary {
    /// The section's size in RFC 9113 §6.5.2's measure, including fields not delivered.
    pub fn size(&self) -> usize {
        self.size
    }

    /// The number of fields in the section, including fields not delivered.
    pub fn field_count(&self) -> usize {
        self.field_count
    }

    /// The first limit the section exceeded. The field that exceeded it, and every later field, were decoded, and updated the dynamic table, but were not delivered.
    pub fn limit_exceeded(&self) -> Option<SectionLimit> {
        self.limit_exceeded
    }
}

/// A decoded field, borrowed from the decoder or its input until the next field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodedField<'a> {
    name: &'a [u8],
    value: &'a [u8],
    static_index: Option<usize>,
    indexing: Indexing,
}

impl<'a> DecodedField<'a> {
    /// The name's bytes, as sent: HPACK does not validate them.
    pub fn name(&self) -> &'a [u8] {
        self.name
    }

    /// The value's bytes, as sent: HPACK does not validate them.
    pub fn value(&self) -> &'a [u8] {
        self.value
    }

    /// The static table index, from 1 to 61, that the name came from, so that a protocol can dispatch on it without comparing names. A name sent as a literal or taken from the dynamic table has none, even when it equals a static table name.
    pub fn static_index(&self) -> Option<usize> {
        self.static_index
    }

    /// How the field was sent. A [`Indexing::NeverIndexed`] field is sensitive and must be forwarded as one.
    pub fn indexing(&self) -> Indexing {
        self.indexing
    }
}

/// Decodes header blocks, keeping the dynamic table that mirrors the peer's encoder.
///
/// Its state is independent of any encoder. Every block the peer sends must be decoded, in order, even when its fields are not wanted, or the table no longer matches the encoder's. A block can be decoded in fragments as they arrive, such as HTTP/2's HEADERS and CONTINUATION frames: [`decode`](Self::decode) delivers each complete field to a callback, which cannot stop decoding, and keeps any incomplete representation for the next fragment, and [`finish`](Self::finish) ends the block.
///
/// Decoding is bounded: integers, string lengths, table sizes and indices are checked as they are read, and the section's size and field count as each field is decoded. A section over its limits is decoded to its end without delivering further fields, so its work stays proportional to its bytes.
#[derive(Clone, Debug)]
pub struct Decoder {
    core: DecoderCore,
    /// An incomplete representation at the end of the last fragment, and the fragment after it while it is decoded.
    pending: Vec<u8>,
}

#[derive(Clone, Debug)]
struct DecoderCore {
    table: DynamicTable,
    table_size_limit: usize,
    section_size_limit: usize,
    field_count_limit: usize,
    string_length_limit: usize,
    /// The smallest table size limit set since the last dynamic table size update, while that is below the table's capacity.
    required_update: Option<usize>,
    /// Decoded Huffman strings of the current field.
    scratch: Vec<u8>,
    block: BlockState,
}

#[derive(Clone, Debug, Default)]
struct BlockState {
    field_seen: bool,
    summary: SectionSummary,
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new(DecoderOptions::default())
    }
}

impl Decoder {
    pub fn new(options: DecoderOptions) -> Self {
        let mut decoder = Self {
            core: DecoderCore {
                table: DynamicTable::new(options.initial_table_size),
                table_size_limit: options.table_size_limit,
                section_size_limit: options.section_size_limit,
                field_count_limit: options.field_count_limit,
                string_length_limit: options.string_length_limit,
                required_update: None,
                scratch: Vec::new(),
                block: BlockState::default(),
            },
            pending: Vec::new(),
        };
        decoder.set_table_size_limit(options.table_size_limit);
        decoder
    }

    /// The decoder's dynamic table.
    pub fn table(&self) -> &DynamicTable {
        &self.core.table
    }

    /// The largest dynamic table size an update may set.
    pub fn table_size_limit(&self) -> usize {
        self.core.table_size_limit
    }

    /// Changes the largest dynamic table size an update may set, such as when an HTTP/2 peer acknowledges a new `SETTINGS_HEADER_TABLE_SIZE`.
    ///
    /// A limit below the table's capacity requires the next block to begin with a dynamic table size update no larger than the smallest limit set before it, since the encoder must signal that, as RFC 7541 §4.2 requires. The table keeps its capacity until then.
    pub fn set_table_size_limit(&mut self, limit: usize) {
        let core = &mut self.core;
        core.table_size_limit = limit;
        if limit < core.table.capacity() {
            core.required_update = Some(
                core.required_update
                    .map_or(limit, |minimum| minimum.min(limit)),
            );
        }
    }

    /// The current block's section, as decoded so far.
    pub fn section(&self) -> SectionSummary {
        self.core.block.summary
    }

    /// Decodes a fragment of a header block, calling `on_field` with each complete field the section limits allow.
    ///
    /// A representation incomplete at the end of `fragment` is kept, at most one field's encoded bytes, and decoded with the next fragment. An error ends the block, and leaves the dynamic table possibly different from the encoder's.
    pub fn decode(
        &mut self,
        fragment: &[u8],
        mut on_field: impl FnMut(DecodedField<'_>),
    ) -> Result<(), DecodeError> {
        let result = if self.pending.is_empty() {
            self.core
                .decode(fragment, &mut on_field)
                .map(|consumed| self.pending.extend_from_slice(&fragment[consumed..]))
        } else {
            self.pending.extend_from_slice(fragment);
            self.core
                .decode(&self.pending, &mut on_field)
                .map(|consumed| {
                    self.pending.drain(..consumed);
                })
        };
        if result.is_err() {
            self.reset_block();
        }
        result
    }

    /// Ends the current header block, returning its section's size and any limit it exceeded.
    ///
    /// Fails with [`DecodeError::Truncated`] if the block ended within a representation, and with [`DecodeError::MissingTableSizeUpdate`] if it lacked a required dynamic table size update.
    pub fn finish(&mut self) -> Result<SectionSummary, DecodeError> {
        let result = if !self.pending.is_empty() {
            Err(DecodeError::Truncated)
        } else if self.core.required_update.is_some() {
            Err(DecodeError::MissingTableSizeUpdate)
        } else {
            Ok(self.core.block.summary)
        };
        self.reset_block();
        result
    }

    /// Decodes a whole header block: [`decode`](Self::decode) and then [`finish`](Self::finish).
    pub fn decode_block(
        &mut self,
        block: &[u8],
        on_field: impl FnMut(DecodedField<'_>),
    ) -> Result<SectionSummary, DecodeError> {
        self.decode(block, on_field)?;
        self.finish()
    }

    fn reset_block(&mut self) {
        self.pending.clear();
        self.core.block = BlockState::default();
    }
}

/// The result of decoding one representation.
enum Step<'a> {
    /// The input ended within the representation; nothing was changed.
    Incomplete,
    /// The representation used this many bytes, producing a field to deliver, if any.
    Consumed(usize, Option<DecodedField<'a>>),
}

/// Where a decoded name or value lies, until every change to the decoder's buffers is made.
#[derive(Clone)]
enum Source {
    Static(&'static [u8]),
    Input(Range<usize>),
    Scratch(Range<usize>),
    TableName(usize),
    TableValue(usize),
}

/// A literal's name: a string literal still to decode, or a table entry's name.
enum Name {
    Literal(StringLiteral),
    Source(Source),
}

/// A string literal located in the input.
struct StringLiteral {
    huffman: bool,
    bytes: Range<usize>,
}

impl DecoderCore {
    /// Decodes the complete representations at the start of `input`, returning the bytes they used.
    fn decode(
        &mut self,
        input: &[u8],
        on_field: &mut impl FnMut(DecodedField<'_>),
    ) -> Result<usize, DecodeError> {
        let mut position = 0;
        while position < input.len() {
            match self.step(&input[position..])? {
                Step::Incomplete => break,
                Step::Consumed(length, field) => {
                    position += length;
                    if let Some(field) = field {
                        on_field(field);
                    }
                }
            }
        }
        Ok(position)
    }

    /// Decodes the representation at the start of the non-empty `input`, RFC 7541 §6.
    fn step<'a>(&'a mut self, input: &'a [u8]) -> Result<Step<'a>, DecodeError> {
        let first = input[0];
        if first & 0x80 != 0 {
            return self.indexed(input);
        }
        if first & 0xe0 == 0x20 {
            return self.table_size_update(input);
        }
        let (indexing, prefix_bits) = if first & 0x40 != 0 {
            (Indexing::Incremental, 6)
        } else if first & 0x10 != 0 {
            (Indexing::NeverIndexed, 4)
        } else {
            (Indexing::NotIndexed, 4)
        };
        self.literal(input, indexing, prefix_bits)
    }

    /// An indexed field, RFC 7541 §6.1.
    fn indexed<'a>(&'a mut self, input: &'a [u8]) -> Result<Step<'a>, DecodeError> {
        let Some((index, length)) = integer::decode(input, 7)? else {
            return Ok(Step::Incomplete);
        };
        self.begin_field()?;
        let (name, value, static_index) = match index {
            0 => return Err(DecodeError::InvalidIndex { index }),
            1..=STATIC_TABLE_LEN => {
                let (name, value) = STATIC_TABLE[index - 1];
                (
                    Source::Static(name.as_bytes()),
                    Source::Static(value.as_bytes()),
                    Some(index),
                )
            }
            _ => {
                let dynamic = self.dynamic_index(index)?;
                (
                    Source::TableName(dynamic),
                    Source::TableValue(dynamic),
                    None,
                )
            }
        };
        Ok(Step::Consumed(
            length,
            self.deliver(input, name, value, static_index, Indexing::Incremental),
        ))
    }

    /// A dynamic table size update, RFC 7541 §6.3, which may only begin a block, §4.2.
    fn table_size_update<'a>(&'a mut self, input: &'a [u8]) -> Result<Step<'a>, DecodeError> {
        let Some((size, length)) = integer::decode(input, 5)? else {
            return Ok(Step::Incomplete);
        };
        if self.block.field_seen {
            return Err(DecodeError::MisplacedTableSizeUpdate);
        }
        if size > self.table_size_limit {
            return Err(DecodeError::TableSizeTooLarge {
                size,
                limit: self.table_size_limit,
            });
        }
        self.table.set_capacity(size);
        if self.required_update.is_some_and(|minimum| size <= minimum) {
            self.required_update = None;
        }
        Ok(Step::Consumed(length, None))
    }

    /// A literal field, RFC 7541 §6.2.
    fn literal<'a>(
        &'a mut self,
        input: &'a [u8],
        indexing: Indexing,
        prefix_bits: u32,
    ) -> Result<Step<'a>, DecodeError> {
        let Some((name_index, mut length)) = integer::decode(input, prefix_bits)? else {
            return Ok(Step::Incomplete);
        };
        self.begin_field()?;
        let name = match name_index {
            0 => match self.locate_string(input, &mut length)? {
                Some(literal) => Name::Literal(literal),
                None => return Ok(Step::Incomplete),
            },
            1..=STATIC_TABLE_LEN => {
                Name::Source(Source::Static(STATIC_TABLE[name_index - 1].0.as_bytes()))
            }
            _ => Name::Source(Source::TableName(self.dynamic_index(name_index)?)),
        };
        let Some(value_literal) = self.locate_string(input, &mut length)? else {
            return Ok(Step::Incomplete);
        };

        // The representation is complete: only now is any state changed.
        self.scratch.clear();
        let mut name = match name {
            Name::Literal(literal) => self.decode_string(input, literal)?,
            Name::Source(source) => source,
        };
        let mut value = self.decode_string(input, value_literal)?;
        let static_index = (1..=STATIC_TABLE_LEN)
            .contains(&name_index)
            .then_some(name_index);

        if indexing == Indexing::Incremental {
            let name_len = self.source_len(input, &name);
            let value_len = self.source_len(input, &value);
            let fits = name_len + value_len + ENTRY_OVERHEAD <= self.table.capacity();
            if let Source::TableName(index) = name
                && !fits
            {
                // Inserting will empty the table, removing the entry that holds the name.
                let start = self.scratch.len();
                self.scratch.extend_from_slice(self.table.entry(index).0);
                name = Source::Scratch(start..self.scratch.len());
            }
            let value_bytes = resolve_literal(&self.scratch, input, &value);
            let entry_name = match &name {
                Source::TableName(index) => EntryName::Entry(*index),
                other => EntryName::Bytes(resolve_literal(&self.scratch, input, other)),
            };
            if self.table.insert(entry_name, value_bytes) {
                name = Source::TableName(0);
                value = Source::TableValue(0);
            }
        }
        Ok(Step::Consumed(
            length,
            self.deliver(input, name, value, static_index, indexing),
        ))
    }

    /// Locates the string literal at `*position`, RFC 7541 §5.2, advancing past it, or returns `None` if the input ends first.
    fn locate_string(
        &self,
        input: &[u8],
        position: &mut usize,
    ) -> Result<Option<StringLiteral>, DecodeError> {
        let rest = &input[*position..];
        let Some((length, header)) = integer::decode(rest, 7)? else {
            return Ok(None);
        };
        if length > self.string_length_limit {
            return Err(DecodeError::StringTooLong {
                length,
                limit: self.string_length_limit,
            });
        }
        if rest.len() - header < length {
            return Ok(None);
        }
        let start = *position + header;
        *position = start + length;
        Ok(Some(StringLiteral {
            huffman: rest[0] & 0x80 != 0,
            bytes: start..start + length,
        }))
    }

    fn decode_string(
        &mut self,
        input: &[u8],
        literal: StringLiteral,
    ) -> Result<Source, DecodeError> {
        if literal.huffman {
            let start = self.scratch.len();
            huffman::decode(&input[literal.bytes], &mut self.scratch)?;
            Ok(Source::Scratch(start..self.scratch.len()))
        } else {
            Ok(Source::Input(literal.bytes))
        }
    }

    /// Converts an HPACK index beyond the static table into a dynamic table index.
    fn dynamic_index(&self, index: usize) -> Result<usize, DecodeError> {
        let dynamic = index - STATIC_TABLE_LEN - 1;
        if dynamic < self.table.len() {
            Ok(dynamic)
        } else {
            Err(DecodeError::InvalidIndex { index })
        }
    }

    /// Begins a field representation, which ends the block's dynamic table size updates.
    fn begin_field(&mut self) -> Result<(), DecodeError> {
        if self.required_update.is_some() {
            return Err(DecodeError::MissingTableSizeUpdate);
        }
        self.block.field_seen = true;
        Ok(())
    }

    fn source_len(&self, input: &[u8], source: &Source) -> usize {
        resolve(&self.table, &self.scratch, input, source).len()
    }

    /// Counts a field against the section limits, returning it if they allow its delivery.
    fn deliver<'a>(
        &'a mut self,
        input: &'a [u8],
        name: Source,
        value: Source,
        static_index: Option<usize>,
        indexing: Indexing,
    ) -> Option<DecodedField<'a>> {
        let name_len = self.source_len(input, &name);
        let value_len = self.source_len(input, &value);
        let summary = &mut self.block.summary;
        summary.size = summary
            .size
            .saturating_add(name_len + value_len + ENTRY_OVERHEAD);
        summary.field_count += 1;
        if summary.limit_exceeded.is_none() {
            if summary.field_count > self.field_count_limit {
                summary.limit_exceeded = Some(SectionLimit::FieldCount);
            } else if summary.size > self.section_size_limit {
                summary.limit_exceeded = Some(SectionLimit::Size);
            }
        }
        if summary.limit_exceeded.is_some() {
            return None;
        }
        Some(DecodedField {
            name: resolve(&self.table, &self.scratch, input, &name),
            value: resolve(&self.table, &self.scratch, input, &value),
            static_index,
            indexing,
        })
    }
}

fn resolve<'a>(
    table: &'a DynamicTable,
    scratch: &'a [u8],
    input: &'a [u8],
    source: &Source,
) -> &'a [u8] {
    match source {
        Source::TableName(index) => table.entry(*index).0,
        Source::TableValue(index) => table.entry(*index).1,
        literal => resolve_literal(scratch, input, literal),
    }
}

/// Resolves a source outside the dynamic table, so that the table can change while the bytes are borrowed.
fn resolve_literal<'a>(scratch: &'a [u8], input: &'a [u8], source: &Source) -> &'a [u8] {
    match source {
        Source::Static(bytes) => bytes,
        Source::Input(range) => &input[range.clone()],
        Source::Scratch(range) => &scratch[range.clone()],
        Source::TableName(_) | Source::TableValue(_) => {
            unreachable!("dynamic table sources are resolved through the table")
        }
    }
}

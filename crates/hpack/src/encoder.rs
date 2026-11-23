// Released under the MIT License.
// Copyright, 2013-2015, by Ilya Grigorik.
// Copyright, 2014-2015, by Kaoru Maeda.
// Copyright, 2015, by Tamir Duberstein.
// Copyright, 2016, by George Ulmer.
// Copyright, 2018, by Byron Formwalt.
// Copyright, 2018, by Kenichi Nakamura.
// Copyright, 2018, by Tiago Cardoso.
// Copyright, 2018-2026, by Samuel Williams.
// Copyright, 2019, by Jingyi Chen.
// Copyright, 2020, by Justin Mazzocchi.
// Copyright, 2024, by Maruth Goyal.
// Copyright, 2024, by Nathan Froyd.

//! The encoder, after `protocol-hpack`'s `Compressor` and the encoding half of its `Context`.

use crate::{
    DEFAULT_TABLE_SIZE, Indexing,
    dynamic_table::{DynamicTable, ENTRY_OVERHEAD, EntryName},
    huffman, integer,
    static_table::{self, STATIC_TABLE_LEN},
};

/// When the encoder Huffman codes string literals.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum HuffmanUse {
    /// Send every string as its plain bytes.
    Never,
    /// Huffman code every string.
    Always,
    /// Huffman code a string only when that is strictly shorter than its plain bytes.
    #[default]
    Shorter,
}

/// Which tables the encoder refers to, after `protocol-hpack`'s `index` option and the strategies of the [hpack-test-case](https://github.com/http2jp/hpack-test-case) corpus.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum TableUse {
    /// Send every field as a literal with a literal name, and insert nothing.
    None,
    /// Refer to the static table only, and insert nothing, so the decoder's dynamic table stays empty.
    Static,
    /// Refer to both tables, and insert fields whose [`Indexing`] allows it.
    #[default]
    All,
}

/// The encoder's configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncoderOptions {
    /// The largest dynamic table the encoder keeps, however large a table the decoder allows: the table's capacity is the smaller of this and the decoder's limit. Bounds the encoder's memory.
    pub table_size_limit: usize,
    /// The decoder's table size limit before any [`Encoder::set_table_size_limit`], agreed outside HPACK: [`DEFAULT_TABLE_SIZE`] for HTTP/2, whose initial `SETTINGS_HEADER_TABLE_SIZE` it is.
    pub initial_table_size: usize,
    pub huffman: HuffmanUse,
    pub tables: TableUse,
}

impl Default for EncoderOptions {
    fn default() -> Self {
        Self {
            table_size_limit: DEFAULT_TABLE_SIZE,
            initial_table_size: DEFAULT_TABLE_SIZE,
            huffman: HuffmanUse::default(),
            tables: TableUse::default(),
        }
    }
}

/// Encodes header blocks, keeping the dynamic table the peer's decoder mirrors.
///
/// Its state is independent of any decoder. Blocks must reach the peer in the order they were encoded, each in full, since every block may change the table that later blocks refer to.
///
/// ```
/// use base_http_hpack::{Decoder, Encoder, Indexing};
///
/// let mut encoder = Encoder::default();
/// let mut block = Vec::new();
/// let mut fields = encoder.begin_block(&mut block);
/// fields.encode(b":status", b"200", Indexing::Incremental);
/// fields.encode(b"set-cookie", b"session=secret", Indexing::NeverIndexed);
///
/// let mut decoder = Decoder::default();
/// let mut decoded = Vec::new();
/// decoder.decode_block(&block, |field| decoded.push((field.name().to_vec(), field.indexing()))).unwrap();
/// assert_eq!(decoded[1], (b"set-cookie".to_vec(), Indexing::NeverIndexed));
/// ```
#[derive(Clone, Debug)]
pub struct Encoder {
    table: DynamicTable,
    table_size_limit: usize,
    huffman: HuffmanUse,
    tables: TableUse,
    /// The smallest capacity since the last block, when the capacity has changed since then.
    update: Option<usize>,
}

impl Default for Encoder {
    fn default() -> Self {
        Self::new(EncoderOptions::default())
    }
}

impl Encoder {
    pub fn new(options: EncoderOptions) -> Self {
        let mut encoder = Self {
            table: DynamicTable::new(options.initial_table_size),
            table_size_limit: options.table_size_limit,
            huffman: options.huffman,
            tables: options.tables,
            update: None,
        };
        encoder.set_table_size_limit(options.initial_table_size);
        encoder
    }

    /// The encoder's dynamic table.
    pub fn table(&self) -> &DynamicTable {
        &self.table
    }

    /// Applies the decoder's table size limit, such as the peer's `SETTINGS_HEADER_TABLE_SIZE` in HTTP/2.
    ///
    /// The table's capacity becomes the smaller of `limit` and [`EncoderOptions::table_size_limit`], evicting entries if it shrinks. When the capacity changes, the next block begins with a dynamic table size update: if it shrank and grew again since the last block, one for the smallest capacity and then one for the final capacity, as RFC 7541 §4.2 requires.
    pub fn set_table_size_limit(&mut self, limit: usize) {
        let capacity = limit.min(self.table_size_limit);
        if capacity != self.table.capacity() {
            self.table.set_capacity(capacity);
            self.update = Some(self.update.map_or(capacity, |minimum| minimum.min(capacity)));
        }
    }

    /// Begins a header block, appending to `output`, which can be reused across blocks.
    ///
    /// Any pending dynamic table size update is written at once. Every byte appended to `output` while the returned encoder exists belongs to this block, and must be sent.
    pub fn begin_block<'a>(&'a mut self, output: &'a mut Vec<u8>) -> HeaderBlockEncoder<'a> {
        if let Some(minimum) = self.update.take() {
            let capacity = self.table.capacity();
            if minimum < capacity {
                integer::encode(output, 0x20, 5, minimum);
            }
            integer::encode(output, 0x20, 5, capacity);
        }
        HeaderBlockEncoder { encoder: self, output }
    }

    fn encode(&mut self, output: &mut Vec<u8>, name: &[u8], value: &[u8], indexing: Indexing) {
        let exact_allowed = indexing != Indexing::NeverIndexed;
        let (mut name_index, mut exact) = match self.tables {
            TableUse::None => (None, None),
            TableUse::Static | TableUse::All => match static_table::find(name, value) {
                Some(found) => (Some(found.name), found.exact.filter(|_| exact_allowed)),
                None => (None, None),
            },
        };
        // Prefer the static table, and an exact match over a name, as `protocol-hpack` does.
        if self.tables == TableUse::All
            && exact.is_none()
            && (exact_allowed || name_index.is_none())
            && let Some(found) = self.table.find(name, value)
        {
            let dynamic = |index: usize| STATIC_TABLE_LEN + 1 + index;
            exact = found.exact.filter(|_| exact_allowed).map(dynamic);
            name_index = name_index.or(Some(dynamic(found.name)));
        }
        if let Some(index) = exact {
            integer::encode(output, 0x80, 7, index);
            return;
        }

        // An entry larger than the table would only empty it, on both sides, so it is sent without indexing instead.
        let insert = indexing == Indexing::Incremental
            && self.tables == TableUse::All
            && name.len() + value.len() + ENTRY_OVERHEAD <= self.table.capacity();
        let (pattern, prefix_bits) = match indexing {
            Indexing::NeverIndexed => (0x10, 4),
            _ if insert => (0x40, 6),
            _ => (0x00, 4),
        };
        match name_index {
            Some(index) => integer::encode(output, pattern, prefix_bits, index),
            None => {
                integer::encode(output, pattern, prefix_bits, 0);
                self.write_string(output, name);
            }
        }
        self.write_string(output, value);
        if insert {
            self.table.insert(EntryName::Bytes(name), value);
        }
    }

    fn write_string(&self, output: &mut Vec<u8>, bytes: &[u8]) {
        let huffman_len = match self.huffman {
            HuffmanUse::Never => None,
            HuffmanUse::Always => Some(huffman::encoded_len(bytes)),
            HuffmanUse::Shorter => Some(huffman::encoded_len(bytes)).filter(|&len| len < bytes.len()),
        };
        match huffman_len {
            Some(len) => {
                output.reserve(len + 5);
                integer::encode(output, 0x80, 7, len);
                huffman::encode(bytes, output);
            }
            None => {
                output.reserve(bytes.len() + 5);
                integer::encode(output, 0x00, 7, bytes.len());
                output.extend_from_slice(bytes);
            }
        }
    }
}

/// Encodes the fields of one header block, returned by [`Encoder::begin_block`].
#[derive(Debug)]
pub struct HeaderBlockEncoder<'a> {
    encoder: &'a mut Encoder,
    output: &'a mut Vec<u8>,
}

impl HeaderBlockEncoder<'_> {
    /// Appends a field with raw name and value bytes, which the encoder sends unchanged: HTTP/2's lowercase names and field validity are the caller's concern.
    ///
    /// The field is sent as a reference to a table entry with its name and value when [`TableUse`] and `indexing` allow, and otherwise as a literal, referring to a table entry with its name when there is one, and inserted into the dynamic table when `indexing` is [`Indexing::Incremental`].
    pub fn encode(&mut self, name: &[u8], value: &[u8], indexing: Indexing) {
        self.encoder.encode(self.output, name, value, indexing);
    }
}

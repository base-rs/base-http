// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Encodes arbitrary fields with arbitrary options and table size changes, and decodes them unchanged.

#![no_main]

use base_http_hpack::{Decoder, DecoderOptions, Encoder, EncoderOptions, HuffmanUse, Indexing, TableUse};
use libfuzzer_sys::fuzz_target;

/// Reads a length-prefixed slice.
fn take<'a>(input: &mut &'a [u8]) -> Option<&'a [u8]> {
    let (&length, rest) = input.split_first()?;
    let length = usize::from(length).min(rest.len());
    let (bytes, rest) = rest.split_at(length);
    *input = rest;
    Some(bytes)
}

fuzz_target!(|input: &[u8]| {
    let Some((&configuration, mut input)) = input.split_first() else {
        return;
    };
    let mut encoder = Encoder::new(EncoderOptions {
        table_size_limit: [0, 64, 256, 4096][usize::from(configuration & 3)],
        huffman: [HuffmanUse::Never, HuffmanUse::Always, HuffmanUse::Shorter, HuffmanUse::Shorter][usize::from(configuration >> 2 & 3)],
        tables: [TableUse::None, TableUse::Static, TableUse::All, TableUse::All][usize::from(configuration >> 4 & 3)],
        ..EncoderOptions::default()
    });
    let mut decoder = Decoder::new(DecoderOptions {
        section_size_limit: usize::MAX,
        field_count_limit: usize::MAX,
        ..DecoderOptions::default()
    });
    let mut block = Vec::new();
    let mut fields = Vec::new();
    // Each field is a flags byte, a name and a value; flags with the top bit set end the block and may change the table size limit.
    loop {
        let flags = input.split_first().map(|(&flags, rest)| {
            input = rest;
            flags
        });
        let field = match flags {
            Some(flags) if flags & 0x80 == 0 => take(&mut input).zip(take(&mut input)).map(|field| (field, flags)),
            _ => None,
        };
        if let Some(((name, value), flags)) = field {
            fields.push((name, value, [Indexing::Incremental, Indexing::NotIndexed, Indexing::NeverIndexed, Indexing::Incremental][usize::from(flags & 3)]));
            continue;
        }
        block.clear();
        let mut fields_encoder = encoder.begin_block(&mut block);
        for &(name, value, indexing) in &fields {
            fields_encoder.encode(name, value, indexing);
        }
        let mut decoded = Vec::new();
        decoder.decode_block(&block, |field| decoded.push((field.name().to_vec(), field.value().to_vec(), field.indexing() == Indexing::NeverIndexed))).unwrap();
        assert!(decoded.iter().map(|(name, value, sensitive)| (&name[..], &value[..], *sensitive)).eq(fields.iter().map(|&(name, value, indexing)| (name, value, indexing == Indexing::NeverIndexed))));
        assert!(decoder.table().iter().eq(encoder.table().iter()));
        fields.clear();
        match flags {
            Some(flags) if flags & 0xc0 == 0xc0 => {
                let limit = usize::from(flags & 0x3f) * 128;
                encoder.set_table_size_limit(limit);
                decoder.set_table_size_limit(limit);
            }
            Some(_) => {}
            None => break,
        }
    }
});

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Decodes arbitrary blocks whole and in fragments, which must agree on every field, error and table.

#![no_main]

use base_http_hpack::{Decoder, DecoderOptions};
use libfuzzer_sys::fuzz_target;

fn options() -> DecoderOptions {
    DecoderOptions {
        table_size_limit: 1024,
        initial_table_size: 1024,
        section_size_limit: 2048,
        field_count_limit: 16,
        string_length_limit: 512,
    }
}

fuzz_target!(|input: &[u8]| {
    // The first byte chooses where fragments split; the rest is a sequence of blocks separated by 0xff 0xfe.
    let Some((&split, input)) = input.split_first() else {
        return;
    };
    let mut whole = Decoder::new(options());
    let mut fragmented = Decoder::new(options());
    for block in input.split(|&byte| byte == 0xfe) {
        let mut whole_fields = Vec::new();
        let whole_result = whole.decode_block(block, |field| {
            whole_fields.push((
                field.name().to_vec(),
                field.value().to_vec(),
                field.static_index(),
                field.indexing(),
            ))
        });
        let mut fragmented_fields = Vec::new();
        let step = usize::from(split).max(1);
        let mut result = Ok(());
        for fragment in block.chunks(step) {
            result = fragmented.decode(fragment, |field| {
                fragmented_fields.push((
                    field.name().to_vec(),
                    field.value().to_vec(),
                    field.static_index(),
                    field.indexing(),
                ))
            });
            if result.is_err() {
                break;
            }
        }
        let fragmented_result = result.and_then(|()| fragmented.finish());
        assert_eq!(whole_fields, fragmented_fields);
        assert_eq!(whole_result, fragmented_result);
        assert!(whole.table().size() <= whole.table().capacity());
        assert!(whole.table().capacity() <= 1024);
        assert!(whole.table().iter().eq(fragmented.table().iter()));
    }
});

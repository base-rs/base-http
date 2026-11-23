// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Once their tables and buffers have grown, encoding and decoding allocate nothing. This target has one test, so no other test allocates while it counts.

use base_http_hpack::{Decoder, Encoder, EncoderOptions, HuffmanUse, Indexing};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::alloc::System;

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// Requests whose changing values keep inserting into, and evicting from, both dynamic tables.
fn request(round: usize, values: &mut [Vec<u8>; 3]) -> [(&'static [u8], usize, Indexing); 9] {
    use std::io::Write;
    for value in values.iter_mut() {
        value.clear();
    }
    write!(values[0], "/items/{}?page={}", round % 50, round % 7).unwrap();
    write!(values[1], "request-{round:08}").unwrap();
    write!(
        values[2],
        "session={:016x}",
        round.wrapping_mul(0x9e37_79b9)
    )
    .unwrap();
    [
        (b":method", usize::MAX, Indexing::Incremental),
        (b":scheme", usize::MAX, Indexing::Incremental),
        (b":path", 0, Indexing::Incremental),
        (b":authority", usize::MAX, Indexing::Incremental),
        (b"user-agent", usize::MAX, Indexing::Incremental),
        (b"accept", usize::MAX, Indexing::Incremental),
        (b"x-request-id", 1, Indexing::NotIndexed),
        (b"cookie", 2, Indexing::NeverIndexed),
        (b"x-trace", 1, Indexing::Incremental),
    ]
}

fn fixed_value(name: &[u8]) -> &'static [u8] {
    match name {
        b":method" => b"GET",
        b":scheme" => b"https",
        b":authority" => b"www.example.com",
        b"user-agent" => b"Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
        _ => b"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
}

#[test]
fn steady_state_encoding_and_decoding_do_not_allocate() {
    let mut encoder = Encoder::new(EncoderOptions {
        table_size_limit: 512,
        huffman: HuffmanUse::Shorter,
        ..EncoderOptions::default()
    });
    let mut decoder = Decoder::default();
    let mut block = Vec::new();
    let mut values = [Vec::new(), Vec::new(), Vec::new()];
    let mut checksum = 0usize;
    let mut run =
        |round: usize, block: &mut Vec<u8>, values: &mut [Vec<u8>; 3], checksum: &mut usize| {
            if round.is_multiple_of(100) {
                // A table size change, signalled at the start of the next block.
                encoder.set_table_size_limit(if round.is_multiple_of(200) { 256 } else { 4096 });
            }
            block.clear();
            let fields = request(round, values);
            let mut fields_encoder = encoder.begin_block(block);
            for (name, value, indexing) in fields {
                let value = values
                    .get(value)
                    .map_or_else(|| fixed_value(name), |value| &value[..]);
                fields_encoder.encode(name, value, indexing);
            }
            // Decode in two fragments, so that a representation is split between them.
            let (first, second) = block.split_at(block.len() / 2);
            let mut count = |field: base_http_hpack::DecodedField<'_>| {
                *checksum += field.name().len() + field.value().len()
            };
            decoder.decode(first, &mut count).unwrap();
            decoder.decode(second, &mut count).unwrap();
            assert_eq!(decoder.finish().unwrap().field_count(), 9);
        };
    for round in 0..1_000 {
        run(round, &mut block, &mut values, &mut checksum);
    }
    let region = Region::new(ALLOCATOR);
    for round in 1_000..3_000 {
        run(round, &mut block, &mut values, &mut checksum);
    }
    let change = region.change();
    assert_eq!((change.allocations, change.reallocations), (0, 0));
    assert!(checksum > 0);
    // The counter does count.
    let region = Region::new(ALLOCATOR);
    let mut grown = Vec::new();
    run(3_000, &mut grown, &mut values, &mut checksum);
    assert!(region.change().allocations + region.change().reallocations > 0);
}

# HPACK

Proposed 2026-11-24 with the first implementation of `base-http-hpack`; the requirements below describe that implementation, and the [open questions](#open-questions) await the user's decisions. The [HTTP/2 requirements](http2.md) set the codec's package boundary, its bounds and its static table indices; this note records the interface that meets them, the behaviour of `protocol-hpack` it does not reproduce, and the evidence for it. The [roadmap](roadmap.md#m7-add-hpack-http2-and-multiplex-admission) schedules it as the first step of M7.

## Reference implementation and its defects

This section is background for the requirements that follow and states no requirement of its own.

`base-http-hpack` ports [`protocol-hpack` `e572ccf`](https://github.com/socketry/protocol-hpack/tree/e572ccf0d291821915bc9d2d57425c7435b7483b) (1.5.1), the revision the [HTTP/2 review](http2.md#reference-implementation-and-its-defects) cites. Its static table, Huffman code, four-bit Huffman decoding machine, dynamic table eviction and encoding strategy carry over: the encoder prefers the static table to the dynamic table, and an exact match to a name, and inserts each literal it sends when the table is in use. Its tests and fixtures carry over as [conformance evidence](#conformance-evidence). Each behaviour below was confirmed by reading the source at that revision, and where the last column gives an input, by running it with Ruby.

The following must not carry over, in addition to the two integer defects the HTTP/2 review records, unbounded integers and truncated integers read as smaller values. Each is addressed by a requirement below.

| Reviewed behavior | Source | Ruby, 2026-11-24 |
| --- | --- | --- |
| A dynamic table size update is accepted anywhere in a block, such as after a field, though RFC 7541 §4.2 permits one only at its beginning. | [`decompressor.rb#L192-L207`](https://github.com/socketry/protocol-hpack/blob/e572ccf0d291821915bc9d2d57425c7435b7483b/lib/protocol/hpack/decompressor.rb#L192-L207) | `82 20 82` decodes as two fields. |
| A block of only dynamic table size updates, which RFC 7541 permits, is rejected as a "trailing table size update". | [`decompressor.rb#L201-L203`](https://github.com/socketry/protocol-hpack/blob/e572ccf0d291821915bc9d2d57425c7435b7483b/lib/protocol/hpack/decompressor.rb#L201-L203) | `3f e1 01` raises `CompressionError`. |
| The decoder's table size limit is `nil`, unlimited, unless the caller passes one, so a peer can set any table size; `protocol-http2` passes its local setting. | [`decompressor.rb#L20-L26`](https://github.com/socketry/protocol-hpack/blob/e572ccf0d291821915bc9d2d57425c7435b7483b/lib/protocol/hpack/decompressor.rb#L20-L26) | An update to 2³¹ is accepted. |
| After the decoder's limit is lowered, a block without the size update RFC 7541 §4.2 requires is accepted: the limit is checked only against updates that are sent. | [`decompressor.rb#L168-L174`](https://github.com/socketry/protocol-hpack/blob/e572ccf0d291821915bc9d2d57425c7435b7483b/lib/protocol/hpack/decompressor.rb#L168-L174) | Source only. |
| String literals have no length bound beyond the block, and no field section size or field count is counted. | [`decompressor.rb#L82-L96`](https://github.com/socketry/protocol-hpack/blob/e572ccf0d291821915bc9d2d57425c7435b7483b/lib/protocol/hpack/decompressor.rb#L82-L96) | Source only. |
| The decoder returns name and value pairs only, so a never-indexed field cannot be forwarded as one, as RFC 7541 §7.1.3 requires of an intermediary; the encoder never sends one. | [`context.rb#L169-L215`](https://github.com/socketry/protocol-hpack/blob/e572ccf0d291821915bc9d2d57425c7435b7483b/lib/protocol/hpack/context.rb#L169-L215) | `10 01 61 01 62` decodes as `["a", "b"]`. |
| When the table size changes more than once between blocks, the encoder signals only the final size, not the smallest as well, as RFC 7541 §4.2 requires. | [`compressor.rb#L173-L189`](https://github.com/socketry/protocol-hpack/blob/e572ccf0d291821915bc9d2d57425c7435b7483b/lib/protocol/hpack/compressor.rb#L173-L189) | Source only. |
| The encoder's table follows the peer's `SETTINGS_HEADER_TABLE_SIZE` with no local cap, so a peer sets the encoder's memory. | [`compressor.rb#L173-L189`](https://github.com/socketry/protocol-hpack/blob/e572ccf0d291821915bc9d2d57425c7435b7483b/lib/protocol/hpack/compressor.rb#L173-L189), as `protocol-http2`'s [`connection.rb#L163`](https://github.com/socketry/protocol-http2/blob/7d5a7db509e1b91783a0f57922cf16b6b8911886/lib/protocol/http2/connection.rb#L163) calls it | Source only. |
| The encoder sends a field larger than its table with incremental indexing, which only empties both tables. | [`context.rb#L224-L298`](https://github.com/socketry/protocol-hpack/blob/e572ccf0d291821915bc9d2d57425c7435b7483b/lib/protocol/hpack/context.rb#L224-L298) | Source only. |
| The `MODES` table maps `linear_huffman`, `static_huffman` and `shorter_huffman` to `NAIVE_HUFFMAN`, so the external compressor tests exercise one Huffman strategy, not four. | [`compressor.rb#L32-L41`](https://github.com/socketry/protocol-hpack/blob/e572ccf0d291821915bc9d2d57425c7435b7483b/lib/protocol/hpack/compressor.rb#L32-L41) | All four keys map to the same hash. |

The truncation checks are otherwise sound: a string truncated after its first byte raises `CompressionError` rather than another exception, and the Huffman decoder rejects padding longer than seven bits.

## Codec interface

### Requirement

The public interface is Base-owned, as the [HTTP/2 packages](http2.md#packages-and-dependencies) require: `Encoder`, `Decoder`, their options, `DynamicTable`, `DecodedField`, `Indexing`, `SectionSummary`, `DecodeError` and `STATIC_TABLE`. Names and values are raw bytes, neither validated nor case-converted.

- An `Encoder` and a `Decoder` each own their dynamic table and share no state, so the two directions of a connection are independent.
- `Encoder::begin_block` writes any pending table size update into a caller's `Vec<u8>` and returns a `HeaderBlockEncoder`, which appends one field at a time with `encode(name, value, indexing)`. Fields from several sources, such as pseudo-fields and `Fields`, are encoded without collecting them first, and the borrow of the encoder for the block keeps a table size update from landing inside a block.
- `Decoder::decode` takes a fragment of a block and calls `FnMut(DecodedField<'_>)` for each complete field; `Decoder::finish` ends the block and returns its `SectionSummary`; `decode_block` is both. The callback cannot stop decoding, so a caller rejecting a message cannot leave the dynamic table unsynchronized. Only a small loop is generic over the callback; the representation decoder is not.
- A representation split between fragments is kept by the decoder, at most one field's encoded bytes, and completed with the next fragment; a field is delivered only once complete, and nothing changes state until then.
- A `DecodedField` borrows its name and value from the input, the dynamic table or the decoder's buffer for Huffman decoding, until the callback returns. It reports the static table index, 1 to 61, its name came from, as the [static table indices](http2.md#static-table-indices) require, and the `Indexing` it was sent with, so a never-indexed field can be forwarded as one.
- `Indexing` has three values for the three literal representations: `Incremental`, `NotIndexed` and `NeverIndexed`. A `NeverIndexed` field is always sent as a never-indexed literal, even when a table holds it exactly; its name may come from a table.

### Rationale

The [component evaluation](components.md#hpack-own-a-small-codec-with-a-familiar-reference) asks for reused output storage, input progress, explicit limits and field delivery without a temporary message or map, and for sensitivity to survive forwarding. A block encoder rather than an iterator of fields avoids monomorphizing the encoder for each caller's iterator type. A callback that cannot fail matches RFC 9113 §4.3, which requires every block to be decoded, and HTTP/2's handling of malformed messages as stream errors after the block is processed. Keeping split representations inside the decoder lets HTTP/2 pass each HEADERS and CONTINUATION payload as it arrives without concatenating the block, at the cost of a copy only for a field split between frames.

### Scope

The public interface of `base-http-hpack`.

### Validation

Inspect the public interface for upstream types and for allocation-owning field types. `tests/decoder.rs` decodes blocks split once at every position and one byte at a time, and `tests/round_trip.rs` decodes random blocks in random fragments and arbitrary input whole and in fragments, requiring identical fields, outcomes and tables. Tests check the static index and indexing reported for each representation, and that never-indexed fields stay never-indexed through the encoder.

## Bounds and errors

### Requirement

Every limit is finite by default. `DecoderOptions` has `table_size_limit` (4,096), `initial_table_size` (4,096), `section_size_limit` (32 KiB, in RFC 9113 §6.5.2's measure), `field_count_limit` (100) and `string_length_limit` (64 KiB of encoded bytes, matching HTTP/2's default header block limit).

- An integer uses at most five continuation bytes and has a value of at most 2³²−1. Index, string length and table size are then checked against the table and the options. A string's length is checked as soon as it is read, before its bytes arrive.
- A section over `section_size_limit` or `field_count_limit` is decoded to its end, updating the dynamic table, without delivering the field that exceeded the limit or any later one, and `SectionSummary::limit_exceeded` names the first limit exceeded. This is not an error.
- Each malformed input fails with a `DecodeError` naming its cause: `Truncated`, `IntegerOverflow`, `InvalidIndex`, `StringTooLong`, `HuffmanEndOfString`, `HuffmanPadding`, `TableSizeTooLarge`, `MisplacedTableSizeUpdate` or `MissingTableSizeUpdate`. An error ends the block; the decoder can be used afterwards, but its table may no longer match the encoder's, so HTTP/2 treats every one as a `COMPRESSION_ERROR`.

### Rationale

These are the [header block requirements](http2.md#header-blocks-and-field-validation) of HTTP/2, with its [default limits](http2.md#default-limits). Five continuation bytes bound an integer's work and admit every 32-bit value, the largest any `SETTINGS` value can be, with room for a non-minimal encoding. Bounding a string's encoded length bounds both the bytes the decoder keeps for a split representation and the Huffman output, at most eight fifths of the input. With the defaults a string cannot exceed its limit without its block exceeding HTTP/2's header block limit, so the section limits, answered with 431, bound ordinary oversized fields.

### Scope

`Decoder`, `DecoderOptions`, `SectionSummary` and `DecodeError`.

### Validation

`tests/decoder.rs` produces each `DecodeError` variant, including every truncation of a block, integers over the bound, a string rejected from its length alone, and invalid Huffman strings, and checks that a section over each limit is decoded to its end and its dynamic table entries are usable by the next block. The [measurements](#measurements) record decoding time for maximum-size blocks.

## Dynamic table size signalling

### Requirement

- The encoder's table capacity is the smaller of the decoder's limit, set with `Encoder::set_table_size_limit`, and `EncoderOptions::table_size_limit`. Both start from `initial_table_size`, HTTP/2's 4,096. When the capacity changes, the next block begins with one update for the final capacity, preceded by one for the smallest capacity since the last block if that is smaller, as RFC 7541 §4.2 requires.
- The decoder accepts table size updates only before a block's first field representation, and none above its limit. `Decoder::set_table_size_limit` below the table's capacity requires the next block, even an empty one, to begin with updates that reach the smallest limit set since the last update; otherwise it fails with `MissingTableSizeUpdate`. A block consisting only of updates is accepted.
- The encoder does not insert a field larger than its table's capacity, sending it without indexing instead.

### Rationale

RFC 7541 §4.2 and RFC 9113 §4.3.1. HTTP/2 applies a local limit only once the peer acknowledges it, so HTTP/2 constructs its decoder with the initial size and calls `set_table_size_limit` on acknowledgment, as `DecoderOptions` documents; requiring the update then is what lets the decoder rely on the encoder's table fitting the limit. Capping the encoder bounds memory per connection, as the HTTP/2 [header block requirements](http2.md#header-blocks-and-field-validation) require. Sending an oversized field without indexing leaves both tables as they were, where `protocol-hpack` empties them.

### Scope

`Encoder`, `Decoder` and their table size options.

### Validation

`tests/encoder.rs` checks the updates for an initial cap, a capped increase, one change, no change, and a decrease followed by an increase, and that oversized fields are not indexed. `tests/decoder.rs` checks misplaced, oversized, missing and sufficient updates, and blocks of only updates. `tests/round_trip.rs` changes both sides' limits between random blocks and checks that the tables and capacities agree after each.

## Storage and allocation

### Requirement

Once the dynamic tables and buffers have grown to their working size, encoding a block into a reused `Vec<u8>` and decoding it, in one fragment or several, allocate nothing. The dynamic table keeps its entries' bytes in one buffer, newest last, reclaiming evicted bytes in bulk: its buffer never exceeds three times its capacity.

### Rationale

The roadmap requires reusable output storage and no allocation per field in the steady state. A buffer of appended entries copies each entry once on insertion and, amortized, at most once more on reclamation, which happens only when the evicted bytes are at least as many as the live ones, and it keeps an entry whose name a new entry refers to until the name has been copied, as RFC 7541 §4.4 requires.

### Scope

`DynamicTable`, `Encoder` and `Decoder`.

### Validation

`tests/allocation.rs` counts allocations with `stats_alloc` over 2,000 blocks of a changing request after warming up, including table size changes, evictions, Huffman strings and blocks split in two, and requires none. The dynamic table's unit tests check its buffer bound and capacity over 10,000 insertions.

## Conformance evidence

### Requirement

The tests import, with their provenance and licenses, `protocol-hpack`'s RFC 7541 Appendix C fixtures and its sequence fixture, and stories 00, 01, 03 and 15 of every directory of [hpack-test-case](https://github.com/http2jp/hpack-test-case) at `8a1406e`, the revision `protocol-hpack` uses. Every imported encoded story decodes; every raw story round-trips with each of nine encoder configurations and two table sizes, and is decoded by `fluke-hpack` as an independent oracle, which also encodes random fields for this decoder. An ignored test runs the whole 66 MB corpus from a checkout. `protocol-hpack`'s unit tests are ported where the behaviour they test is kept, with the defects above tested as their corrections.

### Rationale

The HTTP/2 [validation](http2.md#header-blocks-and-field-validation) asks for both corpora; the subset keeps the repository small while covering every encoder of the corpus. `fluke-hpack` is a development dependency only, as the [HTTP/2 packages](http2.md#packages-and-dependencies) permit.

### Scope

`crates/hpack/tests` and `crates/hpack/fuzz`.

### Validation

`tests/fixtures/readme.md` records the sources, revisions and conversions. On 2026-11-24 the whole corpus passed: 446 encoded stories decoded, and 32 raw stories round-tripped with nine configurations. The `decode` and `round_trip` fuzz targets, which compare whole and fragmented decoding and round trips with arbitrary options, each ran for two minutes, 1.4 and 1.7 million inputs, without a failure.

## Measurements

Screening figures from 2026-11-24 on an AMD Ryzen 9 9950X3D workstation, not a dedicated benchmark host, in a release build of the test harness, 11 interleaved rounds of 200,000 blocks of a ten-field browser request with default options, median and range:

| Path | Encode, ns per field | Decode, ns per field |
| --- | --- | --- |
| Both tables, shorter Huffman (defaults) | 54.9 (54.3–57.2) | 19.3 (19.2–19.7) |
| Both tables, no Huffman | 52.4 (51.5–53.6) | 11.5 (11.4–12.2) |
| Static table only, shorter Huffman | 26.6 (26.2–27.8) | 62.3 (62.1–62.7) |
| Static table only, no Huffman | 10.1 (10.1–10.5) | 13.1 (13.0–13.4) |

With both tables, the dynamic table, its linear search and its insertions, accounts for about 42 ns of the encoder's 52 ns per field without Huffman coding; [an open question](#encoder-dynamic-table-lookup) addresses the search. These figures were taken with a temporary harness that is not part of the repository; an isolated benchmark package would make them reproducible. Decoding a maximum-size block of 64 KiB took 334 µs (329–360 µs) for 65,536 one-byte indexed fields, most of them past the field count limit, 186 µs for one 62 KiB Huffman string, and 94 µs for 64 KiB of distinct incremental literals that churn the table.

## Open questions

### Lowercasing outgoing names

The [HTTP/2 header block requirements](http2.md#header-blocks-and-field-validation) say Base fields keep their case, "so the encoder lowercases names on send", without saying whether `base-http-hpack` or `base-http2` does it. The codec currently sends names unchanged. Recommendation: `base-http2` lowercases, in its validation of outgoing names, into a reused buffer only when a name has an uppercase byte, so the common lowercase name is passed through without a copy and the codec stays a byte codec usable by any HPACK protocol. Lowercasing inside the encoder would also need case-insensitive table lookups.

### Sensitivity of outgoing fields

`base-http` `Field` has no sensitivity flag, so `base-http2` must choose an `Indexing` for each field. Recommendation: `base-http2` sends `authorization`, `proxy-authorization` and `cookie` values shorter than 20 bytes as `NeverIndexed`, following RFC 7541 §7.1.3, and the rest as `Incremental`; whether `Field` carries a decoded never-indexed flag, so a proxy preserves it across hops, is decided with the proxy forwarding work of M11, alongside the recorded idea of a `Field` keeping a well-known name's identity.

### Encoder indexing policy

Like `protocol-hpack`, the encoder inserts every incremental field that fits, so unique values such as request identifiers or `:path` with a query evict useful entries. nghttp2, for example, does not index some fields and entries over three quarters of the table. Recommendation: keep the ported policy, which `Indexing::NotIndexed` lets `base-http2` override per field, until M7's HTTP/2 benchmarks measure compression and CPU with a browser-like workload; then decide on a default policy in `base-http2`, not in the codec.

### Encoder dynamic table lookup

The encoder searches the dynamic table linearly, comparing name lengths first, as `protocol-hpack` does; with a 4 KiB table the dynamic table accounts for about 42 ns of a field's 52 ns without Huffman coding, searching it and inserting into it; how much is the search is not yet measured. A hash index from names and fields to their newest entries would make the search constant-time, at the cost of an index kept in step with insertion and eviction. Recommendation: measure an indexed lookup against this baseline in an isolated encoder benchmark and in the M7 HTTP/2 response benchmark on the dedicated host, and adopt it only for a repeatable reduction in instructions per field, as the performance guidance requires. Server responses usually have few fields, so the request side, the client's encoder, is where it matters most.

### Primitives for QPACK

The [HTTP/2 packages](http2.md#packages-and-dependencies) allow the integer and Huffman primitives to be public for `base-http-qpack`. They are crate-private for now. Recommendation: publish a narrow, documented module when `base-http-qpack` starts, shaped by its string literals, whose Huffman flag and length prefix sit at positions HPACK does not use, rather than guessing that interface now.

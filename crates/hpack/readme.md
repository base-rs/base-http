# Base HTTP HPACK

HPACK field compression ([RFC 7541](https://www.rfc-editor.org/rfc/rfc7541)) for HTTP/2, a native Rust port of Socketry's [`protocol-hpack`](https://github.com/socketry/protocol-hpack) behind a Base-owned interface. It depends on no scheduler, HTTP or other HPACK crate: names and values are raw bytes, and HTTP/2 validates them.

## Usage

An `Encoder` and a `Decoder` each own their dynamic table; an HTTP/2 connection has one of each, for the two directions. The encoder appends each header block to a caller's `Vec<u8>`, which can be reused, and the decoder delivers each field to a callback, borrowing its bytes from the input or the decoder's own buffers:

```rust
use base_http_hpack::{Decoder, Encoder, Indexing};

let mut encoder = Encoder::default();
let mut block = Vec::new();
let mut fields = encoder.begin_block(&mut block);
fields.encode(b":method", b"GET", Indexing::Incremental);
fields.encode(b"authorization", b"Bearer secret", Indexing::NeverIndexed);

let mut decoder = Decoder::default();
let mut names = Vec::new();
let section = decoder
    .decode_block(&block, |field| names.push((field.name().to_vec(), field.static_index())))
    .unwrap();
assert_eq!(names, [(b":method".to_vec(), Some(2)), (b"authorization".to_vec(), Some(23))]);
assert_eq!(section.limit_exceeded(), None);
```

Fields keep the `Indexing` they were sent with, so an intermediary can forward a sensitive, never-indexed field as one. A decoded name that came from the static table reports its index, so a protocol can dispatch on it without comparing names.

A block split across frames is decoded with `Decoder::decode` for each fragment and `Decoder::finish` at its end; a representation split between fragments is kept until the next one. Every limit in `DecoderOptions` is finite: integers, string lengths, table size updates and indices are checked as they are read. A section over its size or field count limit is still decoded to its end, keeping the dynamic table synchronized, but its later fields are not delivered, and `finish` reports the limit it exceeded; HTTP/2 answers such a request with 431 without closing the connection. Malformed input fails with a typed `DecodeError`, which HTTP/2 treats as a `COMPRESSION_ERROR`.

The encoder follows changes to the peer's table size limit with `Encoder::set_table_size_limit`, capped by `EncoderOptions::table_size_limit`, and signals them at the start of the next block. Once the dynamic tables and buffers have grown to their working size, encoding and decoding allocate nothing.

## Behaviour and conformance

The [HTTP/2 requirements](../../design/http2.md) set this crate's boundary and bounds, and the [HPACK design note](../../design/hpack.md) records its interface, the `protocol-hpack` behaviour it does not reproduce, measurements and open questions. The tests run the RFC 7541 examples, `protocol-hpack`'s own tests and a selection of the [hpack-test-case](https://github.com/http2jp/hpack-test-case) corpus, whose provenance and licenses `tests/fixtures/readme.md` records, and compare against the independent `fluke-hpack` codec.

## See Also

- [`base-http`](https://docs.rs/base-http) defines the HTTP semantics that HTTP/2 maps these fields to.

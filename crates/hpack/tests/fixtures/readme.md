# HPACK Fixtures

Conformance fixtures for `base-http-hpack`, imported unchanged except where noted. Both sources are MIT licensed; their license files are kept beside the fixtures.

## `protocol-hpack`

From [`protocol-hpack`](https://github.com/socketry/protocol-hpack) at `e572ccf0d291821915bc9d2d57425c7435b7483b` (1.5.1), the revision the [HTTP/2 requirements](../../../../design/http2.md#reference-implementation-and-its-defects) review; see `protocol-hpack/license.md`.

- `protocol-hpack/rfc7541/*.json` are `test/protocol/hpack/rfc7541/*.yaml`, the RFC 7541 Appendix C examples with the expected dynamic table after each block, converted from YAML to JSON with symbol keys and values written as strings. Their content is otherwise unchanged; `tests/rfc7541.rs` runs them as `test/protocol/hpack/rfc7541.rb` does.
- `protocol-hpack/sequence1.json` is `test/protocol/hpack/sequence1.json`, a sequence of response field lists that `tests/round_trip.rs` encodes and decodes as `test/protocol/hpack/compressor.rb` does.

## hpack-test-case

From [hpack-test-case](https://github.com/http2jp/hpack-test-case) at `8a1406e7d14bfcb6c046021f13cc15cfb162726d`, the revision `protocol-hpack` includes as a submodule; see `hpack-test-case/LICENSE`, copyright 2013 HTTP/2 Japan Community.

The whole corpus is 66 MB, so stories 00, 01, 03 and 15 of every directory are imported: `raw-data` has the original field lists, and each other directory has the same stories as one implementation encoded them, with Huffman coding, static and dynamic tables and table size changes in various combinations. `tests/hpack_test_case.rs` decodes each story and encodes every `raw-data` story with each encoder configuration. Its ignored `full_corpus` test runs the whole corpus from a checkout named by `HPACK_TEST_CASE`:

```sh
git clone https://github.com/http2jp/hpack-test-case target/hpack-test-case
git -C target/hpack-test-case checkout 8a1406e7d14bfcb6c046021f13cc15cfb162726d
HPACK_TEST_CASE=$PWD/target/hpack-test-case cargo test -p base-http-hpack --test hpack_test_case -- --ignored
```

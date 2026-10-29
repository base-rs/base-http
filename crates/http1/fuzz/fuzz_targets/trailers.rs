// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| base_http1::fuzz_trailers(bytes));

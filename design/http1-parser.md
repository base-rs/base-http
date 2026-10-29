# HTTP/1 head parser

Updated 2026-10-29. `base-http1` currently checks each request head in several separate passes: a terminator search, a strict line-ending scan, httparse, then target, Host and field-name checks in `wire::request`. On the hello request, the Host checks alone account for about 320 of the 343 instructions per request separating Base from Hyper at 64 connections. This note defines a single-pass, hand-written parser that replaces httparse for request heads, response heads and trailers, and states the exact grammar it accepts.

## Goals

The parser validates each head byte once and records where each element is. Field names, values and the request target are not validated again later; Host is checked once more as an authority. Safe Rust needs one further UTF-8 check to obtain text, described below. The crate keeps `#![forbid(unsafe_code)]`. Field names and values are scanned 16 bytes at a time by a branch-free fold that sets one mask bit per byte. The compiler turns it into a vector comparison and a mask extraction, `pmovmskb` with SSE2, and the first set bit gives the position of the CR ending a value or of the first name byte other than a letter, digit or `-`. Fewer than 16 remaining bytes are scanned a word at a time with `u64::from_le_bytes`, and then byte by byte. None of this needs `unsafe` or `std::arch`. A 32-byte block that only reported whether it held a control cost more than it saved, because most values end within 32 bytes.

The first change preserves behaviour. The new request parser accepts exactly the heads the previous pipeline accepts and produces the same metadata and framing facts. Rejections can differ in which error is reported. A head with more than one defect may report a different first error, possibly `Invalid` rather than `Limit`, as earlier scan fusions already allowed. An invalid byte in the first read of a head is now reported immediately, so a connection that sends one and then closes gets `Invalid` instead of `Truncated`. Any grammar change is a separate, reviewed change to this document.

Ragel was considered. Its Rust support exists only in unofficial forks, and generated state machines are difficult to keep both fast and free of `unsafe`. The HTTP/1 head grammar is small enough to write as a few tight loops.

## Accepted grammar

Rules cite [RFC 9112](https://www.rfc-editor.org/rfc/rfc9112.html) and [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html). Lines end with CRLF only. A CR not followed by LF, or an LF not preceded by CR, is invalid anywhere in the head (RFC 9112 §2.2 permits recipients to accept a bare LF; we do not).

```text
request-head  = request-line *( field-line CRLF ) CRLF
response-head = status-line  *( field-line CRLF ) CRLF
trailer-section =            *( field-line CRLF ) CRLF

request-line = method SP request-target SP HTTP-version CRLF
status-line  = HTTP-version SP 3DIGIT [ SP reason-phrase ] CRLF
HTTP-version = "HTTP/1.0" / "HTTP/1.1"
method       = token
field-line   = field-name ":" OWS field-value OWS
field-name   = token
field-value  = *( HTAB / SP / VCHAR / obs-text )
reason-phrase = *( HTAB / SP / VCHAR / obs-text )
```

The details below match current behaviour:

- **Before the start line.** A head starting with CR, LF, SP or HTAB is invalid. RFC 9112 §2.2 recommends ignoring at least one empty line before a request line; we currently reject it, and this note keeps that.
- **Separators.** Exactly one SP separates start-line elements. Repeated spaces or HTAB are invalid.
- **Method.** One or more token characters, compared case-sensitively. GET, HEAD, POST, PUT, DELETE, CONNECT, OPTIONS, TRACE, PATCH and QUERY map to constants; other tokens keep their spelling.
- **Request target.** One or more bytes from 0x21 to 0x7E, excluding `#` and `\`. Non-ASCII bytes are invalid. The target must then take one of the four forms in RFC 9112 §3.2: origin form beginning with `/`, `*` with OPTIONS, authority form with CONNECT (port required), or absolute form with an `http` or `https` scheme in any case.
- **Version.** Only `HTTP/1.0` and `HTTP/1.1`, in exactly that spelling. Requests with any other version are invalid.
- **Status line.** Three digits, then CRLF or SP and a reason phrase. The reason phrase is checked and discarded. The status code is then checked by `Status::new`.
- **Field names.** One or more token characters immediately followed by `:`. Whitespace between the name and the colon is invalid (RFC 9112 §5.1).
- **Line folding.** A field line starting with SP or HTAB is invalid. This rejects obsolete line folding (RFC 9112 §5.2) and whitespace before the first field name.
- **Field values.** Leading and trailing SP and HTAB are removed. The remaining bytes must be HTAB, SP, VCHAR or obs-text (0x80–0xFF), which excludes NUL, other controls and DEL. An empty value is valid.
- **Limits.** A head longer than `max_head_bytes` is `Limit("message head")`, and more than `max_fields` field lines is `Limit("field count")`. Both are checked during the scan, so oversized input is rejected without being parsed past the limit.
- **Request fields.** Host, Content-Length, Transfer-Encoding, Connection, Upgrade, TE, Keep-Alive and Proxy-Connection are classified during the scan, and the existing duplicate, framing, Connection-option, Host-requirement and Expect rules apply unchanged. Host must be a valid authority (RFC 9112 §3.2).

## Comparison with nginx

nginx's hand-written parser ([`ngx_http_parse.c`](https://github.com/nginx/nginx/blob/master/src/http/ngx_http_parse.c)) is the reference for later grammar changes. It is more lenient than this grammar in several places:

- It skips CR and LF before the request line, accepts a bare LF as a line ending, and accepts repeated spaces between request-line elements.
- It accepts `HTTP/1.x` with any minor version up to 99, and answers 505 for major versions above 1.
- It restricts methods to uppercase letters, `_` and `-`.
- It rejects a field name containing a control, SP, DEL or a leading `:`, which also rejects obs-fold and whitespace before the colon. With the default `ignore_invalid_headers`, it drops fields whose names contain other non-token bytes, including `_`, instead of rejecting the request.
- It rejects only NUL in field values. Other controls are retained, and only SP, not HTAB, is trimmed.
- Its state machine resumes across reads, so fragmented heads are parsed in linear time.

Each leniency is a deliberate interoperability choice. Adopting any of them changes this grammar and needs its own review, particularly bare LF, which is a request smuggling risk wherever an intermediary disagrees.

## Producing metadata

The request parser validates every line first and records the span of each field, then interprets the recorded fields. Recording replaces httparse's 100-slot header array with a `SmallVec` of spans, stored inline for up to 16 fields. Upgrade offers are collected in the same loop that classifies fields, rather than by a second pass.

`Path`, `Authority` and field names store `str`. Safe code can only obtain a `str` from bytes by checking them, and httparse avoids that check with `from_utf8_unchecked`. A separate `from_utf8` for each element costs about 140 instructions, which with 12 fields added about 1,000 instructions per request. Instead, one `from_utf8` checks the whole head once every line is valid, and the target and names are slices of that text. Every span starts and ends next to an ASCII byte, so slicing cannot split a character. A head containing obs-text that is not UTF-8 falls back to converting each element separately. Host is taken from the same text and validated with the existing authority checks.

Building each element while validating it, by pushing bytes into inline string storage, was considered. It does not beat one whole-head conversion, which reads a word at a time.

`Fields::add` was then the largest cost for requests with many fields. It still checks each name as a token and scans each value for CR, LF and NUL, because `Field` guarantees both for every caller, but both checks are now branch-free folds that the compiler vectorizes, with a full token check only for names containing bytes other than letters, digits and `-`. For a head with more than three field lines, the reader reserves storage for every line before adding fields, so the collection no longer grows and copies its fields repeatedly. Shorter heads grow as needed, so a head that stores a single field still needs no allocation. Together these remove about 4,400 instructions per request with 12 fields. Values longer than 24 bytes still need their own allocation.

## Incomplete input

The reader parses the first read of a head directly, without a separate terminator search. That read usually holds the whole head. A parser that reaches the end of its input returns `Incomplete`; any invalid byte in a prefix is an error immediately, since no later bytes can make it valid. After `Incomplete`, the reader falls back to the current resumable terminator search on each read and parses once when the terminator arrives. Heads delivered in small fragments therefore cost linear rather than quadratic work. A resumable parser state is possible later but is not needed for this change.

## Verification

The current pipeline is kept as a test oracle, and httparse becomes a development dependency once response heads also move (stage 3). These checks compare the new parser with the oracle:

- A deterministic differential test, run in the normal suite, which exhaustively mutates seed heads over small byte alphabets and compares acceptance, the resulting metadata and framing facts, and the head length. Error kinds are not compared, since rejections may report a different first error. A prefix rejected before its terminator arrives must be rejected by the old pipeline once completed.
- A `cargo fuzz` target performing the same comparison on arbitrary input. It needs a nightly toolchain and runs outside the normal suite.
- A request smuggling corpus: bare CR or LF in each position, obs-fold, whitespace before the colon, whitespace before the first field, duplicate or invalid Content-Length, Content-Length with Transfer-Encoding, non-chunked or repeated transfer codings, NUL and other controls in values, invalid Host forms, missing Host on HTTP/1.1, and limits on both sides of each boundary.

Existing codec, fragmentation and TCP tests continue to pass unchanged.

## Measurement

Instructions per request are measured on hana with the hello request at 1 and 64 connections, interleaved against the current main build. A second request variant with about 12 realistic fields (User-Agent, Accept, Accept-Encoding, Accept-Language, Cookie and similar) shows the cost of field names and long values, which hello does not exercise. Both are compared with Hyper.

Median user instructions per request, from five interleaved samples of each build:

| Request | Connections | Previous | This parser | Hyper |
|---|---|---|---|---|
| hello | 64 | 6,217 | 5,483 | 5,877 |
| hello | 1 | 8,814 | 8,071 | 8,739 |
| 12 fields | 64 | 22,525 | 17,929 | 15,331 |
| 12 fields | 1 | 25,012 | 20,417 | 18,143 |

With 12 fields, every Base build alternates between two levels about 600 instructions apart, so the medians there are less precise than the hello figures. The field scan itself now costs less than httparse with AVX2. Most of the saving with 12 fields comes from parsing the first read directly: the terminator search it replaces compares four bytes at every position and cost about 3,700 instructions for that head. After the `Fields::add` changes described above, the 12-field request costs 13,568 instructions at 64 connections and 16,128 at 1, below Hyper in both, and the two levels no longer appear.

## Stages

1. The request head parser, with the Host and target construction above, the differential test, fuzz target and corpus.
2. Response heads for the client and trailer sections, sharing the field-line parser.
3. Removal of the httparse production dependency and the separate `strict_lines` scan.

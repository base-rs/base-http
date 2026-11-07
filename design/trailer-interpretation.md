# Trailer interpretation

## Requirement

A received trailer section must be interpreted as a section of its own, never merged with the initial header section. When `base-http` moves field interpretation onto [`base-message`](https://github.com/base-rs/base-message):

- An application interprets the fields of a `BodyFrame::Headers` with a `FieldSection` of its own, separately from the header section. No lookup returns a value combined from both sections, and no trailer value replaces a header value; the `Last` combination, which lets a trailer `date`, `etag`, `expires`, `last-modified`, `if-modified-since` or `if-unmodified-since` replace the header's, is removed rather than ported.
- `base-http` provides a section that declares nothing and forbids the fields that must not appear in trailers, for applications to flatten into their trailer sections. Its list starts from the fields the current `headers::Policy` marks as not permitted in trailers, reviewed against [RFC 9110 §6.5.1](https://www.rfc-editor.org/rfc/rfc9110#section-6.5.1) when it is written. A received forbidden field is an error, and declaring one in a section that flattens it is a compile-time error.
- Trailer fields that a section neither declares nor forbids are ignored, as in a header section. `unknown = "reject"` remains available to an application that wants a strict trailer section.
- Raw delivery is unchanged: `BodyFrame::Headers` carries raw ordered `Fields`, forwarding a trailer frame needs no interpretation, and the protocol codecs keep enforcing their wire rules, such as removing Connection-nominated fields.

The current `HttpHeaders` does not meet this requirement: `append_trailers` adds the trailer section after a boundary, and `get` and `index` combine both sections. The move onto `base-message` replaces it; until then this document records the agreed target, and changes to `HttpHeaders` should not extend the merged interpretation.

## Rationale

The user decided on 2026-11-08 that trailers are not merged. RFC 9110 §6.5.1 says a recipient must not merge a trailer field into the header section unless the field's definition explicitly permits it and says how. Merging lets a trailer change metadata after a recipient has acted on it, such as a cache that stored a response under its header `ETag`; the `Last` combination inherited from Socketry's `protocol-http` does exactly that. Which of a header and a trailer value to believe is left to the application, which knows what it has already acted on.

Fields are refused by a denylist, as now, rather than admitted by an allowlist: once nothing is merged, an undeclared trailer field is never acted on, so an allowlist would add nothing, while forbidding a field still turns a framing, routing or credential field in trailers, which signals a broken or hostile peer, into an error. Placing the list in a forbid-only section lets `base-message`'s compile-time shadowing check stop an application from declaring such a field.

## Scope

`HttpHeaders::append_trailers`, `get`, `index` and `into_parts`, the `trailer` flag of `headers::FieldPolicy` and `Combination::Last`, their replacement built on `base-message`, and the forbid-only trailer section. Trailer framing and wire restrictions are covered by [HTTP/1 trailers](http1-trailers.md) and the codecs.

## Validation

After the migration, inspect the public API for any operation that takes both sections and returns a combined value, or that appends trailer fields to a header section. Tests must show that a trailer `etag` leaves the interpreted header section unchanged, that a forbidden trailer field is an error at its position in the trailer section, and that an undeclared one is ignored; a compile-fail test must show that declaring a forbidden field in a section that flattens the trailer rules is rejected. Review the forbidden list against RFC 9110 §6.5.1 and the current `headers::Policy` when it is written.

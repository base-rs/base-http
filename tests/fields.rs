// Released under the MIT License.
// Copyright, 2018-2026, by Samuel Williams.

use base_http::{
    Field, FieldError, Fields, HttpHeaders,
    headers::{Combination, FieldPolicy, Policy},
};

fn pairs(fields: &[Field]) -> Vec<(&str, &[u8])> {
    fields
        .iter()
        .map(|field| (field.name(), field.value()))
        .collect()
}

#[test]
fn short_and_spilled_fields_preserve_bytes_order_and_independent_mutation() {
    for length in [23, 24, 25, 256] {
        let name = "X".repeat(length);
        let value = vec![0xff; length];
        let field = Field::new(&name, &value).unwrap();
        assert_eq!(field.name(), name);
        assert_eq!(field.value(), value);
        assert_eq!(field, field.clone());

        let mut original = HttpHeaders::default();
        original.append(field).unwrap();
        original.index().unwrap();
        let mut copy = original.clone();
        // Adding another pair crosses the small collection's inline capacity.
        copy.append_trailers(Fields::new()).unwrap();
        copy.add("X-Tail", "before").unwrap();
        copy.set("x-tail", "after").unwrap();
        assert_eq!(copy.tail(), Some(1));
        assert_eq!(pairs(copy.headers()), [(&name[..], &value[..])]);
        assert_eq!(pairs(copy.trailers()), [("x-tail", &b"after"[..])]);
        assert_eq!(
            copy.get("X-TAIL").unwrap().unwrap().single(),
            Some(&b"after"[..])
        );
        assert_eq!(original.len(), 1);
        assert!(!original.has_trailers());
        assert!(!original.contains("x-tail"));

        copy.remove(&name);
        assert_eq!(copy.tail(), Some(0));
        assert_eq!(pairs(copy.trailers()), [("x-tail", &b"after"[..])]);
        copy.clear();
        copy.add("X-New", "reused").unwrap();
        assert_eq!(pairs(copy.headers()), [("X-New", &b"reused"[..])]);
        assert!(!copy.has_trailers());
    }
}

#[test]
fn indexing_preserves_raw_order_spelling_and_bytes() {
    let mut fields = HttpHeaders::try_from_pairs([
        ("Content-Type", "text/html"),
        ("connection", "Keep-Alive"),
        ("Set-Cookie", "hello=world"),
        ("Accept", "*/*"),
        ("set-cookie", "foo=bar"),
    ])
    .unwrap();
    let original = fields.pairs().to_vec();
    assert_eq!(
        fields.get("CONTENT-TYPE").unwrap().unwrap().single(),
        Some(&b"text/html"[..])
    );
    assert_eq!(
        fields.get("connection").unwrap().unwrap().combined(),
        Some(b"keep-alive".to_vec())
    );
    assert_eq!(
        fields.get("set-cookie").unwrap().unwrap().parts(),
        [b"hello=world".to_vec(), b"foo=bar".to_vec()]
    );
    assert_eq!(fields.pairs(), original);
    fields.add("X-Byte", [0xff, b'\t', b' ']).unwrap();
    assert_eq!(
        fields.values("x-byte").collect::<Vec<_>>(),
        [&[0xff, b'\t', b' '][..]]
    );
}

#[test]
fn singleton_validation_is_lazy_then_transactional_on_addition() {
    let mut fields = HttpHeaders::default();
    fields.add("Content-Length", "1").unwrap();
    fields.add("content-length", "2").unwrap();
    assert_eq!(fields.len(), 2);
    assert_eq!(
        fields.index().unwrap_err(),
        FieldError::Duplicate("content-length".into())
    );
    assert_eq!(
        fields.index().unwrap_err(),
        FieldError::Duplicate("content-length".into())
    );
    assert_eq!(fields.remove("CONTENT-LENGTH"), 2);
    fields.add("content-length", "3").unwrap();
    fields.index().unwrap();
    assert_eq!(
        fields.add("Content-Length", "4"),
        Err(FieldError::Duplicate("content-length".into()))
    );
    assert_eq!(fields.len(), 1);
    assert_eq!(
        fields.get("content-length").unwrap().unwrap().single(),
        Some(&b"3"[..])
    );
}

#[test]
fn cookie_lists_and_separate_fields_have_different_combination_rules() {
    let mut fields = HttpHeaders::default();
    fields.add("Cookie", "a=1").unwrap();
    fields.index().unwrap();
    fields.add("cookie", "b=2").unwrap();
    fields
        .add("Set-Cookie", "a=1; Expires=Wed, 21 Oct 2015 07:28:00 GMT")
        .unwrap();
    fields.add("set-cookie", "b=2; HttpOnly").unwrap();
    fields.add("Accept", "text/html, application/json").unwrap();
    assert_eq!(
        fields.get("cookie").unwrap().unwrap().combined(),
        Some(b"a=1; b=2".to_vec())
    );
    let set_cookie = fields.get("set-cookie").unwrap().unwrap();
    assert_eq!(set_cookie.parts().len(), 2);
    assert_eq!(set_cookie.combined(), None);
    assert_eq!(
        fields.get("accept").unwrap().unwrap().parts(),
        [b"text/html".to_vec(), b"application/json".to_vec()]
    );
}

#[test]
fn comma_lists_respect_quoted_commas_and_escaped_quotes() {
    let mut fields = HttpHeaders::default();
    fields
        .add(
            "Cache-Control",
            br#"private="a,b", extension="a\",b", no-cache"#,
        )
        .unwrap();
    let value = fields.get("cache-control").unwrap().unwrap();
    assert_eq!(
        value.parts(),
        [
            br#"private="a,b""#.to_vec(),
            br#"extension="a\",b""#.to_vec(),
            b"no-cache".to_vec()
        ]
    );
    let original = fields.pairs().to_vec();
    assert!(matches!(
        fields.add("cache-control", b"extension=\"unfinished"),
        Err(FieldError::InvalidList(_))
    ));
    assert_eq!(fields.pairs(), original);
    assert_eq!(
        fields.get("cache-control").unwrap().unwrap().parts().len(),
        3
    );
}

#[test]
fn malformed_list_does_not_poison_a_future_index() {
    let mut fields = HttpHeaders::default();
    fields.add("x-list", "\"unfinished").unwrap();
    assert!(matches!(fields.index(), Err(FieldError::InvalidList(_))));
    fields.set("x-list", "a,b").unwrap();
    assert_eq!(
        fields.get("x-list").unwrap().unwrap().parts(),
        [b"a".to_vec(), b"b".to_vec()]
    );
}

#[test]
fn explicit_trailer_accumulation_is_idempotent_and_index_includes_both_sections() {
    let mut fields = HttpHeaders::default();
    fields.add("X-Trace", "start").unwrap();
    fields.index().unwrap();
    fields.append_trailers(Fields::new()).unwrap();
    fields.add("x-trace", "finish").unwrap();
    fields.append_trailers(Fields::new()).unwrap();
    assert_eq!(fields.tail(), Some(1));
    assert_eq!(pairs(fields.headers()), [("X-Trace", &b"start"[..])]);
    assert_eq!(pairs(fields.trailers()), [("x-trace", &b"finish"[..])]);
    assert_eq!(
        fields.get("x-trace").unwrap().unwrap().parts(),
        [b"start".to_vec(), b"finish".to_vec()]
    );
    assert!(fields.has_trailers());
}

#[test]
fn trailer_policy_allows_extensions_and_rejects_early_processing_fields() {
    let mut fields = HttpHeaders::default();
    fields.append_trailers(Fields::new()).unwrap();
    for name in [
        "Content-Length",
        "Host",
        "Content-Type",
        "Authorization",
        "Cookie",
        "Trailer",
        "Connection",
    ] {
        assert_eq!(
            fields.add(name, "value"),
            Err(FieldError::InvalidTrailer(name.to_ascii_lowercase()))
        );
    }
    assert!(fields.is_empty());
    for name in [
        "grpc-status",
        "X-Custom",
        "ETag",
        "Digest",
        "Server-Timing",
        "Date",
    ] {
        fields.add(name, "value").unwrap();
    }
    assert_eq!(fields.headers().len(), 0);
    assert_eq!(fields.trailers().len(), 6);
    fields.index().unwrap();
}

#[test]
fn remove_and_replace_preserve_the_split_and_cached_values() {
    let mut fields =
        HttpHeaders::try_from_pairs([("X-Remove", "a"), ("X-Keep", "b"), ("x-remove", "c")])
            .unwrap();
    fields.append_trailers(Fields::new()).unwrap();
    fields.add("X-Remove", "d").unwrap();
    fields.add("X-End", "e").unwrap();
    fields.index().unwrap();
    assert_eq!(fields.remove("x-remove"), 3);
    assert_eq!(fields.tail(), Some(1));
    assert_eq!(pairs(fields.headers()), [("X-Keep", &b"b"[..])]);
    assert_eq!(pairs(fields.trailers()), [("X-End", &b"e"[..])]);
    assert!(fields.get("x-remove").unwrap().is_none());
    fields.set("x-keep", "replacement").unwrap();
    assert_eq!(fields.tail(), Some(0));
    assert_eq!(fields.trailers().len(), 2);
    assert_eq!(
        fields.get("x-keep").unwrap().unwrap().single(),
        Some(&b"replacement"[..])
    );
}

#[test]
fn failed_replacement_does_not_remove_existing_values() {
    let mut fields =
        HttpHeaders::try_from_pairs([("content-type", "text/plain"), ("x-list", "good")]).unwrap();
    fields.index().unwrap();
    assert!(fields.set("x-list", "\"unfinished").is_err());
    assert_eq!(
        fields.get("x-list").unwrap().unwrap().single(),
        Some(&b"good"[..])
    );
    fields.append_trailers(Fields::new()).unwrap();
    assert!(fields.set("content-type", "other").is_err());
    assert_eq!(
        fields.get("content-type").unwrap().unwrap().single(),
        Some(&b"text/plain"[..])
    );
    assert_eq!(fields.tail(), Some(2));
}

#[test]
fn recovering_sections_preserves_provenance_including_an_empty_section() {
    let mut headers = HttpHeaders::try_from_pairs([("x-start", "a")]).unwrap();
    headers
        .append_trailers(Fields::try_from_pairs([("digest", "abc")]).unwrap())
        .unwrap();
    let (initial, trailers) = headers.into_parts();
    assert_eq!(initial.pairs()[0].name(), "x-start");
    assert_eq!(trailers.unwrap().pairs()[0].name(), "digest");
    let mut headers = HttpHeaders::default();
    headers.append_trailers(Fields::new()).unwrap();
    assert!(headers.into_parts().1.unwrap().is_empty());
}

#[test]
fn custom_policy_invalidates_the_index_and_controls_trailers() {
    let mut fields = HttpHeaders::try_from_pairs([("x-custom", "a,b")]).unwrap();
    assert_eq!(fields.get("x-custom").unwrap().unwrap().parts().len(), 2);
    let mut policy = Policy::default();
    policy
        .set("X-CUSTOM", FieldPolicy::new(Combination::Separate, false))
        .unwrap();
    fields.set_policy(policy);
    assert_eq!(
        fields.get("x-custom").unwrap().unwrap().parts(),
        [b"a,b".to_vec()]
    );
    fields.append_trailers(Fields::new()).unwrap();
    assert!(fields.add("x-custom", "c").is_err());
    fields.add("x-other", "d").unwrap();
    let mut policy = fields.policy().clone();
    policy
        .set("x-other", FieldPolicy::new(Combination::Singleton, false))
        .unwrap();
    fields.set_policy(policy);
    assert_eq!(
        fields.index().unwrap_err(),
        FieldError::InvalidTrailer("x-other".into())
    );
    fields.remove("x-other");
    assert!(fields.index().is_ok());
}

#[test]
fn cloning_does_not_share_raw_or_indexed_mutation() {
    let mut fields = HttpHeaders::try_from_pairs([("x-value", "a")]).unwrap();
    fields.index().unwrap();
    fields.append_trailers(Fields::new()).unwrap();
    let mut copy = fields.clone();
    copy.add("x-value", "b").unwrap();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields.get("x-value").unwrap().unwrap().parts().len(), 1);
    assert_eq!(copy.get("x-value").unwrap().unwrap().parts().len(), 2);
}

#[test]
fn names_are_tokens_and_values_are_opaque_bytes() {
    for name in ["", ":status", "bad name", "bad:name", "bad\nname", "café"] {
        assert_eq!(Field::new(name, "value"), Err(FieldError::InvalidName));
    }
    // Protocol drivers decide which value bytes they can carry.
    for value in [
        b"line\r\nnext".as_slice(),
        b"nul\0byte",
        b"delete\x7f",
        &[0x80, 0xff, b'\t'],
    ] {
        assert_eq!(
            Field::new("X_!#$%&'*+-.^`|~", value).unwrap().value(),
            value
        );
    }
}

#[test]
fn dates_and_etags_replace_without_splitting_their_content() {
    let mut fields = HttpHeaders::default();
    fields.add("Date", "Wed, 21 Oct 2015 07:28:00 GMT").unwrap();
    fields.index().unwrap();
    fields.append_trailers(Fields::new()).unwrap();
    fields.add("Date", "Thu, 22 Oct 2015 07:28:00 GMT").unwrap();
    assert_eq!(
        fields.get("date").unwrap().unwrap().single(),
        Some(&b"Thu, 22 Oct 2015 07:28:00 GMT"[..])
    );
    assert_eq!(fields.values("date").count(), 2);
    fields.add("ETag", "\"first,tag\"").unwrap();
    fields.add("etag", "W/\"second,tag\"").unwrap();
    assert_eq!(
        fields.get("etag").unwrap().unwrap().single(),
        Some(&b"W/\"second,tag\""[..])
    );
}

#[test]
fn repeated_credentials_are_rejected_without_combining_or_replacing_them() {
    let mut fields = HttpHeaders::try_from_pairs([("Authorization", "Bearer original")]).unwrap();
    fields.index().unwrap();
    assert_eq!(
        fields.add("authorization", "Bearer second"),
        Err(FieldError::Duplicate("authorization".into()))
    );
    assert_eq!(
        fields.get("authorization").unwrap().unwrap().single(),
        Some(&b"Bearer original"[..])
    );
}

#[test]
fn raw_fields_do_not_interpret_duplicates_or_lists() {
    let mut fields = Fields::try_from_pairs([
        ("Content-Length", "1"),
        ("content-length", "2"),
        ("X-List", "\"unfinished"),
    ])
    .unwrap();
    assert_eq!(
        fields.values("CONTENT-LENGTH").collect::<Vec<_>>(),
        [b"1", b"2"]
    );
    fields.set("x-list", "still raw").unwrap();
    let mut headers = HttpHeaders::new(fields.clone());
    assert_eq!(
        headers.index().unwrap_err(),
        FieldError::Duplicate("content-length".into())
    );
    assert_eq!(fields.len(), 3);
    assert_eq!(fields.pairs()[0].name(), "Content-Length");
    let rest = fields.split_off(2);
    fields.extend(rest);
    assert_eq!(fields.pairs()[2].value(), b"still raw");
}

#[test]
fn rejected_trailer_sections_do_not_partially_append_or_change_the_index() {
    for indexed in [false, true] {
        let mut headers = HttpHeaders::try_from_pairs([("x-initial", "one")]).unwrap();
        if indexed {
            headers.index().unwrap();
        }
        let fields =
            Fields::try_from_pairs([("x-tail", "valid"), ("content-length", "1")]).unwrap();
        assert!(headers.append_trailers(fields).is_err());
        assert!(!headers.has_trailers());
        assert_eq!(headers.len(), 1);
        assert!(headers.get("x-tail").unwrap().is_none());
    }
}

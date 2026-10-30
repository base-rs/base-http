// Released under the MIT License.
// Copyright, 2018-2026, by Samuel Williams.

use base_http::{
    FieldError, Fields, HttpHeaders,
    headers::{Combination, FieldPolicy, Policy},
};

fn parts(headers: &mut HttpHeaders, name: &str) -> Vec<Vec<u8>> {
    headers.get(name).unwrap().unwrap().parts().to_vec()
}

#[test]
fn policy_overrides_require_token_names() {
    let mut policy = Policy::default();
    let separate = FieldPolicy::new(Combination::Separate, true);
    assert_eq!(policy.set("x bad", separate), Err(FieldError::InvalidName));
    assert_eq!(
        policy.get("x bad"),
        FieldPolicy::new(Combination::Comma, true)
    );
    policy.set("X-Good", separate).unwrap();
    assert_eq!(policy.get("x-good"), separate);
}

#[test]
fn interpreted_values_report_their_combination_and_single_component() {
    let mut headers = HttpHeaders::try_from_pairs([
        ("Accept", "text/html, text/plain"),
        ("Connection", "Close"),
        ("Cookie", "a=1"),
        ("Cookie", "b=2"),
        ("Set-Cookie", "c=3"),
        ("Host", "example.com"),
        ("ETag", "\"1\""),
        ("ETag", "\"2\""),
    ])
    .unwrap();
    let cases = [
        ("accept", Combination::Comma, None),
        (
            "connection",
            Combination::LowercaseComma,
            Some(&b"close"[..]),
        ),
        ("cookie", Combination::Cookie, None),
        ("set-cookie", Combination::Separate, Some(&b"c=3"[..])),
        ("host", Combination::Singleton, Some(&b"example.com"[..])),
        ("etag", Combination::Last, Some(&b"\"2\""[..])),
    ];
    for (name, combination, single) in cases {
        let value = headers.get(name).unwrap().unwrap();
        assert_eq!(value.combination(), combination, "{name}");
        assert_eq!(value.single(), single, "{name}");
    }
}

#[test]
fn empty_list_elements_are_ignored() {
    // RFC 9110 (section 5.6.1) requires recipients to ignore empty list elements.
    let mut headers =
        HttpHeaders::try_from_pairs([("Accept", ", a,, b ,"), ("Accept", ""), ("Vary", " , ")])
            .unwrap();
    assert_eq!(parts(&mut headers, "accept"), [b"a", b"b"]);
    assert!(parts(&mut headers, "vary").is_empty());
    assert_eq!(
        headers.get("vary").unwrap().unwrap().combined(),
        Some(Vec::new())
    );
}

#[test]
fn invalid_names_are_rejected_before_changing_the_collection() {
    assert_eq!(
        HttpHeaders::try_from_pairs([("Accept", "a"), ("", "b")]).unwrap_err(),
        FieldError::InvalidName
    );
    let mut headers = HttpHeaders::try_from_pairs([("Accept", "a")]).unwrap();
    headers.index().unwrap();
    assert_eq!(headers.add("Bad Name", "b"), Err(FieldError::InvalidName));
    assert_eq!(headers.set("Bad Name", "b"), Err(FieldError::InvalidName));
    assert_eq!(headers.len(), 1);
    assert_eq!(parts(&mut headers, "accept"), [b"a"]);
}

#[test]
fn lookup_reports_index_errors_until_repaired() {
    let mut headers = HttpHeaders::try_from_pairs([("Host", "a"), ("Host", "b")]).unwrap();
    assert_eq!(
        headers.get("accept"),
        Err(FieldError::Duplicate("host".into()))
    );
    assert_eq!(headers.remove("host"), 2);
    assert_eq!(headers.get("host"), Ok(None));
}

#[test]
fn trailers_merge_into_a_cached_index() {
    let mut headers = HttpHeaders::try_from_pairs([("X-Checksum", "a"), ("Date", "1")]).unwrap();
    headers.index().unwrap();
    headers
        .append_trailers(Fields::try_from_pairs([("x-checksum", "b, c"), ("Date", "2")]).unwrap())
        .unwrap();
    assert_eq!(headers.tail(), Some(2));
    assert_eq!(parts(&mut headers, "x-checksum"), [&b"a"[..], b"b", b"c"]);
    assert_eq!(parts(&mut headers, "date"), [b"2"]);
}

#[test]
fn trailers_that_conflict_with_a_cached_index_are_rejected_without_changes() {
    let mut policy = Policy::default();
    policy
        .set("X-Digest", FieldPolicy::new(Combination::Singleton, true))
        .unwrap();
    let initial = Fields::try_from_pairs([("X-Digest", "a"), ("X-List", "b")]).unwrap();
    let mut headers = HttpHeaders::with_policy(initial, policy);
    headers.index().unwrap();

    let duplicate = Fields::try_from_pairs([("X-List", "c"), ("X-Digest", "d")]).unwrap();
    assert_eq!(
        headers.append_trailers(duplicate),
        Err(FieldError::Duplicate("x-digest".into()))
    );
    let unterminated = Fields::try_from_pairs([("X-List", "\"c")]).unwrap();
    assert_eq!(
        headers.append_trailers(unterminated),
        Err(FieldError::InvalidList("x-list".into()))
    );
    assert!(!headers.has_trailers());
    assert_eq!(headers.len(), 2);
    assert_eq!(parts(&mut headers, "x-list"), [b"b"]);
    assert_eq!(parts(&mut headers, "x-digest"), [b"a"]);
}

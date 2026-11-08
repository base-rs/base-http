// Released under the MIT License.
// Copyright, 2018-2026, by Samuel Williams.

use base_http::{Field, FieldError, Fields};

fn names(fields: &Fields) -> Vec<&str> {
    fields.pairs().iter().map(Field::name).collect()
}

#[test]
fn errors_describe_the_rejected_field() {
    assert_eq!(
        FieldError::InvalidName.to_string(),
        "invalid HTTP field name"
    );
    assert_eq!(
        FieldError::Duplicate("host".into()).to_string(),
        "duplicate singleton field: host"
    );
    assert_eq!(
        FieldError::InvalidTrailer("content-length".into()).to_string(),
        "field is not permitted in trailers: content-length"
    );
    assert_eq!(
        FieldError::InvalidList("accept".into()).to_string(),
        "unterminated quoted value in field: accept"
    );
}

#[test]
fn building_from_pairs_stops_at_the_first_invalid_name() {
    assert_eq!(
        Fields::try_from_pairs([("Accept", "a"), ("Bad Name", "b"), ("Host", "c")]),
        Err(FieldError::InvalidName)
    );
    let fields = Fields::try_from_pairs([("Accept", "a"), ("Host", "c")]).unwrap();
    assert_eq!(names(&fields), ["Accept", "Host"]);
}

#[test]
fn rejected_replacement_leaves_existing_pairs() {
    let mut fields = Fields::try_from_pairs([("Accept", "a"), ("accept", "b")]).unwrap();
    assert_eq!(fields.set("", "c"), Err(FieldError::InvalidName));
    assert_eq!(fields.values("accept").collect::<Vec<_>>(), [b"a", b"b"]);
}

#[test]
fn collecting_preserves_order_and_clearing_removes_every_pair() {
    let mut fields: Fields = [("B", "1"), ("A", "2"), ("b", "3")]
        .into_iter()
        .map(|(name, value)| Field::new(name, value).unwrap())
        .collect();
    assert_eq!(names(&fields), ["B", "A", "b"]);
    assert_eq!(fields.values("b").collect::<Vec<_>>(), [b"1", b"3"]);

    fields.clear();
    assert!(fields.is_empty());
    assert!(!fields.contains("a"));
    fields.add("C", "4").unwrap();
    assert_eq!(names(&fields), ["C"]);
}

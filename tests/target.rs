// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::{Authority, Path, Scheme};
use std::borrow::Cow;

fn text(value: impl AsRef<str>) -> String {
    value.as_ref().to_owned()
}

#[test]
fn components_convert_back_into_their_normalized_text() {
    assert_eq!(String::from(Path::from("/A?b")), "/A?b");
    assert_eq!(
        String::from(Authority::from("Example.COM:80")),
        "example.com:80"
    );
    let long = "/".to_owned() + &"Segment".repeat(10);
    assert_eq!(String::from(Path::from(long.as_str())), long);
    assert_eq!(String::from(Scheme::from("WS")), "ws");
}

#[test]
fn owned_cows_are_normalized_like_strings() {
    let long = "Long.Example.COM:".to_owned() + &"8".repeat(10);
    assert_eq!(
        Authority::from(Cow::Owned(long.clone())),
        long.to_ascii_lowercase()
    );
    assert_eq!(Authority::from(Cow::<str>::Owned("A:1".into())), "a:1");
    assert_eq!(Path::from(Cow::<str>::Owned("/A".into())), "/A");
    assert_eq!(Scheme::from(Cow::<str>::Owned("HTTP".into())), Scheme::HTTP);
    assert_eq!(Scheme::from(Cow::<str>::Owned("Coap".into())), "coap");
}

#[test]
fn display_and_as_ref_use_the_normalized_text() {
    assert_eq!(Scheme::from("HTTPS").to_string(), "https");
    assert_eq!(Authority::from("[::ABCD]:443").to_string(), "[::abcd]:443");
    assert_eq!(Path::from("/A B").to_string(), "/A B");
    assert_eq!(
        format!(
            "{}://{}{}",
            Scheme::HTTP,
            Authority::from("A"),
            Path::from("/")
        ),
        "http://a/"
    );
    assert_eq!(text(Scheme::from("Ws")), "ws");
    assert_eq!(text(Authority::from("B:2")), "b:2");
    assert_eq!(text(Path::from("/C")), "/C");
}

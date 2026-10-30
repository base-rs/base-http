// Released under the MIT License.
// Copyright, 2018-2026, by Samuel Williams.

use base_http::{Method, method::InvalidMethod};

fn text(value: impl AsRef<str>) -> String {
    value.as_ref().to_owned()
}

#[test]
fn parsing_accepts_tokens_and_preserves_case() {
    let method: Method = "PROPFIND".parse().unwrap();
    assert_eq!(method.as_str(), "PROPFIND");
    assert_eq!("GET".parse::<Method>(), Ok(Method::GET));
    assert_ne!("get".parse::<Method>(), Ok(Method::GET));
    assert_eq!("".parse::<Method>(), Err(InvalidMethod));
    assert_eq!("GET /".parse::<Method>(), Err(InvalidMethod));
    assert_eq!(InvalidMethod.to_string(), "invalid HTTP method token");
}

#[test]
fn display_and_as_ref_write_the_method_token() {
    assert_eq!(Method::PATCH.to_string(), "PATCH");
    assert_eq!(Method::new("Purge").unwrap().to_string(), "Purge");
    assert_eq!(text(Method::QUERY), "QUERY");
    assert_eq!(text(Method::new("M-SEARCH").unwrap()), "M-SEARCH");
}

// Released under the MIT License.
// Copyright, 2019-2026, by Samuel Williams.

use base_http::{Status, status::InvalidStatus};

#[test]
fn codes_outside_the_three_digit_classes_are_rejected() {
    assert_eq!(Status::new(99), Err(InvalidStatus));
    assert_eq!(Status::new(600), Err(InvalidStatus));
    assert_eq!(Status::try_from(0), Err(InvalidStatus));
    assert_eq!(Status::try_from(100).map(Status::as_u16), Ok(100));
    assert_eq!(Status::try_from(599).map(u16::from), Ok(599));
    assert_eq!(
        InvalidStatus.to_string(),
        "HTTP status must be between 100 and 599"
    );
}

#[test]
fn display_writes_the_numeric_code_without_a_reason_phrase() {
    assert_eq!(Status::NOT_FOUND.to_string(), "404");
    assert_eq!(Status::new(599).unwrap().to_string(), "599");
    assert_eq!(format!("{:>5}", Status::OK), "  200");
}

#[test]
fn redirection_covers_exactly_the_3xx_class() {
    for code in 100..=599 {
        let status = Status::new(code).unwrap();
        assert_eq!(
            status.is_redirection(),
            (300..400).contains(&code),
            "{code}"
        );
    }
    assert!(Status::NOT_MODIFIED.is_redirection());
    assert!(Status::PERMANENT_REDIRECT.is_redirection());
    assert!(!Status::OK.is_redirection());
    assert!(!Status::BAD_REQUEST.is_redirection());
}

#[test]
fn codes_have_conventional_descriptions() {
    let descriptions = [
        (100, "Continue"),
        (101, "Switching Protocols"),
        (102, "Processing"),
        (103, "Early Hints"),
        (200, "OK"),
        (201, "Created"),
        (202, "Accepted"),
        (203, "Non-Authoritative Information"),
        (204, "No Content"),
        (205, "Reset Content"),
        (206, "Partial Content"),
        (207, "Multi-Status"),
        (208, "Already Reported"),
        (226, "IM Used"),
        (300, "Multiple Choices"),
        (301, "Moved Permanently"),
        (302, "Found"),
        (303, "See Other"),
        (304, "Not Modified"),
        (305, "Use Proxy"),
        (306, "Switch Proxy"),
        (307, "Temporary Redirect"),
        (308, "Permanent Redirect"),
        (400, "Bad Request"),
        (401, "Unauthorized"),
        (402, "Payment Required"),
        (403, "Forbidden"),
        (404, "Not Found"),
        (405, "Method Not Allowed"),
        (406, "Not Acceptable"),
        (407, "Proxy Authentication Required"),
        (408, "Request Timeout"),
        (409, "Conflict"),
        (410, "Gone"),
        (411, "Length Required"),
        (412, "Precondition Failed"),
        (413, "Content Too Large"),
        (414, "URI Too Long"),
        (415, "Unsupported Media Type"),
        (416, "Range Not Satisfiable"),
        (417, "Expectation Failed"),
        (418, "I'm a Teapot"),
        (421, "Misdirected Request"),
        (422, "Unprocessable Content"),
        (423, "Locked"),
        (424, "Failed Dependency"),
        (425, "Too Early"),
        (426, "Upgrade Required"),
        (428, "Precondition Required"),
        (429, "Too Many Requests"),
        (431, "Request Header Fields Too Large"),
        (451, "Unavailable For Legal Reasons"),
        (500, "Internal Server Error"),
        (501, "Not Implemented"),
        (502, "Bad Gateway"),
        (503, "Service Unavailable"),
        (504, "Gateway Timeout"),
        (505, "HTTP Version Not Supported"),
        (506, "Variant Also Negotiates"),
        (507, "Insufficient Storage"),
        (508, "Loop Detected"),
        (510, "Not Extended"),
        (511, "Network Authentication Required"),
    ];
    for code in 100..=599 {
        let expected = descriptions
            .iter()
            .find(|(registered, _)| *registered == code)
            .map(|(_, description)| *description);
        assert_eq!(Status::new(code).unwrap().description(), expected, "{code}");
    }
}

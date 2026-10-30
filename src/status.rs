// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::{error::Error, fmt};

/// An HTTP status code in the range 100..=599, including unknown codes.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Status(u16);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidStatus;

impl fmt::Display for InvalidStatus {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str("HTTP status must be between 100 and 599")
    }
}
impl Error for InvalidStatus {}

impl Status {
    pub const CONTINUE: Self = Self(100);
    pub const SWITCHING_PROTOCOLS: Self = Self(101);
    pub const PROCESSING: Self = Self(102);
    pub const EARLY_HINTS: Self = Self(103);
    pub const OK: Self = Self(200);
    pub const CREATED: Self = Self(201);
    pub const ACCEPTED: Self = Self(202);
    pub const NO_CONTENT: Self = Self(204);
    pub const RESET_CONTENT: Self = Self(205);
    pub const PARTIAL_CONTENT: Self = Self(206);
    pub const MOVED_PERMANENTLY: Self = Self(301);
    pub const FOUND: Self = Self(302);
    pub const SEE_OTHER: Self = Self(303);
    pub const NOT_MODIFIED: Self = Self(304);
    pub const TEMPORARY_REDIRECT: Self = Self(307);
    pub const PERMANENT_REDIRECT: Self = Self(308);
    pub const BAD_REQUEST: Self = Self(400);
    pub const UNAUTHORIZED: Self = Self(401);
    pub const FORBIDDEN: Self = Self(403);
    pub const NOT_FOUND: Self = Self(404);
    pub const METHOD_NOT_ALLOWED: Self = Self(405);
    pub const REQUEST_TIMEOUT: Self = Self(408);
    pub const CONFLICT: Self = Self(409);
    pub const CONTENT_TOO_LARGE: Self = Self(413);
    pub const EXPECTATION_FAILED: Self = Self(417);
    pub const TOO_MANY_REQUESTS: Self = Self(429);
    pub const INTERNAL_SERVER_ERROR: Self = Self(500);
    pub const NOT_IMPLEMENTED: Self = Self(501);
    pub const BAD_GATEWAY: Self = Self(502);
    pub const SERVICE_UNAVAILABLE: Self = Self(503);
    pub const GATEWAY_TIMEOUT: Self = Self(504);

    pub const fn new(code: u16) -> Result<Self, InvalidStatus> {
        if code >= 100 && code <= 599 {
            Ok(Self(code))
        } else {
            Err(InvalidStatus)
        }
    }

    pub const fn as_u16(self) -> u16 {
        self.0
    }
    pub const fn is_informational(self) -> bool {
        self.0 >= 100 && self.0 < 200
    }
    /// Switching protocols ends an HTTP/1 exchange despite its 1xx class.
    pub const fn is_final(self) -> bool {
        self.0 == 101 || self.0 >= 200
    }
    pub const fn is_continue(self) -> bool {
        self.0 == 100
    }
    pub const fn is_ok(self) -> bool {
        self.0 == 200
    }
    pub const fn is_success(self) -> bool {
        self.0 >= 200 && self.0 < 300
    }
    pub const fn is_partial(self) -> bool {
        self.0 == 206
    }
    pub const fn is_redirection(self) -> bool {
        self.0 >= 300 && self.0 < 400
    }
    pub const fn is_not_modified(self) -> bool {
        self.0 == 304
    }
    pub const fn preserves_method(self) -> bool {
        self.0 == 307 || self.0 == 308
    }
    pub const fn is_failure(self) -> bool {
        self.0 >= 400
    }
    pub const fn is_client_error(self) -> bool {
        self.0 >= 400 && self.0 < 500
    }
    pub const fn is_server_error(self) -> bool {
        self.0 >= 500
    }

    /// A conventional description, independent of the wire version/reason phrase.
    pub const fn description(self) -> Option<&'static str> {
        Some(match self.0 {
            100 => "Continue",
            101 => "Switching Protocols",
            102 => "Processing",
            103 => "Early Hints",
            200 => "OK",
            201 => "Created",
            202 => "Accepted",
            203 => "Non-Authoritative Information",
            204 => "No Content",
            205 => "Reset Content",
            206 => "Partial Content",
            207 => "Multi-Status",
            208 => "Already Reported",
            226 => "IM Used",
            300 => "Multiple Choices",
            301 => "Moved Permanently",
            302 => "Found",
            303 => "See Other",
            304 => "Not Modified",
            305 => "Use Proxy",
            306 => "Switch Proxy",
            307 => "Temporary Redirect",
            308 => "Permanent Redirect",
            400 => "Bad Request",
            401 => "Unauthorized",
            402 => "Payment Required",
            403 => "Forbidden",
            404 => "Not Found",
            405 => "Method Not Allowed",
            406 => "Not Acceptable",
            407 => "Proxy Authentication Required",
            408 => "Request Timeout",
            409 => "Conflict",
            410 => "Gone",
            411 => "Length Required",
            412 => "Precondition Failed",
            413 => "Content Too Large",
            414 => "URI Too Long",
            415 => "Unsupported Media Type",
            416 => "Range Not Satisfiable",
            417 => "Expectation Failed",
            418 => "I'm a Teapot",
            421 => "Misdirected Request",
            422 => "Unprocessable Content",
            423 => "Locked",
            424 => "Failed Dependency",
            425 => "Too Early",
            426 => "Upgrade Required",
            428 => "Precondition Required",
            429 => "Too Many Requests",
            431 => "Request Header Fields Too Large",
            451 => "Unavailable For Legal Reasons",
            500 => "Internal Server Error",
            501 => "Not Implemented",
            502 => "Bad Gateway",
            503 => "Service Unavailable",
            504 => "Gateway Timeout",
            505 => "HTTP Version Not Supported",
            506 => "Variant Also Negotiates",
            507 => "Insufficient Storage",
            508 => "Loop Detected",
            510 => "Not Extended",
            511 => "Network Authentication Required",
            _ => return None,
        })
    }
}

impl TryFrom<u16> for Status {
    type Error = InvalidStatus;
    fn try_from(value: u16) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<Status> for u16 {
    fn from(value: Status) -> Self {
        value.0
    }
}
impl fmt::Display for Status {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(output)
    }
}

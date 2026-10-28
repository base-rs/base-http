// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Request target components. Schemes and authorities are case-insensitive and
//! stored in their canonical lowercase form (RFC 3986, sections 3.1 and
//! 6.2.2.1); paths are preserved as supplied. Drivers validate target
//! combinations and wire syntax before transmission.

use arrayvec::ArrayString;
use std::{
    borrow::{Borrow, Cow},
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    ops::Deref,
};

/// A lowercase request scheme. `http` and `https` in any case use borrowed
/// constants; other schemes are lowercased when converted. Comparisons with
/// `str` use the lowercase text.
#[derive(Clone)]
pub struct Scheme(Cow<'static, str>);

impl Scheme {
    pub const HTTP: Self = Self(Cow::Borrowed("http"));
    pub const HTTPS: Self = Self(Cow::Borrowed("https"));

    pub fn as_str(&self) -> &str {
        &self.0
    }
    fn into_string(self) -> String {
        self.0.into_owned()
    }
}

impl Scheme {
    fn known(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("http") {
            Some(Self::HTTP)
        } else if value.eq_ignore_ascii_case("https") {
            Some(Self::HTTPS)
        } else {
            None
        }
    }
}

impl From<&str> for Scheme {
    fn from(value: &str) -> Self {
        Self::known(value).unwrap_or_else(|| Self(Cow::Owned(value.to_ascii_lowercase())))
    }
}
impl From<String> for Scheme {
    fn from(mut value: String) -> Self {
        Self::known(&value).unwrap_or_else(|| {
            value.make_ascii_lowercase();
            Self(Cow::Owned(value))
        })
    }
}

// Short authorities and paths require no separate allocation, however they
// were supplied.
#[derive(Clone)]
enum Storage {
    Inline(ArrayString<24>),
    Heap(String),
}

macro_rules! stored {
    ($($(#[$meta:meta])* $name:ident => $normalize:expr),*) => {$(
        $(#[$meta])*
        #[derive(Clone)]
        pub struct $name(Storage);

        impl $name {
            pub fn as_str(&self) -> &str {
                match &self.0 {
                    Storage::Inline(value) => value.as_str(),
                    Storage::Heap(value) => value,
                }
            }
            fn into_string(self) -> String {
                match self.0 {
                    Storage::Inline(value) => value.as_str().to_owned(),
                    Storage::Heap(value) => value,
                }
            }
        }

        impl From<&str> for $name {
            #[inline]
            fn from(value: &str) -> Self {
                let normalize: fn(&mut str) = $normalize;
                match ArrayString::from(value) {
                    Ok(mut value) => {
                        normalize(&mut value);
                        Self(Storage::Inline(value))
                    }
                    Err(_) => {
                        let mut value = value.to_owned();
                        normalize(&mut value);
                        Self(Storage::Heap(value))
                    }
                }
            }
        }
        impl From<String> for $name {
            #[inline]
            fn from(mut value: String) -> Self {
                let normalize: fn(&mut str) = $normalize;
                match ArrayString::from(&value) {
                    Ok(mut inline) => {
                        normalize(&mut inline);
                        Self(Storage::Inline(inline))
                    }
                    Err(_) => {
                        normalize(&mut value);
                        Self(Storage::Heap(value))
                    }
                }
            }
        }
    )*};
}

stored! {
    /// A lowercase request authority, such as `example.com:8080`. HTTP
    /// authorities are a case-insensitive host and an optional port, so
    /// conversions lowercase ASCII letters and comparisons with `str` use the
    /// lowercase text.
    Authority => str::make_ascii_lowercase,
    /// A request path with optional query, or another supplied request-target
    /// form. Paths are case-sensitive and preserved as supplied.
    Path => |_| {}
}

// Each component behaves as its text: comparison, hashing and formatting do
// not depend on storage.
macro_rules! text {
    ($($name:ident),*) => {$(
        impl From<&String> for $name {
            fn from(value: &String) -> Self {
                Self::from(value.as_str())
            }
        }
        impl From<Box<str>> for $name {
            fn from(value: Box<str>) -> Self {
                Self::from(String::from(value))
            }
        }
        impl From<Cow<'_, str>> for $name {
            fn from(value: Cow<'_, str>) -> Self {
                match value {
                    Cow::Borrowed(value) => Self::from(value),
                    Cow::Owned(value) => Self::from(value),
                }
            }
        }
        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.into_string()
            }
        }
        impl Deref for $name {
            type Target = str;
            fn deref(&self) -> &str {
                self.as_str()
            }
        }
        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }
        impl Borrow<str> for $name {
            fn borrow(&self) -> &str {
                self.as_str()
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
                output.write_str(self.as_str())
            }
        }
        impl fmt::Debug for $name {
            fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
                output.debug_tuple(stringify!($name)).field(&self.as_str()).finish()
            }
        }
        impl PartialEq for $name {
            fn eq(&self, other: &Self) -> bool {
                self.as_str() == other.as_str()
            }
        }
        impl Eq for $name {}
        impl Hash for $name {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.as_str().hash(state)
            }
        }
        impl PartialOrd for $name {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for $name {
            fn cmp(&self, other: &Self) -> Ordering {
                self.as_str().cmp(other.as_str())
            }
        }
        text!(@eq $name, str);
        text!(@eq $name, &str);
        text!(@eq $name, String);
    )*};
    (@eq $name:ident, $other:ty) => {
        impl PartialEq<$other> for $name {
            fn eq(&self, other: &$other) -> bool {
                self.as_str() == &other[..]
            }
        }
        impl PartialEq<$name> for $other {
            fn eq(&self, other: &$name) -> bool {
                &self[..] == other.as_str()
            }
        }
    };
}

text!(Scheme, Authority, Path);

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeSet, HashMap};

    #[test]
    fn schemes_use_constants() {
        assert!(matches!(Scheme::from("http").0, Cow::Borrowed(_)));
        assert!(matches!(
            Scheme::from(String::from("https")).0,
            Cow::Borrowed(_)
        ));
        assert!(matches!(Scheme::from("HTTP").0, Cow::Borrowed("http")));
        assert!(matches!(
            Scheme::from(String::from("HttpS")).0,
            Cow::Borrowed("https")
        ));
        assert_eq!(Scheme::from("WS"), "ws");
        assert_eq!(Scheme::from(String::from("Coap+TCP")), "coap+tcp");
        assert_eq!(Scheme::from(&String::from("WSS")), "wss");
        assert_eq!(Scheme::from(Cow::Borrowed("Https")), Scheme::HTTPS);
        assert_eq!(Scheme::from(Box::<str>::from("FTP")), "ftp");
        assert_ne!(Scheme::from("HTTPS"), "HTTPS");
        assert_eq!(String::from(Scheme::HTTPS), "https");
    }

    #[test]
    fn short_text_is_inline_however_supplied() {
        let inline = |path: &Path| matches!(path.0, Storage::Inline(_));
        assert!(inline(&Path::from("/")));
        assert!(inline(&Path::from(String::from("/"))));
        assert!(inline(&Path::from("x".repeat(24))));
        assert!(!inline(&Path::from("x".repeat(25))));
        // 12 two-byte characters fill the inline capacity exactly.
        assert!(inline(&Path::from("é".repeat(12).as_str())));
        assert!(!inline(&Path::from("é".repeat(13).as_str())));
        assert_eq!(String::from(Path::from("x".repeat(25))), "x".repeat(25));
    }

    #[test]
    fn authorities_are_lowercase_and_paths_are_preserved() {
        let long = "Long.Example.COM:".to_owned() + &"8".repeat(10);
        assert_eq!(Authority::from("Example.COM:80"), "example.com:80");
        assert_eq!(
            Authority::from(String::from("[::ABCD]:443")),
            "[::abcd]:443"
        );
        assert_eq!(Authority::from(long.as_str()), long.to_ascii_lowercase());
        assert_eq!(Authority::from(long.clone()), long.to_ascii_lowercase());
        assert_eq!(Authority::from("É.example"), "É.example");
        assert_eq!(Authority::from(Cow::Borrowed("A:1")), "a:1");
        assert_eq!(Authority::from(Box::<str>::from("B:2")), "b:2");
        assert_eq!(Path::from("/A?B=C"), "/A?B=C");
        assert_eq!(Path::from("/A".repeat(20)), "/A".repeat(20));
    }

    #[test]
    fn text_behaves_as_str() {
        let long = "x".repeat(40);
        let owned = String::from("/a");
        assert_eq!(Path::from(&owned), Path::from(Cow::Borrowed("/a")));
        assert_eq!(
            Authority::from(long.clone()),
            Authority::from(long.as_str())
        );
        assert_eq!(Path::from(Box::<str>::from("/a")), "/a");
        let path = Path::from("/a");
        assert!("/a" == path && owned == path && path == owned);
        assert_eq!(format!("{:?}", Path::from("/a")), "Path(\"/a\")");
        assert_eq!(format!("{:?}", Scheme::HTTP), "Scheme(\"http\")");
        let sorted: BTreeSet<_> = ["/b", "/a"].into_iter().map(Path::from).collect();
        assert_eq!(sorted.first().unwrap(), "/a");
        let schemes = HashMap::from([(Scheme::HTTP, 80)]);
        assert_eq!(schemes.get("http"), Some(&80));
    }
}

// Released under the MIT License.
// Copyright, 2018-2026, by Samuel Williams.
// Copyright, 2024, by Maruth Goyal.
// Copyright, 2024, by Nathan Froyd.

//! The static table, [RFC 7541 Appendix A](https://www.rfc-editor.org/rfc/rfc7541#appendix-A).

/// HPACK's static table of names and values, in index order: the entry of index `i`, from 1 to 61, is `STATIC_TABLE[i - 1]`.
///
/// A decoded field whose name came from this table reports its index with [`DecodedField::static_index`](crate::DecodedField::static_index), so that a protocol can dispatch on it without comparing names.
pub const STATIC_TABLE: [(&str, &str); 61] = [
    (":authority", ""),
    (":method", "GET"),
    (":method", "POST"),
    (":path", "/"),
    (":path", "/index.html"),
    (":scheme", "http"),
    (":scheme", "https"),
    (":status", "200"),
    (":status", "204"),
    (":status", "206"),
    (":status", "304"),
    (":status", "400"),
    (":status", "404"),
    (":status", "500"),
    ("accept-charset", ""),
    ("accept-encoding", "gzip, deflate"),
    ("accept-language", ""),
    ("accept-ranges", ""),
    ("accept", ""),
    ("access-control-allow-origin", ""),
    ("age", ""),
    ("allow", ""),
    ("authorization", ""),
    ("cache-control", ""),
    ("content-disposition", ""),
    ("content-encoding", ""),
    ("content-language", ""),
    ("content-length", ""),
    ("content-location", ""),
    ("content-range", ""),
    ("content-type", ""),
    ("cookie", ""),
    ("date", ""),
    ("etag", ""),
    ("expect", ""),
    ("expires", ""),
    ("from", ""),
    ("host", ""),
    ("if-match", ""),
    ("if-modified-since", ""),
    ("if-none-match", ""),
    ("if-range", ""),
    ("if-unmodified-since", ""),
    ("last-modified", ""),
    ("link", ""),
    ("location", ""),
    ("max-forwards", ""),
    ("proxy-authenticate", ""),
    ("proxy-authorization", ""),
    ("range", ""),
    ("referer", ""),
    ("refresh", ""),
    ("retry-after", ""),
    ("server", ""),
    ("set-cookie", ""),
    ("strict-transport-security", ""),
    ("transfer-encoding", ""),
    ("user-agent", ""),
    ("vary", ""),
    ("via", ""),
    ("www-authenticate", ""),
];

/// The number of entries in the static table; dynamic table indices follow it.
pub(crate) const STATIC_TABLE_LEN: usize = STATIC_TABLE.len();

/// The entries of a table matching a field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TableMatch {
    /// The first index whose name matches, counted as the table counts.
    pub(crate) name: usize,
    /// The index whose name and value both match, if any.
    pub(crate) exact: Option<usize>,
}

/// Finds the static table entries matching `name` and `value`.
///
/// The name is dispatched on its length and first byte before the few candidates are compared, without hashing, as the HTTP/2 requirements for static table indices require.
pub(crate) fn find(name: &[u8], value: &[u8]) -> Option<TableMatch> {
    let index = name_index(name)?;
    let exact =
        (index..index + values(index)).find(|&index| STATIC_TABLE[index - 1].1.as_bytes() == value);
    Some(TableMatch { name: index, exact })
}

/// The number of consecutive entries, starting at `index`, that share its name.
fn values(index: usize) -> usize {
    match index {
        // `:method`, `:path` and `:scheme`.
        2 | 4 | 6 => 2,
        // `:status`.
        8 => 7,
        _ => 1,
    }
}

/// The first index of the entry named `name`, among `candidates`.
fn one_of(name: &[u8], candidates: &[usize]) -> Option<usize> {
    candidates
        .iter()
        .copied()
        .find(|&index| STATIC_TABLE[index - 1].0.as_bytes() == name)
}

/// The first index of the entry named `name`.
fn name_index(name: &[u8]) -> Option<usize> {
    match name.len() {
        3 => match name[0] {
            b'a' => one_of(name, &[21]),
            b'v' => one_of(name, &[60]),
            _ => None,
        },
        4 => match name[0] {
            b'd' => one_of(name, &[33]),
            b'e' => one_of(name, &[34]),
            b'f' => one_of(name, &[37]),
            b'h' => one_of(name, &[38]),
            b'l' => one_of(name, &[45]),
            b'v' => one_of(name, &[59]),
            _ => None,
        },
        5 => match name[0] {
            b':' => one_of(name, &[4]),
            b'a' => one_of(name, &[22]),
            b'r' => one_of(name, &[50]),
            _ => None,
        },
        6 => match name[0] {
            b'a' => one_of(name, &[19]),
            b'c' => one_of(name, &[32]),
            b'e' => one_of(name, &[35]),
            b's' => one_of(name, &[54]),
            _ => None,
        },
        7 => match name[0] {
            b':' => one_of(name, &[2, 6, 8]),
            b'e' => one_of(name, &[36]),
            b'r' => one_of(name, &[51, 52]),
            _ => None,
        },
        8 => match name[0] {
            b'i' => one_of(name, &[39, 42]),
            b'l' => one_of(name, &[46]),
            _ => None,
        },
        10 => match name[0] {
            b':' => one_of(name, &[1]),
            b's' => one_of(name, &[55]),
            b'u' => one_of(name, &[58]),
            _ => None,
        },
        11 => match name[0] {
            b'r' => one_of(name, &[53]),
            _ => None,
        },
        12 => match name[0] {
            b'c' => one_of(name, &[31]),
            b'm' => one_of(name, &[47]),
            _ => None,
        },
        13 => match name[0] {
            b'a' => one_of(name, &[18, 23]),
            b'c' => one_of(name, &[24, 30]),
            b'i' => one_of(name, &[41]),
            b'l' => one_of(name, &[44]),
            _ => None,
        },
        14 => match name[0] {
            b'a' => one_of(name, &[15]),
            b'c' => one_of(name, &[28]),
            _ => None,
        },
        15 => match name[0] {
            b'a' => one_of(name, &[16, 17]),
            _ => None,
        },
        16 => match name[0] {
            b'c' => one_of(name, &[26, 27, 29]),
            b'w' => one_of(name, &[61]),
            _ => None,
        },
        17 => match name[0] {
            b'i' => one_of(name, &[40]),
            b't' => one_of(name, &[57]),
            _ => None,
        },
        18 => match name[0] {
            b'p' => one_of(name, &[48]),
            _ => None,
        },
        19 => match name[0] {
            b'c' => one_of(name, &[25]),
            b'i' => one_of(name, &[43]),
            b'p' => one_of(name, &[49]),
            _ => None,
        },
        25 => match name[0] {
            b's' => one_of(name, &[56]),
            _ => None,
        },
        27 => match name[0] {
            b'a' => one_of(name, &[20]),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests;

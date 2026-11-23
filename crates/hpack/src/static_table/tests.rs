// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;

/// The first index of `name`, by linear search, as `protocol-hpack`'s `STATIC_NAME_LOOKUP` finds it.
fn linear_name_index(name: &[u8]) -> Option<usize> {
    STATIC_TABLE
        .iter()
        .position(|entry| entry.0.as_bytes() == name)
        .map(|position| position + 1)
}

#[test]
fn has_the_rfc_entries_at_their_indices() {
    assert_eq!(STATIC_TABLE[0], (":authority", ""));
    assert_eq!(STATIC_TABLE[1], (":method", "GET"));
    assert_eq!(STATIC_TABLE[7], (":status", "200"));
    assert_eq!(STATIC_TABLE[15], ("accept-encoding", "gzip, deflate"));
    assert_eq!(STATIC_TABLE[60], ("www-authenticate", ""));
}

#[test]
fn finds_every_entry_exactly() {
    for (position, (name, value)) in STATIC_TABLE.iter().enumerate() {
        let found = find(name.as_bytes(), value.as_bytes()).unwrap();
        assert_eq!(found.exact, Some(position + 1), "{name}: {value}");
        assert_eq!(
            Some(found.name),
            linear_name_index(name.as_bytes()),
            "{name}"
        );
    }
}

#[test]
fn finds_names_with_other_values() {
    for (name, _) in STATIC_TABLE {
        let found = find(name.as_bytes(), b"other").unwrap();
        assert_eq!(
            found,
            TableMatch {
                name: linear_name_index(name.as_bytes()).unwrap(),
                exact: None
            }
        );
    }
}

#[test]
fn values_counts_each_run_of_one_name() {
    let mut index = 1;
    while index <= STATIC_TABLE_LEN {
        let run = values(index);
        let name = STATIC_TABLE[index - 1].0;
        assert!(
            STATIC_TABLE[index - 1..index - 1 + run]
                .iter()
                .all(|entry| entry.0 == name)
        );
        assert!(
            STATIC_TABLE
                .get(index - 1 + run)
                .is_none_or(|entry| entry.0 != name)
        );
        index += run;
    }
}

#[test]
fn dispatch_agrees_with_linear_search() {
    let mut names: Vec<Vec<u8>> = vec![
        Vec::new(),
        b"x".to_vec(),
        b"ETag".to_vec(),
        b"Host".to_vec(),
        b"x-forwarded-for".to_vec(),
    ];
    for (name, _) in STATIC_TABLE {
        let name = name.as_bytes();
        // The same length and first byte, but another last byte; another first byte; and one byte longer.
        let mut last = name.to_vec();
        *last.last_mut().unwrap() ^= 0x01;
        let mut first = name.to_vec();
        first[0] = b'!';
        let mut longer = name.to_vec();
        longer.push(b's');
        names.extend([
            name.to_vec(),
            last,
            first,
            longer,
            name[..name.len() - 1].to_vec(),
        ]);
    }
    for length in 0..32 {
        names.push(vec![b'a'; length]);
    }
    for name in &names {
        assert_eq!(
            name_index(name),
            linear_name_index(name),
            "{:?}",
            String::from_utf8_lossy(name)
        );
        assert_eq!(
            find(name, b"").map(|found| found.name),
            linear_name_index(name)
        );
    }
}

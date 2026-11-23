// Released under the MIT License.
// Copyright, 2018-2026, by Samuel Williams.

use super::*;

fn entries(table: &DynamicTable) -> Vec<(String, String)> {
    table
        .iter()
        .map(|(name, value)| (String::from_utf8(name.to_vec()).unwrap(), String::from_utf8(value.to_vec()).unwrap()))
        .collect()
}

fn insert(table: &mut DynamicTable, name: &str, value: &str) -> bool {
    table.insert(EntryName::Bytes(name.as_bytes()), value.as_bytes())
}

/// Checks the size against the entries, as `protocol-hpack`'s `compute_current_table_size` does.
fn check_size(table: &DynamicTable) {
    let size: usize = table.iter().map(|(name, value)| name.len() + value.len() + ENTRY_OVERHEAD).sum();
    assert_eq!(table.size(), size);
    assert!(table.size() <= table.capacity());
    assert_eq!(table.len(), table.iter().len());
    assert_eq!(table.is_empty(), table.len() == 0);
}

#[test]
fn starts_empty() {
    let table = DynamicTable::new(2048);
    assert!(table.is_empty());
    assert_eq!(table.size(), 0);
    assert_eq!(table.capacity(), 2048);
    assert_eq!(table.get(0), None);
    assert_eq!(format!("{table:?}"), "[]");
}

#[test]
fn inserts_newest_first() {
    let mut table = DynamicTable::new(2048);
    assert!(insert(&mut table, "test1", "1"));
    assert!(insert(&mut table, "test2", "2"));
    assert_eq!(table.get(0), Some((&b"test2"[..], &b"2"[..])));
    assert_eq!(table.get(1), Some((&b"test1"[..], &b"1"[..])));
    assert_eq!(table.get(2), None);
    assert_eq!(table.size(), 2 * (5 + 1 + 32));
    assert_eq!(format!("{table:?}"), r#"[("test2", "2"), ("test1", "1")]"#);
    check_size(&table);
}

#[test]
fn clones_its_entries() {
    let mut table = DynamicTable::new(2048);
    insert(&mut table, "test1", "1");
    let clone = table.clone();
    insert(&mut table, "test2", "2");
    assert_eq!(entries(&clone), [("test1".into(), "1".into())]);
    assert_eq!(table.len(), 2);
}

#[test]
fn evicts_from_the_end_to_fit() {
    // `protocol-hpack`'s "should drop headers from end of table".
    let mut table = DynamicTable::new(2048);
    insert(&mut table, "test1", &"1".repeat(1024));
    insert(&mut table, "test2", &"2".repeat(500));
    let size = table.size();
    assert!(insert(&mut table, "x-custom", &"a".repeat(2048 - size)));
    assert_eq!(table.len(), 2);
    assert_eq!(table.get(0).unwrap().0, b"x-custom");
    assert_eq!(table.get(1).unwrap().0, b"test2");
    check_size(&table);
}

#[test]
fn an_entry_larger_than_the_table_empties_it() {
    // `protocol-hpack`'s "should clear table if entry exceeds table size".
    let mut table = DynamicTable::new(2048);
    insert(&mut table, "test1", &"1".repeat(1024));
    insert(&mut table, "test2", &"2".repeat(500));
    insert(&mut table, "x-custom", "a");
    assert!(!insert(&mut table, "large", &"a".repeat(2048)));
    assert!(table.is_empty());
    check_size(&table);
}

#[test]
fn an_entry_of_exactly_the_capacity_fits() {
    let mut table = DynamicTable::new(64);
    insert(&mut table, "a", "b");
    assert!(insert(&mut table, "name", &"v".repeat(64 - 4 - 32)));
    assert_eq!(table.len(), 1);
    assert_eq!(table.size(), 64);
}

#[test]
fn shrinking_evicts_the_oldest() {
    // `protocol-hpack`'s "should shrink table if set smaller size".
    let mut table = DynamicTable::new(2048);
    insert(&mut table, "test1", &"1".repeat(1024));
    insert(&mut table, "test2", &"2".repeat(500));
    table.set_capacity(1500);
    assert_eq!(entries(&table).len(), 1);
    assert_eq!(table.get(0).unwrap().0, b"test2");
    table.set_capacity(0);
    assert!(table.is_empty());
    assert!(!insert(&mut table, "a", "b"));
    table.set_capacity(4096);
    assert!(insert(&mut table, "a", "b"));
    check_size(&table);
}

#[test]
fn an_entry_can_take_the_name_of_an_entry_it_evicts() {
    // RFC 7541 §4.4: the referenced name must survive its entry's eviction.
    let mut table = DynamicTable::new(100);
    insert(&mut table, "custom-name", "first-value");
    let size = table.size();
    assert!(table.insert(EntryName::Entry(0), "x".repeat(100 - size).as_bytes()));
    assert_eq!(entries(&table), [("custom-name".into(), "x".repeat(100 - size))]);
    check_size(&table);
}

#[test]
fn finds_the_newest_exact_and_name_matches() {
    let mut table = DynamicTable::new(4096);
    insert(&mut table, "name", "a");
    insert(&mut table, "other", "a");
    insert(&mut table, "name", "b");
    insert(&mut table, "names", "b");
    assert_eq!(table.find(b"name", b"a"), Some(TableMatch { name: 1, exact: Some(3) }));
    assert_eq!(table.find(b"name", b"b"), Some(TableMatch { name: 1, exact: Some(1) }));
    assert_eq!(table.find(b"name", b"c"), Some(TableMatch { name: 1, exact: None }));
    assert_eq!(table.find(b"nameX", b"a"), None);
    assert_eq!(table.find(b"missing", b"a"), None);
}

#[test]
fn reuses_its_buffer_in_the_steady_state() {
    let mut table = DynamicTable::new(256);
    let mut capacity = 0;
    for round in 0..10_000 {
        let value = format!("value-{}", round % 97);
        let name = if round % 3 == 0 { EntryName::Entry(0) } else { EntryName::Bytes(b"name") };
        if table.is_empty() {
            insert(&mut table, "seed", "seed");
        }
        assert!(table.insert(name, value.as_bytes()));
        assert!(table.bytes.len() <= 3 * table.capacity(), "{}", table.bytes.len());
        check_size(&table);
        if round == 1_000 {
            capacity = table.bytes.capacity();
        }
    }
    assert_eq!(table.bytes.capacity(), capacity);
    assert_eq!(table.get(0).unwrap().1, format!("value-{}", 9_999 % 97).as_bytes());
}

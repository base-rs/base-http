// Released under the MIT License.
// Copyright, 2018-2026, by Samuel Williams.
// Copyright, 2024, by Maruth Goyal.
// Copyright, 2024, by Nathan Froyd.

//! The dynamic table, [RFC 7541 §2.3.2 and §4](https://www.rfc-editor.org/rfc/rfc7541#section-4).

use crate::static_table::TableMatch;
use std::{collections::VecDeque, fmt};

/// The size an entry counts beyond its name and value, RFC 7541 §4.1.
pub(crate) const ENTRY_OVERHEAD: usize = 32;

/// The table of fields an encoder and decoder share, newest first.
///
/// Its entries live in one byte buffer, appended as they are inserted and reclaimed in bulk after eviction, so that inserting a field allocates nothing once the buffer has reached its steady size: at most three times the table's capacity, and usually less.
#[derive(Clone)]
pub struct DynamicTable {
    /// Entry bytes, oldest first. Bytes before `start` belong to evicted entries.
    bytes: Vec<u8>,
    start: usize,
    /// Newest first, so that the entry of dynamic index `i` is `entries[i]`.
    entries: VecDeque<Entry>,
    size: usize,
    capacity: usize,
}

#[derive(Clone, Copy)]
struct Entry {
    offset: usize,
    name_len: usize,
    value_len: usize,
}

impl Entry {
    fn size(&self) -> usize {
        self.name_len + self.value_len + ENTRY_OVERHEAD
    }
}

/// Where the name of a new entry comes from.
pub(crate) enum EntryName<'a> {
    Bytes(&'a [u8]),
    /// The name of the entry of this dynamic index, which inserting may evict.
    Entry(usize),
}

impl DynamicTable {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            bytes: Vec::new(),
            start: 0,
            entries: VecDeque::new(),
            size: 0,
            capacity,
        }
    }

    /// The number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the table has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The table's size as RFC 7541 §4.1 defines it: the sum of its entries' name and value lengths, plus 32 for each entry.
    pub fn size(&self) -> usize {
        self.size
    }

    /// The table's maximum size, which its size never exceeds.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The name and value of the entry with zero-based dynamic index `index`, newest first: HPACK index `62 + index`.
    pub fn get(&self, index: usize) -> Option<(&[u8], &[u8])> {
        self.entries.get(index).map(|entry| self.fields(entry))
    }

    /// The names and values of the entries, newest first.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&[u8], &[u8])> + '_ {
        self.entries.iter().map(|entry| self.fields(entry))
    }

    /// The name and value of the entry with dynamic index `index`, which must exist.
    pub(crate) fn entry(&self, index: usize) -> (&[u8], &[u8]) {
        self.fields(&self.entries[index])
    }

    fn fields(&self, entry: &Entry) -> (&[u8], &[u8]) {
        let name_end = entry.offset + entry.name_len;
        (
            &self.bytes[entry.offset..name_end],
            &self.bytes[name_end..name_end + entry.value_len],
        )
    }

    /// Finds the newest entry with `name` and `value`, and the newest with `name`.
    pub(crate) fn find(&self, name: &[u8], value: &[u8]) -> Option<TableMatch> {
        let mut found: Option<TableMatch> = None;
        for (index, entry) in self.entries.iter().enumerate() {
            if entry.name_len != name.len() {
                continue;
            }
            let (entry_name, entry_value) = self.fields(entry);
            if entry_name != name {
                continue;
            }
            let found = found.get_or_insert(TableMatch {
                name: index,
                exact: None,
            });
            if entry_value == value {
                found.exact = Some(index);
                break;
            }
        }
        found
    }

    /// Changes the maximum size, evicting the oldest entries until the table fits.
    pub(crate) fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity;
        while self.size > capacity {
            self.evict();
        }
        self.reclaim();
    }

    /// Inserts a new entry, evicting the oldest until it fits, as RFC 7541 §4.4 describes. An entry larger than the capacity empties the table instead, and is not inserted: returns whether it was.
    pub(crate) fn insert(&mut self, name: EntryName<'_>, value: &[u8]) -> bool {
        let name_len = match name {
            EntryName::Bytes(name) => name.len(),
            EntryName::Entry(index) => self.entries[index].name_len,
        };
        let size = name_len + value.len() + ENTRY_OVERHEAD;
        if size > self.capacity {
            self.clear();
            return false;
        }
        // Compact before copying: the referenced name is still live, and copying it before evicting keeps it when its own entry is evicted.
        if self.start > 0 && self.start >= self.bytes.len() - self.start {
            self.bytes.drain(..self.start);
            for entry in &mut self.entries {
                entry.offset -= self.start;
            }
            self.start = 0;
        }
        let offset = self.bytes.len();
        match name {
            EntryName::Bytes(name) => self.bytes.extend_from_slice(name),
            EntryName::Entry(index) => {
                let entry = self.entries[index];
                self.bytes
                    .extend_from_within(entry.offset..entry.offset + entry.name_len);
            }
        }
        self.bytes.extend_from_slice(value);
        while self.size + size > self.capacity {
            self.evict();
        }
        self.entries.push_front(Entry {
            offset,
            name_len,
            value_len: value.len(),
        });
        self.size += size;
        self.reclaim();
        true
    }

    fn evict(&mut self) {
        let entry = self
            .entries
            .pop_back()
            .expect("a table with a nonzero size has entries");
        self.size -= entry.size();
    }

    /// Marks the bytes of evicted entries for reuse, releasing them all when no entry remains.
    fn reclaim(&mut self) {
        match self.entries.back() {
            Some(oldest) => self.start = oldest.offset,
            None => self.clear(),
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.bytes.clear();
        self.start = 0;
        self.size = 0;
    }
}

impl fmt::Debug for DynamicTable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entries = formatter.debug_list();
        for (name, value) in self.iter() {
            entries.entry(&(
                String::from_utf8_lossy(name),
                String::from_utf8_lossy(value),
            ));
        }
        entries.finish()
    }
}

#[cfg(test)]
mod tests;

// Released under the MIT License.
// Copyright, 2018-2026, by Samuel Williams.

//! Ordered pairs of token names and opaque byte values, without interpretation
//! or section state. Protocol drivers decide which value bytes each protocol can
//! carry: HTTP/2 and HTTP/3 extensions can negotiate binary values, so the
//! semantic model does not restrict them.

use crate::syntax::is_token;
use arrayvec::{ArrayString, ArrayVec};
use smallvec::SmallVec;
use std::{error::Error, fmt, ops::Deref};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FieldError {
    InvalidName,
    Duplicate(String),
    InvalidTrailer(String),
    InvalidList(String),
}

impl fmt::Display for FieldError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidName => output.write_str("invalid HTTP field name"),
            Self::Duplicate(name) => write!(output, "duplicate singleton field: {name}"),
            Self::InvalidTrailer(name) => {
                write!(output, "field is not permitted in trailers: {name}")
            }
            Self::InvalidList(name) => write!(output, "unterminated quoted value in field: {name}"),
        }
    }
}

impl Error for FieldError {}

/// One pair with a token name, retaining the original name spelling and value
/// bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Field {
    name: NameStorage,
    value: ValueStorage,
}

// Common names and short opaque values require no separate allocation. The
// canonical representation depends only on length, preserving value equality.
#[derive(Clone, Debug, Eq, PartialEq)]
enum NameStorage {
    Inline(ArrayString<24>),
    Heap(String),
}
impl NameStorage {
    #[inline]
    fn new(value: &str) -> Self {
        match ArrayString::from(value) {
            Ok(value) => Self::Inline(value),
            Err(_) => Self::Heap(value.to_owned()),
        }
    }
}
impl Deref for NameStorage {
    type Target = str;
    fn deref(&self) -> &str {
        match self {
            Self::Inline(value) => value.as_str(),
            Self::Heap(value) => value,
        }
    }
}

// Each variant also records whether the value contains CR, LF or NUL, which
// fits in the inline variant's padding.
#[derive(Clone, Debug, Eq, PartialEq)]
enum ValueStorage {
    Inline(ArrayVec<u8, 24>, bool),
    Heap(Vec<u8>, bool),
}
impl ValueStorage {
    #[inline]
    fn new(value: &[u8]) -> Self {
        // Folding without stopping early lets the compiler vectorize the scan.
        let dangerous = value.iter().fold(false, |found, byte| {
            found | matches!(byte, b'\r' | b'\n' | 0)
        });
        let mut inline = ArrayVec::new();
        if inline.try_extend_from_slice(value).is_ok() {
            Self::Inline(inline, dangerous)
        } else {
            Self::Heap(value.to_vec(), dangerous)
        }
    }
}
impl Deref for ValueStorage {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::Inline(value, _) => value.as_slice(),
            Self::Heap(value, _) => value,
        }
    }
}

impl Field {
    #[inline]
    pub fn new(name: impl AsRef<str>, value: impl AsRef<[u8]>) -> Result<Self, FieldError> {
        let name = name.as_ref();
        if !is_token(name.as_bytes()) {
            return Err(FieldError::InvalidName);
        }
        Ok(Self {
            name: NameStorage::new(name),
            value: ValueStorage::new(value.as_ref()),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn value(&self) -> &[u8] {
        &self.value
    }
    /// Whether the value contains CR, LF or NUL, which RFC 9110 (section 5.5)
    /// calls invalid and dangerous in field values. HTTP/1 cannot carry them,
    /// and HTTP/2 and HTTP/3 forbid them without a negotiated extension. This is
    /// recorded when the field is constructed.
    pub fn has_dangerous_bytes(&self) -> bool {
        match self.value {
            ValueStorage::Inline(_, dangerous) | ValueStorage::Heap(_, dangerous) => dangerous,
        }
    }
}

/// Raw ordered pairs. Duplicate names, spelling, spacing, and opaque value
/// bytes are preserved. Interpretation belongs to an explicit header policy;
/// trailers are a separate `BodyFrame::Headers` event.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Fields {
    pub(crate) pairs: SmallVec<[Field; 1]>,
}

impl Fields {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
    /// Fields with storage for `capacity` pairs, for a driver that knows how
    /// many it will add.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            pairs: SmallVec::with_capacity(capacity),
        }
    }
    /// Reserve storage for at least `additional` more pairs.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.pairs.reserve(additional);
    }
    pub fn try_from_pairs<N: AsRef<str>, V: AsRef<[u8]>>(
        pairs: impl IntoIterator<Item = (N, V)>,
    ) -> Result<Self, FieldError> {
        let mut fields = Self::new();
        for (name, value) in pairs {
            fields.add(name, value)?;
        }
        Ok(fields)
    }
    pub fn pairs(&self) -> &[Field] {
        &self.pairs
    }
    pub fn len(&self) -> usize {
        self.pairs.len()
    }
    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }
    pub fn values<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a [u8]> + 'a {
        self.pairs
            .iter()
            .filter(move |field| field.name().eq_ignore_ascii_case(name))
            .map(Field::value)
    }
    pub fn contains(&self, name: &str) -> bool {
        self.pairs
            .iter()
            .any(|field| field.name().eq_ignore_ascii_case(name))
    }
    #[inline]
    pub fn add(
        &mut self,
        name: impl AsRef<str>,
        value: impl AsRef<[u8]>,
    ) -> Result<(), FieldError> {
        self.append(Field::new(name, value)?)
    }
    #[inline]
    pub fn append(&mut self, field: Field) -> Result<(), FieldError> {
        self.pairs.push(field);
        Ok(())
    }
    pub fn set(
        &mut self,
        name: impl AsRef<str>,
        value: impl AsRef<[u8]>,
    ) -> Result<(), FieldError> {
        let field = Field::new(name, value)?;
        self.remove(field.name());
        self.pairs.push(field);
        Ok(())
    }
    pub fn remove(&mut self, name: &str) -> usize {
        let original = self.len();
        self.retain(|field| !field.name().eq_ignore_ascii_case(name));
        original - self.len()
    }
    pub fn retain(&mut self, mut keep: impl FnMut(&Field) -> bool) {
        self.pairs.retain(|field| keep(field));
    }
    pub fn clear(&mut self) {
        self.pairs.clear();
    }
    pub fn split_off(&mut self, at: usize) -> Self {
        Self {
            pairs: self.pairs.drain(at..).collect(),
        }
    }
}
impl IntoIterator for Fields {
    type Item = Field;
    type IntoIter = smallvec::IntoIter<[Field; 1]>;
    fn into_iter(self) -> Self::IntoIter {
        self.pairs.into_iter()
    }
}
impl Extend<Field> for Fields {
    fn extend<T: IntoIterator<Item = Field>>(&mut self, iter: T) {
        self.pairs.extend(iter);
    }
}
impl FromIterator<Field> for Fields {
    fn from_iter<T: IntoIterator<Item = Field>>(iter: T) -> Self {
        Self {
            pairs: iter.into_iter().collect(),
        }
    }
}

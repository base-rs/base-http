// Released under the MIT License.
// Copyright, 2018-2026, by Samuel Williams.

//! Ordered byte-valued fields, a trailer split, and lazy combination policies.
//!
//! Raw inspection takes `&self`; interpreted lookup takes `&mut self` to build
//! the index without interior mutability. Indexing never rewrites raw pairs.
//! Combination is separate from field-specific parsing (dates, ranges, etc.).

use crate::syntax::{is_token, trim_ows};
use arrayvec::{ArrayString, ArrayVec};
use smallvec::SmallVec;
use std::{collections::HashMap, error::Error, fmt, ops::Deref};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FieldError {
    InvalidName,
    InvalidValue,
    Duplicate(String),
    InvalidTrailer(String),
    InvalidList(String),
}

impl fmt::Display for FieldError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidName => output.write_str("invalid HTTP field name"),
            Self::InvalidValue => output.write_str("invalid HTTP field value"),
            Self::Duplicate(name) => write!(output, "duplicate singleton field: {name}"),
            Self::InvalidTrailer(name) => {
                write!(output, "field is not permitted in trailers: {name}")
            }
            Self::InvalidList(name) => write!(output, "unterminated quoted value in field: {name}"),
        }
    }
}

impl Error for FieldError {}

/// One syntax-checked pair, retaining the original name spelling and value bytes.
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum ValueStorage {
    Inline(ArrayVec<u8, 24>),
    Heap(Vec<u8>),
}
impl ValueStorage {
    fn new(value: &[u8]) -> Self {
        let mut inline = ArrayVec::new();
        if inline.try_extend_from_slice(value).is_ok() {
            Self::Inline(inline)
        } else {
            Self::Heap(value.to_vec())
        }
    }
}
impl Deref for ValueStorage {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::Inline(value) => value.as_slice(),
            Self::Heap(value) => value,
        }
    }
}

impl Field {
    pub fn new(name: impl AsRef<str>, value: impl AsRef<[u8]>) -> Result<Self, FieldError> {
        let name = name.as_ref();
        if !is_token(name.as_bytes()) {
            return Err(FieldError::InvalidName);
        }
        let value = value.as_ref();
        if !value
            .iter()
            .all(|byte| *byte == b'\t' || (*byte >= b' ' && *byte != 0x7f))
        {
            return Err(FieldError::InvalidValue);
        }
        Ok(Self {
            name: NameStorage::new(name),
            value: ValueStorage::new(value),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

/// How repeated raw values contribute to the interpreted value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Combination {
    Singleton,
    Comma,
    LowercaseComma,
    Cookie,
    Separate,
    Last,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FieldPolicy {
    pub combination: Combination,
    pub trailer: bool,
}

impl FieldPolicy {
    pub const fn new(combination: Combination, trailer: bool) -> Self {
        Self {
            combination,
            trailer,
        }
    }
}

/// Combination/trailer defaults following Socketry, with case-insensitive
/// overrides. Repeated credentials are rejected rather than concatenated.
#[derive(Clone, Debug, Default)]
pub struct Policy {
    overrides: Option<Box<HashMap<String, FieldPolicy>>>,
}

impl Policy {
    pub fn set(&mut self, name: &str, policy: FieldPolicy) -> Result<(), FieldError> {
        if !is_token(name.as_bytes()) {
            return Err(FieldError::InvalidName);
        }
        self.overrides
            .get_or_insert_with(Default::default)
            .insert(name.to_ascii_lowercase(), policy);
        Ok(())
    }

    pub fn get(&self, name: &str) -> FieldPolicy {
        let name = name.to_ascii_lowercase();
        if let Some(policy) = self
            .overrides
            .as_ref()
            .and_then(|overrides| overrides.get(&name))
        {
            return *policy;
        }
        use Combination::*;
        match name.as_str() {
            "content-disposition"
            | "content-length"
            | "content-type"
            | "expect"
            | "from"
            | "host"
            | "location"
            | "max-forwards"
            | "referer"
            | "retry-after"
            | "server"
            | "user-agent"
            | "if-range"
            | "content-range" => FieldPolicy::new(Singleton, false),
            "connection" => FieldPolicy::new(LowercaseComma, false),
            "cookie" => FieldPolicy::new(Cookie, false),
            "set-cookie" | "www-authenticate" | "proxy-authenticate" => {
                FieldPolicy::new(Separate, false)
            }
            "authorization" | "proxy-authorization" => FieldPolicy::new(Singleton, false),
            "date"
            | "expires"
            | "last-modified"
            | "if-modified-since"
            | "if-unmodified-since"
            | "etag" => FieldPolicy::new(Last, true),
            "range" | "transfer-encoding" | "trailer" | "upgrade" | "cache-control" | "te"
            | "vary" | "priority" | "via" | "x-forwarded-for" | "if-match" | "if-none-match"
            | "accept" | "accept-ranges" | "accept-charset" | "accept-encoding"
            | "accept-language" | "content-encoding" => FieldPolicy::new(Comma, false),
            _ => FieldPolicy::new(Comma, true),
        }
    }
}

/// Interpreted components. Raw spelling/spacing always remains in `Fields::pairs`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FieldValue {
    combination: Combination,
    parts: Vec<Vec<u8>>,
}

impl FieldValue {
    pub fn combination(&self) -> Combination {
        self.combination
    }
    pub fn parts(&self) -> &[Vec<u8>] {
        &self.parts
    }

    /// A single component, when present. This does not join a list or cookies.
    pub fn single(&self) -> Option<&[u8]> {
        if self.parts.len() == 1 {
            Some(&self.parts[0])
        } else {
            None
        }
    }

    /// Combine into one value when policy permits. Separate fields such as
    /// `set-cookie` must be emitted individually and return `None`.
    pub fn combined(&self) -> Option<Vec<u8>> {
        let separator: &[u8] = match self.combination {
            Combination::Separate => return None,
            Combination::Cookie => b"; ",
            _ => b",",
        };
        Some(self.parts.join(separator))
    }

    fn parse(name: &str, value: &[u8], combination: Combination) -> Result<Self, FieldError> {
        let mut parts = match combination {
            Combination::Comma | Combination::LowercaseComma => split_list(name, value)?,
            _ => vec![value.to_vec()],
        };
        if combination == Combination::LowercaseComma {
            for part in &mut parts {
                part.make_ascii_lowercase();
            }
        }
        Ok(Self { combination, parts })
    }
}

fn split_list(name: &str, value: &[u8]) -> Result<Vec<Vec<u8>>, FieldError> {
    let mut parts = Vec::new();
    let (mut start, mut quoted, mut escaped) = (0, false, false);
    for (position, byte) in value.iter().enumerate() {
        if escaped {
            escaped = false;
        } else if quoted && *byte == b'\\' {
            escaped = true;
        } else if *byte == b'"' {
            quoted = !quoted;
        } else if !quoted && *byte == b',' {
            let part = trim_ows(&value[start..position]);
            if !part.is_empty() {
                parts.push(part.to_vec());
            }
            start = position + 1;
        }
    }
    if quoted {
        return Err(FieldError::InvalidList(name.to_owned()));
    }
    let part = trim_ows(&value[start..]);
    if !part.is_empty() {
        parts.push(part.to_vec());
    }
    Ok(parts)
}

pub type Index = HashMap<String, FieldValue>;

#[derive(Clone, Debug, Default)]
pub struct Fields {
    pairs: SmallVec<[Field; 1]>,
    tail: Option<usize>,
    indexed: Option<Box<Index>>,
    policy: Policy,
}

impl Fields {
    pub fn new() -> Self {
        Self::default()
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
    pub fn with_policy(policy: Policy) -> Self {
        Self {
            policy,
            ..Self::default()
        }
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
    pub fn tail(&self) -> Option<usize> {
        self.tail
    }
    pub fn has_trailers(&self) -> bool {
        self.tail.is_some()
    }
    pub fn headers(&self) -> &[Field] {
        &self.pairs[..self.tail.unwrap_or(self.pairs.len())]
    }
    pub fn trailers(&self) -> &[Field] {
        &self.pairs[self.tail.unwrap_or(self.pairs.len())..]
    }
    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    /// Changing policy invalidates interpreted values; validation is deferred.
    pub fn set_policy(&mut self, policy: Policy) {
        self.policy = policy;
        self.indexed = None;
    }

    pub fn mark_trailers(&mut self) {
        self.tail.get_or_insert(self.pairs.len());
    }

    pub fn values<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a [u8]> + 'a {
        self.pairs
            .iter()
            .filter(move |field| field.name.eq_ignore_ascii_case(name))
            .map(Field::value)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.pairs
            .iter()
            .any(|field| field.name.eq_ignore_ascii_case(name))
    }

    pub fn add(
        &mut self,
        name: impl AsRef<str>,
        value: impl AsRef<[u8]>,
    ) -> Result<(), FieldError> {
        self.append(Field::new(name, value)?)
    }

    pub fn append(&mut self, field: Field) -> Result<(), FieldError> {
        self.check_trailer(&field)?;
        if let Some(index) = &mut self.indexed {
            merge_into(index, &field, self.policy.get(&field.name))?;
        }
        self.pairs.push(field);
        Ok(())
    }

    /// Replace all matching pairs and append in the current section. Validation
    /// failures leave the original collection and its cached index unchanged.
    pub fn set(
        &mut self,
        name: impl AsRef<str>,
        value: impl AsRef<[u8]>,
    ) -> Result<(), FieldError> {
        let field = Field::new(name, value)?;
        self.check_trailer(&field)?;
        let parsed = if self.indexed.is_some() {
            Some(FieldValue::parse(
                &field.name,
                &field.value,
                self.policy.get(&field.name).combination,
            )?)
        } else {
            None
        };
        self.remove(&field.name);
        if let Some(parsed) = parsed {
            self.indexed
                .as_mut()
                .unwrap()
                .insert(field.name.to_ascii_lowercase(), parsed);
        }
        self.pairs.push(field);
        Ok(())
    }

    /// Remove matching pairs in both sections, preserving the trailer boundary.
    pub fn remove(&mut self, name: &str) -> usize {
        let mut position = 0;
        let mut removed_headers = 0;
        let original_length = self.pairs.len();
        self.pairs.retain(|field| {
            let keep = !field.name.eq_ignore_ascii_case(name);
            if !keep && position < self.tail.unwrap_or(original_length) {
                removed_headers += 1;
            }
            position += 1;
            keep
        });
        if let Some(tail) = &mut self.tail {
            *tail -= removed_headers;
        }
        if let Some(index) = &mut self.indexed {
            index.remove(&name.to_ascii_lowercase());
        }
        original_length - self.pairs.len()
    }

    pub fn clear(&mut self) {
        self.pairs.clear();
        self.tail = None;
        self.indexed = None;
    }

    /// Include trailers among initial fields and remove the `trailer` declaration.
    pub fn flatten_trailers(&mut self) {
        if self.tail.is_some() {
            self.remove("trailer");
            self.tail = None;
        }
    }

    pub fn get(&mut self, name: &str) -> Result<Option<&FieldValue>, FieldError> {
        Ok(self.index()?.get(&name.to_ascii_lowercase()))
    }

    /// Validate and cache the whole semantic index. Failed builds are not cached,
    /// so removing an offending pair or changing policy can repair the collection.
    pub fn index(&mut self) -> Result<&Index, FieldError> {
        if self.indexed.is_none() {
            let mut index = Index::new();
            for (position, field) in self.pairs.iter().enumerate() {
                if self.tail.is_some_and(|tail| position >= tail) {
                    self.check_trailer(field)?;
                }
                merge_into(&mut index, field, self.policy.get(&field.name))?;
            }
            self.indexed = Some(Box::new(index));
        }
        Ok(self.indexed.as_ref().unwrap())
    }

    fn check_trailer(&self, field: &Field) -> Result<(), FieldError> {
        if self.tail.is_some() && !self.policy.get(&field.name).trailer {
            return Err(FieldError::InvalidTrailer(field.name.to_ascii_lowercase()));
        }
        Ok(())
    }
}

fn merge_into(index: &mut Index, field: &Field, policy: FieldPolicy) -> Result<(), FieldError> {
    let name = field.name.to_ascii_lowercase();
    if policy.combination == Combination::Singleton && index.contains_key(&name) {
        return Err(FieldError::Duplicate(name));
    }
    let parsed = FieldValue::parse(&name, &field.value, policy.combination)?;
    match index.get_mut(&name) {
        Some(current) if policy.combination != Combination::Last => {
            current.parts.extend(parsed.parts)
        }
        _ => {
            index.insert(name, parsed);
        }
    }
    Ok(())
}

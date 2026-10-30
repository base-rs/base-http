// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Explicit HTTP interpretation of raw fields. Other application protocols can
//! interpret `Fields` independently or configure their own combination policy.
use crate::{
    Field, FieldError, Fields,
    syntax::{is_token, trim_ows},
};
use std::collections::HashMap;

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
    // Most messages use only the built-in policy. Keep that case pointer-sized;
    // allocate the override table itself only when a caller supplies overrides.
    #[allow(clippy::box_collection)]
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

/// An owning HTTP header interpreter with lazy lookup and explicit trailer
/// accumulation. Initial and trailing sections remain distinguishable; raw
/// `Fields` never carries an interpreter or a hidden trailer marker.
#[derive(Clone, Debug, Default)]
pub struct HttpHeaders {
    fields: Fields,
    tail: Option<usize>,
    indexed: Option<Box<Index>>,
    policy: Policy,
}

impl HttpHeaders {
    pub fn new(fields: Fields) -> Self {
        Self {
            fields,
            ..Self::default()
        }
    }
    pub fn try_from_pairs<N: AsRef<str>, V: AsRef<[u8]>>(
        pairs: impl IntoIterator<Item = (N, V)>,
    ) -> Result<Self, FieldError> {
        Ok(Self::new(Fields::try_from_pairs(pairs)?))
    }
    pub fn with_policy(fields: Fields, policy: Policy) -> Self {
        Self {
            fields,
            policy,
            ..Self::default()
        }
    }
    /// Recover the initial and trailing sections without losing their provenance.
    pub fn into_parts(mut self) -> (Fields, Option<Fields>) {
        let trailers = self.tail.map(|tail| self.fields.split_off(tail));
        (self.fields, trailers)
    }
    pub fn pairs(&self) -> &[Field] {
        &self.fields.pairs
    }
    pub fn len(&self) -> usize {
        self.fields.pairs.len()
    }
    pub fn is_empty(&self) -> bool {
        self.fields.pairs.is_empty()
    }
    pub fn tail(&self) -> Option<usize> {
        self.tail
    }
    pub fn has_trailers(&self) -> bool {
        self.tail.is_some()
    }
    pub fn headers(&self) -> &[Field] {
        &self.fields.pairs[..self.tail.unwrap_or(self.fields.pairs.len())]
    }
    pub fn trailers(&self) -> &[Field] {
        &self.fields.pairs[self.tail.unwrap_or(self.fields.pairs.len())..]
    }
    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    /// Changing policy invalidates interpreted values; validation is deferred.
    pub fn set_policy(&mut self, policy: Policy) {
        self.policy = policy;
        self.indexed = None;
    }

    /// Explicitly append a received trailing section. Failed validation leaves
    /// this interpreter unchanged, including its cached index and section split.
    pub fn append_trailers(&mut self, fields: Fields) -> Result<(), FieldError> {
        for field in fields.pairs() {
            if !self.policy.get(field.name()).trailer {
                return Err(FieldError::InvalidTrailer(
                    field.name().to_ascii_lowercase(),
                ));
            }
        }
        // Validate combinations before moving any pairs into the collection.
        if let Some(index) = &self.indexed {
            let mut next = (**index).clone();
            for field in fields.pairs() {
                merge_into(&mut next, field, self.policy.get(field.name()))?;
            }
            self.indexed = Some(Box::new(next));
        }
        self.tail.get_or_insert(self.fields.len());
        self.fields.extend(fields);
        Ok(())
    }

    pub fn values<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a [u8]> + 'a {
        self.fields
            .pairs
            .iter()
            .filter(move |field| field.name().eq_ignore_ascii_case(name))
            .map(Field::value)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.fields
            .pairs
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
        self.check_trailer(&field)?;
        if let Some(index) = &mut self.indexed {
            merge_into(index, &field, self.policy.get(field.name()))?;
        }
        self.fields.pairs.push(field);
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
                field.name(),
                field.value(),
                self.policy.get(field.name()).combination,
            )?)
        } else {
            None
        };
        self.remove(field.name());
        if let Some(parsed) = parsed {
            self.indexed
                .as_mut()
                .unwrap()
                .insert(field.name().to_ascii_lowercase(), parsed);
        }
        self.fields.pairs.push(field);
        Ok(())
    }

    /// Remove matching pairs in both sections, preserving the trailer boundary.
    pub fn remove(&mut self, name: &str) -> usize {
        let mut position = 0;
        let mut removed_headers = 0;
        let original_length = self.fields.pairs.len();
        self.fields.pairs.retain(|field| {
            let keep = !field.name().eq_ignore_ascii_case(name);
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
        original_length - self.fields.pairs.len()
    }

    pub fn clear(&mut self) {
        self.fields.pairs.clear();
        self.tail = None;
        self.indexed = None;
    }

    pub fn get(&mut self, name: &str) -> Result<Option<&FieldValue>, FieldError> {
        Ok(self.index()?.get(&name.to_ascii_lowercase()))
    }

    /// Validate and cache the whole semantic index. Failed builds are not cached,
    /// so removing an offending pair or changing policy can repair the collection.
    pub fn index(&mut self) -> Result<&Index, FieldError> {
        if self.indexed.is_none() {
            let mut index = Index::new();
            for (position, field) in self.fields.pairs.iter().enumerate() {
                if self.tail.is_some_and(|tail| position >= tail) {
                    self.check_trailer(field)?;
                }
                merge_into(&mut index, field, self.policy.get(field.name()))?;
            }
            self.indexed = Some(Box::new(index));
        }
        Ok(self.indexed.as_ref().unwrap())
    }

    fn check_trailer(&self, field: &Field) -> Result<(), FieldError> {
        if self.tail.is_some() && !self.policy.get(field.name()).trailer {
            return Err(FieldError::InvalidTrailer(
                field.name().to_ascii_lowercase(),
            ));
        }
        Ok(())
    }
}

fn merge_into(index: &mut Index, field: &Field, policy: FieldPolicy) -> Result<(), FieldError> {
    let name = field.name().to_ascii_lowercase();
    if policy.combination == Combination::Singleton && index.contains_key(&name) {
        return Err(FieldError::Duplicate(name));
    }
    let parsed = FieldValue::parse(&name, field.value(), policy.combination)?;
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

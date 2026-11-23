// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// How a field may use the dynamic table, after the literal representations of [RFC 7541 §6.2](https://www.rfc-editor.org/rfc/rfc7541#section-6.2).
///
/// The encoder takes one for each field; the decoder reports the one each field was received with, so that an intermediary can forward a field as it arrived. A field the decoder found in a table, through an indexed representation, reports [`Indexing::Incremental`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Indexing {
    /// The field may be found in the tables or added to the dynamic table, RFC 7541 §6.2.1.
    #[default]
    Incremental,
    /// The field may be found in the tables, but is not added to the dynamic table, RFC 7541 §6.2.2. Suits values unlikely to repeat.
    NotIndexed,
    /// The field is sensitive: it is always sent as a literal value, never added to a table, and an intermediary must forward it the same way, RFC 7541 §6.2.3 and §7.1.3. Its name may still come from a table.
    NeverIndexed,
}

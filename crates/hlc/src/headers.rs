//! Carriage of a stamp on Kafka record headers.

use crate::stamp::Hlc;

/// The Kafka record header key that carries a hybrid logical clock stamp.
///
/// The key is lower case, and a reader matches it byte for byte. Kafka header
/// keys are case-sensitive, and this key belongs to Krabka, so there is no
/// second spelling to accept.
pub const HLC_HEADER: &str = "krabka.hlc";

/// The stamp on a record, or `None` when the record carries none.
///
/// Each header is a key plus a raw byte value, so this function works with any
/// producer or consumer header type. The caller converts its own `Header` into
/// a `(key, value)` pair at the edge.
///
/// This function returns `None` for an absent header, for a value of the wrong
/// length, and for a value it cannot read. It reports no error, because a
/// consumer that cannot read the producer's stamp must still be able to apply
/// the record. Use [`Hlc::decode`] where the caller wants the reason.
///
/// A record can carry the same header key more than once, and Kafka's own
/// `Headers.lastHeader` reads the last one. This function does the same.
#[must_use]
pub fn extract_from_headers<'a, I, V>(headers: I) -> Option<Hlc>
where
    I: IntoIterator<Item = (&'a str, V)>,
    V: AsRef<[u8]>,
{
    headers
        .into_iter()
        .filter(|(key, _)| *key == HLC_HEADER)
        .map(|(_, value)| value)
        .last()
        .and_then(|value| Hlc::decode(value.as_ref()).ok())
}

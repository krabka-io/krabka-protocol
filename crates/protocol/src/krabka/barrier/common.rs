//! Cut types and codec helpers that more than one barrier message uses.
//!
//! A cut is the set of offsets that one epoch's barrier markers took.
//! [`BarrierCutTopic`] holds those offsets for one topic, and
//! [`BarrierMissingPartition`] names a partition that got no marker.
//! [`TriggerBarrierResponse`](super::trigger_barrier::TriggerBarrierResponse)
//! returns the cut it made. [`ListBarrierCutsResponse`](super::list_barrier_cuts::ListBarrierCutsResponse)
//! returns the cuts the coordinator published before.

use bytes::{Buf, BufMut};

use crate::{
    Decode, Encode, ProtocolError, UnknownTaggedFields,
    primitives::{
        array::{array_len_prefix_len, get_array_len, put_array_len},
        fixed::{get_i32, get_i64, put_i32, put_i64},
        string_bytes::{compact_string_len, get_compact_string_owned, put_compact_string},
    },
    tagged_fields::{WriteTaggedFields, read_tagged_fields, tagged_fields_len},
};

/// Every barrier message is flexible from version 0, so each array-length
/// prefix takes the compact form from KIP-482.
pub(crate) const FLEXIBLE: bool = true;

/// Status of a cut that reached every partition of the group.
pub const CUT_STATUS_COMPLETE: i8 = 0;

/// Status of a cut that missed at least one partition. The `missing` array of
/// the response names those partitions.
pub const CUT_STATUS_PARTIAL: i8 = 1;

/// Encoded width of a `bool` field.
pub(crate) const BOOL_LEN: usize = 1;
/// Encoded width of an `i8` field.
pub(crate) const I8_LEN: usize = 1;
/// Encoded width of an `i16` field.
pub(crate) const I16_LEN: usize = 2;
/// Encoded width of an `i32` field.
pub(crate) const I32_LEN: usize = 4;
/// Encoded width of an `i64` field.
pub(crate) const I64_LEN: usize = 8;

/// Rejects a version outside the range that a message type supports.
///
/// `min` and `max` come from the `MIN_VERSION` and `MAX_VERSION` constants of
/// the message module that calls this function.
pub(crate) fn check_version(
    api_key: i16,
    version: i16,
    min: i16,
    max: i16,
) -> Result<(), ProtocolError> {
    if (min..=max).contains(&version) {
        Ok(())
    } else {
        Err(ProtocolError::UnsupportedVersion { api_key, version })
    }
}

/// Writes a compact array of nested structs, and each struct after it.
pub(crate) fn encode_array<T: Encode, B: BufMut>(
    buf: &mut B,
    items: &[T],
    version: i16,
) -> Result<(), ProtocolError> {
    put_array_len(buf, items.len(), FLEXIBLE);
    for item in items {
        item.encode(buf, version)?;
    }
    Ok(())
}

/// Reads a compact array of nested structs.
pub(crate) fn decode_array<T, B>(buf: &mut B, version: i16) -> Result<Vec<T>, ProtocolError>
where
    T: for<'de> Decode<'de>,
    B: Buf,
{
    let count = get_array_len(buf, FLEXIBLE)?;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(T::decode(buf, version)?);
    }
    Ok(items)
}

/// Byte count that [`encode_array`] writes.
pub(crate) fn array_len<T: Encode>(items: &[T], version: i16) -> usize {
    let body: usize = items.iter().map(|item| item.encoded_len(version)).sum();
    array_len_prefix_len(items.len(), FLEXIBLE) + body
}

/// Writes a compact array of compact strings.
pub(crate) fn encode_string_array<B: BufMut>(buf: &mut B, items: &[String]) {
    put_array_len(buf, items.len(), FLEXIBLE);
    for item in items {
        put_compact_string(buf, item);
    }
}

/// Reads a compact array of compact strings.
pub(crate) fn decode_string_array<B: Buf>(buf: &mut B) -> Result<Vec<String>, ProtocolError> {
    let count = get_array_len(buf, FLEXIBLE)?;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(get_compact_string_owned(buf)?);
    }
    Ok(items)
}

/// Byte count that [`encode_string_array`] writes.
pub(crate) fn string_array_len(items: &[String]) -> usize {
    let body: usize = items.iter().map(|item| compact_string_len(item)).sum();
    array_len_prefix_len(items.len(), FLEXIBLE) + body
}

/// Writes the tagged-fields trailer. A barrier message declares no known tag,
/// so the trailer holds only the tags that this build did not recognize.
pub(crate) fn write_unknown_tagged_fields<B: BufMut>(buf: &mut B, unknown: &UnknownTaggedFields) {
    WriteTaggedFields::new().write(buf, unknown);
}

/// Reads the tagged-fields trailer and keeps every tag verbatim.
pub(crate) fn read_unknown_tagged_fields<B: Buf>(
    buf: &mut B,
) -> Result<UnknownTaggedFields, ProtocolError> {
    read_tagged_fields(buf, |_tag, _payload| Ok(false))
}

/// Byte count that [`write_unknown_tagged_fields`] writes.
pub(crate) fn unknown_tagged_fields_len(unknown: &UnknownTaggedFields) -> usize {
    tagged_fields_len(&[], unknown)
}

/// One partition of a cut, and the offset that its barrier marker took.
///
/// Records below `offset` are before the cut. Records at `offset` and above are
/// after it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BarrierCutPartition {
    /// Partition index within the topic.
    pub partition: i32,
    /// Offset of the epoch's barrier marker in this partition.
    pub offset: i64,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for BarrierCutPartition {
    fn encode<B: BufMut>(&self, buf: &mut B, _version: i16) -> Result<(), ProtocolError> {
        put_i32(buf, self.partition);
        put_i64(buf, self.offset);
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        I32_LEN + I64_LEN + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for BarrierCutPartition {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<Self, ProtocolError> {
        Ok(Self {
            partition: get_i32(buf)?,
            offset: get_i64(buf)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

/// The partitions of one topic that a cut covers.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BarrierCutTopic {
    /// Topic name.
    pub topic: String,
    /// Cut offset for each partition of the topic that got a marker.
    pub partitions: Vec<BarrierCutPartition>,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for BarrierCutTopic {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        put_compact_string(buf, &self.topic);
        encode_array(buf, &self.partitions, version)?;
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        compact_string_len(&self.topic)
            + array_len(&self.partitions, version)
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for BarrierCutTopic {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        Ok(Self {
            topic: get_compact_string_owned(buf)?,
            partitions: decode_array(buf, version)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

/// A partition of the frozen target set that got no barrier marker.
///
/// A partial cut names every such partition, so one record tells a reader which
/// partitions to skip for that epoch.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BarrierMissingPartition {
    /// Topic name.
    pub topic: String,
    /// Partition index within the topic.
    pub partition: i32,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for BarrierMissingPartition {
    fn encode<B: BufMut>(&self, buf: &mut B, _version: i16) -> Result<(), ProtocolError> {
        put_compact_string(buf, &self.topic);
        put_i32(buf, self.partition);
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        compact_string_len(&self.topic)
            + I32_LEN
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for BarrierMissingPartition {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<Self, ProtocolError> {
        Ok(Self {
            topic: get_compact_string_owned(buf)?,
            partition: get_i32(buf)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

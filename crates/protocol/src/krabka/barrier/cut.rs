//! Cut types that more than one barrier message uses.
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
    krabka::common::{
        I32_LEN, I64_LEN, array_len, decode_array, encode_array, read_unknown_tagged_fields,
        unknown_tagged_fields_len, write_unknown_tagged_fields,
    },
    primitives::{
        fixed::{get_i32, get_i64, put_i32, put_i64},
        string_bytes::{compact_string_len, get_compact_string_owned, put_compact_string},
    },
};

/// Status of a cut that reached every partition of the group.
pub const CUT_STATUS_COMPLETE: i8 = 0;

/// Status of a cut that missed at least one partition. The `missing` array of
/// the response names those partitions.
pub const CUT_STATUS_PARTIAL: i8 = 1;

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

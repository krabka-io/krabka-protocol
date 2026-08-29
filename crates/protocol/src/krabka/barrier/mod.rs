//! Control-plane messages for cross-topic barrier groups.
//!
//! A barrier group names a set of topics. A broker coordinator puts an
//! epoch-stamped marker into every partition of those topics. The offsets that
//! those markers take are a cut. Two replays of one cut read the same records.
//!
//! # APIs
//!
//! | Api key | Module | Purpose |
//! | --- | --- | --- |
//! | 1010 | [`alter_barrier_groups`] | create, update, and delete barrier groups |
//! | 1011 | [`describe_barrier_groups`] | read group definitions |
//! | 1012 | [`trigger_barrier`] | make a cut on demand |
//! | 1013 | [`list_barrier_cuts`] | read published cuts |
//! | 1014 | [`write_barrier_markers`] | inter-broker fan-out of the markers |
//!
//! The keys sit in the krabka-private range at 1000 and above, as
//! [`crate::krabka`] describes.
//!
//! # Framing
//!
//! Version 0 is the only version of each message, and it is flexible under
//! KIP-482. So every codec here writes compact strings, compact arrays, and a
//! tagged-fields trailer. No message declares a known tag. Each codec keeps an
//! unknown tagged field verbatim and writes it back in ascending tag order, as
//! the generated Kafka codecs do.
//!
//! # Read path for a client
//!
//! [`list_barrier_cuts`] is the RPC read path. A client in another language does
//! not need it. The coordinator also publishes each cut to the `__barrier_state`
//! topic, and any Kafka consumer can read that topic.

pub mod alter_barrier_groups;
pub mod cut;
pub mod describe_barrier_groups;
pub mod list_barrier_cuts;
#[cfg(test)]
mod test_support;
pub mod trigger_barrier;
pub mod write_barrier_markers;

pub use self::{
    alter_barrier_groups::{
        AlterBarrierGroupResult, AlterBarrierGroupsRequest, AlterBarrierGroupsResponse,
        AlterableBarrierGroup,
    },
    cut::{
        BarrierCutPartition, BarrierCutTopic, BarrierMissingPartition, CUT_STATUS_COMPLETE,
        CUT_STATUS_PARTIAL,
    },
    describe_barrier_groups::{
        DescribeBarrierGroupsRequest, DescribeBarrierGroupsResponse, DescribedBarrierGroup,
    },
    list_barrier_cuts::{BarrierCut, ListBarrierCutsRequest, ListBarrierCutsResponse},
    trigger_barrier::{TriggerBarrierRequest, TriggerBarrierResponse},
    write_barrier_markers::{
        WritableBarrierPartition, WritableBarrierTopic, WriteBarrierMarkersRequest,
        WriteBarrierMarkersResponse, WrittenBarrierPartition, WrittenBarrierTopic,
    },
};

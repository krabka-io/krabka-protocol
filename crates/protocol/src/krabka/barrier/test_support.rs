//! Sample cut values for the barrier codec tests.

use crate::{
    UnknownTaggedFields,
    krabka::{
        barrier::cut::{BarrierCutPartition, BarrierCutTopic, BarrierMissingPartition},
        test_support::sample_tagged_fields,
    },
};

/// A cut over two topics, one of them with two partitions.
pub(crate) fn sample_cut_topics() -> Vec<BarrierCutTopic> {
    vec![
        BarrierCutTopic {
            topic: "orders".to_string(),
            partitions: vec![
                BarrierCutPartition {
                    partition: 0,
                    offset: 1_024,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
                BarrierCutPartition {
                    partition: 1,
                    offset: 2_048,
                    unknown_tagged_fields: sample_tagged_fields(),
                },
            ],
            unknown_tagged_fields: sample_tagged_fields(),
        },
        BarrierCutTopic {
            topic: "payments".to_string(),
            partitions: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        },
    ]
}

/// One partition that took no marker.
pub(crate) fn sample_missing_partitions() -> Vec<BarrierMissingPartition> {
    vec![BarrierMissingPartition {
        topic: "payments".to_string(),
        partition: 4,
        unknown_tagged_fields: sample_tagged_fields(),
    }]
}

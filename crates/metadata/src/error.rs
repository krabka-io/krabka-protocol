use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetadataError {
    #[error("topic '{0}' already exists")]
    TopicExists(String),

    #[error("unknown topic '{0}'")]
    UnknownTopic(String),

    #[error("invalid partition {partition} on topic '{topic}'")]
    InvalidPartition { topic: String, partition: i32 },

    #[error("invalid record: {0}")]
    InvalidRecord(&'static str),

    /// A `V1TopicFreeze` record carried an empty scope. A literal scope names
    /// one topic and a prefix scope names a namespace, so neither can be
    /// empty. An empty prefix would cover every topic in the cluster.
    #[error("topic freeze scope is empty")]
    EmptyFreezeScope,

    /// A `V1BreakGlassProposal` record replaced, reordered, or dropped an
    /// approval already stored for the proposal. Two approvers who read the
    /// same image each submit the list they read plus their own entry, so
    /// only an append keeps both.
    #[error("break-glass proposal {0} does not extend the stored approvals")]
    BreakGlassApprovalsNotAnExtension(Uuid),

    /// A `V1BreakGlassProposal` record changed a proposal whose approval is
    /// already spent. One approval authorizes one transition.
    #[error("break-glass proposal {0} is already consumed")]
    BreakGlassProposalConsumed(Uuid),

    /// A `V1BreakGlassProposal` record changed a proposal that the proposer
    /// withdrew.
    #[error("break-glass proposal {0} is withdrawn")]
    BreakGlassProposalWithdrawn(Uuid),
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn display_topic_exists() {
        let e = MetadataError::TopicExists("my-topic".into());
        assert2::assert!(e.to_string() == "topic 'my-topic' already exists");
    }

    #[test]
    fn display_invalid_partition() {
        let e = MetadataError::InvalidPartition {
            topic: "t".into(),
            partition: 7,
        };
        assert2::assert!(e.to_string().contains("partition 7"));
    }

    #[test]
    fn display_freeze_and_break_glass_rejections() {
        let id = Uuid::from_u128(0xB1);
        for (label, error, want) in [
            (
                "empty freeze scope",
                MetadataError::EmptyFreezeScope,
                "topic freeze scope is empty".to_string(),
            ),
            (
                "approvals that do not extend",
                MetadataError::BreakGlassApprovalsNotAnExtension(id),
                format!("break-glass proposal {id} does not extend the stored approvals"),
            ),
            (
                "consumed proposal",
                MetadataError::BreakGlassProposalConsumed(id),
                format!("break-glass proposal {id} is already consumed"),
            ),
            (
                "withdrawn proposal",
                MetadataError::BreakGlassProposalWithdrawn(id),
                format!("break-glass proposal {id} is withdrawn"),
            ),
        ] {
            assert2::check!(error.to_string() == want, "{label}");
        }
    }
}

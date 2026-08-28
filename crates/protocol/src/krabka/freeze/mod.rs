//! Control-plane messages for the topic write-freeze registry.
//!
//! A write freeze is a broker-owned state. The cluster is up and reads work,
//! but the broker refuses every produce to a topic that a freeze covers. The
//! registry lives in the metadata log, so a freeze holds after a restart.
//!
//! # APIs
//!
//! | Api key | Module | Purpose |
//! | --- | --- | --- |
//! | 1015 | [`set_topic_freeze`] | set a freeze on a scope, or remove one |
//! | 1016 | [`describe_topic_freezes`] | read the registry |
//!
//! The keys sit in the krabka-private range at 1000 and above, as
//! [`crate::krabka`] describes. `ApiVersions` advertises neither of them.
//!
//! # Framing
//!
//! Version 0 is the only version of each message, and it is flexible under
//! KIP-482. So every codec here writes compact strings, compact arrays, and a
//! tagged-fields trailer. No message declares a known tag. Each codec keeps an
//! unknown tagged field verbatim and writes it back in ascending tag order, as
//! the generated Kafka codecs do.
//!
//! # Scope
//!
//! A scope is a literal topic name or a topic-name prefix, and `pattern_type`
//! says which. The values are the values that Kafka gives to `PatternType` in
//! an ACL, so `kafka-acls` and a freeze use one vocabulary.

pub mod describe_topic_freezes;
pub mod set_topic_freeze;

pub use self::{
    describe_topic_freezes::{
        DescribeTopicFreezesRequest, DescribeTopicFreezesResponse, DescribedTopicFreeze,
    },
    set_topic_freeze::{SetTopicFreezeRequest, SetTopicFreezeResponse},
};

/// A filter that matches every pattern type. Only a filter takes this value.
///
/// Kafka gives the same value to `ANY` in the `PatternType` of an ACL filter.
/// Kafka gives `0` to `UNKNOWN`, and a filter reads `0` as `ANY` too.
pub const PATTERN_TYPE_ANY: i8 = 1;

/// A scope that is one whole topic name.
///
/// Kafka gives the same value to `LITERAL` in the `PatternType` of an ACL, so
/// an operator who knows `kafka-acls` knows this vocabulary.
pub const PATTERN_TYPE_LITERAL: i8 = 3;

/// A scope that is a topic-name prefix. It covers every topic whose name starts
/// with the prefix, and a topic that the cluster creates later.
///
/// Kafka gives the same value to `PREFIXED` in the `PatternType` of an ACL.
pub const PATTERN_TYPE_PREFIXED: i8 = 4;

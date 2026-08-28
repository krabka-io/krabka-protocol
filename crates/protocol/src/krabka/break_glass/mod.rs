//! Control-plane messages for the break-glass two-person rule.
//!
//! A break-glass workflow needs two different people to agree before the broker
//! does a privileged operation. An operator opens a proposal, a second operator
//! approves it, and the proposal is then a standing authorization for a bounded
//! window.
//!
//! No Kafka request gains a field for this. An operator gets the approval out of
//! band through these APIs, then runs the ordinary tool, and the broker looks
//! for the approval in its metadata image. `kafka-leader-election` and
//! `kafka-topics` keep their wire shapes.
//!
//! # APIs
//!
//! | Api key | Module | Purpose |
//! | --- | --- | --- |
//! | 1017 | [`propose`] | open a proposal |
//! | 1018 | [`approve`] | approve a proposal, or withdraw one |
//! | 1019 | [`describe`] | read proposals and their approvals |
//!
//! The keys sit in the krabka-private range at 1000 and above, as
//! [`crate::krabka`] describes. `ApiVersions` advertises none of them.
//!
//! # Framing
//!
//! Version 0 is the only version of each message, and it is flexible under
//! KIP-482. So every codec here writes compact strings, compact arrays, and a
//! tagged-fields trailer. No message declares a known tag. Each codec keeps an
//! unknown tagged field verbatim and writes it back in ascending tag order, as
//! the generated Kafka codecs do.
//!
//! # Action
//!
//! `action` is an `i8` on the wire, and its values are the values of the
//! broker's break-glass action type in `krabka-metadata`. The wire layer does
//! not name them, so one enum stays the only definition of the set.

pub mod approve;
pub mod describe;
pub mod propose;

pub use self::{
    approve::{ApproveBreakGlassRequest, ApproveBreakGlassResponse},
    describe::{
        BreakGlassApproval, DescribeBreakGlassRequest, DescribeBreakGlassResponse,
        DescribedBreakGlassProposal,
    },
    propose::{ProposeBreakGlassRequest, ProposeBreakGlassResponse},
};

//! Krabka-private wire messages that Apache Kafka does not define.
//!
//! Apache Kafka assigns api keys from 0 upward. Krabka puts its own control-plane
//! APIs at 1000 and above, so a later Kafka assignment cannot collide with them.
//! The broker uses the same range for its controller RPCs in
//! `crates/raft/src/wire.rs`, at keys 1003 and 1004.
//!
//! `krabka-protocol-codegen` reads the upstream Kafka schemas, so it cannot emit
//! these messages. They are hand-written, and they live under `src/` beside the
//! KIP-405 record codec in [`crate::records::remote_log_metadata`].
//!
//! Every message here is version 0 only and flexible from version 0, so each one
//! writes compact strings, compact arrays, and a tagged-fields trailer.
//! One crate-private module holds the codec helpers that give that framing.
//! `ApiVersions` advertises none of these keys, because only krabka tools send
//! them.
//!
//! # Key Modules
//!
//! - [`barrier`] — the five control-plane APIs for cross-topic barrier groups.
//! - [`freeze`] — the two control-plane APIs for the topic write-freeze registry.
//! - [`break_glass`] — the three control-plane APIs for two-person approval.

pub mod barrier;
pub mod break_glass;
pub(crate) mod common;
pub mod freeze;
#[cfg(test)]
mod test_support;

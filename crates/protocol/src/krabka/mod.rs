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
//! # Key Modules
//!
//! - [`barrier`] — the five control-plane APIs for cross-topic barrier groups.

pub mod barrier;

//! KIP-631 metadata record layer: the `ApiMessageAndVersion` value envelope,
//! a decode dispatch over the generated record types, control-record framing,
//! and a `bootstrap.checkpoint` builder. Byte-compatible with apache/kafka 4.x
//! `KRaft`.

pub mod checkpoint;
pub mod control;
pub mod envelope;
pub mod record;

pub use envelope::{EnvelopeError, FRAME_VERSION, ValueHeader, decode_value_header, encode_value};
pub use record::{KraftMetadataRecord, UNKNOWN_FRAME_VERSION};

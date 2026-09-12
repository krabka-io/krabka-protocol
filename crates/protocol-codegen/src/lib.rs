//! Build-time code generator for the `krabka-protocol` crate.
//!
//! The generator reads the vendored Apache Kafka JSON message schemas,
//! validates the subset Krabka supports, resolves nested and common structs,
//! and emits the owned and borrowed Rust protocol modules.
//! `tools/regenerate.sh` drives the binary wrapper and writes the committed
//! tree under `crates/protocol/generated`. The library API is useful for tests
//! and for one-off schema audits.
//!
//! ## Loading and validating schemas
//!
//! ```no_run
//! use std::path::Path;
//!
//! use krabka_protocol_codegen::{ir, validate};
//!
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let specs = ir::load_dir(Path::new("crates/protocol/schemas"))?;
//! validate::validate(&specs)?;
//! println!("loaded {} protocol schemas", specs.len());
//! # Ok(())
//! # }
//! ```
//!
//! ## Resolving generated type paths
//!
//! ```no_run
//! use std::path::Path;
//!
//! use krabka_protocol_codegen::{ir, resolve};
//!
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let specs = ir::load_dir(Path::new("crates/protocol/schemas"))?;
//! let metadata = specs.iter().find(|s| s.name == "MetadataRequest").unwrap();
//! let resolution = resolve::resolve_message(metadata)?;
//! println!("{} referenced struct types", resolution.len());
//! # Ok(())
//! # }
//! ```
pub mod emit;
pub mod fmt;
pub mod ir;
pub mod name_conv;
pub mod resolve;
pub mod type_map;
pub mod validate;

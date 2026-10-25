//! The generator binary: `krabka-protocol-codegen [--namespace NAME] <schemas> <out>`.
use std::{env, path::PathBuf};

use krabka_protocol_codegen::{
    driver::{self, RunError},
    fmt,
};

/// The command line: `[--namespace NAME] <schemas> <out>`.
fn parse_args() -> Result<(PathBuf, PathBuf, Option<String>), RunError> {
    let mut positional: Vec<String> = Vec::new();
    let mut namespace: Option<String> = None;
    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        if a == "--namespace" {
            namespace = Some(args.next().ok_or(RunError::Usage)?);
        } else {
            positional.push(a);
        }
    }
    let [schemas, out] = positional.as_slice() else {
        return Err(RunError::Usage);
    };
    Ok((PathBuf::from(schemas), PathBuf::from(out), namespace))
}

fn main() -> Result<(), RunError> {
    let (schemas, out, namespace) = parse_args()?;
    let count = driver::run(&schemas, &out, namespace.as_deref(), fmt::rustfmt)?;
    eprintln!("Emitted {count} message specs.");
    Ok(())
}

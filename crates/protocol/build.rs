use std::{error::Error, fs, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    println!("cargo:rerun-if-changed=schemas/VERSION");
    println!("cargo:rerun-if-changed=generated");

    let schemas_version = fs::read_to_string(root.join("schemas/VERSION"))
        .map_err(|e| format!("schemas/VERSION must exist: {e}"))?;
    let sha = schemas_version
        .lines()
        .find_map(|l| l.strip_prefix("sha: "))
        .ok_or("schemas/VERSION must contain a `sha:` line")?;

    let one =
        fs::read_to_string(root.join("generated/ApiVersionsRequest.owned.rs")).map_err(|e| {
            format!(
                "generated/ApiVersionsRequest.owned.rs must exist; run tools/regenerate.sh: {e}"
            )
        })?;
    assert2::assert!(one.contains(sha));

    let ns_version = fs::read_to_string(root.join("schemas/versions/kafka_3_6_2/VERSION"))
        .map_err(|e| format!("schemas/versions/kafka_3_6_2/VERSION must exist: {e}"))?;
    let ns_sha = ns_version
        .lines()
        .find_map(|l| l.strip_prefix("sha: "))
        .ok_or("schemas/versions/kafka_3_6_2/VERSION must contain a `sha:` line")?;
    let ns_one = fs::read_to_string(root.join("generated/kafka_3_6_2/ProduceRequest.owned.rs"))
        .map_err(|e| {
            format!(
                "generated/kafka_3_6_2/ProduceRequest.owned.rs must exist; run \
                 tools/regenerate.sh: {e}"
            )
        })?;
    assert2::assert!(ns_one.contains(ns_sha));
    println!("cargo:rerun-if-changed=schemas/versions/kafka_3_6_2/VERSION");
    Ok(())
}

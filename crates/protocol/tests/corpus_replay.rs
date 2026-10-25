use std::{env, fs, path::Path};

use serde::Deserialize;

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/generated/differential_table.rs"
));

#[derive(Debug, Deserialize)]
struct Meta {
    api_key: i16,
    version: i16,
    direction: String,
    #[allow(dead_code)]
    source_kafka_version: String,
    #[allow(dead_code)]
    synthetic: bool,
    #[allow(dead_code)]
    description: String,
}

fn load_pair(stem: &Path) -> datatest_stable::Result<(Meta, Vec<u8>)> {
    let hex_raw: String = fs::read_to_string(stem.with_extension("hex"))?
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let bytes = hex::decode(hex_raw)?;
    let meta: Meta = toml::from_str(&fs::read_to_string(stem.with_extension("toml"))?)?;
    Ok((meta, bytes))
}

/// Map (`api_key`, direction) to the message name with the generated `CASES` table.
fn name_for(api_key: i16, is_request: bool) -> Option<&'static str> {
    CASES
        .iter()
        .find(|c| {
            c.api_key == api_key
                && matches!(
                    (c.kind, is_request),
                    (Kind::Request, true) | (Kind::Response, false)
                )
        })
        .map(|c| c.name)
}

fn corpus_entry_round_trips(path: &Path) -> datatest_stable::Result<()> {
    fs::metadata(path)?;
    let stem = path.with_extension("");
    let (meta, bytes) = load_pair(&stem)?;
    let is_request = match meta.direction.as_str() {
        "request" => true,
        "response" => false,
        other => return Err(format!("bad direction {other} in {}", stem.display()).into()),
    };
    let name = name_for(meta.api_key, is_request).ok_or_else(|| {
        format!(
            "no CASES name for api_key {} in {}",
            meta.api_key,
            stem.display()
        )
    })?;
    let re = roundtrip(name, meta.version, &bytes)?;
    if re != bytes {
        return Err(format!(
            "{name} v{} does not round-trip in {}",
            meta.version,
            stem.display()
        )
        .into());
    }
    Ok(())
}

/// Resolve a crate-relative fixture directory.
///
/// Cargo runs an integration test with the crate directory as the working
/// directory, so a bare `tests/...` resolves as written. Bazel runs it from an
/// execution root instead and stages the same files under
/// `$TEST_SRCDIR/$TEST_WORKSPACE`, with `FIXTURE_ROOT` naming the package inside
/// that tree. Absent those variables this is the Cargo path unchanged.
fn fixture_root(relative: &str) -> String {
    let (Ok(srcdir), Ok(workspace), Ok(package)) = (
        env::var("TEST_SRCDIR"),
        env::var("TEST_WORKSPACE"),
        env::var("FIXTURE_ROOT"),
    ) else {
        return relative.to_owned();
    };
    format!("{srcdir}/{workspace}/{package}/{relative}")
}

datatest_stable::harness! {
    { test = corpus_entry_round_trips, root = fixture_root("tests/corpus"), pattern = r".*\.hex$" },
}

use std::path::{Path, PathBuf};

use krabka_protocol_codegen::{emit, fmt, ir, name_conv, out::Written, validate};
use proc_macro2::Ident;
use quote::{format_ident, quote};

fn parse_args() -> (PathBuf, PathBuf, Option<String>) {
    let mut positional: Vec<String> = Vec::new();
    let mut namespace: Option<String> = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        if a == "--namespace" {
            namespace = Some(args.next().expect("--namespace requires a value"));
        } else {
            positional.push(a);
        }
    }
    assert_eq!(
        positional.len(),
        2,
        "usage: codegen [--namespace NAME] <schemas> <out>"
    );
    (
        PathBuf::from(&positional[0]),
        PathBuf::from(&positional[1]),
        namespace,
    )
}

fn main() -> Result<(), RunError> {
    let (schemas, out, namespace) = parse_args();
    let count = run(&schemas, &out, namespace.as_deref())?;
    eprintln!("Emitted {count} message specs.");
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum RunError {
    #[error(transparent)]
    Ir(#[from] ir::IrError),
    #[error(transparent)]
    Validate(#[from] validate::ValidateError),
    #[error(transparent)]
    Emit(#[from] emit::EmitError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Fmt(#[from] fmt::FmtError),
    #[error(transparent)]
    Write(#[from] krabka_protocol_codegen::out::WriteError),
    #[error("schemas/VERSION must contain a `sha:` line")]
    MissingSha,
}

fn read_schemas_sha(schemas: &std::path::Path) -> Result<String, RunError> {
    let version_text = std::fs::read_to_string(schemas.join("VERSION"))?;
    let sha = version_text
        .lines()
        .find_map(|l| l.strip_prefix("sha: "))
        .ok_or(RunError::MissingSha)?
        .to_owned();
    Ok(sha)
}

/// Returns true if the schema should be emitted (has at least one valid version).
fn should_emit(spec: &ir::MessageSpec) -> bool {
    !spec.valid_versions.is_empty()
}

/// Derive the `crates/protocol/src` directory from the generated-output dir.
/// By convention the generated output is `crates/protocol/generated`, and src
/// is the sibling `src`.
fn protocol_src_from_out(out: &Path) -> PathBuf {
    out.parent().unwrap_or(out).join("src")
}

fn write_wrapper(
    written: &mut Written,
    spec: &ir::MessageSpec,
    flavor: emit::wrappers::Flavor,
    schemas_version: &str,
    protocol_src: &Path,
    namespace: Option<&str>,
) -> std::io::Result<()> {
    use emit::wrappers::Flavor;
    let snake = name_conv::module_name(&spec.name);
    let body = emit::wrappers::emit(spec, flavor, schemas_version, namespace);
    let dir = protocol_src.join(match flavor {
        Flavor::Owned => "owned",
        Flavor::Borrowed => "borrowed",
    });
    std::fs::create_dir_all(&dir)?;
    written.write(dir.join(format!("{snake}.rs")), &body)?;
    Ok(())
}

/// Split a common-struct emitter key `<message_snake>/<struct_snake>` into its
/// two segments.
fn split_common_stem(stem: &str) -> (&str, &str) {
    stem.split_once('/')
        .expect("common-struct key must be `<message_snake>/<struct_snake>`")
}

/// Write the `include!` wrapper for one message-scoped common struct at
/// `src/[<namespace>/]{flavor}/common/<message_snake>/<struct_snake>.rs`.
///
/// The wrapper pulls in the generated body from
/// `generated/[<namespace>/]common/{flavor}/<message_snake>/<struct_snake>.<suffix>.rs`.
/// A namespace run writes its bodies below `generated/<namespace>/`, the same
/// way `wrappers::emit` does for ordinary message files, so the wrapper must
/// carry the namespace segment too.
fn write_common_wrapper(
    written: &mut Written,
    message_snake: &str,
    struct_snake: &str,
    flavor: emit::wrappers::Flavor,
    schemas_version: &str,
    message_src_dir: &Path,
    namespace: Option<&str>,
) -> std::io::Result<()> {
    use emit::wrappers::Flavor;
    let suffix = match flavor {
        Flavor::Owned => "owned",
        Flavor::Borrowed => "borrowed",
    };
    let path_prefix = match namespace {
        None => String::new(),
        Some(ns) => format!("{ns}/"),
    };
    let body = format!(
        "{}include!(concat!(\n    env!(\"CARGO_MANIFEST_DIR\"),\n    \"/generated/{path_prefix}common/{flavor_dir}/{message_snake}/{struct_snake}.{suffix}.rs\"\n));\n",
        emit::common::banner(schemas_version),
        flavor_dir = flavor.dir(),
    );
    written.write(message_src_dir.join(format!("{struct_snake}.rs")), &body)?;
    Ok(())
}

/// Emit the nested wrapper module tree for one flavor's message-scoped common
/// structs:
///
/// - `src/{flavor}/common/mod.rs`          — `pub mod <message_snake>;` per message
/// - `src/{flavor}/common/<msg>/mod.rs`    — `pub mod <struct_snake>;` per struct
/// - `src/{flavor}/common/<msg>/<struct>.rs` — the `include!` wrapper
///
/// Returns the number of files written. It does nothing and returns 0 when
/// `tree` is empty.
fn write_common_wrapper_tree(
    written: &mut Written,
    tree: &std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
    flavor: emit::wrappers::Flavor,
    schemas_version: &str,
    protocol_src: &Path,
    namespace: Option<&str>,
) -> std::io::Result<usize> {
    if tree.is_empty() {
        return Ok(0);
    }
    let mut count = 0;
    let flavor_doc = match flavor {
        emit::wrappers::Flavor::Owned => "Owned",
        emit::wrappers::Flavor::Borrowed => "Borrowed",
    };
    let src_common = protocol_src.join(flavor.dir()).join("common");
    std::fs::create_dir_all(&src_common)?;

    let mut message_mods: Vec<Ident> = Vec::new();
    for (message_snake, structs) in tree {
        message_mods.push(format_ident!("{message_snake}"));
        let message_dir = src_common.join(message_snake);
        std::fs::create_dir_all(&message_dir)?;

        let mut struct_mods: Vec<Ident> = Vec::new();
        for struct_snake in structs {
            write_common_wrapper(
                written,
                message_snake,
                struct_snake,
                flavor,
                schemas_version,
                &message_dir,
                namespace,
            )?;
            struct_mods.push(format_ident!("{struct_snake}"));
            count += 1;
        }
        let message_mod_tokens = quote!(#(pub mod #struct_mods;)*);
        let message_mod = format!(
            "{}{message_mod_tokens}",
            emit::common::banner(schemas_version)
        );
        written.write(message_dir.join("mod.rs"), &message_mod)?;
        count += 1;
    }
    let top_doc = format!(" {flavor_doc} common structs, scoped per owning message schema.");
    let top_tokens = quote!(#![doc = #top_doc] #(pub mod #message_mods;)*);
    let top_mod = format!("{}{top_tokens}", emit::common::banner(schemas_version));
    written.write(src_common.join("mod.rs"), &top_mod)?;
    count += 1;
    Ok(count)
}

fn run(
    schemas: &std::path::Path,
    out: &std::path::Path,
    namespace: Option<&str>,
) -> Result<usize, RunError> {
    let mut written = Written::default();
    let schemas_sha = read_schemas_sha(schemas)?;
    let specs = ir::load_dir(schemas)?;
    validate::validate(&specs)?;
    std::fs::create_dir_all(out)?;
    // Common-struct output directory (owned flavor).
    let common_owned_dir = out.join("common").join("owned");
    let common_borrowed_dir = out.join("common").join("borrowed");

    // Derive the protocol/src directory so we can write wrappers and mod.rs.
    let protocol_src = match namespace {
        None => protocol_src_from_out(out),
        Some(ns) => out
            .parent()
            .expect("out must have a parent") // generated/
            .parent()
            .expect("out parent must have a parent") // crates/protocol/
            .join("src")
            .join(ns),
    };

    // Track the message-scoped common structs emitted, as
    // <message_snake> -> {<struct_snake>, ...}, for the nested wrapper modules.
    let mut all_common_owned =
        std::collections::BTreeMap::<String, std::collections::BTreeSet<String>>::new();
    let mut all_common_borrowed =
        std::collections::BTreeMap::<String, std::collections::BTreeSet<String>>::new();

    let mut count = 0;
    for s in &specs {
        if !should_emit(s) {
            continue;
        }
        let owned_em = emit::owned_quote::emit(s, &schemas_sha)?;
        let borrowed_em = emit::borrowed_quote::emit(s, &schemas_sha, namespace)?;
        written.write_rs(out.join(format!("{}.owned.rs", s.name)), &owned_em.primary)?;
        written.write_rs(
            out.join(format!("{}.borrowed.rs", s.name)),
            &borrowed_em.primary,
        )?;
        count += 2;
        // Write common-struct generated bodies. The emitter key is the relative
        // stem `<message_snake>/<struct_snake>`; bodies land at
        // `generated/common/<flavor>/<message_snake>/<struct_snake>.<flavor>.rs`.
        for (stem, body) in &owned_em.commons {
            let (message_snake, struct_snake) = split_common_stem(stem);
            let dir = common_owned_dir.join(message_snake);
            std::fs::create_dir_all(&dir)?;
            written.write_rs(dir.join(format!("{struct_snake}.owned.rs")), body)?;
            all_common_owned
                .entry(message_snake.to_string())
                .or_default()
                .insert(struct_snake.to_string());
            count += 1;
        }
        for (stem, body) in &borrowed_em.commons {
            let (message_snake, struct_snake) = split_common_stem(stem);
            let dir = common_borrowed_dir.join(message_snake);
            std::fs::create_dir_all(&dir)?;
            written.write_rs(dir.join(format!("{struct_snake}.borrowed.rs")), body)?;
            all_common_borrowed
                .entry(message_snake.to_string())
                .or_default()
                .insert(struct_snake.to_string());
            count += 1;
        }

        // Emit wrapper files — overwrite the hand-written wrappers.
        if emit::wrappers::should_emit_wrapper(s) {
            write_wrapper(
                &mut written,
                s,
                emit::wrappers::Flavor::Owned,
                &schemas_sha,
                &protocol_src,
                namespace,
            )?;
            write_wrapper(
                &mut written,
                s,
                emit::wrappers::Flavor::Borrowed,
                &schemas_sha,
                &protocol_src,
                namespace,
            )?;
            count += 2;
        }
    }

    count += write_module_files(
        &mut written,
        &specs,
        &all_common_owned,
        &all_common_borrowed,
        &schemas_sha,
        &protocol_src,
        namespace,
    )?;

    // Always emit the ApiKey enum and differential dispatch table at the top level,
    // but NOT inside a namespace dir — those files reference top-level types and
    // belong only in the root generated/ output.
    if namespace.is_none() {
        let api_key_src = emit::api_key_enum_quote::emit(&specs, &schemas_sha);
        written.write_rs(out.join("api_key.rs"), &api_key_src)?;
        count += 1;

        // Emit the differential dispatch table for the parameterised sweep test.
        let diff_table = emit::differential_table::emit(&specs, &schemas_sha);
        written.write_rs(out.join("differential_table.rs"), &diff_table)?;
        count += 1;
    }

    // Drop the files an earlier schema set produced and this one does not.
    // The sweep of `out` itself stays flat, because a namespace run owns the
    // subdirectory below it.
    written.prune(out, false)?;
    written.prune(&out.join("common"), true)?;
    written.prune(&protocol_src.join("owned"), true)?;
    written.prune(&protocol_src.join("borrowed"), true)?;

    Ok(count)
}

fn write_module_files(
    written: &mut Written,
    specs: &[ir::MessageSpec],
    common_owned: &std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
    common_borrowed: &std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
    schemas_sha: &str,
    protocol_src: &std::path::Path,
    namespace: Option<&str>,
) -> Result<usize, RunError> {
    let mut count = write_common_wrapper_tree(
        written,
        common_owned,
        emit::wrappers::Flavor::Owned,
        schemas_sha,
        protocol_src,
        namespace,
    )?;
    count += write_common_wrapper_tree(
        written,
        common_borrowed,
        emit::wrappers::Flavor::Borrowed,
        schemas_sha,
        protocol_src,
        namespace,
    )?;
    let active: Vec<_> = specs.iter().filter(|spec| should_emit(spec)).collect();
    let owned = emit::mod_rs::emit(
        &active,
        emit::wrappers::Flavor::Owned,
        schemas_sha,
        !common_owned.is_empty(),
    );
    let borrowed = emit::mod_rs::emit(
        &active,
        emit::wrappers::Flavor::Borrowed,
        schemas_sha,
        !common_borrowed.is_empty(),
    );
    written.write(protocol_src.join("owned/mod.rs"), &owned)?;
    written.write(protocol_src.join("borrowed/mod.rs"), &borrowed)?;
    count += 2;
    if namespace.is_some() {
        written.write(
            protocol_src.join("mod.rs"),
            "pub mod borrowed;\npub mod owned;\n",
        )?;
    }
    Ok(count)
}

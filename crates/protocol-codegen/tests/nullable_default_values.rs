//! Kafka's `FieldSpec.fieldDefaultToJava` default-value rules, on schemas
//! built in memory.
//!
//! `owned::owned_default_expr`, `borrowed::borrowed_default_expr`,
//! `owned::tagged_is_default_cond`, `borrowed::tagged_is_default_cond`,
//! `owned::needs_manual_default`, `borrowed_quote::struct_members`'
//! nullable-struct guard, and `default_json::json_value_expr_versioned` are
//! all `pub(crate)`, exercised here only indirectly through the crate's
//! public `owned_quote::emit`/`borrowed_quote::emit` entry points — the same
//! way the wire codec they generate is exercised. This test module is a
//! plain `tests/*.rs` binary (not the crate's `#[cfg(test)]` unit target),
//! which is what lets it run, and count for coverage, under both `cargo
//! test` and `bazel test`/`bazel coverage`: the unit target is tagged
//! `manual` in `BUILD.bazel` because unrelated modules in it
//! (`src/emit/mod.rs`, `src/resolve.rs`, `src/validate.rs`) read schema
//! fixtures through `env!("CARGO_MANIFEST_DIR")`, which does not survive the
//! Bazel sandbox.
//!
//! Every field shape here mirrors one already pinned by a same-named
//! `#[cfg(test)]` unit test in `owned.rs`, `borrowed.rs`, `borrowed_quote.rs`
//! or `default_json.rs`; see those for the full rationale on each rule.

use assert2::assert;
use krabka_protocol_codegen::{
    emit::{borrowed_quote, owned_quote},
    ir::MessageSpec,
};

fn spec(value: serde_json::Value) -> MessageSpec {
    serde_json::from_value(value).unwrap()
}

/// A message carrying one of every field shape whose default-value rule this
/// PR changed: a plain nullable array/scalar/struct field with no explicit
/// default (empty value, not `None`), a tagged nullable array/scalar field
/// with no explicit default (same rule, plus the tagged is-default check), a
/// tagged nullable field with an explicit `"default": "null"` (still
/// `None`), and a nullable vs. non-nullable `records` field (always `None`
/// when nullable, regardless of any explicit default; the ordinary
/// empty-value rule otherwise).
fn nullable_defaults_message() -> MessageSpec {
    spec(serde_json::json!({
        "name": "NullableDefaults", "type": "request", "apiKey": 1000,
        "validVersions": "0-3", "flexibleVersions": "1+",
        "fields": [
            // Ordered before any nullable-no-default field: owned's
            // `needs_manual_default` is a `fields.iter().any(...)`, which
            // short-circuits on the first field whose closure returns
            // `true` (a nullable field with no explicit default, below).
            // Placing `Level` first guarantees its own closure call (and
            // the `Number(n) if n.as_i64() == Some(0)` arm it exercises)
            // still happens.
            {"name": "Level", "type": "int32", "versions": "0+", "default": 0},
            // A non-nullable field with an explicit non-zero, non-tagged
            // default: the `Some(v) => scalar_*_default(base, v)` arm of
            // `owned_default_expr`/`borrowed_default_expr`.
            {"name": "Priority", "type": "int32", "versions": "0+", "default": 5},
            {"name": "Owners", "type": "[]string", "versions": "0+",
             "nullableVersions": "0+"},
            {"name": "Owner", "type": "string", "versions": "0+",
             "nullableVersions": "0+"},
            {"name": "Nested", "type": "OwnerStruct", "versions": "0+",
             "nullableVersions": "0+", "fields": [
                {"name": "X", "type": "int32", "versions": "0+"}
            ]},
            {"name": "Records", "type": "records", "versions": "0+",
             "nullableVersions": "0+"},
            {"name": "UnalignedRecords", "type": "records", "versions": "0+"},
            {"name": "TaggedOwners", "type": "[]string", "versions": "1+",
             "nullableVersions": "1+", "taggedVersions": "1+", "tag": 0},
            {"name": "TaggedOwner", "type": "string", "versions": "1+",
             "nullableVersions": "1+", "taggedVersions": "1+", "tag": 1},
            {"name": "TaggedNullDefault", "type": "string", "versions": "1+",
             "nullableVersions": "1+", "taggedVersions": "1+", "tag": 2,
             "default": "null"},
            // A *tagged* nullable `records` field: `tagged_is_default_cond`
            // has its own `base == "records" && nullable` special case,
            // separate from `owned_default_expr`'s/`borrowed_default_expr`'s.
            {"name": "TaggedRecords", "type": "records", "versions": "1+",
             "nullableVersions": "1+", "taggedVersions": "1+", "tag": 3},
            // A tagged, non-nullable field with an explicit non-zero
            // default: `tagged_is_default_cond`'s final fallback arm
            // (`self.field == cmp_val`, negated to `!=` by the tagged
            // should-encode check).
            {"name": "TaggedPriority", "type": "int32", "versions": "1+",
             "taggedVersions": "1+", "tag": 4, "default": 9}
        ]
    }))
}

/// Every character but ASCII whitespace, so the check does not depend on how
/// `quote!`/`proc-macro2` space out tokens (`Some(x)` vs. `Some (x)`), only
/// on which tokens appear and in what order.
fn stripped(s: &str) -> String {
    s.chars().filter(|c| !c.is_whitespace()).collect()
}

#[test]
fn owned_default_impl_uses_empty_values_not_none_for_nullable_no_default_fields() {
    let spec = nullable_defaults_message();
    let owned = stripped(&owned_quote::emit(&spec, "sha").unwrap().primary);

    for expected in [
        // A non-nullable field with an explicit default of `0`/non-zero:
        // `owned_default_expr`'s `Some(v) => scalar_owned_default(base, v)`
        // arm, not the empty-value or resolved-struct arms.
        "level:0i32",
        "priority:5i32",
        // Nullable array/scalar/struct fields with no explicit default: the
        // type's empty value, wrapped in `Some`, not `None`. Anchored on the
        // preceding field separator so this can't false-pass by matching
        // inside the tagged counterpart's `tagged_owners:Some(Vec::new())` /
        // `tagged_owner:Some(String::new())`, which the untagged field name
        // is a plain suffix of.
        ",owners:Some(Vec::new())",
        ",owner:Some(String::new())",
        "nested:Some(OwnerStruct::default())",
        // A nullable `records` field always defaults to `None`, regardless
        // of nullability's usual empty-value rule — tagged or not. Anchored
        // for the same reason: `records:None` is a suffix of, and would
        // otherwise false-match inside, `tagged_records:None`.
        ",records:None",
        "tagged_records:None",
        // A `records` field with no `nullableVersions` at all keeps the
        // ordinary empty-value default: there is no `Option` to put `None`
        // into.
        "unaligned_records:crate::records::RecordsPayload::default()",
        // A tagged nullable field with an explicit `"default": "null"` still
        // defaults to `None`.
        "tagged_null_default:None",
        "tagged_priority:9i32",
    ] {
        assert!(
            owned.contains(expected),
            "missing {expected:?} in:\n{owned}"
        );
    }
}

#[test]
fn borrowed_default_impl_uses_empty_values_not_none_for_nullable_no_default_fields() {
    let spec = nullable_defaults_message();
    let borrowed = stripped(&borrowed_quote::emit(&spec, "sha", None).unwrap().primary);

    for expected in [
        "level:0i32",
        "priority:5i32",
        // Anchored on the preceding field separator, as in the owned test
        // above, so these can't false-pass by matching inside the tagged
        // counterparts (`tagged_owners:Some(Vec::new())`,
        // `tagged_owner:Some("")`, `tagged_records:None`), which the
        // untagged field names are plain suffixes of.
        ",owners:Some(Vec::new())",
        // Borrowed scalars are `&str`/`&[u8]`, so the empty value is a
        // borrowed empty literal, not an owned `String::new()`.
        ",owner:Some(\"\")",
        // The borrowed-emitter half of `struct_members`' nullable-struct
        // guard (`!is_nullable(field)` in `borrowed_quote.rs`): a nullable
        // struct field must route through `borrowed_default_expr` (`Some(<Ty>::default())`)
        // rather than the bare `<Ty>::default()` shortcut non-nullable
        // struct fields take.
        "nested:Some(OwnerStruct::default())",
        ",records:None",
        "tagged_records:None",
        "unaligned_records:crate::records::RecordsPayloadBorrowed::default()",
        "tagged_null_default:None",
        "tagged_priority:9i32",
    ] {
        assert!(
            borrowed.contains(expected),
            "missing {expected:?} in:\n{borrowed}"
        );
    }
}

/// `tagged_is_default_cond`'s three new branches (`records`, explicit
/// `"default": "null"`, and no explicit default) feed the tagged
/// should-encode check, which negates the condition
/// (`owned_quote::tagged_should_encode_from_default`): a default-valued
/// tagged field is skipped, so the emitted condition is the field taking a
/// *non*-default value.
#[test]
fn owned_tagged_should_encode_check_treats_empty_value_as_default_not_none() {
    let spec = nullable_defaults_message();
    let owned = stripped(&owned_quote::emit(&spec, "sha").unwrap().primary);

    for expected in [
        // No explicit default: `Some(empty)` is the default, so encoding is
        // skipped only when the field differs from `Some(empty)` — `None`
        // (a real wire null) is a distinct, non-default value that must
        // still be tag-encoded.
        "self.tagged_owners!=Some(Vec::new())",
        "self.tagged_owner!=Some(String::new())",
        // Explicit `"default": "null"`: the default is `None`, so encoding
        // is skipped only when the field is `Some`.
        "self.tagged_null_default.is_some()",
        // `tagged_is_default_cond`'s own `records`-nullable special case.
        "self.tagged_records.is_some()",
        // The final fallback arm (`self.field == cmp_val`, negated by the
        // tagged should-encode check) for a tagged field with an explicit,
        // non-empty, non-null scalar default.
        "self.tagged_priority!=9i32",
    ] {
        assert!(
            owned.contains(expected),
            "missing {expected:?} in:\n{owned}"
        );
    }
}

#[test]
fn borrowed_tagged_should_encode_check_treats_empty_value_as_default_not_none() {
    let spec = nullable_defaults_message();
    let borrowed = stripped(&borrowed_quote::emit(&spec, "sha", None).unwrap().primary);

    for expected in [
        "self.tagged_owners!=Some(Vec::new())",
        "self.tagged_owner!=Some(\"\")",
        "self.tagged_null_default.is_some()",
        "self.tagged_records.is_some()",
        "self.tagged_priority!=9i32",
    ] {
        assert!(
            borrowed.contains(expected),
            "missing {expected:?} in:\n{borrowed}"
        );
    }
}

/// `default_json::json_value_expr_versioned`'s `records`-null special case:
/// a nullable `records` field's default JSON is `null`, and a
/// `records` field with no `nullableVersions` keeps the empty-string
/// placeholder (the JVM JSON-to-Data converter rejects a JSON `null` for a
/// field it does not consider nullable). `emit_default_json` is called
/// inside `owned_quote::emit` and its output appended to `primary`, so this
/// also exercises `json_value_expr_versioned` without needing its own
/// `pub(crate)` visibility widened.
#[test]
fn default_json_uses_null_only_for_nullable_records_fields() {
    let spec = nullable_defaults_message();
    let owned = stripped(&owned_quote::emit(&spec, "sha").unwrap().primary);

    assert!(owned.contains("\"records\".to_string(),::serde_json::Value::Null"));
    assert!(
        owned.contains(
            "\"unalignedRecords\".to_string(),::serde_json::Value::String(String::new())"
        )
    );
}

/// `needs_manual_default`'s new `if nullable { return !default_is_null; }`
/// branch: a message whose only field is nullable with an explicit
/// `"default": "null"` needs no manual `Default` impl at all (Rust's
/// `#[derive(Default)]` on `Option<T>` already produces `None`), unlike the
/// same field shape with no explicit default (which needs `Some(<empty>)`,
/// so `nullable_defaults_message`'s combined message above does take the
/// manual-impl path).
#[test]
fn message_with_only_a_null_default_nullable_field_derives_default() {
    let spec = spec(serde_json::json!({
        "name": "OnlyNullDefault", "type": "request", "apiKey": 1001,
        "validVersions": "0+",
        "fields": [
            {"name": "Owner", "type": "string", "versions": "0+",
             "nullableVersions": "0+", "default": "null"}
        ]
    }));
    let owned = stripped(&owned_quote::emit(&spec, "sha").unwrap().primary);

    assert!(owned.contains(",Default)]"));
    assert!(!owned.contains("implDefaultforOnlyNullDefault"));
}

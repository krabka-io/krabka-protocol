//! Emit `pub fn default_json(version: i16) -> serde_json::Value` per message.
//!
//! The emitted function produces a `serde_json::Value` whose shape mirrors
//! what Kafka's `*DataJsonConverter.read(json, version)` expects for the
//! default state of a message. That is the JSON the JVM oracle should accept
//! to yield the same bytes as `MessageName::default()` after encoding.
//!
//! The function is version-aware. It includes only fields that are valid for
//! the requested version, and it emits null only for versions where the field
//! is actually nullable. The JVM converter therefore never rejects the
//! input.

use proc_macro2::TokenStream;
use quote::quote;

use crate::{
    emit::{
        EmitError,
        common::{format_int_literal, parse_tokens, uses_ident, validate},
    },
    ir::{FieldSpec, MessageSpec, VersionRange},
};

/// Emit the `default_json(version: i16)` function body for the given message.
/// The output is plain Rust source, and the caller appends it to the
/// per-message owned module body.
///
/// # Errors
/// Returns an error when the emitted function is not valid Rust, which is an
/// emitter bug.
pub fn emit_default_json(spec: &MessageSpec) -> Result<String, EmitError> {
    let field_stmts = fields_stmts_tokens(&spec.fields)?;
    // A message whose fields hold at every version never reads `version`, and
    // one with no fields never inserts into `obj`.
    let version = if uses_ident(&field_stmts, "version") {
        quote!(version)
    } else {
        quote!(_version)
    };
    let mut_kw = if field_stmts.is_empty() {
        quote!()
    } else {
        quote!(mut)
    };

    let tokens = quote! {
        #[doc = " Default JSON payload matching `Self::default()` for JVM oracle differential testing."]
        #[doc = " Only includes fields valid for the given version."]
        #[must_use]
        #[allow(unused_comparisons)]
        pub fn default_json(#version: i16) -> ::serde_json::Value {
            let #mut_kw obj = ::serde_json::Map::new();
            #field_stmts
            ::serde_json::Value::Object(obj)
        }
    };

    // Validate at generation time — a parse failure is a generator bug.
    validate::<syn::Item>(&tokens, "default_json fn")?;

    Ok(tokens.to_string())
}

/// Build a `TokenStream` of `obj.insert(...)` statements for each field, with
/// version range checks as guards.
fn fields_stmts_tokens(fields: &[FieldSpec]) -> Result<TokenStream, EmitError> {
    let stmts = fields
        .iter()
        .map(|f| {
            let key = json_field_name(&f.name);
            let val_tokens = parse_tokens(&json_value_expr_versioned(f))?;
            let insert = quote!(obj.insert(#key.to_owned(), #val_tokens););
            // A field valid at every version needs no guard.
            let Some(cond) = version_cond(f.versions) else {
                return Ok(insert);
            };
            let cond_tokens = parse_tokens(&cond)?;
            Ok(quote! {
                if #cond_tokens {
                    #insert
                }
            })
        })
        .collect::<Result<Vec<_>, EmitError>>()?;

    Ok(quote! { #(#stmts)* })
}

/// Return the condition string for a version range, or None if always valid.
fn version_cond(vr: VersionRange) -> Option<String> {
    if vr.min == 0 && vr.max == i16::MAX {
        None // always valid
    } else if vr.max == i16::MAX {
        Some(format!("version >= {}", vr.min))
    } else if vr.min == 0 {
        Some(format!("version <= {}", vr.max))
    } else if vr.min == vr.max {
        Some(format!("version == {}", vr.min))
    } else {
        Some(format!("({}..={}).contains(&version)", vr.min, vr.max))
    }
}

/// Generate a version-aware Rust expression for the default `serde_json::Value`.
///
/// The goal is to match what `MessageName::default()` encodes so that the
/// oracle produces the same bytes as the Rust implementation.
///
/// Key rules, matching Kafka's Java message generator
/// (`FieldSpec.fieldDefaultToJava`):
/// - Fields with an explicit `"default": "null"`: Rust `Default` produces
///   `None`, so emit `null`, staying version-aware for split nullability
///   ranges (null only where the field is actually nullable, the type's zero
///   value otherwise).
/// - Nullable fields with NO explicit default (array, string, or struct):
///   Rust `Default` now produces the *empty* value (`Some(<empty>)`), not
///   `None`, so emit the type's zero value for every version, not just the
///   non-nullable ones.
/// - Non-nullable array fields: always `[]`.
pub(crate) fn json_value_expr_versioned(f: &FieldSpec) -> String {
    let is_array = f.field_type.starts_with("[]");
    let default_is_null = matches!(&f.default, Some(serde_json::Value::Null))
        || matches!(&f.default, Some(serde_json::Value::String(s)) if s == "null")
        // Kafka's `FieldSpec.fieldDefaultToJava` hardcodes `null` as the
        // default for `records` fields unconditionally, regardless of any
        // explicit schema default. This is a permanent special case, not
        // subject to the general nullable-default-value rule below. It only
        // applies when the field is nullable in some version: the JVM's
        // JSON-to-Data converter rejects a JSON `null` for a `records` field
        // that has no `nullableVersions` at all (e.g.
        // `FetchSnapshotResponse.UnalignedRecords`), which keeps the
        // type-zero (empty base64) fallback below.
        || (base_type(&f.field_type) == "records" && f.nullable_versions.is_some());

    if default_is_null {
        if let Some(nv) = f.nullable_versions {
            // Check if nullable for ALL valid versions (trivial case: no branching needed).
            let always_nullable =
                nv.min <= f.versions.min && (nv.max == i16::MAX || nv.max >= f.versions.max);
            if always_nullable {
                return "::serde_json::Value::Null".into();
            }
            // Split: nullable in some versions, not in others.
            // Emit: if <nullable_cond> { Null } else { type_zero }
            let zero = if is_array {
                "::serde_json::Value::Array(vec![])".to_owned()
            } else {
                type_zero_expr(&f.field_type, f)
            };
            if let Some(cond) = version_cond(nv) {
                return format!("if {cond} {{ ::serde_json::Value::Null }} else {{ {zero} }}");
            }
            return "::serde_json::Value::Null".into();
        }
        // No nullable_versions but default_is_null — emit type zero.
        if is_array {
            return "::serde_json::Value::Array(vec![])".into();
        }
        return type_zero_expr(&f.field_type, f);
    }

    if is_array {
        return "::serde_json::Value::Array(vec![])".into();
    }

    f.default.as_ref().map_or_else(
        || type_zero_expr(&f.field_type, f),
        |v| scalar_value_expr(&f.field_type, v, f),
    )
}

/// Convert a schema `default` annotation to a Rust expression that produces a
/// `serde_json::Value`.
fn scalar_value_expr(field_type: &str, val: &serde_json::Value, f: &FieldSpec) -> String {
    let base = base_type(field_type);

    match val {
        serde_json::Value::String(s) if s == "null" => "::serde_json::Value::Null".into(),

        serde_json::Value::String(s) if is_numeric_type(base) => {
            // Convert hex string defaults (e.g. "0x7fffffff") to decimal.
            let trimmed = s.trim();
            let decimal = trimmed
                .strip_prefix("0x")
                .or_else(|| trimmed.strip_prefix("0X"))
                .and_then(|hex_str| i64::from_str_radix(hex_str, 16).ok())
                .map_or_else(|| trimmed.to_owned(), |n| n.to_string());
            // Produce a json! Number value.
            let literal = if base == "float64" {
                decimal
            } else {
                format_int_literal(&decimal, "")
            };
            format!("::serde_json::json!({literal})")
        }

        serde_json::Value::String(s) if base == "bool" => {
            if s == "true" {
                "::serde_json::Value::Bool(true)".into()
            } else {
                "::serde_json::Value::Bool(false)".into()
            }
        }

        serde_json::Value::String(s) if base == "string" => {
            // Escape the string for embedding in Rust source.
            if s.is_empty() {
                return "::serde_json::Value::String(String::new())".into();
            }
            let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
            format!("::serde_json::Value::String(\"{escaped}\".to_owned())")
        }

        serde_json::Value::Number(n) => {
            let literal = if base == "float64" {
                n.to_string()
            } else {
                format_int_literal(&n.to_string(), "")
            };
            format!("::serde_json::json!({literal})")
        }

        serde_json::Value::Bool(b) => {
            format!("::serde_json::Value::Bool({b})")
        }

        _ => type_zero_expr(field_type, f),
    }
}

/// The "zero" `serde_json::Value` expression for a type, used when the schema
/// specifies no default.
fn type_zero_expr(field_type: &str, f: &FieldSpec) -> String {
    let base = base_type(field_type);
    match base {
        "bool" => "::serde_json::Value::Bool(false)".into(),
        "int8" | "int16" | "int32" | "int64" | "uint16" | "uint32" => {
            "::serde_json::json!(0)".into()
        }
        "float64" => "::serde_json::json!(0.0)".into(),
        "string" | "bytes" | "records" => "::serde_json::Value::String(String::new())".into(),
        "uuid" => {
            // Kafka's *DataJsonConverter encodes Uuid as base64 (22 chars),
            // not the standard hyphen UUID format. The zero UUID in base64 is
            // "AAAAAAAAAAAAAAAAAAAAAA" (16 zero bytes, no padding needed).
            "::serde_json::Value::String(\"AAAAAAAAAAAAAAAAAAAAAA\".to_owned())".into()
        }
        _ => {
            if f.fields.is_empty() {
                "::serde_json::Value::Object(::serde_json::Map::new())".into()
            } else {
                emit_nested_struct_expr(&f.fields)
            }
        }
    }
}

/// Emit a Rust expression that builds a `serde_json::Value::Object` for a
/// nested struct.
///
/// Note that this function does NOT apply sub-field version guards, because
/// nested struct defaults are version-independent relative to their parent
/// field's guard. If this causes problems for specific complex schemas, fix it
/// at the per-field level.
fn emit_nested_struct_expr(fields: &[FieldSpec]) -> String {
    let inserts = fields
        .iter()
        .map(|f| {
            let key = json_field_name(&f.name);
            let val = json_value_expr_versioned(f);
            format!("m.insert(\"{key}\".to_owned(), {val}); ")
        })
        .collect::<Vec<_>>()
        .concat();
    format!("{{ let mut m = ::serde_json::Map::new(); {inserts}::serde_json::Value::Object(m) }}")
}

/// Convert a Kafka schema field name in `PascalCase`, such as
/// `TransactionalId`, to the JSON key the JVM's `*DataJsonConverter` uses.
///
/// That key is `camelCase`, such as `transactionalId`, with the first
/// character in lower case.
#[must_use]
pub fn json_field_name(name: &str) -> String {
    let mut chars = name.chars();
    chars.next().map_or_else(String::new, |c| {
        c.to_lowercase().collect::<String>() + chars.as_str()
    })
}

fn base_type(t: &str) -> &str {
    t.strip_prefix("[]").unwrap_or(t)
}

fn is_numeric_type(t: &str) -> bool {
    matches!(
        t,
        "int8" | "int16" | "int32" | "int64" | "uint16" | "uint32" | "float64"
    )
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    /// The JSON-oracle half of the `records`-null-default bug a review
    /// comment on PR #31 flagged: Kafka's `FieldSpec.fieldDefaultToJava`
    /// hardcodes `null` as the default for `records` fields unconditionally.
    /// A nullable `records` field (the shape of
    /// `FetchResponse.PartitionData.records`) must emit `Value::Null`, not
    /// the empty-string placeholder the general nullable-default-value rule
    /// would otherwise produce.
    #[test]
    fn nullable_records_field_emits_json_null() {
        let field: FieldSpec = serde_json::from_value(serde_json::json!({
            "name": "Records", "type": "records", "versions": "0+",
            "nullableVersions": "0+"
        }))
        .unwrap();

        assert!(json_value_expr_versioned(&field) == "::serde_json::Value::Null");
    }

    /// A `records` field with no `nullableVersions` at all (the shape of
    /// `FetchSnapshotResponse.UnalignedRecords`) keeps the empty-string
    /// placeholder: the JVM's JSON-to-Data converter rejects a JSON `null`
    /// for a `records` field it does not consider nullable.
    #[test]
    fn non_nullable_records_field_keeps_empty_string_placeholder() {
        let field: FieldSpec = serde_json::from_value(serde_json::json!({
            "name": "UnalignedRecords", "type": "records", "versions": "0+"
        }))
        .unwrap();

        assert!(json_value_expr_versioned(&field) == "::serde_json::Value::String(String::new())");
    }
}

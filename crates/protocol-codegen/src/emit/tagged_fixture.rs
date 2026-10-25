//! Emit `tagged_fixture()` and `tagged_fixture_json(version)` for each message
//! that has a KIP-482 tagged field at any depth.
//!
//! `tagged_fixture()` returns one owned message. Every tagged field in it, at
//! every depth, holds a non-default value. The value does not depend on the
//! version, so an encode at a version outside a tag's declared range must drop
//! that tag. Each array or struct that leads to a tagged field holds one
//! element, so nested tags are reachable.
//!
//! `tagged_fixture_json(version)` returns the same message as the JSON that
//! Kafka's `*JsonConverter.read(json, version)` accepts. It includes a field
//! only at the versions that its schema declares, and a tagged field only
//! inside `taggedVersions`. Kafka's generated `write` method throws when a
//! field holds a non-default value outside its versions, so the JSON must
//! leave out what the Rust encoder must drop.
//!
//! One walk over the schema builds both functions. So the Rust value and the
//! JSON value cannot drift apart. The version gate in the JSON comes from the
//! schema, and not from the encoder's own gate, so the JVM differential sweep
//! checks the encoder against Kafka.

use std::collections::HashMap;

use quote::{format_ident, quote};

use crate::{
    emit::{
        EmitError,
        common::{parse_tokens, uses_ident, validate},
        default_json::{json_field_name, json_value_expr_versioned},
        owned::{base_type, is_nullable, is_struct_type, is_tagged, struct_path_for},
    },
    ir::{FieldSpec, MessageSpec, VersionRange},
    name_conv,
    resolve::Resolution,
};

type ResMap = HashMap<String, Resolution>;

/// Return true when the message has a tagged field at any depth.
#[must_use]
pub fn has_tagged_field(spec: &MessageSpec) -> bool {
    fields_have_tag(&spec.fields, spec)
}

/// Emit both fixture functions for `spec`, or an empty string when the message
/// has no tagged field.
///
/// # Errors
/// Returns an error when a struct field does not resolve, a tagged field has
/// a type with no non-default value, or the generated functions are not valid
/// Rust. Each is a generator bug.
pub fn emit_tagged_fixture(spec: &MessageSpec, res_map: &ResMap) -> Result<String, EmitError> {
    if !has_tagged_field(spec) {
        return Ok(String::new());
    }
    let walk = Walk { spec, res_map };
    let ty = name_conv::type_name(&spec.name);
    let value = parse_tokens(&walk.rust_struct(&ty, &spec.fields, false)?)?;
    let json = parse_tokens(&walk.json_fields(&spec.fields, false, spec.valid_versions)?)?;
    let ty_ident = format_ident!("{ty}");
    // A fixture whose fields hold at every version never reads `version`.
    let version = if uses_ident(&json, "version") {
        quote!(version)
    } else {
        quote!(_version)
    };

    let tokens = quote! {
        #[doc = " A message with a non-default value in every tagged field, at every depth, for"]
        #[doc = " the JVM oracle differential sweep. The value is the same at every version."]
        #[must_use]
        pub fn tagged_fixture() -> #ty_ident {
            #value
        }

        #[doc = " The JSON form of `tagged_fixture()` that Kafka's JSON converter reads at"]
        #[doc = " `version`. It holds only the fields that the schema declares at that version."]
        #[must_use]
        pub fn tagged_fixture_json(#version: i16) -> ::serde_json::Value {
            let mut m = ::serde_json::Map::new();
            #json
            ::serde_json::Value::Object(m)
        }
    };

    validate::<syn::File>(&tokens, "tagged fixture")?;
    Ok(tokens.to_string())
}

struct Walk<'a> {
    spec: &'a MessageSpec,
    res_map: &'a ResMap,
}

impl Walk<'_> {
    /// A struct literal of type `path`. With `fill`, every scalar field gets a
    /// non-default value. Without it, only the fields that lead to a tag do.
    fn rust_struct(
        &self,
        path: &str,
        fields: &[FieldSpec],
        fill: bool,
    ) -> Result<String, EmitError> {
        let assigns = fields
            .iter()
            .filter(|f| self.sets(f, fill))
            .map(|f| {
                Ok(format!(
                    "{}: {}",
                    name_conv::field_name(&f.name),
                    self.rust_value(f)?
                ))
            })
            .collect::<Result<Vec<_>, EmitError>>()?;
        if assigns.is_empty() {
            return Ok(format!("{path}::default()"));
        }
        Ok(format!(
            "{path} {{ {}, ..{path}::default() }}",
            assigns.join(", ")
        ))
    }

    /// The `serde_json::Value` expression for a struct at the run-time
    /// `version`. `outer` is the version range in which the enclosing field
    /// is present.
    fn json_struct(
        &self,
        fields: &[FieldSpec],
        fill: bool,
        outer: VersionRange,
    ) -> Result<String, EmitError> {
        Ok(format!(
            "{{ let mut m = ::serde_json::Map::new(); {} ::serde_json::Value::Object(m) }}",
            self.json_fields(fields, fill, outer)?
        ))
    }

    /// One `m.insert` statement for each field present in the JSON. A field
    /// that exists only in part of `outer` gets a version check.
    fn json_fields(
        &self,
        fields: &[FieldSpec],
        fill: bool,
        outer: VersionRange,
    ) -> Result<String, EmitError> {
        let mut out = String::new();
        for f in fields {
            let declared = if is_tagged(f) {
                intersect(f.versions, f.tagged_versions.unwrap_or(f.versions))
            } else {
                f.versions
            };
            let range = intersect(declared, outer);
            if range.is_empty() {
                continue;
            }
            let value = if self.sets(f, fill) {
                self.json_value(f, range)?
            } else if is_tagged(f) {
                // A tagged field is never mandatory in Kafka's JSON. Leave it
                // out, and Kafka keeps its default.
                continue;
            } else {
                json_value_expr_versioned(f)
            };
            let insert = format!(
                "m.insert({:?}.to_owned(), {value});",
                json_field_name(&f.name)
            );
            if range == outer {
                out.push_str(&insert);
            } else {
                out.push_str("if ");
                out.push_str(&range_cond(range, outer));
                out.push_str(" { ");
                out.push_str(&insert);
                out.push_str(" }");
            }
            out.push(' ');
        }
        Ok(out)
    }

    /// Whether the fixture gives `f` a non-default value.
    fn sets(&self, f: &FieldSpec, fill: bool) -> bool {
        if base_type(&f.field_type) == "records" {
            return false;
        }
        is_tagged(f) || self.leads_to_tag(f) || (fill && !is_struct_type(base_type(&f.field_type)))
    }

    fn leads_to_tag(&self, f: &FieldSpec) -> bool {
        fields_have_tag(struct_fields(f, self.spec), self.spec)
    }

    /// The non-default Rust value of `f`.
    fn rust_value(&self, f: &FieldSpec) -> Result<String, EmitError> {
        let base = base_type(&f.field_type);
        let elem = if is_struct_type(base) {
            let path = struct_path_for(f, self.res_map)
                .ok_or_else(|| EmitError::Unresolved(f.name.clone()))?;
            // A tagged struct must differ from its default, so fill its
            // scalars. A struct that only leads to a tag keeps its defaults.
            self.rust_struct(&path, struct_fields(f, self.spec), is_tagged(f))?
        } else {
            scalar(base, f)?.rust
        };
        let inner = if f.field_type.starts_with("[]") {
            format!("vec![{elem}]")
        } else {
            elem
        };
        Ok(if owned_is_option(f) {
            format!("Some({inner})")
        } else {
            inner
        })
    }

    /// The JSON form of `rust_value(f)`, for a field present in `range`.
    fn json_value(&self, f: &FieldSpec, range: VersionRange) -> Result<String, EmitError> {
        let base = base_type(&f.field_type);
        let elem = if is_struct_type(base) {
            self.json_struct(struct_fields(f, self.spec), is_tagged(f), range)?
        } else {
            scalar(base, f)?.json
        };
        Ok(if f.field_type.starts_with("[]") {
            format!("::serde_json::Value::Array(vec![{elem}])")
        } else {
            elem
        })
    }
}

/// The owned field type is `Option<T>`. This mirrors `owned_quote`.
fn owned_is_option(f: &FieldSpec) -> bool {
    is_nullable(f) || (is_tagged(f) && matches!(f.default, Some(serde_json::Value::Null)))
}

/// The fields of a struct-typed field: inline, or from the message's
/// `commonStructs`. A primitive field has none.
fn struct_fields<'a>(f: &'a FieldSpec, spec: &'a MessageSpec) -> &'a [FieldSpec] {
    if !f.fields.is_empty() {
        return &f.fields;
    }
    let base = base_type(&f.field_type);
    spec.common_structs
        .iter()
        .find(|cs| cs.name == base)
        .map_or(&[], |cs| cs.fields.as_slice())
}

fn fields_have_tag(fields: &[FieldSpec], spec: &MessageSpec) -> bool {
    fields
        .iter()
        .any(|f| is_tagged(f) || fields_have_tag(struct_fields(f, spec), spec))
}

/// One non-default scalar, as a Rust expression and as a JSON expression.
struct Scalar {
    rust: String,
    json: String,
}

/// A value that differs from the schema default of `f`.
fn scalar(base: &str, f: &FieldSpec) -> Result<Scalar, EmitError> {
    let default = f.default.as_ref().and_then(|d| match d {
        serde_json::Value::String(s) => Some(s.trim().to_owned()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    });
    let pick = |a: &'static str, b: &'static str| {
        if default.as_deref() == Some(a) { b } else { a }
    };
    Ok(match base {
        "bool" => {
            let v = pick("true", "false");
            Scalar {
                rust: v.to_owned(),
                json: format!("::serde_json::Value::Bool({v})"),
            }
        }
        "int8" | "int16" | "int32" | "int64" | "uint16" | "uint32" => {
            let v = pick("1", "2");
            let suffix = match base {
                "int8" => "i8",
                "int16" => "i16",
                "int32" => "i32",
                "int64" => "i64",
                "uint16" => "u16",
                _ => "u32",
            };
            Scalar {
                rust: format!("{v}{suffix}"),
                json: format!("::serde_json::json!({v})"),
            }
        }
        "float64" => Scalar {
            rust: "1.5f64".to_owned(),
            json: "::serde_json::json!(1.5)".to_owned(),
        },
        "string" => {
            let v = pick("x", "y");
            Scalar {
                rust: format!("{v:?}.to_owned()"),
                json: format!("::serde_json::Value::String({v:?}.to_owned())"),
            }
        }
        // Kafka's JSON converter reads bytes as base64. `eA==` is `b"x"`.
        "bytes" => Scalar {
            rust: "::bytes::Bytes::from_static(b\"x\")".to_owned(),
            json: "::serde_json::Value::String(\"eA==\".to_owned())".to_owned(),
        },
        // Kafka writes a UUID as unpadded URL-safe base64 of its 16 bytes.
        "uuid" => Scalar {
            rust: "crate::primitives::uuid::Uuid([1u8; 16])".to_owned(),
            json: "::serde_json::Value::String(\"AQEBAQEBAQEBAQEBAQEBAQ\".to_owned())".to_owned(),
        },
        other => {
            return Err(EmitError::Unsupported(format!(
                "no non-default scalar for schema type `{other}`"
            )));
        }
    })
}

fn intersect(a: VersionRange, b: VersionRange) -> VersionRange {
    VersionRange {
        min: a.min.max(b.min),
        max: a.max.min(b.max),
    }
}

/// A run-time check that a `version` inside `outer` is also inside `r`. `r`
/// is a strict sub-range of `outer`, so a bound that they share needs no
/// check.
fn range_cond(r: VersionRange, outer: VersionRange) -> String {
    if r.min == r.max {
        format!("version == {}", r.min)
    } else if r.max == outer.max {
        format!("version >= {}", r.min)
    } else if r.min == outer.min {
        format!("version <= {}", r.max)
    } else {
        format!("({}..={}).contains(&version)", r.min, r.max)
    }
}

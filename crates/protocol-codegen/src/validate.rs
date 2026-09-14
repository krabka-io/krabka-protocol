use thiserror::Error;

use crate::ir::{FieldSpec, FlexibleVersions, MessageSpec, MessageType};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ValidateError {
    #[error("{message}: in {context}")]
    Unsupported {
        message: &'static str,
        context: String,
    },
}

/// Field types the generator currently understands. Anything else is a hard
/// error. This list is verified against the full Kafka 4.2.0 schema corpus of
/// 197 files. The field-level primitive types found there are bool, bytes,
/// float64, int16, int32, int64, int8, records, string, uint16, and uuid.
/// `uint32` and `float32` are listed here for completeness, because they
/// appear in older or future schemas, but they are not in the 4.2.0 corpus.
const KNOWN_PRIMITIVE_TYPES: &[&str] = &[
    "bool", "int8", "int16", "int32", "int64", "uint16", "uint32", "float64", "string", "bytes",
    "uuid", "records",
];

/// # Errors
/// Returns an error when the schema model is invalid or generated Rust cannot be formatted or written.
pub fn validate(specs: &[MessageSpec]) -> Result<(), ValidateError> {
    for spec in specs {
        let ctx = spec.name.clone();
        if matches!(
            spec.message_type,
            MessageType::Request | MessageType::Response
        ) && spec.api_key.is_none()
        {
            return Err(ValidateError::Unsupported {
                message: "request/response missing apiKey",
                context: ctx,
            });
        }
        if let Some(message) = latest_version_unstable_error(spec) {
            return Err(ValidateError::Unsupported {
                message,
                context: ctx,
            });
        }
        validate_fields(&spec.fields, spec.flexible_versions, &ctx)?;
        for cs in &spec.common_structs {
            validate_fields(
                &cs.fields,
                spec.flexible_versions,
                &format!("{ctx}.{}", cs.name),
            )?;
        }
    }
    Ok(())
}

/// Kafka's `MessageSpec` accepts `latestVersionUnstable` only on a request.
/// The flag marks the highest version in `validVersions`, so this function
/// also requires a range that has a highest version. An empty range has no
/// version to mark. An open-ended range has no fixed highest version.
fn latest_version_unstable_error(spec: &MessageSpec) -> Option<&'static str> {
    if !spec.latest_version_unstable {
        return None;
    }
    if spec.message_type != MessageType::Request {
        return Some("latestVersionUnstable on a message that is not a request");
    }
    if spec.valid_versions.is_empty() {
        return Some("latestVersionUnstable with empty validVersions");
    }
    if spec.valid_versions.max == i16::MAX {
        return Some("latestVersionUnstable with open-ended validVersions");
    }
    None
}

fn validate_fields(
    fields: &[FieldSpec],
    flexible: FlexibleVersions,
    ctx: &str,
) -> Result<(), ValidateError> {
    for f in fields {
        let context = format!("{ctx}.{}", f.name);
        let base = base_type(&f.field_type);

        let known = KNOWN_PRIMITIVE_TYPES.contains(&base)
            || base.starts_with("[]")   // arrays (nested [] not stripped by base_type)
            || is_struct_type(base); // struct reference like `MetadataRequestTopic`

        if !known {
            return Err(ValidateError::Unsupported {
                message: "unknown field type",
                context,
            });
        }

        if f.tag.is_some() && !is_some_flexible(flexible) {
            return Err(ValidateError::Unsupported {
                message: "tagged field on non-flexible message",
                context,
            });
        }

        // Kafka's generator enforces both rules in `FieldSpec`. The JVM
        // differential sweep depends on them too: it builds the borrowed
        // tagged fixture at the highest version, where every tag is in range.
        if f.tag.is_some() {
            let tagged = f.tagged_versions.unwrap_or(f.versions);
            if tagged.max != i16::MAX {
                return Err(ValidateError::Unsupported {
                    message: "taggedVersions is not open-ended",
                    context,
                });
            }
            if tagged.min < f.versions.min || f.versions.max != i16::MAX {
                return Err(ValidateError::Unsupported {
                    message: "taggedVersions is not a subset of versions",
                    context,
                });
            }
        }

        if !f.fields.is_empty() {
            validate_fields(&f.fields, flexible, &context)?; // flexible is Copy
        }
    }
    Ok(())
}

fn is_some_flexible(f: FlexibleVersions) -> bool {
    matches!(f, FlexibleVersions::Range(_))
}

fn base_type(t: &str) -> &str {
    t.strip_prefix("[]").unwrap_or(t)
}

fn is_struct_type(t: &str) -> bool {
    // Kafka schema convention: struct types are PascalCase identifiers.
    t.chars().next().is_some_and(char::is_uppercase)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use assert2::assert;
    use serde_json::json;

    use super::*;
    use crate::ir;

    fn unsupported(message: &'static str) -> Result<(), ValidateError> {
        Err(ValidateError::Unsupported {
            message,
            context: "TestMessage".to_owned(),
        })
    }

    #[test]
    fn latest_version_unstable_rules() {
        let cases = [
            ("request", "0-6", true, Ok(())),
            ("request", "0-6", false, Ok(())),
            ("request", "0+", false, Ok(())),
            // Kafka accepts a request whose only version is unstable. The
            // API then has no enabled version.
            ("request", "0", true, Ok(())),
            ("request", "none", false, Ok(())),
            (
                "request",
                "none",
                true,
                unsupported("latestVersionUnstable with empty validVersions"),
            ),
            (
                "request",
                "0+",
                true,
                unsupported("latestVersionUnstable with open-ended validVersions"),
            ),
            (
                "response",
                "0-6",
                true,
                unsupported("latestVersionUnstable on a message that is not a request"),
            ),
            (
                "header",
                "0-2",
                true,
                unsupported("latestVersionUnstable on a message that is not a request"),
            ),
            (
                "data",
                "0",
                true,
                unsupported("latestVersionUnstable on a message that is not a request"),
            ),
            ("response", "0-6", false, Ok(())),
        ];
        for (message_type, valid_versions, unstable, want) in cases {
            let spec: MessageSpec = serde_json::from_value(json!({
                "name": "TestMessage",
                "type": message_type,
                "apiKey": 22,
                "validVersions": valid_versions,
                "flexibleVersions": "0+",
                "latestVersionUnstable": unstable,
            }))
            .unwrap();
            let got = validate(&[spec]);
            assert!(
                got == want,
                "type {message_type}, validVersions {valid_versions}, unstable {unstable}"
            );
        }
    }

    #[test]
    fn vendored_schemas_validate() {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("protocol")
            .join("schemas");
        let specs = ir::load_dir(&dir).unwrap();
        // If this test fails, the generator needs an update before we can target
        // this Kafka release — surface the offending schema clearly.
        validate(&specs).unwrap_or_else(|e| panic!("validation failed: {e}"));
    }
}

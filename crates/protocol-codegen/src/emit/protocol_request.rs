//! Emit `impl crate::ProtocolRequest for <RequestType>` blocks.
//!
//! This module emits blocks only for `MessageType::Request` schemas. It
//! derives the matching Response type from the type name: it replaces the
//! `Request` suffix with `Response`, then converts the result to a
//! `snake_case` module name.

use quote::{format_ident, quote};

use crate::{
    emit::{EmitError, common::validate},
    ir::{MessageSpec, MessageType},
    name_conv,
};

/// Emit the `impl crate::ProtocolRequest for <Type>` block for a request spec.
///
/// Returns `None` if the spec is not a `MessageType::Request`.
///
/// # Errors
/// Returns an error when the emitted impl is not valid Rust, which is an
/// emitter bug.
pub fn emit_protocol_request(spec: &MessageSpec) -> Result<Option<String>, EmitError> {
    if spec.message_type != MessageType::Request {
        return Ok(None);
    }

    let type_name = name_conv::type_name(&spec.name);

    // Derive the response type name: replace trailing "Request" with "Response".
    // Every Kafka request has a matching response with exactly this naming convention.
    // Fallback: append "Response" directly (should not occur in practice).
    let response_type_name = type_name.strip_suffix("Request").map_or_else(
        || format!("{type_name}Response"),
        |prefix| format!("{prefix}Response"),
    );

    let response_module = name_conv::module_name(&response_type_name);

    let type_ident = format_ident!("{type_name}");
    let response_module_ident = format_ident!("{response_module}");
    let response_type_ident = format_ident!("{response_type_name}");

    let tokens = quote! {
        impl crate::ProtocolRequest for #type_ident {
            const API_KEY: i16 = API_KEY;
            const MIN_VERSION: i16 = MIN_VERSION;
            const MAX_VERSION: i16 = MAX_VERSION;
            const LATEST_STABLE_VERSION: i16 = LATEST_STABLE_VERSION;
            const FLEXIBLE_MIN: i16 = FLEXIBLE_MIN;
            type Response = super::#response_module_ident::#response_type_ident;
        }
    };

    // Validate at generation time; rustfmt (applied by the caller) does the
    // actual formatting. See `crate::fmt`.
    validate::<syn::ItemImpl>(&tokens, "ProtocolRequest impl")?;

    Ok(Some(format!("\n{tokens}")))
}

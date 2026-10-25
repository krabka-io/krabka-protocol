//! Records framing and parse policy for owned codecs, including non-nullable
//! request fields absent from the upstream Produce schemas.
#![cfg(test)]

use assert2::assert;
use krabka_protocol_codegen::{emit::owned_quote, ir::MessageSpec};
use quote::{ToTokens as _, quote};

#[test]
fn records_fields_keep_their_framing_and_request_response_parse_policy() {
    for response in [false, true] {
        for nullable in [false, true] {
            let mut field = serde_json::json!({
                "name": "Records", "type": "records", "versions": "0+"
            });
            if nullable {
                field["nullableVersions"] = "0+".into();
            }
            let spec: MessageSpec = serde_json::from_value(serde_json::json!({
                "name": if response { "FetchResponse" } else { "ProduceRequest" },
                "type": if response { "response" } else { "request" },
                "apiKey": i32::from(response),
                "validVersions": "0+", "flexibleVersions": "1+", "fields": [field]
            }))
            .unwrap();
            let generated = owned_quote::emit(&spec, "test").unwrap();
            let ast = syn::parse_file(&generated.primary).unwrap();
            let decode = ast
                .items
                .iter()
                .find_map(|item| match item {
                    syn::Item::Impl(item)
                        if item.trait_.as_ref().is_some_and(|(path, _)| {
                            path.segments
                                .last()
                                .is_some_and(|part| part.ident == "Decode")
                        }) =>
                    {
                        Some(item.to_token_stream().to_string())
                    }
                    _ => None,
                })
                .unwrap();
            let parse = if response {
                quote!(crate::records::RecordsPayload::from_fetch_bytes(__rb_bytes))
            } else {
                quote!(crate::records::RecordsPayload::from_bytes(__rb_bytes))
            };
            assert!(decode.contains(&parse.to_string()));
            let framing = if nullable {
                quote!(if flex {
                    get_compact_nullable_bytes_owned(buf)?
                } else {
                    get_nullable_bytes_owned(buf)?
                })
            } else {
                quote!(if flex {
                    get_compact_bytes_owned(buf)?
                } else {
                    get_bytes_owned(buf)?
                })
            };
            assert!(decode.contains(&framing.to_string()));
        }
    }
}

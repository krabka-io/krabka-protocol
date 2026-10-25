//! The token helpers that every emitter shares.
#![cfg(test)]
use assert2::assert;
use krabka_protocol_codegen::emit::{
    common::{format_int_literal, parse_tokens, statement},
    owned::EmitError,
};
use quote::quote;

#[test]
fn parse_tokens_reports_the_fragment_it_cannot_lex() {
    assert!(let Err(EmitError::Lex { fragment, .. }) = parse_tokens("(\"unclosed"));
    assert!(fragment == "(\"unclosed");
}

#[test]
fn statement_adds_a_semicolon_unless_the_expression_is_block_like() {
    for (expr, want) in [
        (quote!(f()), quote!(f();)),
        (quote!(-x), quote!(-x;)),
        (quote!(1), quote!(1;)),
        (
            quote!(if a {
                b
            }),
            quote!(if a {
                b
            }),
        ),
        (
            quote!(match a {
                _ => b,
            }),
            quote!(match a {
                _ => b,
            }),
        ),
        (quote!(for a in b {}), quote!(for a in b {})),
        (quote!({ a }), quote!({ a })),
        (quote!((a)), quote!((a);)),
    ] {
        assert!(statement(&expr).to_string() == want.to_string(), "{expr}");
    }
}

#[test]
fn format_int_literal_normalises_hex_and_groups_digits() {
    for (input, suffix, want) in [
        ("0", "i32", "0i32"),
        ("-1", "i32", "-1i32"),
        ("1000000", "i64", "1_000_000i64"),
        ("0x7fffffff", "i32", "2_147_483_647i32"),
        ("0xffffffff", "i32", "-1i32"),
        ("0xff", "i64", "255i64"),
        ("0xff", "u16", "255u16"),
        ("-0x10", "i32", "-16i32"),
    ] {
        assert!(
            format_int_literal(input, suffix) == want,
            "{input} {suffix}"
        );
    }
}

//! The pretty-printing pass that runs before rustfmt.
//!
//! It needs no rustfmt binary, so it runs under Cargo and Bazel alike.
#![cfg(test)]
use assert2::assert;
use krabka_protocol_codegen::fmt::prettyprint;

#[test]
fn keeps_the_banner_and_hoists_long_paths() {
    let src =
        "// AUTO-GENERATED banner.\n// Second line.\n\nfn f ( ) { crate :: a :: b :: g ( ) ; }";
    let want = "// AUTO-GENERATED banner.\n// Second line.\n\nuse crate::a::b::g;\nfn f() {\n    g();\n}\n";
    assert!(prettyprint(src) == want);
}

#[test]
fn returns_a_body_that_does_not_parse_unchanged() {
    let src = "// banner\nfn f( {";
    assert!(prettyprint(src) == src);
}

#[test]
fn a_file_with_no_banner_is_only_pretty_printed() {
    assert!(prettyprint("struct S ;") == "struct S;\n");
}

//! The absolute-path hoisting pass, on source parsed in memory.
//!
//! These tests need no schemas and no rustfmt, so they run under Cargo and
//! Bazel alike. Each case compares token text, which is independent of
//! formatting.
#![cfg(test)]
use assert2::check;
use krabka_protocol_codegen::hoist::hoist_absolute_paths;
use quote::ToTokens as _;

/// `src` after the pass, as token text.
fn hoisted(src: &str) -> String {
    let mut file = syn::parse_file(src).unwrap();
    hoist_absolute_paths(&mut file);
    file.into_token_stream().to_string()
}

/// `src` as token text, with no pass.
fn tokens(src: &str) -> String {
    syn::parse_file(src)
        .unwrap()
        .into_token_stream()
        .to_string()
}

/// Check that the pass turns each `src` into `want`.
fn check_cases(cases: &[(&str, &str, &str)]) {
    for (name, src, want) in cases {
        check!(hoisted(src) == tokens(want), "{name}");
    }
}

#[test]
fn hoists_each_path_to_the_deepest_name_that_does_not_clash() {
    check_cases(&[
        (
            "a function imports by its own name",
            "fn f() { crate::a::b::g(); }",
            "use crate::a::b::g; fn f() { g(); }",
        ),
        (
            "an associated item keeps its type",
            "fn f() { crate::m::n::Ty::new(); }",
            "use crate::m::n::Ty; fn f() { Ty::new(); }",
        ),
        (
            "an extern crate path is hoisted",
            "fn f() { bytes::buf::Writer::new(); }",
            "use bytes::buf::Writer; fn f() { Writer::new(); }",
        ),
        (
            "a leading `::` is kept on the import",
            "fn f() { ::bytes::buf::Writer::new(); }",
            "use ::bytes::buf::Writer; fn f() { Writer::new(); }",
        ),
        (
            "a std path is hoisted",
            "fn f() -> std::collections::BTreeMap<u8, u8> { todo!() }",
            "use std::collections::BTreeMap; fn f() -> BTreeMap<u8, u8> { todo!() }",
        ),
        (
            "one import serves every use of a path",
            "fn f() { crate::a::b::g(); crate::a::b::g(); }",
            "use crate::a::b::g; fn f() { g(); g(); }",
        ),
        (
            "a new import goes after the existing ones",
            "use bytes::Buf; fn f() { crate::a::b::g(); }",
            "use bytes::Buf; use crate::a::b::g; fn f() { g(); }",
        ),
        (
            "a glob import binds no name, so its paths read as crate-rooted",
            "use crate::a::*; fn f() { b::c::g(); }",
            "use crate::a::*; use b::c::g; fn f() { g(); }",
        ),
        (
            "a path inside a generic argument is hoisted",
            "fn f() -> Vec<crate::a::b::T> { todo!() }",
            "use crate::a::b::T; fn f() -> Vec<T> { todo!() }",
        ),
    ]);
}

#[test]
fn falls_back_to_the_parent_module_on_a_collision() {
    check_cases(&[
        (
            "a struct the file defines",
            "struct Data; fn f() -> crate::owned::m::Data { todo!() }",
            "use crate::owned::m; struct Data; fn f() -> m::Data { todo!() }",
        ),
        (
            "a name two paths would import",
            "fn f() { crate::a::x::get(1); crate::a::y::get(2); }",
            "use crate::a::x; use crate::a::y; fn f() { x::get(1); y::get(2); }",
        ),
        (
            "a local variable blocks a function import but not its module",
            "fn f(get: u8) { crate::a::b::get(get); }",
            "use crate::a::b; fn f(get: u8) { b::get(get); }",
        ),
        (
            "a module-scope import of the name from elsewhere",
            "use other::g; fn f() { crate::a::b::g(); }",
            "use other::g; use crate::a::b; fn f() { b::g(); }",
        ),
        (
            "a renamed import binds the new name",
            "use other::h as g; fn f() { crate::a::b::g(); }",
            "use other::h as g; use crate::a::b; fn f() { b::g(); }",
        ),
        (
            "a grouped import binds every name in it",
            "use other::{g, h}; fn f() { crate::a::b::h(); }",
            "use other::{g, h}; use crate::a::b; fn f() { b::h(); }",
        ),
        (
            "a prelude name from a different path",
            "fn f() -> crate::a::b::Option { todo!() }",
            "use crate::a::b; fn f() -> b::Option { todo!() }",
        ),
        (
            "a generic parameter of the same name",
            "fn f<T>(_: T) -> crate::a::b::T { todo!() }",
            "use crate::a::b; fn f<T>(_: T) -> b::T { todo!() }",
        ),
        (
            "a use nested in a function body",
            "fn f() { use other::g; crate::a::b::g(); }",
            "use crate::a::b; fn f() { use other::g; b::g(); }",
        ),
        (
            "two levels of collision fall back two levels",
            "struct T; mod b {} fn f() -> crate::a::b::T { todo!() }",
            "use crate::a; struct T; mod b {} fn f() -> a::b::T { todo!() }",
        ),
    ]);
}

#[test]
fn every_kind_of_item_binds_its_name() {
    for item in [
        "const T: u8 = 0;",
        "enum T {}",
        "extern crate T;",
        "extern crate other as T;",
        "fn T() {}",
        "macro_rules! T { () => {}; }",
        "mod T {}",
        "static T: u8 = 0;",
        "struct T;",
        "trait T {}",
        "trait T = Clone;",
        "type T = u8;",
        "union T { a: u8 }",
    ] {
        let src = format!("{item} fn f() -> crate::a::b::T {{ todo!() }}");
        let want = format!("use crate::a::b; {item} fn f() -> b::T {{ todo!() }}");
        check!(hoisted(&src) == tokens(&want), "{item}");
    }
}

#[test]
fn a_path_that_clashes_at_every_level_stays_whole() {
    for src in [
        "struct T; mod b {} mod a {} fn f() -> crate::a::b::T { todo!() }",
        "fn f(g: u8) { use other::a; use other::b; crate::a::b::g(g); }",
    ] {
        check!(hoisted(src) == tokens(src), "{src}");
    }
}

#[test]
fn reuses_an_existing_import_and_the_prelude() {
    check_cases(&[
        (
            "an existing import of the same path",
            "use crate::a::b::g; fn f() { crate::a::b::g(); }",
            "use crate::a::b::g; fn f() { g(); }",
        ),
        (
            "a prelude type",
            "fn f(v: Option<&Vec<u8>>) { v.map(std::vec::Vec::len); }",
            "fn f(v: Option<&Vec<u8>>) { v.map(Vec::len); }",
        ),
        (
            "a prelude variant keeps its prelude type",
            "fn f() -> Option<u8> { core::option::Option::Some(1) }",
            "fn f() -> Option<u8> { Option::Some(1) }",
        ),
    ]);
}

#[test]
fn rewrites_paths_in_expression_macros() {
    check_cases(&[
        (
            "a vec! element",
            "fn f() { vec![crate::a::b::Uuid(1)]; }",
            "use crate::a::b::Uuid; fn f() { vec![Uuid(1)]; }",
        ),
        (
            "an assert! argument next to a plain one",
            "fn f() { assert!(x, crate::a::b::g()); }",
            "use crate::a::b::g; fn f() { assert!(x, g()); }",
        ),
    ]);
    // A macro whose arguments hold no hoistable path keeps its tokens.
    let src = "fn f() { vec![1, 2]; crate::a::b::g(); }";
    let want = "use crate::a::b::g; fn f() { vec![1, 2]; g(); }";
    check!(hoisted(src) == tokens(want));
}

#[test]
fn hoists_the_self_type_of_a_qualified_path_but_not_the_trait() {
    check_cases(&[
        (
            "in an expression",
            "fn f() { <crate::a::b::T as crate::Tr>::g(); }",
            "use crate::a::b::T; fn f() { <T as crate::Tr>::g(); }",
        ),
        (
            "in a type",
            "fn f() -> <crate::a::b::T as crate::x::y::Tr>::Out { todo!() }",
            "use crate::a::b::T; fn f() -> <T as crate::x::y::Tr>::Out { todo!() }",
        ),
    ]);
}

#[test]
fn leaves_short_relative_and_skipped_paths_alone() {
    for src in [
        "fn f() { crate::g(); bytes::Bytes::new(); }",
        "fn f() { self::a::b::g(); }",
        "fn f() { super::a::b::g(); }",
        "fn f() { Upper::a::b::g(); }",
        "use crate::a::b; fn f() { b::c::g(); }",
        "mod m { pub(in crate::a::b) fn g() {} }",
        "use crate::a::b::c::d;",
        "fn f() { json!({ \"k\": crate::a::b::g() }); }",
    ] {
        check!(hoisted(src) == tokens(src), "{src}");
    }
}

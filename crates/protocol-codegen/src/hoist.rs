//! Hoist absolute paths in generated code into module-scope `use` items.
//!
//! The emitters name an item from another module by its full path, such as
//! `crate::primitives::array::put_array_len` or
//! `crate::owned::fetch_request::PartitionData`. That keeps each emitter local:
//! it never has to know which names the rest of the file imports. The
//! workspace denies `clippy::absolute_paths` beyond two segments, though, so
//! the committed code must name such an item through an import.
//!
//! [`hoist_absolute_paths`] closes that gap on the parsed file. It finds every
//! path that starts at a crate root and names an item through three or more
//! segments, as the lint counts them, picks the
//! deepest segment that it can import without a name clash, adds one `use`
//! item per imported path, and shortens each path to start at the imported
//! name. A name that two different paths would import, or that the file
//! already binds, falls back to its parent module, so
//! `crate::owned::fetch_request::PartitionData` in a file that defines its own
//! `PartitionData` becomes `fetch_request::PartitionData`.
//!
//! A generated file is `include!`d into its wrapper module, so its module-scope
//! `use` items are that module's imports.

use std::{
    collections::{BTreeMap, BTreeSet},
    mem,
};

use proc_macro2::Span;
use quote::ToTokens;
use syn::{
    Expr, ExprPath, File, Ident, Item, ItemUse, Macro, PatIdent, Path, Token, TypeParam, TypePath,
    UseName, UsePath, UseTree, Visibility,
    punctuated::Punctuated,
    visit::{self, Visit},
    visit_mut::{self, VisitMut},
};

/// The longest crate-rooted path that `clippy::absolute_paths` accepts, as
/// `clippy.toml` sets `absolute-paths-max-segments`.
const MAX_SEGMENTS: usize = 2;

/// The crate roots whose paths count as absolute even when nothing in the file
/// names them: the standard library facades.
const STD_ROOTS: [&str; 3] = ["std", "core", "alloc"];

/// The standard-prelude names a generated file uses through a full path, with
/// the path below the `std` root that each one names.
///
/// Such a name needs no `use` item, and no other path may import it.
const PRELUDE: [(&str, &[&str]); 14] = [
    ("Box", &["boxed", "Box"]),
    ("Clone", &["clone", "Clone"]),
    ("Default", &["default", "Default"]),
    ("Err", &["result", "Result", "Err"]),
    ("From", &["convert", "From"]),
    ("Into", &["convert", "Into"]),
    ("None", &["option", "Option", "None"]),
    ("Ok", &["result", "Result", "Ok"]),
    ("Option", &["option", "Option"]),
    ("Result", &["result", "Result"]),
    ("Some", &["option", "Option", "Some"]),
    ("String", &["string", "String"]),
    ("ToString", &["string", "ToString"]),
    ("Vec", &["vec", "Vec"]),
];

/// A path, keyed by its leading `::` and its segment names, without generic
/// arguments.
type PathKey = (bool, Vec<String>);

/// Shorten every crate-rooted path in `file` that names an item through three
/// or more segments to start at an imported name, and add the `use` items that
/// import those names.
///
/// The function also shortens the paths in a macro call whose body is a list
/// of expressions, such as `vec![..]`. It leaves `use` items, visibility
/// paths, qualified paths such as `<T as Trait>::f`, and every other macro
/// body alone. A path that it cannot shorten without a clash at every level
/// stays as it is.
pub fn hoist_absolute_paths(file: &mut File) {
    let scope = Scope::of(file);
    let mut collector = Collector {
        scope: &scope,
        paths: BTreeSet::new(),
    };
    collector.visit_file(file);
    let levels = choose_levels(&scope, &collector.paths);
    if levels.is_empty() {
        return;
    }

    let imports: BTreeSet<PathKey> = levels
        .iter()
        .map(|((colon, segs), &level)| (*colon, segs.iter().take(level + 1).cloned().collect()))
        .filter(|import| !scope.already_imports(import) && !is_prelude(import))
        .collect();

    Rewriter {
        scope: &scope,
        levels: &levels,
        rewritten: 0,
    }
    .visit_file_mut(file);

    let at = file
        .items
        .iter()
        .rposition(|item| matches!(item, Item::Use(_)))
        .map_or(0, |last| last + 1);
    let new_items = imports.iter().filter_map(use_item).map(Item::Use);
    file.items.splice(at..at, new_items);
}

/// The names a file binds, split by where they bind them.
#[derive(Debug, Default)]
struct Scope {
    /// Module-scope `use` items: the bound name and the path it imports.
    imported: BTreeMap<String, PathKey>,
    /// Names in the type namespace that a path segment could resolve to: the
    /// module-scope items, every nested item and `use`, and generic parameters.
    types: BTreeSet<String>,
    /// Names in the value namespace bound by a pattern, such as a local
    /// variable or a parameter.
    values: BTreeSet<String>,
}

impl Scope {
    fn of(file: &File) -> Self {
        let mut scope = Self::default();
        for item in &file.items {
            if let Item::Use(item_use) = item {
                use_bindings(
                    &item_use.tree,
                    &mut Vec::new(),
                    item_use.leading_colon.is_some(),
                    &mut |name, path| {
                        scope.imported.insert(name, path);
                    },
                );
            }
        }
        let mut binder = Binder {
            scope: &mut scope,
            depth: 0,
        };
        binder.visit_file(file);
        scope
    }

    /// True when `name` is bound in the file other than by a module-scope
    /// `use` item.
    fn binds_type(&self, name: &str) -> bool {
        self.types.contains(name)
    }

    /// True when `path` starts at a crate root rather than at a name the file
    /// binds.
    fn is_crate_rooted(&self, path: &Path) -> bool {
        let Some(first) = path.segments.first() else {
            return false;
        };
        let name = first.ident.to_string();
        if path.leading_colon.is_some() || name == "crate" || STD_ROOTS.contains(&name.as_str()) {
            return true;
        }
        let starts_lowercase = name.starts_with(|c: char| c.is_ascii_lowercase());
        starts_lowercase
            && name != "self"
            && name != "super"
            && !self.imported.contains_key(&name)
            && !self.binds_type(&name)
    }

    /// True when a module-scope `use` item already imports exactly `import`.
    fn already_imports(&self, import: &PathKey) -> bool {
        import
            .1
            .last()
            .and_then(|name| self.imported.get(name))
            .is_some_and(|bound| bound == import)
    }
}

/// Walks a file and records the names it binds outside its module-scope `use`
/// items.
struct Binder<'s> {
    scope: &'s mut Scope,
    /// How many items deep the walk is. A `use` at depth zero is a
    /// module-scope import, which [`Scope::of`] records separately.
    depth: usize,
}

impl Binder<'_> {
    fn bind_item(&mut self, item: &Item) {
        let ident = match item {
            Item::Const(i) => Some(&i.ident),
            Item::Enum(i) => Some(&i.ident),
            Item::ExternCrate(i) => Some(i.rename.as_ref().map_or(&i.ident, |(_, rename)| rename)),
            Item::Fn(i) => Some(&i.sig.ident),
            Item::Macro(i) => i.ident.as_ref(),
            Item::Mod(i) => Some(&i.ident),
            Item::Static(i) => Some(&i.ident),
            Item::Struct(i) => Some(&i.ident),
            Item::Trait(i) => Some(&i.ident),
            Item::TraitAlias(i) => Some(&i.ident),
            Item::Type(i) => Some(&i.ident),
            Item::Union(i) => Some(&i.ident),
            Item::Use(i) if self.depth > 0 => {
                let types = &mut self.scope.types;
                use_bindings(
                    &i.tree,
                    &mut Vec::new(),
                    i.leading_colon.is_some(),
                    &mut |name, _| {
                        types.insert(name);
                    },
                );
                None
            }
            _ => None,
        };
        if let Some(ident) = ident {
            self.scope.types.insert(ident.to_string());
        }
    }
}

impl<'ast> Visit<'ast> for Binder<'_> {
    fn visit_item(&mut self, item: &'ast Item) {
        self.bind_item(item);
        self.depth += 1;
        visit::visit_item(self, item);
        self.depth -= 1;
    }

    fn visit_pat_ident(&mut self, pat: &'ast PatIdent) {
        self.scope.values.insert(pat.ident.to_string());
        visit::visit_pat_ident(self, pat);
    }

    fn visit_type_param(&mut self, param: &'ast TypeParam) {
        self.scope.types.insert(param.ident.to_string());
        visit::visit_type_param(self, param);
    }
}

/// Call `bind` with each name a `use` tree binds and the path it imports. A
/// glob binds no single name, so the function skips it.
fn use_bindings(
    tree: &UseTree,
    prefix: &mut Vec<String>,
    leading_colon: bool,
    bind: &mut impl FnMut(String, PathKey),
) {
    match tree {
        UseTree::Path(p) => {
            prefix.push(p.ident.to_string());
            use_bindings(&p.tree, prefix, leading_colon, bind);
            prefix.pop();
        }
        UseTree::Name(n) => {
            let mut path = prefix.clone();
            path.push(n.ident.to_string());
            bind(n.ident.to_string(), (leading_colon, path));
        }
        UseTree::Rename(r) => {
            let mut path = prefix.clone();
            path.push(r.ident.to_string());
            bind(r.rename.to_string(), (leading_colon, path));
        }
        UseTree::Group(g) => {
            for tree in &g.items {
                use_bindings(tree, prefix, leading_colon, bind);
            }
        }
        UseTree::Glob(_) => {}
    }
}

/// The key of `path`: its leading `::` and its segment names.
fn key_of(path: &Path) -> PathKey {
    (
        path.leading_colon.is_some(),
        path.segments.iter().map(|s| s.ident.to_string()).collect(),
    )
}

/// True when `path` is crate-rooted and names an item through more segments
/// than `clippy::absolute_paths` accepts.
///
/// Like the lint, this function measures only the path to the item. A member
/// of a type, such as `bytes::Bytes::new`, counts as the path to its type.
fn is_hoistable(scope: &Scope, path: &Path) -> bool {
    let (_, segs) = key_of(path);
    // The path to the item runs through segment `deepest_level`, so it has one
    // segment more than that index.
    deepest_level(&segs) >= MAX_SEGMENTS && scope.is_crate_rooted(path)
}

/// The arguments of a macro call whose body is a comma-separated list of
/// expressions, such as `vec![..]` or `assert!(..)`. Any other body, such as a
/// `json!` object, fails to parse, and the walk leaves it alone.
fn macro_args(mac: &Macro) -> syn::Result<Punctuated<Expr, Token![,]>> {
    mac.parse_body_with(Punctuated::parse_terminated)
}

/// Collects the key of every hoistable path in a file.
struct Collector<'s> {
    scope: &'s Scope,
    paths: BTreeSet<PathKey>,
}

impl<'ast> Visit<'ast> for Collector<'_> {
    fn visit_item_use(&mut self, _: &'ast ItemUse) {}

    fn visit_visibility(&mut self, _: &'ast Visibility) {}

    fn visit_expr_path(&mut self, expr: &'ast ExprPath) {
        match &expr.qself {
            Some(qself) => self.visit_type(&qself.ty),
            None => visit::visit_expr_path(self, expr),
        }
    }

    fn visit_type_path(&mut self, ty: &'ast TypePath) {
        match &ty.qself {
            Some(qself) => self.visit_type(&qself.ty),
            None => visit::visit_type_path(self, ty),
        }
    }

    fn visit_macro(&mut self, mac: &'ast Macro) {
        if let Ok(args) = macro_args(mac) {
            for arg in &args {
                self.visit_expr(arg);
            }
        }
    }

    fn visit_path(&mut self, path: &'ast Path) {
        if is_hoistable(self.scope, path) {
            self.paths.insert(key_of(path));
        }
        visit::visit_path(self, path);
    }
}

/// Rewrites every hoisted path to start at its imported name.
struct Rewriter<'s> {
    scope: &'s Scope,
    levels: &'s BTreeMap<PathKey, usize>,
    /// How many paths the walk has shortened so far.
    rewritten: usize,
}

impl VisitMut for Rewriter<'_> {
    fn visit_item_use_mut(&mut self, _: &mut ItemUse) {}

    fn visit_visibility_mut(&mut self, _: &mut Visibility) {}

    fn visit_expr_path_mut(&mut self, expr: &mut ExprPath) {
        match &mut expr.qself {
            Some(qself) => self.visit_type_mut(&mut qself.ty),
            None => visit_mut::visit_expr_path_mut(self, expr),
        }
    }

    fn visit_type_path_mut(&mut self, ty: &mut TypePath) {
        match &mut ty.qself {
            Some(qself) => self.visit_type_mut(&mut qself.ty),
            None => visit_mut::visit_type_path_mut(self, ty),
        }
    }

    fn visit_macro_mut(&mut self, mac: &mut Macro) {
        let Ok(mut args) = macro_args(mac) else {
            return;
        };
        let before = self.rewritten;
        for arg in &mut args {
            self.visit_expr_mut(arg);
        }
        if self.rewritten != before {
            mac.tokens = args.into_token_stream();
        }
    }

    fn visit_path_mut(&mut self, path: &mut Path) {
        if is_hoistable(self.scope, path)
            && let Some(&level) = self.levels.get(&key_of(path))
        {
            let segments = mem::take(&mut path.segments);
            path.segments = segments.into_pairs().skip(level).collect();
            path.leading_colon = None;
            self.rewritten += 1;
        }
        visit_mut::visit_path_mut(self, path);
    }
}

/// The deepest segment index a path may import at: its last segment, or its
/// first type-like (capitalised) segment, because the members of a type or
/// trait cannot be imported on their own.
fn deepest_level(segs: &[String]) -> usize {
    let last = segs.len().saturating_sub(1);
    segs.iter()
        .enumerate()
        .skip(1)
        .find(|(_, s)| s.starts_with(|c: char| c.is_ascii_uppercase()))
        .map_or(last, |(i, _)| i)
}

/// The prelude path a name stands for, if it is a prelude name.
fn prelude_path(name: &str) -> Option<&'static [&'static str]> {
    PRELUDE
        .iter()
        .find_map(|(prelude, path)| (*prelude == name).then_some(*path))
}

/// True when `import` names a prelude item through the standard library, so
/// the item is already in scope.
fn is_prelude(import: &PathKey) -> bool {
    let Some((root, rest)) = import.1.split_first() else {
        return false;
    };
    STD_ROOTS.contains(&root.as_str())
        && rest
            .last()
            .and_then(|name| prelude_path(name))
            .is_some_and(|path| path.iter().copied().eq(rest.iter().map(String::as_str)))
}

/// Pick the segment each hoistable path imports at, as an index into its
/// segments. A path that clashes at every level is left out.
///
/// Every path starts at its [`deepest_level`]. A path moves one level up while
/// the name it would import clashes: the file binds it, a module-scope `use`
/// binds it to a different path, it is a prelude name for a different path,
/// or another path would import the same name from a different place. Levels
/// only fall, so the loop ends.
fn choose_levels(scope: &Scope, paths: &BTreeSet<PathKey>) -> BTreeMap<PathKey, usize> {
    let mut levels: BTreeMap<PathKey, usize> = paths
        .iter()
        .map(|p| (p.clone(), deepest_level(&p.1)))
        .collect();
    let mut changed = true;
    while changed {
        changed = false;
        let claims = claims(&levels);
        let mut dropped = Vec::new();
        for (path, level) in &mut levels {
            let Some((import, name)) = import_at(path, *level) else {
                dropped.push(path.clone());
                continue;
            };
            if clashes(scope, &claims, &import, name, *level + 1 < path.1.len()) {
                changed = true;
                if *level > 1 {
                    *level -= 1;
                } else {
                    dropped.push(path.clone());
                }
            }
        }
        for path in dropped {
            levels.remove(&path);
        }
    }
    levels
}

/// The import a path takes at `level`, and the name it binds.
fn import_at(path: &PathKey, level: usize) -> Option<(PathKey, &str)> {
    let name = path.1.get(level)?;
    let import = (path.0, path.1.iter().take(level + 1).cloned().collect());
    Some((import, name.as_str()))
}

/// Every name the current levels import, with each distinct path that imports
/// it.
fn claims(levels: &BTreeMap<PathKey, usize>) -> BTreeMap<String, BTreeSet<PathKey>> {
    let mut claims: BTreeMap<String, BTreeSet<PathKey>> = BTreeMap::new();
    for (path, &level) in levels {
        if let Some((import, name)) = import_at(path, level) {
            claims.entry(name.to_owned()).or_default().insert(import);
        }
    }
    claims
}

/// True when importing `import` under `name` would clash with another binding.
///
/// `prefix_only` says the name only ever opens a longer path. Such a name
/// resolves in the type namespace, so a local variable of the same name does
/// not shadow it.
fn clashes(
    scope: &Scope,
    claims: &BTreeMap<String, BTreeSet<PathKey>>,
    import: &PathKey,
    name: &str,
    prefix_only: bool,
) -> bool {
    if scope.binds_type(name) || (!prefix_only && scope.values.contains(name)) {
        return true;
    }
    if scope
        .imported
        .get(name)
        .is_some_and(|bound| bound != import)
    {
        return true;
    }
    if prelude_path(name).is_some() && !is_prelude(import) {
        return true;
    }
    claims.get(name).is_some_and(|paths| paths.len() > 1)
}

/// A `use` item that imports `import`, or `None` for an empty path.
fn use_item(import: &PathKey) -> Option<ItemUse> {
    let (leading_colon, segs) = import;
    let (name, parents) = segs.split_last()?;
    let mut tree = UseTree::Name(UseName {
        ident: Ident::new(name, Span::call_site()),
    });
    for parent in parents.iter().rev() {
        tree = UseTree::Path(UsePath {
            ident: Ident::new(parent, Span::call_site()),
            colon2_token: <Token![::]>::default(),
            tree: Box::new(tree),
        });
    }
    Some(ItemUse {
        attrs: Vec::new(),
        vis: Visibility::Inherited,
        use_token: <Token![use]>::default(),
        leading_colon: leading_colon.then(<Token![::]>::default),
        tree,
        semi_token: <Token![;]>::default(),
    })
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

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

    #[test]
    fn hoists_each_path_to_the_deepest_name_that_does_not_clash() {
        let cases = [
            (
                "a function imports by its own name",
                "fn f() { crate::a::b::g(); }",
                "use crate::a::b::g; fn f() { g(); }",
            ),
            (
                "a name the file defines falls back to its module",
                "struct Data; fn f() -> crate::owned::m::Data { todo!() }",
                "use crate::owned::m; struct Data; fn f() -> m::Data { todo!() }",
            ),
            (
                "a name two paths would import falls back to each module",
                "fn f() { crate::a::x::get(1); crate::a::y::get(2); }",
                "use crate::a::x; use crate::a::y; fn f() { x::get(1); y::get(2); }",
            ),
            (
                "an existing import is reused",
                "use crate::a::b::g; fn f() { crate::a::b::g(); }",
                "use crate::a::b::g; fn f() { g(); }",
            ),
            (
                "an associated item keeps its type",
                "fn f() { crate::m::n::Ty::new(); }",
                "use crate::m::n::Ty; fn f() { Ty::new(); }",
            ),
            (
                "a prelude type needs no import",
                "fn f(v: Option<&Vec<u8>>) { v.map(std::vec::Vec::len); }",
                "fn f(v: Option<&Vec<u8>>) { v.map(Vec::len); }",
            ),
            (
                "a path inside an expression macro is hoisted",
                "fn f() { vec![crate::a::b::Uuid(1)]; }",
                "use crate::a::b::Uuid; fn f() { vec![Uuid(1)]; }",
            ),
            (
                "the self type of a qualified path is hoisted, the trait path is not",
                "fn f() { <crate::a::b::T as crate::Tr>::g(); }",
                "use crate::a::b::T; fn f() { <T as crate::Tr>::g(); }",
            ),
            (
                "a local variable blocks a function import but not its module",
                "fn f(get: u8) { crate::a::b::get(get); }",
                "use crate::a::b; fn f(get: u8) { b::get(get); }",
            ),
            (
                "an extern crate path is hoisted",
                "fn f() { bytes::buf::Writer::new(); }",
                "use bytes::buf::Writer; fn f() { Writer::new(); }",
            ),
            (
                "a new import goes after the existing ones",
                "use bytes::Buf; fn f() { crate::a::b::g(); }",
                "use bytes::Buf; use crate::a::b::g; fn f() { g(); }",
            ),
        ];
        for (name, src, want) in cases {
            check!(hoisted(src) == tokens(want), "{name}");
        }
    }

    #[test]
    fn leaves_short_and_relative_paths_alone() {
        for src in [
            "fn f() { crate::g(); bytes::Bytes::new(); }",
            "fn f() { self::a::b::g(); }",
            "fn f() { super::a::b::g(); }",
            "use crate::a::b; fn f() { b::c::g(); }",
            "fn f() { json!({ \"k\": crate::a::b::g() }); }",
        ] {
            check!(hoisted(src) == tokens(src), "{src}");
        }
    }
}

"""Bazel macros for this repository's Cargo workspace members.

Every knob these macros need — crate name, edition, feature set, dependency
labels, workspace lints — already exists in the `@crates` repo that
`crate.from_cargo` generates from `Cargo.toml`/`Cargo.lock`. Reading it from
there rather than restating it per crate keeps the BUILD files from drifting
away from the manifests Cargo still resolves.
"""

load("@crates//:data.bzl", "DEP_DATA")
load("@crates//:defs.bzl", "aliases", "all_crate_deps", "crate_name", "edition")
load("@rules_rs//rs:rust_binary.bzl", "rust_binary")
load("@rules_rs//rs:rust_library.bzl", "rust_library")
load("@rules_rs//rs:rust_test.bzl", "rust_test")
load("@rules_rs_mutants//mutants:cargo_mutants_test.bzl", "cargo_mutants_test")

# `[workspace.lints.rust] unsafe_code = "forbid"`. rules_rs 0.0.106 does not
# yet plumb Cargo lint tables into the Bazel build, and this is the one lint
# in that table whose guarantee must not lapse under a second build system.
# The clippy tables stay a Cargo-side gate: clippy runs as an aspect here, not
# as part of a normal build.
WORKSPACE_RUSTC_FLAGS = ["-Funsafe_code"]

def _features():
    return DEP_DATA[native.package_name()]["crate_features"]

def crate_library(name, srcs = None, **kwargs):
    """`rust_library` for a workspace member, configured from Cargo metadata."""
    rust_library(
        name = name,
        srcs = srcs if srcs != None else native.glob(
            ["src/**/*.rs"],
            exclude = ["src/bin/**"],
        ),
        aliases = aliases(),
        crate_features = _features(),
        crate_name = crate_name(),
        edition = edition(),
        rustc_flags = WORKSPACE_RUSTC_FLAGS,
        visibility = ["//visibility:public"],
        deps = all_crate_deps(normal = True),
        **kwargs
    )

def crate_binary(name, crate_root, lib, **kwargs):
    """`rust_binary` for a `[[bin]]` target that links its own crate's library."""
    rust_binary(
        name = name,
        srcs = [crate_root],
        aliases = aliases(),
        crate_features = _features(),
        crate_root = crate_root,
        edition = edition(),
        rustc_flags = WORKSPACE_RUSTC_FLAGS,
        visibility = ["//visibility:public"],
        deps = all_crate_deps(normal = True) + [lib],
        **kwargs
    )

def crate_tests(
        lib,
        data = None,
        compile_data = None,
        env = {},
        rustc_env = {},
        manual = [],
        no_harness = [],
        mutants = True,
        mutants_jobs = 4,
        mutants_shards = 8,
        unit_tags = []):
    """Unit tests, one target per `tests/*.rs`, and a mutation sweep.

    Args:
      lib: the `crate_library` target name in this package.
      data: runtime files every integration test gets (fixtures, corpora).
      compile_data: files reachable from `include!`/`include_str!` at compile time.
      env: runtime environment for every test target in the package.
      rustc_env: extra compile-time environment, e.g. `CARGO_MANIFEST_DIR`.
      manual: test stems to tag `manual` — Docker-driven or otherwise
        non-hermetic suites, the Bazel equivalent of their `#[ignore]`.
      no_harness: test stems declared `harness = false` in Cargo.toml.
      mutants: whether to emit a `cargo_mutants_test` over the unit tests.
      mutants_jobs: mutants built and tested concurrently within one shard.
      mutants_shards: Bazel shards the sweep is split across.
      unit_tags: extra tags for the unit-test target.
    """
    unit = lib + "_test"
    rust_test(
        name = unit,
        aliases = aliases(),
        crate = ":" + lib,
        compile_data = compile_data or [],
        crate_features = _features(),
        data = data or [],
        edition = edition(),
        env = env,
        rustc_env = rustc_env,
        rustc_flags = WORKSPACE_RUSTC_FLAGS,
        tags = unit_tags,
        deps = all_crate_deps(normal_dev = True),
    )

    if mutants:
        # `manual`: a full sweep rebuilds the crate once per mutant, so it runs
        # from the nightly job rather than on every `bazel test //...`.
        cargo_mutants_test(
            name = lib + "_mutants",
            timeout = "eternal",
            jobs = mutants_jobs,
            shard_count = mutants_shards,
            tags = ["manual"],
            test = ":" + unit,
        )

    # Shared helper modules live in `tests/<name>/mod.rs` and are declared with
    # `mod <name>;` by whichever suites need them, so every integration test
    # gets them in `srcs` and names its own file as the crate root.
    helpers = native.glob(
        ["tests/**/*.rs"],
        exclude = ["tests/*.rs"],
        allow_empty = True,
    )

    for src in native.glob(["tests/*.rs"], allow_empty = True):
        stem = src[len("tests/"):-len(".rs")]
        rust_test(
            name = stem + "_test",
            srcs = [src] + helpers,
            crate_root = src,
            aliases = aliases(),
            compile_data = compile_data or [],
            crate_features = _features(),
            data = data or [],
            edition = edition(),
            env = env,
            rustc_env = rustc_env,
            rustc_flags = WORKSPACE_RUSTC_FLAGS,
            tags = ["manual"] if stem in manual else [],
            use_libtest_harness = stem not in no_harness,
            # One call, not two concatenated: an integration test links the
            # crate's normal *and* dev dependencies, and several crates list the
            # same package in both tables. `all_crate_deps` merges the two specs
            # through a set, so asking for both at once dedupes them.
            deps = all_crate_deps(normal = True, normal_dev = True) + [":" + lib],
        )

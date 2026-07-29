//! Machine-checkable divergence registry: completeness and doc match.
//!
//! The registry in `crates/kubo-conformance/src/divergence.rs` is the source of
//! truth for known fake-vs-real divergences. These tests enforce the two
//! bidirectional checks the registry names:
//!
//! - Every `assert_known_divergence(DivId::X)` call in the test files resolves
//!   to a registry id. Compile-enforced via the `DivId` enum — a non-existent
//!   id fails to compile.
//! - Every non-`StripTodo` registry id is referenced by at least one
//!   `assert_known_divergence(DivId::X)` call in the test files. Scan-enforced
//!   by [`every_pinned_id_is_referenced_by_a_test`].
//! - Every registry entry's `pinned_by` test name exists as a function
//!   definition in the named test file. Enforced by
//!   [`every_pinned_by_test_exists`].
//!
//! And the doc-generation check:
//!
//! - The divergence table in `CONFORMANCE.md` at the repo root matches
//!   [`kubo_conformance::render_table`]'s output. Enforced by
//!   [`divergence_table_matches_conformance_md`].

use std::collections::HashSet;

use kubo_conformance::{DivId, DivKind, REGISTRY, render_table};

/// All conformance test files that may pin a divergence. The completeness
/// check scans these for `assert_known_divergence(DivId::X)` calls.
const TEST_FILES: &[(&str, &str)] = &[
    ("ssa_two_managers", include_str!("ssa_two_managers.rs")),
    ("ssa_status", include_str!("ssa_status.rs")),
    ("ssa_status_pruning", include_str!("ssa_status_pruning.rs")),
    ("ssa_finalizers", include_str!("ssa_finalizers.rs")),
    ("ssa_concurrency", include_str!("ssa_concurrency.rs")),
    ("ssa_reconcile", include_str!("ssa_reconcile.rs")),
    (
        "ssa_reconcile_child",
        include_str!("ssa_reconcile_child.rs"),
    ),
    (
        "ssa_reconcile_error",
        include_str!("ssa_reconcile_error.rs"),
    ),
    ("ssa_observe_list", include_str!("ssa_observe_list.rs")),
    (
        "ssa_reconcile_conflict",
        include_str!("ssa_reconcile_conflict.rs"),
    ),
];

/// The set of `DivId` variants referenced by `assert_known_divergence(...)`
/// calls across the test files. A call is `assert_known_divergence(DivId::X)`
/// optionally preceded by whitespace; the variant name is captured.
fn referenced_ids() -> HashSet<String> {
    let mut out = HashSet::new();
    for (_, src) in TEST_FILES {
        for line in src.lines() {
            // Match `assert_known_divergence(DivId::Variant)` calls. Tolerate
            // surrounding whitespace and a trailing `)` or `,` — the call is a
            // statement, so it ends with `);` in practice.
            if let Some(rest) = line.find("assert_known_divergence(DivId::") {
                let after = &line[rest + "assert_known_divergence(DivId::".len()..];
                if let Some(end) = after.find(|c: char| !c.is_alphanumeric() && c != '_') {
                    let variant = &after[..end];
                    if !variant.is_empty() {
                        out.insert(variant.to_string());
                    }
                }
            }
        }
    }
    out
}

/// Every non-`StripTodo` registry id is referenced by a test, and every
/// reference resolves to a non-`StripTodo` registry id. The latter is also
/// compile-enforced (a `DivId::X` call with a non-existent variant fails to
/// build), but this test catches the case where a reference names a variant
/// that exists but is a `StripTodo` (which has no pinning test by design).
#[test]
fn every_pinned_id_is_referenced_by_a_test() {
    let referenced = referenced_ids();

    // Every non-StripTodo registry id must be referenced.
    let mut missing: Vec<&'static str> = Vec::new();
    for d in REGISTRY {
        if d.kind == DivKind::StripTodo {
            continue;
        }
        let label = id_variant_name(d.id);
        if !referenced.contains(label) {
            missing.push(label);
        }
    }
    assert!(
        missing.is_empty(),
        "registry ids not referenced by any `assert_known_divergence` call in the test files: \
         {missing:?}. Either add the pinning call, or, if the divergence is no longer pinned, \
         mark it StripTodo (and record why) or remove it from the registry."
    );

    // Every reference must resolve to a non-StripTodo registry id.
    let registry_ids: HashSet<&'static str> =
        REGISTRY.iter().map(|d| id_variant_name(d.id)).collect();
    let mut unknown: Vec<String> = Vec::new();
    for id in &referenced {
        if !registry_ids.contains(id.as_str()) {
            unknown.push(id.clone());
        }
    }
    assert!(
        unknown.is_empty(),
        "test files reference `assert_known_divergence(DivId::X)` for ids not in the registry: \
         {unknown:?}. This should not compile — if it does, the variant name scan is wrong."
    );

    // No test should pin a StripTodo (those have no pinning test by design).
    let strip_todos: HashSet<&'static str> = REGISTRY
        .iter()
        .filter(|d| d.kind == DivKind::StripTodo)
        .map(|d| id_variant_name(d.id))
        .collect();
    let mut pinned_strips: Vec<String> = Vec::new();
    for id in &referenced {
        if strip_todos.contains(id.as_str()) {
            pinned_strips.push(id.clone());
        }
    }
    assert!(
        pinned_strips.is_empty(),
        "test files pin StripTodo ids {pinned_strips:?}. A StripTodo has no pinning test by \
         design (the going-forward rule makes every normalisation strip a TODO against the fake); remove the call."
    );
}

/// Every registry entry's `pinned_by` test name exists as a function definition
/// in the named test file. A stale `pinned_by` (the test was renamed or removed) is
/// the silent-staleness failure mode a stale `pinned_by` field can produce; this
/// check makes it loud.
#[test]
fn every_pinned_by_test_exists() {
    for d in REGISTRY {
        let Some(test) = d.pinned_by else {
            continue;
        };
        let (file_stem, fn_name) = test
            .split_once("::")
            .unwrap_or_else(|| panic!("pinned_by {:?} is not `<file_stem>::<fn_name>`", test));
        let src = TEST_FILES
            .iter()
            .find(|(stem, _)| *stem == file_stem)
            .map(|(_, s)| *s)
            .unwrap_or_else(|| {
                panic!("pinned_by {:?} names unknown test file `{file_stem}`", test)
            });
        let needle = format!("fn {fn_name}");
        assert!(
            src.contains(&needle),
            "pinned_by {:?} names test fn `{fn_name}` not found in `tests/{file_stem}.rs`",
            test
        );
    }
}

/// Print the rendered table. Run with `--nocapture` to copy it into
/// `CONFORMANCE.md` between the BEGIN/END markers after editing the registry.
#[test]
fn print_divergence_table() {
    print!("{}", render_table());
}

/// The divergence table in `CONFORMANCE.md` at the repo root matches
/// [`render_table`]'s output. The file renders from the registry; if the two
/// drift, this test fails with a diff.
#[test]
fn divergence_table_matches_conformance_md() {
    let doc = include_str!("../../../CONFORMANCE.md");
    let begin = "<!-- BEGIN DIVERGENCE REGISTRY -->";
    let end = "<!-- END DIVERGENCE REGISTRY -->";
    let start = doc
        .find(begin)
        .expect("CONFORMANCE.md is missing the BEGIN DIVERGENCE REGISTRY marker");
    let after_begin = &doc[start + begin.len()..];
    let end_idx = after_begin
        .find(end)
        .expect("CONFORMANCE.md is missing the END DIVERGENCE REGISTRY marker");
    let table = after_begin[..end_idx].trim();

    let rendered = render_table();
    let expected = rendered.trim_end();
    assert_eq!(
        table, expected,
        "CONFORMANCE.md divergence table does not match the registry render.\n\
         Run `cargo test -p kubo-conformance divergence_table_matches_conformance_md` after editing\n\
         the registry, and paste the rendered table between the BEGIN/END markers."
    );
}

/// The variant name of a `DivId`, matching how `assert_known_divergence` calls
/// reference it (`DivId::<Variant>`). Kept in sync with `id_label` in the
/// module by exhaustive match.
fn id_variant_name(id: DivId) -> &'static str {
    match id {
        DivId::SsaStatusSecondManagerConflict => "SsaStatusSecondManagerConflict",
        DivId::SsaStatusCrossManagerOmit => "SsaStatusCrossManagerOmit",
        DivId::SsaStatusPruning => "SsaStatusPruning",
        DivId::MergePatchClearsSsaOwnedFinalizer => "MergePatchClearsSsaOwnedFinalizer",
        DivId::StaleResourceVersionStatusPatch => "StaleResourceVersionStatusPatch",
        DivId::TwoRapidSequentialStatusPatches => "TwoRapidSequentialStatusPatches",
        DivId::AsyncGcAfterFinalizerClear => "AsyncGcAfterFinalizerClear",
    }
}

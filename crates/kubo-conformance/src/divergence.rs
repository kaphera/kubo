//! Machine-checkable divergence registry.
//!
//! Known fake-vs-real divergences used to live across the design doc, the
//! progress record, test doc comments, and code comments. Nothing detected a
//! divergence being silently "fixed" in the fake (the pinning test would still
//! pass), or new framework code coming to depend on a divergent behaviour.
//! Prose alone cannot be trusted to stay current.
//!
//! This module is the structural fix. Each known divergence is declared once,
//! in [`REGISTRY`], with a stable identifier ([`DivId`]). The tests that pin a
//! divergence call [`assert_known_divergence`] with the identifier, so a
//! divergence being silently "fixed" in the fake still surfaces through the
//! pinning test's own assertions, and a divergence being dropped from the
//! registry while still present in the fake surfaces through the completeness
//! check in `tests/divergence_registry.rs` (every registry id must be
//! referenced by a test; every reference resolves to an id — the latter
//! compile-enforced via the [`DivId`] enum).
//!
//! The registry is the source of truth. The `divergence_table_matches_conformance_md`
//! test in `tests/divergence_registry.rs` re-renders the registry to the markdown
//! divergence table in `CONFORMANCE.md` at the repo root and asserts the two
//! match, so that file cannot drift from the registry without a failing test.
//! That test is the one place code reads a doc; it exists to keep the rendered
//! table honest, not to make the code depend on any doc to be understood.
//!
//! ## Kinds
//!
//! - [`DivKind::KnownDivergence`]: fake and real diverge; the pinning test
//!   asserts both sides separately and does *not* call `assert_equivalent`.
//! - [`DivKind::Characterisation`]: the row characterises real apiserver
//!   behaviour the framework does *not* rely on (e.g. force-on-status, which
//!   the framework never uses on the status subresource). Recorded so the
//!   surface is visible; not a framework contract.
//! - [`DivKind::IgnoredGap`]: the harness cannot reproduce the real behaviour
//!   (e.g. a true race). The pinning test is `#[ignore]`d; the entry records the
//!   gap honestly rather than omitting it.
//! - [`DivKind::StripTodo`]: a normalisation strip with a TODO to fix the fake.
//!   Recorded so the strip is visible, but not classed as a pinned divergence —
//!   the test that would pin it does not exist yet. Exempt from the "must be
//!   referenced by a test" completeness check. The going-forward rule is that
//!   a normalisation strip is a TODO against the fake, not a resolution; the
//!   prior `observedGeneration` strip was resolved (the fake now models
//!   `metadata.generation`), so the registry currently has no `StripTodo`
//!   entries. The kind remains for any future strip.

/// A stable identifier for a known fake-vs-real divergence.
///
/// Pinning tests pass a variant of this enum to [`assert_known_divergence`]
/// to anchor themselves to the registry entry. A reference to a non-existent
/// id fails to compile, which is the "every reference resolves" half of the
/// bidirectional check the registry enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DivId {
    /// A second manager claims a status field it does not own, `force=false`.
    /// Fake returns 200 (no status-subresource ownership); real returns 409.
    /// This is the expected guard once status is non-force, and a regression
    /// net for re-adding force on status.
    SsaStatusSecondManagerConflict,
    /// Cross-manager omit — a second manager applies `/status` with a
    /// different field, omitting a field the first manager owns. Fake removes
    /// the first manager's field (full `status` replace); real persists it
    /// (SSA ownership: a manager cannot release another manager's field by
    /// omission). Same 200, different persisted content.
    SsaStatusCrossManagerOmit,
    /// Structural-schema pruning. Real silently prunes a status field the
    /// CRD's `openAPIV3Schema` does not declare (200 on the write, field
    /// absent on the next GET); fake preserves it (no schema). The ledger
    /// killer.
    SsaStatusPruning,
    /// Merge-patch clearing a finalizer another manager SSA-owns. Fake's
    /// merge-patch overwrites the finalizer list regardless of SSA ownership;
    /// real's merge-patch also overwrites the value but leaves the SSA
    /// ownership record in `managedFields` untouched, so a subsequent SSA-apply
    /// by the original manager re-adds it without conflict (200 on k3d).
    /// Characterisation, not equivalence.
    MergePatchClearsSsaOwnedFinalizer,
    /// A status patch carrying a stale `resourceVersion`. Fake does not
    /// enforce the precondition (200, status overwritten); real enforces it
    /// (409, intervening write's status left in place).
    StaleResourceVersionStatusPatch,
    /// Two rapid sequential status patches. The harness is sequential and
    /// cannot reproduce a real race; the pinning test is `#[ignore]`d and
    /// records the gap.
    TwoRapidSequentialStatusPatches,
    /// Async GC after finalizer clear. Fake GCs a terminating object
    /// synchronously when its last finalizer is cleared (404 on the next
    /// GET); real GCs asynchronously (200 window with no finalizer, then
    /// 404). Both prove the finalizer was removed; divergence is in *when*
    /// the object disappears.
    AsyncGcAfterFinalizerClear,
}

/// The class of a [`Divergence`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DivKind {
    /// Fake and real diverge; the pinning test asserts both sides separately
    /// and does not call `assert_equivalent`.
    KnownDivergence,
    /// Characterises real apiserver behaviour the framework does not rely on.
    /// Recorded for visibility; not a framework contract.
    Characterisation,
    /// The harness cannot reproduce the real behaviour; the pinning test is
    /// `#[ignore]`d.
    IgnoredGap,
    /// A normalisation strip with a TODO to fix the fake. Not pinned by a
    /// test; exempt from the "must be referenced" completeness check.
    StripTodo,
}

impl DivKind {
    /// Render the kind as a short lowercase label for the generated table.
    pub fn as_label(self) -> &'static str {
        match self {
            DivKind::KnownDivergence => "known divergence",
            DivKind::Characterisation => "characterisation",
            DivKind::IgnoredGap => "ignored gap",
            DivKind::StripTodo => "strip TODO",
        }
    }
}

/// A single known fake-vs-real divergence, declared once in [`REGISTRY`].
#[derive(Debug, Clone)]
pub struct Divergence {
    /// The stable identifier. One per divergence; unique across the registry.
    pub id: DivId,
    /// A short label naming the scenario family this divergence belongs to
    /// (e.g. "status SSA row 2", "structural pruning"). Rendered in the
    /// generated table and matched by the doc-sync test.
    pub section: &'static str,
    /// One-line summary, for the generated table.
    pub summary: &'static str,
    /// What the fake does.
    pub fake_behaviour: &'static str,
    /// What the real apiserver does.
    pub real_behaviour: &'static str,
    /// The class of the entry.
    pub kind: DivKind,
    /// The test that pins this divergence, as `"<file_stem>::<test_fn>"` (e.g.
    /// `"ssa_status::ssa_status_second_manager_conflict_is_known_fake_divergence"`).
    /// `None` for [`DivKind::StripTodo`] (no pinning test yet).
    pub pinned_by: Option<&'static str>,
}

/// The source-of-truth registry. Declare each known divergence once here.
///
/// Ordering follows the scenario-family ordering: status-subresource SSA,
/// structural-schema pruning, finalizers and deletion, concurrency, the
/// reconcile loop, then the strip-TODO. The completeness test in
/// `tests/divergence_registry.rs` asserts every non-`StripTodo` entry's
/// `pinned_by` test exists and references the entry's id, and every
/// `assert_known_divergence(DivId::X)` call in the test files resolves to a
/// registry id (compile-enforced) and to a non-`StripTodo` entry.
pub const REGISTRY: &[Divergence] = &[
    Divergence {
        id: DivId::SsaStatusSecondManagerConflict,
        section: "status SSA — second manager conflict",
        summary: "Second manager claims a status field it does not own, force=false",
        fake_behaviour: "200 — fake does not model status-subresource ownership",
        real_behaviour: "409 — the expected guard with non-force status; regression net for force on status",
        kind: DivKind::KnownDivergence,
        pinned_by: Some("ssa_status::ssa_status_second_manager_conflict_is_known_fake_divergence"),
    },
    Divergence {
        id: DivId::SsaStatusCrossManagerOmit,
        section: "status SSA — cross-manager omit",
        summary: "Second manager omits a field the first manager owns",
        fake_behaviour: "200, field removed — full `status` replace",
        real_behaviour: "200, field persists — SSA ownership forbids releasing another manager's field by omission",
        kind: DivKind::KnownDivergence,
        pinned_by: Some("ssa_status::ssa_status_cross_manager_omit_is_known_fake_divergence"),
    },
    Divergence {
        id: DivId::SsaStatusPruning,
        section: "status SSA — structural pruning",
        summary: "Structural-schema pruning — the ledger killer",
        fake_behaviour: "200, undeclared status field preserved (no schema)",
        real_behaviour: "200, undeclared status field pruned silently; absent on next GET",
        kind: DivKind::KnownDivergence,
        pinned_by: Some("ssa_status_pruning::ssa_status_pruning_is_the_ledger_killer"),
    },
    Divergence {
        id: DivId::MergePatchClearsSsaOwnedFinalizer,
        section: "finalizers — merge-patch clears SSA-owned",
        summary: "Merge-patch clears a finalizer another manager SSA-owns",
        fake_behaviour: "Overwrites finalizer list; no ownership tracking on merge-patch",
        real_behaviour: "Overwrites the value; leaves the `managedFields` ownership record — re-apply by the original manager re-adds it (200 on k3d)",
        kind: DivKind::Characterisation,
        pinned_by: Some(
            "ssa_finalizers::merge_patch_clearing_ssa_owned_finalizer_is_known_fake_divergence",
        ),
    },
    Divergence {
        id: DivId::StaleResourceVersionStatusPatch,
        section: "concurrency — stale resourceVersion status patch",
        summary: "Status patch carrying a stale resourceVersion",
        fake_behaviour: "200 — fake does not enforce the resourceVersion precondition",
        real_behaviour: "409 — precondition enforced; intervening write's status left in place",
        kind: DivKind::KnownDivergence,
        pinned_by: Some(
            "ssa_concurrency::stale_resource_version_status_patch_is_known_fake_divergence",
        ),
    },
    Divergence {
        id: DivId::TwoRapidSequentialStatusPatches,
        section: "concurrency — two rapid sequential status patches",
        summary: "Two rapid sequential status patches (ledger append then strike)",
        fake_behaviour: "Both accepted in order — harness is sequential, no race",
        real_behaviour: "Both accepted if no race; 409 if racy — the harness cannot reproduce a real race",
        kind: DivKind::IgnoredGap,
        pinned_by: Some(
            "ssa_concurrency::two_rapid_sequential_status_patches_cannot_be_raced_in_harness",
        ),
    },
    Divergence {
        id: DivId::AsyncGcAfterFinalizerClear,
        section: "reconcile — async GC after finalizer clear",
        summary: "Async GC after finalizer clear",
        fake_behaviour: "Synchronous GC on last-finalizer clear — 404 on next GET",
        real_behaviour: "Asynchronous GC — 200 window with no finalizer, then 404",
        kind: DivKind::KnownDivergence,
        pinned_by: Some("ssa_reconcile::reconcile_ledger_progression_matches_fake_and_real"),
    },
];

/// Look up a registry entry by id. Panics if the id is not in the registry —
/// which is a registry bug (every `DivId` variant must have an entry).
pub fn lookup(id: DivId) -> &'static Divergence {
    REGISTRY
        .iter()
        .find(|d| d.id == id)
        .unwrap_or_else(|| panic!("divergence id {:?} has no REGISTRY entry", id))
}

/// Anchor a pinning test to a registry entry.
///
/// Call this once where the test asserts the divergence (fake side, real
/// side, or both). It returns the [`Divergence`] so the test can quote it in
/// assertions or doc comments, and it makes the test's reference to the id
/// visible to the completeness check in `tests/divergence_registry.rs` (which
/// scans the test files for `assert_known_divergence(DivId::X)` calls).
///
/// The helper itself is pure — it does not assert anything. The test's own
/// assertions (fake 200, real 409, etc.) are what detects a divergence being
/// silently "fixed" in the fake; the registry's job is the inverse check: that
/// every divergence is still declared and every declaration is still pinned.
pub fn assert_known_divergence(id: DivId) -> &'static Divergence {
    lookup(id)
}

/// Render the registry as the markdown divergence table. The
/// `divergence_table_matches_conformance_md` test asserts this output matches
/// the table in `CONFORMANCE.md` at the repo root, so that file cannot drift
/// from the registry without a failing test.
pub fn render_table() -> String {
    let mut out = String::new();
    out.push_str("| ID | Section | Kind | Summary | Fake | Real | Pinned by |\n");
    out.push_str("|---|---|---|---|---|---|---|\n");
    for d in REGISTRY {
        out.push_str(&format!(
            "| `{id}` | {section} | {kind} | {summary} | {fake} | {real} | {pinned} |\n",
            id = id_label(d.id),
            section = d.section,
            kind = d.kind.as_label(),
            summary = d.summary,
            fake = d.fake_behaviour,
            real = d.real_behaviour,
            pinned = d.pinned_by.unwrap_or("—"),
        ));
    }
    out
}

/// The markdown identifier for a [`DivId`] variant, as used in the rendered
/// table and in `assert_known_divergence(...)` call sites. Kept in sync with
/// the variant names by the `divergence_ids_match_doc` test.
pub fn id_label(id: DivId) -> &'static str {
    match id {
        DivId::SsaStatusSecondManagerConflict => "DivId::SsaStatusSecondManagerConflict",
        DivId::SsaStatusCrossManagerOmit => "DivId::SsaStatusCrossManagerOmit",
        DivId::SsaStatusPruning => "DivId::SsaStatusPruning",
        DivId::MergePatchClearsSsaOwnedFinalizer => "DivId::MergePatchClearsSsaOwnedFinalizer",
        DivId::StaleResourceVersionStatusPatch => "DivId::StaleResourceVersionStatusPatch",
        DivId::TwoRapidSequentialStatusPatches => "DivId::TwoRapidSequentialStatusPatches",
        DivId::AsyncGcAfterFinalizerClear => "DivId::AsyncGcAfterFinalizerClear",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every `DivId` variant has exactly one registry entry, and every entry
    /// has a unique id. A new variant added without a registry entry (or vice
    /// versa) fails here.
    #[test]
    fn registry_covers_every_id_uniquely() {
        use std::collections::HashSet;
        let mut seen: HashSet<DivId> = HashSet::new();
        for d in REGISTRY {
            assert!(seen.insert(d.id), "duplicate registry entry for {:?}", d.id);
        }
        // Exhaustive match: if a variant is added to DivId, this stops
        // compiling until it is handled.
        for id in [
            DivId::SsaStatusSecondManagerConflict,
            DivId::SsaStatusCrossManagerOmit,
            DivId::SsaStatusPruning,
            DivId::MergePatchClearsSsaOwnedFinalizer,
            DivId::StaleResourceVersionStatusPatch,
            DivId::TwoRapidSequentialStatusPatches,
            DivId::AsyncGcAfterFinalizerClear,
        ] {
            assert!(seen.contains(&id), "DivId {:?} has no REGISTRY entry", id);
        }
    }

    /// `StripTodo` entries have no pinning test; every other kind does.
    #[test]
    fn only_strip_todo_lacks_a_pinning_test() {
        for d in REGISTRY {
            match d.kind {
                DivKind::StripTodo => assert!(
                    d.pinned_by.is_none(),
                    "{:?} is a StripTodo but names a pinning test",
                    d.id
                ),
                _ => assert!(
                    d.pinned_by.is_some(),
                    "{:?} is {:?} but has no pinning test",
                    d.id,
                    d.kind
                ),
            }
        }
    }

    /// `id_label` returns the variant's name for every id.
    #[test]
    fn id_label_covers_every_id() {
        for d in REGISTRY {
            assert!(
                id_label(d.id).contains(id_label_name(d.id)),
                "id_label({:?}) does not name the variant",
                d.id
            );
        }
    }

    fn id_label_name(id: DivId) -> &'static str {
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
}

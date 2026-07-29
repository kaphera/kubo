//! SSA on the status subresource: the ledger's hot path.
//!
//! These scenarios pin the framework's status contract after the move to
//! non-force status writes (the framework dropped force on status so that a
//! second writer on the controller's own status would be surfaced as a 409
//! rather than silently overwritten): `patch_status` is non-force, the
//! controller is the sole writer of its own CR's status, and it sends the
//! complete status every reconcile.  The harness pins that contract and
//! documents where the fake cannot reproduce the real apiserver's behaviour.
//!
//! Row 1 and Row 3 are asserted equivalent.  Row 2 and the cross-manager omit
//! test are *known fake divergences* — the harness records the boundary rather
//! than asserting equivalence, because the fake does not model
//! status-subresource ownership (`Store::status_patch` replaces the `status`
//! field verbatim and returns 200 regardless of `fieldManager` / `force`).
//! Row 4 is a characterisation of the real apiserver's force-on-status
//! semantics, not a framework contract (the framework does not force-apply
//! status).

#[cfg(feature = "k3d")]
use kubo_conformance::assert_equivalent;
use kubo_conformance::{DivId, Scenario, assert_known_divergence};
use serde_json::json;

/// Row 1 — `patch_status` (SSA, non-force) writes `pendingCleanup`.
///
/// The framework's actual hot path after the move to non-force status writes:
/// sole owner, non-force, complete status.  The fake stores the status
/// verbatim and returns 200; the real apiserver returns 200 and, under the
/// current `x-kubernetes-preserve-unknown-fields: true` CRD, preserves the
/// field too. A GET after the write confirms what each server persisted.
/// Fake and real are asserted equivalent entry-by-entry.
#[test]
fn ssa_status_write_matches_real_apiserver() {
    let scenario = Scenario::new("ssa-status-write")
        .apply("kubo-reconcile", json!({"spec": {"marker": "x"}}))
        .apply_status("kubo-reconcile", json!({"pendingCleanup": ["child-1"]}))
        .named("status-write")
        .get()
        .named("get");

    let fake_trace = scenario.run_against_fake();
    assert_eq!(fake_trace.entries.len(), 3, "scenario has three steps");
    assert_eq!(
        fake_trace.entry("status-write").status,
        200,
        "fake accepts the status apply"
    );
    assert_eq!(
        fake_trace
            .entry("get")
            .status_subresource
            .as_ref()
            .and_then(|s| s.get("pendingCleanup")),
        Some(&json!(["child-1"])),
        "fake must persist pendingCleanup verbatim"
    );

    #[cfg(feature = "k3d")]
    {
        let real_trace = scenario.run_against_kube_apiserver();
        assert_equivalent(&fake_trace, &real_trace);
    }
}

/// Row 2 — a second manager claims a status field it does not own, `force=false`.
///
/// This is the *expected guard* under the non-force contract, not a divergence
/// to skip: the real apiserver returns 409 because `kubo-reconcile` owns the
/// field, which is exactly the loud failure the framework now wants when
/// something else writes its status.  The fake cannot reproduce it (it ignores
/// status-subresource ownership), so the harness records the boundary: fake
/// returns 200, real returns 409.  This row is also a regression net — if
/// someone re-adds `.force()` on status, the real apiserver would stop
/// returning 409 here and this test would flag it.
#[test]
fn ssa_status_second_manager_conflict_is_known_fake_divergence() {
    let scenario = Scenario::new("ssa-status-second-manager-conflict")
        .apply("kubo-reconcile", json!({"spec": {"marker": "x"}}))
        .apply_status("kubo-reconcile", json!({"conflictField": "a"}))
        .apply_status("other-manager", json!({"conflictField": "b"}))
        .named("conflict");

    let _ = assert_known_divergence(DivId::SsaStatusSecondManagerConflict);

    let fake_trace = scenario.run_against_fake();
    assert_eq!(
        fake_trace.entry("conflict").status,
        200,
        "fake does not model status-subresource ownership, so it accepts the conflicting apply"
    );

    #[cfg(feature = "k3d")]
    {
        let real_trace = scenario.run_against_kube_apiserver();
        assert_eq!(
            real_trace.entry("conflict").status,
            409,
            "real apiserver must 409 when a second manager claims a status field it does not own"
        );
    }
}

/// Row 3 — the sole owner re-applies status omitting a field it previously
/// emitted.
///
/// The conformance doc and the initial handoff both had this backwards.  Empirically (verified against k3d with a raw PATCH and a
/// managedFields dump), real SSA on `/status` *releases* a field when the
/// manager that owns it omits it from a later apply: the field is removed and
/// ownership is dropped from `managedFields`.  This is standard SSA semantics
/// — omitting a field *you own* is the SSA way of saying "I no longer want
/// this field"; only omitting a field *another manager owns* leaves it in
/// place (you cannot release another manager's field by omission).
///
/// The fake does a full `status` field replace (`Store::status_patch` does
/// `o.insert("status", status)`), so it also removes the omitted field — for
/// a different reason, but with the same end state.  This is the framework's
/// desired behaviour: `patch_status` sends the complete status every
/// reconcile, so a field the controller stops emitting is cleaned up on both
/// servers.  Row 3 is therefore an **equivalence** case, not a divergence.
/// The genuine fake divergence on status ownership is exercised by
/// `ssa_status_cross_manager_omit_is_known_fake_divergence` below.
#[test]
fn ssa_status_field_removal_on_reapply_matches_real_apiserver() {
    let scenario = Scenario::new("ssa-status-field-removal")
        .apply("kubo-reconcile", json!({"spec": {"marker": "x"}}))
        .apply_status(
            "kubo-reconcile",
            json!({"keptField": "k", "droppedField": "d"}),
        )
        .apply_status("kubo-reconcile", json!({"keptField": "k"}))
        .get()
        .named("get");

    let fake_trace = scenario.run_against_fake();
    let fake_status = &fake_trace.entry("get").status_subresource;
    assert_eq!(
        fake_status.as_ref().and_then(|s| s.get("keptField")),
        Some(&json!("k")),
        "fake must keep the reasserted field"
    );
    assert!(
        fake_status
            .as_ref()
            .and_then(|s| s.get("droppedField"))
            .is_none(),
        "fake replaces status verbatim, so an omitted field must be gone"
    );

    #[cfg(feature = "k3d")]
    {
        let real_trace = scenario.run_against_kube_apiserver();
        let real_status = &real_trace.entry("get").status_subresource;
        assert_eq!(
            real_status.as_ref().and_then(|s| s.get("keptField")),
            Some(&json!("k")),
            "real must keep the reasserted field"
        );
        assert!(
            real_status
                .as_ref()
                .and_then(|s| s.get("droppedField"))
                .is_none(),
            "real SSA releases a field the applying manager owns when it is omitted"
        );
        assert_equivalent(&fake_trace, &real_trace);
    }
}

/// The genuine fake divergence on status ownership: a second manager applies
/// `/status` with a *different* field, omitting a field the first manager
/// owns.
///
/// This is the divergence the design doc was reaching for but mis-stated as a
/// same-manager case.  Under real SSA, a manager cannot release another
/// manager's field by omission — `manager-a`'s `ownedField` persists when
/// `manager-b` applies `{newField: "b"}` (both 200, no conflict, because
/// `manager-b` is writing a field *it* owns, not touching `ownedField`).  The
/// fake does a full `status` replace, so `ownedField` is gone on the fake.
/// Same status code (200), different persisted content — exactly the content
/// divergence a status-code-only trace would miss and the content-aware trace
/// exists to surface.  Equivalence is *not* asserted; the harness records the
/// boundary.
#[test]
fn ssa_status_cross_manager_omit_is_known_fake_divergence() {
    let scenario = Scenario::new("ssa-status-cross-manager-omit")
        .apply("kubo-reconcile", json!({"spec": {"marker": "x"}}))
        .apply_status("manager-a", json!({"ownedField": "a"}))
        .apply_status("manager-b", json!({"newField": "b"}))
        .get()
        .named("get");

    let _ = assert_known_divergence(DivId::SsaStatusCrossManagerOmit);

    let fake_trace = scenario.run_against_fake();
    let fake_status = &fake_trace.entry("get").status_subresource;
    assert_eq!(
        fake_status.as_ref().and_then(|s| s.get("newField")),
        Some(&json!("b")),
        "fake must reflect the second manager's new field"
    );
    assert!(
        fake_status
            .as_ref()
            .and_then(|s| s.get("ownedField"))
            .is_none(),
        "fake replaces status verbatim, so a field the second manager omitted is gone"
    );

    #[cfg(feature = "k3d")]
    {
        let real_trace = scenario.run_against_kube_apiserver();
        let real_status = &real_trace.entry("get").status_subresource;
        assert_eq!(
            real_status.as_ref().and_then(|s| s.get("newField")),
            Some(&json!("b")),
            "real must reflect the second manager's new field"
        );
        assert_eq!(
            real_status.as_ref().and_then(|s| s.get("ownedField")),
            Some(&json!("a")),
            "real SSA: a manager cannot release another manager's field by omission, so ownedField persists"
        );
    }
}

/// Row 4 — `patch_status` with `force` steals a status field owned by another
/// manager.
///
/// Characterisation of the real apiserver's force-on-status semantics, *not* a
/// framework contract: the framework's `patch_status` is non-force after the
/// move to non-force status writes, so the framework never takes this path.  The harness still exercises it at
/// the wire level (it builds raw HTTP, it does not go through `KubeHandle`)
/// to document what force-on-status does — ownership of the conflicting field
/// is transferred to the forcing manager and the value overwritten, with a
/// 200 either way.  The fake ignores force and replaces the status field
/// verbatim, so the returned status matches and equivalence holds.
#[test]
fn ssa_status_force_steal_matches_real_apiserver() {
    let scenario = Scenario::new("ssa-status-force-steal")
        .apply("kubo-reconcile", json!({"spec": {"marker": "x"}}))
        .apply_status("manager-a", json!({"stealField": "a"}))
        .apply_status_force("manager-b", json!({"stealField": "b"}))
        .named("force")
        .get()
        .named("get");

    let fake_trace = scenario.run_against_fake();
    assert_eq!(
        fake_trace.entry("force").status,
        200,
        "fake accepts the force-apply"
    );
    assert_eq!(
        fake_trace
            .entry("get")
            .status_subresource
            .as_ref()
            .and_then(|s| s.get("stealField")),
        Some(&json!("b")),
        "fake must end with the forcing manager's value"
    );

    #[cfg(feature = "k3d")]
    {
        let real_trace = scenario.run_against_kube_apiserver();
        assert_equivalent(&fake_trace, &real_trace);
    }
}

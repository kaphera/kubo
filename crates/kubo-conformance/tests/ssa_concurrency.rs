//! Concurrency and optimistic locking.
//!
//! The framework's status-patch wire shape (`KubeHandle::patch_status`) is
//! non-force SSA with no `resourceVersion` precondition. A controller that
//! wanted optimistic concurrency would GET to observe the current
//! `resourceVersion`, then send that rv in the apply body's `metadata`; the
//! apiserver 409s if an intervening write made it stale. The fake does not
//! enforce the resourceVersion precondition, so a stale rv that real rejects
//! with 409 is silently accepted (200) by the fake.
//!
//! This is a forward-looking conformance test: the rv precondition is
//! exercised at the wire level, and the framework's `patch_status` does not
//! send one. The stale-rv status-patch scenario is a known fake divergence
//! (registry kind `KnownDivergence`), not equivalence: fake and real diverge
//! by design on the stale-rv step, so [`assert_equivalent`] is not called.

use kubo_conformance::{DivId, Scenario, assert_known_divergence};
use serde_json::json;

/// Row 1 — a status patch with a stale `resourceVersion` is the stale-rv
/// status-patch divergence.
///
/// Procedure: apply the spec, write an initial status, GET to capture the
/// current `resourceVersion` as `rv1`, perform an intervening status write
/// (which bumps the rv on both fake and real), then send a status apply
/// carrying the now-stale `rv1` as a precondition. The fake ignores the
/// precondition and accepts the patch (200), overwriting the status. The real
/// apiserver enforces the precondition and rejects the patch (409), leaving
/// the intervening write's status in place. A final GET pins the persisted
/// status on each side.
///
/// This is a known fake divergence (the fake does not enforce the
/// `resourceVersion` precondition), not equivalence —
/// [`assert_equivalent`] is deliberately not called.
#[test]
fn stale_resource_version_status_patch_is_known_fake_divergence() {
    let scenario = Scenario::new("stale-rv-status-patch")
        // 0: create the object with a spec.
        .apply("kubo-reconcile", json!({ "spec": { "marker": "x" } }))
        // 1: write an initial status.
        .apply_status_force("kubo-reconcile", json!({ "phase": "initial" }))
        // 2: GET to observe and capture the current resourceVersion as "rv1".
        .get()
        .capture_rv("rv1")
        // 3: intervening status write — bumps the rv on both fake and real.
        .apply_status_force("kubo-reconcile", json!({ "phase": "intervening" }))
        // 4: status apply carrying the now-stale "rv1" as a precondition.
        //    Fake: ignores the rv, accepts (200), overwrites phase -> "precondition".
        //    Real: enforces the rv, rejects (409), phase stays "intervening".
        .apply_status_precondition("kubo-reconcile", json!({ "phase": "precondition" }), "rv1")
        .named("stale-rv")
        // 5: final GET to observe what each server actually persisted.
        .get()
        .named("final-get");

    let _ = assert_known_divergence(DivId::StaleResourceVersionStatusPatch);

    let fake_trace = scenario.run_against_fake();

    // The fake does not enforce the resourceVersion precondition, so the
    // stale-rv status apply is accepted.
    assert_eq!(
        fake_trace.entry("stale-rv").status,
        200,
        "fake: stale-rv status apply is accepted (no precondition enforcement)"
    );
    let fake_final_status = &fake_trace.entry("final-get").status_subresource;
    assert_eq!(
        fake_final_status,
        &Some(json!({ "phase": "precondition" })),
        "fake: stale-rv apply overwrote the status to the precondition's value"
    );

    #[cfg(feature = "k3d")]
    {
        let real_trace = scenario.run_against_kube_apiserver();

        // The real apiserver enforces the resourceVersion precondition: an
        // intervening write made "rv1" stale, so the apply is rejected with 409.
        assert_eq!(
            real_trace.entry("stale-rv").status,
            409,
            "real: stale-rv status apply is rejected with 409 (precondition enforced)"
        );
        let real_final_status = &real_trace.entry("final-get").status_subresource;
        assert_eq!(
            real_final_status,
            &Some(json!({ "phase": "intervening" })),
            "real: 409 left the intervening write's status in place (precondition rejected)"
        );
    }
}

/// Row 2 — two rapid sequential status patches (ledger append then strike).
///
/// The "two rapid sequential status patches" case is expected to be "both
/// accepted if no race; 409 if racy". The conformance harness runs steps
/// sequentially against a single apiserver with no concurrent writers, so it
/// cannot reproduce a real race — both patches land in order and both succeed
/// on fake and real. A meaningful test of this row needs two concurrent
/// clients racing on the same rv, which is outside the harness's sequential
/// model. Marked `#[ignore]` so the intent and the gap are recorded without a
/// green-but-misleading test.
#[test]
#[ignore = "needs concurrent writers; the harness is sequential and cannot reproduce a real race"]
fn two_rapid_sequential_status_patches_cannot_be_raced_in_harness() {
    let scenario = Scenario::new("two-rapid-status-patches")
        .apply("kubo-reconcile", json!({ "spec": { "marker": "x" } }))
        .apply_status_force("kubo-reconcile", json!({ "phase": "append" }))
        .named("append")
        .apply_status_force("kubo-reconcile", json!({ "phase": "strike" }))
        .named("strike")
        .get()
        .named("get");

    let _ = assert_known_divergence(DivId::TwoRapidSequentialStatusPatches);

    let fake_trace = scenario.run_against_fake();
    assert_eq!(fake_trace.entry("append").status, 200);
    assert_eq!(fake_trace.entry("strike").status, 200);
    assert_eq!(
        fake_trace.entry("get").status_subresource,
        Some(json!({ "phase": "strike" })),
        "fake: sequential patches land in order"
    );

    #[cfg(feature = "k3d")]
    {
        let real_trace = scenario.run_against_kube_apiserver();
        assert_eq!(real_trace.entry("append").status, 200);
        assert_eq!(real_trace.entry("strike").status, 200);
        assert_eq!(
            real_trace.entry("get").status_subresource,
            Some(json!({ "phase": "strike" })),
            "real: sequential patches land in order (no race under the harness)"
        );
    }
}

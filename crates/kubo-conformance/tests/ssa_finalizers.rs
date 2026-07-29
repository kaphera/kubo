//! Finalizers and deletion.
//!
//! The framework's deletion contract: `patch_finalizers` (merge-patch, no
//! `fieldManager`) adds or clears finalizers, and `delete` (plain DELETE, no
//! body, no preconditions) starts the deletion sequence. The apiserver retains
//! an object with finalizers after DELETE (setting `deletionTimestamp`), and
//! removes it once the last finalizer is cleared — the teardown signal the
//! framework's cleanup loop relies on.
//!
//! Rows 1–3 are equivalence: the fake and real apiserver agree on the
//! finalizer-aware delete, the teardown completion, and the no-finalizer
//! baseline. Row 4 is a characterisation: the framework clears
//! finalizers with merge-patch (no `fieldManager`), which bypasses SSA
//! ownership. On a real cluster this is legal but interacts with SSA-owned
//! finalizers in ways the fake's merge-patch simply overwrites — the row
//! characterises what real does rather than asserting equivalence.

#[cfg(feature = "k3d")]
use kubo_conformance::assert_equivalent;
use kubo_conformance::{DivId, Scenario, assert_known_divergence};
use serde_json::json;

/// Row 1 — `patch_finalizers` adds, then DELETE retains the object.
///
/// After a finalizer is added via merge-patch, a DELETE sets `deletionTimestamp`
/// and retains the object (finalizer-aware delete). A subsequent GET returns
/// 200, not 404 — the object is still present, awaiting finalizer clearance.
/// `deletionTimestamp` is server-assigned (the fake uses a stable sentinel, real
/// uses wall-clock) and is stripped by normalisation, so equivalence holds on
/// the retained content (finalizers, spec). Retention itself is pinned by the
/// GET status code (200 vs 404).
#[test]
fn patch_finalizers_then_delete_retains_object() {
    let scenario = Scenario::new("finalizers-add-then-delete")
        .apply("kubo-reconcile", json!({ "spec": { "marker": "x" } }))
        .patch_finalizers(&["kubo.io/cleanup"])
        .delete()
        .expect_last_status(200)
        .named("delete")
        .get()
        .expect_last_status(200)
        .named("get");

    let fake_trace = scenario.run_against_fake();
    assert_eq!(
        fake_trace.entry("delete").status,
        200,
        "fake: DELETE of an object with finalizers returns 200 (retained)"
    );
    assert_eq!(
        fake_trace.entry("get").status,
        200,
        "fake: GET after DELETE-with-finalizers returns 200 (object retained)"
    );
    let fake_obj = &fake_trace.entry("get").object;
    assert_eq!(
        fake_obj
            .as_ref()
            .and_then(|o| o.get("metadata"))
            .and_then(|m| m.get("finalizers")),
        Some(&json!(["kubo.io/cleanup"])),
        "fake: retained object must still carry the finalizer"
    );

    #[cfg(feature = "k3d")]
    {
        let real_trace = scenario.run_against_kube_apiserver();
        assert_equivalent(&fake_trace, &real_trace);
    }
}

/// Row 2 — clearing the last finalizer after DELETE removes the object.
///
/// Continuing from row 1's state (object terminating with one finalizer), a
/// merge-patch that empties the finalizer list triggers the apiserver's GC
/// controller: the patch returns 200 with the updated object (finalizers
/// empty), but the object is gone — the next GET returns 404. The fake models
/// this synchronously in `Store::merge_patch` (see
/// `is_terminating_with_no_finalizers`); without that modelling the fake
/// would retain the terminating object and diverge on the teardown row.
#[test]
fn clearing_last_finalizer_after_delete_removes_object() {
    let scenario = Scenario::new("finalizers-clear-after-delete")
        .apply("kubo-reconcile", json!({ "spec": { "marker": "x" } }))
        .patch_finalizers(&["kubo.io/cleanup"])
        .delete()
        .expect_last_status(200)
        .patch_finalizers(&[])
        .expect_last_status(200)
        .get()
        .expect_last_status(404)
        .named("get");

    let fake_trace = scenario.run_against_fake();
    assert_eq!(
        fake_trace.entry("get").status,
        404,
        "fake: GET after clearing the last finalizer on a terminating object must 404 (GC'd)"
    );

    #[cfg(feature = "k3d")]
    {
        let real_trace = scenario.run_against_kube_apiserver();
        assert_equivalent(&fake_trace, &real_trace);
    }
}

/// Row 3 — DELETE with no finalizers removes the object immediately.
///
/// Baseline: an object with no finalizers is removed by a plain DELETE with no
/// teardown sequence. The DELETE returns 200 and the next GET returns 404.
#[test]
fn delete_with_no_finalizers_removes_immediately() {
    let scenario = Scenario::new("delete-no-finalizers")
        .apply("kubo-reconcile", json!({ "spec": { "marker": "x" } }))
        .delete()
        .expect_last_status(200)
        .named("delete")
        .get()
        .expect_last_status(404)
        .named("get");

    let fake_trace = scenario.run_against_fake();
    assert_eq!(
        fake_trace.entry("delete").status,
        200,
        "fake: DELETE of an object with no finalizers returns 200"
    );
    assert_eq!(
        fake_trace.entry("get").status,
        404,
        "fake: GET after DELETE-with-no-finalizers returns 404 (removed immediately)"
    );

    #[cfg(feature = "k3d")]
    {
        let real_trace = scenario.run_against_kube_apiserver();
        assert_equivalent(&fake_trace, &real_trace);
    }
}

/// Row 4 — merge-patch clearing a finalizer another manager SSA-owns.
///
/// The framework removes finalizers with merge-patch (`patch_finalizers`), which
/// bypasses SSA ownership — no `fieldManager`, no ownership tracking. On the
/// fake, merge-patch silently overwrites the finalizer list regardless of who
/// owns it. On a real cluster, merge-patch is legal but the SSA ownership record
/// in `managedFields` may still reference the removed finalizer; a subsequent
/// SSA-apply by the original manager may 409 or may re-add it. This row
/// characterises what real does rather than asserting equivalence — this is
/// the "possible 409 / ownership interplay" row.
///
/// Procedure: manager-a SSA-applies the finalizer (claiming ownership), then a
/// merge-patch clears it. We observe whether the finalizer is gone (it should
/// be on both — merge-patch overwrites), then manager-a re-applies the same
/// finalizer via SSA and we observe whether real 409s (ownership record still
/// references it) or re-adds it cleanly.
#[test]
fn merge_patch_clearing_ssa_owned_finalizer_is_known_fake_divergence() {
    let scenario = Scenario::new("merge-patch-vs-ssa-finalizer")
        .apply("kubo-reconcile", json!({ "spec": { "marker": "x" } }))
        // manager-a SSA-owns the finalizer.
        .apply(
            "manager-a",
            json!({ "metadata": { "finalizers": ["kubo.io/cleanup"] } }),
        )
        // merge-patch (no fieldManager) clears it — bypasses SSA ownership.
        .patch_finalizers(&[])
        .get()
        .named("get-after-clear")
        // manager-a re-applies the same finalizer via SSA. Does real 409 (its
        // managedFields still reference the now-removed finalizer) or re-add it
        // cleanly (merge-patch reset the ownership)?
        .apply(
            "manager-a",
            json!({ "metadata": { "finalizers": ["kubo.io/cleanup"] } }),
        )
        .named("reapply")
        .get()
        .named("get-after-reapply");

    let _ = assert_known_divergence(DivId::MergePatchClearsSsaOwnedFinalizer);

    let fake_trace = scenario.run_against_fake();

    // On the fake, merge-patch silently clears the finalizer regardless of
    // ownership. The GET after the clear shows no finalizers. The fake persists
    // `metadata.finalizers: []`, but `normalise_object` strips an empty array
    // (real omits the field once empty), so the cleared state normalises to a
    // missing `finalizers` field — `None` here.
    let fake_after_clear = &fake_trace.entry("get-after-clear").object;
    assert_eq!(
        fake_after_clear
            .as_ref()
            .and_then(|o| o.get("metadata"))
            .and_then(|m| m.get("finalizers")),
        None,
        "fake: merge-patch clears the SSA-owned finalizer (no ownership tracking on merge-patch)"
    );

    // The fake's SSA-apply by manager-a re-adds the finalizer without conflict
    // (the fake's ownership map still has manager-a owning the finalizer path,
    // so re-applying is a no-op ownership-wise). Status 200.
    assert_eq!(
        fake_trace.entry("reapply").status,
        200,
        "fake: manager-a re-applying the finalizer via SSA succeeds (ownership unchanged)"
    );
    let fake_after_reapply = &fake_trace.entry("get-after-reapply").object;
    assert_eq!(
        fake_after_reapply
            .as_ref()
            .and_then(|o| o.get("metadata"))
            .and_then(|m| m.get("finalizers")),
        Some(&json!(["kubo.io/cleanup"])),
        "fake: finalizer is back after manager-a re-applies it"
    );

    #[cfg(feature = "k3d")]
    {
        let real_trace = scenario.run_against_kube_apiserver();

        // Real: merge-patch clears the finalizer (merge-patch bypasses SSA
        // ownership, so the finalizer list is overwritten to []). Real omits
        // `metadata.finalizers` once empty, and `normalise_object` strips the
        // fake's `[]` to match — so the cleared state is a missing field.
        let real_after_clear = &real_trace.entry("get-after-clear").object;
        assert_eq!(
            real_after_clear
                .as_ref()
                .and_then(|o| o.get("metadata"))
                .and_then(|m| m.get("finalizers")),
            None,
            "real: merge-patch clears the SSA-owned finalizer (merge-patch bypasses ownership)"
        );

        // The characterisation point: does manager-a's re-apply 409 (its
        // managedFields still reference the finalizer the merge-patch removed)
        // or succeed (merge-patch reset ownership)? Record what real does
        // without asserting equivalence — this is the merge-patch-clears-SSA-
        // owned-finalizer interplay.
        let real_reapply_status = real_trace.entry("reapply").status;
        assert!(
            real_reapply_status == 200 || real_reapply_status == 409,
            "real: manager-a re-applying the finalizer after a merge-patch clear \
             should either succeed (200, ownership was reset) or conflict (409, \
             ownership record still references the removed finalizer). Got \
             {real_reapply_status}. Record this as the merge-patch-clears-SSA-\
             owned-finalizer characterisation."
        );

        // Whatever the status, the final GET shows the finalizer is back (200
        // re-adds it; 409 leaves it cleared). Assert the observed state
        // matches the status — do not assert equivalence with the fake.
        let real_after_reapply = &real_trace.entry("get-after-reapply").object;
        let real_finalizers = real_after_reapply
            .as_ref()
            .and_then(|o| o.get("metadata"))
            .and_then(|m| m.get("finalizers"));
        if real_reapply_status == 200 {
            assert_eq!(
                real_finalizers,
                Some(&json!(["kubo.io/cleanup"])),
                "real: 200 on re-apply means the finalizer was re-added"
            );
        } else {
            assert_eq!(
                real_finalizers, None,
                "real: 409 on re-apply means the finalizer stayed cleared (field absent)"
            );
        }
    }
}

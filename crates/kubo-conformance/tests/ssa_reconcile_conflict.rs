//! Verify conflict-recovery behaviour.
//!
//! The framework's status write is non-force by contract, so a 409 from a
//! second writer is the designed failure mode on status: a second writer on
//! the controller's own status is a bug worth surfacing, not papering over
//! with `force`. The `"conflict"` error class captures *what* a 409 looks
//! like in a trace; this scenario captures *what the framework does with it*
//! — the recovery decision: does `Reconcile::execute` return `Err`, return an
//! `Action::requeue`, or something else?
//!
//! Reading `crates/kubo/src/reconcile.rs` pins the answer before the scenario
//! is written: `run_apply` calls `patch_status_via_ops`, which calls
//! `KubeHandle::patch_status` (SSA, non-force) and propagates the failure
//! with `?`. There is no catch, no requeue-on-conflict, no retry; a 409 on
//! status surfaces as `Err(ReconcileError)` straight out of
//! `Reconcile::execute`. The `reconcile_error_trace` helper walks that `Err`
//! to the underlying `kube::Error::Api` with `code == 409` and records the
//! `"conflict"` class with a synthetic 409 status. So the recovery behaviour
//! is "propagate as `Err`" — and this scenario pins it by asserting the real
//! arm records `error: Some("conflict")` (which only happens if `execute`
//! returned `Err`). If the framework ever swallows a 409 into an
//! `Action::requeue`, the real arm would record `status: 200, error: None`
//! and this test would flag it.
//!
//! Conflict trigger: a conflicting field manager, not a stale `resourceVersion`.
//! The stale-rv 409 is already pinned by
//! `DivId::StaleResourceVersionStatusPatch`; this scenario is about the
//! framework's recovery path inside a reconcile pass, and the faithful
//! reproduction of what a real controller hits is a *conflicting manager* on
//! status, not a stale rv. Before the `run_reconcile` step, a non-force
//! `apply_status` with `fieldManager = "other-manager"` pre-claims
//! `observedGeneration` — a field the framework writes on every pass
//! (`ResourceStatus.observed_generation`). The framework's `patch_status`
//! (manager `kubo-conformance`, non-force) then tries to claim
//! `observedGeneration` and the real apiserver 409s; the fake does not model
//! status-subresource ownership and accepts the patch (200).
//!
//! This is the same fake gap as `DivId::SsaStatusSecondManagerConflict`: fake
//! accepts a conflicting-manager status patch, real 409s. That divergence is
//! pinned at the raw-HTTP level by `ssa_status.rs`; this scenario pins it
//! through the framework's `Reconcile::execute` recovery path. The divergence
//! is one and the same, so the registry records it once and this test
//! references the same id — it is a second pinning test for
//! `SsaStatusSecondManagerConflict`, not a new divergence. The scenario's
//! distinct contribution is pinning the framework's "propagate as `Err`, do
//! not swallow" recovery contract, which is observable only on the real arm
//! (the fake never 409s, so there is no recovery to observe on the fake
//! side).
//!
//! `observedGeneration` is chosen as the conflict field because it is always
//! written by the framework (`run_apply` sets `observed_generation: Some(generation)`
//! after the effect loop), it is declared in the CRD schema (so it is not pruned
//! by the pruning mechanism), and it is a scalar (no `Condition` serialisation
//! complexity). The choice of field does not affect the recovery behaviour; any
//! field the framework writes would surface the same 409.

use kubo::reconcile::Reconcile;
#[cfg(feature = "k3d")]
use kubo_conformance::Trace;
use kubo_conformance::{
    ConformanceFields, ConformanceObject, DivId, Scenario, assert_known_divergence,
};
use serde_json::json;

/// Build the minimal reconcile pipeline: no effects, no polls, just the status
/// builder. `run_apply` sets the reconciled condition and patches status
/// unconditionally (no prior `last_patched`), so the single status patch is the
/// only wire write the framework makes beyond the finalizer patch. That patch
/// is where the 409 fires on the real arm.
fn pipeline() -> impl for<'a> Fn(
    &'a kubo::environment::KubeHandle,
    std::sync::Arc<ConformanceObject>,
) -> kubo::reconcile::ReadyReconcile<
    'a,
    ConformanceObject,
    ConformanceFields,
> + Send
+ Sync
+ 'static {
    move |ops: &kubo::environment::KubeHandle, obj: std::sync::Arc<ConformanceObject>| {
        Reconcile::new(ops, obj)
            .status(|_state, _prev: &ConformanceFields| ConformanceFields::default())
    }
}

/// The scenario name doubles as the parent object name (slugified).
const SCENARIO_NAME: &str = "ssa-reconcile-conflict";

/// A 409 on `patch_status` inside a reconcile pass propagates as
/// `Err(ReconcileError)`, classified as `"conflict"`; the framework does not
/// swallow it into an `Action::requeue`.
///
/// Procedure:
/// 1. Apply the parent conformance object (empty spec).
/// 2. Pre-claim `observedGeneration` with `fieldManager = "other-manager"`
///    (non-force). Real: 200, `other-manager` owns `observedGeneration`. Fake:
///    200, stores verbatim (no ownership tracking).
/// 3. `run_reconcile` (manager `kubo-conformance`) — the framework adds the
///    finalizer (merge patch, no conflict), then `run_apply` builds the status
///    and `patch_status` writes `observedGeneration` with manager
///    `kubo-conformance`. Real: 409 (conflicting manager on `observedGeneration`,
///    non-force) → `Reconcile::execute` returns `Err` → the harness records
///    `status: 409, error: Some("conflict")`. Fake: 200 (no ownership tracking)
///    → `Reconcile::execute` returns `Ok(Action::requeue(...))` → the harness
///    records `status: 200, error: None`.
///
/// The fake-vs-real difference is the known divergence `SsaStatusSecondManagerConflict`
/// (the conflicting-manager status patch: the fake accepts it, the real 409s).
/// This test references the same id — it is a second pinning test for the same
/// fake gap, exercised through the framework's recovery path. The framework
/// contract pinned here — "propagate as `Err`, do not swallow" — is observable
/// only on the real arm; the fake never 409s, so its `Ok` is the divergence,
/// not the recovery. `assert_equivalent` is therefore not called; the two arms
/// are asserted separately.
#[test]
fn reconcile_status_conflict_propagates_as_err_not_requeue() {
    let _ = assert_known_divergence(DivId::SsaStatusSecondManagerConflict);

    let scenario = Scenario::new(SCENARIO_NAME)
        // 0. Create the parent conformance object.
        .apply("kubo-conformance", json!({ "spec": {} }))
        // 1. Pre-claim observedGeneration with a different field manager
        //    non-force). The framework writes observedGeneration on every
        //    pass, so this is the field the reconcile will conflict over.
        .apply_status("other-manager", json!({ "observedGeneration": 99 }))
        // 2. Reconcile pass: the framework's patch_status (manager
        //    "kubo-conformance", non-force) writes observedGeneration and
        //    409s on the real apiserver (other-manager owns the field); the
        //    fake accepts it (no ownership tracking). The harness records the
        //    recovery: Err → "conflict" class on real, Ok → 200 on fake.
        .run_reconcile("kubo-conformance", "kubo.io/cleanup", pipeline())
        .named("reconcile");

    let fake_trace = scenario.run_against_fake();

    // Entry 2: the reconcile pass. The fake does not model status-subresource
    // ownership, so the framework's conflicting status patch is accepted (200)
    // and Reconcile::execute returns Ok. The harness records a plain 200 with
    // no error class — the recovery path is not exercised on the fake because
    // there is no 409 to recover from.
    let fake_reconcile_entry = fake_trace.entry("reconcile");
    assert_eq!(
        fake_reconcile_entry.status, 200,
        "fake: the conflicting status patch is accepted (no ownership tracking); \
         Reconcile::execute returns Ok"
    );
    assert_eq!(
        fake_reconcile_entry.error, None,
        "fake: no 409 occurs, so no error class is recorded"
    );

    #[cfg(feature = "k3d")]
    {
        let real_trace = scenario.run_against_kube_apiserver();

        // The real apiserver 409s on the conflicting status patch, and the
        // framework propagates it as Err(ReconcileError) — it does not swallow
        // it into an Action::requeue. The reconcile_error_trace helper
        // walks the Err to the underlying kube::Error::Api with code == 409 and
        // records the "conflict" class with a synthetic 409 status. This is the
        // framework's conflict-recovery contract: a 409 on patch_status surfaces
        // as Err, classified as "conflict". If the framework ever swallowed a
        // 409 into a requeue, the real arm would record status: 200, error:
        // None and this assertion would flag the regression.
        let real_reconcile_entry = real_trace.entry("reconcile");
        assert_eq!(
            real_reconcile_entry.status, 409,
            "real: the framework's patch_status must 409 when another manager \
             owns observedGeneration (non-force SSA); if this is 200, either \
             the conflict setup is wrong or force was re-added on status"
        );
        assert_eq!(
            real_reconcile_entry.error.as_deref(),
            Some("conflict"),
            "real: Reconcile::execute must propagate the 409 as Err classified \
             as \"conflict\"; if this is None, the framework swallowed the 409 \
             into an Action::requeue instead of surfacing it"
        );

        // Silence unused-import warning for `Trace` when only the fake path
        // runs; the type is referenced in this block for documentation.
        let _ = std::marker::PhantomData::<Trace>;
    }
}

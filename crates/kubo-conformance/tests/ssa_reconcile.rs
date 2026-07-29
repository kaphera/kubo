//! The reconcile loop, end to end, driven through the framework.
//!
//! The non-reconcile scenarios test primitives against raw HTTP. The
//! ssa_reconcile* scenarios execute a real
//! [`Reconcile::execute`](kubo::reconcile::Reconcile::execute) — finalizer
//! management, status-patch cadence, the `pendingCleanup` ledger, and the
//! reverse-order cleanup walk — against both the fake and a real apiserver.
//! This one asserts the ledger progression matches; its siblings pin
//! child-resource writes, error recording, and conflict recovery.
//!
//! The trace is the `status.pendingCleanup` progression:
//!
//! | Phase                          | Expected `pendingCleanup`       |
//! |--------------------------------|---------------------------------|
//! | After apply (two effects Done) | `["effect-a", "effect-b"]`      |
//! | After cleanup of `effect-b`    | `["effect-a"]`                  |
//! | After cleanup of `effect-a`    | `[]`                            |
//! | Finalizer                      | removed                         |
//!
//! The `[]` state is observable only on the real cluster: on the fake, the
//! framework's finalizer-clear triggers synchronous GC (the fake models the
//! apiserver's GC controller in `Store::merge_patch`), so the object is gone
//! before a GET can read the emptied ledger. On real, the GC controller runs
//! asynchronously, leaving a window where the object exists with
//! `pendingCleanup: []` and no finalizer. This is the async-GC divergence
//! surface, handled path-specifically (like the
//! merge-patch-clears-SSA-owned-finalizer row) rather than via
//! [`assert_equivalent`] on that final GET.
//!
//! Against the real cluster this is also the test that would catch
//! structural-schema pruning breaking the ledger mid-loop: if the CRD forgot
//! to declare `pendingCleanup`, the apiserver would prune it on every status
//! patch, the cleanup walk would see an empty ledger, skip teardown, and the
//! progression would diverge. The scenario reinstalls the derived default
//! schema (`default_open_api_v3_schema`), which declares every framework-managed
//! status field (`conditions`, `observedGeneration`, `pendingCleanup`) with
//! `x-kubernetes-preserve-unknown-fields: true` on the status node, matching
//! what a real operator CRD looks like.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use kubo::{
    ReconcileContext,
    cleanup::CleanupAction,
    effect::{Effect, EffectOutcome},
    environment::{EnvironmentError, KubeHandle},
    reconcile::Reconcile,
    state::StateKey,
};
use kubo_conformance::{
    ConformanceFields, ConformanceObject, DivId, Scenario, assert_known_divergence,
    default_open_api_v3_schema,
};
use serde_json::json;

// ═══════════════════════════════════════════════════════════════════════
// Effects: apply always succeeds; cleanup records itself and (for EffectA)
// retries once so the ledger drains across two cleanup passes.
// ═══════════════════════════════════════════════════════════════════════

/// Recorder shared between the fake and real runs. Each successful cleanup
/// appends its effect's name, so the test can assert the teardown order
/// independently of the (trace-observable) ledger. Cleared between runs
/// because the pipeline closure is `'static` and reused across both backends.
type Recorder = Arc<Mutex<Vec<String>>>;

/// EffectA: apply Done; cleanup retries on the first pass, succeeds on the
/// second. The retry splits the ledger drain across two cleanup passes —
/// pass 1 strikes EffectB and leaves EffectA outstanding (`["effect-a"]`),
/// pass 2 strikes EffectA (`[]`), which is the `pendingCleanup` ledger
/// progression.
struct EffectA {
    recorder: Recorder,
    attempts: Arc<AtomicUsize>,
}

impl StateKey for EffectA {
    type Value = ();
}

#[async_trait]
impl Effect<ConformanceObject> for EffectA {
    type Error = EnvironmentError;

    async fn apply(
        &self,
        _cx: &ReconcileContext<ConformanceObject>,
        _ops: &KubeHandle,
        _state: &kubo::state::State,
    ) -> Result<EffectOutcome<()>, Self::Error> {
        Ok(EffectOutcome::Done(()))
    }

    async fn cleanup(
        &self,
        _cx: &ReconcileContext<ConformanceObject>,
        _ops: &KubeHandle,
    ) -> Result<CleanupAction, Self::Error> {
        let n = self.attempts.fetch_add(1, Ordering::Relaxed);
        if n == 0 {
            // First cleanup pass: request a retry. The framework keeps the
            // finalizer and requeues; the ledger entry survives so the next
            // pass resumes here.
            Ok(CleanupAction::Retry(Box::new(
                kubo::backoff::fixed::Fixed::new(std::time::Duration::from_secs(1)),
            )))
        } else {
            self.recorder.lock().unwrap().push("effect-a".to_string());
            Ok(CleanupAction::Done)
        }
    }

    fn effect_name(&self) -> Option<&'static str> {
        Some("effect-a")
    }
}

/// EffectB: apply Done; cleanup always succeeds. Registered after EffectA, so
/// cleanup runs it first (reverse declaration order) — it is struck off the
/// ledger in pass 1, leaving EffectA for pass 2.
struct EffectB {
    recorder: Recorder,
}

impl StateKey for EffectB {
    type Value = ();
}

#[async_trait]
impl Effect<ConformanceObject> for EffectB {
    type Error = EnvironmentError;

    async fn apply(
        &self,
        _cx: &ReconcileContext<ConformanceObject>,
        _ops: &KubeHandle,
        _state: &kubo::state::State,
    ) -> Result<EffectOutcome<()>, Self::Error> {
        Ok(EffectOutcome::Done(()))
    }

    async fn cleanup(
        &self,
        _cx: &ReconcileContext<ConformanceObject>,
        _ops: &KubeHandle,
    ) -> Result<CleanupAction, Self::Error> {
        self.recorder.lock().unwrap().push("effect-b".to_string());
        Ok(CleanupAction::Done)
    }

    fn effect_name(&self) -> Option<&'static str> {
        Some("effect-b")
    }
}

/// Build the reconcile pipeline for a single pass. Captures the recorder and
/// the retry counter by clone; both are `'static` so the closure satisfies the
/// `RunReconcilePipeline` HRTB without borrowing the test's stack.
fn pipeline(
    recorder: Recorder,
    attempts: Arc<AtomicUsize>,
) -> impl for<'a> Fn(
    &'a KubeHandle,
    Arc<ConformanceObject>,
) -> kubo::reconcile::ReadyReconcile<'a, ConformanceObject, ConformanceFields>
+ Send
+ Sync
+ 'static {
    move |ops: &KubeHandle, obj: Arc<ConformanceObject>| {
        Reconcile::new(ops, obj)
            .effect(EffectA {
                recorder: recorder.clone(),
                attempts: attempts.clone(),
            })
            .effect(EffectB {
                recorder: recorder.clone(),
            })
            .status(|_state, _prev: &ConformanceFields| ConformanceFields::default())
    }
}

// ═══════════════════════════════════════════════════════════════════════
// CRD schema: declare `pendingCleanup` so the real apiserver does not prune
// it, while preserving unknown status fields (conditions, observedGeneration)
// so the framework's full status round-trips.
// ═══════════════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════════════
// The scenario.
// ═══════════════════════════════════════════════════════════════════════
//
// The CRD schema the scenario reinstalls at the start (and restores at the
// end) is the derived default (`default_open_api_v3_schema`): it declares every
// framework-managed status field (`conditions`, `observedGeneration`,
// `pendingCleanup`) with `x-kubernetes-preserve-unknown-fields: true` on the
// status node, so the ledger is never pruned and a new framework status field
// auto-enters.  This is the shape a real operator CRD takes — declare the
// fields you own, preserve the rest.

/// The `pendingCleanup` ledger progression matches fake and real when the
/// framework drives the full reconcile lifecycle.
///
/// Procedure:
/// 1. Reinstall the CRD with a schema declaring `pendingCleanup` (real only;
///    fake no-ops).
/// 2. Apply the spec to create the scenario's object.
/// 3. `run_reconcile` — apply pass: both effects Done, finalizer added.
/// 4. `get` — observe `["effect-a", "effect-b"]` + finalizer present.
/// 5. `delete` — set `deletionTimestamp`, retain (finalizer present).
/// 6. `run_reconcile` — cleanup pass 1: EffectB cleaned (struck), EffectA
///    retries.
/// 7. `get` — observe `["effect-a"]` + finalizer present.
/// 8. `run_reconcile` — cleanup pass 2: EffectA cleaned, finalizer removed.
/// 9. `get` — fake 404 (synchronous GC after finalizer clear); real 200
///    (`[]`, no finalizer, not yet GC'd) or 404 (GC'd). The async-GC
///    divergence — asserted path-specifically, not via `assert_equivalent`.
/// 10. Reinstall the preserve-unknown default so later scenarios are not
///     poisoned.
///
/// Entries 0–7 (the two equivalent `get`s at indices 3 and 6, plus the
/// surrounding raw-HTTP and reconcile steps) are compared with
/// [`assert_equivalent`]. Entry 8 (the final `get`) is the known-divergent
/// tail. Entry 9 (the restore) is equivalent on both paths.
#[test]
fn reconcile_ledger_progression_matches_fake_and_real() {
    let recorder: Recorder = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::new(AtomicUsize::new(0));

    let scenario = Scenario::new("ssa-reconcile-ledger")
        // 0. Reinstall the derived default schema so the framework's status
        //    fields (including pendingCleanup) are declared and not pruned
        //    (real only; fake no-ops).
        .reinstall_crd(default_open_api_v3_schema())
        .named("declare-schema")
        // 1. Create the scenario's object.
        .apply("kubo-conformance", json!({ "spec": {} }))
        .named("seed")
        // 2. Apply pass: both effects Done, finalizer added.
        .run_reconcile(
            "kubo-conformance",
            "kubo.io/cleanup",
            pipeline(recorder.clone(), attempts.clone()),
        )
        .named("reconcile-apply")
        // 3. Observe ["effect-a", "effect-b"] + finalizer.
        .get()
        .named("get-after-apply")
        // 4. Start deletion (retain — finalizer present).
        .delete()
        .named("delete")
        // 5. Cleanup pass 1: EffectB cleaned, EffectA retries.
        .run_reconcile(
            "kubo-conformance",
            "kubo.io/cleanup",
            pipeline(recorder.clone(), attempts.clone()),
        )
        .named("reconcile-cleanup-1")
        // 6. Observe ["effect-a"] + finalizer.
        .get()
        .named("get-after-cleanup-b")
        // 7. Cleanup pass 2: EffectA cleaned, finalizer removed.
        .run_reconcile(
            "kubo-conformance",
            "kubo.io/cleanup",
            pipeline(recorder.clone(), attempts.clone()),
        )
        .named("reconcile-cleanup-2")
        // 8. Observe: fake 404 (GC'd); real 200 (no finalizer) or 404 (GC'd).
        .get()
        .named("get-after-cleanup-a")
        // 9. Restore the derived default so later scenarios are not poisoned.
        .reinstall_crd(default_open_api_v3_schema())
        .named("restore-schema");

    let _ = assert_known_divergence(DivId::AsyncGcAfterFinalizerClear);

    let fake_trace = scenario.run_against_fake();

    // After the apply pass, both effects are recorded and the finalizer is
    // present.
    let fake_after_apply = fake_trace.entry("get-after-apply");
    assert_eq!(
        fake_after_apply.status, 200,
        "fake: get after apply must return 200 (object retained with finalizer)"
    );
    assert_eq!(
        fake_after_apply
            .status_subresource
            .as_ref()
            .and_then(|s| s.get("pendingCleanup")),
        Some(&json!(["effect-a", "effect-b"])),
        "fake: pendingCleanup must list both effects after the apply pass"
    );
    assert_eq!(
        fake_after_apply
            .object
            .as_ref()
            .and_then(|o| o.get("metadata"))
            .and_then(|m| m.get("finalizers")),
        Some(&json!(["kubo.io/cleanup"])),
        "fake: finalizer must be present after the apply pass"
    );

    // After cleanup pass 1, EffectB is struck off and EffectA is still
    // outstanding; the finalizer is still present (cleanup incomplete).
    let fake_after_cleanup_b = fake_trace.entry("get-after-cleanup-b");
    assert_eq!(
        fake_after_cleanup_b.status, 200,
        "fake: get after cleanup pass 1 must return 200 (finalizer still present)"
    );
    assert_eq!(
        fake_after_cleanup_b
            .status_subresource
            .as_ref()
            .and_then(|s| s.get("pendingCleanup")),
        Some(&json!(["effect-a"])),
        "fake: pendingCleanup must list only effect-a after EffectB's cleanup"
    );
    assert_eq!(
        fake_after_cleanup_b
            .object
            .as_ref()
            .and_then(|o| o.get("metadata"))
            .and_then(|m| m.get("finalizers")),
        Some(&json!(["kubo.io/cleanup"])),
        "fake: finalizer must still be present after a retrying cleanup pass"
    );

    // After cleanup pass 2, the finalizer is removed and the fake GCs the
    // object synchronously. The 404 proves the finalizer was cleared (the fake
    // only GCs a terminating object once its last finalizer is gone) — the
    // "Finalizer removed" row of the ledger progression.
    let fake_after_cleanup_a = fake_trace.entry("get-after-cleanup-a");
    assert_eq!(
        fake_after_cleanup_a.status, 404,
        "fake: get after the final cleanup must 404 — the finalizer was cleared \
         and the fake GC'd the object synchronously"
    );

    // The recorder proves both cleanups actually ran, in reverse declaration
    // order (EffectB then EffectA), across the two passes.
    assert_eq!(
        recorder.lock().unwrap().as_slice(),
        ["effect-b", "effect-a"],
        "fake: cleanups must run in reverse order (effect-b then effect-a), \
         split across the two passes by EffectA's retry"
    );

    #[cfg(feature = "k3d")]
    {
        // The recorder and retry counter are shared between the fake and real
        // runs (the closure is `'static` and reused). Reset them so the real
        // run starts from the same initial state as the fake run.
        recorder.lock().unwrap().clear();
        attempts.store(0, Ordering::Relaxed);

        let real_trace = scenario.run_against_kube_apiserver();

        // The schema reinstall, apply, reconcile passes, and the two
        // retained-object GETs whose `pendingCleanup` and finalizer state the
        // framework owns identically are genuinely equivalent on both arms.
        // This is the `pendingCleanup` ledger progression assertion, compared by
        // name so it survives step reordering.
        let equivalent = [
            "declare-schema",
            "seed",
            "reconcile-apply",
            "get-after-apply",
            "delete",
            "reconcile-cleanup-1",
            "get-after-cleanup-b",
            "reconcile-cleanup-2",
        ];
        kubo_conformance::assert_equivalent(
            &fake_trace.select(&equivalent),
            &real_trace.select(&equivalent),
        );

        // The final GET is the async-GC divergence: the fake GCs
        // synchronously (404); the real GC controller runs async, so the
        // object is either still present with no finalizer (200) or already
        // GC'd (404). Both prove the finalizer was removed — the framework
        // only clears it on a fully-successful cleanup. Assert path-specifically
        // rather than via `assert_equivalent`, like the
        // merge-patch-clears-SSA-owned-finalizer row.
        let real_after_cleanup_a = real_trace.entry("get-after-cleanup-a");
        assert!(
            real_after_cleanup_a.status == 200 || real_after_cleanup_a.status == 404,
            "real: get after the final cleanup must be 200 (finalizer gone, not \
             yet GC'd) or 404 (GC'd) — both prove the finalizer was removed. \
             Got {}",
            real_after_cleanup_a.status
        );
        if real_after_cleanup_a.status == 200 {
            // Object still present: the finalizer must be gone (normalised to a
            // missing `finalizers` field once empty).
            assert_eq!(
                real_after_cleanup_a
                    .object
                    .as_ref()
                    .and_then(|o| o.get("metadata"))
                    .and_then(|m| m.get("finalizers")),
                None,
                "real: a 200 after the final cleanup means the finalizer was \
                 cleared (absent), awaiting async GC"
            );
            assert_eq!(
                real_after_cleanup_a
                    .status_subresource
                    .as_ref()
                    .and_then(|s| s.get("pendingCleanup")),
                Some(&json!([])),
                "real: a 200 after the final cleanup must show the emptied ledger"
            );
        }

        // The restore reinstall is equivalent on both paths.
        assert_eq!(
            fake_trace.entry("restore-schema"),
            real_trace.entry("restore-schema"),
            "the restore reinstall must record identically on both paths"
        );

        // The recorder proves the real cluster ran the same cleanups in the
        // same order — the framework's reverse-order cleanup walk is
        // backend-independent.
        assert_eq!(
            recorder.lock().unwrap().as_slice(),
            ["effect-b", "effect-a"],
            "real: cleanups must run in reverse order (effect-b then effect-a), \
             matching the fake"
        );
    }
}

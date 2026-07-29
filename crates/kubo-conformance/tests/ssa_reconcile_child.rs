//! Effects that write: exercise `KubeHandle::apply` end-to-end against a child
//! resource.
//!
//! The `ssa_reconcile.rs` harness drives a real `Reconcile::execute` and
//! asserts the `pendingCleanup` ledger progression, the finalizer lifecycle,
//! and the status cadence.  But its effects return `Done` without touching the
//! `KubeHandle`, so the framework's second-hottest write — child-resource apply
//! via `KubeHandle::apply`, and child deletion via `KubeHandle::delete` during
//! cleanup — never runs against either backend.  The end-to-end test was
//! end-to-end for the ledger, not for an operator's actual shape (a CR plus its
//! children).
//!
//! This scenario closes that gap.  Its single effect applies a ConfigMap child
//! in `apply` (via `ops.apply::<ConfigMap>`) and deletes it in `cleanup` (via
//! `ops.delete::<ConfigMap>`), so the framework's own SSA-apply and delete code
//! paths run end-to-end against both the fake and a real apiserver.  The child's
//! lifecycle is observed independently through the new `get_path` step (a GET
//! against an arbitrary path) rather than only through the effect's own return
//! value, so the equivalence assertion rides on the observable surface — the
//! child appearing then disappearing on both backends — not on "the framework
//! returned `Ok`".
//!
//! ConfigMap is a cluster-native core-v1 resource, so this scenario needs no
//! `ReinstallCrd` step and coexists with the parent `ConformanceObject` CRD
//! without any schema interaction.  The fake's `parse_route` is GVK-agnostic, so
//! a core-group path (`/api/v1/namespaces/default/configmaps/<name>`) routes
//! through the same object handler as the conformance CRD's path.
//!
//! The child is named `<scenario-object>-child` and lives in `default` alongside
//! the parent.  No harness-level child teardown is needed: on the success path
//! the scenario's own cleanup pass deletes the child (step 5), and SSA-apply is
//! idempotent so a lingering child from a prior panicking run is overwritten by
//! the next run's apply and then deleted by its cleanup — self-healing across
//! runs.  A panicking run leaves the child for inspection; CI deletes the
//! cluster `if: always()` (the per-scenario-object isolation discipline).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kubo::{
    ReconcileContext,
    cleanup::CleanupAction,
    effect::{Effect, EffectOutcome},
    environment::{EnvironmentError, KubeHandle, ObjectRef},
    reconcile::Reconcile,
    state::StateKey,
};
use kubo_conformance::{ConformanceFields, ConformanceObject, Scenario};
use serde_json::json;

// ═══════════════════════════════════════════════════════════════════════
// Effect: applies a ConfigMap child in `apply`, deletes it in `cleanup`.
// ═══════════════════════════════════════════════════════════════════════

/// Recorder shared between the fake and real runs.  Each successful effect
/// phase appends its name, so the test can assert the framework actually drove
/// the effect's `apply` and `cleanup` (the `get_path` steps assert the
/// *outcome*; the recorder asserts the *cause*).  Cleared between runs because
/// the pipeline closure is `'static` and reused across both backends.
type Recorder = Arc<Mutex<Vec<String>>>;

/// The child ConfigMap's name, captured by value into the `'static` pipeline
/// closure so the effect can build and address the child without borrowing the
/// test's stack.
type ChildName = String;

/// An effect that exercises the framework's child-resource write path: it
/// applies a ConfigMap (the operator's canonical child shape) in `apply` and
/// deletes it in `cleanup`.  This is the shape `ssa_reconcile.rs` deliberately
/// avoided — its effects return `Done` without touching `KubeHandle` — so this
/// is the only scenario that runs `KubeHandle::apply` / `KubeHandle::delete`
/// against a child through the framework's own code on both backends.
struct ChildEffect {
    recorder: Recorder,
    child_name: ChildName,
}

impl StateKey for ChildEffect {
    type Value = ();
}

#[async_trait]
impl Effect<ConformanceObject> for ChildEffect {
    type Error = EnvironmentError;

    async fn apply(
        &self,
        _cx: &ReconcileContext<ConformanceObject>,
        ops: &KubeHandle,
        _state: &kubo::state::State,
    ) -> Result<EffectOutcome<()>, Self::Error> {
        // Build the child ConfigMap with deterministic data so the post-apply
        // `get_path` can assert the persisted content matches fake and real.
        let mut data = BTreeMap::new();
        data.insert("conformance".to_string(), "child".to_string());
        let cm = ConfigMap {
            metadata: ObjectMeta {
                name: Some(self.child_name.clone()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            data: Some(data),
            ..Default::default()
        };
        // The framework's non-force SSA apply — the second-hottest write after
        // status.  `KubeHandle::apply` requires `metadata.name`; the child name
        // is set above.  A failure here surfaces as a reconcile `Err`, which
        // `run_reconcile_pass` records as an error-class trace entry (the
        // "reconcile errors are trace data, not panics" scenario); here the
        // success path is what the scenario asserts.
        ops.apply::<ConfigMap>("default", cm).await?;
        self.recorder.lock().unwrap().push("applied".to_string());
        Ok(EffectOutcome::Done(()))
    }

    async fn cleanup(
        &self,
        _cx: &ReconcileContext<ConformanceObject>,
        ops: &KubeHandle,
    ) -> Result<CleanupAction, Self::Error> {
        // The framework's child-delete path during cleanup.  A ConfigMap has no
        // finalizers, so the apiserver removes it immediately; the subsequent
        // `get_path` observes 404 on both backends.
        ops.delete::<ConfigMap>(&ObjectRef::new("default", &self.child_name))
            .await?;
        self.recorder.lock().unwrap().push("deleted".to_string());
        Ok(CleanupAction::Done)
    }

    fn effect_name(&self) -> Option<&'static str> {
        Some("child-effect")
    }
}

/// Build the reconcile pipeline for a single pass.  Captures the recorder and
/// the child name by clone; both are `'static` so the closure satisfies the
/// `RunReconcilePipeline` HRTB without borrowing the test's stack.
fn pipeline(
    recorder: Recorder,
    child_name: ChildName,
) -> impl for<'a> Fn(
    &'a KubeHandle,
    Arc<ConformanceObject>,
) -> kubo::reconcile::ReadyReconcile<'a, ConformanceObject, ConformanceFields>
+ Send
+ Sync
+ 'static {
    move |ops: &KubeHandle, obj: Arc<ConformanceObject>| {
        Reconcile::new(ops, obj)
            .effect(ChildEffect {
                recorder: recorder.clone(),
                child_name: child_name.clone(),
            })
            .status(|_state, _prev: &ConformanceFields| ConformanceFields::default())
    }
}

// ═══════════════════════════════════════════════════════════════════════
// The scenario.
// ═══════════════════════════════════════════════════════════════════════

/// The scenario name doubles as the parent object name (slugified).  It is
/// already lowercase-hyphenated, so slugification is a no-op and the child name
/// derived below matches the actual object name the harness uses.
const SCENARIO_NAME: &str = "ssa-effect-apply";

/// The framework's `KubeHandle::apply` / `delete` against a child resource
/// produce the same observable lifecycle on the fake and a real apiserver.
///
/// Procedure:
/// 1. Apply the parent conformance object (empty spec).
/// 2. `run_reconcile` — apply pass: the effect applies the ConfigMap child via
///    `ops.apply`; the framework adds the finalizer and `pendingCleanup:
///    ["child-effect"]`.
/// 3. `get` the parent — observe the finalizer and the ledger entry.
/// 4. `get_path` the child — observe it exists (200) with the expected data.
/// 5. `delete` the parent — set `deletionTimestamp` (retained — finalizer
///    present).
/// 6. `run_reconcile` — cleanup pass: the effect deletes the child via
///    `ops.delete`; the framework strikes the ledger entry and clears the
///    finalizer.
/// 7. `get_path` the child — observe it is gone (404).
///
/// The whole trace is equivalent on both backends: there is no async-GC tail
/// here because the scenario never GETs the parent after the cleanup pass
/// (that is the reconcile-ledger scenario's concern).  The child delete is
/// synchronous (a ConfigMap has no finalizers), so step 7 is 404 on both.  The recorder asserts the effect's
/// `apply` and `cleanup` ran on both backends.
#[test]
fn reconcile_child_lifecycle_matches_fake_and_real() {
    let recorder: Recorder = Arc::new(Mutex::new(Vec::new()));

    // The scenario name is already slugified, so the parent object name is
    // `SCENARIO_NAME` and the child ConfigMap name is `<parent>-child`.  The
    // child lives in `default` alongside the parent.
    let child_name: ChildName = format!("{SCENARIO_NAME}-child");
    let child_path = format!("/api/v1/namespaces/default/configmaps/{child_name}");

    let scenario = Scenario::new(SCENARIO_NAME)
        // 0. Create the parent conformance object.
        .apply("kubo-conformance", json!({ "spec": {} }))
        // 1. Apply pass: effect applies the child; finalizer + ledger entry added.
        .run_reconcile(
            "kubo-conformance",
            "kubo.io/cleanup",
            pipeline(recorder.clone(), child_name.clone()),
        )
        // 2. Observe the parent: finalizer present, pendingCleanup ["child-effect"].
        .get()
        .named("parent-after-apply")
        // 3. Observe the child independently: 200 with the expected data.
        .get_path(child_path.clone())
        .named("child-after-apply")
        // 4. Start deletion (retain — finalizer present).
        .delete()
        // 5. Cleanup pass: effect deletes the child; ledger struck, finalizer cleared.
        .run_reconcile(
            "kubo-conformance",
            "kubo.io/cleanup",
            pipeline(recorder.clone(), child_name.clone()),
        )
        // 6. Observe the child independently: 404 (gone on both backends).
        .get_path(child_path)
        .named("child-after-cleanup");

    let fake_trace = scenario.run_against_fake();

    // After the apply pass, the parent carries the finalizer and the ledger
    // entry for the child effect.
    let fake_parent_after_apply = fake_trace.entry("parent-after-apply");
    assert_eq!(
        fake_parent_after_apply.status, 200,
        "fake: get after apply must return 200 (object retained with finalizer)"
    );
    assert_eq!(
        fake_parent_after_apply
            .status_subresource
            .as_ref()
            .and_then(|s| s.get("pendingCleanup")),
        Some(&json!(["child-effect"])),
        "fake: pendingCleanup must list the child effect after the apply pass"
    );
    assert_eq!(
        fake_parent_after_apply
            .object
            .as_ref()
            .and_then(|o| o.get("metadata"))
            .and_then(|m| m.get("finalizers")),
        Some(&json!(["kubo.io/cleanup"])),
        "fake: finalizer must be present after the apply pass"
    );

    // The child ConfigMap exists with the expected data — independent
    // observation that the framework's `ops.apply` actually persisted the child.
    let fake_child_after_apply = fake_trace.entry("child-after-apply");
    assert_eq!(
        fake_child_after_apply.status, 200,
        "fake: get_path of the child must return 200 after the apply pass"
    );
    assert_eq!(
        fake_child_after_apply
            .object
            .as_ref()
            .and_then(|o| o.get("data"))
            .and_then(|d| d.get("conformance")),
        Some(&json!("child")),
        "fake: the child ConfigMap must carry the applied data"
    );

    // After the cleanup pass, the child is gone — independent observation
    // that the framework's `ops.delete` removed the child.
    let fake_child_after_cleanup = fake_trace.entry("child-after-cleanup");
    assert_eq!(
        fake_child_after_cleanup.status, 404,
        "fake: get_path of the child must return 404 after the cleanup pass"
    );

    // The recorder proves the effect's apply and cleanup both ran.
    assert_eq!(
        recorder.lock().unwrap().as_slice(),
        ["applied", "deleted"],
        "fake: the effect must have applied then deleted the child"
    );

    #[cfg(feature = "k3d")]
    {
        use kubo_conformance::Trace;

        // The recorder is shared between the fake and real runs (the closure is
        // `'static` and reused).  Reset it so the real run starts from the same
        // initial state as the fake run.
        recorder.lock().unwrap().clear();

        let real_trace = scenario.run_against_kube_apiserver();

        // The whole trace is equivalent: the parent apply, both reconcile passes
        // (synthetic 200/None), the parent get (finalizer + ledger, framework-owned
        // and identical on both), the child get after apply (200, deterministic
        // data, normalised), the parent delete (200/None), and the child get after
        // cleanup (404/None).  There is no async-GC tail because the parent is not
        // GET'd after the cleanup pass — that is the reconcile-ledger scenario's
        // surface, already covered by `ssa_reconcile.rs`.  This is the
        // child-resource apply/delete equivalence: the framework's child
        // apply/delete produce the same observable lifecycle on both backends.
        kubo_conformance::assert_equivalent(&fake_trace, &real_trace);

        // The recorder proves the real cluster ran the same effect phases in the
        // same order — the framework's effect dispatch is backend-independent.
        assert_eq!(
            recorder.lock().unwrap().as_slice(),
            ["applied", "deleted"],
            "real: the effect must have applied then deleted the child, matching the fake"
        );

        // Belt-and-braces: the equivalence check above already compares entries 3
        // and 6, but assert the child-specific expectations explicitly so a failure
        // names the child lifecycle rather than dumping the full trace diff.
        let real_child_after_apply = real_trace.entry("child-after-apply");
        assert_eq!(
            real_child_after_apply.status, 200,
            "real: get_path of the child must return 200 after the apply pass"
        );
        let real_child_after_cleanup = real_trace.entry("child-after-cleanup");
        assert_eq!(
            real_child_after_cleanup.status, 404,
            "real: get_path of the child must return 404 after the cleanup pass"
        );

        // Silence unused-import warning for `Trace` when only the fake path runs;
        // the type is used in `assert_equivalent`'s signature on the real path.
        let _ = std::marker::PhantomData::<Trace>;
    }
}

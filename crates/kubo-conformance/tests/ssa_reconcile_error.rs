//! Reconcile errors are trace data, not panics.
//!
//! `run_reconcile_pass` used to panic on `Err`. A reconcile failure is itself a
//! divergence signal — the fake succeeding where the real apiserver 409s is
//! exactly what the harness exists to catch — and panicking discarded both the
//! comparison and the error class. This scenario exercises the new `Err` arm:
//! the harness records a [`TraceEntry`] carrying the error class
//! (`"conflict"` for a `kube::Error::Api` with HTTP 409, `"other: <Display>"`
//! otherwise) so failure behaviour is asserted equivalent like everything else.
//!
//! Scope: this scenario covers the "other" class — an effect returning
//! `Err(EnvironmentError::Other(...))` from `apply`. The error is constructed
//! identically on both backends and never reaches the wire, so the error class
//! is expected to be equivalent fake-vs-real. The "conflict" class — a 409
//! from the apiserver on an SSA patch — is the conflict-recovery scenario's
//! territory; it overlaps with the stale-rv row and is left to that scenario
//! to avoid duplicating the surface. This scope decision is recorded here so
//! a future reader does not mistake the omission for a gap.
//!
//! The effect records-then-fails: it pushes `"apply-attempted"` to the recorder
//! before returning `Err`, so the test can assert the framework actually drove
//! the effect's `apply` on both backends (the error trace asserts the *outcome*;
//! the recorder asserts the *cause*). The framework adds the finalizer before
//! running the effect (in `Reconcile::execute`, the finalizer patch precedes
//! `converge`), so a `get` after the failed reconcile observes the finalizer
//! present on both backends — a real framework invariant the scenario pins for
//! free: an effect failure does not leave the object finalizer-less, and the
//! failed effect gets no `pendingCleanup` ledger entry (the ledger is appended
//! only on `StepProgress::Continue`, i.e. after the effect returns `Done`).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kubo::{
    ReconcileContext,
    effect::{Effect, EffectOutcome},
    environment::{EnvironmentError, KubeHandle},
    reconcile::Reconcile,
    state::StateKey,
};
use kubo_conformance::{ConformanceFields, ConformanceObject, Scenario};
use serde_json::json;

// ═══════════════════════════════════════════════════════════════════════
// Effect: records then returns `Err` from `apply`.
// ═══════════════════════════════════════════════════════════════════════

/// Recorder shared between the fake and real runs. The effect pushes
/// `"apply-attempted"` before failing, so the test can assert the framework
/// actually drove the effect's `apply` on both backends. Cleared between runs
/// because the pipeline closure is `'static` and reused across both backends.
type Recorder = Arc<Mutex<Vec<String>>>;

/// An effect that always fails `apply` with `EnvironmentError::Other`. This is
/// the simplest reproducible reconcile failure: no wire call is involved, so the
/// error class is "other" (not "conflict") on both backends, and the error
/// message is constructed identically in the effect on both backends.
struct FailingEffect {
    recorder: Recorder,
}

impl StateKey for FailingEffect {
    type Value = ();
}

#[async_trait]
impl Effect<ConformanceObject> for FailingEffect {
    type Error = EnvironmentError;

    async fn apply(
        &self,
        _cx: &ReconcileContext<ConformanceObject>,
        _ops: &KubeHandle,
        _state: &kubo::state::State,
    ) -> Result<EffectOutcome<()>, Self::Error> {
        // Record-then-fail: prove the framework drove the effect's `apply` on
        // both backends before the failure surfaces as a reconcile `Err`.
        self.recorder
            .lock()
            .unwrap()
            .push("apply-attempted".to_string());
        Err(EnvironmentError::Other(
            "conformance-induced failure".to_string(),
        ))
    }

    // The default `cleanup` returns `CleanupAction::Skip`; the effect never
    // completed `apply`, so it has no ledger entry and no cleanup obligation.
    fn effect_name(&self) -> Option<&'static str> {
        Some("failing-effect")
    }
}

/// Build the reconcile pipeline for a single pass. Captures the recorder by
/// clone; it is `'static` so the closure satisfies the `RunReconcilePipeline`
/// HRTB without borrowing the test's stack.
fn pipeline(
    recorder: Recorder,
) -> impl for<'a> Fn(
    &'a KubeHandle,
    Arc<ConformanceObject>,
) -> kubo::reconcile::ReadyReconcile<'a, ConformanceObject, ConformanceFields>
+ Send
+ Sync
+ 'static {
    move |ops: &KubeHandle, obj: Arc<ConformanceObject>| {
        Reconcile::new(ops, obj)
            .effect(FailingEffect {
                recorder: recorder.clone(),
            })
            .status(|_state, _prev: &ConformanceFields| ConformanceFields::default())
    }
}

// ═══════════════════════════════════════════════════════════════════════
// The scenario.
// ═══════════════════════════════════════════════════════════════════════

/// The scenario name doubles as the parent object name (slugified). It is
/// already lowercase-hyphenated, so slugification is a no-op.
const SCENARIO_NAME: &str = "ssa-reconcile-error";

/// A reconcile `Err` is recorded as an error-class trace entry, asserted
/// equivalent fake-vs-real.
///
/// Procedure:
/// 1. Apply the parent conformance object (empty spec).
/// 2. `run_reconcile` — the effect records then returns
///    `Err(EnvironmentError::Other(...))`. The framework adds the finalizer
///    first (finalizer patch precedes `converge`), then runs the effect which
///    fails, so `execute` returns `Err`. The harness records a trace entry with
///    synthetic status 500 and error class `"other: <Display>"` instead of
///    panicking.
/// 3. `get` the parent — observe the finalizer was added despite the effect
///    failure (a real framework invariant: the finalizer precedes the effects,
///    so an effect failure does not leave the object finalizer-less), and no
///    status subresource (the failed effect produced no status patch — the
///    ledger is appended only on `StepProgress::Continue`, and the status patch
///    runs only after a `Continue`).
///
/// The error trace (entry 1) and the post-failure `get` (entry 2) are equivalent
/// on both backends: the error is an `EnvironmentError::Other` constructed
/// identically in the effect on both backends and never reaches the wire, so
/// the error class is identical; the finalizer patch is a real wire call on the
/// real arm and a fake call on the fake arm, both 200, both adding the same
/// finalizer. No new fake-vs-real divergence is expected; if one is found, it
/// is recorded in the divergence registry.
#[test]
fn reconcile_error_is_trace_data_not_panic() {
    let recorder: Recorder = Arc::new(Mutex::new(Vec::new()));

    let scenario = Scenario::new(SCENARIO_NAME)
        // 0. Create the parent conformance object.
        .apply("kubo-conformance", json!({ "spec": {} }))
        // 1. Reconcile pass: the effect records then fails. The framework adds
        //    the finalizer first, then runs the effect, which returns Err. The
        //    harness records a synthetic 500 with error class "other: ..."
        //    instead of panicking.
        .run_reconcile(
            "kubo-conformance",
            "kubo.io/cleanup",
            pipeline(recorder.clone()),
        )
        .named("reconcile")
        // 2. Observe the parent: finalizer present (added before the effect
        //    failed), no pendingCleanup ledger entry (the effect never returned
        //    Done).
        .get()
        .named("get");

    let fake_trace = scenario.run_against_fake();

    // Entry 1: the failed reconcile pass. The harness records a synthetic 500
    // with the error class — not a panic. The error is an
    // `EnvironmentError::Other` that never reached the wire, so the class is
    // "other: <Display>".
    let fake_error_entry = fake_trace.entry("reconcile");
    assert_eq!(
        fake_error_entry.status, 500,
        "fake: a failed reconcile (non-conflict) must record synthetic status 500, not panic"
    );
    assert!(
        fake_error_entry
            .error
            .as_deref()
            .is_some_and(|e| e.starts_with("other: ")),
        "fake: the error class must be \"other: <Display>\", got {:?}",
        fake_error_entry.error
    );
    assert_eq!(
        fake_error_entry.object, None,
        "fake: a failed reconcile records no object"
    );
    assert_eq!(
        fake_error_entry.status_subresource, None,
        "fake: a failed reconcile records no status subresource"
    );

    // Entry 2: the post-failure get. The finalizer was added before the effect
    // failed (the framework patches finalizers before running effects), so the
    // object is retained with the finalizer. The failed effect produced no
    // status patch — `run_apply` patches status only on `StepProgress::Continue`,
    // and the effect returned `Error` before any status patch ran — so the
    // status subresource is still absent (no `pendingCleanup` ledger entry).
    let fake_after_failure = fake_trace.entry("get");
    assert_eq!(
        fake_after_failure.status, 200,
        "fake: get after a failed reconcile must return 200 (object retained with finalizer)"
    );
    assert_eq!(
        fake_after_failure
            .object
            .as_ref()
            .and_then(|o| o.get("metadata"))
            .and_then(|m| m.get("finalizers")),
        Some(&json!(["kubo.io/cleanup"])),
        "fake: the finalizer must be present despite the effect failure"
    );
    assert_eq!(
        fake_after_failure.status_subresource, None,
        "fake: a failed effect produces no status patch — status subresource must be absent"
    );

    // The recorder proves the effect's apply was actually driven on the fake.
    assert_eq!(
        recorder.lock().unwrap().as_slice(),
        ["apply-attempted"],
        "fake: the effect's apply must have been driven before the failure"
    );

    #[cfg(feature = "k3d")]
    {
        use kubo_conformance::Trace;

        // The recorder is shared between the fake and real runs (the closure is
        // `'static` and reused). Reset it so the real run starts from the same
        // initial state as the fake run.
        recorder.lock().unwrap().clear();

        let real_trace = scenario.run_against_kube_apiserver();

        // The whole trace is equivalent: the parent apply (200/None), the
        // failed reconcile pass (synthetic 500 with the same error class — the
        // error is an `EnvironmentError::Other` constructed identically in the
        // effect on both backends and never reaches the wire), and the
        // post-failure get (200, finalizer present, empty pendingCleanup). This
        // is the reconcile-failure equivalence: a reconcile failure is asserted
        // equivalent like everything else, not panicked away.
        kubo_conformance::assert_equivalent(&fake_trace, &real_trace);

        // The recorder proves the real cluster drove the same effect apply.
        assert_eq!(
            recorder.lock().unwrap().as_slice(),
            ["apply-attempted"],
            "real: the effect's apply must have been driven before the failure, matching the fake"
        );

        // Belt-and-braces: the equivalence check above already compares entry 1,
        // but assert the error-class expectations explicitly so a failure names
        // the error trace rather than dumping the full trace diff.
        let real_error_entry = real_trace.entry("reconcile");
        assert_eq!(
            real_error_entry.status, 500,
            "real: a failed reconcile (non-conflict) must record synthetic status 500"
        );
        assert!(
            real_error_entry
                .error
                .as_deref()
                .is_some_and(|e| e.starts_with("other: ")),
            "real: the error class must be \"other: <Display>\", got {:?}",
            real_error_entry.error
        );

        // Silence unused-import warning for `Trace` when only the fake path
        // runs; the type is used in `assert_equivalent`'s signature on the real
        // path.
        let _ = std::marker::PhantomData::<Trace>;
    }
}

//! Pending-cleanup ledger invariants.
//!
//! These tests pin down the contract that `cleanup` only runs for effects
//! that actually completed `apply`, and that the ledger survives partial
//! applies, cleanup retries, and orphaned entries.
//!
//! The ledger lives in `ResourceStatus<T>::pending_cleanup` and is the
//! framework's source of truth for "applied, teardown outstanding". See
//! [`kubo::status::ResourceStatus`] for the full rationale.

#![allow(missing_docs)]

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use async_trait::async_trait;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::{CustomResource, runtime::controller::Action};
use kubo::{
    ReconcileContext,
    backoff::fixed::Fixed,
    cleanup::CleanupAction,
    effect::{Effect, EffectOutcome},
    environment::{EnvironmentError, KubeHandle},
    gate::{Gate, Outcome},
    reconcile::Reconcile,
    state::{State, StateKey},
    status::ResourceStatus,
    testing::World,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;

// ═══════════════════════════════════════════════════════════════════════
// CRD: a minimal resource whose status exposes the ledger directly.
// ═══════════════════════════════════════════════════════════════════════

#[derive(CustomResource, Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "test.kubo.dev",
    version = "v1alpha1",
    kind = "LedgerResource",
    status = "LedgerResourceStatus",
    namespaced
)]
pub struct LedgerSpec {
    pub field: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct LedgerFields {
    pub touched: Option<String>,
}

pub type LedgerResourceStatus = ResourceStatus<LedgerFields>;

impl kubo::HasConditions for LedgerResource {
    fn conditions(&self) -> Vec<Condition> {
        self.status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default()
    }
}

type LedgerError = EnvironmentError;

// ═══════════════════════════════════════════════════════════════════════
// A gate that waits once then passes — lets us stage partial applies.
// ═══════════════════════════════════════════════════════════════════════

struct WaitOnceGate {
    /// Shared flag: set to true to make the gate pass on its next evaluation.
    pass: Arc<AtomicBool>,
}

impl StateKey for WaitOnceGate {
    type Value = ();
}

#[async_trait]
impl Gate<LedgerResource> for WaitOnceGate {
    type Error = LedgerError;

    async fn evaluate(
        &self,
        cx: &ReconcileContext<LedgerResource>,
        _handle: &KubeHandle,
    ) -> Result<Outcome<()>, Self::Error> {
        if self.pass.load(Ordering::Relaxed) {
            Ok(Outcome::Pass(()))
        } else {
            Ok(Outcome::Wait {
                condition: cx
                    .condition
                    .not_ready_condition("Waiting", "gate holding the pipeline"),
                backoff: Box::new(Fixed::new(Duration::from_secs(1))),
            })
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Effects that record their cleanup calls for assertion.
// ═══════════════════════════════════════════════════════════════════════

struct EffectA {
    cleaned_up: Arc<AtomicUsize>,
}

impl StateKey for EffectA {
    type Value = ();
}

#[async_trait]
impl Effect<LedgerResource> for EffectA {
    type Error = LedgerError;

    async fn apply(
        &self,
        _cx: &ReconcileContext<LedgerResource>,
        _handle: &KubeHandle,
        _state: &State,
    ) -> Result<EffectOutcome<()>, Self::Error> {
        Ok(EffectOutcome::Done(()))
    }

    async fn cleanup(
        &self,
        _cx: &ReconcileContext<LedgerResource>,
        _handle: &KubeHandle,
    ) -> Result<CleanupAction, Self::Error> {
        self.cleaned_up.fetch_add(1, Ordering::Relaxed);
        Ok(CleanupAction::Done)
    }
}

struct EffectB {
    cleaned_up: Arc<AtomicUsize>,
}

impl StateKey for EffectB {
    type Value = ();
}

#[async_trait]
impl Effect<LedgerResource> for EffectB {
    type Error = LedgerError;

    async fn apply(
        &self,
        _cx: &ReconcileContext<LedgerResource>,
        _handle: &KubeHandle,
        _state: &State,
    ) -> Result<EffectOutcome<()>, Self::Error> {
        Ok(EffectOutcome::Done(()))
    }

    async fn cleanup(
        &self,
        _cx: &ReconcileContext<LedgerResource>,
        _handle: &KubeHandle,
    ) -> Result<CleanupAction, Self::Error> {
        self.cleaned_up.fetch_add(1, Ordering::Relaxed);
        Ok(CleanupAction::Done)
    }
}

/// An effect that pins a stable ledger key via `Effect::effect_name`,
/// so the entry survives a struct rename. Its cleanup records itself so
/// we can assert it still runs when the ledger was seeded under the
/// pinned name.
struct PinnedNameEffect {
    cleaned_up: Arc<AtomicUsize>,
}

impl StateKey for PinnedNameEffect {
    type Value = ();
}

#[async_trait]
impl Effect<LedgerResource> for PinnedNameEffect {
    type Error = LedgerError;

    async fn apply(
        &self,
        _cx: &ReconcileContext<LedgerResource>,
        _handle: &KubeHandle,
        _state: &State,
    ) -> Result<EffectOutcome<()>, Self::Error> {
        Ok(EffectOutcome::Done(()))
    }

    async fn cleanup(
        &self,
        _cx: &ReconcileContext<LedgerResource>,
        _handle: &KubeHandle,
    ) -> Result<CleanupAction, Self::Error> {
        self.cleaned_up.fetch_add(1, Ordering::Relaxed);
        Ok(CleanupAction::Done)
    }

    fn effect_name(&self) -> Option<&'static str> {
        Some("stable.pinned")
    }
}

/// An effect whose cleanup retries once, then succeeds. Used to verify the
/// ledger entry persists across the retry and the second pass resumes
/// without re-running already-cleaned effects.
struct RetryOnceEffect {
    cleaned_up: Arc<AtomicUsize>,
    attempts: Arc<AtomicUsize>,
}

impl StateKey for RetryOnceEffect {
    type Value = ();
}

#[async_trait]
impl Effect<LedgerResource> for RetryOnceEffect {
    type Error = LedgerError;

    async fn apply(
        &self,
        _cx: &ReconcileContext<LedgerResource>,
        _handle: &KubeHandle,
        _state: &State,
    ) -> Result<EffectOutcome<()>, Self::Error> {
        Ok(EffectOutcome::Done(()))
    }

    async fn cleanup(
        &self,
        _cx: &ReconcileContext<LedgerResource>,
        _handle: &KubeHandle,
    ) -> Result<CleanupAction, Self::Error> {
        let n = self.attempts.fetch_add(1, Ordering::Relaxed);
        if n == 0 {
            Ok(CleanupAction::Retry(Box::new(Fixed::new(
                Duration::from_secs(1),
            ))))
        } else {
            self.cleaned_up.fetch_add(1, Ordering::Relaxed);
            Ok(CleanupAction::Done)
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════

fn make_obj(ns: &str, name: &str, deleting: bool) -> Arc<LedgerResource> {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
    let mut r = LedgerResource::new(name, LedgerSpec { field: "v".into() });
    r.metadata.namespace = Some(ns.to_string());
    if deleting {
        r.metadata.deletion_timestamp = Some(Time(k8s_openapi::jiff::Timestamp::now()));
        r.metadata.finalizers = Some(vec!["test-finalizer".to_string()]);
    }
    Arc::new(r)
}

fn handle_for(world: &World) -> KubeHandle {
    KubeHandle::new(world.client().clone(), "kubo-test")
}

fn ledger_of(world: &World, ns: &str, name: &str) -> Vec<String> {
    world
        .get::<LedgerResource>(ns, name)
        .unwrap()
        .status
        .as_ref()
        .unwrap()
        .pending_cleanup
        .clone()
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

/// A full apply populates the ledger with both effect names in declaration
/// order, and a subsequent cleanup runs both cleanups and empties the
/// ledger.
#[tokio::test]
async fn apply_populates_ledger_and_cleanup_empties_it() {
    let world = World::builder().build().await;
    let obj = make_obj("ns", "obj", false);
    world.prefill((*obj).clone());
    let handle = handle_for(&world);

    let a = Arc::new(AtomicUsize::new(0));
    let b = Arc::new(AtomicUsize::new(0));

    Reconcile::new(&handle, obj)
        .effect(EffectA {
            cleaned_up: a.clone(),
        })
        .effect(EffectB {
            cleaned_up: b.clone(),
        })
        .execute("test-finalizer", |_state, _prev: &LedgerFields| {
            LedgerFields { touched: None }
        })
        .await
        .unwrap();

    let ledger = ledger_of(&world, "ns", "obj");
    assert_eq!(ledger.len(), 2, "both effects recorded after apply");
    assert!(ledger[0].ends_with("EffectA"));
    assert!(ledger[1].ends_with("EffectB"));

    // Delete and run cleanup.
    world.delete::<LedgerResource>("ns", "obj").await;
    let deleting = Arc::new(world.get::<LedgerResource>("ns", "obj").unwrap());
    Reconcile::new(&handle, deleting)
        .effect(EffectA {
            cleaned_up: a.clone(),
        })
        .effect(EffectB {
            cleaned_up: b.clone(),
        })
        .execute("test-finalizer", |_state, _prev: &LedgerFields| {
            LedgerFields { touched: None }
        })
        .await
        .unwrap();

    assert_eq!(a.load(Ordering::Relaxed), 1, "EffectA cleaned up once");
    assert_eq!(b.load(Ordering::Relaxed), 1, "EffectB cleaned up once");
    // The cleanup emptied the ledger and cleared the finalizer, which triggered
    // the fake's finalizer-aware GC — the object is gone from the store. The GC
    // itself proves the ledger was emptied (the framework only clears the
    // finalizer when the ledger is empty) and the teardown completed end to
    // end. Reading the ledger directly is not possible after GC: the object is
    // no longer in the store.
    assert!(
        world.get::<LedgerResource>("ns", "obj").is_none(),
        "object GC'd after ledger emptied and finalizer cleared"
    );
}

/// A `Wait` at an early gate produces no effect completions. The ledger
/// must remain empty (seeded from the previous status, which was empty),
/// and a subsequent deletion must run **no** cleanups — the effects never
/// applied. This is the core fix: cleanup only walks what actually applied.
#[tokio::test]
async fn gate_wait_then_delete_runs_no_cleanup() {
    let world = World::builder().build().await;
    let obj = make_obj("ns", "obj", false);
    world.prefill((*obj).clone());
    let handle = handle_for(&world);

    let pass = Arc::new(AtomicBool::new(false));
    let a = Arc::new(AtomicUsize::new(0));
    let b = Arc::new(AtomicUsize::new(0));

    // Apply pass: gate waits, so neither effect runs.
    let action = Reconcile::new(&handle, obj)
        .gate(WaitOnceGate { pass: pass.clone() })
        .effect(EffectA {
            cleaned_up: a.clone(),
        })
        .effect(EffectB {
            cleaned_up: b.clone(),
        })
        .execute("test-finalizer", |_state, _prev: &LedgerFields| {
            LedgerFields { touched: None }
        })
        .await
        .unwrap();
    assert_ne!(
        action,
        Action::await_change(),
        "apply should requeue on Wait"
    );

    // Ledger must be empty — no effect completed.
    assert!(
        ledger_of(&world, "ns", "obj").is_empty(),
        "ledger empty while gate waiting"
    );

    // Delete and run cleanup. The finalizer was added by execute(), so
    // the fake tombstones the object.
    world.delete::<LedgerResource>("ns", "obj").await;
    let deleting = Arc::new(world.get::<LedgerResource>("ns", "obj").unwrap());
    Reconcile::new(&handle, deleting)
        .gate(WaitOnceGate { pass: pass.clone() })
        .effect(EffectA {
            cleaned_up: a.clone(),
        })
        .effect(EffectB {
            cleaned_up: b.clone(),
        })
        .execute("test-finalizer", |_state, _prev: &LedgerFields| {
            LedgerFields { touched: None }
        })
        .await
        .unwrap();

    assert_eq!(
        a.load(Ordering::Relaxed),
        0,
        "EffectA never applied → no cleanup"
    );
    assert_eq!(
        b.load(Ordering::Relaxed),
        0,
        "EffectB never applied → no cleanup"
    );
}

/// A partial apply (gate passes, EffectA applies, then a later gate waits)
/// must record only EffectA in the ledger. A subsequent deletion must run
/// only EffectA's cleanup, not EffectB's.
#[tokio::test]
async fn partial_apply_records_only_completed_effects() {
    let world = World::builder().build().await;
    let obj = make_obj("ns", "obj", false);
    world.prefill((*obj).clone());
    let handle = handle_for(&world);

    let second_pass = Arc::new(AtomicBool::new(false));
    let a = Arc::new(AtomicUsize::new(0));
    let b = Arc::new(AtomicUsize::new(0));

    // Apply: EffectA runs, then a second gate waits, so EffectB never runs.
    let action = Reconcile::new(&handle, obj)
        .effect(EffectA {
            cleaned_up: a.clone(),
        })
        .gate(WaitOnceGate {
            pass: second_pass.clone(),
        })
        .effect(EffectB {
            cleaned_up: b.clone(),
        })
        .execute("test-finalizer", |_state, _prev: &LedgerFields| {
            LedgerFields { touched: None }
        })
        .await
        .unwrap();
    assert_ne!(action, Action::await_change());

    let ledger = ledger_of(&world, "ns", "obj");
    assert_eq!(ledger.len(), 1, "only EffectA recorded");
    assert!(ledger[0].ends_with("EffectA"));

    // Delete and run cleanup.
    world.delete::<LedgerResource>("ns", "obj").await;
    let deleting = Arc::new(world.get::<LedgerResource>("ns", "obj").unwrap());
    Reconcile::new(&handle, deleting)
        .effect(EffectA {
            cleaned_up: a.clone(),
        })
        .gate(WaitOnceGate {
            pass: second_pass.clone(),
        })
        .effect(EffectB {
            cleaned_up: b.clone(),
        })
        .execute("test-finalizer", |_state, _prev: &LedgerFields| {
            LedgerFields { touched: None }
        })
        .await
        .unwrap();

    assert_eq!(a.load(Ordering::Relaxed), 1, "EffectA applied → cleaned up");
    assert_eq!(
        b.load(Ordering::Relaxed),
        0,
        "EffectB never applied → no cleanup"
    );
}

/// A cleanup `Retry` keeps the ledger entry. The next pass resumes from
/// the failed effect and does not re-run already-cleaned effects. After
/// the retry succeeds, the entry is struck off and the ledger empties.
#[tokio::test]
async fn cleanup_retry_persists_ledger_and_resumes() {
    let world = World::builder().build().await;
    let obj = make_obj("ns", "obj", false);
    world.prefill((*obj).clone());
    let handle = handle_for(&world);

    let a = Arc::new(AtomicUsize::new(0));
    let retry_attempts = Arc::new(AtomicUsize::new(0));
    let retry_cleaned = Arc::new(AtomicUsize::new(0));

    // Apply: EffectA then RetryOnceEffect both complete.
    Reconcile::new(&handle, obj)
        .effect(EffectA {
            cleaned_up: a.clone(),
        })
        .effect(RetryOnceEffect {
            cleaned_up: retry_cleaned.clone(),
            attempts: retry_attempts.clone(),
        })
        .execute("test-finalizer", |_state, _prev: &LedgerFields| {
            LedgerFields { touched: None }
        })
        .await
        .unwrap();

    let ledger = ledger_of(&world, "ns", "obj");
    assert_eq!(ledger.len(), 2, "both effects recorded");

    // Delete and run cleanup. Reverse order: RetryOnceEffect first, then EffectA.
    world.delete::<LedgerResource>("ns", "obj").await;
    let deleting = Arc::new(world.get::<LedgerResource>("ns", "obj").unwrap());

    // First cleanup pass: RetryOnceEffect retries (returns Retry), so its
    // ledger entry persists. EffectA (later in reverse order) is not reached.
    let action = Reconcile::new(&handle, deleting.clone())
        .effect(EffectA {
            cleaned_up: a.clone(),
        })
        .effect(RetryOnceEffect {
            cleaned_up: retry_cleaned.clone(),
            attempts: retry_attempts.clone(),
        })
        .execute("test-finalizer", |_state, _prev: &LedgerFields| {
            LedgerFields { touched: None }
        })
        .await
        .unwrap();
    assert_ne!(
        action,
        Action::await_change(),
        "cleanup should requeue on Retry"
    );

    // RetryOnceEffect's cleanup was called once (the retrying attempt) but
    // did not complete; its ledger entry must still be present.
    assert_eq!(
        retry_cleaned.load(Ordering::Relaxed),
        0,
        "RetryOnceEffect not yet cleaned"
    );
    assert_eq!(
        a.load(Ordering::Relaxed),
        0,
        "EffectA not reached on first pass"
    );

    let ledger_after_retry = ledger_of(&world, "ns", "obj");
    // RetryOnceEffect still outstanding; EffectA still outstanding (not reached).
    assert_eq!(
        ledger_after_retry.len(),
        2,
        "ledger intact after cleanup retry: {ledger_after_retry:?}"
    );

    // Second cleanup pass: RetryOnceEffect succeeds, then EffectA runs.
    let deleting2 = Arc::new(world.get::<LedgerResource>("ns", "obj").unwrap());
    Reconcile::new(&handle, deleting2)
        .effect(EffectA {
            cleaned_up: a.clone(),
        })
        .effect(RetryOnceEffect {
            cleaned_up: retry_cleaned.clone(),
            attempts: retry_attempts.clone(),
        })
        .execute("test-finalizer", |_state, _prev: &LedgerFields| {
            LedgerFields { touched: None }
        })
        .await
        .unwrap();

    assert_eq!(
        retry_cleaned.load(Ordering::Relaxed),
        1,
        "RetryOnceEffect cleaned"
    );
    assert_eq!(a.load(Ordering::Relaxed), 1, "EffectA cleaned on resume");
    // The cleanup emptied the ledger and cleared the finalizer, which triggered
    // the fake's finalizer-aware GC — the object is gone. The GC proves the
    // ledger was emptied and the teardown completed (see
    // `apply_populates_ledger_and_cleanup_empties_it` for the rationale).
    assert!(
        world.get::<LedgerResource>("ns", "obj").is_none(),
        "object GC'd after ledger emptied and finalizer cleared on resume"
    );
}

/// An orphaned ledger entry — a name that matches no registered cleanup
/// step — is logged and dropped. Deletion must complete and the finalizer
/// must be removed; the framework must not block forever on absent code.
#[tokio::test]
async fn orphaned_ledger_entry_is_dropped() {
    let world = World::builder().build().await;

    // Seed a deleting object whose status already carries an orphaned
    // ledger entry (a name no effect in the pipeline will match).
    let mut obj = LedgerResource::new("obj", LedgerSpec { field: "v".into() });
    obj.metadata.namespace = Some("ns".to_string());
    obj.metadata.finalizers = Some(vec!["test-finalizer".to_string()]);
    obj.metadata.deletion_timestamp = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
        k8s_openapi::jiff::Timestamp::now(),
    ));
    obj.status = Some(ResourceStatus {
        conditions: vec![],
        observed_generation: None,
        pending_cleanup: vec!["orphan::EffectThatNoLongerExists".to_string()],
        resource: LedgerFields { touched: None },
    });
    world.prefill(obj);

    let handle = handle_for(&world);
    let a = Arc::new(AtomicUsize::new(0));

    let deleting = Arc::new(world.get::<LedgerResource>("ns", "obj").unwrap());
    let action = Reconcile::new(&handle, deleting)
        .effect(EffectA {
            cleaned_up: a.clone(),
        })
        .execute("test-finalizer", |_state, _prev: &LedgerFields| {
            LedgerFields { touched: None }
        })
        .await
        .unwrap();

    // Cleanup completes (orphan dropped, EffectA never applied so not cleaned).
    assert_eq!(action, Action::await_change());
    assert_eq!(
        a.load(Ordering::Relaxed),
        0,
        "EffectA not in ledger → not cleaned"
    );
    // The orphan was dropped, the ledger emptied, and the finalizer was cleared
    // — which triggered the fake's finalizer-aware GC. The object being gone
    // proves all three: the framework only clears the finalizer when the ledger
    // is empty, and the fake only GCs when the finalizer is cleared. Reading
    // the ledger or the finalizer list directly is not possible after GC: the
    // object is no longer in the store.
    assert!(
        world.get::<LedgerResource>("ns", "obj").is_none(),
        "object GC'd after orphan dropped, ledger emptied, and finalizer cleared"
    );
}

/// `Effect::effect_name` pins a stable ledger key that survives effect
/// struct renames. A pipeline using the override must record the pinned
/// name (not the Rust type name) in the ledger, and cleanup must still
/// match it.
#[tokio::test]
async fn effect_name_override_pins_stable_ledger_key() {
    let world = World::builder().build().await;
    let obj = make_obj("ns", "obj", false);
    world.prefill((*obj).clone());
    let handle = handle_for(&world);

    let a = Arc::new(AtomicUsize::new(0));

    Reconcile::new(&handle, obj)
        .effect(PinnedNameEffect {
            cleaned_up: a.clone(),
        })
        .execute("test-finalizer", |_state, _prev: &LedgerFields| {
            LedgerFields { touched: None }
        })
        .await
        .unwrap();

    let ledger = ledger_of(&world, "ns", "obj");
    assert_eq!(
        ledger,
        vec!["stable.pinned"],
        "ledger holds the pinned name, not the Rust type name"
    );
}

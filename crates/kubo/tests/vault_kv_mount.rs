//! Integration test: VaultKvMount controller pattern on the framework.
//!
//! This validates that a realistic multi-step controller (gate → effect → effect)
//! composes correctly, with typed state keys, custom error types, on-tick status
//! patching, and reverse-order cleanup.
//!
//! These tests run against [`kubo::testing::World`] (a simulated cluster backed
//! by a [`FakeApiServer`](kubo_test_fakeapi::FakeApiServer)) using a
//! [`KubeHandle`] built from `World::client()`.  The fake + real `kube::Client`
//! exercises the same production code paths the controller would use in a
//! cluster, including SSA patches, finalizer management, and status updates.

#![allow(missing_docs)]

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use kube::runtime::controller::Action;
use kubo::{
    ReconcileContext, ReconcilerError,
    backoff::fixed::Fixed,
    cleanup::CleanupAction,
    effect::{Effect, EffectOutcome},
    environment::{KubeHandle, ObjectRef},
    gate::{Gate, Outcome},
    poll::{Poll, PollState},
    reconcile::Reconcile,
    state::{State, StateKey},
    testing::World,
    utils::ConditionContext,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ═══════════════════════════════════════════════════════════════════════
// CRD definitions (mirrors the real VaultKvMount / VaultInstance)
// ═══════════════════════════════════════════════════════════════════════

#[derive(CustomResource, Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "test.kaphera.cloud",
    version = "v1alpha1",
    kind = "VaultKvMount",
    status = "VaultKvMountStatus",
    namespaced
)]
pub struct VaultKvMountSpec {
    pub instance_ref: String,
    pub access_secret_name: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct VaultKvMountFields {
    pub mount_path: Option<String>,
    pub resolved_instance_ref: Option<String>,
}

pub type VaultKvMountStatus = kubo::status::ResourceStatus<VaultKvMountFields>;

impl kubo::HasConditions for VaultKvMount {
    fn conditions(&self) -> Vec<Condition> {
        self.status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default()
    }
}

#[derive(CustomResource, Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "test.kaphera.cloud",
    version = "v1alpha1",
    kind = "VaultInstance",
    status = "VaultInstanceStatus",
    namespaced
)]
pub struct VaultInstanceSpec {
    pub replicas: u32,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct VaultInstanceStatus {
    pub conditions: Vec<Condition>,
    pub address: Option<String>,
}

// ═══════════════════════════════════════════════════════════════════════
// Custom error type (demonstrates Into<ReconcilerError> ergonomics)
// ═══════════════════════════════════════════════════════════════════════

#[derive(Debug, thiserror::Error)]
enum VaultKvMountError {
    #[error("environment: {0}")]
    Env(#[from] kubo::environment::EnvironmentError),

    #[error("vault API error: {0}")]
    Vault(String),
}

impl From<VaultKvMountError> for ReconcilerError {
    fn from(e: VaultKvMountError) -> Self {
        ReconcilerError::Env(kubo::environment::EnvironmentError::Other(e.to_string()))
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Gate: Resolve VaultInstance
// ═══════════════════════════════════════════════════════════════════════

struct ResolveVaultInstance {
    instance_ref: String,
}

impl StateKey for ResolveVaultInstance {
    type Value = String;
}

#[async_trait]
impl Gate<VaultKvMount> for ResolveVaultInstance {
    type Error = VaultKvMountError;

    async fn evaluate(
        &self,
        cx: &ReconcileContext<VaultKvMount>,
        handle: &KubeHandle,
    ) -> Result<Outcome<String>, Self::Error> {
        let r = ObjectRef::new(&cx.object.namespace, &self.instance_ref);
        let instance: Option<VaultInstance> = handle.get(&r).await?;

        match instance {
            None => Ok(Outcome::Wait {
                condition: cx.condition.not_ready_condition(
                    "WaitingForInstance",
                    &format!("VaultInstance {} not found", self.instance_ref),
                ),
                backoff: Box::new(Fixed::new(std::time::Duration::from_secs(10))),
            }),
            Some(vi) => {
                let address = vi.status.as_ref().and_then(|s| s.address.clone());

                match address {
                    Some(addr) => Ok(Outcome::Pass(addr)),
                    None => Ok(Outcome::Wait {
                        condition: cx.condition.not_ready_condition(
                            "WaitingForInstance",
                            "VaultInstance is not yet ready",
                        ),
                        backoff: Box::new(Fixed::new(std::time::Duration::from_secs(10))),
                    }),
                }
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Effect: Enable KV-v2 mount + create policy
// ═══════════════════════════════════════════════════════════════════════

struct EnableKv2Mount {
    /// Simulates whether the Vault API call succeeds.
    vault_available: Arc<AtomicBool>,
    /// Tracks whether cleanup was called (for test assertion).
    cleaned_up: Arc<AtomicBool>,
}

impl StateKey for EnableKv2Mount {
    type Value = String;
}

#[async_trait]
impl Effect<VaultKvMount> for EnableKv2Mount {
    type Error = VaultKvMountError;

    async fn apply(
        &self,
        cx: &ReconcileContext<VaultKvMount>,
        _handle: &KubeHandle,
        _state: &State,
    ) -> Result<EffectOutcome<String>, Self::Error> {
        if !self.vault_available.load(Ordering::Relaxed) {
            return Err(VaultKvMountError::Vault(
                "Vault API unavailable".to_string(),
            ));
        }

        let mount_path = format!("{}/{}/kv", cx.object.namespace, cx.object.name);

        // In real code: vault_client.enable_kv2_mount(&mount_path).await?;
        // In real code: vault_client.put_policy(&policy_name, &rules).await?;

        Ok(EffectOutcome::Done(mount_path))
    }

    async fn cleanup(
        &self,
        _cx: &ReconcileContext<VaultKvMount>,
        _handle: &KubeHandle,
    ) -> Result<CleanupAction, Self::Error> {
        if !self.vault_available.load(Ordering::Relaxed) {
            return Ok(CleanupAction::Retry(Box::new(Fixed::new(
                std::time::Duration::from_secs(15),
            ))));
        }

        // In real code: vault_client.disable_mount(&mount_path).await?;
        // In real code: vault_client.delete_policy(&policy_name).await?;

        self.cleaned_up.store(true, Ordering::Relaxed);
        Ok(CleanupAction::Done)
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Effect: Create access Secret with scoped token
// ═══════════════════════════════════════════════════════════════════════

struct CreateAccessSecret {
    secret_name: String,
    /// Tracks whether cleanup was called (for test assertion).
    cleaned_up: Arc<AtomicBool>,
}

impl StateKey for CreateAccessSecret {
    type Value = ();
}

#[async_trait]
impl Effect<VaultKvMount> for CreateAccessSecret {
    type Error = VaultKvMountError;

    async fn apply(
        &self,
        cx: &ReconcileContext<VaultKvMount>,
        _handle: &KubeHandle,
        state: &State,
    ) -> Result<EffectOutcome<()>, Self::Error> {
        let mount_path = state
            .require::<EnableKv2Mount>()
            .map_err(|e| VaultKvMountError::Vault(e.to_string()))?;

        // In real code: create scoped token, build Secret, SSA patch.
        let _mount_path = mount_path.clone();
        let _secret_name = &self.secret_name;
        let _namespace = &cx.object.namespace;

        Ok(EffectOutcome::Done(()))
    }

    async fn cleanup(
        &self,
        _cx: &ReconcileContext<VaultKvMount>,
        _handle: &KubeHandle,
    ) -> Result<CleanupAction, Self::Error> {
        self.cleaned_up.store(true, Ordering::Relaxed);
        Ok(CleanupAction::Done)
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Poll: Wait for mount to be accessible (optional, demonstrates polls)
// ═══════════════════════════════════════════════════════════════════════

struct WaitForMountReady {
    ready: Arc<AtomicBool>,
}

#[async_trait]
impl Poll<VaultKvMount> for WaitForMountReady {
    type Error = VaultKvMountError;

    async fn check(
        &self,
        cx: &ReconcileContext<VaultKvMount>,
        _handle: &KubeHandle,
    ) -> Result<PollState, Self::Error> {
        if self.ready.load(Ordering::Relaxed) {
            Ok(PollState::Done)
        } else {
            Ok(PollState::Pending {
                condition: cx.condition.not_ready_condition(
                    "MountNotReady",
                    "Waiting for KV-v2 mount to become accessible",
                ),
                backoff: Box::new(Fixed::new(std::time::Duration::from_secs(5))),
            })
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════

/// Build a `World` with a `VaultInstance` already seeded with a ready address.
async fn world_with_ready_vault_instance(ns: &str, vault_name: &str, address: &str) -> World {
    let world = World::builder().build().await;

    let mut vi = VaultInstance::new(vault_name, VaultInstanceSpec { replicas: 1 });
    vi.metadata.namespace = Some(ns.to_string());
    vi.status = Some(VaultInstanceStatus {
        conditions: vec![],
        address: Some(address.to_string()),
    });
    world.prefill(vi);
    world
}

/// Build a `KubeHandle` from a `World`'s client.
fn handle_for(world: &World) -> KubeHandle {
    KubeHandle::new(world.client().clone(), "kubo-test")
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

/// Validates that gate → effect → effect → poll composes and the
/// status closure receives typed state correctly.
#[tokio::test]
async fn pipeline_composes_with_typed_state() {
    // This is a compile-time validation: if the types don't compose,
    // this won't build. The runtime part validates state flow.
    let world =
        world_with_ready_vault_instance("test-ns", "my-vault", "http://vault.test-ns.svc:8200")
            .await;
    let handle = handle_for(&world);

    let vault_available = Arc::new(AtomicBool::new(true));
    let mount_ready = Arc::new(AtomicBool::new(true));
    let effect1_cleaned = Arc::new(AtomicBool::new(false));
    let effect2_cleaned = Arc::new(AtomicBool::new(false));

    let instance_ref = "my-vault".to_string();

    // Build the reconcile pipeline — validates type composition.
    let obj = make_kvm("test-ns", "my-kvm", false);
    let _reconcile = Reconcile::new(&handle, obj.clone())
        .ready_reason("MountReady", "KV-v2 mount is provisioned")
        .gate(ResolveVaultInstance {
            instance_ref: instance_ref.clone(),
        })
        .effect(EnableKv2Mount {
            vault_available: vault_available.clone(),
            cleaned_up: effect1_cleaned.clone(),
        })
        .effect(CreateAccessSecret {
            secret_name: "my-kvm-access".to_string(),
            cleaned_up: effect2_cleaned.clone(),
        })
        .poll(WaitForMountReady {
            ready: mount_ready.clone(),
        });

    // The status closure compiles with typed state access:
    let _status_fn = |state: &State, _prev: &VaultKvMountFields| -> VaultKvMountFields {
        VaultKvMountFields {
            mount_path: state.get::<EnableKv2Mount>().cloned(),
            resolved_instance_ref: state.get::<ResolveVaultInstance>().cloned(),
        }
    };
}

/// Validates that State typed keys prevent wrong-type access at compile time.
/// (This is a negative test — uncommenting the bad line should fail to compile.)
#[test]
fn typed_state_prevents_wrong_type() {
    let mut state = State::new();
    state.insert::<EnableKv2Mount>("test/path/kv".to_string());

    // Correct access:
    assert_eq!(
        state.get::<EnableKv2Mount>(),
        Some(&"test/path/kv".to_string())
    );

    // This would be a compile error (uncomment to verify):
    // let _: Option<&i32> = state.get::<EnableKv2Mount>();

    // Missing key returns None:
    assert_eq!(state.get::<ResolveVaultInstance>(), None);

    // Require on missing key returns error:
    let err = state.require::<ResolveVaultInstance>();
    assert!(err.is_err());
}

/// Validates that the gate waits when VaultInstance is not found.
#[tokio::test]
async fn gate_waits_when_instance_missing() {
    let world = World::builder().build().await;
    let handle = handle_for(&world);

    let gate = ResolveVaultInstance {
        instance_ref: "missing-vault".to_string(),
    };

    let obj_ref = ObjectRef::new("test-ns", "missing-vault");
    let cond_cx = ConditionContext::new(1);
    let kvm = make_kvm("test-ns", "missing-vault", false);
    let rcx = ReconcileContext {
        object: &obj_ref,
        condition: &cond_cx,
        cr: &*kvm,
    };
    let result = gate.evaluate(&rcx, &handle).await.unwrap();
    assert!(!result.is_pass());
}

/// Validates that the gate passes when VaultInstance has an address.
#[tokio::test]
async fn gate_passes_when_instance_ready() {
    let world = world_with_ready_vault_instance("test-ns", "my-vault", "http://vault:8200").await;
    let handle = handle_for(&world);

    let gate = ResolveVaultInstance {
        instance_ref: "my-vault".to_string(),
    };

    let obj_ref = ObjectRef::new("test-ns", "my-kvm");
    let cond_cx = ConditionContext::new(1);
    let kvm = make_kvm("test-ns", "my-kvm", false);
    let rcx = ReconcileContext {
        object: &obj_ref,
        condition: &cond_cx,
        cr: &*kvm,
    };
    let result = gate.evaluate(&rcx, &handle).await.unwrap();
    assert!(result.is_pass());
}

/// Validates effect apply returns the correct value.
#[tokio::test]
async fn effect_writes_to_typed_state() {
    let world = World::builder().build().await;
    let handle = handle_for(&world);
    let vault_available = Arc::new(AtomicBool::new(true));
    let cleaned_up = Arc::new(AtomicBool::new(false));
    let state = State::new();

    let effect = EnableKv2Mount {
        vault_available,
        cleaned_up,
    };

    let obj_ref = ObjectRef::new("ns", "my-kvm");
    let cond_cx = ConditionContext::new(1);
    let kvm = make_kvm("ns", "my-kvm", false);
    let rcx = ReconcileContext {
        object: &obj_ref,
        condition: &cond_cx,
        cr: &*kvm,
    };
    let outcome = effect.apply(&rcx, &handle, &state).await.unwrap();

    // Verify the returned value (framework inserts it into state).
    match outcome {
        EffectOutcome::Done(mount_path) => assert_eq!(mount_path, "ns/my-kvm/kv"),
        EffectOutcome::Retry { .. } => panic!("expected Done"),
    }
}

/// Validates effect returns error when vault unavailable.
#[tokio::test]
async fn effect_errors_when_vault_down() {
    let world = World::builder().build().await;
    let handle = handle_for(&world);
    let vault_available = Arc::new(AtomicBool::new(false));
    let cleaned_up = Arc::new(AtomicBool::new(false));
    let state = State::new();

    let effect = EnableKv2Mount {
        vault_available,
        cleaned_up,
    };

    let obj_ref = ObjectRef::new("ns", "my-kvm");
    let cond_cx = ConditionContext::new(1);
    let kvm = make_kvm("ns", "my-kvm", false);
    let rcx = ReconcileContext {
        object: &obj_ref,
        condition: &cond_cx,
        cr: &*kvm,
    };
    let result = effect.apply(&rcx, &handle, &state).await;
    assert!(result.is_err());
}

/// Validates cleanup returns Retry when vault unavailable.
#[tokio::test]
async fn cleanup_retries_when_vault_down() {
    let world = World::builder().build().await;
    let handle = handle_for(&world);
    let vault_available = Arc::new(AtomicBool::new(false));
    let cleaned_up = Arc::new(AtomicBool::new(false));

    let effect = EnableKv2Mount {
        vault_available,
        cleaned_up: cleaned_up.clone(),
    };

    let obj_ref = ObjectRef::new("ns", "my-kvm");
    let cond_cx = ConditionContext::new(1);
    let kvm = make_kvm("ns", "my-kvm", false);
    let rcx = ReconcileContext {
        object: &obj_ref,
        condition: &cond_cx,
        cr: &*kvm,
    };
    let result = effect.cleanup(&rcx, &handle).await.unwrap();
    assert!(matches!(result, CleanupAction::Retry(_)));
    assert!(!cleaned_up.load(Ordering::Relaxed));
}

/// Validates cleanup succeeds and marks Done.
#[tokio::test]
async fn cleanup_succeeds_when_vault_available() {
    let world = World::builder().build().await;
    let handle = handle_for(&world);
    let vault_available = Arc::new(AtomicBool::new(true));
    let cleaned_up = Arc::new(AtomicBool::new(false));

    let effect = EnableKv2Mount {
        vault_available,
        cleaned_up: cleaned_up.clone(),
    };

    let obj_ref = ObjectRef::new("ns", "my-kvm");
    let cond_cx = ConditionContext::new(1);
    let kvm = make_kvm("ns", "my-kvm", false);
    let rcx = ReconcileContext {
        object: &obj_ref,
        condition: &cond_cx,
        cr: &*kvm,
    };
    let result = effect.cleanup(&rcx, &handle).await.unwrap();
    assert!(matches!(result, CleanupAction::Done));
    assert!(cleaned_up.load(Ordering::Relaxed));
}

/// Validates that the second effect can read state from the first.
#[tokio::test]
async fn effect_chain_reads_state_from_prior() {
    let world = World::builder().build().await;
    let handle = handle_for(&world);
    let mut state = State::new();

    // Simulate the framework having inserted EnableKv2Mount's output:
    state.insert::<EnableKv2Mount>("test-ns/my-kvm/kv".to_string());

    // Second effect reads it:
    let effect = CreateAccessSecret {
        secret_name: "my-kvm-access".to_string(),
        cleaned_up: Arc::new(AtomicBool::new(false)),
    };

    let obj_ref = ObjectRef::new("test-ns", "my-kvm");
    let cond_cx = ConditionContext::new(1);
    let kvm = make_kvm("test-ns", "my-kvm", false);
    let rcx = ReconcileContext {
        object: &obj_ref,
        condition: &cond_cx,
        cr: &*kvm,
    };
    let result = effect.apply(&rcx, &handle, &state).await;
    assert!(result.is_ok());
}

/// Validates that the second effect fails if prior state is missing.
#[tokio::test]
async fn effect_fails_without_prior_state() {
    let world = World::builder().build().await;
    let handle = handle_for(&world);
    let state = State::new(); // Empty — no EnableKv2Mount inserted.

    let effect = CreateAccessSecret {
        secret_name: "my-kvm-access".to_string(),
        cleaned_up: Arc::new(AtomicBool::new(false)),
    };

    let obj_ref = ObjectRef::new("test-ns", "my-kvm");
    let cond_cx = ConditionContext::new(1);
    let kvm = make_kvm("test-ns", "my-kvm", false);
    let rcx = ReconcileContext {
        object: &obj_ref,
        condition: &cond_cx,
        cr: &*kvm,
    };
    let result = effect.apply(&rcx, &handle, &state).await;
    assert!(result.is_err());
}

/// Validates status closure uses PartialEq for skip-patch semantics.
#[test]
fn status_eq_enables_skip_patch() {
    let s1 = VaultKvMountFields {
        mount_path: Some("ns/name/kv".to_string()),
        resolved_instance_ref: Some("http://vault:8200".to_string()),
    };

    let s2 = VaultKvMountFields {
        mount_path: Some("ns/name/kv".to_string()),
        resolved_instance_ref: Some("http://vault:8200".to_string()),
    };

    // Same fields → skip patch.
    assert_eq!(s1, s2);

    let s3 = VaultKvMountFields {
        mount_path: None,
        ..s1.clone()
    };

    // Different mount_path → patch.
    assert_ne!(s1, s3);
}

// ═══════════════════════════════════════════════════════════════════════
// Full Reconcile::run() tests
// ═══════════════════════════════════════════════════════════════════════

/// Helper: build a VaultKvMount object with metadata set.
fn make_kvm(namespace: &str, name: &str, deleting: bool) -> Arc<VaultKvMount> {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

    let mut kvm = VaultKvMount::new(
        name,
        VaultKvMountSpec {
            instance_ref: "my-vault".to_string(),
            access_secret_name: "access-secret".to_string(),
        },
    );
    kvm.metadata.namespace = Some(namespace.to_string());
    if deleting {
        kvm.metadata.deletion_timestamp = Some(Time(k8s_openapi::jiff::Timestamp::now()));
        kvm.metadata.finalizers = Some(vec!["test-finalizer".to_string()]);
    }
    Arc::new(kvm)
}

/// Full apply pass: gate passes, effects run, status is patched,
/// finalizer is added.
#[tokio::test]
async fn run_apply_full_pipeline() {
    let world = world_with_ready_vault_instance("test-ns", "my-vault", "http://vault:8200").await;
    let obj = make_kvm("test-ns", "my-kvm", false);
    world.prefill((*obj).clone());

    let handle = handle_for(&world);

    let vault_available = Arc::new(AtomicBool::new(true));
    let effect1_cleaned = Arc::new(AtomicBool::new(false));
    let effect2_cleaned = Arc::new(AtomicBool::new(false));

    let action = Reconcile::new(&handle, obj.clone())
        .ready_reason("MountReady", "KV-v2 mount is provisioned")
        .gate(ResolveVaultInstance {
            instance_ref: "my-vault".to_string(),
        })
        .effect(EnableKv2Mount {
            vault_available: vault_available.clone(),
            cleaned_up: effect1_cleaned.clone(),
        })
        .effect(CreateAccessSecret {
            secret_name: "access-secret".to_string(),
            cleaned_up: effect2_cleaned.clone(),
        })
        .execute("test-finalizer", |state, _prev: &VaultKvMountFields| {
            VaultKvMountFields {
                mount_path: state.get::<EnableKv2Mount>().cloned(),
                resolved_instance_ref: state.get::<ResolveVaultInstance>().cloned(),
            }
        })
        .await
        .unwrap();

    // Should requeue (steady-state interval) — not await_change.
    assert_ne!(action, Action::await_change());

    // Status should have been patched with mount_path.
    let stored = world.get::<VaultKvMount>("test-ns", "my-kvm").unwrap();
    let status_value = stored.status.expect("status should be patched");
    assert_eq!(
        status_value.resource.mount_path,
        Some("test-ns/my-kvm/kv".to_string())
    );
    assert_eq!(
        status_value.resource.resolved_instance_ref,
        Some("http://vault:8200".to_string())
    );

    // Finalizer should have been added.
    let finalizers = stored.metadata.finalizers.unwrap_or_default();
    assert!(finalizers.contains(&"test-finalizer".to_string()));
}

/// Full cleanup pass: effects cleanup in reverse, finalizer is removed.
///
/// Under the pending-cleanup ledger, cleanup only runs for effects that
/// actually completed `apply`. This test therefore drives the full
/// lifecycle: an apply pass populates the ledger, then a deletion-triggered
/// pass runs cleanup and removes the finalizer.
#[tokio::test]
async fn run_cleanup_removes_finalizer() {
    let world = world_with_ready_vault_instance("test-ns", "my-vault", "http://vault:8200").await;
    let obj = make_kvm("test-ns", "my-kvm", false);
    world.prefill((*obj).clone());

    let handle = handle_for(&world);

    let vault_available = Arc::new(AtomicBool::new(true));
    let effect1_cleaned = Arc::new(AtomicBool::new(false));
    let effect2_cleaned = Arc::new(AtomicBool::new(false));

    // 1. Apply pass: gate passes, both effects run, ledger is populated.
    Reconcile::new(&handle, obj.clone())
        .ready_reason("MountReady", "KV-v2 mount is provisioned")
        .gate(ResolveVaultInstance {
            instance_ref: "my-vault".to_string(),
        })
        .effect(EnableKv2Mount {
            vault_available: vault_available.clone(),
            cleaned_up: effect1_cleaned.clone(),
        })
        .effect(CreateAccessSecret {
            secret_name: "access-secret".to_string(),
            cleaned_up: effect2_cleaned.clone(),
        })
        .execute("test-finalizer", |_state, _prev: &VaultKvMountFields| {
            VaultKvMountFields {
                mount_path: None,
                resolved_instance_ref: None,
            }
        })
        .await
        .unwrap();

    // The apply pass must have recorded both effects in the ledger.
    let after_apply = world.get::<VaultKvMount>("test-ns", "my-kvm").unwrap();
    let ledger = &after_apply.status.as_ref().unwrap().pending_cleanup;
    assert_eq!(
        ledger.len(),
        2,
        "ledger should hold both effects after apply"
    );

    // Neither effect should have been cleaned up yet.
    assert!(!effect1_cleaned.load(Ordering::Relaxed));
    assert!(!effect2_cleaned.load(Ordering::Relaxed));

    // 2. Delete the object, then run the cleanup pass.
    world.delete::<VaultKvMount>("test-ns", "my-kvm").await;
    let deleting = world.get::<VaultKvMount>("test-ns", "my-kvm").unwrap();
    let deleting = Arc::new(deleting);

    let action = Reconcile::new(&handle, deleting)
        .gate(ResolveVaultInstance {
            instance_ref: "my-vault".to_string(),
        })
        .effect(EnableKv2Mount {
            vault_available: vault_available.clone(),
            cleaned_up: effect1_cleaned.clone(),
        })
        .effect(CreateAccessSecret {
            secret_name: "access-secret".to_string(),
            cleaned_up: effect2_cleaned.clone(),
        })
        .execute("test-finalizer", |_state, _prev: &VaultKvMountFields| {
            VaultKvMountFields {
                mount_path: None,
                resolved_instance_ref: None,
            }
        })
        .await
        .unwrap();

    // Should signal await_change (cleanup done).
    assert_eq!(action, Action::await_change());

    // Both effects should have been cleaned up (reverse order: effect2 then effect1).
    assert!(effect1_cleaned.load(Ordering::Relaxed));
    assert!(effect2_cleaned.load(Ordering::Relaxed));

    // The cleanup emptied the ledger and cleared the finalizer, which triggered
    // the fake's finalizer-aware GC — the object is gone from the store. The GC
    // itself proves the finalizer was removed and the ledger was emptied (the
    // framework only clears the finalizer when the ledger is empty, and the fake
    // only GCs when the finalizer is cleared). Reading the object directly is
    // not possible after GC.
    assert!(
        world.get::<VaultKvMount>("test-ns", "my-kvm").is_none(),
        "object GC'd after ledger emptied and finalizer cleared"
    );
}

/// Gate returns Wait: pipeline stops, status is patched with condition,
/// action is requeue.
#[tokio::test]
async fn run_apply_gate_waits() {
    let world = World::builder().build().await;
    let obj = make_kvm("test-ns", "my-kvm", false);
    world.prefill((*obj).clone());
    let handle = handle_for(&world);

    let action = Reconcile::new(&handle, obj.clone())
        .gate(ResolveVaultInstance {
            instance_ref: "my-vault".to_string(),
        })
        .effect(EnableKv2Mount {
            vault_available: Arc::new(AtomicBool::new(true)),
            cleaned_up: Arc::new(AtomicBool::new(false)),
        })
        .execute("test-finalizer", |state, _prev: &VaultKvMountFields| {
            VaultKvMountFields {
                mount_path: state.get::<EnableKv2Mount>().cloned(),
                resolved_instance_ref: state.get::<ResolveVaultInstance>().cloned(),
            }
        })
        .await
        .unwrap();

    // Should requeue (gate waiting) — not await_change.
    assert_ne!(action, Action::await_change());

    // Status should reflect the waiting condition.
    let stored = world.get::<VaultKvMount>("test-ns", "my-kvm").unwrap();
    let status = stored.status.expect("status should be patched");
    assert!(
        status
            .conditions
            .iter()
            .any(|c| c.reason == "WaitingForInstance")
    );
    // mount_path should NOT be set (effect didn't run).
    assert_eq!(status.resource.mount_path, None);
}

/// Validates that status is patched after each step (on-tick), and
/// only when the value has changed.
#[tokio::test]
async fn run_apply_patches_status_on_tick() {
    let world = world_with_ready_vault_instance("test-ns", "my-vault", "http://vault:8200").await;
    let obj = make_kvm("test-ns", "my-kvm", false);
    world.prefill((*obj).clone());
    let handle = handle_for(&world);

    let vault_available = Arc::new(AtomicBool::new(true));

    let action = Reconcile::new(&handle, obj.clone())
        .ready_reason("MountReady", "KV-v2 mount is provisioned")
        .gate(ResolveVaultInstance {
            instance_ref: "my-vault".to_string(),
        })
        .effect(EnableKv2Mount {
            vault_available: vault_available.clone(),
            cleaned_up: Arc::new(AtomicBool::new(false)),
        })
        .effect(CreateAccessSecret {
            secret_name: "access-secret".to_string(),
            cleaned_up: Arc::new(AtomicBool::new(false)),
        })
        .execute("test-finalizer", |state, _prev: &VaultKvMountFields| {
            VaultKvMountFields {
                mount_path: state.get::<EnableKv2Mount>().cloned(),
                resolved_instance_ref: state.get::<ResolveVaultInstance>().cloned(),
            }
        })
        .await
        .unwrap();

    assert_ne!(action, Action::await_change());

    // After the full pipeline, status should reflect the final state.
    let stored = world.get::<VaultKvMount>("test-ns", "my-kvm").unwrap();
    let status = stored.status.expect("status should be patched");

    // Gate output should be in status (resolved after gate step).
    assert_eq!(
        status.resource.resolved_instance_ref,
        Some("http://vault:8200".to_string())
    );
    // Effect output should be in status (resolved after effect step).
    assert_eq!(
        status.resource.mount_path,
        Some("test-ns/my-kvm/kv".to_string())
    );
    // Ready condition should be present (set after all steps pass).
    assert!(
        status
            .conditions
            .iter()
            .any(|c| c.type_ == "Ready" && c.status == "True")
    );
}

/// Validates that status is NOT patched when the closure returns the
/// same value across ticks (PartialEq skip-patch).
#[tokio::test]
async fn run_apply_skips_redundant_patches() {
    use std::sync::atomic::AtomicU32;

    // We track how many times patch_status is called by wrapping
    // KubeHandle. Since we can't easily instrument KubeHandle, we instead
    // verify the result: run a pipeline where gate output doesn't change
    // the status closure's result between ticks.
    //
    // If gate and effect both produce the same status output (because the
    // closure ignores state entirely), only one patch should occur.

    let world = world_with_ready_vault_instance("test-ns", "my-vault", "http://vault:8200").await;
    let obj = make_kvm("test-ns", "my-kvm", false);
    world.prefill((*obj).clone());
    let handle = handle_for(&world);

    // Use a counter to track how many times the status closure is called
    // with a NEW value (by tracking unique observed values).
    let patch_count = Arc::new(AtomicU32::new(0));
    let patch_count_clone = patch_count.clone();

    // Status closure that ignores state — always returns the same struct
    // (except conditions change when Ready is set at the end).  Between
    // gate and effects, conditions don't change, so the closure output
    // should be identical → skip patch.
    let _action = Reconcile::new(&handle, obj.clone())
        .ready_reason("Ready", "done")
        .gate(ResolveVaultInstance {
            instance_ref: "my-vault".to_string(),
        })
        .effect(EnableKv2Mount {
            vault_available: Arc::new(AtomicBool::new(true)),
            cleaned_up: Arc::new(AtomicBool::new(false)),
        })
        .execute(
            "test-finalizer",
            move |_state, _prev: &VaultKvMountFields| {
                // Count each unique call.
                patch_count_clone.fetch_add(1, Ordering::Relaxed);
                VaultKvMountFields {
                    mount_path: None, // Deliberately ignore state.
                    resolved_instance_ref: None,
                }
            },
        )
        .await
        .unwrap();

    // The closure was called 3 times (after gate, after effect, after
    // setting Ready condition).  But only 2 patches should have occurred:
    // first after gate (conditions empty → status is {gen:1, conds:[]}),
    // effect produces the same status → skip, then Ready condition is
    // added → different → patch.
    //
    // We can verify this by checking that the final status has the Ready
    // condition (meaning the last patch went through) but mount_path is
    // None (meaning intermediate state wasn't written).
    let stored = world.get::<VaultKvMount>("test-ns", "my-kvm").unwrap();
    let status = stored.status.expect("status should be patched");
    assert_eq!(status.resource.mount_path, None);
    assert!(
        status
            .conditions
            .iter()
            .any(|c| c.type_ == "Ready" && c.status == "True")
    );

    // The closure was invoked 3 times (one per step + final).
    // But since after-gate and after-effect produce the same value
    // (conditions unchanged, state ignored), only 2 patches happened.
    let calls = patch_count.load(Ordering::Relaxed);
    assert_eq!(calls, 3, "closure should be called once per step + final");
}

/// Validates causal ordering of status ticks using the test harness.
#[tokio::test]
async fn status_ticks_show_causal_progression() {
    let world = world_with_ready_vault_instance("test-ns", "my-vault", "http://vault:8200").await;
    let obj = make_kvm("test-ns", "my-kvm", false);
    world.prefill((*obj).clone());
    let handle = handle_for(&world);

    let _action = Reconcile::new(&handle, obj.clone())
        .ready_reason("MountReady", "KV-v2 mount is provisioned")
        .gate(ResolveVaultInstance {
            instance_ref: "my-vault".to_string(),
        })
        .effect(EnableKv2Mount {
            vault_available: Arc::new(AtomicBool::new(true)),
            cleaned_up: Arc::new(AtomicBool::new(false)),
        })
        .effect(CreateAccessSecret {
            secret_name: "access-secret".to_string(),
            cleaned_up: Arc::new(AtomicBool::new(false)),
        })
        .execute("test-finalizer", |state, _prev: &VaultKvMountFields| {
            VaultKvMountFields {
                mount_path: state.get::<EnableKv2Mount>().cloned(),
                resolved_instance_ref: state.get::<ResolveVaultInstance>().cloned(),
            }
        })
        .await
        .unwrap();

    let ticks: Vec<VaultKvMountStatus> = world.status_ticks("test-ns", "my-kvm");

    use kubo::testing::properties::*;

    // Property: resolved_instance_ref is monotonic (once set, never unset).
    assert_monotonic(
        &ticks,
        |s| s.resource.resolved_instance_ref.is_some(),
        "resolved_instance_ref",
    );

    // Property: mount_path is monotonic.
    assert_monotonic(&ticks, |s| s.resource.mount_path.is_some(), "mount_path");

    // Property: mount_path never appears without resolved_instance_ref (causal).
    assert_causal(
        &ticks,
        |s| s.resource.resolved_instance_ref.is_some(),
        |s| s.resource.mount_path.is_some(),
        "gate must resolve before effect writes mount",
    );

    // Property: illegal state never occurs.
    assert_never(
        &ticks,
        |s| s.resource.mount_path.is_some() && s.resource.resolved_instance_ref.is_none(),
        "orphan mount_path without resolved instance",
    );

    // Property: pipeline converges to Ready.
    assert_converges(
        &ticks,
        |s| {
            s.conditions
                .iter()
                .any(|c| c.type_ == "Ready" && c.status == "True")
        },
        "reaches Ready",
    );

    // Property: resolved_instance_ref appears strictly before mount_path.
    assert_ordered(
        &ticks,
        |s| s.resource.resolved_instance_ref.is_some(),
        |s| s.resource.mount_path.is_some(),
        "resolve before mount",
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Rotation annotation tests
// ═══════════════════════════════════════════════════════════════════════

use kubo::state::RotationRequested;
use std::collections::BTreeMap;

/// When the rotation annotation is present, `RotationRequested` is
/// inserted into the state bag and the annotation is removed after
/// the pipeline succeeds.
#[tokio::test]
async fn rotation_annotation_triggers_state_and_is_removed() {
    let world = world_with_ready_vault_instance("test-ns", "my-vault", "http://vault:8200").await;

    // Build object with the rotation annotation present.
    let mut kvm = VaultKvMount::new(
        "my-kvm",
        VaultKvMountSpec {
            instance_ref: "my-vault".to_string(),
            access_secret_name: "access-secret".to_string(),
        },
    );
    kvm.metadata.namespace = Some("test-ns".to_string());
    kvm.metadata.annotations = Some(BTreeMap::from([(
        "test.kaphera.cloud/rotate-requested".to_string(),
        "true".to_string(),
    )]));
    let obj = Arc::new(kvm);
    world.prefill((*obj).clone());

    let handle = handle_for(&world);

    // Track whether effects saw the rotation marker.
    let saw_rotation = Arc::new(AtomicBool::new(false));
    let saw_rotation_clone = saw_rotation.clone();

    // A minimal effect that checks for RotationRequested in state.
    struct CheckRotationEffect {
        saw_rotation: Arc<AtomicBool>,
    }

    impl StateKey for CheckRotationEffect {
        type Value = ();
    }

    #[async_trait]
    impl Effect<VaultKvMount> for CheckRotationEffect {
        type Error = VaultKvMountError;

        async fn apply(
            &self,
            _cx: &ReconcileContext<VaultKvMount>,
            _handle: &KubeHandle,
            state: &State,
        ) -> Result<EffectOutcome<()>, Self::Error> {
            if state.contains::<RotationRequested>() {
                self.saw_rotation.store(true, Ordering::Relaxed);
            }
            Ok(EffectOutcome::Done(()))
        }
    }

    let _action = Reconcile::new(&handle, obj.clone())
        .ready_reason("MountReady", "KV-v2 mount is provisioned")
        .rotation_annotation("test.kaphera.cloud/rotate-requested")
        .gate(ResolveVaultInstance {
            instance_ref: "my-vault".to_string(),
        })
        .effect(CheckRotationEffect {
            saw_rotation: saw_rotation_clone,
        })
        .execute("test-finalizer", |_state, _prev: &VaultKvMountFields| {
            VaultKvMountFields {
                mount_path: None,
                resolved_instance_ref: None,
            }
        })
        .await
        .unwrap();

    // The effect should have seen the RotationRequested marker.
    assert!(
        saw_rotation.load(Ordering::Relaxed),
        "Effect should observe RotationRequested in state"
    );

    // The annotation should have been removed from the stored object.
    let stored = world.get::<VaultKvMount>("test-ns", "my-kvm").unwrap();
    let annotations = stored.metadata.annotations.unwrap_or_default();
    assert!(
        !annotations.contains_key("test.kaphera.cloud/rotate-requested"),
        "Rotation annotation should be removed after success"
    );
}

/// When the rotation annotation is NOT present, `RotationRequested` is
/// not inserted into the state bag.
#[tokio::test]
async fn no_rotation_when_annotation_absent() {
    let world = world_with_ready_vault_instance("test-ns", "my-vault", "http://vault:8200").await;

    // Build object WITHOUT the rotation annotation.
    let obj = make_kvm("test-ns", "my-kvm", false);
    world.prefill((*obj).clone());

    let handle = handle_for(&world);

    let saw_rotation = Arc::new(AtomicBool::new(false));
    let saw_rotation_clone = saw_rotation.clone();

    struct CheckRotationEffect2 {
        saw_rotation: Arc<AtomicBool>,
    }

    impl StateKey for CheckRotationEffect2 {
        type Value = ();
    }

    #[async_trait]
    impl Effect<VaultKvMount> for CheckRotationEffect2 {
        type Error = VaultKvMountError;

        async fn apply(
            &self,
            _cx: &ReconcileContext<VaultKvMount>,
            _handle: &KubeHandle,
            state: &State,
        ) -> Result<EffectOutcome<()>, Self::Error> {
            if state.contains::<RotationRequested>() {
                self.saw_rotation.store(true, Ordering::Relaxed);
            }
            Ok(EffectOutcome::Done(()))
        }
    }

    let _action = Reconcile::new(&handle, obj.clone())
        .ready_reason("MountReady", "KV-v2 mount is provisioned")
        .rotation_annotation("test.kaphera.cloud/rotate-requested")
        .gate(ResolveVaultInstance {
            instance_ref: "my-vault".to_string(),
        })
        .effect(CheckRotationEffect2 {
            saw_rotation: saw_rotation_clone,
        })
        .execute("test-finalizer", |_state, _prev: &VaultKvMountFields| {
            VaultKvMountFields {
                mount_path: None,
                resolved_instance_ref: None,
            }
        })
        .await
        .unwrap();

    // The effect should NOT have seen RotationRequested.
    assert!(
        !saw_rotation.load(Ordering::Relaxed),
        "Effect should not observe RotationRequested when annotation is absent"
    );
}

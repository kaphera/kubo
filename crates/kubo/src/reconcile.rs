//! The reconcile-pass machinery: pipeline construction and execution.
//!
//! [`Reconcile`] is the builder and runner for a single reconcile pass. It
//! manages the finalizer, routes to the apply pipeline or the reverse-order
//! cleanup pipeline based on the object's deletion timestamp, patches status
//! after each step, and handles rotation annotations and the steady-state
//! requeue interval. [`ReadyReconcile`] is the terminal builder state with the
//! status closure attached; [`ReconcileError`] wraps pipeline-step and
//! finalizer failures.
//!
//! # Apply pass (no deletion timestamp)
//!
//! 1. Ensures the finalizer is present on the object, patching the finalizer
//!    list if it is absent.
//! 2. Runs each step in declaration order.
//! 3. After each completed step, evaluates the status closure and patches
//!    status only when the wrapped [`ResourceStatus`] differs from the
//!    last-patched value.
//! 4. As each effect completes ([`EffectOutcome::Done`](crate::effect::EffectOutcome::Done)),
//!    records the effect's identity in the `pendingCleanup` ledger — a
//!    framework-owned list of effects that have applied but not yet been torn
//!    down. The ledger is seeded from the previous status at the start of the
//!    pass, so a `Wait` at an early step never truncates it.
//! 5. If a step returns `Wait`, patches the condition onto the status and
//!    requeues with the step's backoff.
//! 6. If all steps pass, sets the reconciled condition (default `Ready=True`,
//!    configurable via [`ready_reason`](Reconcile::ready_reason)) and requeues
//!    at the steady-state interval (default 60 s).
//!
//! # Cleanup pass (deletion timestamp present, finalizer found)
//!
//! 1. Runs effect `cleanup` methods in reverse declaration order, **filtered
//!    by the `pendingCleanup` ledger** — cleanup only runs for effects that
//!    actually completed apply.
//! 2. Strikes each effect off the ledger after its cleanup succeeds and
//!    patches the shrunk ledger back to status, giving resumability: a
//!    `Retry` resumes from the failed effect and skips effects already
//!    cleaned up.
//! 3. If a cleanup returns `Retry`, requeues with backoff and **keeps the
//!    finalizer** so the object survives for the next pass.
//! 4. Removes the finalizer only after the ledger is emptied. Ledger entries
//!    that match no registered cleanup step (e.g. after an effect rename or
//!    removal) are logged and dropped — blocking deletion forever on absent
//!    code is worse than leaking the orphaned resource.
//!
//! The ledger keys on the effect's Rust type name by default; override
//! [`Effect::effect_name`] to pin a stable, release-independent key for
//! effects whose `cleanup` must not leak on rename.

mod steps;

use std::{sync::Arc, time::Duration};

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::runtime::controller::Action;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    HasConditions, ReconcileContext, ReconcilerError,
    backoff::{Backoff, BackoffContext},
    effect::Effect,
    environment::{EnvironmentError, KubeHandle, NamespaceResource, ObjectRef},
    gate::Gate,
    group::Group,
    poll::Poll,
    reconcile::steps::{
        ApplyStep, CleanableStep, CleanupProgress, EffectStep, GateAsStep, GateStep, GuardStep,
        PollStep,
    },
    state::{RotationRequested, State, StateKey},
    status::ResourceStatus,
    utils::{ConditionContext, set_condition},
};

use tracing::warn;

enum StepProgress {
    Continue,
    Wait {
        condition: Condition,
        backoff: Box<dyn Backoff>,
    },
    Error(ReconcilerError),
}

const DEFAULT_STEADY_INTERVAL: Duration = Duration::from_secs(60);

/// A reconciler error wrapper that includes finalizer-level failures.
#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    /// A pipeline step or a status patch failed.
    #[error("reconciler error: {0}")]
    Reconciler(#[from] ReconcilerError),

    /// Adding or removing the finalizer failed.
    #[error("finalizer error: {0}")]
    Finalizer(#[from] EnvironmentError),
}

/// Builder and runner for a complete reconcile pass (apply + cleanup).
///
/// The reconciler owns the full lifecycle: it manages the finalizer
/// string, routes to the apply pipeline or the reverse-order cleanup
/// pipeline based on the object's deletion timestamp, and, during
/// apply, patches status after every step (only when changed).
///
/// All Kubernetes interactions go through [`KubeHandle`], which wraps a
/// `kube::Client`. Swap the client for a `kubo::testing::World`-backed one
/// to run the same pipeline against the fake API server in tests.
///
/// ```rust,ignore (illustrative fragment: placeholder literals and undefined context)
/// use kubo::reconcile::Reconcile;
///
/// Reconcile::new(ctx.handle(), obj.clone())
///     .ready_reason("Provisioned", "All resources ready")
///     .gate(MyGate { .. })
///     .effect(CreateExternalThing { .. })
///     .effect(ApplyK8sResource { .. })
///     .poll(WaitForReady { .. })
///     .status(|state, _prev| MyStatus {
///         some_field: state.get::<MyGate>().cloned(),
///     })
///     .execute(FINALIZER)
///     .await
/// ```
pub struct Reconcile<'r, R: NamespaceResource + HasConditions> {
    handle: &'r KubeHandle,
    cx: ConditionContext,
    conditions: Vec<Condition>,
    obj: Arc<R>,
    apply_steps: Vec<Box<dyn ApplyStep<R> + 'r>>,
    cleanup_steps: Vec<Box<dyn CleanableStep<R> + 'r>>,
    steady_interval: Duration,
    rotation_annotation: Option<&'r str>,
}

impl<'r, R: NamespaceResource + HasConditions> Reconcile<'r, R> {
    /// Start a reconcile pass.
    ///
    /// The framework derives `generation` and initial `conditions` from the
    /// object automatically. Use [`ready_reason`](Self::ready_reason) to
    /// configure the terminal Ready condition.
    pub fn new(handle: &'r KubeHandle, obj: Arc<R>) -> Self {
        let generation = obj.meta().generation.unwrap_or(0);
        let conditions = obj.conditions();
        Self {
            handle,
            cx: ConditionContext::new(generation),
            conditions,
            obj,
            apply_steps: Vec::new(),
            cleanup_steps: Vec::new(),
            steady_interval: DEFAULT_STEADY_INTERVAL,
            rotation_annotation: None,
        }
    }

    /// Set the reason and message for the terminal Ready condition emitted
    /// when all pipeline steps pass.
    pub fn ready_reason(mut self, reason: impl Into<String>, message: impl Into<String>) -> Self {
        self.cx = self.cx.with_ready_reason(reason.into(), message.into());
        self
    }

    /// Add a gate whose output is stored in `State` under its own type.
    ///
    /// The gate struct must implement both [`Gate`] and [`StateKey`].
    /// Downstream effects and the status closure retrieve the value via
    /// `state.get::<G>()`.
    pub fn gate<G>(mut self, gate: G) -> Self
    where
        G: Gate<R> + StateKey + Send + Sync + 'r,
        G::Error: Into<ReconcilerError>,
        <G as StateKey>::Value: Send + Sync,
    {
        self.apply_steps.push(Box::new(GateStep::new(gate)));
        self
    }

    /// Add a gate whose output is stored in `State` under an explicit key `K`.
    ///
    /// Use this when you have multiple instances of the same gate type
    /// in a single pipeline (e.g. resolving two different VaultInstances).
    pub fn gate_as<K, G>(mut self, gate: G) -> Self
    where
        G: Gate<R> + Send + Sync + 'r,
        G::Error: Into<ReconcilerError>,
        <G as StateKey>::Value: Send + Sync,
        K: StateKey<Value = <G as StateKey>::Value> + Send + Sync + 'r,
    {
        self.apply_steps
            .push(Box::new(GateAsStep::<G, K>::new(gate)));
        self
    }

    /// Add a guard — a gate whose output is evaluated but discarded.
    ///
    /// Use this for checks that must pass before the pipeline continues
    /// but whose output is not needed downstream.
    pub fn guard<G>(mut self, gate: G) -> Self
    where
        G: Gate<R> + Send + Sync + 'r,
        G::Error: Into<ReconcilerError>,
        <G as StateKey>::Value: Send + Sync,
    {
        self.apply_steps.push(Box::new(GuardStep::new(gate)));
        self
    }

    /// Add an effect. During apply, effects run after all preceding
    /// gates have passed. During cleanup, effects run in **reverse**
    /// declaration order via [`Effect::cleanup`].
    ///
    /// The effect's pending-cleanup ledger entry is keyed by its Rust type
    /// name (`std::any::type_name::<F>()`) unless the effect overrides
    /// [`Effect::effect_name`] to pin a
    /// stable, release-independent string. The type-name default is
    /// correct by construction for uniqueness within a pipeline, but
    /// **renaming the effect struct across a release orphans the old ledger
    /// entry** — the framework logs and drops it, and the old external
    /// resource may leak. For effects whose `cleanup` deletes external
    /// state that must not leak, override `Effect::effect_name` with a
    /// stable name so the entry survives the rename.
    pub fn effect<F>(mut self, effect: F) -> Self
    where
        F: Effect<R> + StateKey + Send + Sync + 'r,
        F::Error: Into<ReconcilerError>,
        <F as StateKey>::Value: Send + Sync,
    {
        let step = EffectStep::new(effect);
        let shared = Arc::new(step);
        self.apply_steps
            .push(Box::new(SharedApplyStep(Arc::clone(&shared))));
        self.cleanup_steps
            .push(Box::new(SharedCleanableStep(shared)));
        self
    }

    /// Add a poll. Polls observe state without mutating it.
    pub fn poll<P>(mut self, poll: P) -> Self
    where
        P: Poll<R> + Send + Sync + 'r,
        P::Error: Into<ReconcilerError>,
    {
        self.apply_steps.push(Box::new(PollStep::new(poll)));
        self
    }

    /// Add a grouped sub-pipeline that collapses into a single effect.
    ///
    /// The group maintains a scoped state bag — sub-effects within the group
    /// can read each other's outputs, but those intermediates never leak into
    /// the outer pipeline. Only the reducer's output is published under the
    /// group's `StateKey`.
    ///
    /// Cleanup runs in reverse declaration order within the group, at the
    /// position of the group in the outer pipeline's cleanup sequence.
    pub fn group<V>(mut self, group: Group<V, R>) -> Self
    where
        V: Send + Sync + 'static,
    {
        let step = EffectStep::new(group);
        let shared = Arc::new(step);
        self.apply_steps
            .push(Box::new(SharedApplyStep(Arc::clone(&shared))));
        self.cleanup_steps
            .push(Box::new(SharedCleanableStep(shared)));
        self
    }

    /// Declare an annotation key that triggers credential rotation.
    ///
    /// When the reconciled object carries this annotation, the framework
    /// inserts [`RotationRequested`] into
    /// the state bag before running the pipeline. Effects query it via
    /// `state.contains::<RotationRequested>()` to decide whether to
    /// regenerate secrets.
    ///
    /// After a fully-successful apply pass the framework removes the
    /// annotation via merge patch, preventing re-rotation on subsequent
    /// reconciles.
    pub fn rotation_annotation(mut self, key: &'r str) -> Self {
        self.rotation_annotation = Some(key);
        self
    }

    /// Requeue interval after a fully-successful reconcile (default: 60s).
    pub fn steady_state_interval(mut self, d: Duration) -> Self {
        self.steady_interval = d;
        self
    }

    /// Run the pipeline steps and patch status, without managing finalizers.
    ///
    /// This is the core "tick" of the reconcile loop: given the current
    /// object state, run the appropriate pipeline (apply or cleanup based
    /// on deletion timestamp) and patch status after each step.
    ///
    /// Use this when finalizer lifecycle is handled externally, or when
    /// the controller only needs to converge state without owning cleanup
    /// guarantees (e.g. status-only controllers, read-only observers).
    ///
    /// The closure receives the current pipeline [`State`] bag and the
    /// previous resource-specific status `T`. The framework wraps `T` in
    /// [`ResourceStatus<T>`](crate::status::ResourceStatus) automatically.
    pub async fn converge<T, F>(&mut self, build_status: F) -> Result<Action, ReconcileError>
    where
        T: Serialize + DeserializeOwned + Default + PartialEq + Clone,
        F: Fn(&State, &T) -> T,
    {
        let namespace = self.obj.meta().namespace.clone().unwrap_or_default();
        let name = self.obj.meta().name.clone().unwrap_or_default();
        let obj_ref = ObjectRef::new(&namespace, &name);

        let prev_wrapped: ResourceStatus<T> = serde_json::to_value(&*self.obj)
            .ok()
            .and_then(|v| v.get("status").cloned())
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();
        let prev_resource = prev_wrapped.resource;

        let is_deleting = self.obj.meta().deletion_timestamp.is_some();

        let has_rotation = self.rotation_annotation.and_then(|key| {
            self.obj
                .meta()
                .annotations
                .as_ref()
                .filter(|a| a.contains_key(key))
                .map(|_| key)
        });

        if is_deleting {
            self.run_cleanup::<T, F>(
                &obj_ref,
                &prev_resource,
                &prev_wrapped.pending_cleanup,
                &build_status,
            )
            .await
        } else {
            self.run_apply::<T, F>(
                &obj_ref,
                &prev_resource,
                &prev_wrapped.pending_cleanup,
                &build_status,
                has_rotation,
            )
            .await
        }
    }

    /// Full reconcile lifecycle: finalizer management + pipeline execution.
    ///
    /// Ensures the finalizer is present before applying, runs cleanup on
    /// deletion, and removes the finalizer once cleanup succeeds. This is
    /// the standard entry point for controllers that own external resources
    /// requiring cleanup guarantees.
    ///
    /// The closure receives the current pipeline [`State`] bag and the
    /// previous resource-specific status `T`. The framework wraps `T` in
    /// [`ResourceStatus<T>`](crate::status::ResourceStatus) automatically,
    /// supplying `conditions` and `observedGeneration`.
    pub async fn execute<T, F>(
        mut self,
        finalizer_name: &str,
        build_status: F,
    ) -> Result<Action, ReconcileError>
    where
        T: Serialize + DeserializeOwned + Default + PartialEq + Clone,
        F: Fn(&State, &T) -> T,
    {
        let is_deleting = self.obj.meta().deletion_timestamp.is_some();
        let existing_finalizers: Vec<String> =
            self.obj.meta().finalizers.clone().unwrap_or_default();

        if is_deleting {
            let has_our_finalizer = existing_finalizers.iter().any(|f| f == finalizer_name);

            if !has_our_finalizer {
                return Ok(Action::await_change());
            }

            let action = self.converge::<T, F>(build_status).await?;

            if action == Action::await_change() {
                let namespace = self.obj.meta().namespace.clone().unwrap_or_default();
                let name = self.obj.meta().name.clone().unwrap_or_default();
                let obj_ref = ObjectRef::new(&namespace, &name);
                let finalizers: Vec<String> = existing_finalizers
                    .into_iter()
                    .filter(|f| f.as_str() != finalizer_name)
                    .collect();
                self.handle
                    .patch_finalizers::<R>(&obj_ref, finalizers)
                    .await?;
            }
            Ok(action)
        } else {
            if !existing_finalizers.iter().any(|f| f == finalizer_name) {
                let namespace = self.obj.meta().namespace.clone().unwrap_or_default();
                let name = self.obj.meta().name.clone().unwrap_or_default();
                let obj_ref = ObjectRef::new(&namespace, &name);
                let mut new_finalizers = existing_finalizers;
                new_finalizers.push(finalizer_name.to_string());
                self.handle
                    .patch_finalizers::<R>(&obj_ref, new_finalizers)
                    .await?;
            }

            self.converge::<T, F>(build_status).await
        }
    }

    /// Backward-compatible alias for [`execute`](Self::execute).
    ///
    /// Prefer `execute` in new code.
    pub async fn run<T, F>(
        self,
        finalizer_name: &str,
        build_status: F,
    ) -> Result<Action, ReconcileError>
    where
        T: Serialize + DeserializeOwned + Default + PartialEq + Clone,
        F: Fn(&State, &T) -> T,
    {
        self.execute(finalizer_name, build_status).await
    }

    /// Attach a status builder to the pipeline, producing a [`ReadyReconcile`]
    /// that can be executed without passing the closure separately.
    ///
    /// This is the terminal builder method — call it last, after all gates,
    /// effects, and polls are registered:
    ///
    /// ```rust,ignore (illustrative fragment: placeholder literals)
    /// Reconcile::new(handle, obj)
    ///     .effect(CreateDatabase { .. })
    ///     .poll(WaitForReady { .. })
    ///     .status(|state, prev| MyStatus {
    ///         database_url: state.get::<CreateDatabase>().cloned(),
    ///     })
    ///     .execute(FINALIZER)
    ///     .await
    /// ```
    pub fn status<T, F>(self, build_status: F) -> ReadyReconcile<'r, R, T>
    where
        T: Serialize + DeserializeOwned + Default + PartialEq + Clone,
        F: Fn(&State, &T) -> T + Send + Sync + 'r,
    {
        ReadyReconcile {
            inner: self,
            build_status: Box::new(build_status),
        }
    }

    /// Walk the apply pipeline: gates → effects → polls, in order.
    ///
    /// After each step, the status closure is evaluated. If the
    /// resulting status differs from the last-patched value, a status
    /// patch is issued via the operations handle. This gives intermediate
    /// observability without redundant API calls on no-op reconciles.
    ///
    /// When `rotation_key` is `Some`, the framework inserts
    /// [`RotationRequested`] into the state bag before running steps,
    /// and removes the annotation from the object after all steps
    /// complete successfully.
    async fn run_apply<T, F>(
        &mut self,
        obj_ref: &ObjectRef,
        prev_resource: &T,
        prev_pending_cleanup: &[String],
        build_status: &F,
        rotation_key: Option<&str>,
    ) -> Result<Action, ReconcileError>
    where
        T: Serialize + Default + PartialEq + Clone,
        F: Fn(&State, &T) -> T,
    {
        let handle = self.handle;
        let generation = self.obj.meta().generation.unwrap_or(0);
        let mut state = State::new();
        let mut last_patched: Option<ResourceStatus<T>> = None;

        let mut pending_cleanup: Vec<String> = prev_pending_cleanup.to_vec();

        // Signal rotation to effects via the state bag.
        if rotation_key.is_some() {
            state.insert::<RotationRequested>(());
        }

        let cx = ReconcileContext {
            object: obj_ref,
            condition: &self.cx,
            cr: &*self.obj,
        };

        for step in self.apply_steps.iter() {
            match step.run_apply(&cx, handle, &mut state).await {
                StepProgress::Continue => {
                    if let Some(name) = step.effect_name() {
                        if !pending_cleanup.iter().any(|n| n == name) {
                            pending_cleanup.push(name.to_string());
                        }
                    }
                    let resource = build_status(&state, prev_resource);
                    let wrapped = ResourceStatus {
                        conditions: self.conditions.clone(),
                        observed_generation: Some(generation),
                        pending_cleanup: pending_cleanup.clone(),
                        resource,
                    };
                    if last_patched.as_ref() != Some(&wrapped) {
                        patch_status_via_handle::<R, _>(handle, obj_ref, &wrapped).await?;
                        last_patched = Some(wrapped);
                    }
                }
                StepProgress::Wait { condition, backoff } => {
                    let retry_after = compute_retry_after(&self.conditions, &condition, &*backoff);
                    set_condition(&mut self.conditions, condition);
                    let resource = build_status(&state, prev_resource);
                    let wrapped = ResourceStatus {
                        conditions: self.conditions.clone(),
                        observed_generation: Some(generation),
                        pending_cleanup: pending_cleanup.clone(),
                        resource,
                    };
                    if last_patched.as_ref() != Some(&wrapped) {
                        patch_status_via_handle::<R, _>(handle, obj_ref, &wrapped).await?;
                    }
                    return Ok(Action::requeue(retry_after));
                }
                StepProgress::Error(e) => return Err(e.into()),
            }
        }

        set_condition(&mut self.conditions, self.cx.reconciled_condition());
        let resource = build_status(&state, prev_resource);
        let wrapped = ResourceStatus {
            conditions: self.conditions.clone(),
            observed_generation: Some(generation),
            pending_cleanup: pending_cleanup.clone(),
            resource,
        };
        if last_patched.as_ref() != Some(&wrapped) {
            patch_status_via_handle::<R, _>(handle, obj_ref, &wrapped).await?;
        }

        if let Some(key) = rotation_key {
            if let Err(e) = handle.remove_annotation::<R>(obj_ref, key).await {
                warn!(
                    error = %e,
                    annotation = %key,
                    obj = %obj_ref,
                    "Failed to remove rotation annotation (non-fatal)"
                );
            }
        }

        Ok(Action::requeue(self.steady_interval))
    }

    /// Walk effects in reverse, calling cleanup on each.
    ///
    /// The walk is filtered by the pending-cleanup ledger persisted in
    /// status: `cleanup` only runs for effects whose name appears in the
    /// ledger, i.e. effects that actually completed `apply`. After each
    /// successful cleanup the entry is struck off and the shrunk ledger is
    /// patched back to status, giving real resumability across cleanup
    /// retries — a retry resumes from the failed effect and skips effects
    /// already cleaned up.
    ///
    /// Orphaned entries — names in the ledger that no registered cleanup
    /// step matches (e.g. after an effect rename or removal) — are logged
    /// and dropped. Blocking deletion forever on absent code is worse than
    /// leaking the orphaned external resource; the operator author can
    /// reconcile manually.
    ///
    /// Status patches that fail with 404 are tolerated — the resource may
    /// have been garbage-collected before cleanup completes.
    async fn run_cleanup<T, F>(
        &self,
        obj_ref: &ObjectRef,
        prev_resource: &T,
        prev_pending_cleanup: &[String],
        build_status: &F,
    ) -> Result<Action, ReconcileError>
    where
        T: Serialize + Default + PartialEq + Clone,
        F: Fn(&State, &T) -> T,
    {
        let handle = self.handle;
        let generation = self.obj.meta().generation.unwrap_or(0);
        let state = State::new();

        let cx = ReconcileContext {
            object: obj_ref,
            condition: &self.cx,
            cr: &*self.obj,
        };

        let mut outstanding: Vec<String> = prev_pending_cleanup.to_vec();

        for step in self.cleanup_steps.iter().rev() {
            let Some(name) = step.effect_name() else {
                continue;
            };

            if !outstanding.iter().any(|n| n == name) {
                continue;
            }

            match step.run_cleanup(&cx, handle).await {
                CleanupProgress::Continue => {
                    outstanding.retain(|n| n != name);
                    let resource = build_status(&state, prev_resource);
                    let wrapped = ResourceStatus {
                        conditions: self.conditions.clone(),
                        observed_generation: Some(generation),
                        pending_cleanup: outstanding.clone(),
                        resource,
                    };
                    let _ = patch_status_via_handle::<R, _>(handle, obj_ref, &wrapped).await;
                }
                CleanupProgress::Retry(backoff) => {
                    let retry_after = backoff.next(BackoffContext {
                        emitting: &Condition {
                            type_: "Cleanup".to_string(),
                            status: "False".to_string(),
                            reason: "RetryingCleanup".to_string(),
                            message: "An effect cleanup requested retry".to_string(),
                            last_transition_time: crate::utils::now(),
                            observed_generation: None,
                        },
                        previous: self.conditions.iter().find(|c| c.type_ == "Cleanup"),
                        now: k8s_openapi::jiff::Timestamp::now(),
                    });
                    let resource = build_status(&state, prev_resource);
                    let wrapped = ResourceStatus {
                        conditions: self.conditions.clone(),
                        observed_generation: Some(generation),
                        pending_cleanup: outstanding.clone(),
                        resource,
                    };
                    let _ = patch_status_via_handle::<R, _>(handle, obj_ref, &wrapped).await;
                    return Ok(Action::requeue(retry_after));
                }
                CleanupProgress::Error(e) => return Err(e.into()),
            }
        }

        for orphan in &outstanding {
            warn!(
                obj = %obj_ref,
                effect = %orphan,
                "pending-cleanup ledger entry matches no registered cleanup step; dropping (effect renamed or removed?)",
            );
        }

        let resource = build_status(&state, prev_resource);
        let wrapped = ResourceStatus {
            conditions: self.conditions.clone(),
            observed_generation: Some(generation),
            pending_cleanup: Vec::new(),
            resource,
        };
        let _ = patch_status_via_handle::<R, _>(handle, obj_ref, &wrapped).await;
        Ok(Action::await_change())
    }
}

/// A fully-configured reconcile pipeline with its status builder attached.
///
/// Produced by [`Reconcile::status`]. Call [`ReadyReconcile::execute`] for the
/// full finalizer-managed lifecycle, or [`ReadyReconcile::converge`] to run
/// the pipeline without finalizer management.
pub struct ReadyReconcile<'r, R: NamespaceResource + HasConditions, T>
where
    T: Serialize + DeserializeOwned + Default + PartialEq + Clone,
{
    inner: Reconcile<'r, R>,
    build_status: Box<dyn Fn(&State, &T) -> T + Send + Sync + 'r>,
}

impl<'r, R: NamespaceResource + HasConditions, T> ReadyReconcile<'r, R, T>
where
    T: Serialize + DeserializeOwned + Default + PartialEq + Clone,
{
    /// Full reconcile lifecycle: finalizer management + pipeline execution.
    pub async fn execute(self, finalizer_name: &str) -> Result<Action, ReconcileError> {
        self.inner.execute(finalizer_name, self.build_status).await
    }

    /// Run the pipeline without managing finalizers.
    pub async fn converge(mut self) -> Result<Action, ReconcileError> {
        self.inner.converge(self.build_status).await
    }
}

// ─── Arc wrappers to share EffectStep between apply and cleanup vecs ─

struct SharedApplyStep<T>(Arc<T>);

#[async_trait::async_trait]
impl<T, K> ApplyStep<K> for SharedApplyStep<T>
where
    T: ApplyStep<K> + Send + Sync,
    K: Send + Sync,
{
    async fn run_apply<'a>(
        &'a self,
        cx: &'a ReconcileContext<K>,
        handle: &'a KubeHandle,
        state: &mut State,
    ) -> StepProgress {
        self.0.run_apply(cx, handle, state).await
    }

    fn effect_name(&self) -> Option<&'static str> {
        self.0.effect_name()
    }
}

struct SharedCleanableStep<T>(Arc<T>);

#[async_trait::async_trait]
impl<T, K> CleanableStep<K> for SharedCleanableStep<T>
where
    T: CleanableStep<K> + Send + Sync,
    K: Send + Sync,
{
    async fn run_cleanup<'a>(
        &'a self,
        cx: &'a ReconcileContext<K>,
        handle: &'a KubeHandle,
    ) -> CleanupProgress {
        self.0.run_cleanup(cx, handle).await
    }

    fn effect_name(&self) -> Option<&'static str> {
        self.0.effect_name()
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────

/// Serialize the status value and patch it via KubeHandle.
async fn patch_status_via_handle<R, S>(
    handle: &KubeHandle,
    obj_ref: &ObjectRef,
    status: &S,
) -> Result<(), ReconcileError>
where
    R: NamespaceResource,
    S: Serialize,
{
    let value = serde_json::to_value(status)
        .map_err(|e| ReconcilerError::Env(EnvironmentError::Serde(e)))?;
    handle
        .patch_status::<R>(obj_ref, value)
        .await
        .map_err(ReconcilerError::Env)?;
    Ok(())
}

fn compute_retry_after(
    conditions: &[Condition],
    emitting: &Condition,
    backoff: &dyn Backoff,
) -> Duration {
    let previous = conditions.iter().find(|c| c.type_ == emitting.type_);
    backoff.next(BackoffContext {
        emitting,
        previous,
        now: k8s_openapi::jiff::Timestamp::now(),
    })
}

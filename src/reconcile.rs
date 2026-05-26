mod steps;

use std::{sync::Arc, time::Duration};

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::runtime::controller::Action;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    HasConditions, ReconcileContext, ReconcilerError,
    backoff::{Backoff, BackoffContext},
    effect::Effect,
    environment::{EnvResource, Environment, EnvironmentError, ObjectRef},
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
    #[error("reconciler error: {0}")]
    Reconciler(#[from] ReconcilerError),

    #[error("finalizer error: {0}")]
    Finalizer(#[from] EnvironmentError),
}

/// Builder and runner for a complete reconcile pass (apply + cleanup).
///
/// The reconciler owns the full lifecycle: it manages the finalizer
/// string, routes to the apply pipeline or the reverse-order cleanup
/// pipeline based on the object's deletion timestamp, and patches
/// status after every step (only when changed).
///
/// All Kubernetes interactions go through the [`Environment`] trait,
/// making the entire pipeline unit-testable without a cluster.
///
/// ```ignore
/// use kubo::reconcile::Reconcile;
///
/// Reconcile::new(ctx.env(), obj.clone())
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
pub struct Reconcile<'r, E: Environment + Sync, R: EnvResource + HasConditions> {
    env: &'r E,
    cx: ConditionContext,
    conditions: Vec<Condition>,
    obj: Arc<R>,
    apply_steps: Vec<Box<dyn ApplyStep<E> + 'r>>,
    cleanup_steps: Vec<Box<dyn CleanableStep<E> + 'r>>,
    steady_interval: Duration,
    rotation_annotation: Option<&'r str>,
}

impl<'r, E: Environment + Sync, R: EnvResource + HasConditions> Reconcile<'r, E, R> {
    /// Start a reconcile pass.
    ///
    /// The framework derives `generation` and initial `conditions` from the
    /// object automatically. Use [`ready_reason`](Self::ready_reason) to
    /// configure the terminal Ready condition.
    pub fn new(env: &'r E, obj: Arc<R>) -> Self {
        let generation = obj.meta().generation.unwrap_or(0);
        let conditions = obj.conditions();
        Self {
            env,
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
        G: Gate<E> + StateKey + Send + Sync + 'r,
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
        G: Gate<E> + Send + Sync + 'r,
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
        G: Gate<E> + Send + Sync + 'r,
        G::Error: Into<ReconcilerError>,
        <G as StateKey>::Value: Send + Sync,
    {
        self.apply_steps.push(Box::new(GuardStep::new(gate)));
        self
    }

    /// Add an effect. During apply, effects run after all preceding
    /// gates have passed. During cleanup, effects run in **reverse**
    /// declaration order via [`Effect::cleanup`].
    pub fn effect<F>(mut self, effect: F) -> Self
    where
        F: Effect<E> + StateKey + Send + Sync + 'r,
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
        P: Poll<E> + Send + Sync + 'r,
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
    pub fn group<V>(mut self, group: Group<V, E>) -> Self
    where
        V: Send + Sync + 'static,
        E: 'static,
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
            self.run_cleanup::<T, F>(&obj_ref, &prev_resource, &build_status)
                .await
        } else {
            self.run_apply::<T, F>(&obj_ref, &prev_resource, &build_status, has_rotation)
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

            let namespace = self.obj.meta().namespace.clone().unwrap_or_default();
            let name = self.obj.meta().name.clone().unwrap_or_default();
            let obj_ref = ObjectRef::new(&namespace, &name);
            let finalizers: Vec<String> = existing_finalizers
                .into_iter()
                .filter(|f| f.as_str() != finalizer_name)
                .collect();
            self.env.patch_finalizers::<R>(&obj_ref, finalizers).await?;
            Ok(action)
        } else {
            if !existing_finalizers.iter().any(|f| f == finalizer_name) {
                let namespace = self.obj.meta().namespace.clone().unwrap_or_default();
                let name = self.obj.meta().name.clone().unwrap_or_default();
                let obj_ref = ObjectRef::new(&namespace, &name);
                let mut new_finalizers = existing_finalizers;
                new_finalizers.push(finalizer_name.to_string());
                self.env
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
    /// ```ignore
    /// Reconcile::new(env, obj)
    ///     .effect(CreateDatabase { .. })
    ///     .poll(WaitForReady { .. })
    ///     .status(|state, prev| MyStatus {
    ///         database_url: state.get::<CreateDatabase>().cloned(),
    ///     })
    ///     .execute(FINALIZER)
    ///     .await
    /// ```
    pub fn status<T, F>(self, build_status: F) -> ReadyReconcile<'r, E, R, T>
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
    /// patch is issued via the environment. This gives intermediate
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
        build_status: &F,
        rotation_key: Option<&str>,
    ) -> Result<Action, ReconcileError>
    where
        T: Serialize + Default + PartialEq + Clone,
        F: Fn(&State, &T) -> T,
    {
        let env = self.env;
        let generation = self.obj.meta().generation.unwrap_or(0);
        let mut state = State::new();
        let mut last_patched: Option<ResourceStatus<T>> = None;

        // Signal rotation to effects via the state bag.
        if rotation_key.is_some() {
            state.insert::<RotationRequested>(());
        }

        let cx = ReconcileContext {
            object: obj_ref,
            condition: &self.cx,
        };

        for step in self.apply_steps.iter() {
            match step.run_apply(&cx, env, &mut state).await {
                StepProgress::Continue => {
                    let resource = build_status(&state, prev_resource);
                    let wrapped = ResourceStatus {
                        conditions: self.conditions.clone(),
                        observed_generation: Some(generation),
                        resource,
                    };
                    if last_patched.as_ref() != Some(&wrapped) {
                        patch_status_via_env::<R, _>(env, obj_ref, &wrapped).await?;
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
                        resource,
                    };
                    if last_patched.as_ref() != Some(&wrapped) {
                        patch_status_via_env::<R, _>(env, obj_ref, &wrapped).await?;
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
            resource,
        };
        if last_patched.as_ref() != Some(&wrapped) {
            patch_status_via_env::<R, _>(env, obj_ref, &wrapped).await?;
        }

        if let Some(key) = rotation_key {
            if let Err(e) = env.remove_annotation::<R>(obj_ref, key).await {
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
    /// Status patches that fail with 404 are tolerated — the resource may
    /// have been garbage-collected before cleanup completes.
    async fn run_cleanup<T, F>(
        &self,
        obj_ref: &ObjectRef,
        prev_resource: &T,
        build_status: &F,
    ) -> Result<Action, ReconcileError>
    where
        T: Serialize + Default + PartialEq + Clone,
        F: Fn(&State, &T) -> T,
    {
        let env = self.env;
        let generation = self.obj.meta().generation.unwrap_or(0);
        let state = State::new();

        let cx = ReconcileContext {
            object: obj_ref,
            condition: &self.cx,
        };

        for step in self.cleanup_steps.iter().rev() {
            match step.run_cleanup(&cx, env).await {
                CleanupProgress::Continue => continue,
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
                        resource,
                    };
                    let _ = patch_status_via_env::<R, _>(env, obj_ref, &wrapped).await;
                    return Ok(Action::requeue(retry_after));
                }
                CleanupProgress::Error(e) => return Err(e.into()),
            }
        }

        let resource = build_status(&state, prev_resource);
        let wrapped = ResourceStatus {
            conditions: self.conditions.clone(),
            observed_generation: Some(generation),
            resource,
        };
        let _ = patch_status_via_env::<R, _>(env, obj_ref, &wrapped).await;
        Ok(Action::await_change())
    }
}

/// A `Reconcile` pipeline with its status builder already attached.
///
/// Produced by [`Reconcile::status()`]. Call `.execute(finalizer)` or
/// `.converge()` to run the pipeline — no closure needed at that point.
pub struct ReadyReconcile<'r, E: Environment + Sync, R: EnvResource + HasConditions, T>
where
    T: Serialize + DeserializeOwned + Default + PartialEq + Clone,
{
    inner: Reconcile<'r, E, R>,
    build_status: Box<dyn Fn(&State, &T) -> T + Send + Sync + 'r>,
}

impl<'r, E: Environment + Sync, R: EnvResource + HasConditions, T> ReadyReconcile<'r, E, R, T>
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
impl<T, E> ApplyStep<E> for SharedApplyStep<T>
where
    T: ApplyStep<E> + Send + Sync,
    E: Environment + Sync,
{
    async fn run_apply<'a>(
        &'a self,
        cx: &'a ReconcileContext,
        env: &'a E,
        state: &mut State,
    ) -> StepProgress {
        self.0.run_apply(cx, env, state).await
    }
}

struct SharedCleanableStep<T>(Arc<T>);

#[async_trait::async_trait]
impl<T, E> CleanableStep<E> for SharedCleanableStep<T>
where
    T: CleanableStep<E> + Send + Sync,
    E: Environment + Sync,
{
    async fn run_cleanup<'a>(&'a self, cx: &'a ReconcileContext, env: &'a E) -> CleanupProgress {
        self.0.run_cleanup(cx, env).await
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────

/// Serialize the status value and patch it via Environment.
async fn patch_status_via_env<R, S>(
    env: &impl Environment,
    obj_ref: &ObjectRef,
    status: &S,
) -> Result<(), ReconcileError>
where
    R: EnvResource,
    S: Serialize,
{
    let value = serde_json::to_value(status)
        .map_err(|e| ReconcilerError::Env(EnvironmentError::Serde(e)))?;
    env.patch_status::<R>(obj_ref, value)
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

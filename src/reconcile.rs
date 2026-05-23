mod steps;

use std::{marker::PhantomData, sync::Arc, time::Duration};

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::runtime::controller::Action;
use serde::Serialize;

use crate::{
    ReconcilerError,
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
/// making the entire `run()` method unit-testable without a cluster.
///
/// ```ignore
/// let cx = ConditionContext::new(generation)
///     .with_ready_reason("Provisioned", "All resources ready");
///
/// Reconcile::new(&env, &cx, &mut conditions)
///     .gate(MyGate { ... })
///     .effect(CreateExternalThing { ... })
///     .effect(ApplyK8sResource { ... })
///     .poll(WaitForReady { ... })
///     .run(FINALIZER_NAME, obj, |conds, state| {
///         MyStatus {
///             observed_generation: Some(generation),
///             conditions: conds.to_vec(),
///             some_field: state.get::<MyGate>().cloned(),
///         }
///     })
///     .await
/// ```
pub struct Reconcile<'r, E: Environment + Sync> {
    env: &'r E,
    cx: &'r ConditionContext,
    conditions: &'r mut Vec<Condition>,
    apply_steps: Vec<Box<dyn ApplyStep<E> + 'r>>,
    cleanup_steps: Vec<Box<dyn CleanableStep<E> + 'r>>,
    steady_interval: Duration,
    rotation_annotation: Option<&'r str>,
    _e: PhantomData<E>,
}

impl<'r, E: Environment + Sync> Reconcile<'r, E> {
    /// Start a reconcile pass.
    ///
    /// `conditions` is mutated in place — the runner appends/updates
    /// gate/poll conditions as it walks the pipeline. The status
    /// closure receives a reference to this vec on every tick.
    pub fn new(env: &'r E, cx: &'r ConditionContext, conditions: &'r mut Vec<Condition>) -> Self {
        Self {
            env,
            cx,
            conditions,
            apply_steps: Vec::new(),
            cleanup_steps: Vec::new(),
            steady_interval: DEFAULT_STEADY_INTERVAL,
            rotation_annotation: None,
            _e: PhantomData,
        }
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
    /// inserts [`RotationRequested`](crate::state::RotationRequested) into
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

    /// Execute the reconcile pass.
    ///
    /// Manages the finalizer lifecycle, routes to apply or cleanup
    /// based on the object's deletion timestamp, and patches status
    /// after every step (only when changed, compared via `PartialEq`).
    ///
    /// All Kubernetes interactions go through the [`Environment`] trait
    /// passed to [`Reconcile::new`], making this method fully
    /// unit-testable.
    pub async fn run<R, S, F>(
        self,
        finalizer_name: &str,
        obj: Arc<R>,
        build_status: F,
    ) -> Result<Action, ReconcileError>
    where
        R: EnvResource,
        S: Serialize + PartialEq,
        F: Fn(&[Condition], &State) -> S,
    {
        let namespace = obj.meta().namespace.clone().unwrap_or_default();
        let name = obj.meta().name.clone().unwrap_or_default();
        let obj_ref = ObjectRef::new(&namespace, &name);

        let is_deleting = obj.meta().deletion_timestamp.is_some();

        if is_deleting {
            let has_our_finalizer = obj
                .meta()
                .finalizers
                .as_deref()
                .unwrap_or_default()
                .iter()
                .any(|f| f == finalizer_name);

            if !has_our_finalizer {
                return Ok(Action::await_change());
            }

            let action = self.run_cleanup::<R, S, F>(&obj_ref, &build_status).await?;
            let finalizers: Vec<String> = obj
                .meta()
                .finalizers
                .as_deref()
                .unwrap_or_default()
                .iter()
                .filter(|f| f.as_str() != finalizer_name)
                .cloned()
                .collect();
            self.env.patch_finalizers::<R>(&obj_ref, finalizers).await?;
            Ok(action)
        } else {
            let finalizers = obj.meta().finalizers.as_deref().unwrap_or_default();
            if !finalizers.iter().any(|f| f == finalizer_name) {
                let mut new_finalizers: Vec<String> = finalizers.to_vec();
                new_finalizers.push(finalizer_name.to_string());
                self.env
                    .patch_finalizers::<R>(&obj_ref, new_finalizers)
                    .await?;
            }

            // Detect rotation annotation before running the pipeline.
            let has_rotation = self.rotation_annotation.and_then(|key| {
                obj.meta()
                    .annotations
                    .as_ref()
                    .filter(|a| a.contains_key(key))
                    .map(|_| key)
            });

            let action = self
                .run_apply::<R, S, F>(&obj_ref, &build_status, has_rotation)
                .await?;
            Ok(action)
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
    async fn run_apply<R, S, F>(
        self,
        obj_ref: &ObjectRef,
        build_status: &F,
        rotation_key: Option<&str>,
    ) -> Result<Action, ReconcileError>
    where
        R: EnvResource,
        S: Serialize + PartialEq,
        F: Fn(&[Condition], &State) -> S,
    {
        let env = self.env;
        let mut state = State::new();
        let mut last_patched: Option<S> = None;

        // Signal rotation to effects via the state bag.
        if rotation_key.is_some() {
            state.insert::<RotationRequested>(());
        }

        for step in self.apply_steps.iter() {
            match step.run_apply(env, &mut state).await {
                StepProgress::Continue => {
                    let current = build_status(self.conditions, &state);
                    if last_patched.as_ref() != Some(&current) {
                        patch_status_via_env::<R, S>(env, obj_ref, &current).await?;
                        last_patched = Some(current);
                    }
                }
                StepProgress::Wait { condition, backoff } => {
                    let retry_after = compute_retry_after(self.conditions, &condition, &*backoff);
                    set_condition(self.conditions, condition);
                    let current = build_status(self.conditions, &state);
                    if last_patched.as_ref() != Some(&current) {
                        patch_status_via_env::<R, S>(env, obj_ref, &current).await?;
                    }
                    return Ok(Action::requeue(retry_after));
                }
                StepProgress::Error(e) => return Err(e.into()),
            }
        }

        set_condition(self.conditions, self.cx.reconciled_condition());
        let current = build_status(self.conditions, &state);
        if last_patched.as_ref() != Some(&current) {
            patch_status_via_env::<R, S>(env, obj_ref, &current).await?;
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
    async fn run_cleanup<R, S, F>(
        &self,
        obj_ref: &ObjectRef,
        build_status: &F,
    ) -> Result<Action, ReconcileError>
    where
        R: EnvResource,
        S: Serialize + PartialEq,
        F: Fn(&[Condition], &State) -> S,
    {
        let env = self.env;
        let state = State::new();

        for step in self.cleanup_steps.iter().rev() {
            match step.run_cleanup(env).await {
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
                    let current = build_status(self.conditions, &state);
                    patch_status_via_env::<R, S>(env, obj_ref, &current).await?;
                    return Ok(Action::requeue(retry_after));
                }
                CleanupProgress::Error(e) => return Err(e.into()),
            }
        }

        let current = build_status(self.conditions, &state);
        patch_status_via_env::<R, S>(env, obj_ref, &current).await?;
        Ok(Action::await_change())
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
    async fn run_apply(&self, env: &E, state: &mut State) -> StepProgress {
        self.0.run_apply(env, state).await
    }
}

struct SharedCleanableStep<T>(Arc<T>);

#[async_trait::async_trait]
impl<T, E> CleanableStep<E> for SharedCleanableStep<T>
where
    T: CleanableStep<E> + Send + Sync,
    E: Environment + Sync,
{
    async fn run_cleanup(&self, env: &E) -> CleanupProgress {
        self.0.run_cleanup(env).await
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

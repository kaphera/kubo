//! Grouped steps — composable sub-pipelines that collapse into a single effect.
//!
//! A [`Group`] is a sequence of gates, effects, and polls that share an internal
//! state scope and produce a single output value for the outer pipeline. During
//! apply, steps in the group run sequentially — gates feed the scoped state,
//! effects mutate external state and feed the scoped state, polls observe
//! convergence. A reducer closure collapses the scoped state into the group's
//! published output.
//!
//! If any sub-step returns Wait/Pending, the group propagates this as
//! `EffectOutcome::Retry` to the outer pipeline — the entire group will be
//! re-entered on the next reconcile pass.
//!
//! During cleanup, only effects within the group run (in reverse declaration
//! order). Gates and polls have no cleanup obligations.
//!
//! # Motivation
//!
//! Some logical operations consist of several tightly-coupled sub-steps that
//! share intermediate state (e.g. "resolve a credential, apply a ConfigMap,
//! apply a Secret, then compute a hash from both resource versions"). These
//! sub-steps aren't meaningful as independent pipeline steps — their
//! intermediate outputs are implementation details, not things downstream
//! steps should depend on.
//!
//! Groups let you compose atomic steps into a single pipeline position without
//! leaking internal state into the outer pipeline's state bag.
//!
//! # Example
//!
//! ```ignore
//! use k8s_operator_reconciler::group::Group;
//!
//! Reconcile::new(&env, &cx, &mut conditions)
//!     .gate(ResolvePostgresDatabase::new(...))
//!     .group(
//!         Group::new()
//!             .gate(ResolveAdminPassword::new(...))
//!             .effect(ApplyConfigMap::new(&obj))
//!             .effect(ApplyAdminSecret::new(&obj))
//!             .reduce(|state| {
//!                 let cm_rv = state.require::<ApplyConfigMap>()?;
//!                 let secret_rv = state.require::<ApplyAdminSecret>()?;
//!                 Ok(compute_hash(cm_rv, secret_rv))
//!             })
//!     )
//!     .effect(ApplyDeployment::new(&obj))
//!     .poll(WaitForReady::new(...))
//!     .run(FINALIZER, obj, |conds, state| { ... })
//!     .await
//! ```

use async_trait::async_trait;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;

use crate::{
    ReconcileContext, ReconcilerError,
    backoff::Backoff,
    cleanup::CleanupAction,
    effect::{Effect, EffectOutcome},
    environment::Environment,
    gate::{Gate, Outcome},
    poll::{Poll, PollState},
    state::{State, StateError, StateKey},
};

enum SubStepResult {
    Continue,
    Wait {
        condition: Condition,
        backoff: Box<dyn Backoff>,
    },
}

/// A type-erased step within a group. Each sub-step operates on the
/// group's internal state bag. Only effects carry cleanup obligations.
#[async_trait]
trait SubStep<E: Environment + Sync>: Send + Sync {
    async fn run(
        &self,
        cx: &ReconcileContext,
        env: &E,
        state: &mut State,
    ) -> Result<SubStepResult, ReconcilerError>;
    async fn cleanup(
        &self,
        cx: &ReconcileContext,
        env: &E,
    ) -> Result<CleanupAction, ReconcilerError>;
}

struct SubGateStep<G> {
    gate: G,
}

#[async_trait]
impl<G, E> SubStep<E> for SubGateStep<G>
where
    E: Environment + Sync,
    G: Gate<E> + StateKey + Send + Sync,
    G::Error: Into<ReconcilerError>,
    <G as StateKey>::Value: Send + Sync,
{
    async fn run(
        &self,
        cx: &ReconcileContext,
        env: &E,
        state: &mut State,
    ) -> Result<SubStepResult, ReconcilerError> {
        match self.gate.evaluate(cx, env).await {
            Ok(Outcome::Pass(v)) => {
                state.insert::<G>(v);
                Ok(SubStepResult::Continue)
            }
            Ok(Outcome::Wait { condition, backoff }) => {
                Ok(SubStepResult::Wait { condition, backoff })
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn cleanup(
        &self,
        _cx: &ReconcileContext,
        _env: &E,
    ) -> Result<CleanupAction, ReconcilerError> {
        Ok(CleanupAction::Skip)
    }
}

struct SubEffectStep<F> {
    effect: F,
}

#[async_trait]
impl<F, E> SubStep<E> for SubEffectStep<F>
where
    E: Environment + Sync,
    F: Effect<E> + StateKey + Send + Sync,
    F::Error: Into<ReconcilerError>,
    <F as StateKey>::Value: Send + Sync,
{
    async fn run(
        &self,
        cx: &ReconcileContext,
        env: &E,
        state: &mut State,
    ) -> Result<SubStepResult, ReconcilerError> {
        match self.effect.apply(cx, env, state).await {
            Ok(EffectOutcome::Done(v)) => {
                state.insert::<F>(v);
                Ok(SubStepResult::Continue)
            }
            Ok(EffectOutcome::Retry { condition, backoff }) => {
                Ok(SubStepResult::Wait { condition, backoff })
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn cleanup(
        &self,
        cx: &ReconcileContext,
        env: &E,
    ) -> Result<CleanupAction, ReconcilerError> {
        self.effect.cleanup(cx, env).await.map_err(Into::into)
    }
}

struct SubPollStep<P> {
    poll: P,
}

#[async_trait]
impl<P, E> SubStep<E> for SubPollStep<P>
where
    E: Environment + Sync,
    P: Poll<E> + Send + Sync,
    P::Error: Into<ReconcilerError>,
{
    async fn run(
        &self,
        cx: &ReconcileContext,
        env: &E,
        _state: &mut State,
    ) -> Result<SubStepResult, ReconcilerError> {
        match self.poll.check(cx, env).await {
            Ok(PollState::Done) => Ok(SubStepResult::Continue),
            Ok(
                PollState::Pending { condition, backoff }
                | PollState::Failed { condition, backoff },
            ) => Ok(SubStepResult::Wait { condition, backoff }),
            Err(e) => Err(e.into()),
        }
    }

    async fn cleanup(
        &self,
        _cx: &ReconcileContext,
        _env: &E,
    ) -> Result<CleanupAction, ReconcilerError> {
        Ok(CleanupAction::Skip)
    }
}

/// A function that collapses the group's internal state into a single output.
///
/// The reducer has read-only access to the scoped state bag after all
/// sub-steps have completed. It produces the value that gets published to
/// the outer pipeline under the group's `StateKey`.
pub trait Reducer<V>: Send + Sync {
    fn reduce(&self, state: &State) -> Result<V, StateError>;
}

impl<F, V> Reducer<V> for F
where
    F: Fn(&State) -> Result<V, StateError> + Send + Sync,
{
    fn reduce(&self, state: &State) -> Result<V, StateError> {
        (self)(state)
    }
}

/// A composable sub-pipeline that collapses into a single effect step.
///
/// The group maintains its own state bag, separate from the outer pipeline.
/// Sub-steps can read each other's outputs within the group, but those
/// intermediate values never leak into the outer state. Only the reducer's
/// output is published externally.
///
/// Cleanup walks effects in reverse declaration order (gates and polls are
/// skipped — they have no cleanup obligations).
pub struct Group<V, E: Environment + Sync> {
    steps: Vec<Box<dyn SubStep<E>>>,
    reducer: Option<Box<dyn Reducer<V>>>,
}

impl<V: Send + Sync + 'static, E: Environment + Sync + 'static> Group<V, E> {
    /// Create an empty group.
    pub fn new() -> Self {
        Self {
            steps: Vec::new(),
            reducer: None,
        }
    }

    /// Add a gate to the group.
    ///
    /// The gate's output is inserted into the scoped state bag and is
    /// readable by subsequent sub-steps. If the gate returns Wait, the
    /// group propagates it as a Retry to the outer pipeline.
    pub fn gate<G>(mut self, gate: G) -> Self
    where
        G: Gate<E> + StateKey + Send + Sync + 'static,
        G::Error: Into<ReconcilerError>,
        <G as StateKey>::Value: Send + Sync,
    {
        self.steps.push(Box::new(SubGateStep { gate }));
        self
    }

    /// Add an effect to the group.
    ///
    /// The effect's output is inserted into the scoped state bag. Its
    /// cleanup method is called (in reverse order) when the outer
    /// pipeline runs cleanup.
    pub fn effect<F>(mut self, effect: F) -> Self
    where
        F: Effect<E> + StateKey + Send + Sync + 'static,
        F::Error: Into<ReconcilerError>,
        <F as StateKey>::Value: Send + Sync,
    {
        self.steps.push(Box::new(SubEffectStep { effect }));
        self
    }

    /// Add a poll to the group.
    ///
    /// If the poll returns Pending or Failed, the group propagates it
    /// as a Retry to the outer pipeline.
    pub fn poll<P>(mut self, poll: P) -> Self
    where
        P: Poll<E> + Send + Sync + 'static,
        P::Error: Into<ReconcilerError>,
    {
        self.steps.push(Box::new(SubPollStep { poll }));
        self
    }

    /// Set the reducer that collapses the group's scoped state into a single
    /// output value.
    ///
    /// The reducer runs after all sub-steps complete successfully. Its
    /// output is what gets published to the outer pipeline under the group's
    /// `StateKey`.
    pub fn reduce<R>(mut self, reducer: R) -> Self
    where
        R: Reducer<V> + 'static,
    {
        self.reducer = Some(Box::new(reducer));
        self
    }
}

#[async_trait]
impl<V, E> Effect<E> for Group<V, E>
where
    V: Send + Sync + 'static,
    E: Environment + Sync + 'static,
{
    type Error = ReconcilerError;

    async fn apply(
        &self,
        cx: &ReconcileContext,
        env: &E,
        _outer_state: &State,
    ) -> Result<EffectOutcome<V>, ReconcilerError> {
        let mut scoped_state = State::new();

        for step in &self.steps {
            match step.run(cx, env, &mut scoped_state).await? {
                SubStepResult::Continue => continue,
                SubStepResult::Wait { condition, backoff } => {
                    return Ok(EffectOutcome::Retry { condition, backoff });
                }
            }
        }

        let reducer = self.reducer.as_ref().ok_or_else(|| {
            ReconcilerError::Env(crate::environment::EnvironmentError::Other(
                "group has no reducer".into(),
            ))
        })?;

        let output = reducer
            .reduce(&scoped_state)
            .map_err(|e| ReconcilerError::MissingInput(format!("group reducer failed: {e}")))?;

        Ok(EffectOutcome::Done(output))
    }

    async fn cleanup(
        &self,
        cx: &ReconcileContext,
        env: &E,
    ) -> Result<CleanupAction, ReconcilerError> {
        for step in self.steps.iter().rev() {
            match step.cleanup(cx, env).await? {
                CleanupAction::Skip | CleanupAction::Done => continue,
                CleanupAction::Retry(backoff) => return Ok(CleanupAction::Retry(backoff)),
            }
        }
        Ok(CleanupAction::Done)
    }
}

impl<V: Send + Sync + 'static, E: Environment + Sync + 'static> StateKey for Group<V, E> {
    type Value = V;
}

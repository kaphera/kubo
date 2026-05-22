use std::marker::PhantomData;

use async_trait::async_trait;

use crate::{
    cleanup::CleanupAction,
    effect::{Effect, EffectOutcome},
    environment::Environment,
    gate::{Gate, Outcome},
    poll::{Poll, PollState},
    reconcile::ReconcilerError,
    state::{State, StateKey},
};

use super::StepProgress;

// ─── Apply-phase steps ───────────────────────────────────────────────

#[async_trait]
pub(super) trait ApplyStep<E: Environment>: Send + Sync {
    async fn run_apply(&self, env: &E, state: &mut State) -> StepProgress;
}

#[async_trait]
pub(super) trait CleanableStep<E: Environment>: Send + Sync {
    async fn run_cleanup(&self, env: &E) -> CleanupProgress;
}

pub(super) enum CleanupProgress {
    Continue,
    Retry(Box<dyn crate::backoff::Backoff>),
    Error(ReconcilerError),
}

// ─── GateStep ────────────────────────────────────────────────────────

/// A gate whose output is stored in `State` under its own type as key.
pub(super) struct GateStep<G> {
    gate: G,
}

impl<G> GateStep<G> {
    pub fn new(gate: G) -> Self {
        Self { gate }
    }
}

#[async_trait]
impl<G, E> ApplyStep<E> for GateStep<G>
where
    E: Environment + Sync,
    G: Gate<E> + StateKey + Send + Sync,
    G::Error: Into<ReconcilerError>,
    <G as StateKey>::Value: Send + Sync,
{
    async fn run_apply(&self, env: &E, state: &mut State) -> StepProgress {
        match self.gate.evaluate(env).await {
            Ok(Outcome::Pass(v)) => {
                state.insert::<G>(v);
                StepProgress::Continue
            }
            Ok(Outcome::Wait { condition, backoff }) => StepProgress::Wait { condition, backoff },
            Err(e) => StepProgress::Error(e.into()),
        }
    }
}

// ─── GateAsStep ──────────────────────────────────────────────────────

/// A gate whose output is stored in `State` under an explicit key `K`.
pub(super) struct GateAsStep<G, K> {
    gate: G,
    _key: PhantomData<K>,
}

impl<G, K> GateAsStep<G, K> {
    pub fn new(gate: G) -> Self {
        Self {
            gate,
            _key: PhantomData,
        }
    }
}

#[async_trait]
impl<G, K, E> ApplyStep<E> for GateAsStep<G, K>
where
    E: Environment + Sync,
    G: Gate<E> + Send + Sync,
    G::Error: Into<ReconcilerError>,
    <G as StateKey>::Value: Send + Sync,
    K: StateKey<Value = <G as StateKey>::Value> + Send + Sync,
{
    async fn run_apply(&self, env: &E, state: &mut State) -> StepProgress {
        match self.gate.evaluate(env).await {
            Ok(Outcome::Pass(v)) => {
                state.insert::<K>(v);
                StepProgress::Continue
            }
            Ok(Outcome::Wait { condition, backoff }) => StepProgress::Wait { condition, backoff },
            Err(e) => StepProgress::Error(e.into()),
        }
    }
}

// ─── GuardStep ───────────────────────────────────────────────────────

/// A gate used purely as a guard; its output is not stored.
pub(super) struct GuardStep<G> {
    gate: G,
}

impl<G> GuardStep<G> {
    pub fn new(gate: G) -> Self {
        Self { gate }
    }
}

#[async_trait]
impl<G, E> ApplyStep<E> for GuardStep<G>
where
    E: Environment + Sync,
    G: Gate<E> + Send + Sync,
    G::Error: Into<ReconcilerError>,
    <G as StateKey>::Value: Send + Sync,
{
    async fn run_apply(&self, env: &E, _state: &mut State) -> StepProgress {
        match self.gate.evaluate(env).await {
            Ok(Outcome::Pass(_)) => StepProgress::Continue,
            Ok(Outcome::Wait { condition, backoff }) => StepProgress::Wait { condition, backoff },
            Err(e) => StepProgress::Error(e.into()),
        }
    }
}

// ─── EffectStep ──────────────────────────────────────────────────────

pub(super) struct EffectStep<F> {
    effect: F,
}

impl<F> EffectStep<F> {
    pub fn new(effect: F) -> Self {
        Self { effect }
    }
}

#[async_trait]
impl<F, E> ApplyStep<E> for EffectStep<F>
where
    E: Environment + Sync,
    F: Effect<E> + StateKey + Send + Sync,
    F::Error: Into<ReconcilerError>,
    <F as StateKey>::Value: Send + Sync,
{
    async fn run_apply(&self, env: &E, state: &mut State) -> StepProgress {
        match self.effect.apply(env, state).await {
            Ok(EffectOutcome::Done(v)) => {
                state.insert::<F>(v);
                StepProgress::Continue
            }
            Ok(EffectOutcome::Retry { condition, backoff }) => {
                StepProgress::Wait { condition, backoff }
            }
            Err(e) => StepProgress::Error(e.into()),
        }
    }
}

#[async_trait]
impl<F, E> CleanableStep<E> for EffectStep<F>
where
    E: Environment + Sync,
    F: Effect<E> + StateKey + Send + Sync,
    F::Error: Into<ReconcilerError>,
    <F as StateKey>::Value: Send + Sync,
{
    async fn run_cleanup(&self, env: &E) -> CleanupProgress {
        match self.effect.cleanup(env).await {
            Ok(CleanupAction::Skip) | Ok(CleanupAction::Done) => CleanupProgress::Continue,
            Ok(CleanupAction::Retry(backoff)) => CleanupProgress::Retry(backoff),
            Err(e) => CleanupProgress::Error(e.into()),
        }
    }
}

// ─── PollStep ────────────────────────────────────────────────────────

pub(super) struct PollStep<P> {
    poll: P,
}

impl<P> PollStep<P> {
    pub fn new(poll: P) -> Self {
        Self { poll }
    }
}

#[async_trait]
impl<P, E> ApplyStep<E> for PollStep<P>
where
    E: Environment + Sync,
    P: Poll<E> + Send + Sync,
    P::Error: Into<ReconcilerError>,
{
    async fn run_apply(&self, env: &E, _state: &mut State) -> StepProgress {
        match self.poll.check(env).await {
            Ok(PollState::Done) => StepProgress::Continue,
            Ok(PollState::Pending { condition, backoff }) => {
                StepProgress::Wait { condition, backoff }
            }
            Ok(PollState::Failed { condition, backoff }) => {
                StepProgress::Wait { condition, backoff }
            }
            Err(e) => StepProgress::Error(e.into()),
        }
    }
}

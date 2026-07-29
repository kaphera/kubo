use std::marker::PhantomData;

use async_trait::async_trait;

use crate::{
    cleanup::CleanupAction,
    effect::{Effect, EffectOutcome},
    environment::KubeHandle,
    gate::{Gate, Outcome},
    poll::{Poll, PollState},
    reconcile::ReconcilerError,
    state::{State, StateKey},
};

use super::ReconcileContext;
use super::StepProgress;

// ─── Apply-phase steps ───────────────────────────────────────────────

#[async_trait]
pub(super) trait ApplyStep<K: Send + Sync>: Send + Sync {
    async fn run_apply<'a>(
        &'a self,
        cx: &'a ReconcileContext<K>,
        handle: &'a KubeHandle,
        state: &mut State,
    ) -> StepProgress;

    fn effect_name(&self) -> Option<&'static str> {
        None
    }
}

#[async_trait]
pub(super) trait CleanableStep<K: Send + Sync>: Send + Sync {
    async fn run_cleanup<'a>(
        &'a self,
        cx: &'a ReconcileContext<K>,
        handle: &'a KubeHandle,
    ) -> CleanupProgress;

    fn effect_name(&self) -> Option<&'static str> {
        None
    }
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
impl<G, K> ApplyStep<K> for GateStep<G>
where
    K: Send + Sync,
    G: Gate<K> + StateKey + Send + Sync,
    G::Error: Into<ReconcilerError>,
    <G as StateKey>::Value: Send + Sync,
{
    async fn run_apply<'a>(
        &'a self,
        cx: &'a ReconcileContext<K>,
        handle: &'a KubeHandle,
        state: &mut State,
    ) -> StepProgress {
        match self.gate.evaluate(cx, handle).await {
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

/// A gate whose output is stored in `State` under an explicit key `J`.
pub(super) struct GateAsStep<G, J> {
    gate: G,
    _key: PhantomData<J>,
}

impl<G, J> GateAsStep<G, J> {
    pub fn new(gate: G) -> Self {
        Self {
            gate,
            _key: PhantomData,
        }
    }
}

#[async_trait]
impl<G, J, K> ApplyStep<K> for GateAsStep<G, J>
where
    K: Send + Sync,
    G: Gate<K> + Send + Sync,
    G::Error: Into<ReconcilerError>,
    <G as StateKey>::Value: Send + Sync,
    J: StateKey<Value = <G as StateKey>::Value> + Send + Sync,
{
    async fn run_apply<'a>(
        &'a self,
        cx: &'a ReconcileContext<K>,
        handle: &'a KubeHandle,
        state: &mut State,
    ) -> StepProgress {
        match self.gate.evaluate(cx, handle).await {
            Ok(Outcome::Pass(v)) => {
                state.insert::<J>(v);
                StepProgress::Continue
            }
            Ok(Outcome::Wait { condition, backoff }) => StepProgress::Wait { condition, backoff },
            Err(e) => StepProgress::Error(e.into()),
        }
    }
}

// ─── EffectStep ──────────────────────────────────────────────────────

pub(super) struct EffectStep<F> {
    effect: F,
    name: &'static str,
}

impl<F> EffectStep<F> {
    pub fn new<K: Send + Sync>(effect: F) -> Self
    where
        F: Effect<K>,
    {
        let name = effect
            .effect_name()
            .unwrap_or_else(|| std::any::type_name::<F>());
        Self { effect, name }
    }
}

#[async_trait]
impl<F, K> ApplyStep<K> for EffectStep<F>
where
    K: Send + Sync,
    F: Effect<K> + StateKey + Send + Sync,
    F::Error: Into<ReconcilerError>,
    <F as StateKey>::Value: Send + Sync,
{
    async fn run_apply<'a>(
        &'a self,
        cx: &'a ReconcileContext<K>,
        handle: &'a KubeHandle,
        state: &mut State,
    ) -> StepProgress {
        match self.effect.apply(cx, handle, state).await {
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

    fn effect_name(&self) -> Option<&'static str> {
        Some(self.name)
    }
}

#[async_trait]
impl<F, K> CleanableStep<K> for EffectStep<F>
where
    K: Send + Sync,
    F: Effect<K> + StateKey + Send + Sync,
    F::Error: Into<ReconcilerError>,
    <F as StateKey>::Value: Send + Sync,
{
    async fn run_cleanup<'a>(
        &'a self,
        cx: &'a ReconcileContext<K>,
        handle: &'a KubeHandle,
    ) -> CleanupProgress {
        match self.effect.cleanup(cx, handle).await {
            Ok(CleanupAction::Skip) | Ok(CleanupAction::Done) => CleanupProgress::Continue,
            Ok(CleanupAction::Retry(backoff)) => CleanupProgress::Retry(backoff),
            Err(e) => CleanupProgress::Error(e.into()),
        }
    }

    fn effect_name(&self) -> Option<&'static str> {
        Some(self.name)
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
impl<P, K> ApplyStep<K> for PollStep<P>
where
    K: Send + Sync,
    P: Poll<K> + Send + Sync,
    P::Error: Into<ReconcilerError>,
{
    async fn run_apply<'a>(
        &'a self,
        cx: &'a ReconcileContext<K>,
        handle: &'a KubeHandle,
        _state: &mut State,
    ) -> StepProgress {
        match self.poll.check(cx, handle).await {
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

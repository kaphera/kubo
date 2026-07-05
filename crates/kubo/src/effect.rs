//! Effects are the read-write phase of the pipeline — where side-effects live.
//!
//! An effect creates, updates, or configures external state (Kubernetes resources,
//! Vault mounts, database schemas, cloud APIs). It is the only step kind that
//! mutates anything, and consequently the only one that carries a cleanup obligation.
//!
//! # Design Constraints
//!
//! Effects must be **idempotent**. The framework may call `apply` multiple times
//! for the same logical operation (e.g. after a transient failure or operator restart).
//! Use Server-Side Apply semantics or external create-if-not-exists patterns to
//! ensure repeated calls converge to the same result.
//!
//! Effects must be **self-contained for cleanup**. During the cleanup phase, the
//! state bag from the original apply pass is not available. The cleanup method must
//! re-derive any context it needs (object references, credentials, API clients)
//! from the `env` directly or from fields on the effect struct.
//!
//! # Implementing an Effect
//!
//! ```ignore
//! struct CreateVaultMount {
//!     namespace: String,
//!     name: String,
//! }
//!
//! impl StateKey for CreateVaultMount {
//!     type Value = String; // the mount path
//! }
//!
//! #[async_trait]
//! impl<E: Environment + Sync> Effect<E, MyResource> for CreateVaultMount {
//!     type Error = MyError;
//!
//!     async fn apply(
//!         &self,
//!         cx: &ReconcileContext<MyResource>,
//!         env: &E,
//!         state: &State,
//!     ) -> Result<EffectOutcome<String>, Self::Error> {
//!         let vault_addr = state.require::<ResolveVaultInstance>()?;
//!         // ... create the mount via Vault API ...
//!         Ok(EffectOutcome::Done(format!("{}/{}/kv", self.namespace, self.name)))
//!     }
//!
//!     async fn cleanup(
//!         &self,
//!         cx: &ReconcileContext<MyResource>,
//!         env: &E,
//!     ) -> Result<CleanupAction, Self::Error> {
//!         // ... delete the mount via Vault API ...
//!         Ok(CleanupAction::Done)
//!     }
//! }
//! ```

use async_trait::async_trait;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;

use crate::{
    ReconcileContext,
    backoff::Backoff,
    cleanup::CleanupAction,
    environment::Environment,
    state::{State, StateKey},
};

/// The result of a successful [`Effect::apply`] call.
///
/// Most effects simply complete and return [`EffectOutcome::Done`] with
/// their output value. When an effect encounters a transient failure
/// that it has already handled (e.g. invalidating a cache) but needs
/// the reconciler to retry, it returns [`EffectOutcome::Retry`] with a
/// condition and backoff — the same semantics as a gate returning
/// [`Outcome::Wait`](crate::gate::Outcome::Wait)
pub enum EffectOutcome<T> {
    /// The effect completed successfully, producing a value.
    ///
    /// The framework inserts this value into `State` under the effect's
    /// own type (via its [`StateKey`] implementation). Downstream
    /// effects and the status closure access it via
    /// `state.get::<MyEffect>()`.
    Done(T),

    /// The effect handled a transient failure and requests a retry.
    ///
    /// The framework will patch the condition onto the status and
    /// requeue using the provided backoff strategy. The backoff
    /// receives the previous condition (if any) via [`BackoffContext`](crate::backoff::BackoffContext),
    /// giving it access to `last_transition_time` for duration-based
    /// escalation.
    Retry {
        condition: Condition,
        backoff: Box<dyn Backoff>,
    },
}

/// A write operation that mutates cluster or external state.
///
/// Effects are the only read-write phase in the reconcile pipeline.
/// Gates and Polls are read-only; Effects are where side-effects live.
///
/// Like gates, effects implement [`StateKey`] to declare what they
/// produce. The framework inserts the value returned via
/// [`EffectOutcome::Done`] into the state bag automatically — effects
/// never mutate state directly.
///
/// Because Effects are the sole source of external mutations, they are
/// also the sole source of cleanup obligations. Each Effect declares
/// how to undo itself via [`Effect::cleanup`]. The default
/// implementation returns [`CleanupAction::Skip`], meaning either the
/// resource is managed by Kubernetes garbage collection (owner
/// references) or there is nothing external to tear down.
///
/// The reconciler calls `cleanup` in **reverse declaration order** when
/// the parent resource is being deleted (i.e. during the finalizer's
/// `Cleanup` event).
#[async_trait]
pub trait Effect<E: Environment + Sync, K: Send + Sync = ()>: StateKey {
    type Error;

    /// Apply this effect — create or update external state.
    ///
    /// Called during the `Apply` phase of a reconcile pass. The `state`
    /// bag contains outputs from gates and preceding effects (read-only).
    ///
    /// Return [`EffectOutcome::Done`] on success — the framework
    /// will insert the value into state under this effect's type. Return
    /// [`EffectOutcome::Retry`] to signal a transient failure that the
    /// effect has already handled internally (e.g. cache invalidation)
    /// but requires the framework to requeue.
    async fn apply(
        &self,
        cx: &ReconcileContext<K>,
        env: &E,
        state: &State,
    ) -> Result<EffectOutcome<<Self as StateKey>::Value>, Self::Error>;

    /// Undo this effect — remove external state created by [`Effect::apply`](Effect::apply).
    ///
    /// Called during the `Cleanup` phase (finalizer deletion) in reverse
    /// declaration order. The implementation should be self-contained:
    /// it must re-derive any context it needs (secrets, tokens, client
    /// connections) from `env` directly, since the state bag from the
    /// original apply pass is not available.
    ///
    /// The default returns [`CleanupAction::Skip`], indicating that no
    /// explicit teardown is needed.
    async fn cleanup(
        &self,
        cx: &ReconcileContext<K>,
        env: &E,
    ) -> Result<CleanupAction, Self::Error> {
        let _ = cx;
        let _ = env;
        Ok(CleanupAction::Skip)
    }
}

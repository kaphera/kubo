//! Gates are read-only dependency checks that block the pipeline until satisfied.
//!
//! A gate answers a single question: "is my upstream dependency ready?" If yes,
//! it returns a typed value that downstream steps can consume. If not, it returns
//! a condition and backoff strategy, and the framework requeues.
//!
//! Gates are expected not to mutate external state: they read from the
//! Kubernetes API via [`KubeHandle`] and make a pass/wait decision. Following
//! this convention keeps them idempotent and safe to retry indefinitely.
//!
//! Use a gate when you need to block the pipeline on an upstream resource that
//! another controller manages; use a [`Poll`](crate::poll) when you need to
//! wait for something your own effects created to converge. When the gate's
//! output is not needed downstream, use
//! [`Reconcile::guard`](crate::reconcile::Reconcile::guard) instead.
//!
//! # Implementing a Gate
//!
//! ```rust,ignore (illustrative fragment: invented domain types)
//! struct ResolveDatabase {
//!     name: String,
//!     namespace: String,
//!     cx: ConditionContext,
//! }
//!
//! impl StateKey for ResolveDatabase {
//!     type Value = String; // the connection URL we extract
//! }
//!
//! #[async_trait]
//! impl Gate<Database> for ResolveDatabase {
//!     type Error = MyError;
//!
//!     async fn evaluate(&self, cx: &ReconcileContext<Database>, ops: &KubeHandle) -> Result<Outcome<String>, Self::Error> {
//!         let db = ops.get::<Database>(&ObjectRef::new(&self.namespace, &self.name)).await?;
//!         match db {
//!             Some(db) if is_ready(&db) => Ok(Outcome::Pass(db.status.url.unwrap())),
//!             _ => Ok(Outcome::Wait {
//!                 condition: self.cx.not_ready_condition("DatabasePending", "waiting for database"),
//!                 backoff: Box::new(Fixed::new(Duration::from_secs(10))),
//!             }),
//!         }
//!     }
//! }
//! ```
//!
//! The framework stores the `Pass` value in the state bag under `ResolveDatabase`
//! as the key (via its [`StateKey`] impl). Downstream effects retrieve it with
//! `state.require::<ResolveDatabase>()`.

use async_trait::async_trait;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;

use crate::{ReconcileContext, backoff::Backoff, environment::KubeHandle, state::StateKey};

/// A read-only dependency check that either passes (producing a value) or
/// waits (producing a condition for status and a backoff for requeue timing).
///
/// Gates implement [`StateKey`] to declare their output type. The framework
/// inserts the `Pass` value into the pipeline's [`State`](crate::state::State)
/// bag automatically — gates never touch state directly.
#[async_trait]
pub trait Gate<K: Send + Sync = ()>: StateKey {
    /// The error type returned when the gate encounters an unrecoverable failure.
    ///
    /// Must implement `Into<ReconcilerError>` so the framework can propagate it.
    type Error;

    /// Evaluate the gate against the current cluster state.
    ///
    /// Return `Outcome::Pass(value)` when the dependency is satisfied, or
    /// `Outcome::Wait { condition, backoff }` when it is not yet ready.
    /// Returning `Err` signals a hard failure that aborts the reconcile pass.
    async fn evaluate(
        &self,
        cx: &ReconcileContext<K>,
        ops: &KubeHandle,
    ) -> Result<Outcome<<Self as StateKey>::Value>, Self::Error>;
}

/// The result of a gate evaluation.
pub enum Outcome<T> {
    /// The dependency is satisfied. The contained value is stored in the
    /// pipeline's state bag for downstream consumption.
    Pass(T),

    /// The dependency is not yet ready. The framework patches this condition
    /// onto the object's status and requeues after the backoff duration.
    Wait {
        /// The not-ready condition patched onto the object's status while the
        /// dependency is unsatisfied.
        condition: Condition,
        /// The backoff strategy driving the requeue.
        backoff: Box<dyn Backoff>,
    },
}

impl<T> Outcome<T> {
    /// Returns `true` if the gate passed.
    pub fn is_pass(&self) -> bool {
        matches!(self, Outcome::Pass(_))
    }

    /// Transform the inner value of a `Pass` variant, leaving `Wait` unchanged.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Outcome<U> {
        match self {
            Outcome::Pass(t) => Outcome::Pass(f(t)),
            Outcome::Wait { condition, backoff } => Outcome::Wait { condition, backoff },
        }
    }
}

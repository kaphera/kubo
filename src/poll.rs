//! Polls observe external state without mutating it, waiting for convergence.
//!
//! A poll is a read-only check placed after effects in the pipeline. Where a
//! gate checks whether a *dependency* is ready (blocking downstream steps), a
//! poll checks whether something *this controller created* has converged to its
//! desired state. The distinction matters for semantics: polls sit after effects
//! and never produce state values for downstream consumption.
//!
//! # When to Use a Poll vs a Gate
//!
//! Use a **gate** when you need to block the pipeline on an upstream resource
//! that another controller manages. Use a **poll** when you need to wait for
//! a resource that *your* effect just created/updated to become ready.
//!
//! # Implementing a Poll
//!
//! ```ignore
//! struct WaitForDatabaseReady {
//!     name: String,
//!     namespace: String,
//!     cx: ConditionContext,
//! }
//!
//! #[async_trait]
//! impl Poll<E> for WaitForDatabaseReady {
//!     type Error = MyError;
//!
//!     async fn check(&self, env: &E) -> Result<PollState, Self::Error> {
//!         let db = env.get::<Database>(&ObjectRef::new(&self.namespace, &self.name)).await?;
//!         match db {
//!             Some(db) if has_ready_condition(&db) => Ok(PollState::Done),
//!             Some(_) => Ok(PollState::Pending {
//!                 condition: self.cx.not_ready_condition("DatabasePending", "waiting"),
//!                 backoff: Box::new(Fixed::new(Duration::from_secs(15))),
//!             }),
//!             None => Ok(PollState::Failed {
//!                 condition: self.cx.not_ready_condition("DatabaseMissing", "not found"),
//!                 backoff: Box::new(Fixed::new(Duration::from_secs(30))),
//!             }),
//!         }
//!     }
//! }
//! ```

use async_trait::async_trait;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;

use crate::{backoff::Backoff, environment::Environment};

/// The result of a poll check.
pub enum PollState {
    /// The observed state matches the desired state. The pipeline continues.
    Done,

    /// The resource exists but has not converged yet. The framework patches
    /// the condition and requeues with the provided backoff.
    Pending {
        condition: Condition,
        backoff: Box<dyn Backoff>,
    },

    /// The resource is in a failed or unexpected state. Semantically identical
    /// to `Pending` from the framework's perspective (requeue with backoff),
    /// but distinguished so that conditions can carry different reasons and
    /// operators can alert on `Failed` vs `Pending` in monitoring.
    Failed {
        condition: Condition,
        backoff: Box<dyn Backoff>,
    },
}

/// A read-only observation that waits for external state to converge.
///
/// Unlike gates, polls do not produce values for the state bag and do not
/// implement [`StateKey`](crate::state::StateKey). They exist purely to
/// block the pipeline until a condition is met and to surface progress
/// information via status conditions.
#[async_trait]
pub trait Poll<E: Environment + Sync> {
    /// The error type for unrecoverable failures during the check.
    type Error;

    /// Observe the current state and return whether convergence has been reached.
    async fn check(&self, env: &E) -> Result<PollState, Self::Error>;
}

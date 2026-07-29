//! Backoff strategies for controlling requeue timing on wait/retry outcomes.
//!
//! When a gate returns `Wait` or an effect returns `Retry`, it provides a
//! `Box<dyn Backoff>` alongside the condition. The framework calls
//! [`Backoff::next`] with the previous condition (if any) to determine how
//! long to wait before the next reconcile attempt. The same holds for
//! [`PollState::Pending`](crate::poll::PollState::Pending) and
//! [`PollState::Failed`](crate::poll::PollState::Failed), which also pair a
//! condition with a backoff; [`CleanupAction::Retry`](crate::cleanup::CleanupAction::Retry)
//! carries only the backoff — the framework synthesises the `Cleanup`
//! condition for backoff computation. The reconcile loop is the only caller
//! of [`Backoff::next`].
//!
//! The framework ships two strategies: [`Fixed`](fixed::Fixed) for constant
//! intervals and [`Exponential`](exponential::Exponential) for duration-aware
//! escalation. You can implement your own by implementing the [`Backoff`] trait.

pub mod exponential;
pub mod fixed;

use std::time::Duration;

use k8s_openapi::{apimachinery::pkg::apis::meta::v1::Condition, jiff::Timestamp};

/// Context passed to [`Backoff::next`] so the strategy can make duration-aware
/// decisions.
///
/// The `previous` field lets strategies like [`Exponential`](exponential::Exponential)
/// compute how long the condition has been in its current state (by comparing
/// `previous.last_transition_time` to `now`), enabling time-based escalation
/// without any mutable counter.
pub struct BackoffContext<'a> {
    /// The condition the wait/retry outcome produced. On the apply path this
    /// is the condition the framework patches onto the object's status; on
    /// the cleanup-retry path the framework synthesises a `Cleanup` condition
    /// purely for backoff computation and never persists it.
    pub emitting: &'a Condition,

    /// The existing condition with the same `type_` from the object's current
    /// status, if any. `None` on the first failure (no prior condition exists).
    /// On the cleanup-retry path it is effectively always `None`, because the
    /// synthesised `Cleanup` condition is never persisted.
    pub previous: Option<&'a Condition>,

    /// The current wall-clock time, for computing elapsed durations.
    pub now: Timestamp,
}

/// A strategy that computes the requeue duration for a waiting/retrying step.
///
/// Implementations must be `Send + Sync` because they are stored in
/// `Box<dyn Backoff>` and passed across async boundaries.
pub trait Backoff: Send + Sync {
    /// Compute the duration to wait before the next reconcile attempt.
    fn next(&self, ctx: BackoffContext<'_>) -> Duration;
}

//! Duration-aware backoff that escalates with time spent failing.
//!
//! Unlike counter-based backoff (which requires mutable state across reconcile
//! passes), this strategy derives its escalation from the elapsed time since
//! the condition's `last_transition_time`. The longer a condition has been in
//! the same state, the longer the next wait — up to a configurable cap.
//!
//! This is stateless by design: the Kubernetes status subresource already
//! records when a condition last transitioned, so the backoff can reconstruct
//! its position in the escalation curve from cluster state alone.
//!
//! # Formula
//!
//! ```text
//! wait = base + elapsed * (factor - 1)
//! wait = min(wait, cap)
//! ```
//!
//! On the first failure (`elapsed = 0`), the wait equals `base`. As the
//! condition persists unchanged, `elapsed` grows and the wait escalates
//! linearly (scaled by `factor - 1`) until it hits `cap`. A factor at or
//! below 1.0 clamps to zero growth, so the wait stays at `base` (or `cap`,
//! if that is smaller) — the strategy then behaves as a fixed backoff.

use std::time::Duration;

use super::{Backoff, BackoffContext};

/// Escalating backoff derived from time-in-state rather than a retry counter.
///
/// Defaults: base = 15s, cap = 300s (5 min), factor = 2.0.
///
/// ```ignore
/// Outcome::Wait {
///     condition: cx.not_ready_condition("VaultUnavailable", "connection refused"),
///     backoff: Box::new(Exponential::new(Duration::from_secs(5)).cap(Duration::from_secs(120))),
/// }
/// ```
#[derive(Clone, Copy, Debug)]
pub struct Exponential {
    base: Duration,
    cap: Duration,
    factor: f64,
}

impl Default for Exponential {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(15),
            cap: Duration::from_secs(300),
            factor: 2.0,
        }
    }
}

impl Exponential {
    /// Create an exponential backoff with a custom base interval.
    ///
    /// Uses the default cap (300s) and factor (2.0).
    pub fn new(base: Duration) -> Self {
        Self {
            base,
            ..Self::default()
        }
    }

    /// Set the maximum wait duration. The backoff never exceeds this value.
    pub fn cap(mut self, cap: Duration) -> Self {
        self.cap = cap;
        self
    }

    /// Set the escalation factor. Higher values mean faster escalation.
    ///
    /// A factor of 1.0 means no escalation (equivalent to fixed backoff).
    /// A factor of 2.0 means the wait grows by `elapsed * 1.0` on top of base.
    pub fn factor(mut self, factor: f64) -> Self {
        self.factor = factor;
        self
    }
}

impl Backoff for Exponential {
    fn next(&self, ctx: BackoffContext<'_>) -> Duration {
        let elapsed = elapsed_in_state(&ctx);
        let growth = (self.factor - 1.0).max(0.0);
        let secs = self.base.as_secs_f64() + elapsed.as_secs_f64() * growth;
        let capped = secs.min(self.cap.as_secs_f64()).max(0.0);
        Duration::from_secs_f64(capped)
    }
}

/// Compute how long the condition has been in its current state.
///
/// Returns `Duration::ZERO` if there is no previous condition or if the
/// condition has changed (different status/reason/message), indicating a
/// fresh failure that resets the escalation.
fn elapsed_in_state(ctx: &BackoffContext<'_>) -> Duration {
    let Some(prev) = ctx.previous else {
        return Duration::ZERO;
    };
    if prev.status != ctx.emitting.status
        || prev.reason != ctx.emitting.reason
        || prev.message != ctx.emitting.message
    {
        return Duration::ZERO;
    }
    let prev_ts = prev.last_transition_time.0;
    let signed = ctx.now.duration_since(prev_ts);
    let secs = signed.as_secs_f64().max(0.0);
    Duration::from_secs_f64(secs)
}

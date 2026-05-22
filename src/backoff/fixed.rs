//! A constant-interval backoff strategy.
//!
//! Always returns the same duration regardless of how long the condition
//! has been failing. Use this when you want predictable, non-escalating
//! retry intervals — for example, polling an external API that you expect
//! to become ready within a known timeframe.

use std::time::Duration;

use super::{Backoff, BackoffContext};

/// Returns the same duration on every call.
///
/// ```ignore
/// Outcome::Wait {
///     condition: cx.not_ready_condition("Pending", "waiting for dependency"),
///     backoff: Box::new(Fixed::new(Duration::from_secs(10))),
/// }
/// ```
#[derive(Clone, Copy, Debug)]
pub struct Fixed(Duration);

impl Fixed {
    /// Create a fixed backoff with the given interval.
    pub fn new(d: Duration) -> Self {
        Self(d)
    }
}

impl Backoff for Fixed {
    fn next(&self, _ctx: BackoffContext<'_>) -> Duration {
        self.0
    }
}

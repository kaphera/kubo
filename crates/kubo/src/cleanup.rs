//! Cleanup outcomes for the effect teardown phase.
//!
//! When a custom resource is being deleted (deletion timestamp present and
//! finalizer matched), the framework runs each effect's `cleanup` method in
//! reverse declaration order, filtered by the pending-cleanup ledger so that
//! cleanup only runs for effects that actually applied — see
//! [the ledger's semantics](crate::status#pending-cleanup-ledger). The
//! [`CleanupAction`] returned by each cleanup tells the framework whether to
//! continue to the next effect or to retry on a later pass.

use crate::backoff::Backoff;

/// The outcome of an [`Effect::cleanup`](crate::effect::Effect::cleanup) call.
///
/// The framework processes these as follows:
///
/// `Skip` or `Done` → continue to the next effect's cleanup (moving backward
/// through the pipeline). `Retry` → patch status, requeue with the backoff,
/// and resume from this effect on the next pass. The finalizer is only removed
/// once all effects return `Skip` or `Done`.
pub enum CleanupAction {
    /// No external state to clean up. Typical for effects that rely on
    /// Kubernetes owner references for garbage collection, or effects
    /// whose created resources are purely cluster-internal.
    Skip,

    /// External state was successfully removed. The framework proceeds
    /// to the next effect in reverse order.
    Done,

    /// Cleanup could not complete (e.g. external API unavailable). The
    /// framework requeues with the provided backoff. On the next pass,
    /// cleanup resumes from this effect.
    Retry(Box<dyn Backoff>),
}

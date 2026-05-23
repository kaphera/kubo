//! A minimal, opinionated framework for building Kubernetes operators in Rust.
//!
//! `k8s-operator-reconciler` eliminates the boilerplate of writing reconcile
//! loops by modelling them as sequential pipelines of composable steps. You
//! declare what your controller does — resolve dependencies, create resources,
//! wait for readiness — and the framework handles finalizer lifecycle, status
//! patching, backoff, and cleanup ordering.
//!
//! # Core Concepts
//!
//! A reconcile pipeline is a sequence of typed steps that run top-to-bottom on
//! every reconcile pass. Each step is one of four kinds:
//!
//! | Step kind | Trait    | Purpose                                      | Produces state? | Has cleanup? |
//! |-----------|----------|----------------------------------------------|-----------------|--------------|
//! | Gate      | [`Gate`](gate::Gate) | Read-only check; blocks pipeline until ready | Yes             | No           |
//! | Guard     | [`Gate`](gate::Gate) | Read-only check; output discarded            | No              | No           |
//! | Effect    | [`Effect`](effect::Effect) | Write operation; creates/updates resources | Yes             | Yes (reverse order) |
//! | Poll      | [`Poll`](poll::Poll) | Read-only observation; waits for convergence | No              | No           |
//!
//! Gates and effects produce typed values that flow downstream through a
//! [`State`](state::State) bag. Each step declares its output type via the
//! [`StateKey`](state::StateKey) trait. Downstream steps retrieve values
//! with `state.require::<UpstreamStep>()`.
//!
//! # Pipeline Lifecycle
//!
//! On **apply** (no deletion timestamp), the framework:
//! 1. Ensures the finalizer is present on the object.
//! 2. Runs each step in declaration order.
//! 3. After each step, evaluates the status closure and patches status if changed.
//! 4. If a step returns `Wait`, patches the condition and requeues with backoff.
//! 5. If all steps pass, sets `Ready=True` and requeues at the steady-state interval.
//!
//! On **cleanup** (deletion timestamp present, finalizer found), the framework:
//! 1. Runs effect cleanup methods in reverse declaration order.
//! 2. Removes the finalizer once all cleanups succeed.
//!
//! # Testability
//!
//! All Kubernetes interactions go through the [`Environment`](environment::Environment)
//! trait. The framework ships a [`TestEnvironment`](testing::TestEnvironment)
//! that runs entirely in-memory — no cluster needed. Combined with the
//! [`testing::properties`] module, you can write property-based assertions
//! over your controller's status progression without any infrastructure.
//!
//! # Quick Example
//!
//! ```ignore
//! use k8s_operator_reconciler::reconcile::Reconcile;
//! use k8s_operator_reconciler::utils::ConditionContext;
//!
//! let cx = ConditionContext::new(generation)
//!     .with_ready_reason("Provisioned", "All resources created");
//!
//! Reconcile::new(&env, &cx, &mut conditions)
//!     .gate(ResolveUpstreamDependency { .. })
//!     .effect(CreateDatabase { .. })
//!     .effect(CreateSecret { .. })
//!     .poll(WaitForDatabaseReady { .. })
//!     .run(FINALIZER, obj, |conds, state| MyStatus {
//!         observed_generation: Some(generation),
//!         conditions: conds.to_vec(),
//!         database_url: state.get::<CreateDatabase>().cloned(),
//!     })
//!     .await
//! ```

pub mod environment;
pub mod utils;

pub mod backoff;
pub mod state;

pub mod cleanup;
pub mod effect;
pub mod gate;
pub mod poll;

pub mod group;
pub mod reconcile;

#[cfg(feature = "test-support")]
pub mod testing;

pub use async_trait::async_trait;

use crate::environment::EnvironmentError;
pub use crate::environment::RealEnvironment;

/// Top-level error returned by individual pipeline steps (gates, effects, polls).
///
/// Controllers define their own domain error types and implement
/// `Into<ReconcilerError>` so the framework can propagate failures uniformly.
/// The two built-in variants cover the most common cases; domain errors
/// typically map into `Env(EnvironmentError::Other(..))`.
#[derive(Debug, thiserror::Error)]
pub enum ReconcilerError {
    /// A Kubernetes API call or serialisation failure surfaced through
    /// the [`Environment`](environment::Environment) abstraction.
    #[error("environment error: {0}")]
    Env(#[from] EnvironmentError),

    /// A required input was not present in the [`State`](state::State) bag.
    ///
    /// This typically means a step tried to read a value that a preceding
    /// gate or effect should have produced, but the pipeline was
    /// mis-ordered or the upstream step did not run.
    #[error("desired-state input not satisfied: {0}")]
    MissingInput(String),
}

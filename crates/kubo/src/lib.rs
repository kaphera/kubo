//! A complete, opinionated framework for building Kubernetes operators in Rust.
//!
//! Kubo eliminates the boilerplate of writing reconcile loops by modelling
//! controllers as sequential pipelines of composable steps. You declare what
//! your controller does — resolve dependencies, create resources, wait for
//! readiness — and the framework handles finalizer lifecycle, status patching,
//! backoff, and cleanup ordering. The top-level [`Runtime`](runtime::Runtime)
//! manages leader election and OS signal handling so you never wire that up
//! yourself.
//!
//! # Architecture
//!
//! Kubo is structured in three layers, each with a single responsibility:
//!
//! ```text
//! Runtime  — leader election, signal handling, wiring listeners together
//!   └─ Listener  — declares one controller: which CRD to watch + how to reconcile it
//!        └─ Reconcile  — the step pipeline for a single reconcile pass
//! ```
//!
//! [`Runtime`](runtime::Runtime) is the entry point. You register one
//! [`Listener`](listener::Listener) per CRD kind, and the runtime drives them
//! all. Each `Listener` owns exactly one reconcile definition, keeping
//! controllers isolated from each other.
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
//! Status is managed automatically. Wrap your custom status struct in
//! [`ResourceStatus<T>`](status::ResourceStatus) and Kubo will track
//! conditions and `observedGeneration` for you — no manual bookkeeping needed.
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
//! use kubo::runtime::Runtime;
//! use kubo::listener::{Listener, ControllerContext};
//! use kubo::reconcile::{ReadyReconcile, Reconcile};
//! use kubo::environment::RealEnvironment;
//!
//! // Wire up the runtime in main():
//! Runtime::new("my-operator", client, env)
//!     .with(reqwest::Client::new())
//!     .listener(DatabaseListener)
//!     .run()
//!     .await
//!
//! // One struct per controller:
//! pub struct DatabaseListener;
//!
//! impl Listener<RealEnvironment, Database> for DatabaseListener {
//!     type Status = DatabaseStatus;
//!
//!     const NAME: &'static str = "database";
//!     const FINALIZER: &'static str = "databases.example.com/cleanup";
//!
//!     // Optional: decorate the framework-provided controller with
//!     // `.owns()` / `.watches()`. The default implementation returns
//!     // it unchanged.
//!     fn controller(
//!         &self,
//!         ctx: &ControllerContext<RealEnvironment>,
//!         ctrl: Controller<Database>,
//!     ) -> Controller<Database> {
//!         let client = ctx.require::<kube::Client>();
//!         ctrl.owns(Api::<Secret>::all(client.clone()), Config::default())
//!     }
//!
//!     fn reconcile<'a>(
//!         &'a self,
//!         ctx: &'a ControllerContext<RealEnvironment>,
//!         obj: Arc<Database>,
//!         rec: Reconcile<'a, RealEnvironment, Database>,
//!     ) -> ReadyReconcile<'a, RealEnvironment, Database, DatabaseStatus> {
//!         rec.ready_reason("Provisioned", "Database and credentials ready")
//!             .gate(ResolveUpstream { .. })
//!             .effect(CreateDatabase { .. })
//!             .effect(CreateCredentialSecret { .. })
//!             .poll(WaitForReady { .. })
//!             .status(|state, prev| DatabaseStatus {
//!                 // Fall back to `prev` so a Wait on an early step never
//!                 // clobbers a value published on an earlier pass.
//!                 connection_url: state
//!                     .get::<CreateDatabase>()
//!                     .cloned()
//!                     .or(prev.connection_url.clone()),
//!             })
//!     }
//! }
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
pub mod status;

pub mod extensions;

pub mod listener;
pub mod runtime;

#[cfg(feature = "test-support")]
pub mod testing;

#[cfg(feature = "test-support")]
pub use kubo_test_macros::test;

#[cfg(feature = "test-support")]
pub use kubo_test_macros::property_test;

pub use async_trait::async_trait;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, OwnerReference};
use kube::ResourceExt;

use crate::{
    environment::{EnvResource, EnvironmentError, ObjectRef},
    utils::ConditionContext,
};

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

/// Trait for resources that expose status conditions.
///
/// Implement this on your CRD type to allow the reconcile framework
/// to extract the current conditions automatically. The implementation
/// is always the same pattern:
///
/// ```ignore
/// impl HasConditions for MyResource {
///     fn conditions(&self) -> Vec<Condition> {
///         self.status.as_ref().map(|s| s.conditions.clone()).unwrap_or_default()
///     }
/// }
/// ```
pub trait HasConditions {
    fn conditions(&self) -> Vec<Condition>;
}

/// Per-step context passed to every gate, effect, and poll invocation.
///
/// Bundles the three things a step implementation typically needs:
///
/// - `object` — the [`ObjectRef`] (namespace + name) of the resource being
///   reconciled, for constructing references to sibling resources.
/// - `condition` — the [`ConditionContext`] for building status conditions
///   with consistent `observedGeneration` (e.g.
///   `cx.condition.not_ready_condition("Pending", "waiting")`).
/// - `cr` — the full custom resource `K`, giving steps read access to the
///   spec without carrying their own clone.
///
/// The framework constructs one per reconcile pass and hands a reference to
/// each step. The `K` type parameter is the CR type declared by the
/// pipeline, so steps written against a concrete resource (e.g.
/// `Gate<E, WebService>`) get typed access to `cx.cr.spec`.
pub struct ReconcileContext<'a, K> {
    pub object: &'a ObjectRef,
    pub condition: &'a ConditionContext,
    pub cr: &'a K,
}

impl<'a, K> ReconcileContext<'a, K>
where
    K: EnvResource,
{
    pub fn current(&self) -> ObjectRef {
        let namespace = self.cr.namespace().unwrap_or_default();
        let name = self.cr.name_any();
        ObjectRef { namespace, name }
    }

    pub fn ensure_default_namespace(&self, namespace: Option<String>, name: String) -> ObjectRef {
        let namespace = namespace.unwrap_or(self.current().namespace.clone());
        ObjectRef { namespace, name }
    }

    pub fn owner_reference(&self) -> Option<OwnerReference> {
        self.cr.controller_owner_ref(&())
    }
}

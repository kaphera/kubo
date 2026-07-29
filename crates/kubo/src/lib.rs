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
//! 5. As each effect completes (`EffectOutcome::Done`), records the effect's identity in `ResourceStatus<T>::pending_cleanup` — a framework-owned ledger of effects that have applied but not yet been torn down.
//! 6. If all steps pass, sets `Ready=True` and requeues at the steady-state interval.
//!
//! On **cleanup** (deletion timestamp present, finalizer found), the framework:
//! 1. Runs effect `cleanup` methods in reverse declaration order, **filtered by the `pendingCleanup` ledger** — cleanup only runs for effects that actually completed `apply`.
//! 2. Strikes each effect off the ledger after its cleanup succeeds and patches the shrunk ledger back to status, giving resumability: a `Retry` resumes from the failed effect and skips effects already cleaned up.
//! 3. If any cleanup returns `Retry`, requeues with backoff and **keeps the finalizer** so the object survives for the next pass.
//! 4. Removes the finalizer only after all cleanups succeed. Ledger entries that match no registered cleanup step (e.g. after an effect rename or removal) are logged and dropped — blocking deletion forever on absent code is worse than leaking the orphaned resource.
//!
//! The ledger keys on the effect's Rust type name by default; override [`Effect::effect_name`](effect::Effect::effect_name) to pin a stable, release-independent key for effects whose `cleanup` must not leak on rename.
//!
//! # Testability
//!
//! All Kubernetes interactions go through [`KubeHandle`],
//! which wraps a `kube::Client`. In tests, construct a `KubeHandle` from the
//! client provided by `kubo::testing::World` — which points at a
//! `kubo_test_fakeapi::FakeApiServer` — and run the same
//! production code paths without a real cluster. Enable the `test-support`
//! feature to access the `testing` module, the `World` simulation API, and
//! the `#[kubo::test]` / `#[kubo::property_test]` macros.
//!
//! # Quick Example
//!
//! ```ignore
//! use kubo::runtime::Runtime;
//! use kubo::listener::{Listener, ControllerContext};
//! use kubo::reconcile::{ReadyReconcile, Reconcile};
//! use kubo::environment::KubeHandle;
//!
//! async fn main() {
//!     Runtime::new("my-operator", client)
//!         .with(reqwest::Client::new())
//!         .listener(DatabaseListener)
//!         .run()
//!         .await
//! }
//!
//! // One struct per controller:
//! pub struct DatabaseListener;
//!
//! impl Listener<Database> for DatabaseListener {
//!     type Status = DatabaseStatus;
//!
//!     const NAME: &'static str = "database";
//!     const FINALIZER: &'static str = "databases.example.com/cleanup";
//!
//!     fn reconcile<'a>(
//!         &'a self,
//!         ctx: &'a ControllerContext,
//!         obj: Arc<Database>,
//!         rec: Reconcile<'a, Database>,
//!     ) -> ReadyReconcile<'a, Database, DatabaseStatus> {
//!         rec.ready_reason("Provisioned", "Database and credentials ready")
//!            .gate(ResolveUpstream { .. })
//!            .effect(CreateDatabase { .. })
//!            .effect(CreateCredentialSecret { .. })
//!            .poll(WaitForReady { .. })
//!            .status(|state, _prev| DatabaseStatus {
//!                connection_url: state.get::<CreateDatabase>().cloned(),
//!            })
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
use kube::ResourceExt;

pub use crate::environment::KubeHandle;
use crate::{
    environment::{EnvResource, EnvironmentError, ObjectRef},
    utils::ConditionContext,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, OwnerReference};

/// Top-level error returned by individual pipeline steps (gates, effects, polls).
///
/// Controllers define their own domain error types and implement
/// `Into<ReconcilerError>` so the framework can propagate failures uniformly.
/// The two built-in variants cover the most common cases; domain errors
/// typically map into `Env(EnvironmentError::Other(..))`.
#[derive(Debug, thiserror::Error)]
pub enum ReconcilerError {
    /// A Kubernetes API call or serialisation failure.
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
    /// Returns the resource's current status conditions.
    ///
    /// The canonical implementation reads `status.conditions` and falls back
    /// to an empty vec when the status is unset.
    /// [`Reconcile::new`](crate::reconcile::Reconcile::new) seeds the
    /// pipeline's initial conditions from this.
    fn conditions(&self) -> Vec<Condition>;
}

/// Per-pass context handed to every pipeline step.
///
/// Constructed fresh for the apply pass and for the cleanup pass, and threaded
/// into [`Gate::evaluate`](crate::gate::Gate::evaluate),
/// [`Effect::apply`](crate::effect::Effect::apply),
/// [`Effect::cleanup`](crate::effect::Effect::cleanup), and
/// [`Poll::check`](crate::poll::Poll::check).
pub struct ReconcileContext<'a, K> {
    /// Namespace and name of the object being reconciled.
    pub object: &'a ObjectRef,
    /// Generation-scoped factory for building status conditions.
    pub condition: &'a ConditionContext,
    /// The custom resource being reconciled.
    pub cr: &'a K,
}

impl<'a, K> ReconcileContext<'a, K>
where
    K: EnvResource,
{
    /// Returns an [`ObjectRef`] for the reconciled resource: its namespace
    /// (empty when unset) and its name.
    pub fn current(&self) -> ObjectRef {
        let namespace = self.cr.namespace().unwrap_or_default();
        let name = self.cr.name_any();
        ObjectRef { namespace, name }
    }

    /// Returns an [`ObjectRef`] for a child object, using the supplied
    /// namespace or falling back to the reconciled resource's namespace when
    /// `None`. The name is used as given.
    pub fn ensure_default_namespace(&self, namespace: Option<String>, name: String) -> ObjectRef {
        let namespace = namespace.unwrap_or(self.current().namespace.clone());
        ObjectRef { namespace, name }
    }

    /// Returns the controller [`OwnerReference`] for the reconciled resource,
    /// for stamping onto child objects so Kubernetes garbage-collects them
    /// with their parent.
    pub fn owner_reference(&self) -> Option<OwnerReference> {
        self.cr.controller_owner_ref(&())
    }
}

#![cfg_attr(docsrs, feature(doc_cfg))]

//! A framework for building Kubernetes operators in Rust on top of [`kube`].
//!
//! Controllers are modelled as sequential pipelines of typed steps. The
//! framework takes over the finalizer lifecycle, status patching, requeue
//! backoff, reverse-order cleanup, and leader election. Client machinery, CRD
//! types, and watches remain kube's: [`Listener::controller`](crate::listener::Listener::controller)
//! receives kube's `kube::runtime::Controller` and may decorate it before it
//! runs. kubo does not replace kube's client or API machinery.
//!
//! # Overview
//!
//! Three layers make up a controller:
//!
//! - [`Runtime`](crate::runtime::Runtime) collects listeners, elects leadership
//!   over a Coordination v1 `Lease`, installs SIGTERM/SIGINT handling, and
//!   drains controllers on leadership loss or shutdown.
//! - [`Listener`](crate::listener::Listener) declares one controller: the
//!   resource kind it watches and the pipeline built in
//!   [`Listener::reconcile`](crate::listener::Listener::reconcile). Both methods
//!   receive a [`ListenerContext`](crate::listener::ListenerContext), the
//!   per-leadership-term context holding the [`KubeHandle`]
//!   and the extensions map.
//! - [`Reconcile`](crate::reconcile::Reconcile) is the pipeline for one
//!   reconcile pass; [`ReadyReconcile::execute`](crate::reconcile::ReadyReconcile::execute)
//!   runs it with finalizer management.
//!
//! A pipeline is a sequence of steps of three kinds:
//!
//! | Step | Registered via | Behaviour |
//! |------|----------------|-----------|
//! | Gate | [`Gate`](crate::gate::Gate) | read-only dependency check; passes a value or waits |
//! | Effect | [`Effect`](crate::effect::Effect) | write step; owns its cleanup |
//! | Poll | [`Poll`](crate::poll::Poll) | read-only convergence check, typically placed after effects |
//!
//! # Quick start
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
//! use kube::{Client, CustomResource};
//! use schemars::JsonSchema;
//! use serde::{Deserialize, Serialize};
//!
//! use kubo::listener::{ListenerContext, Listener};
//! use kubo::reconcile::{ReadyReconcile, Reconcile};
//! use kubo::runtime::Runtime;
//! use kubo::{environment::KubeHandle, gate::{Gate, Outcome}, state::StateKey, HasConditions, ReconcileContext, ReconcilerError};
//!
//! #[derive(CustomResource, Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
//! #[kube(group = "example.com", version = "v1", kind = "Website", status = "WebsiteStatus", namespaced)]
//! struct WebsiteSpec {
//!     host: String,
//! }
//!
//! #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
//! struct WebsiteStatus {
//!     host: Option<String>,
//!     #[serde(default, skip_serializing_if = "Vec::is_empty")]
//!     conditions: Vec<Condition>,
//! }
//!
//! impl HasConditions for Website {
//!     fn conditions(&self) -> Vec<Condition> {
//!         self.status.as_ref().map(|s| s.conditions.clone()).unwrap_or_default()
//!     }
//! }
//!
//! struct ResolveHost;
//! impl StateKey for ResolveHost {
//!     type Value = String;
//! }
//!
//! #[kubo::async_trait]
//! impl Gate<Website> for ResolveHost {
//!     type Error = ReconcilerError;
//!     async fn evaluate(&self, _cx: &ReconcileContext<Website>, _handle: &KubeHandle) -> Result<Outcome<String>, Self::Error> {
//!         Ok(Outcome::Pass("https://example.com".into()))
//!     }
//! }
//!
//! struct WebsiteListener;
//!
//! impl Listener<Website> for WebsiteListener {
//!     type Status = WebsiteStatus;
//!     const NAME: &'static str = "website";
//!     const FINALIZER: &'static str = "websites.example.com/cleanup";
//!     fn reconcile<'a>(&'a self, _ctx: &'a ListenerContext, _obj: Arc<Website>, rec: Reconcile<'a, Website>)
//!         -> ReadyReconcile<'a, Website, WebsiteStatus> {
//!         rec.gate(ResolveHost).status(|state, _prev| WebsiteStatus {
//!             host: state.get::<ResolveHost>().cloned(),
//!             conditions: vec![],
//!         })
//!     }
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! #     tokio::runtime::Runtime::new()?.block_on(async {
//! #         let client = Client::try_default().await?;
//! Runtime::new("website-operator", client)
//!     .listener(WebsiteListener)
//!     .run()
//!     .await?;
//! #         Ok(())
//! #     })
//! # }
//! ```
//!
//! Running the example requires a Kubernetes cluster reachable through the
//! default kubeconfig and the `Website` CRD installed.
//!
//! # Core concepts
//!
//! ## Gates
//!
//! A gate answers one question: is an upstream dependency ready?
//! [`Gate::evaluate`](crate::gate::Gate::evaluate) returns
//! [`Outcome::Pass`](crate::gate::Outcome::Pass) with a value that enters the
//! state bag, or [`Outcome::Wait`](crate::gate::Outcome::Wait) with a condition
//! and a backoff; the framework patches the condition and requeues.
//!
//! ## Effects
//!
//! Effects are the only steps that mutate external state, and the only ones
//! with a cleanup obligation. [`Effect::apply`](crate::effect::Effect::apply)
//! returns [`EffectOutcome::Done`](crate::effect::EffectOutcome::Done) or
//! requests a retry; [`Effect::cleanup`](crate::effect::Effect::cleanup)
//! defaults to [`CleanupAction::Skip`](crate::cleanup::CleanupAction::Skip)
//! for effects that rely on owner-reference garbage collection. The cleanup
//! ledger keys on the effect's Rust type name unless
//! [`Effect::effect_name`](crate::effect::Effect::effect_name) pins a stable,
//! release-independent name.
//!
//! ## Polls
//!
//! A poll observes state this controller created and blocks the pipeline until
//! convergence. [`Poll::check`](crate::poll::Poll::check) returns
//! [`PollState::Done`](crate::poll::PollState::Done), `Pending`, or `Failed`;
//! the latter two patch a condition and requeue, and polls produce no state
//! values.
//!
//! ## State
//!
//! Steps communicate through the [`State`](crate::state::State) bag and
//! nothing else. Gates and effects declare their output type via
//! [`StateKey`](crate::state::StateKey); downstream steps read it with
//! [`State::get`](crate::state::State::get) or
//! [`State::require`](crate::state::State::require), which fails with a
//! descriptive error when a step is mis-ordered.
//!
//! ## Groups
//!
//! [`Group`](crate::group::Group) composes several steps into a sub-pipeline
//! with a scoped state bag; only the reducer's output is published to the
//! outer pipeline. A sub-step wait propagates as a single effect retry, and
//! cleanup runs the group's effects in reverse order at the group's position.
//!
//! ## Status
//!
//! The framework wraps the listener's `Status` type in
//! [`ResourceStatus`](crate::status::ResourceStatus), maintaining `conditions`,
//! `observedGeneration`, and the `pendingCleanup` ledger alongside the
//! flattened domain fields, and patches status after each step only when
//! something changed. Conditions are built through a generation-scoped
//! [`ConditionContext`].
//!
//! **The CRD schema must declare the framework-managed status fields** — see
//! [`status`] for why structural-schema pruning makes this
//! mandatory.
//!
//! ## Finalizers and cleanup
//!
//! On apply, the framework ensures the finalizer is present and records each
//! completed effect in the `pendingCleanup` ledger. On deletion, it runs
//! effect cleanups in reverse declaration order filtered by the ledger,
//! strikes entries off as they succeed, and removes the finalizer only when
//! the ledger is empty; a cleanup that retries keeps the finalizer and
//! requeues with backoff. Ledger entries matching no registered step are
//! logged and dropped. The full lifecycle is documented on
//! [`Reconcile`](crate::reconcile).
//!
//! ## Backoff
//!
//! Steps that wait carry a [`Backoff`](crate::backoff::Backoff) strategy.
//! [`Fixed`](crate::backoff::fixed::Fixed) requeues at a constant interval;
//! [`Exponential`](crate::backoff::exponential::Exponential) escalates from
//! the condition's `last_transition_time`, so the wait grows with
//! time-in-state rather than with retry count.
//!
//! # Testing
//!
//! The `test-support` feature enables the `testing` module. `World` is an
//! in-memory cluster backed by `kubo_test_fakeapi::FakeApiServer` with a real
//! `kube::Client` in front of it, so pipelines exercise the same code paths
//! as production. Time is paused and the world's RNG is seeded, so
//! runs are reproducible: set `SEED` to replay one run, set `ITERATIONS` to
//! run across seeds (read only on the multi-run path — a plain `#[kubo::test]`
//! ignores it; when both are set, `SEED` takes precedence and a single run
//! executes), or pin seeds with the `seed`/`seeds` attribute options. The
//! `#[kubo::test]` macro wraps a test in paused-time tokio, builds a `World`
//! for each `&mut World` parameter, and prints the replay seed to stderr on
//! failure; `#[kubo::property_test]` additionally draws parameters from
//! proptest strategies (both macros are also available as
//! `kubo::testing::test` and `kubo::testing::property_test`). `World::fault`
//! injects faults (conflict, not-found,
//! internal, throttled, latency, disconnect) into mutable operations; random
//! triggering is drawn from the world's seeded RNG.
//!
//! # Feature flags
//!
//! - `test-support` — enables the `testing` module, the `#[kubo::test]` and
//!   `#[kubo::property_test]` macros, and the `kubo-test-fakeapi`,
//!   `kubo-test-macros`, and `proptest` dependencies, and turns on the
//!   `kube/client`, `kube/derive`, `tokio/test-util`, and `tokio/time`
//!   sub-features. The default feature set is empty.
//!
//! # Stability and compatibility
//!
//! The workspace manifest declares `rust-version = "1.95"` and dependencies on
//! kube 3.1.0 and k8s-openapi 0.27 (built with its `latest` API-version
//! feature). No compatibility policy is declared beyond these manifest values.
//!
//! # Links
//!
//! - Repository: <https://github.com/kaphera/kubo>
//! - `CONFORMANCE.md`, the registry of known divergences between the fake API
//!   server and a real kube-apiserver, machine-checked by the
//!   `kubo-conformance` crate:
//!   <https://github.com/kaphera/kubo/blob/main/CONFORMANCE.md>
//! - kube documentation: <https://kube.rs>

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
#[cfg_attr(docsrs, doc(cfg(feature = "test-support")))]
pub mod testing;

#[cfg(feature = "test-support")]
#[cfg_attr(docsrs, doc(cfg(feature = "test-support")))]
pub use kubo_test_macros::test;

#[cfg(feature = "test-support")]
#[cfg_attr(docsrs, doc(cfg(feature = "test-support")))]
pub use kubo_test_macros::property_test;

pub use async_trait::async_trait;
use kube::ResourceExt;

pub use crate::environment::KubeHandle;
use crate::{
    environment::{EnvironmentError, NamespaceResource, ObjectRef},
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
/// ```rust
/// # use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
/// # use kubo::HasConditions;
/// # struct MyStatus {
/// #     conditions: Vec<Condition>,
/// # }
/// # struct MyResource {
/// #     status: Option<MyStatus>,
/// # }
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
    K: NamespaceResource,
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

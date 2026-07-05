use std::fmt::Debug;
use std::sync::Arc;

use kube::runtime::Controller;
use serde::{Serialize, de::DeserializeOwned};

use crate::extensions::Extensions;
use crate::reconcile::Reconcile;
use crate::{
    HasConditions,
    environment::{EnvResource, Environment},
    reconcile::ReadyReconcile,
};

/// Per-controller context, scoped to a single leadership term.
///
/// This type IS the kube-rs reconcile context — it goes into `Arc` and gets
/// passed as the third argument to `Controller::run()`. Inside the reconcile
/// closure, kube-rs hands back `Arc<ControllerContext<E>>`, giving direct
/// access to the environment and extensions.
///
/// Listeners use this in both `controller()` (to get the kube client for
/// building watchers) and `reconcile()` (to grab operator-specific state
/// for constructing effects).
///
/// Shutdown is handled separately via `Controller::graceful_shutdown_on()`
/// because it's a stream-level concern, not a per-reconcile concern.
pub struct ControllerContext<E: Environment + Sync> {
    env: Arc<E>,
    extensions: Arc<Extensions>,
}

impl<E: Environment + Sync> ControllerContext<E> {
    pub fn new(env: Arc<E>, extensions: Arc<Extensions>) -> Self {
        Self { env, extensions }
    }

    /// Borrow the environment for building `Reconcile` pipelines.
    ///
    /// This is the bridge call: inside kube-rs's reconcile closure you have
    /// `Arc<ControllerContext<E>>` and need `&E` for `Reconcile::new(env, obj)`.
    pub fn env(&self) -> &E {
        &self.env
    }

    /// Access the extensions type-map.
    ///
    /// ```ignore
    /// let client = ctx.get::<kube::Client>().unwrap();
    /// let api = Api::<MyResource>::all(client.clone());
    /// ```
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.extensions.get::<T>()
    }

    /// Access a required extension, panicking if absent.
    pub fn require<T: Send + Sync + 'static>(&self) -> &T {
        self.extensions.require::<T>()
    }
}

/// A declarative controller definition.
///
/// Each Listener describes one controller: what resource it watches and how
/// to reconcile each object. The `Runtime` type-erases `R` at registration
/// and runs all listeners under leader election.
///
/// The trait separates watch setup (`controller`) from reconcile logic
/// (`reconcile`) so the Runtime can compose shutdown and error policies
/// uniformly — but crucially, both methods receive the `ControllerContext`
/// which interoperates directly with kube-rs's controller loop.
pub trait Listener<E, R>: Send + Sync
where
    E: Environment + Sync,
    R: EnvResource + HasConditions + Debug,
{
    /// The domain-specific status type for this resource.
    ///
    /// The framework wraps this in `ResourceStatus<Status>` automatically,
    /// adding `conditions` and `observedGeneration`.
    type Status: Serialize + DeserializeOwned + Default + PartialEq + Clone + Send + Sync;

    /// Human-readable name for logging and leader election diagnostics.
    const NAME: &'static str;

    /// Finalizer string managed by this controller.
    ///
    /// The runtime uses this with `ReadyReconcile::execute()` to ensure
    /// cleanup runs before the object is garbage-collected.
    const FINALIZER: &'static str;

    /// Build the kube-rs Controller watcher for this resource kind.
    ///
    /// Receives the `ControllerContext` so you can pull the kube client
    /// (or anything else) from extensions for building `Api<R>`.
    /// The returned Controller is not yet running — the Runtime attaches
    /// shutdown via `graceful_shutdown_on` before calling `.run()`.
    fn controller(&self, ctx: &ControllerContext<E>, ctrl: Controller<R>) -> Controller<R> {
        let _ = ctx;
        ctrl
    }

    /// Build the reconcile pipeline for a single object.
    ///
    /// Called on every reconcile event. Receives the full `ControllerContext`
    /// so you can pull operator-specific state (HTTP clients, caches, etc.)
    /// for constructing effects. The object comes from kube-rs's watcher,
    /// and `rec` is a fresh `Reconcile` constructed by the runtime — chain
    /// your gates, effects, and polls off it.
    ///
    /// Returns a fully-configured pipeline with status builder attached
    /// (via `.status()`). The Runtime calls `.execute(FINALIZER)` on it.
    ///
    /// ```ignore
    /// fn reconcile<'a>(
    ///     &'a self,
    ///     ctx: &'a ControllerContext<E>,
    ///     obj: Arc<R>,
    ///     rec: Reconcile<'a, E, R>,
    /// ) -> ReadyReconcile<'a, E, R, Self::Status> {
    ///     rec.effect(CreateDatabase { .. })
    ///         .poll(WaitForReady { .. })
    ///         .status(|state, prev| MyStatus {
    ///             database_url: state
    ///                 .get::<CreateDatabase>()
    ///                 .cloned()
    ///                 .or(prev.database_url.clone()),
    ///         })
    /// }
    /// ```
    fn reconcile<'a>(
        &'a self,
        ctx: &'a ControllerContext<E>,
        obj: Arc<R>,
        rec: Reconcile<'a, E, R>,
    ) -> ReadyReconcile<'a, E, R, Self::Status>;
}

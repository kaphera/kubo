use std::fmt::Debug;
use std::sync::Arc;

use kube::runtime::Controller;
use serde::{Serialize, de::DeserializeOwned};

use crate::extensions::Extensions;
use crate::reconcile::Reconcile;
use crate::{
    HasConditions,
    environment::{EnvResource, KubeHandle},
    reconcile::ReadyReconcile,
};

/// Per-controller context, scoped to a single leadership term.
///
/// This type IS the kube-rs reconcile context — it goes into `Arc` and gets
/// passed as the third argument to `Controller::run()`. Inside the reconcile
/// closure, kube-rs hands back `Arc<ControllerContext>`, giving direct
/// access to the operations handle and extensions.
///
/// Listeners use this in both `controller()` (to get the kube client for
/// building watchers) and `reconcile()` (to grab operator-specific state
/// for constructing effects).
pub struct ControllerContext {
    ops: Arc<KubeHandle>,
    extensions: Arc<Extensions>,
}

impl ControllerContext {
    pub fn new(ops: Arc<KubeHandle>, extensions: Arc<Extensions>) -> Self {
        Self { ops, extensions }
    }

    /// Borrow the operations handle for building `Reconcile` pipelines.
    pub fn ops(&self) -> &KubeHandle {
        &self.ops
    }

    /// Access the extensions type-map.
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
pub trait Listener<R>: Send + Sync
where
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
    fn controller(&self, ctx: &ControllerContext, ctrl: Controller<R>) -> Controller<R> {
        let _ = ctx;
        ctrl
    }

    /// Build the reconcile pipeline for a single object.
    ///
    /// Called on every reconcile event. Receives the full `ControllerContext`
    /// so you can pull operator-specific state (HTTP clients, caches, etc.)
    /// for constructing effects. The object comes from kube-rs's watcher.
    ///
    /// Returns a fully-configured pipeline with status builder attached
    /// (via `.status()`). The Runtime calls `.execute(FINALIZER)` on it.
    ///
    /// ```ignore
    /// fn reconcile<'a>(&self, ctx: &'a ControllerContext, obj: Arc<R>, rec: Reconcile<'a, R>) -> ... {
    ///     rec.effect(CreateDatabase { .. })
    ///        .poll(WaitForReady { .. })
    ///        .status(|state, _prev| MyStatus {
    ///            database_url: state.get::<CreateDatabase>().cloned(),
    ///        })
    /// }
    /// ```
    fn reconcile<'a>(
        &'a self,
        ctx: &'a ControllerContext,
        obj: Arc<R>,
        rec: Reconcile<'a, R>,
    ) -> ReadyReconcile<'a, R, Self::Status>;
}

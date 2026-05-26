use std::fmt::Debug;
use std::sync::Arc;

use kube::runtime::Controller;
use serde::{Serialize, de::DeserializeOwned};

use crate::extensions::Extensions;
use crate::{
    HasConditions,
    environment::{EnvResource, Environment},
    reconcile::ReadyReconcile,
};

// ─── Context Hierarchy ───────────────────────────────────────────────────────
//
// RuntimeContext  (process-scoped, owned by Runtime)
//   └─ ControllerContext  (leadership-term-scoped, kube-rs Ctx bridge)
//       └─ Reconcile      (single-object-scoped, already exists)
//
// Both contexts carry an Extensions type-map for operator-specific state.
// The kube::Client, reqwest::Client, database pools — anything an operator
// needs — goes in there. No hardcoded fields, no privileged types.

// ─── Extensions ──────────────────────────────────────────────────────────────

// /// A type-keyed map for operator-specific state.
// ///
// /// Unlike the pipeline [`State`] (which uses [`StateKey`](crate::state::StateKey)
// /// marker types for step-to-step communication), `Extensions` keys directly
// /// by value type. Insert a `reqwest::Client` and retrieve it as
// /// `ctx.get::<reqwest::Client>()`.
// ///
// /// This is the same pattern as `http::Extensions` / tower layers.
// #[derive(Default)]
// pub struct Extensions {
//     map: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
// }

// impl Extensions {
//     pub fn new() -> Self {
//         Self::default()
//     }

//     /// Insert a value, keyed by its concrete type. Replaces any previous
//     /// value of the same type.
//     pub fn insert<T: Send + Sync + 'static>(&mut self, val: T) {
//         self.map.insert(TypeId::of::<T>(), Box::new(val));
//     }

//     /// Retrieve a reference to a value by type, or `None` if absent.
//     pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
//         self.map.get(&TypeId::of::<T>())?.downcast_ref::<T>()
//     }

//     /// Retrieve a reference to a value by type, panicking with a clear
//     /// message if absent. Use when the value is guaranteed to be present
//     /// (e.g. inserted at startup).
//     pub fn require<T: Send + Sync + 'static>(&self) -> &T {
//         self.get::<T>().unwrap_or_else(|| {
//             panic!(
//                 "required extension `{}` not found in context",
//                 type_name::<T>()
//             )
//         })
//     }
// }

// ─── RuntimeContext ──────────────────────────────────────────────────────────

// /// Process-scoped context that the Runtime owns and shares with all controllers.
// ///
// /// Holds the operator name (used for leader election and SSA field manager)
// /// and an extensions map for operator-specific state. No hardcoded fields
// /// beyond the name — everything else goes through `.get::<T>()`.
// pub struct RuntimeContext<E: Environment + Sync> {
//     name: &'static str,
//     env: Arc<E>,
//     extensions: Arc<Extensions>,
// }

// impl<E: Environment + Sync> RuntimeContext<E> {
//     pub fn name(&self) -> &'static str {
//         self.name
//     }

//     pub fn env(&self) -> &E {
//         &self.env
//     }

//     pub fn env_arc(&self) -> Arc<E> {
//         Arc::clone(&self.env)
//     }

//     /// Access the extensions type-map.
//     pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
//         self.extensions.get::<T>()
//     }

//     /// Access a required extension, panicking if absent.
//     pub fn require<T: Send + Sync + 'static>(&self) -> &T {
//         self.extensions.require::<T>()
//     }

//     fn extensions_arc(&self) -> Arc<Extensions> {
//         Arc::clone(&self.extensions)
//     }
// }

// ─── ControllerContext ───────────────────────────────────────────────────────

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

// ─── Listener Trait ──────────────────────────────────────────────────────────

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
    fn controller(&self, ctx: &ControllerContext<E>) -> Controller<R>;

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
    /// fn reconcile<'a>(&self, ctx: &'a ControllerContext<E>, obj: Arc<R>) -> ... {
    ///     Reconcile::new(ctx.env(), obj)
    ///         .effect(CreateDatabase { .. })
    ///         .poll(WaitForReady { .. })
    ///         .status(|state, _prev| MyStatus {
    ///             database_url: state.get::<CreateDatabase>().cloned(),
    ///         })
    /// }
    /// ```
    fn reconcile<'a>(
        &'a self,
        ctx: &'a ControllerContext<E>,
        obj: Arc<R>,
    ) -> ReadyReconcile<'a, E, R, Self::Status>;
}

// ─── Type Erasure ────────────────────────────────────────────────────────────
//
// Listeners are generic over R, but Runtime stores them homogeneously. We
// erase R at registration time by converting each Listener into a closure
// that produces a boxed controller future. The closure captures the typed
// listener and knows how to wire it into kube-rs's Controller::run().
//
// This is the same trick OperatorRuntime uses with ControllerFn<C>.

// /// Type-erased controller factory. Produces a future that runs one controller
// /// for the duration of a leadership term.
// type ControllerFn<E> = Box<
//     dyn Fn(
//             &RuntimeContext<E>,
//             CancellationToken,
//         ) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>
//         + Send
//         + Sync,
// >;

// ─── Runtime ─────────────────────────────────────────────────────────────────

// /// Collects listeners and runs them under a unified lifecycle.
// ///
// /// Manages leader election via `k8s-operator-runtime`'s Coordination v1 Lease,
// /// signal handling (SIGTERM/SIGINT), and controller spawning/cancellation.
// ///
// /// ```text
// /// Runtime (leader election, signals, drain)
// ///   └─ Listener (declarative controller)
// ///       └─ Reconcile (step pipeline)
// /// ```
// ///
// /// # Example
// ///
// /// ```ignore
// /// Runtime::new("kaphera-edc-operator", env)
// ///     .with(client)                     // kube::Client
// ///     .with(reqwest::Client::new())     // HTTP client for external APIs
// ///     .listener(VaultInstanceListener)
// ///     .listener(PostgresDatabaseListener)
// ///     .run()
// ///     .await
// /// ```
// pub struct Runtime<E: Environment + Sync + 'static> {
//     name: &'static str,
//     client: kube::Client,
//     env: E,
//     extensions: Extensions,
//     controllers: Vec<(&'static str, ControllerFn<E>)>,
// }

// impl<E: Environment + Sync + 'static> Runtime<E> {
//     /// Create a new runtime.
//     ///
//     /// The `client` is used for leader election (Lease API) and is
//     /// automatically inserted into the extensions map so listeners
//     /// can access it via `ctx.require::<kube::Client>()`.
//     pub fn new(name: &'static str, client: kube::Client, env: E) -> Self {
//         let mut extensions = Extensions::new();
//         extensions.insert(client.clone());

//         Self {
//             name,
//             client,
//             env,
//             extensions,
//             controllers: Vec::new(),
//         }
//     }

//     /// Insert an arbitrary value into the operator's extensions map.
//     ///
//     /// Anything inserted here is available to all listeners via
//     /// `ctx.get::<T>()` in both `controller()` and `reconcile()`.
//     pub fn with<T: Send + Sync + 'static>(mut self, val: T) -> Self {
//         self.extensions.insert(val);
//         self
//     }

//     /// Register a listener. The resource type `R` is erased here — the
//     /// Runtime only retains a name and a closure that knows how to spawn
//     /// the typed controller loop.
//     pub fn listener<R, L>(mut self, listener: L) -> Self
//     where
//         R: EnvResource + HasConditions + Debug + 'static,
//         L: Listener<E, R> + 'static,
//     {
//         let listener = Arc::new(listener);

//         let factory: ControllerFn<E> = Box::new(move |rt_ctx, cancel| {
//             let listener = Arc::clone(&listener);
//             let env = rt_ctx.env_arc();
//             let extensions = rt_ctx.extensions_arc();

//             Box::pin(async move {
//                 let ctrl_ctx = Arc::new(ControllerContext::new(env, extensions));
//                 let controller = listener.controller(&ctrl_ctx);

//                 info!(controller = L::NAME, "Starting controller");

//                 controller
//                     .graceful_shutdown_on(cancel.cancelled_owned())
//                     .run(
//                         {
//                             let listener = Arc::clone(&listener);
//                             move |obj: Arc<R>, ctx: Arc<ControllerContext<E>>| {
//                                 let listener = Arc::clone(&listener);
//                                 async move {
//                                     listener
//                                         .reconcile(&ctx, obj)
//                                         .execute(L::FINALIZER)
//                                         .await
//                                 }
//                             }
//                         },
//                         |_obj: Arc<R>, _err: &ReconcileError, _ctx: Arc<ControllerContext<E>>| {
//                             Action::requeue(std::time::Duration::from_secs(5))
//                         },
//                         ctrl_ctx,
//                     )
//                     .for_each(|res| async move {
//                         if let Err(e) = res {
//                             error!(
//                                 controller = L::NAME,
//                                 error = ?e,
//                                 "Reconciliation failed"
//                             );
//                         }
//                     })
//                     .await;
//             })
//         });

//         self.controllers.push((L::NAME, factory));
//         self
//     }

//     /// Run all registered controllers under leader election.
//     ///
//     /// Blocks for the lifetime of the process. While this instance holds
//     /// the Coordination v1 Lease, all registered controllers run concurrently.
//     /// When leadership is lost or SIGTERM/SIGINT is received, controllers are
//     /// cancelled and given the drain timeout to finish before being aborted.
//     pub async fn run(self) -> Result<()> {
//         let config = RuntimeConfig::from_env(self.name);
//         let shutdown = CancellationToken::new();

//         let lease_lock = LeaseLock::new(
//             self.client,
//             &config.namespace,
//             &config.holder_id,
//             &config.name,
//             config.lease_ttl,
//         );

//         install_signal_handler(shutdown.clone());

//         let controller_names: Vec<&str> = self.controllers.iter().map(|(n, _)| *n).collect();
//         info!(
//             operator = %self.name,
//             controllers = ?controller_names,
//             "Starting operator with {} controllers",
//             self.controllers.len(),
//         );

//         let rt_ctx = Arc::new(RuntimeContext {
//             name: self.name,
//             env: Arc::new(self.env),
//             extensions: Arc::new(self.extensions),
//         });

//         let controllers: Arc<Vec<ControllerFn<E>>> =
//             Arc::new(self.controllers.into_iter().map(|(_, f)| f).collect());

//         k8s_operator_runtime::leader::run(
//             lease_lock,
//             Arc::clone(&rt_ctx),
//             shutdown,
//             &config,
//             move |rt_ctx, cancel: CancellationToken| {
//                 let controllers = Arc::clone(&controllers);
//                 async move {
//                     let futs: Vec<_> = controllers
//                         .iter()
//                         .map(|f| f(&rt_ctx, cancel.clone()))
//                         .collect();
//                     futures::future::join_all(futs).await;
//                     Ok(())
//                 }
//             },
//         )
//         .await
//     }
// }

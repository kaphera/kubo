//! `kubo::testing` — simulation runtime and test utilities for kubo controllers.
//!
//! The centerpiece is [`World`]: a simulated Kubernetes cluster backed by
//! [`FakeApiServer`].  Each `World` owns a
//! real [`kube::Client`] that speaks HTTP to the fake, so every reconcile
//! pipeline the test drives runs the same production code paths it would in a
//! pod.
//!
//! ## Driving controllers
//!
//! - [`WorldBuilder::listener`] — register a [`Listener`] to
//!   run as a background task inside the test's tokio runtime.
//! - [`World::run_until_idle`] — yield a fixed number of times (200), giving
//!   spawned controller tasks a chance to run.
//! - [`World::step`] — a single cooperative yield point.
//! - [`World::advance`] — advance paused tokio time by a [`Duration`] then
//!   call [`run_until_idle`](World::run_until_idle).  Requires the test to use
//!   paused time (`#[tokio::test(start_paused = true)]`).
//! - [`World::forbid_parking`] / [`World::allow_parking`] — parking-policy
//!   flags recorded on the `World`; not consulted by any runtime path.
//!
//! ## Asserting properties
//!
//! - [`properties`](crate::testing::properties) — property assertion functions (`assert_converges`,
//!   `assert_monotonic`, `assert_causal`, etc.) and helpers (`is_ready`,
//!   `assert_no_leaks`) for checking temporal invariants over status tick
//!   histories.

use std::{fmt::Debug, sync::Arc, time::Duration};

use futures::StreamExt as _;
use kube::{
    Api,
    api::{Patch, PatchParams},
    runtime::{Controller, controller::Action, watcher::Config as WatcherConfig},
};
use serde::de::DeserializeOwned;

use crate::{
    HasConditions,
    environment::EnvResource,
    extensions::Extensions,
    listener::{ControllerContext, Listener},
    reconcile::Reconcile,
    testing::Op,
};
use kubo_test_fakeapi::{FakeApiServer, Fault, FaultBuilder};

use crate::environment::KubeHandle;
use crate::reconcile::ReconcileError;

// ── ObjRef ────────────────────────────────────────────────────────────────────

/// A reference to a Kubernetes object by namespace and name.
///
/// Returned by [`World::list_refs`] and accepted by [`World::status_ticks_for`]
/// and [`World::is_deleted`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjRef {
    /// The object's namespace.
    pub namespace: String,
    /// The object's name.
    pub name: String,
}

impl ObjRef {
    /// Constructs a reference from a namespace and a name.
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }
}

impl std::fmt::Display for ObjRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.namespace, self.name)
    }
}

// ── Listener type erasure ─────────────────────────────────────────────────────

/// A factory that, given a `kube::Client`, spawns a controller loop and
/// returns its [`AbortHandle`] so the caller can cancel it on demand.
type ListenerFn = Box<dyn FnOnce(kube::Client) -> tokio::task::AbortHandle + Send>;

// ── ListenerResource ──────────────────────────────────────────────────────────

/// Associates a [`Listener`] type with the resource kind it reconciles.
///
/// Implementing this trait lets call sites write `L::Resource` instead of
/// threading a second type parameter everywhere.  The bound is intentionally
/// minimal: it captures the association only, leaving the full
/// `Listener<Self::Resource>` constraint to the sites that
/// actually need it.
pub trait ListenerResource {
    /// The resource kind the listener reconciles.
    type Resource: EnvResource + HasConditions + Debug;
}

// ── World ─────────────────────────────────────────────────────────────────────

/// A simulated Kubernetes cluster.
///
/// Owns a [`FakeApiServer`] and a [`kube::Client`] wired to it.  Every clone
/// of the client talks to the same in-memory store, so tests can hold as many
/// `Api` handles as they need.
///
/// Listeners registered via [`WorldBuilder::listener`] run as background tasks
/// on the same tokio runtime as the test.  Call [`World::run_until_idle`] to
/// yield a fixed number of times, giving those tasks a chance to run.
pub struct World {
    fake: FakeApiServer,
    client: kube::Client,
    forbid_parking: bool,
    seed: u64,
    /// Abort handles for all listener tasks spawned into this world.
    ///
    /// Called in `Drop` so that when a `World` goes out of scope (e.g. at the
    /// end of a property-test case), its background controller tasks are
    /// cancelled and removed from the scheduler immediately rather than
    /// accumulating across cases.
    listener_abort_handles: std::sync::Mutex<Vec<tokio::task::AbortHandle>>,
}

impl Drop for World {
    fn drop(&mut self) {
        for handle in self.listener_abort_handles.lock().unwrap().iter() {
            handle.abort();
        }
    }
}

impl World {
    /// Start building a new `World`.
    pub fn builder() -> WorldBuilder {
        WorldBuilder::new()
    }

    /// The kube-rs client wired to the fake API server.
    ///
    /// Use this to construct `Api<T>` handles and interact with the simulated
    /// cluster exactly as you would in production code.
    pub fn client(&self) -> &kube::Client {
        &self.client
    }

    /// The seed used to initialise this world's [`FakeApiServer`].
    ///
    /// Controlled by the `SEED` environment variable (default `0`) or by
    /// [`WorldBuilder::with_seed`].  Use this in tests to branch on the seed
    /// (e.g. for deliberately flaky regression tests).
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// A [`KubeHandle`] wrapping the fake client.
    ///
    /// Pass this to `Reconcile::new` to run a pipeline against the fake:
    ///
    /// ```ignore
    /// let env = world.env("kubo-test");
    /// let action = Reconcile::new(&env, obj)
    ///     .status(|_, _| MyStatus::default())
    ///     .execute(FINALIZER)
    ///     .await?;
    /// ```
    pub fn env(&self, field_manager: &str) -> KubeHandle {
        KubeHandle::new(self.client.clone(), field_manager)
    }

    /// Pre-populate the cluster with a typed object.
    ///
    /// Serializes `obj` and inserts it directly into the fake's store,
    /// bypassing HTTP.  This is the idiomatic way to seed upstream
    /// dependencies before running a reconcile pipeline.
    ///
    /// The name `prefill` distinguishes this from [`World::seed`] (which
    /// returns the numeric RNG seed).
    pub fn prefill<T: EnvResource>(&self, obj: T) -> &Self {
        let group = T::group(&()).into_owned();
        let version = T::version(&()).into_owned();
        let resource = T::plural(&()).into_owned();
        let namespace = obj
            .meta()
            .namespace
            .as_deref()
            .unwrap_or("default")
            .to_string();
        let name = obj.meta().name.clone().unwrap_or_default();
        let value = serde_json::to_value(&obj).expect("World::prefill: serialize");
        self.fake
            .insert(&group, &version, &resource, Some(&namespace), &name, value);
        self
    }

    /// Fetch a typed object directly from the fake's store, bypassing HTTP.
    ///
    /// Returns `None` if the object does not exist.  For HTTP-level reads,
    /// construct an `Api<T>` via [`World::client`] instead.
    pub fn get<T: EnvResource>(&self, ns: &str, name: &str) -> Option<T> {
        let group = T::group(&()).into_owned();
        let version = T::version(&()).into_owned();
        let resource = T::plural(&()).into_owned();
        let raw = self
            .fake
            .get_raw(&group, &version, &resource, Some(ns), name)?;
        serde_json::from_value(raw).ok()
    }

    /// Returns `true` if the named object exists in the fake's store.
    pub fn has<T: EnvResource>(&self, ns: &str, name: &str) -> bool {
        let group = T::group(&()).into_owned();
        let version = T::version(&()).into_owned();
        let resource = T::plural(&()).into_owned();
        self.fake
            .get_raw(&group, &version, &resource, Some(ns), name)
            .is_some()
    }

    /// Returns `true` if a condition with `type_ == type_` has `status: "True"`
    /// on the stored object.
    ///
    /// Reads directly from the fake's store so it reflects the latest status
    /// patch without needing a round-trip through the kube client.
    pub fn condition<T: EnvResource>(&self, ns: &str, name: &str, type_: &str) -> bool {
        let group = T::group(&()).into_owned();
        let version = T::version(&()).into_owned();
        let resource = T::plural(&()).into_owned();
        let raw = match self
            .fake
            .get_raw(&group, &version, &resource, Some(ns), name)
        {
            Some(v) => v,
            None => return false,
        };
        raw.get("status")
            .and_then(|s| s.get("conditions"))
            .and_then(|c| c.as_array())
            .map(|arr| {
                arr.iter().any(|c| {
                    c.get("type").and_then(|t| t.as_str()) == Some(type_)
                        && c.get("status").and_then(|s| s.as_str()) == Some("True")
                })
            })
            .unwrap_or(false)
    }

    /// Returns the ordered history of status values patched onto `(ns, name)`,
    /// deserialized as `S`.
    ///
    /// Each entry corresponds to one `PATCH .../status` call made during
    /// a reconcile pass.  Entries that fail to deserialize as `S` are silently
    /// skipped.  Use this with `kubo::testing::properties` to assert
    /// on status progression.
    pub fn status_ticks<S: DeserializeOwned>(&self, ns: &str, name: &str) -> Vec<S> {
        self.fake
            .status_history_for(ns, name)
            .into_iter()
            .filter_map(|v| serde_json::from_value(v).ok())
            .collect()
    }

    /// Apply a typed resource via HTTP, triggering watch broadcasts.
    ///
    /// Uses `PatchParams::apply("kubo-test")` (SSA field manager).  Returns
    /// the stored object with its injected `resourceVersion`.
    pub async fn create<T: EnvResource>(&self, obj: T) -> T {
        let ns = obj
            .meta()
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());
        let name = obj.meta().name.clone().unwrap_or_default();
        let api: Api<T> = Api::namespaced(self.client.clone(), &ns);
        api.patch(&name, &PatchParams::apply("kubo-test"), &Patch::Apply(&obj))
            .await
            .expect("World::create failed")
    }

    /// Delete a typed resource via HTTP, triggering watch broadcasts.
    ///
    /// Uses default delete options (no grace period override).  If the object
    /// has finalizers the fake will tombstone it rather than remove it.
    pub async fn delete<T: EnvResource>(&self, ns: &str, name: &str) {
        let api: Api<T> = Api::namespaced(self.client.clone(), ns);
        api.delete(name, &Default::default())
            .await
            .expect("World::delete failed");
    }

    // ── Time-control methods ──────────────────────────────────────────────────

    /// Yield once, giving other tasks (controller watcher loops, reconcile
    /// tasks) one turn.
    ///
    /// A single call is usually not enough for a full reconcile cycle; prefer
    /// [`run_until_idle`](Self::run_until_idle) for that.
    pub async fn step(&self) {
        tokio::task::yield_now().await;
    }

    /// Yield a fixed number of times (200), giving spawned tasks a chance to run.
    ///
    /// Each `yield_now()` gives the tokio executor one turn to run spawned tasks
    /// (controller watcher loops, reconcile tasks).  The method returns after
    /// the fixed budget regardless of whether work remains: pipelines needing
    /// more cooperative turns are not fully drained, and a task stuck waiting
    /// on non-timer I/O does not settle — the method returns anyway.
    ///
    /// Parking detection ([`forbid_parking`](Self::forbid_parking)) is not
    /// implemented: the flag is recorded but never consulted.
    pub async fn run_until_idle(&self) {
        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
    }

    /// Advance paused tokio time by `duration`, then call
    /// [`run_until_idle`](Self::run_until_idle).
    ///
    /// This wakes any timers (requeue sleeps, backoff sleeps) whose deadline
    /// falls within the advanced window.  Requires the enclosing test to use
    /// paused time:
    ///
    /// ```rust,no_run
    /// #[tokio::test(start_paused = true)]
    /// async fn my_test() { /* … */ }
    /// ```
    ///
    /// Panics at runtime if called without paused time (tokio's own panic).
    pub async fn advance(&self, duration: Duration) {
        tokio::time::advance(duration).await;
        self.fake.flush_pending_watch_events();
        self.run_until_idle().await;
    }

    /// Advance paused tokio time by `total`, divided into chunks of `step`,
    /// calling [`run_until_idle`](Self::run_until_idle) after each chunk.
    ///
    /// A single [`advance`](Self::advance) call only fires timers that already
    /// exist at the moment it is called.  After a reconcile error the
    /// controller sets a *new* requeue timer — that timer falls outside the
    /// original window.  Advancing in steps that are slightly larger than the
    /// controller's error-requeue interval ensures every recovery attempt gets
    /// its own tick window:
    ///
    /// ```rust,no_run
    /// # use std::time::Duration;
    /// # async fn example(world: &kubo::testing::World) {
    /// // Error requeue = 5 s; step = 6 s to ensure each retry fires.
    /// // 20 × 6 s = 120 virtual seconds.
    /// world.advance_stepping(Duration::from_secs(120), Duration::from_secs(6)).await;
    /// # }
    /// ```
    ///
    /// If `total` is not exactly divisible by `step`, the final chunk covers
    /// the remainder.  If `step` is zero this is a no-op.
    pub async fn advance_stepping(&self, total: Duration, step: Duration) {
        let step_ns = step.as_nanos();
        if step_ns == 0 {
            return;
        }
        let total_ns = total.as_nanos();
        let full_steps = total_ns / step_ns;
        for _ in 0..full_steps {
            self.advance(step).await;
        }
        let remainder_ns = total_ns % step_ns;
        if remainder_ns > 0 {
            self.advance(Duration::from_nanos(remainder_ns as u64))
                .await;
        }
    }

    /// Not implemented: the body performs a single cooperative yield.
    ///
    /// Use [`advance`](Self::advance) with an explicit `Duration` instead.
    pub async fn advance_to_next_timer(&self) {
        // Introspecting the timer queue requires tokio internals not yet
        // exposed.  Use advance(Duration) until this is implemented.
        tokio::task::yield_now().await;
    }

    /// Enable or disable watch event fuzzing on this world's fake API server.
    ///
    /// When enabled, watch events generated by mutable operations (`create`,
    /// `delete`, status patches, finalizer patches, and direct inserts via
    /// [`prefill`](Self::prefill) or [`seed_obj`](Self::seed_obj)) are held
    /// in a per-tick buffer rather than broadcast immediately.  During each
    /// [`advance`](Self::advance) call, after the clock moves and before
    /// [`run_until_idle`](Self::run_until_idle), the buffer is shuffled using
    /// the fake's seeded RNG and broadcast to all watch subscribers.  Events
    /// buffered while fuzzing was enabled are broadcast unshuffled if
    /// fuzzing is disabled before the next [`advance`](Self::advance).
    ///
    /// Shuffling simulates out-of-order event delivery across multiple objects,
    /// exposing assumptions like "ADDED for obj-A always arrives before ADDED
    /// for obj-B".  A correct controller must converge regardless of event
    /// ordering.  The ordering depends on the seed.
    ///
    /// Disabled by default.  Enable before any [`create`](Self::create) calls
    /// whose events you want shuffled.
    pub fn fuzz_watch_events(&self, enabled: bool) {
        self.fake.set_fuzz_watch_events(enabled);
    }

    /// Record the parking policy "forbidden" on the `World` (the initial
    /// value set by [`WorldBuilder::build`]).
    ///
    /// Detection is not implemented: the flag is recorded but currently has
    /// no effect.
    pub fn forbid_parking(&mut self) {
        self.forbid_parking = true;
    }

    /// Record the parking policy "allowed" on the `World`.
    ///
    /// Detection is not implemented: the flag is recorded but currently has
    /// no effect.
    pub fn allow_parking(&mut self) {
        self.forbid_parking = false;
    }

    /// Direct access to the fake server, for `insert`/`get_raw` in helpers.
    #[allow(dead_code)]
    pub(crate) fn fake(&self) -> &FakeApiServer {
        &self.fake
    }

    /// Register a [`Listener`] on an already-built `World`.
    ///
    /// Spawns the listener as a background tokio task immediately, wired to the
    /// same [`FakeApiServer`] as this world.  Equivalent to calling
    /// [`WorldBuilder::listener`] before [`WorldBuilder::build`], but usable
    /// after construction — which is what `#[kubo::test]` needs when a test
    /// registers its listener inside the function body rather than ahead of
    /// time.
    ///
    /// Use [`seed_listener_with_extensions`](Self::seed_listener_with_extensions)
    /// when the listener's reconcile pipeline pulls values from
    /// [`ctx.require`](crate::extensions::Extensions::require) — e.g. an HTTP
    /// client or a token cache.
    pub fn seed_listener<R, L>(&self, listener: L)
    where
        R: EnvResource + HasConditions + Debug + 'static,
        L: Listener<R> + 'static,
    {
        self.seed_listener_with_extensions::<R, L>(listener, Extensions::new());
    }

    /// Like [`seed_listener`](Self::seed_listener) but seeds the
    /// [`ControllerContext`] extensions with caller-supplied values before
    /// spawning the controller loop.
    ///
    /// The `kube::Client` is always inserted automatically, replacing any
    /// caller-supplied `kube::Client` (`Extensions::insert` replaces values of
    /// the same type), so there is no need to add it manually.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut ext = Extensions::new();
    /// ext.insert(reqwest::Client::default());
    /// ext.insert(Arc::new(MyTokenCache::default()));
    /// world.seed_listener_with_extensions::<MyResource, _>(MyListener::default(), ext);
    /// ```
    pub fn seed_listener_with_extensions<R, L>(&self, listener: L, extensions: Extensions)
    where
        R: EnvResource + HasConditions + Debug + 'static,
        L: Listener<R> + 'static,
    {
        let handle = spawn_listener_task::<R, L>(self.client.clone(), listener, extensions);
        self.listener_abort_handles.lock().unwrap().push(handle);
    }

    /// Register a fault rule with the fake API server.
    ///
    /// Returns a [`FaultBuilder`] — call one of its trigger methods to
    /// activate the rule:
    ///
    /// ```ignore
    /// world.fault(Fault::Conflict).at_random_rate(0.10);
    /// world.fault(Fault::Internal).next_n(3);
    /// world.fault(Fault::Throttled).once();
    /// ```
    ///
    /// Faults are injected on mutable operations (PATCH / DELETE) only.  The
    /// probabilistic variant ([`FaultBuilder::at_random_rate`]) is seeded from
    /// the world's RNG so results are reproducible from a single `SEED` value.
    pub fn fault(&self, fault: Fault) -> FaultBuilder {
        self.fake.fault_builder(fault)
    }

    /// List all objects of type `T` currently in the store as [`ObjRef`]s.
    ///
    /// Includes tombstoned objects (those with `deletionTimestamp` set) — use
    /// [`is_deleted`](Self::is_deleted) to distinguish live from pending-delete.
    pub fn list_refs<T: EnvResource>(&self) -> Vec<ObjRef> {
        let group = T::group(&()).into_owned();
        let version = T::version(&()).into_owned();
        let resource = T::plural(&()).into_owned();
        self.fake
            .list_refs(&group, &version, &resource, None)
            .into_iter()
            .map(|(ns, name)| ObjRef::new(ns, name))
            .collect()
    }

    /// Returns `true` if the object has a `deletionTimestamp` — i.e., it has
    /// been deleted but still has finalizers preventing removal from the store.
    ///
    /// Returns `false` if the object is live, or if it has been fully removed
    /// from the store (in which case [`list_refs`](Self::list_refs) would not
    /// have included it).
    pub fn is_deleted<T: EnvResource>(&self, obj_ref: &ObjRef) -> bool {
        let group = T::group(&()).into_owned();
        let version = T::version(&()).into_owned();
        let resource = T::plural(&()).into_owned();
        match self.fake.get_raw(
            &group,
            &version,
            &resource,
            Some(&obj_ref.namespace),
            &obj_ref.name,
        ) {
            Some(v) => v
                .get("metadata")
                .and_then(|m| m.get("deletionTimestamp"))
                .is_some(),
            None => false,
        }
    }

    /// Status tick history for the object identified by `obj_ref`.
    ///
    /// Equivalent to calling [`status_ticks`](Self::status_ticks) with the
    /// namespace and name extracted from the ref.  Use this in property tests
    /// where you hold an [`ObjRef`] from [`list_refs`](Self::list_refs).
    pub fn status_ticks_for<S: serde::de::DeserializeOwned>(&self, obj_ref: &ObjRef) -> Vec<S> {
        self.status_ticks(&obj_ref.namespace, &obj_ref.name)
    }

    /// Insert an object into the fake cluster and return its [`ObjRef`].
    ///
    /// This is the idiomatic way to seed objects for direct-reconcile property
    /// tests: insert the object, then call [`reconcile`](Self::reconcile) on
    /// the returned ref without needing a controller loop.
    ///
    /// Unlike [`create`](Self::create), this bypasses HTTP: there is no SSA
    /// field-manager registration and no fault injection, but the store's
    /// normal mutation path still emits an `ADDED`/`MODIFIED` watch event
    /// (broadcast immediately, or buffered until the next
    /// [`advance`](Self::advance) when watch fuzzing is enabled). Use it
    /// when the test drives reconciles explicitly rather than relying on
    /// the controller watcher.
    pub fn seed_obj<T: EnvResource>(&self, obj: T) -> ObjRef {
        let ns = obj
            .meta()
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());
        let name = obj.meta().name.clone().unwrap_or_default();
        self.prefill(obj);
        ObjRef::new(ns, name)
    }

    /// Invoke a listener's reconcile pipeline directly on a stored object.
    ///
    /// This is the **direct mode** — no kube-rs controller loop, no watcher,
    /// no Tower stack. One async call executes the full reconcile pipeline
    /// (finalizer management + status patching) against the fake API server
    /// and returns the resulting [`Action`].
    ///
    /// Direct mode avoids the controller loop and watcher, so property tests
    /// can run many cases quickly. Use it for Layer 2 property tests that
    /// exercise reconcile logic correctness across generated inputs.
    ///
    /// The listener is instantiated via `Default` so it must be stateless (or
    /// carry only configuration injected through the `ControllerContext`
    /// extensions, which this method does not populate beyond the kube client).
    ///
    /// # Panics
    ///
    /// Panics if the object identified by `obj_ref` does not exist in the
    /// store. Seed it first with [`seed_obj`](Self::seed_obj) or
    /// [`create`](Self::create).
    pub async fn reconcile<L>(&self, obj_ref: &ObjRef) -> Result<Action, ReconcileError>
    where
        L: Listener<<L as ListenerResource>::Resource> + Default + 'static,
        L: ListenerResource,
    {
        let obj: <L as ListenerResource>::Resource = self
            .get::<<L as ListenerResource>::Resource>(&obj_ref.namespace, &obj_ref.name)
            .unwrap_or_else(|| {
                panic!("World::reconcile: object {obj_ref} not found in store — seed it first")
            });

        let obj = Arc::new(obj);
        let ops = Arc::new(KubeHandle::new(self.client.clone(), L::NAME));
        let mut extensions = Extensions::new();
        extensions.insert(self.client.clone());
        let extensions = Arc::new(extensions);
        let ctx = Arc::new(ControllerContext::new(ops, extensions));

        let listener = L::default();
        let rec = Reconcile::new(ctx.ops(), obj.clone());
        listener
            .reconcile(&ctx, obj, rec)
            .execute(L::FINALIZER)
            .await
    }

    /// Apply a single [`Op`] against the fake cluster, triggering watch events.
    ///
    /// `Create` uses `PatchParams::apply("kubo-test")` (SSA field manager).
    /// `Delete` ignores all errors (including 404) so that delete-before-create
    /// sequences do not abort the test.
    pub async fn apply_op<T>(&self, op: Op<T>)
    where
        T: EnvResource,
    {
        match op {
            Op::Create(obj) => {
                self.create(obj).await;
            }
            Op::Delete { namespace, name } => {
                let api: Api<T> = Api::namespaced(self.client.clone(), &namespace);
                // Silently ignore any error (object may not exist yet).
                let _ = api.delete(&name, &Default::default()).await;
            }
        }
    }
}

// ── spawn_listener_task ──────────────────────────────────────────────────────

/// Spawn a listener controller as a background tokio task on the given client.
///
/// Returns the task's [`AbortHandle`] so the caller can cancel it when the
/// associated [`World`] is dropped, preventing stale listener tasks from
/// accumulating in the scheduler across property-test cases.
///
/// This is the shared implementation used by both [`WorldBuilder::listener`]
/// (deferred until `build()`) and [`World::seed_listener`] (immediate).
fn spawn_listener_task<R, L>(
    client: kube::Client,
    listener: L,
    mut extensions: Extensions,
) -> tokio::task::AbortHandle
where
    R: EnvResource + HasConditions + Debug + 'static,
    L: Listener<R> + 'static,
{
    let listener = Arc::new(listener);
    tokio::spawn(async move {
        // Build a KubeHandle and populate extensions.  The kube::Client is
        // always inserted so listeners can call ctx.require::<kube::Client>();
        // any caller-supplied entries are preserved underneath it.
        let ops = Arc::new(KubeHandle::new(client.clone(), L::NAME));
        extensions.insert(client.clone());
        let extensions = Arc::new(extensions);
        let ctrl_ctx = Arc::new(ControllerContext::new(ops, extensions));

        // Wire up the kube-rs Controller watcher for this resource kind.
        let api = Api::<R>::all(client);
        let base_ctrl = Controller::new(api, WatcherConfig::default());
        let controller = listener.controller(&ctrl_ctx, base_ctrl);

        controller
            .run(
                {
                    let listener = Arc::clone(&listener);
                    move |obj: Arc<R>, ctx: Arc<ControllerContext>| {
                        let listener = Arc::clone(&listener);
                        async move {
                            let rec = Reconcile::new(ctx.ops(), obj.clone());
                            listener
                                .reconcile(&ctx, obj, rec)
                                .execute(L::FINALIZER)
                                .await
                        }
                    }
                },
                |_obj, _err, _ctx: Arc<ControllerContext>| Action::requeue(Duration::from_secs(5)),
                ctrl_ctx,
            )
            .for_each(|res| async move {
                if let Err(e) = res {
                    eprintln!("[kubo-test] reconcile error: {e:?}");
                }
            })
            .await;
    })
    .abort_handle()
}

// ── WorldBuilder ─────────────────────────────────────────────────────────────

/// Fluent builder for [`World`].
pub struct WorldBuilder {
    listener_fns: Vec<ListenerFn>,
    seed: Option<u64>,
}

impl WorldBuilder {
    /// Creates an empty builder.
    pub fn new() -> Self {
        Self {
            listener_fns: Vec::new(),
            seed: None,
        }
    }

    /// Register a CRD descriptor with the discovery cache.
    ///
    /// Currently a no-op: the descriptor is discarded.
    pub fn install_crd<C>(self, _crd: C) -> Self {
        self
    }

    /// Pin this world's seed, overriding the `SEED` environment variable.
    ///
    /// Useful when you want a specific world to have a known seed
    /// independently of the env var (e.g. in a multi-world test where different
    /// worlds need different seeds).
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    /// Register a [`Listener`] to run as a background controller task.
    ///
    /// The listener is spawned via `tokio::spawn` inside [`build`](Self::build).
    /// It runs on the same tokio runtime as the test — including under paused
    /// time — so reconciles are driven cooperatively by
    /// [`World::run_until_idle`].
    ///
    /// Use [`listener_with_extensions`](Self::listener_with_extensions) when
    /// the listener pulls values from `ctx.require`.
    pub fn listener<R, L>(self, listener: L) -> Self
    where
        R: EnvResource + HasConditions + Debug + 'static,
        L: Listener<R> + 'static,
    {
        self.listener_with_extensions::<R, L>(listener, Extensions::new())
    }

    /// Like [`listener`](Self::listener) but seeds the [`ControllerContext`]
    /// extensions with caller-supplied values.
    ///
    /// The `kube::Client` is always inserted automatically.
    pub fn listener_with_extensions<R, L>(mut self, listener: L, extensions: Extensions) -> Self
    where
        R: EnvResource + HasConditions + Debug + 'static,
        L: Listener<R> + 'static,
    {
        let factory: ListenerFn = Box::new(move |client: kube::Client| {
            spawn_listener_task::<R, L>(client, listener, extensions)
        });
        self.listener_fns.push(factory);
        self
    }

    /// Build the `World`.
    ///
    /// Starts the [`FakeApiServer`], constructs a [`kube::Client`] backed by
    /// it, and spawns each registered listener as a background tokio task.
    /// The default namespace is `"default"`.
    pub async fn build(self) -> World {
        let seed = self.seed.unwrap_or_else(|| {
            std::env::var("SEED")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0)
        });
        let fake = FakeApiServer::new_seeded(seed);
        let client = kube::Client::new(fake.clone(), "default");

        let abort_handles: Vec<tokio::task::AbortHandle> = self
            .listener_fns
            .into_iter()
            .map(|f| f(client.clone()))
            .collect();

        World {
            fake,
            client,
            forbid_parking: true,
            seed,
            listener_abort_handles: std::sync::Mutex::new(abort_handles),
        }
    }
}

impl Default for WorldBuilder {
    fn default() -> Self {
        Self::new()
    }
}

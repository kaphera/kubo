//! Fake Kubernetes API server for testing.
//!
//! [`FakeApiServer`] is a [`tower::Service`] that implements a minimal slice
//! of the Kubernetes HTTP API protocol against an in-memory [`Store`].  Pass
//! it directly to `kube::Client::new` to get a real kube-rs client that talks
//! to the fake instead of a cluster.
//!
//! ## Supported operations
//!
//! - `GET`  namespaced and cluster-scoped objects (200 / 404)
//! - `GET`  collection (list) — returns all matching objects as a Kubernetes List
//! - `GET?watch=1` collection (watch) — upgrades to a streaming NDJSON response
//!   backed by a per-GVK `tokio::sync::broadcast` channel; future mutations are
//!   forwarded as `ADDED` / `MODIFIED` / `DELETED` events
//! - `PATCH` with `application/apply-patch+yaml` — server-side apply:
//!   per-field ownership tracking with 409 Conflict detection, deep merge with
//!   RFC 7396 semantics; creates the object when absent
//! - `PATCH` with `application/merge-patch+json` — RFC 7396 merge patch
//! - `PATCH` (object path) with any other content type — full-replace
//!   upsert (`.../status` ignores the content type)
//! - `PATCH .../status` — isolated status update + history recording
//! - `DELETE` — finalizer-aware

use std::{
    collections::HashMap,
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body::{Body as HttpBody, Frame};
use http_body_util::{BodyExt, Full};
use rand::SeedableRng;
use serde_json::Value;
use tokio::sync::{broadcast, mpsc};
use tower::Service;

// ── Types ─────────────────────────────────────────────────────────────────────

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Ring-buffer capacity for per-GVK broadcast channels.
///
/// Events beyond this count are dropped: lagged broadcast receivers skip the
/// missed events, and the fake does not signal the gap.
const WATCH_CAPACITY: usize = 256;

// ── WatchEventRaw ─────────────────────────────────────────────────────────────

/// A raw Kubernetes watch event, broadcast on every store mutation (buffered
/// when fuzzing is enabled).
///
/// The per-GVK watch channel carries these; the forwarder task serialises them
/// as newline-delimited JSON into the streaming watch response body.
#[derive(Clone, Debug)]
struct WatchEventRaw {
    /// `"ADDED"`, `"MODIFIED"`, or `"DELETED"`.
    event_type: &'static str,
    object: Value,
}

// ── ResponseBody ──────────────────────────────────────────────────────────────

/// Unified HTTP response body for the fake API server.
///
/// Regular responses use `Full` (a completed in-memory payload).
/// Watch responses use `Watch` (a streaming channel-backed body that yields
/// newline-delimited JSON watch events over time).
pub enum ResponseBody {
    /// A completed in-memory payload, used for regular (non-watch) responses.
    Full(Full<Bytes>),
    /// A streaming body yielding newline-delimited JSON watch events, used
    /// for watch responses.
    Watch(WatchBody),
}

/// Streaming body backed by a [`mpsc::Receiver`].
///
/// Produced by the watch event forwarder task; consumed by the kube-rs HTTP
/// client as it reads the long-lived WATCH response.
pub struct WatchBody {
    rx: mpsc::Receiver<Bytes>,
}

impl HttpBody for ResponseBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.get_mut() {
            ResponseBody::Full(full) => Pin::new(full)
                .poll_frame(cx)
                .map(|opt| opt.map(|r| r.map_err(|e: Infallible| match e {}))),

            ResponseBody::Watch(watch) => match watch.rx.poll_recv(cx) {
                Poll::Ready(Some(bytes)) => Poll::Ready(Some(Ok(Frame::data(bytes)))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

// ── StatusRecord ──────────────────────────────────────────────────────────────

/// A recorded status patch for a specific object.
///
/// Accumulated whenever a `PATCH .../status` request is received.  Used by
/// `World::status_ticks` to assert on the status progression of a controller.
#[derive(Clone, Debug)]
pub struct StatusRecord {
    /// Namespace of the object whose status was patched; empty for
    /// cluster-scoped objects.
    pub namespace: String,
    /// Name of the object whose status was patched.
    pub name: String,
    /// The raw `status` value extracted from the patch body.
    pub status: Value,
}

// ── Store ─────────────────────────────────────────────────────────────────────

/// The key that uniquely identifies a Kubernetes object in the store.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StoreKey {
    group: String,
    version: String,
    resource: String,
    namespace: Option<String>,
    name: String,
}

/// Key for per-GVK broadcast channels: `(group, version, resource)`.
type WatchKey = (String, String, String);

// ── Fault injection ───────────────────────────────────────────────────────────

/// A failure to inject at the fake API server boundary.
///
/// Probabilistic faults use the fake's seeded RNG (seeded from the world's
/// seed) so every choice is deterministic and reproducible from a single
/// `u64` seed.
#[derive(Clone, Debug)]
pub enum Fault {
    /// Returns HTTP 409 Conflict — simulates concurrent SSA conflicts.
    Conflict,
    /// Returns HTTP 404 Not Found — simulates a race where the object was deleted.
    NotFound,
    /// Returns HTTP 500 Internal Server Error — simulates transient server faults.
    Internal,
    /// Returns HTTP 429 Too Many Requests — simulates API rate limiting.
    Throttled,
    /// Delays the response by `d` — simulates network latency.
    Latency(Duration),
    /// Fails the request before a response is produced — simulates a network
    /// partition.
    Disconnect,
}

/// When a fault fires and how many times it applies.
#[derive(Clone, Copy)]
enum FaultTrigger {
    /// Fire on every matching request with probability `p`. Uses the seeded RNG.
    RandomRate(f64),
    /// Fire exactly once, then remove the rule.
    Once,
    /// Fire for the next `n` matching requests.
    NextN(u64),
    /// Fire on every matching request indefinitely.
    Always,
}

struct FaultRule {
    fault: Fault,
    trigger: FaultTrigger,
}

// ── Disconnect error ──────────────────────────────────────────────────────────

#[derive(Debug)]
struct FaultDisconnectError;

impl std::fmt::Display for FaultDisconnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "injected Disconnect fault: connection closed mid-response"
        )
    }
}

impl std::error::Error for FaultDisconnectError {}

// ── SSA conflict ──────────────────────────────────────────────────────────────

/// Returned when a Server-Side Apply operation would conflict.
///
/// Produced by [`Store::ssa_apply`] when `force = false` and the patch
/// contains a field already owned by a different field manager.  The fake
/// serialises this into a Kubernetes `Status` 409 response body.
#[derive(Debug)]
pub(crate) struct SsaConflict {
    /// Dot-separated path of the conflicting field (e.g. `"spec.field"`).
    pub field: String,
    /// Name of the manager that currently owns the field.
    pub owner: String,
}

/// The in-memory object store.
///
/// Every write that stores an object increments `resource_version` and
/// injects it into `metadata.resourceVersion`; a delete that removes the
/// object does not.
///
/// Each write also emits a `WatchEventRaw` to the per-GVK channel —
/// broadcast immediately, or buffered when watch fuzzing is enabled — so
/// any active watch streams see the mutation.
pub struct Store {
    objects: HashMap<StoreKey, Value>,
    resource_version: u64,
    status_history: Vec<StatusRecord>,
    /// Per-GVK broadcast channels, lazily created on first `subscribe()` call.
    watch_senders: HashMap<WatchKey, broadcast::Sender<WatchEventRaw>>,
    /// Seeded RNG for the fake's randomised choices.
    ///
    /// Seeded from [`FakeApiServer::new_seeded`].  It has exactly two uses —
    /// the `RandomRate` fault draw and the pending-event shuffle in
    /// [`flush_pending_watch_events`](Self::flush_pending_watch_events) — so
    /// every randomised choice is reproducible from a single [`u64`] seed.
    rng: rand::rngs::StdRng,
    /// Active fault rules consulted before every mutable request (PATCH / DELETE).
    fault_rules: Vec<FaultRule>,
    /// Pending watch events buffered for deterministic fuzz delivery.
    ///
    /// When [`fuzz_watch_events`](Self::fuzz_watch_events) is `true`, mutation
    /// methods push here instead of broadcasting immediately.
    /// [`flush_pending_watch_events`](Self::flush_pending_watch_events) —
    /// called by `World::advance` after the clock moves and before
    /// `run_until_idle` yields to spawned tasks — shuffles (using the seeded
    /// RNG, when fuzzing is still enabled) and broadcasts.
    pending_events: Vec<(String, String, String, WatchEventRaw)>,
    /// When `true`, mutations buffer their events instead of broadcasting them.
    fuzz_watch_events: bool,
    /// Per-object field ownership: object key → (field path → manager name).
    ///
    /// Built up by [`ssa_apply`](Self::ssa_apply).  Consulted on every
    /// non-force `application/apply-patch+yaml` request to detect conflicts
    /// between field managers (a force apply skips the conflict check and
    /// steals ownership).  Stored separately from the object JSON so that the
    /// conflict-detection logic does not have to parse `managedFields`.
    field_ownership: HashMap<StoreKey, HashMap<String, String>>,
}

impl Store {
    fn new_seeded(seed: u64) -> Self {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[..8].copy_from_slice(&seed.to_le_bytes());
        Self {
            objects: HashMap::new(),
            resource_version: 0,
            status_history: Vec::new(),
            watch_senders: HashMap::new(),
            rng: rand::rngs::StdRng::from_seed(seed_bytes),
            fault_rules: Vec::new(),
            pending_events: Vec::new(),
            fuzz_watch_events: false,
            field_ownership: HashMap::new(),
        }
    }

    // ── Watch support ──────────────────────────────────────────────────────────

    /// Subscribe to future watch events for a given GVK.
    ///
    /// Creates the broadcast channel lazily if this is the first subscriber.
    /// Returns a receiver that will be notified of all future mutations.
    fn subscribe(
        &mut self,
        group: &str,
        version: &str,
        resource: &str,
    ) -> broadcast::Receiver<WatchEventRaw> {
        let key: WatchKey = (group.to_string(), version.to_string(), resource.to_string());
        self.watch_senders
            .entry(key)
            .or_insert_with(|| broadcast::channel(WATCH_CAPACITY).0)
            .subscribe()
    }

    /// Broadcast a watch event to all current subscribers for a GVK.
    ///
    /// If no one is subscribed (no sender exists), the call is a no-op.
    fn broadcast(
        &self,
        group: &str,
        version: &str,
        resource: &str,
        event_type: &'static str,
        object: Value,
    ) {
        let key: WatchKey = (group.to_string(), version.to_string(), resource.to_string());
        if let Some(tx) = self.watch_senders.get(&key) {
            let _ = tx.send(WatchEventRaw { event_type, object });
        }
    }

    /// Route an event to the immediate broadcast channel or to the pending
    /// buffer, depending on whether watch fuzzing is active.
    ///
    /// Called by every mutation method instead of [`broadcast`](Self::broadcast)
    /// directly.  When fuzzing is off the behaviour is identical to a direct
    /// broadcast.  When fuzzing is on, events accumulate until
    /// [`flush_pending_watch_events`](Self::flush_pending_watch_events) is
    /// called by `World::advance`, after the clock moves and before
    /// `run_until_idle` yields to spawned tasks.
    fn queue_or_broadcast(
        &mut self,
        group: &str,
        version: &str,
        resource: &str,
        event_type: &'static str,
        object: Value,
    ) {
        if self.fuzz_watch_events {
            self.pending_events.push((
                group.to_string(),
                version.to_string(),
                resource.to_string(),
                WatchEventRaw { event_type, object },
            ));
        } else {
            self.broadcast(group, version, resource, event_type, object);
        }
    }

    /// Shuffle the pending event buffer (when fuzzing is still enabled at
    /// flush time) and broadcast all queued events to their respective GVK
    /// channels.
    ///
    /// Called by `World::advance` during each advance call, after the clock
    /// moves and before `run_until_idle` yields to spawned tasks.  The
    /// shuffle uses the seeded RNG so results are reproducible from a single
    /// seed value.
    fn flush_pending_watch_events(&mut self) {
        if self.pending_events.is_empty() {
            return;
        }
        if self.fuzz_watch_events {
            use rand::seq::SliceRandom;
            self.pending_events.shuffle(&mut self.rng);
        }
        let events = std::mem::take(&mut self.pending_events);
        for (group, version, resource, event) in events {
            self.broadcast(&group, &version, &resource, event.event_type, event.object);
        }
    }

    // ── SSA apply ──────────────────────────────────────────────────────────────

    /// Apply a Server-Side Apply patch, tracking per-field ownership.
    ///
    /// Implements the minimal SSA semantics required for conformance testing:
    ///
    /// 1. Extract leaf field paths from the patch (excluding `metadata`,
    ///    `apiVersion`, `kind`, and `status` at the root level).
    /// 2. Check each path against the existing ownership map.  If any path is
    ///    owned by a *different* manager and `force = false`, return
    ///    [`SsaConflict`] without modifying the store.
    /// 3. Deep-merge the patch onto the stored object (RFC 7396 semantics)
    ///    and upsert the result.
    /// 4. Update the ownership map: every extracted path is now owned by
    ///    `manager` (stealing from any prior owner when `force = true`).
    pub(crate) fn ssa_apply(
        &mut self,
        key: StoreKey,
        manager: &str,
        patch: Value,
        force: bool,
    ) -> Result<Value, SsaConflict> {
        let paths = extract_field_paths(&patch, "");

        if !force && let Some(ownership) = self.field_ownership.get(&key) {
            for path in &paths {
                if let Some(owner) = ownership.get(path)
                    && owner != manager
                {
                    return Err(SsaConflict {
                        field: path.clone(),
                        owner: owner.clone(),
                    });
                }
            }
        }

        // Capture the pre-merge state so the generation bump can decide whether
        // `.spec` actually changed.  The real apiserver increments
        // `metadata.generation` only on spec-changing writes: a create starts at
        // 1, a subsequent apply that mutates `.spec` increments, and an
        // idempotent re-apply (or a metadata-only apply) does not.  Status
        // patches and finalizer merge-patches go through other entry points and
        // never bump generation.  Modelling this here is what lets the framework
        // copy `metadata.generation` into `status.observedGeneration` and have it
        // compare equivalent fake-vs-real.
        let is_new = !self.objects.contains_key(&key);
        let old_spec = self.objects.get(&key).and_then(|o| o.get("spec")).cloned();

        // Merge patch onto existing object (or start from an empty object).
        let mut obj = self
            .objects
            .get(&key)
            .cloned()
            .unwrap_or_else(|| Value::Object(Default::default()));
        do_merge_patch(&mut obj, &patch);

        let new_spec = obj.get("spec").cloned();
        if is_new || old_spec != new_spec {
            let next_gen = if is_new {
                1u64
            } else {
                self.objects
                    .get(&key)
                    .and_then(|o| o.get("metadata"))
                    .and_then(|m| m.get("generation"))
                    .and_then(Value::as_u64)
                    .map(|g| g + 1)
                    .unwrap_or(1)
            };
            if let Some(meta) = obj.get_mut("metadata").and_then(Value::as_object_mut) {
                meta.insert("generation".to_string(), Value::from(next_gen));
            }
        }

        // Update field ownership after a successful merge.
        let ownership = self.field_ownership.entry(key.clone()).or_default();
        for path in paths {
            ownership.insert(path, manager.to_string());
        }

        Ok(self.upsert(key, obj))
    }

    // ── Object mutations ───────────────────────────────────────────────────────

    /// Insert or replace an object (full replace semantics).
    ///
    /// Increments the resource version, injects it into
    /// `metadata.resourceVersion`, stores, and returns the stored value.
    /// Broadcasts `ADDED` or `MODIFIED` depending on whether the key was new
    /// (buffered when fuzzing is enabled).
    fn upsert(&mut self, key: StoreKey, mut obj: Value) -> Value {
        let is_new = !self.objects.contains_key(&key);
        self.resource_version += 1;
        let rv = self.resource_version.to_string();
        if let Some(meta) = obj.get_mut("metadata").and_then(Value::as_object_mut) {
            meta.insert("resourceVersion".to_string(), Value::String(rv));
        }
        self.objects.insert(key.clone(), obj.clone());
        let event_type = if is_new { "ADDED" } else { "MODIFIED" };
        self.queue_or_broadcast(
            &key.group,
            &key.version,
            &key.resource,
            event_type,
            obj.clone(),
        );
        obj
    }

    fn get(&self, key: &StoreKey) -> Option<&Value> {
        self.objects.get(key)
    }

    /// Return all objects matching `(group, version, resource)`, optionally
    /// filtered to a single `namespace`.
    fn list(
        &self,
        group: &str,
        version: &str,
        resource: &str,
        namespace: Option<&str>,
    ) -> Vec<Value> {
        self.objects
            .iter()
            .filter(|(k, _)| {
                k.group == group
                    && k.version == version
                    && k.resource == resource
                    && namespace.is_none_or(|ns| k.namespace.as_deref() == Some(ns))
            })
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// Apply an RFC 7396 JSON merge patch to an existing object.
    ///
    /// Returns the updated object, or `None` if the key was not found.
    /// Broadcasts `MODIFIED` (buffered when fuzzing is enabled) — unless the
    /// merged object is terminating (`metadata.deletionTimestamp` set) with
    /// no remaining finalizers, in which case
    /// the object is removed from the store and `DELETED` is broadcast
    /// (likewise buffered when fuzzing is enabled) while the merged object is
    /// still returned (see [`is_terminating_with_no_finalizers`]).
    fn merge_patch(&mut self, key: &StoreKey, patch: Value) -> Option<Value> {
        if !self.objects.contains_key(key) {
            return None;
        }
        self.resource_version += 1;
        let rv = self.resource_version.to_string();

        let obj = self.objects.get_mut(key).unwrap();
        do_merge_patch(obj, &patch);

        if let Some(meta) = obj.get_mut("metadata").and_then(Value::as_object_mut) {
            meta.insert("resourceVersion".to_string(), Value::String(rv));
        }

        let stored = obj.clone();

        // Finalizer-triggered garbage collection.  In a real apiserver, once an
        // object has a `deletionTimestamp` and its finalizer list becomes empty,
        // the GC controller removes it asynchronously.  The patch response still
        // carries the updated object (finalizers empty, `deletionTimestamp` set),
        // but the object is gone from the store before the next request — so a
        // GET returns 404.  Model this synchronously: return the merged object
        // (matching the real patch response) but remove it from the store and
        // broadcast DELETED instead of MODIFIED.  Without this, the fake retains
        // a terminating object after its last finalizer is cleared, diverging
        // from real on the finalizer-teardown row (a terminating object whose
        // last finalizer is cleared must disappear from the store, not linger).
        if is_terminating_with_no_finalizers(&stored) {
            self.objects.remove(key);
            self.queue_or_broadcast(
                &key.group,
                &key.version,
                &key.resource,
                "DELETED",
                stored.clone(),
            );
        } else {
            self.queue_or_broadcast(
                &key.group,
                &key.version,
                &key.resource,
                "MODIFIED",
                stored.clone(),
            );
        }
        Some(stored)
    }

    /// Replace the `status` field of an existing object, then record the patch.
    ///
    /// Returns the updated full object, or `None` if the key was not found.
    /// Broadcasts `MODIFIED` (buffered when fuzzing is enabled).  The
    /// store-wide resourceVersion bump and `metadata.resourceVersion`
    /// injection still apply.  A request body without a `status` key empties
    /// the status (replaces it with an empty object).
    fn status_patch(&mut self, key: &StoreKey, status: Value) -> Option<Value> {
        if !self.objects.contains_key(key) {
            return None;
        }

        self.resource_version += 1;
        let rv = self.resource_version.to_string();

        let obj = self.objects.get_mut(key).unwrap();
        if let Some(o) = obj.as_object_mut() {
            o.insert("status".to_string(), status.clone());
        }
        if let Some(meta) = obj.get_mut("metadata").and_then(Value::as_object_mut) {
            meta.insert("resourceVersion".to_string(), Value::String(rv));
        }

        let stored = obj.clone();
        self.status_history.push(StatusRecord {
            namespace: key.namespace.clone().unwrap_or_default(),
            name: key.name.clone(),
            status,
        });
        self.queue_or_broadcast(
            &key.group,
            &key.version,
            &key.resource,
            "MODIFIED",
            stored.clone(),
        );
        Some(stored)
    }

    /// Finalizer-aware delete.
    ///
    /// If the object has non-empty `metadata.finalizers`, sets
    /// `metadata.deletionTimestamp` (stable sentinel) and keeps the object
    /// in the store, broadcasting `MODIFIED` (buffered when fuzzing is
    /// enabled).  Otherwise removes the object and broadcasts `DELETED`
    /// (likewise buffered when fuzzing is enabled).
    ///
    /// The retain path also bumps `metadata.generation`, but only when the
    /// object already carries a `generation` field — objects without one are
    /// left alone.  This is distinct from a merge-patch on
    /// `metadata.finalizers` (the `patch_finalizers` wire shape), which does
    /// *not* bump generation.  Modelling the bump here is what keeps the
    /// framework's `status.observedGeneration` — copied from
    /// `metadata.generation` at the next reconcile pass — equivalent
    /// fake-vs-real across a delete-then-cleanup lifecycle.
    ///
    /// Returns `None` if the key was not found.
    fn finalizer_delete(&mut self, key: &StoreKey) -> Option<Value> {
        let finalizer_count = self
            .objects
            .get(key)?
            .get("metadata")
            .and_then(|m| m.get("finalizers"))
            .and_then(|f| f.as_array())
            .map(|a| a.iter().filter(|v| !v.is_null()).count())
            .unwrap_or(0);

        if finalizer_count > 0 {
            self.resource_version += 1;
            let rv = self.resource_version.to_string();
            let obj = self.objects.get_mut(key).unwrap();
            if let Some(meta) = obj.get_mut("metadata").and_then(Value::as_object_mut) {
                meta.insert("resourceVersion".to_string(), Value::String(rv));
                meta.entry("deletionTimestamp")
                    .or_insert_with(|| Value::String("2024-01-01T00:00:00Z".to_string()));
                // Bump generation on the delete-retain update, as the real
                // apiserver does.  Resources that do not track generation (no
                // `generation` field set) are left alone — matching the real
                // apiserver, which only bumps for kinds that carry generation.
                if let Some(g) = meta.get("generation").and_then(Value::as_u64) {
                    meta.insert("generation".to_string(), Value::from(g + 1));
                }
            }
            let stored = obj.clone();
            self.queue_or_broadcast(
                &key.group,
                &key.version,
                &key.resource,
                "MODIFIED",
                stored.clone(),
            );
            Some(stored)
        } else {
            let removed = self.objects.remove(key)?;
            self.queue_or_broadcast(
                &key.group,
                &key.version,
                &key.resource,
                "DELETED",
                removed.clone(),
            );
            Some(removed)
        }
    }

    fn all_status_records(&self) -> Vec<StatusRecord> {
        self.status_history.clone()
    }

    fn status_records_for(&self, namespace: &str, name: &str) -> Vec<Value> {
        self.status_history
            .iter()
            .filter(|r| r.namespace == namespace && r.name == name)
            .map(|r| r.status.clone())
            .collect()
    }

    // ── Fault injection ────────────────────────────────────────────────────────

    /// Register a fault rule.  Rules are evaluated in insertion order.
    pub(crate) fn add_fault_rule(&mut self, rule: FaultRule) {
        self.fault_rules.push(rule);
    }

    /// Evaluate the active fault rules and return the first fault that fires,
    /// removing or decrementing the rule as appropriate.
    ///
    /// For [`FaultTrigger::RandomRate`] the seeded RNG is consulted so the
    /// decision is reproducible.  All other triggers are deterministic.
    pub(crate) fn check_fault(&mut self) -> Option<Fault> {
        use rand::Rng;
        for i in 0..self.fault_rules.len() {
            // Copy the trigger to avoid borrow conflicts with `self.rng`.
            let trigger_copy = self.fault_rules[i].trigger;
            let fires = match trigger_copy {
                FaultTrigger::RandomRate(p) => self.rng.random::<f64>() < p,
                FaultTrigger::Once => true,
                FaultTrigger::NextN(n) => n > 0,
                FaultTrigger::Always => true,
            };
            if !fires {
                continue;
            }
            let fault = self.fault_rules[i].fault.clone();
            let remove = match &mut self.fault_rules[i].trigger {
                FaultTrigger::Once => true,
                FaultTrigger::NextN(n) => {
                    *n -= 1;
                    *n == 0
                }
                _ => false,
            };
            if remove {
                self.fault_rules.remove(i);
            }
            return Some(fault);
        }
        None
    }
}

// ── FakeApiServer ─────────────────────────────────────────────────────────────

/// A fake Kubernetes API server backed by an in-memory [`Store`].
///
/// Implements `tower::Service<Request<B>>` for any body type `B` that
/// produces `bytes::Bytes`, so it can be passed directly to
/// `kube::Client::new` without coupling this crate to the kube body type.
///
/// The service is cheaply `Clone` — every clone shares the same underlying
/// `Arc<Mutex<Store>>`, so mutations made via the kube client are immediately
/// visible through [`FakeApiServer::get_raw`], and objects inserted via
/// [`FakeApiServer::insert`] are immediately visible to the kube client.
#[derive(Clone)]
pub struct FakeApiServer {
    store: Arc<Mutex<Store>>,
}

impl FakeApiServer {
    /// Create a new server with an empty store, seeded RNG initialised from `seed`.
    ///
    /// The seed controls the fake's two randomised choices — the `RandomRate`
    /// fault draw and the pending watch-event shuffle — so using the same
    /// seed always produces the same sequence of decisions.
    pub fn new_seeded(seed: u64) -> Self {
        Self {
            store: Arc::new(Mutex::new(Store::new_seeded(seed))),
        }
    }

    /// Create a new server with seed `0`.  Equivalent to `new_seeded(0)`.
    pub fn new() -> Self {
        Self::new_seeded(0)
    }

    /// Insert a raw JSON object directly into the store, bypassing HTTP.
    ///
    /// Used by `kubo::testing::World::prefill` (and, through it,
    /// `kubo::testing::World::seed_obj`) to pre-populate the cluster without
    /// going through the kube client.  `group` may be empty for core-group
    /// resources.
    pub fn insert(
        &self,
        group: &str,
        version: &str,
        resource: &str,
        namespace: Option<&str>,
        name: &str,
        obj: Value,
    ) {
        let key = StoreKey {
            group: group.to_string(),
            version: version.to_string(),
            resource: resource.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
        };
        self.store.lock().unwrap().upsert(key, obj);
    }

    /// Retrieve a raw JSON object directly from the store, bypassing HTTP.
    pub fn get_raw(
        &self,
        group: &str,
        version: &str,
        resource: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Option<Value> {
        let key = StoreKey {
            group: group.to_string(),
            version: version.to_string(),
            resource: resource.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
        };
        self.store.lock().unwrap().get(&key).cloned()
    }

    /// All recorded status patches, in the order they were received.
    pub fn status_history(&self) -> Vec<StatusRecord> {
        self.store.lock().unwrap().all_status_records()
    }

    /// Status patches for a specific object, in order.
    pub fn status_history_for(&self, namespace: &str, name: &str) -> Vec<Value> {
        self.store
            .lock()
            .unwrap()
            .status_records_for(namespace, name)
    }

    /// Return `(namespace, name)` pairs for every object whose GVR matches
    /// `(group, version, resource)`, optionally restricted to `namespace`.
    ///
    /// Used by `kubo::testing::World::list_refs` to enumerate all
    /// objects of a given type.
    pub fn list_refs(
        &self,
        group: &str,
        version: &str,
        resource: &str,
        namespace: Option<&str>,
    ) -> Vec<(String, String)> {
        let store = self.store.lock().unwrap();
        store
            .objects
            .iter()
            .filter(|(k, _)| {
                k.group == group
                    && k.version == version
                    && k.resource == resource
                    && namespace.is_none_or(|ns| k.namespace.as_deref() == Some(ns))
            })
            .map(|(k, _)| (k.namespace.clone().unwrap_or_default(), k.name.clone()))
            .collect()
    }

    /// Return a [`FaultBuilder`] that installs a fault rule into this server
    /// when a trigger method is called.
    ///
    /// ```ignore
    /// server.fault_builder(Fault::Conflict).at_random_rate(0.10);
    /// server.fault_builder(Fault::Internal).next_n(3);
    /// ```
    pub fn fault_builder(&self, fault: Fault) -> FaultBuilder {
        FaultBuilder {
            fault,
            store: Arc::clone(&self.store),
        }
    }

    /// Enable or disable watch event fuzzing.
    ///
    /// When enabled, mutation events are held in a buffer rather than
    /// broadcast immediately.  The buffer is flushed during each
    /// `kubo::testing::World::advance` call, after the clock moves and before
    /// `run_until_idle` yields to spawned tasks: the flush shuffles the
    /// buffered events using the seeded RNG only when fuzzing is still
    /// enabled at flush time, then broadcasts them to all watch subscribers.
    /// Buffered events are broadcast unshuffled if fuzzing is disabled before
    /// the next advance.  This makes different event orderings across objects
    /// deterministically reproducible from a single seed.
    ///
    /// Disabled by default.
    pub fn set_fuzz_watch_events(&self, enabled: bool) {
        self.store.lock().unwrap().fuzz_watch_events = enabled;
    }

    /// Flush the pending watch-event buffer, broadcasting all queued events
    /// (shuffled only when fuzzing is still enabled at flush time) to their
    /// respective GVK channels.
    ///
    /// Called automatically by `kubo::testing::World::advance` during each
    /// advance call, after the clock moves and before `run_until_idle` yields
    /// to spawned tasks.
    pub fn flush_pending_watch_events(&self) {
        self.store.lock().unwrap().flush_pending_watch_events();
    }
}

impl Default for FakeApiServer {
    fn default() -> Self {
        Self::new()
    }
}

// ── FaultBuilder ─────────────────────────────────────────────────────────────

/// Builder returned by [`FakeApiServer::fault_builder`] and `kubo::testing::World::fault`.
///
/// Call one of the trigger methods to register the fault rule into the fake
/// server.  The rule is active immediately after the call.
///
/// ```ignore
/// world.fault(Fault::Conflict).at_random_rate(0.10);
/// world.fault(Fault::Internal).next_n(3);
/// world.fault(Fault::Throttled).once();
/// world.fault(Fault::NotFound).always();
/// ```
pub struct FaultBuilder {
    fault: Fault,
    store: Arc<Mutex<Store>>,
}

impl FaultBuilder {
    /// Inject the fault on every mutable request with probability `probability`.
    ///
    /// The decision is deterministic — it uses the fake's seeded RNG (seeded
    /// from the world's seed).
    pub fn at_random_rate(self, probability: f64) {
        self.store.lock().unwrap().add_fault_rule(FaultRule {
            fault: self.fault,
            trigger: FaultTrigger::RandomRate(probability),
        });
    }

    /// Inject the fault once, then remove the rule.
    pub fn once(self) {
        self.store.lock().unwrap().add_fault_rule(FaultRule {
            fault: self.fault,
            trigger: FaultTrigger::Once,
        });
    }

    /// Inject the fault for the next `n` matching requests.
    pub fn next_n(self, n: u64) {
        self.store.lock().unwrap().add_fault_rule(FaultRule {
            fault: self.fault,
            trigger: FaultTrigger::NextN(n),
        });
    }

    /// Inject the fault on every matching request indefinitely.
    pub fn always(self) {
        self.store.lock().unwrap().add_fault_rule(FaultRule {
            fault: self.fault,
            trigger: FaultTrigger::Always,
        });
    }
}

// ── tower::Service impl ───────────────────────────────────────────────────────

impl<B> Service<Request<B>> for FakeApiServer
where
    B: HttpBody<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    type Response = Response<ResponseBody>;
    type Error = BoxError;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let store = Arc::clone(&self.store);
        Box::pin(async move { dispatch(req, store).await })
    }
}

// ── Request dispatch ──────────────────────────────────────────────────────────

async fn dispatch<B>(
    req: Request<B>,
    store: Arc<Mutex<Store>>,
) -> Result<Response<ResponseBody>, BoxError>
where
    B: HttpBody<Data = Bytes>,
    B::Error: Into<BoxError>,
{
    let (parts, body) = req.into_parts();
    let method = parts.method;
    let uri = parts.uri;
    let path = uri.path().to_string();
    let query = uri.query().unwrap_or("").to_string();
    let content_type = parts
        .headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let body_bytes = body.collect().await.map_err(Into::into)?.to_bytes();

    // Inject faults on mutable operations only (PATCH / DELETE).  Read-only
    // operations (GET, LIST, WATCH) bypass fault rules so the controller's
    // watcher loop is never disrupted by injected errors.
    if method == Method::PATCH || method == Method::DELETE {
        let maybe_fault = store.lock().unwrap().check_fault();
        if let Some(fault) = maybe_fault {
            match fault {
                Fault::Latency(d) => {
                    tokio::time::sleep(d).await;
                    // Fall through to normal routing after the delay.
                }
                Fault::Conflict => {
                    return json_response(
                        StatusCode::CONFLICT,
                        serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "message": "Operation cannot be fulfilled: injected Conflict fault",
                            "reason": "Conflict",
                            "code": 409
                        }),
                    );
                }
                Fault::NotFound => {
                    return json_response(
                        StatusCode::NOT_FOUND,
                        serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "reason": "NotFound",
                            "code": 404
                        }),
                    );
                }
                Fault::Internal => {
                    return json_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "reason": "InternalError",
                            "code": 500
                        }),
                    );
                }
                Fault::Throttled => {
                    return json_response(
                        StatusCode::TOO_MANY_REQUESTS,
                        serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "reason": "Throttled",
                            "code": 429
                        }),
                    );
                }
                Fault::Disconnect => {
                    return Err(Box::new(FaultDisconnectError));
                }
            }
        }
    }

    route(&method, &path, &query, &content_type, body_bytes, store)
}

fn route(
    method: &Method,
    path: &str,
    query: &str,
    content_type: &str,
    body: Bytes,
    store: Arc<Mutex<Store>>,
) -> Result<Response<ResponseBody>, BoxError> {
    let Some(parsed) = parse_route(path) else {
        return json_response(
            StatusCode::NOT_FOUND,
            serde_json::json!({
                "error": "unsupported path",
                "path": path,
            }),
        );
    };

    match parsed {
        ParsedRoute::Collection {
            group,
            version,
            resource,
            namespace,
        } => handle_collection(
            method,
            query,
            &group,
            &version,
            &resource,
            namespace.as_deref(),
            store,
        ),

        ParsedRoute::Object(ParsedPath { key, subresource }) => handle_object(
            method,
            key,
            subresource.as_deref(),
            content_type,
            body,
            query,
            store,
        ),
    }
}

// ── Collection handler (LIST + WATCH) ─────────────────────────────────────────

fn handle_collection(
    method: &Method,
    query: &str,
    group: &str,
    version: &str,
    resource: &str,
    namespace: Option<&str>,
    store: Arc<Mutex<Store>>,
) -> Result<Response<ResponseBody>, BoxError> {
    if *method != Method::GET {
        return json_response(
            StatusCode::METHOD_NOT_ALLOWED,
            serde_json::json!({"error": "method not allowed on collection"}),
        );
    }

    let params = parse_query(query);
    let is_watch = params
        .get("watch")
        .map(|v| *v == "1" || *v == "true")
        .unwrap_or(false);

    if is_watch {
        handle_watch(group, version, resource, namespace, store)
    } else {
        let store_guard = store.lock().unwrap();
        let mut items = store_guard.list(group, version, resource, namespace);
        let rv = store_guard.resource_version;
        drop(store_guard);

        // `labelSelector` equality filtering.  The real
        // apiserver supports the full selector grammar (set-based, existence,
        // `!=`); the fake models only the comma-separated `key=value` equality
        // form, which is what the conformance harness uses to isolate a
        // scenario's own object under parallel real-cluster runs.  Unknown
        // selector forms are treated as no match (the item is dropped), so a
        // selector that matches nothing returns an empty `items` array — the
        // same observable outcome as the real apiserver.
        if let Some(selector) = params.get("labelSelector") {
            items.retain(|obj| matches_label_selector(obj, selector));
        }

        let api_version = if group.is_empty() {
            version.to_string()
        } else {
            format!("{}/{}", group, version)
        };

        json_response(
            StatusCode::OK,
            serde_json::json!({
                "apiVersion": api_version,
                "kind": "List",
                "metadata": {
                    "resourceVersion": rv.to_string(),
                    "continue": ""
                },
                "items": items
            }),
        )
    }
}

/// Upgrade a `GET ?watch=1` request to a streaming watch response.
///
/// Subscribes to the per-GVK broadcast channel before returning the response,
/// so no mutation between subscription and the forwarder task's first receive
/// is dropped, then spawns a forwarder task that:
///
/// 1. Forwards future broadcast events — `ADDED` / `MODIFIED` / `DELETED` —
///    as newline-delimited JSON into an mpsc channel.
/// 2. Applies an optional namespace filter for namespaced watches.
/// 3. Exits cleanly when the mpsc receiver is dropped (client disconnected)
///    or the broadcast channel is closed (store dropped).
///
/// Lagged broadcast receivers (ring buffer overflow) skip the missed events;
/// the fake does not signal the gap.
fn handle_watch(
    group: &str,
    version: &str,
    resource: &str,
    namespace: Option<&str>,
    store: Arc<Mutex<Store>>,
) -> Result<Response<ResponseBody>, BoxError> {
    // Subscribe before reading anything so no mutation between subscribe
    // and the forwarder loop's first recv() can be silently dropped.
    let mut broadcast_rx = {
        let mut guard = store.lock().unwrap();
        guard.subscribe(group, version, resource)
    };

    let (tx, rx) = mpsc::channel::<Bytes>(64);
    let ns_filter = namespace.map(str::to_string);

    tokio::spawn(async move {
        loop {
            match broadcast_rx.recv().await {
                Ok(event) => {
                    // Apply namespace filter if the watch is namespaced.
                    if let Some(ref ns) = ns_filter {
                        let obj_ns = event
                            .object
                            .get("metadata")
                            .and_then(|m| m.get("namespace"))
                            .and_then(|n| n.as_str())
                            .unwrap_or("");
                        if obj_ns != ns {
                            continue;
                        }
                    }

                    let mut line = serde_json::to_vec(&serde_json::json!({
                        "type": event.event_type,
                        "object": event.object,
                    }))
                    .unwrap_or_default();
                    line.push(b'\n');

                    if tx.send(Bytes::from(line)).await.is_err() {
                        // Receiver dropped — client disconnected.
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    // Some events were missed due to ring buffer overflow.
                    // The watcher will re-list when it detects a gap.
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    // Broadcast channel was closed (store dropped).
                    return;
                }
            }
        }
    });

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(ResponseBody::Watch(WatchBody { rx }))
        .map_err(|e| Box::new(e) as BoxError)
}

// ── Object handler (GET / PATCH / DELETE) ─────────────────────────────────────

fn handle_object(
    method: &Method,
    key: StoreKey,
    subresource: Option<&str>,
    content_type: &str,
    body: Bytes,
    query: &str,
    store: Arc<Mutex<Store>>,
) -> Result<Response<ResponseBody>, BoxError> {
    match (method, subresource) {
        // ── GET ──────────────────────────────────────────────────────────────
        (&Method::GET, None) => {
            let store = store.lock().unwrap();
            match store.get(&key) {
                Some(obj) => json_response(StatusCode::OK, obj.clone()),
                None => json_response(StatusCode::NOT_FOUND, not_found_status(&key.name)),
            }
        }

        // ── PATCH /status ─────────────────────────────────────────────────────
        (&Method::PATCH, Some("status")) => {
            let patch: Value = serde_json::from_slice(&body)?;
            let status = patch
                .get("status")
                .cloned()
                .unwrap_or_else(|| Value::Object(Default::default()));
            let mut store = store.lock().unwrap();
            match store.status_patch(&key, status) {
                Some(obj) => json_response(StatusCode::OK, obj),
                None => json_response(StatusCode::NOT_FOUND, not_found_status(&key.name)),
            }
        }

        // ── PATCH (merge) ─────────────────────────────────────────────────────
        (&Method::PATCH, None) if content_type.starts_with("application/merge-patch+json") => {
            let patch: Value = serde_json::from_slice(&body)?;
            let mut store = store.lock().unwrap();
            match store.merge_patch(&key, patch) {
                Some(obj) => json_response(StatusCode::OK, obj),
                None => json_response(StatusCode::NOT_FOUND, not_found_status(&key.name)),
            }
        }

        // ── PATCH (SSA apply) ─────────────────────────────────────────────────
        (&Method::PATCH, None) if content_type.starts_with("application/apply-patch") => {
            let params = parse_query(query);
            let manager = params.get("fieldManager").copied().unwrap_or("unknown");
            let force = params.get("force").map(|v| *v == "true").unwrap_or(false);
            let patch: Value = serde_json::from_slice(&body)?;
            let mut store = store.lock().unwrap();
            match store.ssa_apply(key, manager, patch, force) {
                Ok(obj) => json_response(StatusCode::OK, obj),
                Err(conflict) => json_response(
                    StatusCode::CONFLICT,
                    serde_json::json!({
                        "kind": "Status",
                        "apiVersion": "v1",
                        "status": "Failure",
                        "message": format!(
                            "Apply failed with 1 conflict: \"{}\": field \"{}\" is owned by manager \"{}\"",
                            manager, conflict.field, conflict.owner
                        ),
                        "reason": "Conflict",
                        "details": {
                            "causes": [{
                                "reason": "FieldManagerConflict",
                                "message": format!(
                                    "conflict: manager \"{}\" owns field \"{}\"",
                                    conflict.owner, conflict.field
                                ),
                                "field": conflict.field,
                            }]
                        },
                        "code": 409
                    }),
                ),
            }
        }

        // ── PATCH (replace fallback) ──────────────────────────────────────────────
        (&Method::PATCH, None) => {
            let obj: Value = serde_json::from_slice(&body)?;
            let stored = store.lock().unwrap().upsert(key, obj);
            json_response(StatusCode::OK, stored)
        }

        // ── DELETE ────────────────────────────────────────────────────────────
        (&Method::DELETE, None) => {
            let mut store = store.lock().unwrap();
            match store.finalizer_delete(&key) {
                Some(obj) => json_response(StatusCode::OK, obj),
                None => json_response(StatusCode::NOT_FOUND, not_found_status(&key.name)),
            }
        }

        _ => json_response(
            StatusCode::METHOD_NOT_ALLOWED,
            serde_json::json!({"error": "method not allowed"}),
        ),
    }
}

// ── RFC 7396 merge patch ────────────────────────────────────────────────────────────

fn do_merge_patch(target: &mut Value, patch: &Value) {
    match (target, patch) {
        (Value::Object(t), Value::Object(p)) => {
            for (key, val) in p {
                if val.is_null() {
                    t.remove(key);
                } else if let Some(existing) = t.get_mut(key) {
                    do_merge_patch(existing, val);
                } else {
                    t.insert(key.clone(), val.clone());
                }
            }
        }
        (t, p) => *t = p.clone(),
    }
}

/// Whether an object is terminating (has `metadata.deletionTimestamp`) with no
/// remaining finalizers — the condition under which the apiserver's GC
/// controller removes it.
///
/// Used by [`Store::merge_patch`] to model finalizer-triggered garbage
/// collection synchronously: a merge-patch whose merged result is terminating
/// with no remaining finalizers returns the updated object (matching the real
/// patch response) but removes it from the store so the next GET returns 404.
fn is_terminating_with_no_finalizers(obj: &Value) -> bool {
    let meta = match obj.get("metadata").and_then(|m| m.as_object()) {
        Some(m) => m,
        None => return false,
    };
    if !meta.contains_key("deletionTimestamp") {
        return false;
    }
    match meta.get("finalizers").and_then(|f| f.as_array()) {
        Some(arr) => arr.iter().filter(|v| !v.is_null()).count() == 0,
        None => true,
    }
}

// ── SSA field path extraction ───────────────────────────────────────────────────

/// Extract all leaf field paths from a JSON value as dot-separated strings.
///
/// Used by [`Store::ssa_apply`] to determine which fields a patch claims
/// ownership of.  At the root level, `metadata`, `apiVersion`, `kind`, and
/// `status` are skipped: those are either system-managed or handled via a
/// dedicated subresource, and including them in the ownership map would
/// cause spurious conflicts.
///
/// Examples:
/// - `{"spec": {"field": "a"}}` → `["spec.field"]`
/// - `{"spec": {"a": 1, "b": 2}}` → `["spec.a", "spec.b"]`
fn extract_field_paths(value: &Value, prefix: &str) -> Vec<String> {
    let Value::Object(map) = value else {
        // Leaf scalar or array: the prefix itself is the owned path.
        return if prefix.is_empty() {
            vec![]
        } else {
            vec![prefix.to_string()]
        };
    };

    let mut paths = Vec::new();
    for (k, v) in map {
        // Skip system-managed root-level keys.
        if prefix.is_empty() && matches!(k.as_str(), "metadata" | "apiVersion" | "kind" | "status")
        {
            continue;
        }
        let child = if prefix.is_empty() {
            k.clone()
        } else {
            format!("{}.{}", prefix, k)
        };
        paths.extend(extract_field_paths(v, &child));
    }
    paths
}

// ── URL parser ────────────────────────────────────────────────────────────────

/// A parsed Kubernetes API path.
enum ParsedRoute {
    /// A request targeting a single named object (with optional subresource).
    Object(ParsedPath),
    /// A request targeting a collection (LIST or WATCH).
    Collection {
        group: String,
        version: String,
        resource: String,
        namespace: Option<String>,
    },
}

struct ParsedPath {
    key: StoreKey,
    subresource: Option<String>,
}

/// Parse a Kubernetes API path into a [`ParsedRoute`].
///
/// Recognised patterns:
/// ```text
/// /api/{version}/namespaces/{ns}/{resource}[/{name}[/{sub}]]
/// /api/{version}/{resource}[/{name}]
/// /apis/{group}/{version}/namespaces/{ns}/{resource}[/{name}[/{sub}]]
/// /apis/{group}/{version}/{resource}[/{name}]
/// ```
fn parse_route(full_path: &str) -> Option<ParsedRoute> {
    let path = full_path.split('?').next().unwrap_or(full_path);
    let segs: Vec<&str> = path.trim_start_matches('/').split('/').collect();

    match segs.as_slice() {
        // ── Object patterns (with name) ───────────────────────────────────────

        // Core group, namespaced, with subresource
        ["api", version, "namespaces", ns, resource, name, sub] => {
            Some(ParsedRoute::Object(ParsedPath {
                key: StoreKey {
                    group: String::new(),
                    version: version.to_string(),
                    resource: resource.to_string(),
                    namespace: Some(ns.to_string()),
                    name: name.to_string(),
                },
                subresource: Some(sub.to_string()),
            }))
        }
        // Core group, namespaced
        ["api", version, "namespaces", ns, resource, name] => {
            Some(ParsedRoute::Object(ParsedPath {
                key: StoreKey {
                    group: String::new(),
                    version: version.to_string(),
                    resource: resource.to_string(),
                    namespace: Some(ns.to_string()),
                    name: name.to_string(),
                },
                subresource: None,
            }))
        }
        // Core group, cluster-scoped
        ["api", version, resource, name] => Some(ParsedRoute::Object(ParsedPath {
            key: StoreKey {
                group: String::new(),
                version: version.to_string(),
                resource: resource.to_string(),
                namespace: None,
                name: name.to_string(),
            },
            subresource: None,
        })),

        // Custom group, namespaced, with subresource
        [
            "apis",
            group,
            version,
            "namespaces",
            ns,
            resource,
            name,
            sub,
        ] => Some(ParsedRoute::Object(ParsedPath {
            key: StoreKey {
                group: group.to_string(),
                version: version.to_string(),
                resource: resource.to_string(),
                namespace: Some(ns.to_string()),
                name: name.to_string(),
            },
            subresource: Some(sub.to_string()),
        })),
        // Custom group, namespaced
        ["apis", group, version, "namespaces", ns, resource, name] => {
            Some(ParsedRoute::Object(ParsedPath {
                key: StoreKey {
                    group: group.to_string(),
                    version: version.to_string(),
                    resource: resource.to_string(),
                    namespace: Some(ns.to_string()),
                    name: name.to_string(),
                },
                subresource: None,
            }))
        }
        // Custom group, cluster-scoped
        ["apis", group, version, resource, name] => Some(ParsedRoute::Object(ParsedPath {
            key: StoreKey {
                group: group.to_string(),
                version: version.to_string(),
                resource: resource.to_string(),
                namespace: None,
                name: name.to_string(),
            },
            subresource: None,
        })),

        // ── Collection patterns (no name) ─────────────────────────────────────

        // Core group, namespaced collection
        ["api", version, "namespaces", ns, resource] => Some(ParsedRoute::Collection {
            group: String::new(),
            version: version.to_string(),
            resource: resource.to_string(),
            namespace: Some(ns.to_string()),
        }),
        // Core group, cluster-scoped collection
        ["api", version, resource] => Some(ParsedRoute::Collection {
            group: String::new(),
            version: version.to_string(),
            resource: resource.to_string(),
            namespace: None,
        }),
        // Custom group, namespaced collection
        ["apis", group, version, "namespaces", ns, resource] => Some(ParsedRoute::Collection {
            group: group.to_string(),
            version: version.to_string(),
            resource: resource.to_string(),
            namespace: Some(ns.to_string()),
        }),
        // Custom group, cluster-scoped collection (also covers Api::all watches)
        ["apis", group, version, resource] => Some(ParsedRoute::Collection {
            group: group.to_string(),
            version: version.to_string(),
            resource: resource.to_string(),
            namespace: None,
        }),

        _ => None,
    }
}

// ── Query string parser ───────────────────────────────────────────────────────

/// Parse `key=value` pairs from a URL query string.
fn parse_query(query: &str) -> HashMap<&str, &str> {
    query
        .split('&')
        .filter(|s| !s.is_empty())
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?;
            let val = parts.next().unwrap_or("");
            Some((key, val))
        })
        .collect()
}

// ── Label-selector matching ──────────────────────────────────────────────────

/// Evaluate a comma-separated `key=value` equality `labelSelector` against an
/// object's `metadata.labels`.
///
/// This is the subset of the Kubernetes label-selector grammar the fake
/// models: equality only (`key=value`), multiple terms ANDed by commas, no
/// `!=`, no `key` (existence), no `key in (...)`, no `!key`.  It exists so the
/// conformance harness can isolate a scenario's own object under parallel
/// real-cluster runs (the real apiserver returns every object in the
/// collection; the fake's `Store::list` returns only the objects the scenario
/// created, so without a selector a List differential would break under
/// parallelism).  The harness never relies on the richer selector forms, so the
/// fake does not model them — a selector using an unsupported form matches
/// nothing, the same observable outcome as the real apiserver returning an
/// empty page.  An empty selector (no terms) matches everything.
///
/// A missing `metadata.labels` is treated as no labels (no term matches).
fn matches_label_selector(obj: &Value, selector: &str) -> bool {
    let labels = obj
        .get("metadata")
        .and_then(|m| m.get("labels"))
        .and_then(Value::as_object);
    for term in selector.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let Some((key, val)) = term.split_once('=') else {
            return false;
        };
        let matched = labels
            .and_then(|l| l.get(key.trim()))
            .and_then(Value::as_str)
            .map(|v| v == val.trim())
            .unwrap_or(false);
        if !matched {
            return false;
        }
    }
    true
}

// ── Response helpers ──────────────────────────────────────────────────────────

fn json_response(status: StatusCode, body: Value) -> Result<Response<ResponseBody>, BoxError> {
    let bytes = Bytes::from(serde_json::to_vec(&body)?);
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(ResponseBody::Full(Full::new(bytes)))
        .map_err(|e| Box::new(e) as BoxError)
}

fn not_found_status(name: &str) -> Value {
    serde_json::json!({
        "kind": "Status",
        "apiVersion": "v1",
        "metadata": {},
        "status": "Failure",
        "message": format!("{name} not found"),
        "reason": "NotFound",
        "code": 404,
    })
}

#[cfg(test)]
mod tests {
    use super::matches_label_selector;
    use serde_json::json;

    fn obj_with_labels(labels: serde_json::Value) -> serde_json::Value {
        json!({ "metadata": { "labels": labels } })
    }

    #[test]
    fn label_selector_equality_matches_a_present_label() {
        let obj = obj_with_labels(json!({ "conformance.kubo.dev/scenario": "ssa-observe-list" }));
        assert!(matches_label_selector(
            &obj,
            "conformance.kubo.dev/scenario=ssa-observe-list"
        ));
    }

    #[test]
    fn label_selector_equality_rejects_a_mismatched_value() {
        let obj = obj_with_labels(json!({ "conformance.kubo.dev/scenario": "ssa-observe-list" }));
        assert!(!matches_label_selector(
            &obj,
            "conformance.kubo.dev/scenario=other"
        ));
    }

    #[test]
    fn label_selector_rejects_when_the_label_is_absent() {
        let obj = obj_with_labels(json!({ "other": "value" }));
        assert!(!matches_label_selector(
            &obj,
            "conformance.kubo.dev/scenario=ssa-observe-list"
        ));
    }

    #[test]
    fn label_selector_rejects_when_metadata_labels_is_missing() {
        let obj = json!({ "metadata": {} });
        assert!(!matches_label_selector(
            &obj,
            "conformance.kubo.dev/scenario=ssa-observe-list"
        ));
    }

    #[test]
    fn label_selector_ands_multiple_terms() {
        let obj = obj_with_labels(json!({ "a": "1", "b": "2" }));
        assert!(matches_label_selector(&obj, "a=1,b=2"));
        assert!(!matches_label_selector(&obj, "a=1,b=3"));
    }

    #[test]
    fn label_selector_unsupported_form_matches_nothing() {
        // Existence-only (`key`) and set-based (`key in (...)`) forms are not
        // modelled; they match nothing so the observable outcome (empty page)
        // matches the real apiserver returning no items for an unmodelled form
        // only when the selector genuinely matches nothing — the conformance
        // harness never uses these forms.
        let obj = obj_with_labels(json!({ "a": "1" }));
        assert!(!matches_label_selector(&obj, "a"));
        assert!(!matches_label_selector(&obj, "a in (1)"));
    }
}

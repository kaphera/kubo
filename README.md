<div align="center">
  <h1>kubo</h1>
  <p>
      <b>
        A framework for building Kubernetes operators in Rust.
      </b>
  </p>
  <sub>
    Built with ❤️ at <a href="https://kaphera.com">Kaphera</a>.
  </sub>
</div>

---

Declare your reconcile logic as a typed, sequential pipeline of gates, effects, and polls. The framework handles finalizer lifecycle, status patching, backoff, and reverse-order cleanup — you focus on what your controller does, not the ceremony around it.

Every Kubernetes interaction goes through a `KubeHandle` — a concrete handle wrapping a `kube::Client` and a field-manager identity — so a reconcile pass runs the same code path against a real cluster or an in-process fake; the conformance suite pins the known divergences between the two. A top-level `Runtime` wires together leader election, signal handling, and graceful drain so your `main.rs` stays thin.

## Usage

The runtime is your entry point. Register one listener per CRD kind:

```rust
use kubo::runtime::Runtime;

Runtime::new("my-operator", client)
    .with(app_config)
    .listener(WebServiceListener)
    .listener(DatabaseListener)
    .run()
    .await
```

`.with` stores a value in the runtime's type-keyed extension map; listener code reads it back with `ctx.get::<T>()`.

Each listener implements the `Listener` trait — one struct per controller. The framework constructs the base `Controller` and a fresh `Reconcile` for you, so `controller()` decorates the watcher with `.owns()` / `.watches()` and `reconcile()` chains steps off the passed-in `rec`:

```rust
use std::{sync::Arc, time::Duration};
use k8s_openapi::api::{apps::v1::Deployment, core::v1::{ConfigMap, Service}};
use kubo::{
    HasConditions, ReconcileContext, async_trait,
    environment::{EnvironmentError, KubeHandle, ObjectRef},
    gate::{Gate, Outcome},
    effect::{Effect, EffectOutcome},
    backoff::fixed::Fixed,
    listener::{ControllerContext, Listener},
    reconcile::{ReadyReconcile, Reconcile},
    state::{State, StateKey},
    utils::is_condition_true,
};
use kube::{
    Api, Resource, ResourceExt,
    runtime::{Controller, reflector::ObjectRef as KubeObjectRef, watcher::Config as WatcherConfig},
};

pub struct WebServiceListener;

impl Listener<WebService> for WebServiceListener {
    type Status = WebServiceStatus;

    const NAME: &'static str = "web-service";
    const FINALIZER: &'static str = "webservices.example.com/cleanup";

    fn controller(
        &self,
        ctx: &ControllerContext,
        ctrl: Controller<WebService>,
    ) -> Controller<WebService> {
        let client = ctx.require::<kube::Client>();
        let store = ctrl.store();

        // Reconcile any WebService that depends on a changed Database.
        ctrl.watches(
            Api::<Database>::all(client.clone()),
            WatcherConfig::default(),
            {
                let store = store.clone();
                move |db: Database| {
                    let upstream_name = db.name_any();
                    let upstream_ns = db.namespace().unwrap_or_default();
                    store
                        .state()
                        .into_iter()
                        .filter(|ws| {
                            let r = &ws.spec.database_ref;
                            let ws_ns = ws.metadata.namespace.as_deref().unwrap_or_default();
                            let ref_ns = r.namespace.as_deref().unwrap_or(ws_ns);
                            r.name == upstream_name && ref_ns == upstream_ns
                        })
                        .map(|ws| KubeObjectRef::from_obj(&*ws))
                        .collect::<Vec<_>>()
                }
            },
        )
        .owns(Api::<Deployment>::all(client.clone()), WatcherConfig::default())
        .owns(Api::<Service>::all(client.clone()), WatcherConfig::default())
        .owns(Api::<ConfigMap>::all(client), WatcherConfig::default())
    }

    fn reconcile<'a>(
        &'a self,
        _ctx: &'a ControllerContext,
        obj: Arc<WebService>,
        rec: Reconcile<'a, WebService>,
    ) -> ReadyReconcile<'a, WebService, WebServiceStatus> {
        rec.ready_reason("DeploymentReady", "WebService is serving traffic")
            .gate(ResolveDatabase)
            .gate(ResolveTlsSecret::new())
            .effect(ApplyWebServiceConfigMap::new())
            .effect(ApplyDeployment)
            .effect(ApplyService::new())
            .poll(WaitForDeploymentReady::new())
            .effect(ApplyConnectionSecret::new())
            .steady_state_interval(Duration::from_secs(60))
            .status(|state, prev: &WebServiceStatus| {
                // Fall back to `prev` for any field whose upstream gate
                // returned Wait on this pass — never clobber a value
                // we've already published.
                let db = state.get::<ResolveDatabase>();
                WebServiceStatus {
                    database_url: db
                        .map(|d| d.url.clone())
                        .or(prev.database_url.clone()),
                    connection_url: state
                        .get::<ApplyConnectionSecret>()
                        .cloned()
                        .or(prev.connection_url.clone()),
                }
            })
    }
}
```

The `Runtime` calls `.execute(FINALIZER)` on the returned pipeline automatically — you declare the what, the framework handles the when.

### Gates, effects, and polls

A gate is a read-only dependency check that either passes (producing a typed value) or waits (producing a condition and backoff). Effects consume upstream outputs via `state.require::<UpstreamStep>()` and return `EffectOutcome::Done(value)` or `EffectOutcome::Retry { condition, backoff }`. The lookup is runtime-checked: a missing key produces an error naming the key and the expected value type. Polls observe convergence without mutating state.

```rust
/// Gate: block until the referenced Database exists and is Ready.
pub struct ResolveDatabase;

impl StateKey for ResolveDatabase {
    type Value = DatabaseEndpoint;  // the typed value downstream steps consume
}

#[async_trait]
impl Gate<WebService> for ResolveDatabase {
    type Error = EnvironmentError;

    async fn evaluate(
        &self,
        cx: &ReconcileContext<WebService>,
        ops: &KubeHandle,
    ) -> Result<Outcome<DatabaseEndpoint>, Self::Error> {
        let ns = cx.cr.metadata.namespace.as_deref().unwrap_or_default();
        let db_ref = ObjectRef::new(ns, &cx.cr.spec.database_ref.name);
        let db: Option<Database> = ops.get::<Database>(&db_ref).await?;

        match db {
            None => Ok(Outcome::Wait {
                condition: cx.condition.not_ready_condition(
                    "DatabaseMissing",
                    &format!("Database '{db_ref}' not found"),
                ),
                backoff: Box::new(Fixed::new(Duration::from_secs(15))),
            }),
            Some(db) if !is_condition_true(&db.conditions(), "Ready") => Ok(Outcome::Wait {
                condition: cx.condition.not_ready_condition(
                    "WaitingForDatabase",
                    &format!("Database '{db_ref}' is not yet ready"),
                ),
                backoff: Box::new(Fixed::new(Duration::from_secs(15))),
            }),
            Some(db) => Ok(Outcome::Pass(DatabaseEndpoint {
                url: db.status.as_ref().and_then(|s| s.url.clone())
                    .unwrap_or_default(),
            })),
        }
    }
}

/// Effect: apply the Deployment, consuming the ResolveDatabase gate's output from state.
pub struct ApplyDeployment;

impl StateKey for ApplyDeployment {
    type Value = ();
}

#[async_trait]
impl Effect<WebService> for ApplyDeployment {
    type Error = EnvironmentError;

    async fn apply(
        &self,
        cx: &ReconcileContext<WebService>,
        ops: &KubeHandle,
        state: &State,
    ) -> Result<EffectOutcome<()>, Self::Error> {
        let endpoint = state
            .require::<ResolveDatabase>()
            .map_err(|e| EnvironmentError::Other(e.to_string()))?;

        let deployment = Self::desired_deployment(&cx.cr, endpoint);
        ops.apply::<Deployment>(&cx.object.namespace, deployment).await?;
        Ok(EffectOutcome::Done(()))
    }
}
```

## Concepts

**Gate** — A read-only dependency check. Either passes (inserting a typed value into the state bag for downstream steps) or waits (producing a condition for status and a backoff for requeue timing). No cleanup obligation. Use gates to resolve upstream dependencies — a referenced `Database` reaching `Ready`, a `Secret` whose resource version feeds a config hash.

**Effect** — A write operation that creates or updates external state. Produces a typed value like a gate (consumed via `state.require::<UpstreamEffect>()`), and declares a `cleanup` method called in reverse declaration order during deletion. Effects are the only step kind that mutates anything, and consequently the only one that carries a teardown obligation.

**Poll** — A read-only observation placed after effects. Waits for something this controller created to converge (e.g. a `Deployment` reaching its desired replica count). No state output, no cleanup.

**Guard** — A gate whose output is evaluated but discarded. Use for precondition checks that must pass but don't produce data for downstream steps.

**State** — A typed, heterogeneous map keyed by `StateKey` (a trait with an associated `Value` type). Each gate and effect declares its output type; downstream steps retrieve values with `state.get::<UpstreamStep>()` or `state.require::<UpstreamStep>()` (which returns a descriptive error naming the missing key and expected type). Each step publishes under its own type, so values from steps of different types never collide.

**Group** — A composable sub-pipeline that collapses into a single effect position. Sub-steps (gates, effects, polls) share a scoped state bag that never leaks into the outer pipeline; a reducer closure produces the single value published under the group's `StateKey`. Cleanup walks the group's effects in reverse declaration order, at the group's position in the outer cleanup sequence. Use it for tightly-coupled sub-steps whose intermediates are implementation details.

**Listener** — Implements the `Listener<R>` trait, which declares an associated `Status` type, a `NAME` constant, a `FINALIZER` constant, a `controller()` method that decorates the framework-provided `kube::runtime::Controller<R>` with `.owns()` (for resources this controller creates) and `.watches()` (for upstream resources whose changes should trigger reconciliation), and a `reconcile()` method returning a `ReadyReconcile`. One listener per CRD kind.

**Runtime** — Top-level entry point. Holds operator-wide `Extensions` (a type-keyed map of shared clients and config), registers listeners, manages leader election via a Coordination v1 `Lease`, handles OS signals, and drains in-flight reconciliations on shutdown.

## Pipeline Lifecycle

On **apply** (no deletion timestamp), the framework ensures the finalizer is present, runs each step in declaration order, and patches `ResourceStatus<T>` (which wraps your domain type with conditions and `observedGeneration`) after every step that changes it. As each effect completes (`EffectOutcome::Done`), the framework records the effect's identity in the `pendingCleanup` field of `ResourceStatus<T>` — a framework-owned ledger of effects that have applied but not yet been torn down. Once all steps pass, it requeues at the configured steady-state interval.

On **cleanup** (deletion timestamp present), effect `cleanup` methods run in reverse declaration order, **filtered by the `pendingCleanup` ledger** — cleanup only runs for effects that actually completed `apply`. After each successful cleanup the entry is struck off and the shrunk ledger is patched back to status, giving real resumability: a `Retry` resumes from the failed effect and skips effects already cleaned up. If any cleanup returns `Retry`, the pipeline requeues with backoff and **keeps the finalizer** so the object survives for the next pass. The finalizer is removed only after all cleanups succeed. Ledger entries that match no registered cleanup step (e.g. after an effect rename or removal) are logged and dropped — blocking deletion forever on absent code is worse than leaking the orphaned resource.

`.execute(FINALIZER)` runs the full lifecycle. `.converge()` runs the pipeline only, with no finalizer management — useful for resources you don't own the deletion of.

`.effect(effect)` records the effect's identity in the ledger. By default the ledger key is the effect's Rust type name, so a struct rename across a release silently orphans the old `type_name`-keyed entry — the framework logs and drops it, and the old external resource may leak. For effects whose `cleanup` deletes external state that must not leak, override `Effect::effect_name` to return a stable, release-independent name; the pinned key survives struct renames so cleanup still matches the original apply.

`.rotation_annotation(key)` — while the annotation is present, the framework inserts `RotationRequested` into the state bag for each pass; effects opt in by checking `state.contains::<RotationRequested>()` (e.g. to regenerate credentials without a deletion/recreation cycle). After a fully successful pass the framework removes the annotation.

`.steady_state_interval(duration)` sets the requeue interval once the pipeline reaches a stable `Ready` state.

## Testing

Tests construct a `KubeHandle` from the `kube::Client` provided by `World`, which points at an in-process `FakeApiServer`. Seed upstream resources, run your pipeline, and assert on resulting cluster state — no k3d, no kind, no waiting.

```rust
use std::sync::Arc;
use kubo::testing::World;
use kubo::environment::{KubeHandle, ObjectRef};
use kubo::reconcile::Reconcile;

let world = World::builder().build().await;
world.prefill(make_vault_instance("my-vault", true));

let ops = KubeHandle::new(world.client().clone(), "my-operator");
let obj = Arc::new(make_vault_kv_mount("my-mount", "ns", "my-vault"));

Reconcile::new(&ops, obj)
    .gate(ResolveVaultInstance { .. })
    .effect(CreateMount { .. })
    .status(|state, _prev: &MyStatus| MyStatus {
        mount_path: state.get::<CreateMount>().cloned(),
    })
    .execute(FINALIZER)
    .await?;

assert!(world.has::<Secret>("ns", "my-secret"));
```

### Property-based testing with `kubo::testing`

The `test-support` feature on `kubo` ships a `World` harness that drives a listener through arbitrary create/delete sequences with paused time, plus a library of composable property assertions over the recorded status-tick history. Properties compose: a typical controller test asserts several over the same tick sequence.

```rust
use kubo::testing::{
    World,
    fixtures::{DummyListener, DummyResource, DummyStatus, dummy},
    properties::{assert_converges, assert_monotonic, is_ready},
};
use kube::CustomResourceExt as _;
use std::time::Duration;

#[tokio::test(start_paused = true)]
async fn converges_and_never_regresses() {
    let world = World::builder()
        .install_crd(DummyResource::crd())
        .listener::<DummyResource, _>(DummyListener)
        .build()
        .await;

    world.create(dummy("obj-1", "ns", "hello")).await;
    world.run_until_idle().await;
    world.advance(Duration::from_secs(120)).await;

    let ticks = world.status_ticks::<DummyStatus>("ns", "obj-1");

    assert_converges(&ticks, |s| is_ready(&s.conditions), "reaches Ready");
    assert_monotonic(&ticks, |s| s.ready, "ready flag");
}
```

Available assertions: `monotonic` (once true, stays true), `causal` (effect cannot precede its cause), `converges` (final tick satisfies predicate), `never` (illegal state unreachable), `eventually` (predicate holds at least once), `ordered` (the second predicate may only become true on the same tick as or after the first). Each ships in `assert_*` (panics) and `check_*` (returns `Result` for `prop_assert!`) variants, the latter for integration with `proptest` strategies over `Op<T>` sequences.

### Test macros: `#[kubo::test]` and `#[kubo::property_test]`

The `#[kubo::test]` attribute macro removes the harness ceremony: it wraps the function in paused-time tokio, constructs a fresh `World` for every `&mut World` parameter (including multiple worlds for multi-cluster scenarios), and on failure prints the seed with a rerun instruction:

```text
seed = 7193847216284
You can rerun from this seed by setting the SEED environment variable to 7193847216284
```

Combined with fault injection, a controller opts into chaos testing across many deterministic seeds with two lines:

```rust
#[kubo::test(iterations = 32)]
async fn survives_transient_conflicts(world: &mut World) {
    world.seed_listener(DummyListener);
    world.create(make_dummy("ns", "obj-1")).await;

    // 10% of subsequent PATCH/DELETE requests return 409 Conflict,
    // derived from the world's seeded RNG.
    world.fault(Fault::Conflict).at_random_rate(0.10);

    world.advance_stepping(Duration::from_secs(120), Duration::from_secs(6)).await;

    assert!(world.condition::<DummyResource>("ns", "obj-1", "Ready"));
    assert_monotonic(
        &world.status_ticks::<DummyStatus>("ns", "obj-1"),
        |s| s.touched.is_some(),
        "touched",
    );
}
```

`iterations = 32` runs the body across seeds `0..32`; `seed = N` pins one; `seeds = [a, b, c]` replays a known set. The `iterations` count applies only when neither `SEED` nor an explicit `seed`/`seeds` attribute is set: a set `SEED` collapses the run to a single iteration at that seed, and `ITERATIONS` overrides the attribute value. Watch-event fuzzing (`world.fuzz_watch_events(true)`) uses the same seeds to shuffle event delivery order across objects, so `iterations = 128` exercises 128 distinct interleavings, exposing assumptions about the order in which events arrive.

`#[kubo::property_test]` bridges proptest into the same machinery. Parameters annotated with `#[strategy(expr)]` are drawn from proptest strategies; `&mut World` parameters are injected per case:

```rust
#[kubo::property_test]
async fn well_behaved(
    world: &mut World,
    #[strategy(any_dummy_spec())] spec: DummySpec,
) {
    let obj_ref = world.seed_obj(dummy_from_spec("ns", "obj", spec));

    // Direct reconcile — one async call, no controller loop. Second
    // pass verifies idempotence.
    world.reconcile::<DummyListener>(&obj_ref).await.unwrap();
    world.reconcile::<DummyListener>(&obj_ref).await.unwrap();

    let ticks = world.status_ticks_for::<DummyStatus>(&obj_ref);
    assert_converges(&ticks, |s| is_ready(&s.conditions), "ready");
    assert_monotonic(&ticks, |s| s.touched.is_some(), "touched");
}
```

A single `SEED=N` environment variable seeds both proptest's case-generation RNG and the fake API server's RNG, making a failing case reproducible byte-for-byte — the generated spec, the fault decisions, and the watch-event shuffle all replay identically.

## Conformance

The `kubo-conformance` crate checks that the in-process `FakeApiServer` (used by `World`) matches a real kube-apiserver on the SSA semantics the framework relies on, and pins the known divergences between the two. A `Scenario` is a sequence of steps — SSA applies, status applies with or without a `resourceVersion` precondition, gets of the scenario object or an arbitrary path, lists, finalizer patches, deletes, CRD reinstalls, and full reconcile passes. `run_against_fake` executes it against the fake and records, per step, the HTTP status, the normalised returned object, the normalised `status` subresource, the `resourceVersion`, the error class, and the step name. With the `k3d` feature enabled, `run_against_kube_apiserver` runs the same scenario against a real cluster and `assert_equivalent` compares the two traces entry-by-entry.

```rust
use kubo_conformance::Scenario;
use serde_json::json;

let scenario = Scenario::new("ssa-two-managers")
    .apply("manager-a", json!({"spec": {"field": "a"}}))
    .apply("manager-b", json!({"spec": {"field": "b"}}))
    .expect_last_status(409);

let fake_trace = scenario.run_against_fake();
```

The comparison covers status, the normalised returned object, the normalised `status` subresource, and the error class; any mismatch panics the test.

## Backoff Strategies

**Fixed** — Constant interval. Use when readiness is expected within a known timeframe.

**Exponential** — Duration-aware escalation derived from `lastTransitionTime` on the relevant status condition, not a mutable counter. Stateless by design — the escalation position is reconstructed from cluster state on every pass, so no in-memory backoff state survives a pod restart.

## Build

```sh
cargo build
```

```sh
cargo test
```

```sh
cargo doc --open
```

## Why not just `kube::runtime::Controller` directly?

`kube::runtime::Controller` gives you a reconcile callback and requeue primitives — a solid foundation, but a blank slate. You're on your own for finalizer management, status condition upserts, ordered cleanup, backoff escalation, and threading data between logical steps. Every operator ends up reimplementing these patterns, usually slightly differently each time.

`kubo` encodes those patterns into a typed pipeline model that provides finalizer lifecycle, status patching, ordered cleanup, backoff, and a typed state bag.

## Licence

[Apache 2.0](LICENSE)

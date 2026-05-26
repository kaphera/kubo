<div align="center">
  <h1>kubo</h1>
  <p>
      <b>
        A complete, opinionated framework for building Kubernetes operators in Rust.
      </b>
  </p>
  <sub>
    Built with ❤️ at <a href="https://kaphera.com">Kaphera</a>.
  </sub>
</div>

---

Declare your reconcile logic as a typed, sequential pipeline of gates, effects, and polls. The framework handles finalizer lifecycle, status patching, backoff, and reverse-order cleanup — you focus on what your controller does, not the ceremony around it.

Every Kubernetes interaction goes through an `Environment` trait, so the entire reconcile pass is unit-testable in-memory without a cluster. A top-level `Runtime` wires together leader election, signal handling, and graceful drain so your `main.rs` stays thin.

## Usage

The runtime is your entry point. Register one listener per CRD kind:

```rust
use kubo::runtime::Runtime;

Runtime::new("my-operator", client, env)
    .with(reqwest::Client::new())
    .listener(VaultKvMountListener)
    .listener(PostgresDatabaseListener)
    .run()
    .await
```

Each listener implements the `Listener` trait — one struct per controller:

```rust
use std::sync::Arc;
use kubo::listener::{ControllerContext, Listener};
use kubo::reconcile::{ReadyReconcile, Reconcile};
use kubo::environment::{RealEnvironment, ObjectRef};
use kube::{Api, ResourceExt, runtime::{Controller, watcher::Config}};

pub struct VaultKvMountListener;

impl Listener<RealEnvironment, VaultKvMount> for VaultKvMountListener {
    type Status = VaultKvMountStatus;

    const NAME: &'static str = "vault-kv-mount";
    const FINALIZER: &'static str = "vaultkvmounts.cloud.kaphera.com/cleanup";

    fn controller(&self, ctx: &ControllerContext<RealEnvironment>) -> Controller<VaultKvMount> {
        let client = ctx.require::<kube::Client>().clone();
        Controller::new(Api::<VaultKvMount>::all(client.clone()), Config::default())
            .owns(Api::<Secret>::all(client), Config::default())
    }

    fn reconcile<'a>(
        &'a self,
        ctx: &'a ControllerContext<RealEnvironment>,
        obj: Arc<VaultKvMount>,
    ) -> ReadyReconcile<'a, RealEnvironment, VaultKvMount, VaultKvMountStatus> {
        let namespace = obj.namespace().unwrap_or_default();
        let spec = &obj.spec;

        Reconcile::new(ctx.env(), obj.clone())
            .ready_reason("Provisioned", "KV mount and access secret ready")
            .gate(ResolveVaultInstance::new(ObjectRef::new(
                &namespace,
                &spec.instance_ref,
            )))
            .effect(EnableKv2Mount::new(&obj))
            .effect(CreateAccessSecret::new(&obj))
            .poll(WaitForMountReady::new(&obj))
            .status(|state, _prev| VaultKvMountStatus {
                mount_path: state.get::<EnableKv2Mount>().cloned(),
            })
    }
}
```

The `Runtime` calls `.execute(FINALIZER)` on the returned pipeline automatically — you declare the what, the framework handles the when.

## Concepts

**Gate** — A read-only dependency check. Blocks the pipeline until satisfied, then inserts a typed value into the state bag for downstream steps. No cleanup obligation.

**Effect** — A write operation that creates or updates external state. Produces a typed value like a gate, and also declares a `cleanup` method called in reverse declaration order during deletion.

**Poll** — A read-only observation placed after effects. Waits for something this controller created to converge (e.g. a CNPG `Cluster` reaching `Ready`). No state output, no cleanup.

**Guard** — A gate whose output is evaluated but discarded. Use for precondition checks that don't produce data for downstream steps.

**State** — A typed, heterogeneous map that flows through the pipeline. Each gate and effect declares its output type; downstream steps retrieve values with `state.get::<UpstreamStep>()`.

**Listener** — Implements the `Listener<R>` trait, which declares an associated `Status` type, a `NAME` constant, a `FINALIZER` constant, a `controller()` method returning a `kube::runtime::Controller<R>`, and a `reconcile()` method returning a `ReadyReconcile`. One listener per CRD kind.

**Runtime** — Top-level entry point. Holds operator-wide `Extensions` (a type-keyed map of shared clients and config), registers listeners, manages leader election via a Coordination v1 `Lease`, handles OS signals, and drains in-flight reconciliations on shutdown.

## Pipeline Lifecycle

On **apply** (no deletion timestamp), the framework ensures the finalizer is present, runs each step in declaration order, and patches `ResourceStatus<T>` (which wraps your domain type with conditions and `observedGeneration`) after every step that changes it. Once all steps pass, it requeues at the configured steady-state interval.

On **cleanup** (deletion timestamp present), effect `cleanup` methods run in reverse declaration order. If any cleanup returns `Retry`, the pipeline requeues with backoff and resumes from that step on the next pass. The finalizer is removed only after all cleanups succeed.

`.execute(FINALIZER)` runs the full lifecycle. `.converge()` runs the pipeline only, with no finalizer management — useful for resources you don't own the deletion of.

`.rotation_annotation(key)` triggers a full re-run of effects when the annotation value changes, enabling credential rotation without a deletion/recreation cycle.

`.steady_state_interval(duration)` sets the requeue interval once the pipeline reaches a stable `Ready` state.

## Testing

`TestEnvironment` is an in-memory implementation of the `Environment` trait. Seed upstream resources, run your pipeline, and assert on resulting cluster state — no k3d, no kind, no waiting.

```rust
use kubo::testing::TestEnvironment;
use kubo::reconcile::Reconcile;

let env = TestEnvironment::new();
env.seed(make_vault_instance("my-vault", true));

let action = Reconcile::new(&env, obj)
    .gate(ResolveVaultInstance { .. })
    .effect(CreateMount { .. })
    .status(|state, _prev| MyStatus {
        mount_path: state.get::<CreateMount>().cloned(),
    })
    .execute(FINALIZER)
    .await?;

assert!(env.has::<Secret>(&ObjectRef::new("ns", "my-secret")));
```

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

`kubo` encodes those patterns into a typed pipeline model so that every controller in your operator behaves consistently, is testable without a cluster, and reads top-to-bottom as a declaration of intent rather than imperative ceremony.

## Licence

[Apache 2.0](LICENSE)

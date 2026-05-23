<div align="center">
  <h1>k8s-operator-reconciler</h1>
  <p>
      <b>
        A minimal, opinionated framework for building Kubernetes operator reconcile loops in Rust with <a href="https://github.com/kube-rs/kube">kube-rs</a>.
      </b>
  </p>
  <sub>
    Built with ❤️ at <a href="https://kaphera.com">Kaphera</a>.
  </sub>
</div>

---

Declare your reconcile logic as a typed, sequential pipeline of gates, effects, and polls. The framework handles finalizer lifecycle, status patching, backoff, and reverse-order cleanup — you focus on what your controller does, not the ceremony around it.

Every Kubernetes interaction goes through an `Environment` trait, so the entire reconcile pass is unit-testable in-memory without a cluster. Ship with confidence.

## Usage

```rust
use k8s_operator_reconciler::reconcile::Reconcile;
use k8s_operator_reconciler::utils::ConditionContext;

let cx = ConditionContext::new(generation)
    .with_ready_reason("Provisioned", "All resources created");

let action = Reconcile::new(&env, &cx, &mut conditions, obj)
    .gate(ResolveVaultInstance { namespace, instance_ref, cx })
    .effect(EnableKv2Mount { namespace, name, .. })
    .effect(CreateAccessSecret { namespace, secret_name, .. })
    .poll(WaitForMountReady { ready, cx })
    .run(FINALIZER, |conds, state, prev| MyStatus {
        observed_generation: Some(generation),
        conditions: conds.to_vec(),
        mount_path: state.get::<EnableKv2Mount>().cloned().or(|| prev.mount_path),
    })
    .await?;
```

## Concepts

**Gate** — A read-only dependency check. Blocks the pipeline until satisfied, then inserts a typed value into the state bag for downstream consumption. No cleanup obligation.

**Effect** — A write operation that creates or updates external state. Produces a typed value like a gate, but also declares a `cleanup` method. During deletion, the framework calls cleanup in reverse declaration order.

**Poll** — A read-only observation placed after effects. Waits for something this controller created to converge (e.g. a CNPG Cluster reaching `Ready`). No state output, no cleanup.

**Guard** — A gate whose output is evaluated but discarded. Use for precondition checks that don't produce data for downstream steps.

**State** — A typed, heterogeneous map that flows through the pipeline. Each gate and effect declares its output type via `StateKey`; downstream steps retrieve values with `state.require::<UpstreamStep>()`.

## Pipeline Lifecycle

On **apply** (no deletion timestamp), the framework ensures the finalizer is present, runs each step in declaration order, patches status after every step that changes it, and requeues at the steady-state interval once all steps pass.

On **cleanup** (deletion timestamp present), the framework runs effect cleanup methods in reverse order, then removes the finalizer. If a cleanup returns `Retry`, it requeues with backoff and resumes from that effect on the next pass.

## Testing

The `TestEnvironment` is an in-memory implementation of the `Environment` trait. Seed upstream resources, run your pipeline, and assert on the resulting cluster state and status progression — no k3d, no kind, no waiting.

```rust
let env = TestEnvironment::new();
env.seed(make_vault_instance("my-vault", /* ready = */ true));

let action = Reconcile::new(&env, &cx, &mut conditions)
    .gate(ResolveVaultInstance { .. })
    .effect(CreateMount { .. })
    .run(FINALIZER, obj, |conds, state| build_status(conds, state))
    .await?;

// Assert on cluster state:
assert!(env.has::<Secret>(&ObjectRef::new("ns", "my-secret")));

// Assert on status progression with property-based checks:
let ticks = env.status_ticks::<MyStatus>(&obj_ref);
assert_monotonic(&ticks, |s| s.mount_path.is_some(), "mount_path");
assert_causal(&ticks, |s| s.resolved_ref.is_some(), |s| s.mount_path.is_some(), "gate → effect");
assert_converges(&ticks, |s| is_ready(&s.conditions), "reaches Ready");
```

## Backoff Strategies

**`Fixed`** — Constant interval. Use when you expect readiness within a known timeframe.

**`Exponential`** — Duration-aware escalation derived from `lastTransitionTime` on the status condition, not a mutable counter. Stateless by design — the escalation position is reconstructed from cluster state on every pass.

## Build

```sh
cargo build
```

Run tests:

```sh
cargo test
```

Generate documentation:

```sh
cargo doc --open
```

## Why not just `kube::runtime::Controller` directly?

`kube::runtime::Controller` gives you a reconcile callback and requeue primitives. That's a solid foundation, but the callback is a blank slate — you're on your own for finalizer management, status condition upserts, ordered cleanup, backoff escalation, and passing data between logical steps. Every operator ends up re-implementing these patterns, usually slightly differently each time.

This crate encodes those patterns into a typed pipeline model so that every controller in your operator behaves consistently, is testable without a cluster, and reads top-to-bottom as a declaration of intent rather than imperative ceremony.

## Licence

[Apache 2.0](LICENSE)

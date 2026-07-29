# kubo

**A framework for building Kubernetes operators in Rust.**

<sub>Built with ❤️ at <a href="https://kaphera.com">Kaphera</a>.</sub>

[![CI](https://github.com/kaphera/kubo/actions/workflows/ci.yml/badge.svg)](https://github.com/kaphera/kubo/actions/workflows/ci.yml)

kubo is a framework for building Kubernetes operators in Rust on top of kube: client machinery, CRD types, and watches remain kube's, while kubo takes over the reconcile pipeline — gates, effects, polls, and guards — with finalizer lifecycle, status patching, backoff, and reverse-order cleanup.

## Documentation

The full documentation is the crate docs: run `cargo doc --open` and read the `kubo` crate. The narrative lives in the crate root, [crates/kubo/src/lib.rs](https://github.com/kaphera/kubo/blob/main/crates/kubo/src/lib.rs), which can also be read in-repo.

The crate is not yet published to crates.io, so there is no docs.rs page yet.

## Feature flags

`default` is empty.

`test-support` gates the testing harness — `kubo::testing` (the `World` simulated cluster and the property-assertion functions) and the `#[kubo::test]` / `#[kubo::property_test]` macros — and enables the `kubo-test-fakeapi`, `kubo-test-macros`, and `proptest` dependencies plus the `kube` and `tokio` features they need.

The minimum supported Rust version is 1.95.

## Testing

The testing harness lives behind the `test-support` feature; see the Testing section of the crate docs (`cargo doc --open`), or read [crates/kubo/src/testing.rs](https://github.com/kaphera/kubo/blob/main/crates/kubo/src/testing.rs).

Reusable effects are user-written composition, not framework API: kubo's public API contains no `ApplyConfigMap` and no `ConfigMapRenderer`. [crates/kubo/tests/apply_config_map.rs](https://github.com/kaphera/kubo/blob/main/crates/kubo/tests/apply_config_map.rs) defines both the way a controller author would — one generic `ApplyConfigMap<K, R>` effect plus a small per-controller `ConfigMapRenderer` trait — using only kubo's public API (`Effect`, `StateKey`, the state bag, `CleanupAction`, `KubeHandle`). The same effect type serves two controllers, and multiple instantiations compose within a single pipeline; the tests in that file prove the pattern end to end.

## Conformance

The fake-vs-real API-server divergences are pinned in a machine-checked registry: [CONFORMANCE.md](https://github.com/kaphera/kubo/blob/main/CONFORMANCE.md). The conformance suite lives in the [kubo-conformance](https://github.com/kaphera/kubo/tree/main/crates/kubo-conformance) crate, whose tests run against both the in-process fake and a real k3d cluster (the `k3d` feature belongs to that crate, not to kubo); the CI workflow runs both.

## Licence

Apache-2.0; see [LICENSE](https://github.com/kaphera/kubo/blob/main/LICENSE).

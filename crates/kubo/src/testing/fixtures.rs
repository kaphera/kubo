//! Test fixtures — typed CRDs used by kubo testing integration tests.
//!
//! `DummyResource` is a minimal custom resource that mirrors the shape of real
//! operator CRDs (spec field, optional status, conditions) without depending on
//! any domain logic. It is the canonical guinea pig for framework-level tests.
//!
//! `DummyListener` is the simplest possible [`Listener`]: no steps, sets
//! `Ready=True` on every reconcile pass.
//!
//! `BackoffDummyListener` is identical but sets a 30-second steady-state
//! requeue interval, making it useful for testing time-advance behaviour.

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::{CustomResource, api::ObjectMeta};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    HasConditions,
    listener::{ControllerContext, Listener},
    reconcile::{ReadyReconcile, Reconcile},
};

// ── DummyResource ─────────────────────────────────────────────────────────────

#[derive(CustomResource, Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "test.kubo.dev",
    version = "v1alpha1",
    kind = "DummyResource",
    status = "DummyStatus",
    namespaced
)]
pub struct DummySpec {
    pub field: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DummyStatus {
    pub ready: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    /// Set to `Some("reconciled")` on the first successful reconcile.
    ///
    /// Useful for monotonicity assertions: once set, it must not regress
    /// to `None`, even when the controller retries after a fault.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub touched: Option<String>,
}

// ── Constructor helpers ───────────────────────────────────────────────────────

/// Build a `DummyResource` object suitable for `Api::patch` / `World::create`.
pub fn dummy(name: &str, namespace: &str, field: &str) -> DummyResource {
    DummyResource {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        spec: DummySpec {
            field: field.to_string(),
        },
        status: None,
    }
}

/// Convenience wrapper around [`dummy`] with a default `field` value.
///
/// Use this in tests that only care about the object's identity, not its spec:
///
/// ```ignore
/// world.create(make_dummy("ns", "obj-1")).await;
/// ```
pub fn make_dummy(ns: &str, name: &str) -> DummyResource {
    dummy(name, ns, "default")
}

/// Build a `DummyResource` with a given [`DummySpec`], suitable for property
/// tests where the spec is strategy-generated.
///
/// ```ignore
/// fn my_test(
///     #[strategy(any_dummy_spec())] spec: DummySpec,
/// ) {
///     // ...
///     let obj_ref = world.seed_obj(dummy_from_spec("ns", "obj", spec));
/// }
/// ```
pub fn dummy_from_spec(ns: &str, name: &str, spec: DummySpec) -> DummyResource {
    DummyResource {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(ns.to_string()),
            ..Default::default()
        },
        spec,
        status: None,
    }
}

// ── HasConditions ─────────────────────────────────────────────────────────────

impl HasConditions for DummyResource {
    fn conditions(&self) -> Vec<k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition> {
        self.status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default()
    }
}

// ── DummyListener ─────────────────────────────────────────────────────────────

/// A minimal listener that reconciles [`DummyResource`] by setting `ready:
/// true` in status. All steps pass immediately, so every reconcile terminates
/// with `Ready=True` and requeues at the default 60-second steady-state
/// interval.
#[derive(Default)]
pub struct DummyListener;

impl Listener<DummyResource> for DummyListener {
    type Status = DummyStatus;
    const NAME: &'static str = "dummy-listener";
    const FINALIZER: &'static str = "dummyresources.test.kubo.dev/cleanup";

    fn reconcile<'a>(
        &'a self,
        _ctx: &'a ControllerContext,
        _obj: Arc<DummyResource>,
        rec: Reconcile<'a, DummyResource>,
    ) -> ReadyReconcile<'a, DummyResource, DummyStatus> {
        rec.status(|_state, _prev: &DummyStatus| DummyStatus {
            ready: true,
            touched: Some("reconciled".into()),
            conditions: vec![],
        })
    }
}

// ── BackoffDummyListener ──────────────────────────────────────────────────────

/// Like [`DummyListener`] but uses a 30-second steady-state requeue interval.
///
/// Useful for testing `World::advance()`: create an object, run until idle,
/// capture the status-tick count, advance 60 seconds, and assert that at
/// least one more tick was recorded.
#[derive(Default)]
pub struct BackoffDummyListener;

impl Listener<DummyResource> for BackoffDummyListener {
    type Status = DummyStatus;
    const NAME: &'static str = "backoff-dummy-listener";
    const FINALIZER: &'static str = "dummyresources.test.kubo.dev/cleanup";

    fn reconcile<'a>(
        &'a self,
        _ctx: &'a ControllerContext,
        _obj: Arc<DummyResource>,
        rec: Reconcile<'a, DummyResource>,
    ) -> ReadyReconcile<'a, DummyResource, DummyStatus> {
        rec.steady_state_interval(Duration::from_secs(30))
            .status(|_state, _prev: &DummyStatus| DummyStatus {
                ready: true,
                touched: Some("reconciled".into()),
                conditions: vec![],
            })
    }
}

// ── ListenerResource impls ────────────────────────────────────────────────────

impl crate::testing::ListenerResource for DummyListener {
    type Resource = DummyResource;
}

impl crate::testing::ListenerResource for BackoffDummyListener {
    type Resource = DummyResource;
}

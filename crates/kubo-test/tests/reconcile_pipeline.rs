//! Step 3 gate: a `Reconcile` pipeline runs end-to-end against the fake API server.
//!
//! This exercises the full `Reconcile::execute` path — finalizer management,
//! status patching, and the terminal Ready condition — without a real cluster.
//! The pipeline talks to [`FakeApiServer`] via a real `kube::Client`, so every
//! code path from the pipeline down to the HTTP bytes is production code.

use std::sync::Arc;

use kubo::reconcile::Reconcile;
use kubo_test::{
    World,
    fixtures::{DummyResource, DummyStatus, dummy},
};

#[tokio::test]
async fn reconcile_pipeline_runs_against_fake() {
    let world = World::builder().build().await;
    world.prefill(dummy("obj-1", "ns", "hello"));

    // Fetch the seeded object so the pipeline sees a real resourceVersion.
    let obj: DummyResource = world.get("ns", "obj-1").unwrap();
    let obj = Arc::new(obj);

    let env = world.env("kubo-test");

    let action = Reconcile::new(&env, obj)
        .status(|_state, _prev: &DummyStatus| DummyStatus {
            ready: true,
            touched: Some("reconciled".into()),
            conditions: vec![],
        })
        .execute("dummy.kubo.test/cleanup")
        .await
        .unwrap();

    // A successful apply pass requeues at the steady-state interval — not await_change.
    assert_ne!(
        action,
        kube::runtime::controller::Action::await_change(),
        "expected a requeue action, got await_change",
    );

    // The framework must have recorded a Ready=True condition on the object.
    assert!(
        world.condition::<DummyResource>("ns", "obj-1", "Ready"),
        "Ready condition not found after reconcile",
    );

    // At least one status tick must have been recorded by the fake.
    let ticks: Vec<DummyStatus> = world.status_ticks("ns", "obj-1");
    assert!(!ticks.is_empty(), "no status ticks recorded");
}

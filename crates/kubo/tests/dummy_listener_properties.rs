//! Property assertions over a real controller tick history.
//!
//! Verifies that `assert_converges` and `assert_monotonic` correctly evaluate
//! a [`DummyListener`] tick sequence collected from a running [`World`].
//!
//! The test uses paused time so the clock-advance assertion is deterministic
//! and free of real sleeps.

use std::time::Duration;

use kube::CustomResourceExt as _;
use kubo::testing::{
    World,
    fixtures::{DummyListener, DummyResource, DummyStatus, dummy},
    properties::{assert_converges, assert_monotonic, is_ready},
};

/// A [`DummyListener`] reconciles an object, converges to `Ready=True`, and
/// never regresses its `ready` flag across multiple reconcile passes.
///
/// This proves that:
/// - `World::status_ticks` returns a non-empty history once the listener has
///   reconciled.
/// - `assert_converges` passes when the final tick has `Ready=True`.
/// - `assert_monotonic` passes when `ready: true` never flips back to `false`.
/// - Both assertions hold after an additional `advance(120s)` that drives
///   further reconcile passes via the steady-state requeue timer.
#[tokio::test(start_paused = true)]
async fn dummy_listener_converges_and_is_monotonic() {
    let world = World::builder()
        .install_crd(DummyResource::crd())
        .listener::<DummyResource, _>(DummyListener)
        .build()
        .await;

    world.create(dummy("obj-1", "ns", "hello")).await;
    world.run_until_idle().await;

    // Advance 120 s past the 60-second steady-state requeue, driving at least
    // one additional reconcile pass and lengthening the tick history.
    world.advance(Duration::from_secs(120)).await;

    let ticks = world.status_ticks::<DummyStatus>("ns", "obj-1");
    assert!(
        !ticks.is_empty(),
        "expected at least one status tick after reconcile",
    );

    // The framework sets Ready=True in the final condition set when all steps pass.
    assert_converges(&ticks, |s| is_ready(&s.conditions), "obj-1 reaches Ready");

    // Once ready=true is set it should never revert to false across requeues.
    assert_monotonic(&ticks, |s| s.ready, "ready flag");
}

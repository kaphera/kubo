//! Step 5 gate: registered listeners reconcile in response to object lifecycle
//! events when running under a paused-time tokio runtime.
//!
//! These tests verify that:
//!
//! - A [`Listener`] spawned via [`WorldBuilder::listener`] processes a created
//!   object and sets `Ready=True` after [`World::run_until_idle`].
//! - A listener with a periodic requeue timer reconciles again after
//!   [`World::advance`] advances the paused clock past the requeue deadline.
//!
//! Both tests use `#[tokio::test(start_paused = true)]`.  With paused time,
//! no real timer sleeps are taken; `advance()` is the only way the clock
//! moves.  All tokio tasks (controller watcher loop, Tower buffer workers,
//! reconcile tasks) run cooperatively via `yield_now()` inside
//! `run_until_idle()`.

use std::time::Duration;

use kubo_test::{
    World,
    fixtures::{BackoffDummyListener, DummyListener, DummyResource, DummyStatus, dummy},
};

/// A [`DummyListener`] reconciles a newly created object and sets `Ready=True`
/// within a single `run_until_idle()` pass.
///
/// This proves that:
/// - `WorldBuilder::listener()` correctly spawns the controller loop.
/// - The loop receives the watch event for the created object.
/// - The reconcile pipeline completes and patches `Ready=True` status.
/// - `run_until_idle()` yields enough for the full pipeline to complete.
#[tokio::test(start_paused = true)]
async fn listener_reconciles_on_create() {
    let world = World::builder()
        .listener::<DummyResource, _>(DummyListener)
        .build()
        .await;

    world.create(dummy("obj-1", "ns", "hello")).await;
    world.run_until_idle().await;

    assert!(
        world.condition::<DummyResource>("ns", "obj-1", "Ready"),
        "Ready condition not set after run_until_idle",
    );
}

/// A [`BackoffDummyListener`] re-reconciles an object after time advances
/// past its 30-second steady-state requeue deadline.
///
/// This proves that:
/// - Timer-driven requeues (from `Action::requeue`) fire when `advance()` moves
///   the paused clock forward.
/// - Each reconcile pass records a status tick in the fake's history.
/// - `advance(60s)` fires the 30-second requeue timer at least once.
#[tokio::test(start_paused = true)]
async fn requeue_fires_after_advance() {
    let world = World::builder()
        .listener::<DummyResource, _>(BackoffDummyListener)
        .build()
        .await;

    world.create(dummy("obj-1", "ns", "hello")).await;
    world.run_until_idle().await;

    // Capture the tick count after the initial reconcile.
    let ticks_before = world.status_ticks::<DummyStatus>("ns", "obj-1").len();
    assert!(ticks_before > 0, "no initial status ticks recorded");

    // Advancing 60 seconds fires the 30-second requeue timer.
    world.advance(Duration::from_secs(60)).await;

    let ticks_after = world.status_ticks::<DummyStatus>("ns", "obj-1").len();
    assert!(
        ticks_after > ticks_before,
        "expected more status ticks after advance, got {ticks_after} (was {ticks_before})",
    );
}

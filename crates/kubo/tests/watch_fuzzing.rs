//! Watch fuzzing + concurrent reconcile scenario test.
//!
//! Verifies that `DummyListener` converges correctly across 128 seeds even when
//! watch events for multiple objects arrive in a shuffled order.  Each seed
//! produces a different event permutation, exercising a distinct interleaving
//! of `ADDED` / `MODIFIED` events across the four test objects.
//!
//! This is a **Layer 3** scenario test: it exercises the controller's
//! integration with the kube-rs event loop under pathological event orderings,
//! not the reconcile function's logic (which Layer 2 property tests in
//! `property_test.rs` already cover via direct reconcile).
//!
//! Most operator bugs in production come from "I assumed event A would arrive
//! before event B".  Watch fuzzing makes those assumptions surface
//! deterministically, so every controller built on the framework is forced
//! to tolerate event reordering.

use std::time::Duration;

use kubo_test::{
    fixtures::{DummyListener, DummyResource, DummyStatus, make_dummy},
    properties::{assert_converges, is_ready},
};

/// Scenario test: `DummyListener` converges all objects to `Ready=True` across
/// 128 distinct watch-event orderings.
///
/// The seeded RNG produces a different event permutation per iteration, covering
/// 128 distinct interleaving patterns of `ADDED` / `MODIFIED` events for four
/// objects.  A correct controller must converge regardless of arrival order.
///
/// Layer 3 test: uses the full kube-rs controller loop + watch integration.
/// Layer 2 property tests (`dummy_listener_well_behaved`) cover reconcile logic;
/// this test covers controller-loop resilience under event reordering.
#[kubo::test(iterations = 128)]
async fn dummy_listener_concurrent_safe(world: &mut World) {
    world.seed_listener(DummyListener);
    world.fuzz_watch_events(true);

    // Create several objects so watch events can be reordered across objects.
    for i in 0..4 {
        world.create(make_dummy("ns", &format!("obj-{i}"))).await;
    }

    // Advance enough to let the controller process all events under the
    // fuzzing schedule.  The seeded RNG produces a different ordering per
    // iteration — 128 seeds exercise 128 distinct event permutations.
    world
        .advance_stepping(Duration::from_secs(60), Duration::from_secs(6))
        .await;

    for obj_ref in world.list_refs::<DummyResource>() {
        let ticks = world.status_ticks_for::<DummyStatus>(&obj_ref);
        if !world.is_deleted::<DummyResource>(&obj_ref) {
            assert_converges(&ticks, |s| is_ready(&s.conditions), &obj_ref.to_string());
        }
    }
}

//! Fault injection.
//!
//! Verifies that a controller satisfies the convergence and monotonicity
//! properties even when the fake API server randomly returns 409 Conflict
//! responses on mutable operations.
//!
//! This is the framework's primary value proposition: a controller test that
//! opts into fault injection with a single line and asserts that the full
//! correctness specification still holds across many seeds.

use std::time::Duration;

use kubo_test::{
    Fault,
    fixtures::{DummyListener, DummyResource, DummyStatus, make_dummy},
    properties::assert_monotonic,
};

/// Property test: `DummyListener` converges to `Ready=True` and never regresses
/// its `touched` field across 32 seeds, even with 10% random Conflict faults.
///
/// The fault is installed after the initial `create` so the object definitely
/// reaches the store.  Subsequent PATCH operations (finalizer + status) are
/// each independently subject to the 10% Conflict rate, derived from the
/// world's seeded RNG.
///
/// Assertions:
/// - `world.condition::<DummyResource>("ns", "obj-1", "Ready")` — convergence.
/// - `assert_monotonic(..., |s| s.touched.is_some(), "touched")` — monotonicity:
///   once the controller records a status patch, `touched` never regresses.
#[kubo::test(iterations = 32)]
async fn dummy_listener_survives_transient_conflicts(world: &mut World) {
    world.seed_listener(DummyListener);

    // Create the object before installing the fault so `world.create` (itself
    // a PATCH) is not subject to the 10% conflict rate.
    world.create(make_dummy("ns", "obj-1")).await;

    // Activate the fault rule for all subsequent mutable requests.
    world.fault(Fault::Conflict).at_random_rate(0.10);

    // Advance 120 virtual seconds in 6 s steps (just past the 5 s error
    // requeue interval) so each retry gets its own tick window.
    world
        .advance_stepping(Duration::from_secs(120), Duration::from_secs(6))
        .await;

    assert!(
        world.condition::<DummyResource>("ns", "obj-1", "Ready"),
        "DummyResource must converge to Ready=True despite faults (seed {})",
        world.seed(),
    );
    assert_monotonic(
        &world.status_ticks::<DummyStatus>("ns", "obj-1"),
        |s| s.touched.is_some(),
        "touched",
    );
}

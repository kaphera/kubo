//! Verifies that `DummyListener` satisfies convergence and monotonicity
//! across arbitrary specs using direct reconcile invocation — no controller
//! loop, no watcher, no Tower stack. Each case calls `world.reconcile::<L>()`
//! directly: one async call per reconcile pass, milliseconds per case.
//!
//! Two passes per case establish idempotence: a second reconcile on the same
//! object must not regress any status field.
//!
//! A single `SEED=N` environment variable reproduces both the proptest case
//! sequence and the fake API server's RNG state byte-for-byte.

use kubo::testing::{
    fixtures::{DummyListener, DummyStatus, dummy_from_spec},
    properties::{assert_converges, assert_monotonic, is_ready},
    strategies::any_dummy_spec,
};
use kubo::property_test as kubo_property_test;

/// Property test: `DummyListener` converges and is monotonic on a single object
/// with a strategy-generated spec, using direct reconcile (Layer 2).
///
/// For the seeded object:
/// - After two reconcile passes status must have `Ready=True` (convergence).
/// - `touched` must not regress to `None` after it is first set (monotonicity).
#[kubo_property_test]
async fn dummy_listener_well_behaved(
    world: &mut World,
    #[strategy(any_dummy_spec())] spec: DummySpec,
) {
    let obj_ref = world.seed_obj(dummy_from_spec("ns", "obj", spec));

    // Direct reconcile — one async call, no controller loop.
    world.reconcile::<DummyListener>(&obj_ref).await.unwrap();

    // Second pass verifies idempotence.
    world.reconcile::<DummyListener>(&obj_ref).await.unwrap();

    let ticks = world.status_ticks_for::<DummyStatus>(&obj_ref);
    assert_converges(&ticks, |s| is_ready(&s.conditions), "ready");
    assert_monotonic(&ticks, |s| s.touched.is_some(), "touched");
}

/// Property test: convergence holds even for an object that starts life without
/// a `Ready` condition (no pre-existing status at all).
///
/// This case is trivially true for `DummyListener` but acts as a regression
/// guard: if someone changes the reconcile function to require a prior status,
/// this test will catch it at Layer 2 without needing a controller loop.
#[kubo_property_test]
async fn dummy_listener_converges_from_blank(
    world: &mut World,
    #[strategy(any_dummy_spec())] spec: DummySpec,
) {
    // Explicitly seeded with no status field — mirrors what the API server
    // returns for a freshly created resource.
    let obj_ref = world.seed_obj(dummy_from_spec("ns", "blank", spec));

    world.reconcile::<DummyListener>(&obj_ref).await.unwrap();

    let ticks = world.status_ticks_for::<DummyStatus>(&obj_ref);
    assert_converges(&ticks, |s| is_ready(&s.conditions), "ready");
}

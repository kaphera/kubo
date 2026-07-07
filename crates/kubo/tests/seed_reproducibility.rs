//! Seed reproducibility.
//!
//! Verifies:
//! - `#[kubo_test(iterations = N)]` runs the test body N times with
//!   deterministic seeds `0..N`.
//! - `World::seed()` returns the seed used for the current run.
//! - A deliberately flaky test fails at a known seed and prints the canonical
//!   "rerun from this seed" message (manual gate; passes in normal CI).

use kubo::testing::fixtures::{DummyListener, DummyResource, dummy};
use kubo::test as kubo_test;

/// Property check: for all seeds 0..64 the `DummyListener` converges.
///
/// This exercises both the iteration loop and the seed plumbing.  Each
/// iteration receives a fresh `World` with a distinct seed; since
/// `DummyListener` is deterministic and always sets `Ready=True`, all 64
/// must pass.
#[kubo_test(iterations = 64)]
async fn dummy_listener_is_reproducible(world: &mut World) {
    world.seed_listener(DummyListener);
    world.create(dummy("obj-1", "ns", "hello")).await;
    world.run_until_idle().await;
    assert!(
        world.condition::<DummyResource>("ns", "obj-1", "Ready"),
        "DummyResource should converge to Ready=True at seed {}",
        world.seed(),
    );
}

/// Regression gate for the seed-based failure-reporting mechanism.
///
/// Passes with the default seed (`0`).  Fails at seed `7193847216284`,
/// printing:
///
/// ```text
/// seed = 7193847216284
/// You can rerun from this seed by setting the SEED environment variable to 7193847216284
/// ```
///
/// **Manual gate** (expected to fail):
/// ```sh
/// SEED=7193847216284 cargo test --workspace flaky_regression -- --nocapture
/// ```
#[kubo_test]
async fn flaky_regression(world: &mut World) {
    // At this specific seed, deliberately fail to exercise the seed-reporting
    // mechanism.  With any other seed (including the default 0) the test
    // passes normally, so it does not disrupt CI.
    if world.seed() == 7_193_847_216_284 {
        panic!("deliberate failure — exercising seed-based failure reporting");
    }
    world.seed_listener(DummyListener);
    world.create(dummy("obj-1", "ns", "hello")).await;
    world.run_until_idle().await;
    assert!(
        world.condition::<DummyResource>("ns", "obj-1", "Ready"),
        "DummyResource should be Ready=True",
    );
}

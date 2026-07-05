//! `#[kubo::test]` macro with single- and multi-world forms.
//!
//! Verifies that:
//! - The macro correctly wraps an async function in paused-time tokio.
//! - `World::seed_listener` registers a listener post-construction.
//! - A single-world test converges a `DummyResource` to `Ready=True`.
//! - A two-world test drives independent clusters on the same runtime.

use kubo_test::fixtures::{DummyListener, DummyResource, dummy};

#[kubo::test]
async fn converges(world: &mut World) {
    world.seed_listener::<DummyResource, _>(DummyListener);
    world.create(dummy("obj-1", "ns", "hello")).await;
    world.run_until_idle().await;
    assert!(
        world.condition::<DummyResource>("ns", "obj-1", "Ready"),
        "DummyResource should converge to Ready=True",
    );
}

#[kubo::test]
async fn two_clusters_share_one_simulation(world_a: &mut World, world_b: &mut World) {
    world_a.seed_listener::<DummyResource, _>(DummyListener);
    world_b.seed_listener::<DummyResource, _>(DummyListener);
    world_a.create(dummy("a", "ns", "hello")).await;
    world_b.create(dummy("b", "ns", "hello")).await;
    world_a.run_until_idle().await;
    world_b.run_until_idle().await;
    assert!(
        world_a.condition::<DummyResource>("ns", "a", "Ready"),
        "world_a: DummyResource 'a' should be Ready",
    );
    assert!(
        world_b.condition::<DummyResource>("ns", "b", "Ready"),
        "world_b: DummyResource 'b' should be Ready",
    );
}

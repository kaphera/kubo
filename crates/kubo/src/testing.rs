//! Property-based testing utilities for reconcile pipelines.
//!
//! The [`World`] type runs a full `Reconcile` pipeline against an
//! in-memory store backed by [`FakeApiServer`](kubo_test_fakeapi::FakeApiServer) —
//! no cluster needed.  In production, a `KubeHandle` wraps a real `kube::Client`;
//! in tests, [`World::env`] constructs one around the fake's client, and the
//! same production code paths run through the fake.
//!
//! Property assertion functions live in [`properties`].
//! Import them from there:
//!
//! ```ignore
//! use kubo::testing::properties::*;
//!
//! let ticks: Vec<MyStatus> = world.status_ticks("ns", "obj");
//!
//! assert_monotonic(&ticks, |s| s.mount_path.is_some(), "mount_path");
//! assert_converges(&ticks, |s| is_ready(&s.conditions), "reaches Ready");
//! ```

pub mod fixtures;
pub mod properties;
pub mod strategies;
pub mod world;

pub use world::{ListenerResource, ObjRef, World, WorldBuilder};

pub use strategies::Op;

pub use kubo_test_fakeapi::{Fault, FaultBuilder};

pub use kubo_test_macros::{property_test, test};

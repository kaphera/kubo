//! Property-based testing utilities for reconcile pipelines.
//!
//! The [`TestEnvironment`] lets you run a full `Reconcile` pipeline against an
//! in-memory store — no cluster needed.
//!
//! For full controller-loop simulation, use [`World`]: it owns a
//! [`FakeApiServer`](kubo_test_fakeapi::FakeApiServer) and a real `kube::Client`
//! wired to it, so every reconcile pipeline the test drives runs the same
//! production code paths it would in a pod.
//!
//! Property assertion functions live in [`properties`](self::properties).
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

pub mod test_environment;

pub mod fixtures;
pub mod properties;
pub mod strategies;
pub mod world;

pub use test_environment::{StatusPatch, TestEnvironment};

pub use world::{ListenerResource, ObjRef, World, WorldBuilder};

pub use strategies::Op;

pub use kubo_test_fakeapi::{Fault, FaultBuilder};

pub use kubo_test_macros::{property_test, test};

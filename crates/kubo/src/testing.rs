//! Property-based testing utilities for reconcile pipelines.
//!
//! The [`TestEnvironment`] lets you run a full `Reconcile` pipeline against an
//! in-memory store — no cluster needed.
//!
//! Property assertion functions live in [`kubo_test::properties`](kubo_test::properties).
//! Import them from there:
//!
//! ```ignore
//! use kubo_test::properties::*;
//!
//! let ticks: Vec<MyStatus> = world.status_ticks("ns", "obj");
//!
//! assert_monotonic(&ticks, |s| s.mount_path.is_some(), "mount_path");
//! assert_converges(&ticks, |s| is_ready(&s.conditions), "reaches Ready");
//! ```

pub mod test_environment;

pub use test_environment::TestEnvironment;

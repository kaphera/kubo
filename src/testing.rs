//! Property-based testing utilities for reconcile pipelines.
//!
//! The [`properties`] module provides assertion functions that verify temporal
//! invariants over a controller's status progression. Combined with
//! [`TestEnvironment::status_ticks`](test_environment::TestEnvironment::status_ticks),
//! these let you write tests that prove your controller behaves correctly
//! regardless of the specific inputs — not just for one happy path.
//!
//! # Available Properties
//!
//! | Property       | What it checks                                             |
//! |----------------|------------------------------------------------------------|
//! | `monotonic`    | Once a predicate becomes true, it never regresses to false |
//! | `causal`       | An effect is never observed before its cause               |
//! | `converges`    | The final tick satisfies the target predicate              |
//! | `never`        | A predicate is never true in any tick                      |
//! | `eventually`   | A predicate becomes true in at least one tick              |
//! | `ordered`      | One predicate becomes true strictly before another         |
//!
//! Each property has two variants: `assert_*` (panics on violation) and
//! `check_*` (returns `Result` for use with proptest's `prop_assert!`).
//!
//! # Example
//!
//! ```ignore
//! use k8s_operator_reconciler::testing::properties::*;
//!
//! let ticks: Vec<MyStatus> = env.status_ticks(&obj_ref);
//!
//! // The mount_path field, once populated, must never disappear:
//! assert_monotonic(&ticks, |s| s.mount_path.is_some(), "mount_path");
//!
//! // The effect output must not appear before the gate output:
//! assert_causal(
//!     &ticks,
//!     |s| s.resolved_ref.is_some(),
//!     |s| s.mount_path.is_some(),
//!     "gate output must precede effect output",
//! );
//!
//! // The pipeline must reach Ready:
//! assert_converges(&ticks, |s| is_ready(&s.conditions), "reaches Ready");
//! ```

pub mod properties;
pub mod test_environment;

pub use test_environment::TestEnvironment;

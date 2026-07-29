//! Reusable property assertions over status tick histories.
//!
//! Each function takes a `&[S]` (the tick sequence from
//! [`World::status_ticks`](crate::testing::World::status_ticks))
//! and a predicate, and panics with a descriptive message if the property
//! is violated.
//!
//! The `assert_*` variants panic immediately — use them in standard `#[tokio::test]`
//! functions. The `check_*` variants return `Result<(), PropertyViolation>` for
//! integration with proptest's `prop_assert!` macro.
//!
//! # Combining Properties
//!
//! Properties compose naturally. A typical controller test asserts several
//! properties over the same tick sequence to build confidence that the
//! pipeline behaves correctly:
//!
//! ```ignore
//! let ticks: Vec<MyStatus> = world.status_ticks("ns", "obj");
//!
//! // Structural: fields never regress once populated.
//! assert_monotonic(&ticks, |s| s.url.is_some(), "url");
//! assert_monotonic(&ticks, |s| s.secret_ref.is_some(), "secret_ref");
//!
//! // Causal: secret cannot appear before the URL it depends on.
//! assert_causal(&ticks, |s| s.url.is_some(), |s| s.secret_ref.is_some(), "url → secret");
//!
//! // Terminal: the final state must be ready.
//! assert_converges(&ticks, |s| is_ready(&s.conditions), "ready");
//!
//! // Safety: we must never publish a status with both fields absent after
//! // either has been populated (no full regression).
//! assert_never(&ticks[1..], |s| s.url.is_none() && s.secret_ref.is_none(), "full wipe");
//! ```

use std::fmt;

use crate::environment::EnvResource;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;

use crate::testing::World;

/// A property check result.
pub type PropertyResult = Result<(), PropertyViolation>;

/// Describes a property violation with enough context to debug.
#[derive(Debug, Clone)]
pub struct PropertyViolation {
    /// The name of the violated property (e.g. `"monotonic"`, `"causal"`).
    pub property: &'static str,
    /// The caller-supplied label describing what was checked.
    pub label: String,
    /// The index in the tick sequence where the violation occurred, when a
    /// single offending tick applies.
    pub tick_index: Option<usize>,
    /// The description of the violation.
    pub detail: String,
}

impl fmt::Display for PropertyViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.tick_index {
            Some(i) => write!(
                f,
                "property '{}' violated at tick {}: {} [{}]",
                self.property, i, self.detail, self.label
            ),
            None => write!(
                f,
                "property '{}' violated: {} [{}]",
                self.property, self.detail, self.label
            ),
        }
    }
}

impl std::error::Error for PropertyViolation {}

// ─── is_ready ────────────────────────────────────────────────────────

/// Returns `true` if `conditions` contains a condition with `type == "Ready"`
/// and `status == "True"`.
///
/// Use this as a predicate with [`assert_converges`]:
///
/// ```ignore
/// assert_converges(&ticks, |s| is_ready(&s.conditions), "reaches Ready");
/// ```
pub fn is_ready(conditions: &[Condition]) -> bool {
    conditions
        .iter()
        .any(|c| c.type_ == "Ready" && c.status == "True")
}

// ─── assert_no_leaks ─────────────────────────────────────────────────

/// Asserts that the named object carries no finalizer equal to `finalizer`.
///
/// Use this after deleting an object and running until idle to verify that
/// the controller cleaned up correctly:
///
/// ```ignore
/// world.delete::<DummyResource>("ns", "obj-1").await;
/// world.run_until_idle().await;
/// assert_no_leaks::<DummyResource>(&world, "ns", "obj-1", DummyListener::FINALIZER);
/// ```
///
/// The assertion passes if the object no longer exists (it was fully
/// garbage-collected) or if it still exists but carries no matching finalizer.
pub fn assert_no_leaks<T: EnvResource>(world: &World, ns: &str, name: &str, finalizer: &str) {
    let Some(obj) = world.get::<T>(ns, name) else {
        // Object was deleted entirely — no leaks possible.
        return;
    };
    let finalizers = obj.meta().finalizers.clone().unwrap_or_default();
    assert!(
        !finalizers.iter().any(|f| f == finalizer),
        "finalizer '{finalizer}' still present on {ns}/{name} after deletion",
    );
}

// ─── Monotonic ───────────────────────────────────────────────────────

/// Once `predicate` becomes true, it must remain true for all
/// subsequent ticks. Verifies that state never regresses.
pub fn assert_monotonic<S>(ticks: &[S], predicate: impl Fn(&S) -> bool, label: &str) {
    check_monotonic(ticks, predicate, label).unwrap();
}

/// Check variant that returns a `PropertyResult` for proptest usage.
pub fn check_monotonic<S>(
    ticks: &[S],
    predicate: impl Fn(&S) -> bool,
    label: &str,
) -> PropertyResult {
    let mut became_true = false;
    for (i, tick) in ticks.iter().enumerate() {
        let holds = predicate(tick);
        if holds {
            became_true = true;
        } else if became_true {
            return Err(PropertyViolation {
                property: "monotonic",
                label: label.to_string(),
                tick_index: Some(i),
                detail: "predicate regressed to false after being true".to_string(),
            });
        }
    }
    Ok(())
}

// ─── Causal ──────────────────────────────────────────────────────────

/// If `effect` is true at tick N, then `cause` must be true at some
/// tick M where M <= N. Verifies causal dependency between fields.
pub fn assert_causal<S>(
    ticks: &[S],
    cause: impl Fn(&S) -> bool,
    effect: impl Fn(&S) -> bool,
    label: &str,
) {
    check_causal(ticks, cause, effect, label).unwrap();
}

/// Check variant that returns a `PropertyResult` for proptest usage.
pub fn check_causal<S>(
    ticks: &[S],
    cause: impl Fn(&S) -> bool,
    effect: impl Fn(&S) -> bool,
    label: &str,
) -> PropertyResult {
    let mut cause_seen = false;
    for (i, tick) in ticks.iter().enumerate() {
        if cause(tick) {
            cause_seen = true;
        }
        if effect(tick) && !cause_seen {
            return Err(PropertyViolation {
                property: "causal",
                label: label.to_string(),
                tick_index: Some(i),
                detail: "effect observed before cause".to_string(),
            });
        }
    }
    Ok(())
}

// ─── Converges ───────────────────────────────────────────────────────

/// The last tick in the sequence must satisfy `predicate`.
/// Verifies that the pipeline reaches its target state.
pub fn assert_converges<S>(ticks: &[S], predicate: impl Fn(&S) -> bool, label: &str) {
    check_converges(ticks, predicate, label).unwrap();
}

/// Check variant that returns a `PropertyResult` for proptest usage.
pub fn check_converges<S>(
    ticks: &[S],
    predicate: impl Fn(&S) -> bool,
    label: &str,
) -> PropertyResult {
    match ticks.last() {
        None => Err(PropertyViolation {
            property: "converges",
            label: label.to_string(),
            tick_index: None,
            detail: "no ticks recorded".to_string(),
        }),
        Some(last) if !predicate(last) => Err(PropertyViolation {
            property: "converges",
            label: label.to_string(),
            tick_index: Some(ticks.len() - 1),
            detail: "final tick does not satisfy predicate".to_string(),
        }),
        _ => Ok(()),
    }
}

// ─── Never ───────────────────────────────────────────────────────────

/// No tick in the sequence may satisfy `predicate`.
/// Verifies that an illegal state is never reached.
pub fn assert_never<S>(ticks: &[S], predicate: impl Fn(&S) -> bool, label: &str) {
    check_never(ticks, predicate, label).unwrap();
}

/// Check variant that returns a `PropertyResult` for proptest usage.
pub fn check_never<S>(ticks: &[S], predicate: impl Fn(&S) -> bool, label: &str) -> PropertyResult {
    for (i, tick) in ticks.iter().enumerate() {
        if predicate(tick) {
            return Err(PropertyViolation {
                property: "never",
                label: label.to_string(),
                tick_index: Some(i),
                detail: "predicate was true (should never be)".to_string(),
            });
        }
    }
    Ok(())
}

// ─── Eventually ──────────────────────────────────────────────────────

/// At least one tick must satisfy `predicate`.
pub fn assert_eventually<S>(ticks: &[S], predicate: impl Fn(&S) -> bool, label: &str) {
    check_eventually(ticks, predicate, label).unwrap();
}

/// Check variant that returns a `PropertyResult` for proptest usage.
pub fn check_eventually<S>(
    ticks: &[S],
    predicate: impl Fn(&S) -> bool,
    label: &str,
) -> PropertyResult {
    for tick in ticks {
        if predicate(tick) {
            return Ok(());
        }
    }
    Err(PropertyViolation {
        property: "eventually",
        label: label.to_string(),
        tick_index: None,
        detail: "predicate never became true in any tick".to_string(),
    })
}

// ─── Ordered ─────────────────────────────────────────────────────────

/// `first` must become true at a tick index strictly before where
/// `second` first becomes true. Verifies temporal ordering.
pub fn assert_ordered<S>(
    ticks: &[S],
    first: impl Fn(&S) -> bool,
    second: impl Fn(&S) -> bool,
    label: &str,
) {
    check_ordered(ticks, first, second, label).unwrap();
}

/// Check variant that returns a `PropertyResult` for proptest usage.
pub fn check_ordered<S>(
    ticks: &[S],
    first: impl Fn(&S) -> bool,
    second: impl Fn(&S) -> bool,
    label: &str,
) -> PropertyResult {
    let first_at = ticks.iter().position(first);
    let second_at = ticks.iter().position(second);

    match (first_at, second_at) {
        (_, None) => Ok(()), // Second never happens — no violation.
        (None, Some(j)) => Err(PropertyViolation {
            property: "ordered",
            label: label.to_string(),
            tick_index: Some(j),
            detail: "second became true but first was never true".to_string(),
        }),
        (Some(i), Some(j)) if i > j => Err(PropertyViolation {
            property: "ordered",
            label: label.to_string(),
            tick_index: Some(j),
            detail: format!("second became true at tick {j} before first at tick {i}"),
        }),
        _ => Ok(()),
    }
}

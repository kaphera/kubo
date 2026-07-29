//! Typed state bag for passing data between pipeline steps.
//!
//! The state bag is the mechanism by which gates and effects communicate their
//! outputs to downstream steps without coupling them directly. Each gate or
//! effect registered with [`Reconcile::gate`](crate::reconcile::Reconcile::gate)
//! or [`Reconcile::effect`](crate::reconcile::Reconcile::effect) declares a
//! [`StateKey`] with an associated `Value` type; the framework inserts the
//! value after a successful step, and downstream steps retrieve it by key
//! type. [`Reconcile::gate_as`](crate::reconcile::Reconcile::gate_as) stores
//! the value under a separate marker key rather than the gate's own type.
//!
//! This design gives you compile-time type safety on the values flowing through
//! the pipeline, while keeping steps fully decoupled — a step only knows about
//! the key type it reads from, not the step that produced it.
//!
//! # Usage in Effects
//!
//! ```rust,ignore (illustrative fragment: bare method sketch outside its impl)
//! // In your effect's apply method:
//! async fn apply(&self, cx: &ReconcileContext<K>, handle: &KubeHandle, state: &State) -> Result<EffectOutcome<String>, Self::Error> {
//!     let upstream_url = state.require::<ResolveDatabase>()?;
//!     // Use upstream_url to configure this effect...
//!     Ok(EffectOutcome::Done(format!("{upstream_url}/my-table")))
//! }
//! ```
//!
//! The `require` method returns `Err(StateError::Missing { .. })` with a
//! human-readable message if the key is absent — this catches pipeline
//! mis-ordering at runtime with a clear diagnostic.

use std::{
    any::{Any, TypeId, type_name},
    collections::HashMap,
};

/// Marker trait that associates a pipeline step with its output type.
///
/// Implement this on your gate or effect struct. The `Value` type is what
/// gets stored in and retrieved from the [`State`] bag.
///
/// ```rust
/// # use kubo::state::StateKey;
/// struct ResolveVaultInstance { /* fields */ }
///
/// impl StateKey for ResolveVaultInstance {
///     type Value = String; // the Vault address
/// }
/// ```
///
/// When you need multiple instances of the same gate type in a single pipeline
/// (e.g. resolving two different upstream resources), create a zero-sized marker
/// struct as an alternative key and use `.gate_as::<MarkerKey, _>(gate)` on the
/// reconciler builder.
pub trait StateKey: 'static {
    /// The type of value this key produces.
    type Value: Any + Send + Sync;
}

/// Error returned when a required state key is missing.
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// The requested key was never inserted — either the producing step has not
    /// run yet (pipeline mis-ordering) or it returned `Wait`/`Err` and never
    /// reached `Pass`/`Done`.
    #[error("state key '{key}' missing (expected value type {expected})")]
    Missing {
        /// The type name of the [`StateKey`] that was never inserted.
        key: &'static str,
        /// The type name of the value the key was expected to hold.
        expected: &'static str,
    },
}

/// A type-safe, heterogeneous map keyed by [`StateKey`] implementors.
///
/// The framework creates a fresh `State` at the start of each apply pass.
/// Gates and effects insert their outputs; downstream steps read them.
/// Steps receive the bag from the framework, and controller code is not
/// expected to construct or mutate `State` directly — the framework manages
/// its lifecycle.
#[derive(Default)]
pub struct State(HashMap<TypeId, Box<dyn Any + Send + Sync>>);

impl State {
    /// Create an empty state bag.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a value under the given key type, replacing any previous value.
    pub fn insert<K: StateKey>(&mut self, value: K::Value) {
        self.0.insert(TypeId::of::<K>(), Box::new(value));
    }

    /// Retrieve a reference to the value for key `K`, or `None` if absent.
    pub fn get<K: StateKey>(&self) -> Option<&K::Value> {
        self.0.get(&TypeId::of::<K>())?.downcast_ref::<K::Value>()
    }

    /// Retrieve a reference to the value for key `K`, or return a descriptive
    /// error identifying the missing key and expected type by name.
    ///
    /// Prefer this over `get` in effect implementations — the error message
    /// makes pipeline mis-ordering immediately obvious in logs.
    pub fn require<K: StateKey>(&self) -> Result<&K::Value, StateError> {
        self.get::<K>().ok_or_else(|| StateError::Missing {
            key: type_name::<K>(),
            expected: type_name::<K::Value>(),
        })
    }

    /// Check whether a value for key `K` has been inserted.
    pub fn contains<K: StateKey>(&self) -> bool {
        self.0.contains_key(&TypeId::of::<K>())
    }
}

/// Framework-provided key that signals an annotation-triggered rotation was
/// requested on the reconciled resource.
///
/// Effects check `state.contains::<RotationRequested>()` to decide whether
/// they should regenerate credentials, even when the resource spec is otherwise
/// unchanged. After a fully-successful apply pass the framework removes the
/// annotation via merge patch (a failed removal is logged and non-fatal), so
/// that the effect does not fire on every subsequent reconciliation.
pub struct RotationRequested;

impl StateKey for RotationRequested {
    type Value = ();
}

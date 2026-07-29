//! Type-keyed extension storage shared across the runtime.
//!
//! [`Extensions`] is a map keyed by value type, in the same pattern as
//! `http::Extensions`. The [`Runtime`](crate::runtime::Runtime) owns one
//! instance and shares it with every listener through
//! [`ListenerContext`](crate::listener::ListenerContext) and
//! [`RuntimeContext`](crate::runtime::RuntimeContext).

use std::{
    any::{Any, TypeId, type_name},
    collections::HashMap,
};

/// A type-keyed map for operator-specific state.
///
/// Unlike the pipeline [`State`](crate::state::State) (which uses [`StateKey`](crate::state::StateKey)
/// marker types for step-to-step communication), `Extensions` keys directly
/// by value type. Insert a `reqwest::Client` and retrieve it as
/// `ctx.get::<reqwest::Client>()`.
///
/// This is the same pattern as `http::Extensions` / tower layers.
#[derive(Default)]
pub struct Extensions {
    map: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl Extensions {
    /// Creates an empty extension map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a value, keyed by its concrete type. Replaces any previous
    /// value of the same type.
    pub fn insert<T: Send + Sync + 'static>(&mut self, val: T) {
        self.map.insert(TypeId::of::<T>(), Box::new(val));
    }

    /// Retrieve a reference to a value by type, or `None` if absent.
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.map.get(&TypeId::of::<T>())?.downcast_ref::<T>()
    }

    /// Retrieve a reference to a value by type, panicking with a clear
    /// message if absent. Use when the value is guaranteed to be present
    /// (e.g. inserted at startup).
    pub fn require<T: Send + Sync + 'static>(&self) -> &T {
        self.get::<T>().unwrap_or_else(|| {
            panic!(
                "required extension `{}` not found in context",
                type_name::<T>()
            )
        })
    }
}

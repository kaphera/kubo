//! Proptest strategies for kubo controller property tests.
//!
//! The centrepiece is [`Op<T>`], which models a single operation on the
//! simulated cluster.  [`op_sequence`] combines it into a `Vec` strategy
//! suitable for driving a listener through arbitrary lifecycles while
//! asserting correctness properties on every step.

use kube::api::ObjectMeta;
use proptest::prelude::*;

use crate::fixtures::{DummyResource, DummySpec};

// ── Op ────────────────────────────────────────────────────────────────────────

/// A single operation on a simulated Kubernetes cluster.
///
/// Used with [`World::apply_op`](crate::World::apply_op) to drive a listener
/// through an arbitrary sequence of creates and deletes during a property test.
#[derive(Clone, Debug)]
pub enum Op<T> {
    /// Apply (create or update) a resource.
    Create(T),
    /// Delete the resource identified by `namespace` and `name`.
    ///
    /// If the object does not exist, the operation is silently ignored so that
    /// `Delete` operations before a matching `Create` do not abort the test.
    Delete { namespace: String, name: String },
}

// ── Strategies ────────────────────────────────────────────────────────────────

/// Strategy for arbitrary [`DummySpec`] values.
///
/// The `field` is a short alphanumeric string drawn from a bounded alphabet so
/// that proptest's shrinker can effectively reduce failing cases.
pub fn any_dummy_spec() -> impl Strategy<Value = DummySpec> {
    "[a-z0-9]{1,8}".prop_map(|field| DummySpec { field })
}

/// Strategy for a sequence of up to `max_ops` operations on [`DummyResource`].
///
/// Object names are drawn from a small pool (`obj-0`…`obj-3`) so that
/// Create/Delete pairs naturally interact — some sequences create an object,
/// delete it, then create it again, exercising the full finalizer lifecycle.
///
/// The Create/Delete ratio is 3:2 so most sequences contain more creates than
/// deletes, giving the listener a chance to converge before cleanup.
pub fn op_sequence(max_ops: usize) -> impl Strategy<Value = Vec<Op<DummyResource>>> {
    let ns = Just("ns".to_string());
    let name = prop_oneof![Just("obj-0"), Just("obj-1"), Just("obj-2"), Just("obj-3"),]
        .prop_map(str::to_string);

    let create = (ns.clone(), name.clone(), any_dummy_spec()).prop_map(|(ns, nm, spec)| {
        Op::Create(DummyResource {
            metadata: ObjectMeta {
                namespace: Some(ns),
                name: Some(nm),
                ..Default::default()
            },
            spec,
            status: None,
        })
    });

    let delete = (ns, name).prop_map(|(namespace, name)| Op::Delete { namespace, name });

    let op = prop_oneof![
        3 => create,
        2 => delete,
    ];

    proptest::collection::vec(op, 0..=max_ops)
}

/// Like [`op_sequence`] but intended for use alongside fault injection.
///
/// Currently identical to [`op_sequence`]; fault rates are configured
/// separately via [`World::fault`](crate::World::fault).  This alias exists so
/// that test authors can signal intent and future steps can bias the sequence
/// towards operations that exercise fault paths (e.g. PATCH-heavy sequences).
pub fn faulty_op_sequence(max_ops: usize) -> impl Strategy<Value = Vec<Op<DummyResource>>> {
    op_sequence(max_ops)
}

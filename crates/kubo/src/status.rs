//! Generic status wrapper used by the reconcile framework.
//!
//! [`ResourceStatus<T>`] provides the standard Kubernetes status fields
//! (`conditions` and `observedGeneration`) alongside resource-specific fields
//! via `#[serde(flatten)]`. The reconcile framework wraps the user's domain
//! type `T` in this automatically — the status closure only needs to return `T`.
//!
//! Conditions come from the CR type's [`HasConditions`](crate::HasConditions)
//! implementation, which seeds the pipeline's initial conditions;
//! [`ResourceStatus<T>`] carries them in every patch.
//!
//! # Pending-cleanup ledger
//!
//! `pending_cleanup` is a framework-owned list of effect names that have
//! successfully completed `apply` but whose `cleanup` has not yet run. The
//! reconcile loop seeds it from the previous status at the start of each
//! apply pass (so a `Wait` at an early gate never truncates the record),
//! appends to it as each effect returns `Done`, and filters the cleanup walk
//! by it so `cleanup` only runs for effects that actually applied.
//!
//! Entries are struck off after a successful cleanup and the shrunk ledger
//! is patched back to status, giving real resumability across cleanup
//! retries. Orphaned entries — names that no longer match a registered
//! cleanup step, e.g. after an effect rename — are logged and dropped;
//! blocking deletion forever on absent code is worse.
//!
//! **CRD regeneration is mandatory** when adopting a release that adds
//! this field. Kubernetes structural-schema pruning silently strips
//! unknown status fields, so a stale CRD will make the apiserver drop
//! `pendingCleanup` on every patch — the framework will then see an empty
//! ledger, skip every effect cleanup, and proceed with deletion, leaking
//! the external resources.
//! `#[serde(default, skip_serializing_if = "Vec::is_empty")]` keeps empty
//! ledgers off the wire, but it cannot protect against a missing CRD
//! schema entry.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
/// The framework-owned status wrapper: the standard Kubernetes status fields
/// plus the user's domain type `T` flattened alongside them. The [module
/// documentation](crate::status) describes the pending-cleanup ledger
/// semantics and the CRD-regeneration requirement.
pub struct ResourceStatus<T: Default> {
    /// Status conditions maintained by the framework; serialised only when
    /// non-empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,

    /// The `metadata.generation` this status was computed for, set at patch
    /// time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Framework-owned ledger of effect names that completed `apply` but whose
    /// `cleanup` has not yet run. See the [module documentation](crate::status).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_cleanup: Vec<String>,

    /// The user's domain status fields, flattened alongside the framework
    /// fields.
    #[serde(flatten)]
    pub resource: T,
}

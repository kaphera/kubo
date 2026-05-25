//! Generic status wrapper used by the reconcile framework.
//!
//! [`ResourceStatus<T>`] provides the standard Kubernetes status fields
//! (`conditions` and `observedGeneration`) alongside resource-specific fields
//! via `#[serde(flatten)]`. The reconcile framework wraps the user's domain
//! type `T` in this automatically — the status closure only needs to return `T`.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceStatus<T: Default> {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    #[serde(flatten)]
    pub resource: T,
}

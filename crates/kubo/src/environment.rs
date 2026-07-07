//! Kubernetes resource bounds and the [`KubeHandle`] operations handle.
//!
//! [`KubeHandle`] is the sole Kubernetes seam in the framework.  Every gate,
//! effect, and poll receives `&KubeHandle` and uses its inherent methods to read
//! or mutate cluster state.  In production you construct one from a real
//! `kube::Client`; in tests you construct one from the client provided by
//! [`World`](crate::testing::World), which points at a [`FakeApiServer`] — no
//! cluster needed.
//!
//! [`EnvResource`] and [`ClusterEnvResource`] are bound-collection aliases
//! for the constraints every typed Kubernetes resource must satisfy.  They are
//! auto-implemented; you never write them manually.

mod kube_handle;
pub use kube_handle::KubeHandle;

use std::fmt;

use k8s_openapi::{ClusterResourceScope, NamespaceResourceScope};
use kube::Resource;
use serde::{Serialize, de::DeserializeOwned};

/// Errors surfaced by [`KubeHandle`] methods.
///
/// Domain error types in your controller should wrap or convert into
/// `EnvironmentError::Other` for anything that isn't a direct Kubernetes
/// API failure or serialisation issue.
#[derive(Debug, thiserror::Error)]
pub enum EnvironmentError {
    /// A `kube::Error` from the Kubernetes client.
    #[error("kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    /// A JSON serialisation/deserialisation failure.
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),

    /// A catch-all for domain-specific failures surfaced through the operations handle.
    #[error("environment error: {0}")]
    Other(String),
}

/// Bound alias for namespace-scoped Kubernetes resources.
///
/// Auto-implemented for any type satisfying the constraints. You never
/// implement this manually — if your CRD derives `CustomResource` with
/// `namespaced`, it satisfies this automatically.
pub trait EnvResource:
    Resource<Scope = NamespaceResourceScope, DynamicType = ()>
    + Clone
    + fmt::Debug
    + Serialize
    + DeserializeOwned
    + Send
    + Sync
    + 'static
{
}

impl<T> EnvResource for T where
    T: Resource<Scope = NamespaceResourceScope, DynamicType = ()>
        + Clone
        + fmt::Debug
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static
{
}

/// Bound alias for cluster-scoped Kubernetes resources.
///
/// Auto-implemented for any type satisfying the constraints.
pub trait ClusterEnvResource:
    Resource<Scope = ClusterResourceScope, DynamicType = ()>
    + Clone
    + fmt::Debug
    + Serialize
    + DeserializeOwned
    + Send
    + Sync
    + 'static
{
}

impl<T> ClusterEnvResource for T where
    T: Resource<Scope = ClusterResourceScope, DynamicType = ()>
        + Clone
        + fmt::Debug
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static
{
}

/// A namespace + name pair identifying a Kubernetes object.
///
/// Used throughout the framework as the canonical way to reference a
/// namespaced resource without carrying the full object.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ObjectRef {
    pub namespace: String,
    pub name: String,
}

impl ObjectRef {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }
}

impl fmt::Display for ObjectRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.namespace, self.name)
    }
}

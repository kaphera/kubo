//! The environment abstraction — a seam between your controller logic and Kubernetes.
//!
//! Every Kubernetes interaction in the reconcile pipeline flows through the
//! [`Environment`] trait. This is what makes controllers unit-testable without a
//! cluster: in production you use [`RealEnvironment`]
//! (backed by a real `kube::Client`), in tests you use
//! [`TestEnvironment`](crate::testing::TestEnvironment) (an in-memory store).
//!
//! The trait surface is deliberately small — get, list, apply (SSA), delete, patch
//! status, patch finalizers — because those are the only operations a well-behaved
//! operator needs. If your controller requires something outside this set, extend
//! the trait or add a domain-specific abstraction behind it.

mod real_environment;
pub use real_environment::RealEnvironment;

use std::fmt;

use async_trait::async_trait;
use k8s_openapi::{ClusterResourceScope, NamespaceResourceScope};
use kube::Resource;
use kube::api::DynamicObject;
use kube::discovery::ApiResource;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

/// Errors surfaced by [`Environment`] implementations.
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

    /// A catch-all for domain-specific failures surfaced through the environment.
    #[error("environment error: {0}")]
    Other(String),
}

/// Marker trait for namespace-scoped Kubernetes resources usable with
/// the [`Environment`] namespaced methods.
///
/// Auto-implemented for any type satisfying the bounds. You never implement
/// this manually — if your CRD derives `CustomResource` with `namespaced`,
/// it satisfies this automatically.
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

/// Marker trait for cluster-scoped Kubernetes resources usable with
/// the [`Environment`] cluster methods.
///
/// Auto-implemented for any type satisfying the bounds.
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

/// The Kubernetes interaction surface for reconcile pipelines.
///
/// This trait is the sole boundary between your controller and the cluster.
/// The framework calls these methods for finalizer management, status patching,
/// and resource application. Your gates and effects call them for reads and writes.
///
/// All methods are generic over the resource type, so a single `Environment`
/// instance handles every CRD and built-in resource your controller touches.
#[async_trait]
pub trait Environment: Send + Sync {
    /// Fetch a single namespaced resource. Returns `None` if not found (404).
    async fn get<T: EnvResource>(&self, r: &ObjectRef) -> Result<Option<T>, EnvironmentError>;

    /// List all resources of type `T` in the given namespace.
    async fn list<T: EnvResource>(&self, namespace: &str) -> Result<Vec<T>, EnvironmentError>;

    /// Server-Side Apply a namespaced resource (create or update).
    async fn apply<T: EnvResource>(&self, namespace: &str, obj: T) -> Result<(), EnvironmentError>;

    /// Patch the `/status` subresource via Server-Side Apply.
    async fn patch_status<T: EnvResource>(
        &self,
        r: &ObjectRef,
        status: Value,
    ) -> Result<(), EnvironmentError>;

    /// Delete a namespaced resource.
    async fn delete<T: EnvResource>(&self, r: &ObjectRef) -> Result<(), EnvironmentError>;

    /// Replace the finalizer list on a namespaced resource (merge patch).
    async fn patch_finalizers<T: EnvResource>(
        &self,
        r: &ObjectRef,
        finalizers: Vec<String>,
    ) -> Result<(), EnvironmentError>;

    /// Remove a single annotation from a namespaced resource (merge patch).
    ///
    /// Used by the framework to clear rotation trigger annotations after a
    /// successful reconcile pass. The merge patch sets the annotation value
    /// to `null`, which removes it regardless of which field manager owns it.
    async fn remove_annotation<T: EnvResource>(
        &self,
        r: &ObjectRef,
        annotation_key: &str,
    ) -> Result<(), EnvironmentError>;

    /// Fetch a single cluster-scoped resource by name. Returns `None` if not found.
    async fn get_cluster<T: ClusterEnvResource>(
        &self,
        name: &str,
    ) -> Result<Option<T>, EnvironmentError>;

    /// Server-Side Apply a cluster-scoped resource.
    async fn apply_cluster<T: ClusterEnvResource>(&self, obj: T) -> Result<(), EnvironmentError>;

    /// Delete a cluster-scoped resource by name.
    async fn delete_cluster<T: ClusterEnvResource>(
        &self,
        name: &str,
    ) -> Result<(), EnvironmentError>;

    /// Fetch a namespaced dynamic resource. Returns `None` if not found (404).
    ///
    /// Use this for CRDs that lack typed Rust bindings (e.g. Bank-Vaults
    /// Vault, CNPG Cluster). The `ApiResource` descriptor tells the client
    /// which GVK to query.
    async fn get_dynamic(
        &self,
        api_resource: &ApiResource,
        r: &ObjectRef,
    ) -> Result<Option<DynamicObject>, EnvironmentError>;

    /// Server-Side Apply a namespaced dynamic resource.
    ///
    /// The `obj` value must be a complete resource manifest (with
    /// `apiVersion`, `kind`, `metadata`, and `spec`).
    async fn apply_dynamic(
        &self,
        api_resource: &ApiResource,
        namespace: &str,
        name: &str,
        obj: &Value,
    ) -> Result<(), EnvironmentError>;
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

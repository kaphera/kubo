//! [`KubeHandle`] — the concrete Kubernetes operations handle for reconcile pipelines.
//!
//! [`KubeHandle`] wraps a [`kube::Client`] and a field-manager name, and exposes
//! every Kubernetes operation the framework needs: get, list, apply (SSA),
//! patch-status (SSA), delete, finalizer management, annotation removal, and
//! dynamic-resource support.
//!
//! This is what you construct at operator startup and pass into
//! [`Runtime::new`](crate::runtime::Runtime::new). In tests, construct one from
//! the [`World::client()`](crate::testing::World::client) to run the same
//! production code paths against the fake API server.
//!
//! ```ignore
//! let client = kube::Client::try_default().await?;
//! // Field manager is derived from the operator name automatically by Runtime.
//! // To construct one manually (e.g. in integration tests):
//! let ops = KubeHandle::new(client, "my-operator");
//! ```

use kube::{
    Api,
    api::{DeleteParams, DynamicObject, Patch, PatchParams},
    discovery::ApiResource,
};
use serde_json::json;

use super::{ClusterEnvResource, EnvResource, EnvironmentError, ObjectRef};

/// The Kubernetes operations handle — wraps a [`kube::Client`] and a
/// field-manager name, providing every operation a reconcile pipeline needs.
///
/// All mutations (apply, patch-status) use **Server-Side Apply** with forced
/// field ownership under the configured field manager.  This is the only type
/// your gates, effects, and polls ever see — there is no trait to mock out;
/// swap the client for one backed by [`kubo_test::World`] to run against the
/// fake API server instead.
#[derive(Clone)]
pub struct KubeHandle {
    client: kube::Client,
    field_manager: String,
}

impl KubeHandle {
    /// Create a new handle with the given client and field-manager name.
    ///
    /// The field manager identifies this operator in SSA conflict resolution.
    /// Use a stable, unique name (e.g. `"kaphera-operator"`).  [`Runtime`](crate::runtime::Runtime)
    /// derives this automatically from the operator name passed to
    /// [`Runtime::new`](crate::runtime::Runtime::new).
    pub fn new(client: kube::Client, field_manager: impl Into<String>) -> Self {
        Self {
            client,
            field_manager: field_manager.into(),
        }
    }

    /// The underlying kube client.
    pub fn client(&self) -> &kube::Client {
        &self.client
    }

    fn ssa_params(&self) -> PatchParams {
        PatchParams::apply(&self.field_manager).force()
    }

    // ── Namespaced ────────────────────────────────────────────────────────────

    /// Fetch a single namespaced resource. Returns `None` if not found (404).
    pub async fn get<T: EnvResource>(&self, r: &ObjectRef) -> Result<Option<T>, EnvironmentError> {
        let api = Api::<T>::namespaced(self.client.clone(), &r.namespace);
        Ok(api.get_opt(&r.name).await?)
    }

    /// List all resources of type `T` in the given namespace.
    pub async fn list<T: EnvResource>(&self, namespace: &str) -> Result<Vec<T>, EnvironmentError> {
        let api = Api::<T>::namespaced(self.client.clone(), namespace);
        Ok(api.list(&Default::default()).await?.items)
    }

    /// List all resources of type `T` across all namespaces.
    pub async fn list_all<T: EnvResource>(&self) -> Result<Vec<T>, EnvironmentError> {
        let api = Api::<T>::all(self.client.clone());
        Ok(api.list(&Default::default()).await?.items)
    }

    /// Server-Side Apply a namespaced resource (create or update).
    pub async fn apply<T: EnvResource>(
        &self,
        namespace: &str,
        obj: T,
    ) -> Result<(), EnvironmentError> {
        let api = Api::<T>::namespaced(self.client.clone(), namespace);
        let params = self.ssa_params();
        let name =
            obj.meta().name.clone().ok_or_else(|| {
                EnvironmentError::Other("apply: object missing metadata.name".into())
            })?;
        api.patch(&name, &params, &Patch::Apply(&obj)).await?;
        Ok(())
    }

    /// Patch the `/status` subresource via Server-Side Apply.
    pub async fn patch_status<T: EnvResource>(
        &self,
        r: &ObjectRef,
        status: serde_json::Value,
    ) -> Result<(), EnvironmentError> {
        let api = Api::<T>::namespaced(self.client.clone(), &r.namespace);
        let params = self.ssa_params();
        let patch = json!({
            "apiVersion": T::api_version(&()),
            "kind": T::kind(&()),
            "status": status,
        });
        api.patch_status(&r.name, &params, &Patch::Apply(&patch))
            .await?;
        Ok(())
    }

    /// Delete a namespaced resource.
    pub async fn delete<T: EnvResource>(&self, r: &ObjectRef) -> Result<(), EnvironmentError> {
        let api = Api::<T>::namespaced(self.client.clone(), &r.namespace);
        api.delete(&r.name, &DeleteParams::default()).await?;
        Ok(())
    }

    /// Replace the finalizer list on a namespaced resource (merge patch).
    pub async fn patch_finalizers<T: EnvResource>(
        &self,
        r: &ObjectRef,
        finalizers: Vec<String>,
    ) -> Result<(), EnvironmentError> {
        let api = Api::<T>::namespaced(self.client.clone(), &r.namespace);
        let patch = serde_json::json!({"metadata": {"finalizers": finalizers}});
        api.patch(&r.name, &PatchParams::default(), &Patch::Merge(&patch))
            .await?;
        Ok(())
    }

    /// Remove a single annotation from a namespaced resource (merge patch).
    ///
    /// Sets the annotation value to `null`, which removes it regardless of
    /// which field manager owns it.
    pub async fn remove_annotation<T: EnvResource>(
        &self,
        r: &ObjectRef,
        annotation_key: &str,
    ) -> Result<(), EnvironmentError> {
        let api = Api::<T>::namespaced(self.client.clone(), &r.namespace);
        let patch = serde_json::json!({
            "metadata": {
                "annotations": {
                    annotation_key: null
                }
            }
        });
        api.patch(&r.name, &PatchParams::default(), &Patch::Merge(&patch))
            .await?;
        Ok(())
    }

    // ── Cluster-scoped ────────────────────────────────────────────────────────

    /// Fetch a single cluster-scoped resource by name. Returns `None` if not found.
    pub async fn get_cluster<T: ClusterEnvResource>(
        &self,
        name: &str,
    ) -> Result<Option<T>, EnvironmentError> {
        let api = Api::<T>::all(self.client.clone());
        Ok(api.get_opt(name).await?)
    }

    /// Server-Side Apply a cluster-scoped resource.
    pub async fn apply_cluster<T: ClusterEnvResource>(
        &self,
        obj: T,
    ) -> Result<(), EnvironmentError> {
        let api = Api::<T>::all(self.client.clone());
        let params = self.ssa_params();
        let name = obj.meta().name.clone().ok_or_else(|| {
            EnvironmentError::Other("apply_cluster: object missing metadata.name".into())
        })?;
        api.patch(&name, &params, &Patch::Apply(&obj)).await?;
        Ok(())
    }

    /// Delete a cluster-scoped resource by name.
    pub async fn delete_cluster<T: ClusterEnvResource>(
        &self,
        name: &str,
    ) -> Result<(), EnvironmentError> {
        let api = Api::<T>::all(self.client.clone());
        api.delete(name, &DeleteParams::default()).await?;
        Ok(())
    }

    // ── Dynamic ───────────────────────────────────────────────────────────────

    /// Fetch a namespaced dynamic resource. Returns `None` if not found (404).
    ///
    /// Use this for CRDs that lack typed Rust bindings. The `ApiResource`
    /// descriptor tells the client which GVK to query.
    pub async fn get_dynamic(
        &self,
        api_resource: &ApiResource,
        r: &ObjectRef,
    ) -> Result<Option<DynamicObject>, EnvironmentError> {
        let api =
            Api::<DynamicObject>::namespaced_with(self.client.clone(), &r.namespace, api_resource);
        Ok(api.get_opt(&r.name).await?)
    }

    /// Server-Side Apply a namespaced dynamic resource.
    ///
    /// The `obj` value must be a complete resource manifest (with `apiVersion`,
    /// `kind`, `metadata`, and `spec`).
    pub async fn apply_dynamic(
        &self,
        api_resource: &ApiResource,
        namespace: &str,
        name: &str,
        obj: &serde_json::Value,
    ) -> Result<(), EnvironmentError> {
        let api =
            Api::<DynamicObject>::namespaced_with(self.client.clone(), namespace, api_resource);
        let params = self.ssa_params();
        api.patch(name, &params, &Patch::Apply(obj)).await?;
        Ok(())
    }
}

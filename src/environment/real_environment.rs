//! Production [`Environment`] implementation backed by a real `kube::Client`.
//!
//! [`RealEnvironment`] performs all Kubernetes operations via Server-Side Apply (SSA)
//! with a configurable field manager. This is the implementation you wire into your
//! operator binary — typically constructed once during startup and shared across all
//! controllers via `Arc`.
//!
//! ```ignore
//! let client = kube::Client::try_default().await?;
//! let env = RealEnvironment::new(client, "my-operator");
//! ```

use async_trait::async_trait;
use kube::{
    Api,
    api::{DeleteParams, DynamicObject, Patch, PatchParams},
    discovery::ApiResource,
};
use serde_json::json;

use super::{ClusterEnvResource, EnvResource, Environment, EnvironmentError, ObjectRef};

/// The production environment — wraps a `kube::Client` and applies all mutations
/// via Server-Side Apply with forced field ownership.
#[derive(Clone)]
pub struct RealEnvironment {
    client: kube::Client,
    field_manager: String,
}

impl RealEnvironment {
    /// Create a new environment with the given client and field manager name.
    ///
    /// The field manager identifies this operator in SSA conflict resolution.
    /// Use a stable, unique name (e.g. `"kaphera-operator"`).
    pub fn new(client: kube::Client, field_manager: impl Into<String>) -> Self {
        Self {
            client,
            field_manager: field_manager.into(),
        }
    }

    fn ssa_params(&self) -> PatchParams {
        PatchParams::apply(&self.field_manager).force()
    }
}

#[async_trait]
impl Environment for RealEnvironment {
    async fn get<T: EnvResource>(&self, r: &ObjectRef) -> Result<Option<T>, EnvironmentError> {
        let api = Api::<T>::namespaced(self.client.clone(), &r.namespace);
        Ok(api.get_opt(&r.name).await?)
    }

    async fn list<T: EnvResource>(&self, namespace: &str) -> Result<Vec<T>, EnvironmentError> {
        let api = Api::<T>::namespaced(self.client.clone(), namespace);
        Ok(api.list(&Default::default()).await?.items)
    }

    async fn apply<T: EnvResource>(&self, namespace: &str, obj: T) -> Result<(), EnvironmentError> {
        let api = Api::<T>::namespaced(self.client.clone(), namespace);
        let params = self.ssa_params();
        let name =
            obj.meta().name.clone().ok_or_else(|| {
                EnvironmentError::Other("apply: object missing metadata.name".into())
            })?;
        api.patch(&name, &params, &Patch::Apply(&obj)).await?;
        Ok(())
    }

    async fn patch_status<T: EnvResource>(
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

    async fn delete<T: EnvResource>(&self, r: &ObjectRef) -> Result<(), EnvironmentError> {
        let api = Api::<T>::namespaced(self.client.clone(), &r.namespace);
        api.delete(&r.name, &DeleteParams::default()).await?;
        Ok(())
    }

    async fn patch_finalizers<T: EnvResource>(
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

    async fn remove_annotation<T: EnvResource>(
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

    async fn get_cluster<T: ClusterEnvResource>(
        &self,
        name: &str,
    ) -> Result<Option<T>, EnvironmentError> {
        let api = Api::<T>::all(self.client.clone());
        Ok(api.get_opt(name).await?)
    }

    async fn apply_cluster<T: ClusterEnvResource>(&self, obj: T) -> Result<(), EnvironmentError> {
        let api = Api::<T>::all(self.client.clone());
        let params = self.ssa_params();
        let name = obj.meta().name.clone().ok_or_else(|| {
            EnvironmentError::Other("apply_cluster: object missing metadata.name".into())
        })?;
        api.patch(&name, &params, &Patch::Apply(&obj)).await?;
        Ok(())
    }

    async fn delete_cluster<T: ClusterEnvResource>(
        &self,
        name: &str,
    ) -> Result<(), EnvironmentError> {
        let api = Api::<T>::all(self.client.clone());
        api.delete(name, &DeleteParams::default()).await?;
        Ok(())
    }

    async fn get_dynamic(
        &self,
        api_resource: &ApiResource,
        r: &ObjectRef,
    ) -> Result<Option<DynamicObject>, EnvironmentError> {
        let api =
            Api::<DynamicObject>::namespaced_with(self.client.clone(), &r.namespace, api_resource);
        Ok(api.get_opt(&r.name).await?)
    }

    async fn apply_dynamic(
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

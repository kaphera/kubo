//! [`KubeHandle`] — the concrete Kubernetes operations handle for reconcile pipelines.
//!
//! [`KubeHandle`] wraps a [`kube::Client`] and a field-manager name, and exposes
//! every Kubernetes operation the framework needs: get, list, apply (SSA),
//! patch-status (SSA), delete, finalizer management, annotation removal, and
//! dynamic-resource support.
//!
//! This is what you construct at operator startup and pass into
//! [`Runtime::new`](crate::runtime::Runtime::new). In tests,
//! `World::env(field_manager)` returns a ready handle, so the same production
//! code paths run against the fake API server.
//!
//! ```rust,no_run
//! # use kubo::environment::KubeHandle;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = kube::Client::try_default().await?;
//! // Field manager is derived from the operator name automatically by Runtime.
//! // To construct one manually (e.g. in integration tests):
//! let handle = KubeHandle::new(client, "my-operator");
//! # Ok(())
//! # }
//! ```

use kube::{
    Api,
    api::{DeleteParams, DynamicObject, Patch, PatchParams},
    discovery::ApiResource,
};
use serde_json::json;

use super::{ClusterResource, EnvironmentError, NamespaceResource, ObjectRef};

/// The Kubernetes operations handle — wraps a [`kube::Client`] and a
/// field-manager name, providing every operation a reconcile pipeline needs.
///
/// All mutations (apply, patch-status) use **Server-Side Apply** under the
/// configured field manager.  By default the apply is *not* forced: a conflict
/// with another field owner surfaces as a `409` rather than being silently
/// overwritten, so a controller that is the legitimate sole writer of a field
/// never sees the difference, and a controller that is not gets a loud error
/// instead of a silent data-loss event.  The rare case that genuinely needs to
/// steal ownership (e.g. reclaiming a label the controller owns that a user
/// overrode) opts in explicitly via [`KubeHandle::apply_force`] /
/// [`KubeHandle::apply_cluster_force`] / [`KubeHandle::apply_dynamic_force`].
/// Status is never forced: the controller is the sole writer of its own CR's
/// status by contract, so force would be dead code in the steady state and a
/// footgun the moment it actually fired.  This is the only type your gates,
/// effects, and polls ever see — there is no trait to mock out; swap the client
/// for one backed by `kubo::testing::World` to run against the fake API server
/// instead.
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

    /// SSA patch params without `force`.
    ///
    /// This is the default for every mutation.  A conflict with another field
    /// owner returns `409` instead of being silently overwritten, which is the
    /// behaviour a sole-owner controller wants: in the steady state there is no
    /// conflict to win, and the moment one appears it is a signal worth
    /// surfacing, not masking.
    fn ssa_params(&self) -> PatchParams {
        PatchParams::apply(&self.field_manager)
    }

    /// SSA patch params with `force`.
    ///
    /// Force-apply steals field ownership from any conflicting manager.  This
    /// is the explicit, named opt-in for the rare case where a controller must
    /// reclaim a field it owns from another writer (e.g. a label the controller
    /// manages that a user overrode).  It is deliberately not the default:
    /// force turns a loud `409` into a silent overwrite, which is the wrong
    /// thing to hide behind a shared helper.  Status applies never use this —
    /// see [`KubeHandle::patch_status`].
    fn ssa_params_force(&self) -> PatchParams {
        PatchParams::apply(&self.field_manager).force()
    }

    // ── Namespaced ────────────────────────────────────────────────────────────

    /// Fetch a single namespaced resource. Returns `None` if not found (404).
    pub async fn get<T: NamespaceResource>(
        &self,
        r: &ObjectRef,
    ) -> Result<Option<T>, EnvironmentError> {
        let api = Api::<T>::namespaced(self.client.clone(), &r.namespace);
        Ok(api.get_opt(&r.name).await?)
    }

    /// List all resources of type `T` in the given namespace.
    pub async fn list<T: NamespaceResource>(
        &self,
        namespace: &str,
    ) -> Result<Vec<T>, EnvironmentError> {
        let api = Api::<T>::namespaced(self.client.clone(), namespace);
        Ok(api.list(&Default::default()).await?.items)
    }

    /// List all resources of type `T` across all namespaces.
    pub async fn list_all<T: NamespaceResource>(&self) -> Result<Vec<T>, EnvironmentError> {
        let api = Api::<T>::all(self.client.clone());
        Ok(api.list(&Default::default()).await?.items)
    }

    /// Server-Side Apply a namespaced resource (create or update).
    ///
    /// Uses non-forced SSA: a conflict with another field owner returns `409`.
    /// Use [`KubeHandle::apply_force`] only when this controller must
    /// deliberately reclaim a field it owns from another writer.
    pub async fn apply<T: NamespaceResource>(
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

    /// Server-Side Apply a namespaced resource, forcing field ownership.
    ///
    /// This is the explicit opt-in for the rare case where the controller must
    /// steal ownership of a field from another manager (e.g. reclaiming a label
    /// the controller owns that a user overrode).  It is deliberately separate
    /// from [`KubeHandle::apply`] so that stealing ownership is visible at the
    /// call site rather than hidden in a shared helper.  Do not use this for
    /// status — see [`KubeHandle::patch_status`].
    pub async fn apply_force<T: NamespaceResource>(
        &self,
        namespace: &str,
        obj: T,
    ) -> Result<(), EnvironmentError> {
        let api = Api::<T>::namespaced(self.client.clone(), namespace);
        let params = self.ssa_params_force();
        let name = obj.meta().name.clone().ok_or_else(|| {
            EnvironmentError::Other("apply_force: object missing metadata.name".into())
        })?;
        api.patch(&name, &params, &Patch::Apply(&obj)).await?;
        Ok(())
    }

    /// Patch the `/status` subresource via Server-Side Apply, without force.
    ///
    /// The controller is the sole writer of its own CR's status by contract,
    /// and must send the *complete* status every reconcile (conditions,
    /// observed generation, pending cleanup, resource) so that no stale fields
    /// linger.  Under that contract there is never a status conflict to win, so
    /// force would be dead code in the steady state — and a silent data-loss
    /// event the moment it actually fired (a second writer on status is a bug
    /// worth surfacing as a `409`, not papering over).  Force on status is
    /// therefore not offered; if a genuine shared-ownership case ever arises it
    /// is a framework design conversation, not a per-call escape hatch.
    pub async fn patch_status<T: NamespaceResource>(
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
    pub async fn delete<T: NamespaceResource>(
        &self,
        r: &ObjectRef,
    ) -> Result<(), EnvironmentError> {
        let api = Api::<T>::namespaced(self.client.clone(), &r.namespace);
        api.delete(&r.name, &DeleteParams::default()).await?;
        Ok(())
    }

    /// Replace the finalizer list on a namespaced resource (merge patch).
    pub async fn patch_finalizers<T: NamespaceResource>(
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
    pub async fn remove_annotation<T: NamespaceResource>(
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
    pub async fn get_cluster<T: ClusterResource>(
        &self,
        name: &str,
    ) -> Result<Option<T>, EnvironmentError> {
        let api = Api::<T>::all(self.client.clone());
        Ok(api.get_opt(name).await?)
    }

    /// Server-Side Apply a cluster-scoped resource.
    ///
    /// Uses non-forced SSA; see [`KubeHandle::apply`].  Use
    /// [`KubeHandle::apply_cluster_force`] only when deliberately reclaiming a
    /// field from another writer.
    pub async fn apply_cluster<T: ClusterResource>(&self, obj: T) -> Result<(), EnvironmentError> {
        let api = Api::<T>::all(self.client.clone());
        let params = self.ssa_params();
        let name = obj.meta().name.clone().ok_or_else(|| {
            EnvironmentError::Other("apply_cluster: object missing metadata.name".into())
        })?;
        api.patch(&name, &params, &Patch::Apply(&obj)).await?;
        Ok(())
    }

    /// Server-Side Apply a cluster-scoped resource, forcing field ownership.
    ///
    /// Cluster-scoped counterpart of [`KubeHandle::apply_force`]; see that
    /// method for why force is an explicit opt-in.
    pub async fn apply_cluster_force<T: ClusterResource>(
        &self,
        obj: T,
    ) -> Result<(), EnvironmentError> {
        let api = Api::<T>::all(self.client.clone());
        let params = self.ssa_params_force();
        let name = obj.meta().name.clone().ok_or_else(|| {
            EnvironmentError::Other("apply_cluster_force: object missing metadata.name".into())
        })?;
        api.patch(&name, &params, &Patch::Apply(&obj)).await?;
        Ok(())
    }

    /// Delete a cluster-scoped resource by name.
    pub async fn delete_cluster<T: ClusterResource>(
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
    /// `kind`, `metadata`, and `spec`).  Uses non-forced SSA; see
    /// [`KubeHandle::apply`].  Use [`KubeHandle::apply_dynamic_force`] only when
    /// deliberately reclaiming a field from another writer.
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

    /// Server-Side Apply a namespaced dynamic resource, forcing field ownership.
    ///
    /// Dynamic counterpart of [`KubeHandle::apply_force`]; see that method for
    /// why force is an explicit opt-in.
    pub async fn apply_dynamic_force(
        &self,
        api_resource: &ApiResource,
        namespace: &str,
        name: &str,
        obj: &serde_json::Value,
    ) -> Result<(), EnvironmentError> {
        let api =
            Api::<DynamicObject>::namespaced_with(self.client.clone(), namespace, api_resource);
        let params = self.ssa_params_force();
        api.patch(name, &params, &Patch::Apply(obj)).await?;
        Ok(())
    }
}

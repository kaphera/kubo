//! In-memory [`Environment`] for unit testing controllers without a cluster.
//!
//! [`TestEnvironment`] stores resources in a `HashMap` behind an `Arc<Mutex<..>>`,
//! making it trivially cloneable and shareable across async test code. It records
//! every status patch in an ordered history, enabling property-based assertions
//! over your controller's status progression.
//!
//! # Typical Test Pattern
//!
//! ```ignore
//! let env = TestEnvironment::new();
//!
//! // Seed upstream dependencies your gates will look up:
//! env.seed(make_vault_instance("my-vault", /* ready = */ true));
//!
//! // Run the reconcile pipeline:
//! let action = Reconcile::new(&env, &cx, &mut conditions)
//!     .gate(ResolveVault { .. })
//!     .effect(CreateMount { .. })
//!     .run(FINALIZER, obj, |conds, state| build_status(conds, state))
//!     .await?;
//!
//! // Assert on the resulting cluster state:
//! assert!(env.has::<Secret>(&ObjectRef::new("ns", "my-secret")));
//!
//! // Assert on status progression:
//! let ticks = env.status_ticks::<MyStatus>(&obj_ref);
//! assert_monotonic(&ticks, |s| s.ready(), "readiness");
//! ```

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use serde_json::Value;

use crate::environment::{
    ClusterEnvResource, EnvResource, Environment, EnvironmentError, ObjectRef,
};

#[derive(Clone, Debug)]
struct Row {
    body: Value,
}

/// A recorded status patch, capturing the object reference, the
/// resource kind, and the status value at the moment of patching.
#[derive(Clone, Debug)]
pub struct StatusPatch {
    pub kind: String,
    pub obj_ref: ObjectRef,
    pub status: Value,
}

#[derive(Default)]
struct InMemoryStore {
    rows: HashMap<(String, ObjectRef), Row>,
    /// Cluster-scoped resources keyed by (kind, name).
    cluster_rows: HashMap<(String, String), Row>,
    /// Ordered history of all status patches.
    status_history: Vec<StatusPatch>,
}

#[derive(Clone, Default)]
pub struct TestEnvironment {
    store: Arc<Mutex<InMemoryStore>>,
}

impl TestEnvironment {
    pub fn new() -> Self {
        Self::default()
    }

    fn key<T: EnvResource>(r: &ObjectRef) -> (String, ObjectRef) {
        (T::kind(&()).to_string(), r.clone())
    }

    fn cluster_key<T: ClusterEnvResource>(name: &str) -> (String, String) {
        (T::kind(&()).to_string(), name.to_string())
    }

    pub fn seed<T: EnvResource>(&self, obj: T) -> &Self {
        let r = ObjectRef::new(
            obj.meta().namespace.clone().unwrap_or_default(),
            obj.meta().name.clone().unwrap_or_default(),
        );
        let body = serde_json::to_value(&obj).expect("seed: serialize");
        self.store
            .lock()
            .unwrap()
            .rows
            .insert(Self::key::<T>(&r), Row { body });
        self
    }

    pub fn seed_cluster<T: ClusterEnvResource>(&self, obj: T) -> &Self {
        let name = obj.meta().name.clone().unwrap_or_default();
        let body = serde_json::to_value(&obj).expect("seed_cluster: serialize");
        self.store
            .lock()
            .unwrap()
            .cluster_rows
            .insert(Self::cluster_key::<T>(&name), Row { body });
        self
    }

    pub fn mutate<T: EnvResource>(
        &self,
        r: &ObjectRef,
        f: impl FnOnce(&mut Value),
    ) -> Result<(), EnvironmentError> {
        let mut store = self.store.lock().unwrap();
        let row = store
            .rows
            .get_mut(&Self::key::<T>(r))
            .ok_or_else(|| EnvironmentError::Other(format!("mutate: not found: {r}")))?;
        f(&mut row.body);
        Ok(())
    }

    pub fn get_condition<T: EnvResource>(&self, r: &ObjectRef, type_: &str) -> Option<Condition> {
        let store = self.store.lock().unwrap();
        let row = store.rows.get(&Self::key::<T>(r))?;
        let conds = row
            .body
            .get("status")?
            .get("conditions")?
            .as_array()?
            .iter()
            .filter_map(|v| serde_json::from_value::<Condition>(v.clone()).ok())
            .collect::<Vec<_>>();
        conds.into_iter().find(|c| c.type_ == type_)
    }

    pub fn has<T: EnvResource>(&self, r: &ObjectRef) -> bool {
        self.store
            .lock()
            .unwrap()
            .rows
            .contains_key(&Self::key::<T>(r))
    }

    pub fn has_cluster<T: ClusterEnvResource>(&self, name: &str) -> bool {
        self.store
            .lock()
            .unwrap()
            .cluster_rows
            .contains_key(&Self::cluster_key::<T>(name))
    }

    /// Returns the full ordered history of status patches.
    pub fn status_history(&self) -> Vec<StatusPatch> {
        self.store.lock().unwrap().status_history.clone()
    }

    /// Returns status patches filtered to a specific object.
    pub fn status_history_for(&self, r: &ObjectRef) -> Vec<StatusPatch> {
        self.store
            .lock()
            .unwrap()
            .status_history
            .iter()
            .filter(|p| p.obj_ref == *r)
            .cloned()
            .collect()
    }

    /// Deserialise each recorded status patch for an object into `S`.
    ///
    /// This lets you assert on the typed status at each tick:
    /// ```ignore
    /// let ticks = env.status_ticks::<VaultKvMountStatus>(&obj_ref);
    /// assert_eq!(ticks[0].mount_path, None);
    /// assert_eq!(ticks[1].mount_path, Some("ns/name/kv".into()));
    /// ```
    pub fn status_ticks<S: serde::de::DeserializeOwned>(&self, r: &ObjectRef) -> Vec<S> {
        self.status_history_for(r)
            .into_iter()
            .filter_map(|p| serde_json::from_value(p.status).ok())
            .collect()
    }
}

#[async_trait]
impl Environment for TestEnvironment {
    async fn get<T: EnvResource>(&self, r: &ObjectRef) -> Result<Option<T>, EnvironmentError> {
        let key = Self::key::<T>(r);
        let store = self.store.lock().unwrap();
        match store.rows.get(&key) {
            None => Ok(None),
            Some(row) => Ok(Some(serde_json::from_value(row.body.clone())?)),
        }
    }

    async fn list<T: EnvResource>(&self, namespace: &str) -> Result<Vec<T>, EnvironmentError> {
        let kind = T::kind(&()).to_string();
        let store = self.store.lock().unwrap();
        let mut out = Vec::new();
        for ((k, r), row) in store.rows.iter() {
            if k == &kind && r.namespace == namespace {
                out.push(serde_json::from_value(row.body.clone())?);
            }
        }
        Ok(out)
    }

    async fn apply<T: EnvResource>(&self, namespace: &str, obj: T) -> Result<(), EnvironmentError> {
        let name = obj
            .meta()
            .name
            .clone()
            .ok_or_else(|| EnvironmentError::Other("apply: missing metadata.name".into()))?;
        let r = ObjectRef::new(namespace.to_string(), name);
        let body = serde_json::to_value(&obj)?;
        let mut store = self.store.lock().unwrap();
        store.rows.insert(Self::key::<T>(&r), Row { body });
        Ok(())
    }

    async fn patch_status<T: EnvResource>(
        &self,
        r: &ObjectRef,
        status: Value,
    ) -> Result<(), EnvironmentError> {
        let key = Self::key::<T>(r);
        let mut store = self.store.lock().unwrap();
        let row = store
            .rows
            .get_mut(&key)
            .ok_or_else(|| EnvironmentError::Other(format!("patch_status: not found: {r}")))?;
        row.body
            .as_object_mut()
            .ok_or_else(|| EnvironmentError::Other("patch_status: body is not an object".into()))?
            .insert("status".into(), status.clone());
        store.status_history.push(StatusPatch {
            kind: T::kind(&()).to_string(),
            obj_ref: r.clone(),
            status,
        });
        Ok(())
    }

    async fn delete<T: EnvResource>(&self, r: &ObjectRef) -> Result<(), EnvironmentError> {
        let key = Self::key::<T>(r);
        let mut store = self.store.lock().unwrap();
        store.rows.remove(&key);
        Ok(())
    }

    async fn patch_finalizers<T: EnvResource>(
        &self,
        r: &ObjectRef,
        finalizers: Vec<String>,
    ) -> Result<(), EnvironmentError> {
        let key = Self::key::<T>(r);
        let mut store = self.store.lock().unwrap();
        let row = store
            .rows
            .get_mut(&key)
            .ok_or_else(|| EnvironmentError::Other(format!("patch_finalizers: not found: {r}")))?;
        let metadata = row
            .body
            .as_object_mut()
            .and_then(|o| o.get_mut("metadata"))
            .and_then(|m| m.as_object_mut())
            .ok_or_else(|| EnvironmentError::Other("patch_finalizers: missing metadata".into()))?;
        metadata.insert(
            "finalizers".into(),
            serde_json::to_value(&finalizers).unwrap(),
        );
        Ok(())
    }

    async fn get_cluster<T: ClusterEnvResource>(
        &self,
        name: &str,
    ) -> Result<Option<T>, EnvironmentError> {
        let key = Self::cluster_key::<T>(name);
        let store = self.store.lock().unwrap();
        match store.cluster_rows.get(&key) {
            None => Ok(None),
            Some(row) => Ok(Some(serde_json::from_value(row.body.clone())?)),
        }
    }

    async fn apply_cluster<T: ClusterEnvResource>(&self, obj: T) -> Result<(), EnvironmentError> {
        let name = obj.meta().name.clone().ok_or_else(|| {
            EnvironmentError::Other("apply_cluster: missing metadata.name".into())
        })?;
        let body = serde_json::to_value(&obj)?;
        let mut store = self.store.lock().unwrap();
        store
            .cluster_rows
            .insert(Self::cluster_key::<T>(&name), Row { body });
        Ok(())
    }

    async fn delete_cluster<T: ClusterEnvResource>(
        &self,
        name: &str,
    ) -> Result<(), EnvironmentError> {
        let key = Self::cluster_key::<T>(name);
        let mut store = self.store.lock().unwrap();
        store.cluster_rows.remove(&key);
        Ok(())
    }
}

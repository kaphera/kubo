//! Example: a user-written reusable effect pattern — one generic effect that
//! renders and applies a ConfigMap child, plus a small renderer trait, reused
//! across controllers.
//!
//! This is NOT framework API. Everything between the CRD definitions and the
//! tests is user-written code, exactly as a controller author would write it
//! in their own crate, using only kubo's public API: `Effect`,
//! `EffectOutcome`, `StateKey`, the typed state bag (`State`), `CleanupAction`,
//! `KubeHandle`, `ObjectRef`, `ReconcileContext`, `ReconcilerError`, and
//! `EnvironmentError`. The framework's existing machinery already supports this
//! pattern; this file proves it end to end.
//!
//! The pattern: write one `ConfigMapRenderer` per controller and one generic
//! `ApplyConfigMap<K, R>` effect that consumes it. The same effect type serves
//! any number of (resource, renderer) pairs — each instantiation is a distinct
//! type, so each gets its own state-bag key and its own pending-cleanup ledger
//! entry. Instantiations also compose within one pipeline: a single
//! `Reconcile` can chain two `ApplyConfigMap` effects with different
//! renderers, each applying its own ConfigMap and publishing its own state
//! value and ledger entry.
//!
//! Identity contract: `ConfigMapRenderer::child_ref` is the single source of
//! truth for the child's identity. The rendered ConfigMap's own metadata name
//! and namespace are ignored — the effect stamps the `child_ref` values over
//! them before applying. This is deliberate and load-bearing: cleanup receives
//! no state, so it must re-derive the child's identity from the parent CR
//! alone, and `child_ref` is exactly that derivation.
//!
//! Cost: each apply pass costs one non-forced Server-Side Apply plus one GET —
//! the GET reads back the applied object to publish its current
//! `resourceVersion` as the effect's state value. Each cleanup pass costs one
//! GET (is the child already gone?) plus one DELETE when it is not. The apply
//! is non-forced SSA: a 409 conflict with another field owner surfaces as an
//! error and requeues rather than being silently overwritten.
//!
//! Ledger key: by default the pending-cleanup ledger keys on
//! `std::any::type_name::<Self>()` — the full generic instantiation, distinct
//! per (resource, renderer) pair. The framework dedupes ledger entries by
//! name, so a fixed constant name shared by two instantiations would record
//! one entry for two applied ConfigMaps and leak the second on cleanup.
//! `named` pins a stable key instead — use it when the ConfigMap must not leak
//! across an effect rename, and keep the pinned name unique within the
//! pipeline.

#![allow(missing_docs)]

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::{CustomResource, ResourceExt};
use kubo::{
    ReconcileContext, ReconcilerError,
    cleanup::CleanupAction,
    effect::{Effect, EffectOutcome},
    environment::{EnvironmentError, KubeHandle, ObjectRef},
    reconcile::Reconcile,
    state::{State, StateKey},
    status::ResourceStatus,
    testing::World,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Renders the ConfigMap child a controller owns for its parent custom
/// resource.
///
/// Implement this once per controller and hand the renderer to
/// [`ApplyConfigMap::new`] — the generic effect supplies the apply, the
/// follow-up read, the state publication, and the cleanup.
///
/// [`child_ref`](ConfigMapRenderer::child_ref) is the single source of truth
/// for the child's identity: the ConfigMap returned by
/// [`render`](ConfigMapRenderer::render) may carry its own metadata name and
/// namespace, but they are ignored — the effect stamps the `child_ref` values
/// onto the rendered object before applying. This matters because cleanup
/// receives no state, so it must re-derive the child's identity from the
/// parent CR alone.
trait ConfigMapRenderer<K>: Send + Sync {
    /// Error type produced by [`render`](ConfigMapRenderer::render); must
    /// convert into `ReconcilerError` so the framework can propagate it.
    type Error: Into<ReconcilerError> + Send + 'static;

    /// The namespace and name of the ConfigMap child this controller owns.
    ///
    /// Called on every apply pass and every cleanup pass. Must be a pure
    /// function of the parent CR: cleanup has no other input, so identity
    /// must be re-derivable from the CR alone.
    fn child_ref(&self, cr: &K) -> ObjectRef;

    /// Render the desired ConfigMap content for the parent CR.
    ///
    /// The returned object's metadata name and namespace are ignored —
    /// identity comes solely from [`child_ref`](ConfigMapRenderer::child_ref).
    /// Everything else (data, labels, annotations, the immutable flag) is
    /// applied as rendered.
    fn render(&self, cr: &K) -> Result<ConfigMap, Self::Error>;
}

/// A generic effect that renders and applies a ConfigMap child for a parent
/// custom resource, and deletes it during cleanup.
///
/// [`StateKey::Value`] is `String`: the `resourceVersion` of the applied
/// ConfigMap, read back with one follow-up GET after the apply. Downstream
/// steps and the status closure read it via
/// `state.get::<ApplyConfigMap<K, R>>()`.
///
/// The apply is non-forced SSA, so a 409 conflict with another field owner
/// surfaces as an error and requeues rather than being silently overwritten.
///
/// By default the pending-cleanup ledger keys on
/// `std::any::type_name::<Self>()` — the full generic instantiation, which is
/// distinct per (resource, renderer) pair. The framework dedupes ledger
/// entries by name, so a fixed constant name shared by two instantiations
/// would record one entry for two applied ConfigMaps and leak the second on
/// cleanup. [`named`](ApplyConfigMap::named) pins a stable key instead — use
/// it when the ConfigMap must not leak across an effect rename, and keep the
/// pinned name unique within the pipeline.
struct ApplyConfigMap<K, R> {
    renderer: R,
    name: Option<&'static str>,
    marker: PhantomData<fn(K)>,
}

impl<K, R> ApplyConfigMap<K, R> {
    /// Create the effect from a renderer.
    ///
    /// The pending-cleanup ledger keys on the full type name of this
    /// instantiation (see the struct docs). Use [`named`](Self::named) to pin
    /// a stable key instead.
    fn new(renderer: R) -> Self {
        Self {
            renderer,
            name: None,
            marker: PhantomData,
        }
    }

    /// Pin a stable pending-cleanup ledger key for this effect.
    ///
    /// The pinned name must be unique within the pipeline: the framework
    /// dedupes ledger entries by name, so two effects sharing a pinned name
    /// would record one entry for two applied resources and leak the second
    /// on cleanup. Prefer the default type-name key unless the ConfigMap must
    /// survive an effect struct rename across a release.
    fn named(mut self, name: &'static str) -> Self {
        self.name = Some(name);
        self
    }
}

impl<K: 'static, R: 'static> StateKey for ApplyConfigMap<K, R> {
    type Value = String;
}

#[async_trait]
impl<K, R> Effect<K> for ApplyConfigMap<K, R>
where
    K: Send + Sync + 'static,
    R: ConfigMapRenderer<K> + 'static,
    R::Error: Into<ReconcilerError> + Send + 'static,
{
    type Error = ReconcilerError;

    /// Render and apply the ConfigMap child, then publish its
    /// `resourceVersion`.
    ///
    /// The child's identity is taken from
    /// [`ConfigMapRenderer::child_ref`]; the rendered object's own metadata
    /// name and namespace are stamped over with those values before applying.
    /// On success the applied object is read back with one GET and its
    /// `resourceVersion` is returned as [`EffectOutcome::Done`] — the
    /// framework inserts it into the state bag under this effect's own type.
    async fn apply(
        &self,
        cx: &ReconcileContext<K>,
        ops: &KubeHandle,
        _state: &State,
    ) -> Result<EffectOutcome<String>, ReconcilerError> {
        let child = self.renderer.child_ref(cx.cr);
        let mut cm = self.renderer.render(cx.cr).map_err(Into::into)?;
        cm.metadata.name = Some(child.name.clone());
        cm.metadata.namespace = Some(child.namespace.clone());
        ops.apply(&child.namespace, cm).await?;
        let applied = ops.get::<ConfigMap>(&child).await?.ok_or_else(|| {
            ReconcilerError::from(EnvironmentError::Other(format!(
                "child ConfigMap {}/{} not found after apply",
                child.namespace, child.name
            )))
        })?;
        let rv = applied.metadata.resource_version.clone().ok_or_else(|| {
            ReconcilerError::from(EnvironmentError::Other(format!(
                "applied ConfigMap {}/{} has no resourceVersion",
                child.namespace, child.name
            )))
        })?;
        Ok(EffectOutcome::Done(rv))
    }

    /// Delete the ConfigMap child.
    ///
    /// The child's identity is re-derived from the parent CR alone via
    /// [`ConfigMapRenderer::child_ref`] — cleanup receives no state, so this
    /// is the only way to address the object that `apply` created. If the
    /// child is already gone (deleted externally, or by a previous cleanup
    /// pass that was interrupted before the ledger was struck), the effect
    /// returns [`CleanupAction::Done`] so an external deletion never blocks
    /// the finalizer.
    async fn cleanup(
        &self,
        cx: &ReconcileContext<K>,
        ops: &KubeHandle,
    ) -> Result<CleanupAction, ReconcilerError> {
        let child = self.renderer.child_ref(cx.cr);
        if ops.get::<ConfigMap>(&child).await?.is_none() {
            return Ok(CleanupAction::Done);
        }
        ops.delete::<ConfigMap>(&child).await?;
        Ok(CleanupAction::Done)
    }

    /// The pending-cleanup ledger key: the pinned name if
    /// [`named`](ApplyConfigMap::named) was used, otherwise `None` — which
    /// makes the framework fall back to `std::any::type_name::<Self>()`, the
    /// full generic instantiation. See the struct docs for both behaviours
    /// and the rename-orphan hazard.
    fn effect_name(&self) -> Option<&'static str> {
        self.name
    }
}

/// First parent kind: a config target whose child ConfigMap carries its payload.
#[derive(CustomResource, Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "test.kubo.dev",
    version = "v1alpha1",
    kind = "ConfigTarget",
    status = "ConfigTargetStatus",
    namespaced
)]
pub struct ConfigTargetSpec {
    pub payload: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ConfigTargetFields {
    pub config_rv: Option<String>,
    pub overlay_rv: Option<String>,
}

pub type ConfigTargetStatus = ResourceStatus<ConfigTargetFields>;

impl kubo::HasConditions for ConfigTarget {
    fn conditions(&self) -> Vec<Condition> {
        self.status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default()
    }
}

/// Second parent kind: a settings source whose child ConfigMap carries its mode.
#[derive(CustomResource, Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "test.kubo.dev",
    version = "v1alpha1",
    kind = "SettingsSource",
    status = "SettingsSourceStatus",
    namespaced
)]
pub struct SettingsSourceSpec {
    pub mode: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SettingsSourceFields {
    pub settings_rv: Option<String>,
}

pub type SettingsSourceStatus = ResourceStatus<SettingsSourceFields>;

impl kubo::HasConditions for SettingsSource {
    fn conditions(&self) -> Vec<Condition> {
        self.status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default()
    }
}

/// Renderer for [`ConfigTarget`]. The `render` name is a deliberate decoy: the
/// applied ConfigMap must be named by `child_ref`, not by the rendered object.
struct ConfigTargetRenderer;

impl ConfigMapRenderer<ConfigTarget> for ConfigTargetRenderer {
    type Error = EnvironmentError;

    fn child_ref(&self, cr: &ConfigTarget) -> ObjectRef {
        ObjectRef::new(
            cr.namespace().unwrap_or_default(),
            format!("{}-config", cr.name_any()),
        )
    }

    fn render(&self, cr: &ConfigTarget) -> Result<ConfigMap, EnvironmentError> {
        let mut data = BTreeMap::new();
        data.insert("payload".to_string(), cr.spec.payload.clone());
        Ok(ConfigMap {
            metadata: kube::api::ObjectMeta {
                name: Some("identity-comes-from-child-ref".to_string()),
                ..Default::default()
            },
            data: Some(data),
            ..Default::default()
        })
    }
}

/// Second renderer over the same [`ConfigTarget`] kind, for the
/// same-pipeline composition test. Same decoy-name discipline as
/// [`ConfigTargetRenderer`], with its own child identity and its own
/// deterministic content.
struct ConfigTargetOverlayRenderer;

impl ConfigMapRenderer<ConfigTarget> for ConfigTargetOverlayRenderer {
    type Error = EnvironmentError;

    fn child_ref(&self, cr: &ConfigTarget) -> ObjectRef {
        ObjectRef::new(
            cr.namespace().unwrap_or_default(),
            format!("{}-overlay", cr.name_any()),
        )
    }

    fn render(&self, cr: &ConfigTarget) -> Result<ConfigMap, EnvironmentError> {
        let mut data = BTreeMap::new();
        data.insert(
            "overlay".to_string(),
            format!("{}::overlay", cr.spec.payload),
        );
        Ok(ConfigMap {
            metadata: kube::api::ObjectMeta {
                name: Some("identity-comes-from-child-ref".to_string()),
                ..Default::default()
            },
            data: Some(data),
            ..Default::default()
        })
    }
}

/// Renderer for [`SettingsSource`]. Same decoy-name discipline as
/// [`ConfigTargetRenderer`], with distinct deterministic content.
struct SettingsSourceRenderer;

impl ConfigMapRenderer<SettingsSource> for SettingsSourceRenderer {
    type Error = EnvironmentError;

    fn child_ref(&self, cr: &SettingsSource) -> ObjectRef {
        ObjectRef::new(
            cr.namespace().unwrap_or_default(),
            format!("{}-settings", cr.name_any()),
        )
    }

    fn render(&self, cr: &SettingsSource) -> Result<ConfigMap, EnvironmentError> {
        let mut data = BTreeMap::new();
        data.insert("mode".to_string(), cr.spec.mode.clone());
        Ok(ConfigMap {
            metadata: kube::api::ObjectMeta {
                name: Some("identity-comes-from-child-ref".to_string()),
                ..Default::default()
            },
            data: Some(data),
            ..Default::default()
        })
    }
}

fn make_target(ns: &str, name: &str) -> Arc<ConfigTarget> {
    let mut r = ConfigTarget::new(
        name,
        ConfigTargetSpec {
            payload: "hello".to_string(),
        },
    );
    r.metadata.namespace = Some(ns.to_string());
    Arc::new(r)
}

fn make_source(ns: &str, name: &str) -> Arc<SettingsSource> {
    let mut r = SettingsSource::new(
        name,
        SettingsSourceSpec {
            mode: "strict".to_string(),
        },
    );
    r.metadata.namespace = Some(ns.to_string());
    Arc::new(r)
}

fn ops_for(world: &World) -> KubeHandle {
    KubeHandle::new(world.client().clone(), "kubo-test")
}

fn target_ledger_of(world: &World, ns: &str, name: &str) -> Vec<String> {
    world
        .get::<ConfigTarget>(ns, name)
        .unwrap()
        .status
        .as_ref()
        .unwrap()
        .pending_cleanup
        .clone()
}

/// One generic effect type serves two different (resource, renderer) pairs
/// through two pipelines: both ConfigMaps are applied with renderer-produced
/// content under their `child_ref` names, and both parents publish a non-empty
/// resourceVersion into their status.
#[tokio::test]
async fn apply_config_map_serves_two_kinds() {
    let world = World::builder().build().await;
    let ops = ops_for(&world);

    let target = make_target("ns", "target");
    world.prefill((*target).clone());
    let source = make_source("ns", "source");
    world.prefill((*source).clone());

    Reconcile::new(&ops, target)
        .effect(ApplyConfigMap::new(ConfigTargetRenderer))
        .status(|state, _prev: &ConfigTargetFields| ConfigTargetFields {
            config_rv: state
                .get::<ApplyConfigMap<ConfigTarget, ConfigTargetRenderer>>()
                .cloned(),
            overlay_rv: None,
        })
        .execute("test-finalizer")
        .await
        .unwrap();

    Reconcile::new(&ops, source)
        .effect(ApplyConfigMap::new(SettingsSourceRenderer))
        .status(|state, _prev: &SettingsSourceFields| SettingsSourceFields {
            settings_rv: state
                .get::<ApplyConfigMap<SettingsSource, SettingsSourceRenderer>>()
                .cloned(),
        })
        .execute("test-finalizer")
        .await
        .unwrap();

    let target_cm = world
        .get::<ConfigMap>("ns", "target-config")
        .expect("target ConfigMap applied under its child_ref name");
    assert_eq!(
        target_cm.data.as_ref().unwrap().get("payload"),
        Some(&"hello".to_string()),
        "target ConfigMap carries the renderer-produced content"
    );
    assert_eq!(
        target_cm.metadata.name.as_deref(),
        Some("target-config"),
        "identity stamped from child_ref, not from the rendered decoy name"
    );
    assert_eq!(
        target_cm.metadata.namespace.as_deref(),
        Some("ns"),
        "child ConfigMap lives in the parent's namespace"
    );

    let source_cm = world
        .get::<ConfigMap>("ns", "source-settings")
        .expect("source ConfigMap applied under its child_ref name");
    assert_eq!(
        source_cm.data.as_ref().unwrap().get("mode"),
        Some(&"strict".to_string()),
        "source ConfigMap carries the renderer-produced content"
    );
    assert_eq!(
        source_cm.metadata.name.as_deref(),
        Some("source-settings"),
        "identity stamped from child_ref, not from the rendered decoy name"
    );

    let target_status = world.get::<ConfigTarget>("ns", "target").unwrap();
    let target_rv = target_status
        .status
        .as_ref()
        .unwrap()
        .resource
        .config_rv
        .clone()
        .expect("target status carries the published resourceVersion");
    assert!(!target_rv.is_empty(), "resourceVersion is non-empty");

    let source_status = world.get::<SettingsSource>("ns", "source").unwrap();
    let source_rv = source_status
        .status
        .as_ref()
        .unwrap()
        .resource
        .settings_rv
        .clone()
        .expect("source status carries the published resourceVersion");
    assert!(!source_rv.is_empty(), "resourceVersion is non-empty");

    let target_ledger = target_ledger_of(&world, "ns", "target");
    assert_eq!(target_ledger.len(), 1, "one ledger entry per instantiation");
    assert!(
        target_ledger[0].ends_with("ConfigTargetRenderer>"),
        "default ledger key is the full instantiation type name, got {}",
        target_ledger[0]
    );

    let source_ledger = world
        .get::<SettingsSource>("ns", "source")
        .unwrap()
        .status
        .as_ref()
        .unwrap()
        .pending_cleanup
        .clone();
    assert_eq!(source_ledger.len(), 1, "one ledger entry per instantiation");
    assert!(
        source_ledger[0].ends_with("SettingsSourceRenderer>"),
        "default ledger key is the full instantiation type name, got {}",
        source_ledger[0]
    );
    assert_ne!(
        target_ledger[0], source_ledger[0],
        "two instantiations get distinct ledger entries"
    );
}

/// Two instantiations of the same generic effect compose in ONE pipeline:
/// both ConfigMaps are applied with their own renderer's content under their
/// own `child_ref` names, the status closure reads both state values into two
/// status fields, the ledger records both distinct type-name keys, and
/// deletion cleans up both children before the parent is GC'd.
#[tokio::test]
async fn two_instantiations_compose_in_one_pipeline() {
    let world = World::builder().build().await;
    let ops = ops_for(&world);

    let obj = make_target("ns", "composed");
    world.prefill((*obj).clone());

    Reconcile::new(&ops, obj)
        .effect(ApplyConfigMap::new(ConfigTargetRenderer))
        .effect(ApplyConfigMap::new(ConfigTargetOverlayRenderer))
        .status(|state, _prev: &ConfigTargetFields| ConfigTargetFields {
            config_rv: state
                .get::<ApplyConfigMap<ConfigTarget, ConfigTargetRenderer>>()
                .cloned(),
            overlay_rv: state
                .get::<ApplyConfigMap<ConfigTarget, ConfigTargetOverlayRenderer>>()
                .cloned(),
        })
        .execute("test-finalizer")
        .await
        .unwrap();

    let base_cm = world
        .get::<ConfigMap>("ns", "composed-config")
        .expect("base ConfigMap applied under its child_ref name");
    assert_eq!(
        base_cm.data.as_ref().unwrap().get("payload"),
        Some(&"hello".to_string()),
        "base ConfigMap carries the first renderer's content"
    );
    assert_eq!(
        base_cm.metadata.name.as_deref(),
        Some("composed-config"),
        "identity stamped from child_ref, not from the rendered decoy name"
    );

    let overlay_cm = world
        .get::<ConfigMap>("ns", "composed-overlay")
        .expect("overlay ConfigMap applied under its child_ref name");
    assert_eq!(
        overlay_cm.data.as_ref().unwrap().get("overlay"),
        Some(&"hello::overlay".to_string()),
        "overlay ConfigMap carries the second renderer's content"
    );
    assert_eq!(
        overlay_cm.metadata.name.as_deref(),
        Some("composed-overlay"),
        "identity stamped from child_ref, not from the rendered decoy name"
    );

    let status = world.get::<ConfigTarget>("ns", "composed").unwrap();
    let base_rv = status
        .status
        .as_ref()
        .unwrap()
        .resource
        .config_rv
        .clone()
        .expect("status carries the first effect's resourceVersion");
    assert!(!base_rv.is_empty(), "resourceVersion is non-empty");
    let overlay_rv = status
        .status
        .as_ref()
        .unwrap()
        .resource
        .overlay_rv
        .clone()
        .expect("status carries the second effect's resourceVersion");
    assert!(!overlay_rv.is_empty(), "resourceVersion is non-empty");
    assert_ne!(
        base_rv, overlay_rv,
        "the two effects published distinct resourceVersions"
    );

    let ledger = target_ledger_of(&world, "ns", "composed");
    assert_eq!(
        ledger.len(),
        2,
        "one ledger entry per composed instantiation"
    );
    assert!(
        ledger
            .iter()
            .any(|key| key.ends_with("ConfigTargetRenderer>")),
        "first instantiation keyed on its own type name, ledger = {:?}",
        ledger
    );
    assert!(
        ledger
            .iter()
            .any(|key| key.ends_with("ConfigTargetOverlayRenderer>")),
        "second instantiation keyed on its own type name, ledger = {:?}",
        ledger
    );
    assert_ne!(ledger[0], ledger[1], "the two ledger entries are distinct");

    world.delete::<ConfigTarget>("ns", "composed").await;
    let deleting = Arc::new(world.get::<ConfigTarget>("ns", "composed").unwrap());
    Reconcile::new(&ops, deleting)
        .effect(ApplyConfigMap::new(ConfigTargetRenderer))
        .effect(ApplyConfigMap::new(ConfigTargetOverlayRenderer))
        .status(|state, _prev: &ConfigTargetFields| ConfigTargetFields {
            config_rv: state
                .get::<ApplyConfigMap<ConfigTarget, ConfigTargetRenderer>>()
                .cloned(),
            overlay_rv: state
                .get::<ApplyConfigMap<ConfigTarget, ConfigTargetOverlayRenderer>>()
                .cloned(),
        })
        .execute("test-finalizer")
        .await
        .unwrap();

    assert!(
        world.get::<ConfigMap>("ns", "composed-config").is_none(),
        "base ConfigMap deleted by cleanup"
    );
    assert!(
        world.get::<ConfigMap>("ns", "composed-overlay").is_none(),
        "overlay ConfigMap deleted by cleanup"
    );
    assert!(
        world.get::<ConfigTarget>("ns", "composed").is_none(),
        "parent GC'd after both ledger entries struck and finalizer cleared"
    );
}

/// Each pipeline's state bag holds only its own instantiation's value: the
/// TypeId keys for `ApplyConfigMap<A, RA>` and `ApplyConfigMap<B, RB>` are
/// distinct, so inserting under one leaves the other absent.
#[test]
fn distinct_instantiations_have_isolated_state() {
    let mut s = State::new();
    s.insert::<ApplyConfigMap<ConfigTarget, ConfigTargetRenderer>>("rv-a".to_string());

    assert_eq!(
        s.get::<ApplyConfigMap<ConfigTarget, ConfigTargetRenderer>>(),
        Some(&"rv-a".to_string()),
        "the inserted instantiation reads back its own value"
    );
    assert!(
        s.require::<ApplyConfigMap<ConfigTarget, ConfigTargetRenderer>>()
            .is_ok(),
        "require succeeds for the inserted instantiation"
    );
    assert!(
        s.get::<ApplyConfigMap<SettingsSource, SettingsSourceRenderer>>()
            .is_none(),
        "a different instantiation has its own TypeId key and sees nothing"
    );
}

/// On deletion, cleanup removes the ConfigMap child and the pendingCleanup
/// ledger entry is struck: once cleanup succeeds the framework clears the
/// finalizer, and the fake's GC — which removes a terminating object once
/// its finalizer list empties — then removes the parent.
#[tokio::test]
async fn cleanup_removes_configmap_and_clears_ledger() {
    let world = World::builder().build().await;
    let ops = ops_for(&world);

    let obj = make_target("ns", "obj");
    world.prefill((*obj).clone());

    Reconcile::new(&ops, obj)
        .effect(ApplyConfigMap::new(ConfigTargetRenderer).named("test-config-map"))
        .status(|state, _prev: &ConfigTargetFields| ConfigTargetFields {
            config_rv: state
                .get::<ApplyConfigMap<ConfigTarget, ConfigTargetRenderer>>()
                .cloned(),
            overlay_rv: None,
        })
        .execute("test-finalizer")
        .await
        .unwrap();

    assert_eq!(
        target_ledger_of(&world, "ns", "obj"),
        vec!["test-config-map".to_string()],
        "pinned ledger name recorded after apply"
    );
    let child = ObjectRef::new("ns", "obj-config");
    assert!(
        ops.get::<ConfigMap>(&child).await.unwrap().is_some(),
        "ConfigMap exists after apply"
    );

    world.delete::<ConfigTarget>("ns", "obj").await;
    let deleting = Arc::new(world.get::<ConfigTarget>("ns", "obj").unwrap());
    Reconcile::new(&ops, deleting)
        .effect(ApplyConfigMap::new(ConfigTargetRenderer).named("test-config-map"))
        .status(|state, _prev: &ConfigTargetFields| ConfigTargetFields {
            config_rv: state
                .get::<ApplyConfigMap<ConfigTarget, ConfigTargetRenderer>>()
                .cloned(),
            overlay_rv: None,
        })
        .execute("test-finalizer")
        .await
        .unwrap();

    assert!(
        world.get::<ConfigMap>("ns", "obj-config").is_none(),
        "ConfigMap deleted by cleanup"
    );
    assert!(
        world.get::<ConfigTarget>("ns", "obj").is_none(),
        "parent GC'd after ledger emptied and finalizer cleared"
    );
}

/// An externally deleted child must not block the finalizer: cleanup observes
/// the child is already gone and returns Done, so the parent is still GC'd.
#[tokio::test]
async fn cleanup_tolerates_missing_child() {
    let world = World::builder().build().await;
    let ops = ops_for(&world);

    let obj = make_target("ns", "obj");
    world.prefill((*obj).clone());

    Reconcile::new(&ops, obj)
        .effect(ApplyConfigMap::new(ConfigTargetRenderer).named("test-config-map"))
        .status(|state, _prev: &ConfigTargetFields| ConfigTargetFields {
            config_rv: state
                .get::<ApplyConfigMap<ConfigTarget, ConfigTargetRenderer>>()
                .cloned(),
            overlay_rv: None,
        })
        .execute("test-finalizer")
        .await
        .unwrap();

    let child = ObjectRef::new("ns", "obj-config");
    ops.delete::<ConfigMap>(&child).await.unwrap();

    world.delete::<ConfigTarget>("ns", "obj").await;
    let deleting = Arc::new(world.get::<ConfigTarget>("ns", "obj").unwrap());
    Reconcile::new(&ops, deleting)
        .effect(ApplyConfigMap::new(ConfigTargetRenderer).named("test-config-map"))
        .status(|state, _prev: &ConfigTargetFields| ConfigTargetFields {
            config_rv: state
                .get::<ApplyConfigMap<ConfigTarget, ConfigTargetRenderer>>()
                .cloned(),
            overlay_rv: None,
        })
        .execute("test-finalizer")
        .await
        .unwrap();

    assert!(
        world.get::<ConfigTarget>("ns", "obj").is_none(),
        "parent GC'd even though the child was already gone"
    );
}

/// The default ledger key is unset (the framework falls back to the full
/// instantiation type name) and `named` pins an explicit key.
#[test]
fn effect_name_defaults_and_override() {
    assert!(
        ApplyConfigMap::new(ConfigTargetRenderer)
            .effect_name()
            .is_none(),
        "default effect_name is None, so the ledger keys on the type name"
    );
    assert_eq!(
        ApplyConfigMap::new(ConfigTargetRenderer)
            .named("x")
            .effect_name(),
        Some("x"),
        "named pins an explicit ledger key"
    );
}

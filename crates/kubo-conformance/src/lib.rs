//! Conformance harness for the fake Kubernetes API server.
//!
//! A [`Scenario`] describes a sequence of API operations and their expected
//! outcomes.  Call [`Scenario::run_against_fake`] to execute operations against
//! the in-process [`FakeApiServer`] and record a content-aware [`Trace`] for
//! each step.
//!
//! When the `k3d` feature is enabled, `run_against_kube_apiserver` can run the
//! same scenario against a real cluster so that [`assert_equivalent`] can
//! verify protocol equivalence — not just of HTTP status codes, but of the
//! normalised object content the framework actually depends on.
//!
//! ## Content-aware trace
//!
//! A [`TraceEntry`] carries the HTTP status code, the normalised returned
//! object, and the normalised `status` subresource as the server persisted it.
//! Normalisation strips server-assigned and non-deterministic fields (see
//! [`normalise_object`] / [`normalise_status`]) so that fake and real traces
//! can be compared on the surface the framework reads and writes, without
//! brittleness from cosmetic server differences.

pub mod divergence;

// Re-export the registry's public surface so tests can write
// `use kubo_conformance::{DivId, assert_known_divergence}`.
pub use divergence::{
    DivId, DivKind, Divergence, REGISTRY, assert_known_divergence, lookup, render_table,
};

use std::sync::Arc;

use bytes::Bytes;
use http::{Method, Request};
use http_body_util::{BodyExt, Full};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kubo::reconcile::ReadyReconcile;
use kubo_test_fakeapi::FakeApiServer;
use serde_json::Value;
use tower::Service;

// ── ConformanceObject ───────────────────────────────────────────────────────
//
// The typed CRD the framework-level harness drives a real `Reconcile::execute`
// against.  Raw-HTTP scenarios operate on the same object as opaque JSON; the
// `RunReconcile` step needs a concrete `EnvResource + HasConditions` type to
// satisfy `Reconcile<'r, R>`'s bounds.
//
// The status is `ResourceStatus<ConformanceFields>` so the framework owns
// `pendingCleanup`, `conditions`, and `observedGeneration` exactly as a real
// operator's CRD would.  `ConformanceFields` is intentionally empty — the
// ledger progression is observable through the framework-managed
// `pendingCleanup` field alone, so no resource-specific status is needed.

/// The conformance CRD's spec.  Empty by design: the raw-HTTP scenarios apply
/// arbitrary spec fields (`marker`, etc.) that the CRD's
/// `x-kubernetes-preserve-unknown-fields: true` spec stanza preserves, and the
/// reconcile pipeline does not read spec.  The struct exists as the spec of the
/// `CustomResource`-derived [`ConformanceObject`].
#[derive(
    kube::CustomResource,
    Clone,
    Debug,
    Default,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[kube(
    group = "conformance.kubo.dev",
    version = "v1",
    kind = "ConformanceObject",
    status = "ConformanceObjectStatus",
    namespaced
)]
pub struct ConformanceObjectSpec {}

/// Resource-specific status fields for the conformance object.  Empty: the
/// framework's `ResourceStatus` wrapper supplies `conditions`,
/// `observedGeneration`, and `pendingCleanup`, which is the entire surface the
/// ledger-progression scenario asserts on.
#[derive(
    Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub struct ConformanceFields {}

/// The framework-managed status wrapper.  Aliased so the `CustomResource`
/// derive's `status = "ConformanceObjectStatus"` picks up `pendingCleanup` and
/// the conditions the framework writes, without the harness re-declaring them.
pub type ConformanceObjectStatus = kubo::status::ResourceStatus<ConformanceFields>;

impl kubo::HasConditions for ConformanceObject {
    fn conditions(&self) -> Vec<Condition> {
        self.status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default()
    }
}

// ── Derived CRD schema ─────────────────────────────────────────────────────
//
// The test CRD's `openAPIV3Schema` used to be hand-written JSON duplicating the
// framework's status types — `pendingCleanup`, `conditions`, `observedGeneration`.
// A new framework status field would silently go stale in the conformance schema
// (the "extend the structural-pruning scenario first" rule was enforced by
// convention only).  The helpers below derive the schema from
// `ConformanceObject`'s schemars output via `kube::CustomResourceExt::crd`, so
// a new field added to `ResourceStatus` or `ConformanceFields` automatically
// enters the schema the conformance cluster installs and the structural-pruning
// scenario iterates.

/// The raw `openAPIV3Schema` derived from `ConformanceObject::crd()`, before the
/// preserve-unknown post-processing the default install applies.
///
/// This is the schemars output as-is: it declares `spec`, `status` (with
/// `conditions`, `observedGeneration`, `pendingCleanup`), and `required: ["spec"]`,
/// but carries no `x-kubernetes-preserve-unknown-fields` anywhere.  The raw-HTTP
/// status-subresource scenarios apply arbitrary undeclared fields (`marker`,
/// `stealField`, `keptField`, …) that must round-trip on the real apiserver, so
/// the default install does NOT use this verbatim — [`default_open_api_v3_schema`]
/// adds preserve-unknown to the `spec` and `status` nodes first.
fn derived_open_api_v3_schema() -> Value {
    use kube::CustomResourceExt;
    let crd = ConformanceObject::crd();
    serde_json::to_value(&crd)
        .expect("derived CRD serialises")
        .get("spec")
        .and_then(|s| s.get("versions"))
        .and_then(|v| v.get(0))
        .and_then(|v| v.get("schema"))
        .and_then(|s| s.get("openAPIV3Schema"))
        .expect("derived CRD has an openAPIV3Schema")
        .clone()
}

/// The default `openAPIV3Schema` for the conformance CRD, derived from
/// `ConformanceObject`'s schemars output with `x-kubernetes-preserve-unknown-fields`
/// added to the `spec` and `status` property nodes.
///
/// Deriving from the framework types means a new status field added to
/// [`kubo::status::ResourceStatus`] or [`ConformanceFields`] automatically
/// enters the schema the conformance cluster installs — closing the
/// "extend the structural-pruning scenario first by convention" gap.  The
/// schemars output is post-processed (not used verbatim) because it declares
/// `spec` and `status` as structured properties without preserve-unknown: the
/// raw-HTTP scenarios apply arbitrary, undeclared fields (`marker`,
/// `stealField`, `keptField`, …) that must round-trip on the real apiserver
/// exactly as the fake's verbatim replace does, so the derived schema must be
/// at least as permissive as the hand-written
/// `{ "type": "object", "x-kubernetes-preserve-unknown-fields": true }` it
/// replaces.  Declaring the framework fields AND preserving the unknown ones is
/// exactly the shape a real operator CRD takes.
///
/// Used by [`install_conformance_crd`] and as the restore schema the
/// structural-pruning and ledger-progression scenarios reinstall at the end of
/// a run so a later scenario in the same cluster is not poisoned by a tightened
/// status schema.
pub fn default_open_api_v3_schema() -> Value {
    let mut schema = derived_open_api_v3_schema();
    if let Some(props) = schema.get_mut("properties").and_then(|p| p.as_object_mut()) {
        for key in ["spec", "status"] {
            if let Some(node) = props.get_mut(key).and_then(|v| v.as_object_mut()) {
                node.insert(
                    "x-kubernetes-preserve-unknown-fields".to_string(),
                    Value::Bool(true),
                );
            }
        }
    }
    schema
}

/// A framework-managed status field the conformance CRD declares, with the
/// minimal OpenAPI `type` keyword and a representative value the
/// structural-pruning scenario writes via `apply_status`.
///
/// `type_keyword` is the field's top-level type ("array", "integer", …); the
/// "declaring" schema emits a minimal `{ "type": <type_keyword> }` declaration
/// per field (no `items`/`properties` constraints), which is enough to keep the
/// apiserver from pruning the field while accepting the representative value.
/// `representative_value` is a minimal value valid under that minimal
/// declaration.  Both are derived from the schemars output, so a new framework
/// status field automatically enters the structural-pruning surface without a
/// manual update.
#[derive(Clone, Debug)]
pub struct DerivedStatusField {
    pub name: String,
    pub type_keyword: String,
    pub representative_value: Value,
}

/// The framework-managed status fields the conformance CRD declares, derived
/// from `ConformanceObject`'s schemars output.
///
/// This is the set the structural-pruning scenario iterates when exercising
/// structural-schema pruning: the "declaring" schema declares every field in
/// this list, the "omitting" schema declares none, and the assertions walk
/// each field asserting present-when-declared / absent-when-omitted on the real
/// apiserver (and present in both passes on the fake, which has no schema).
/// A new field added to [`kubo::status::ResourceStatus`] or [`ConformanceFields`]
/// automatically enters this list, so the pruning test exercises it without a
/// manual update.  Sorted by name for deterministic iteration.
pub fn derived_status_fields() -> Vec<DerivedStatusField> {
    let schema = derived_open_api_v3_schema();
    let status_props = schema
        .get("properties")
        .and_then(|p| p.get("status"))
        .and_then(|s| s.get("properties"))
        .and_then(|p| p.as_object())
        .expect("derived status schema has properties");
    let mut fields = Vec::new();
    for (name, field_schema) in status_props {
        let type_keyword = field_schema
            .get("type")
            .and_then(|t| t.as_str())
            .unwrap_or("string")
            .to_string();
        let representative_value = match type_keyword.as_str() {
            "array" => serde_json::json!(["v"]),
            "integer" | "number" => serde_json::json!(1),
            "string" => serde_json::json!("v"),
            "object" => serde_json::json!({}),
            _ => serde_json::json!("v"),
        };
        fields.push(DerivedStatusField {
            name: name.clone(),
            type_keyword,
            representative_value,
        });
    }
    fields.sort_by(|a, b| a.name.cmp(&b.name));
    fields
}

// ── Trace ─────────────────────────────────────────────────────────────────────

/// The recorded outcome of a single scenario step.
///
/// Carries the HTTP status code together with the normalised content of the
/// response, so that divergences invisible to a status-only trace — e.g. a
/// status patch that returns `200` but silently prunes `pendingCleanup` — can
/// be detected.
#[derive(Debug, Clone, PartialEq)]
pub struct TraceEntry {
    /// HTTP status code returned by the API server for this step.
    pub status: u16,
    /// The returned object, normalised (see [`normalise_object`]).  Present
    /// for operations that return the object (GET, PATCH,
    /// DELETE-of-finalizer-blocked).  `None` for error responses, which carry
    /// a Kubernetes `Status` body rather than the resource itself.
    pub object: Option<Value>,
    /// The `status` subresource as the server persisted it, normalised (see
    /// [`normalise_status`]).  Extracted from the returned object's `status`
    /// field when present.  This is the surface the framework's ledger rides
    /// on, surfaced separately so scenarios can assert on status progression
    /// directly.
    pub status_subresource: Option<Value>,
    /// The raw `metadata.resourceVersion` the server returned for this step,
    /// captured *before* normalisation (which strips it as server-assigned).
    /// `None` for error responses or body-less operations (e.g. DELETE).
    ///
    /// This is not compared by [`assert_equivalent`] — it is server-assigned
    /// and differs between fake and real — but it is the handle a later step
    /// uses to send a `resourceVersion` precondition (the optimistic-concurrency
    /// pattern: GET to observe the rv, then patch with it).  See
    /// [`StepKind::ApplyStatusPrecondition`].
    pub resource_version: Option<String>,
    /// The error class of a reconcile failure, for the [`StepKind::RunReconcile`]
    /// `Err` arm.  `None` for every wire-level step and for a successful
    /// reconcile pass; `Some("conflict")` when the underlying error is a
    /// `kube::Error::Api` with HTTP 409 (the apiserver rejected an SSA patch on
    /// ownership/precondition grounds), or `Some("other: <Display>")` for any
    /// other reconcile failure (an effect returning `Err`, a serialisation
    /// failure, a missing input, etc.).  The synthetic `status` field on the same
    /// entry mirrors the wire status the conflict case would have returned (409)
    /// or a synthetic 500 for the "other" class (no wire call occurred), so the
    /// existing status comparison rides; this field carries the class for the
    /// new equivalence comparison so a fake succeeding where the real apiserver
    /// 409s is caught as a divergence rather than a panic.
    pub error: Option<String>,
    /// The name the scenario assigned to this step, if any.  Set from
    /// [`Step::name`] when the entry is recorded.  Not compared by
    /// [`assert_equivalent`] — both arms run the same scenario, so the names
    /// are identical by construction; the field exists so a test can look an
    /// entry up by name with [`Trace::entry`] / [`Trace::entry_opt`] instead of
    /// indexing by position, which shifts whenever a step is inserted or removed
    /// earlier in the scenario.
    pub name: Option<String>,
}

/// The recorded outcome of running a [`Scenario`].
#[derive(Debug, Clone, PartialEq)]
pub struct Trace {
    /// One entry per step, in execution order.
    pub entries: Vec<TraceEntry>,
}

impl Trace {
    /// Look up the first entry whose step was named `name`.  Panics if no step
    /// was given that name — a missing name is a programming error (the
    /// scenario forgot `.named(name)` on the step the test then tries to look
    /// up), and panicking here surfaces it at the assertion site rather than
    /// as a silent index-off-by-one.
    pub fn entry(&self, name: &str) -> &TraceEntry {
        self.entry_opt(name).unwrap_or_else(|| {
            panic!(
                "conformance: no trace entry named {name:?}; named steps are: {:?}",
                self.entries
                    .iter()
                    .filter_map(|e| e.name.as_deref())
                    .collect::<Vec<_>>()
            )
        })
    }

    /// Look up the first entry whose step was named `name`, or `None` if no step
    /// was given that name.  Prefer [`entry`](Trace::entry) for assertions where
    /// a missing name is a bug; use this when the assertion itself is "the step
    /// was never reached".
    pub fn entry_opt(&self, name: &str) -> Option<&TraceEntry> {
        self.entries
            .iter()
            .find(|e| e.name.as_deref() == Some(name))
    }

    /// Build a sub-trace from the entries whose steps were named, in the given
    /// order.  Each name resolves via [`entry`](Trace::entry) (first match
    /// wins; a name repeated in `names` yields the same entry twice).  Panics
    /// if any name is missing — a missing name is a programming error.
    ///
    /// This is the named analogue of positional slicing (`entries[0..8]`): it
    /// lets [`assert_equivalent`] compare a chosen subset of steps without
    /// coupling to step indices that shift when a step is inserted or removed
    /// earlier in the scenario.
    pub fn select(&self, names: &[&str]) -> Trace {
        Trace {
            entries: names.iter().map(|n| self.entry(n).clone()).collect(),
        }
    }
}

/// Assert that two [`Trace`]s are equivalent.
///
/// Compares status codes, the normalised returned object, and the normalised
/// `status` subresource, entry-by-entry.  Panics on any mismatch with the
/// failing step index and a side-by-side diff so the divergence is visible in
/// test output.
pub fn assert_equivalent(a: &Trace, b: &Trace) {
    assert_eq!(
        a.entries.len(),
        b.entries.len(),
        "trace length mismatch: fake has {}, real has {}",
        a.entries.len(),
        b.entries.len()
    );
    for (i, (ae, be)) in a.entries.iter().zip(b.entries.iter()).enumerate() {
        assert_eq!(
            ae.status, be.status,
            "status mismatch at step {i}: fake={}, real={}",
            ae.status, be.status
        );
        assert_eq!(
            ae.object, be.object,
            "object mismatch at step {i}:\n  fake: {:#?}\n  real: {:#?}",
            ae.object, be.object
        );
        assert_eq!(
            ae.status_subresource, be.status_subresource,
            "status subresource mismatch at step {i}:\n  fake: {:#?}\n  real: {:#?}",
            ae.status_subresource, be.status_subresource
        );
        assert_eq!(
            ae.error, be.error,
            "error class mismatch at step {i}:\n  fake: {:#?}\n  real: {:#?}",
            ae.error, be.error
        );
    }
}

// ── Normalisation ────────────────────────────────────────────────────────────

/// Fields under `metadata` that are server-assigned or non-deterministic across
/// fake and real clusters.  Stripped before comparison.
const SERVER_ASSIGNED_METADATA: &[&str] = &[
    "resourceVersion",
    "uid",
    "creationTimestamp",
    // `generation` is server-assigned and bumped on spec writes.  The fake
    // now models it too, but it is still server-assigned and not a field
    // the framework writes — so it stays stripped from the `metadata`
    // comparison.  Its reflection in `status.observedGeneration` (which the
    // framework copies it into) is the compared surface, and is *not* stripped.
    "generation",
    "managedFields",
    // `deletionTimestamp` is server-assigned on DELETE (the fake uses a stable
    // sentinel, real uses wall-clock) and is not a field the framework reads or
    // writes.  Stripping it lets the finalizer-scenario equivalence rows
    // compare the retained object's content (finalizers, spec) without
    // breaking on the timestamp value.  Retention itself is asserted via the
    // GET status code (200 vs 404), not via the timestamp's presence.
    "deletionTimestamp",
    // `deletionGracePeriodSeconds` is server-assigned on DELETE-with-finalizers
    // (real sets it; the fake does not model it).  Not read or written by the
    // framework.  Stripped so the retained-object comparison holds.
    "deletionGracePeriodSeconds",
];

/// Strip server-assigned and non-deterministic fields from a returned object
/// in place.
///
/// Removes the noisy `metadata` fields listed in
/// [`SERVER_ASSIGNED_METADATA`].  What remains — `spec`, framework-owned
/// `status`, and the stable parts of `metadata` (name, namespace, labels,
/// annotations, finalizers) — is the surface the framework reads and writes,
/// and is where fake/real equivalence must hold.
pub fn normalise_object(obj: &mut Value) {
    if let Some(meta) = obj.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        for k in SERVER_ASSIGNED_METADATA {
            meta.remove(*k);
        }
        // A real apiserver omits `metadata.finalizers` once the list is empty;
        // the fake persists `[]`.  Both mean "no finalizers" to the framework
        // (the cleanup loop treats absent and empty identically), so drop an
        // empty array to make equivalence hold on the cleared state — the
        // teardown-patch response and its "merge-patch clears the last
        // finalizer" row characterisation.
        if matches!(meta.get("finalizers"), Some(Value::Array(a)) if a.is_empty()) {
            meta.remove("finalizers");
        }
    }
}

/// Strip non-deterministic fields from a `status` subresource value in place.
///
/// Removes `conditions[*].lastTransitionTime` — wall-clock driven, differs
/// between fake and real even when the condition is otherwise identical.
///
/// `observedGeneration` (top-level and per-condition) is deliberately *not*
/// stripped: the fake now models `metadata.generation` (bumped on spec writes,
/// starting at 1 on create, mirroring the real apiserver), so the framework's
/// `status.observedGeneration` — the staleness signal it writes on every status
/// patch — compares equivalent fake-vs-real.  Stripping a framework-owned,
/// server-deterministic field to mask a fake gap is the normalisation-creep
/// anti-pattern; the gap is fixed in the fake instead.
pub fn normalise_status(status: &mut Value) {
    if let Some(conds) = status.get_mut("conditions").and_then(|c| c.as_array_mut()) {
        for cond in conds {
            if let Some(c) = cond.as_object_mut() {
                c.remove("lastTransitionTime");
            }
        }
    }
}

/// Build a [`TraceEntry`] from a raw HTTP status and an optional response body.
///
/// For non-2xx responses the body is a Kubernetes `Status` object, not the
/// resource, so both `object` and `status_subresource` are set to `None`.  For
/// 2xx responses the body is parsed as JSON, the `status` field is extracted and
/// normalised separately, and the remaining object is normalised.
fn make_entry(status: u16, body: Option<Value>) -> TraceEntry {
    if !(200..300).contains(&status) {
        return TraceEntry {
            status,
            object: None,
            status_subresource: None,
            resource_version: None,
            error: None,
            name: None,
        };
    }
    let mut obj = match body {
        Some(v) => v,
        None => {
            return TraceEntry {
                status,
                object: None,
                status_subresource: None,
                resource_version: None,
                error: None,
                name: None,
            };
        }
    };
    // Capture the server-assigned resourceVersion before normalisation strips
    // it — a later precondition step references it to send a stale rv.
    let resource_version = obj
        .get("metadata")
        .and_then(|m| m.get("resourceVersion"))
        .and_then(Value::as_str)
        .map(|s| s.to_string());
    let status_subresource = obj.get("status").cloned().map(|mut s| {
        normalise_status(&mut s);
        s
    });
    // Normalise the `status` embedded in the returned object too, so the
    // `object` comparison (which includes `status`) is consistent with the
    // `status_subresource` comparison.  A no-op for raw-HTTP scenarios, whose
    // hand-crafted status JSON carries no server-dependent fields; for the
    // framework-level `RunReconcile` harness it strips
    // `conditions[*].lastTransitionTime` (wall-clock driven).  `observedGeneration`
    // is left in place — the fake models `metadata.generation` the same way
    // the real apiserver does, so it compares equivalent.
    if let Some(s) = obj.get_mut("status") {
        normalise_status(s);
    }
    normalise_object(&mut obj);
    TraceEntry {
        status,
        object: Some(obj),
        status_subresource,
        resource_version,
        error: None,
        name: None,
    }
}

/// Read `metadata.name` from an object `Value`, for deterministic List sorting.
///
/// Free-standing (not a closure) so the borrow checker accepts the higher-ranked
/// reference returned into both sides of `sort_by`.
fn item_name(v: &Value) -> Option<&str> {
    v.get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(Value::as_str)
}

/// Build a [`TraceEntry`] from a LIST response (a Kubernetes `List` envelope:
/// `{ apiVersion, kind: "List", metadata: { resourceVersion, continue }, items }`).
///
/// `make_entry` assumes a single-object body and would normalise the envelope's
/// `metadata` (stripping `resourceVersion`) while leaving `items` untouched —
/// wrong for a List, where each item is a full object with server-assigned
/// metadata of its own.  This helper normalises each item (both the item's
/// `metadata` via [`normalise_object`] and its embedded `status` via
/// [`normalise_status`], so the per-item comparison is consistent with a
/// single-object `Get` of the same resource), then sorts the items by
/// `metadata.name` so the fake's `HashMap` iteration order and the real
/// apiserver's list order agree.
///
/// The normalised `items` array is stored in [`TraceEntry::object`] as a
/// `Value::Array`.  There is no `status` subresource at the List envelope level
/// (the envelope has no top-level `status` field), so `status_subresource` is
/// `None`.  The framework's `list()` discards the envelope's `metadata` and
/// returns `items` — equivalence on the normalised items is the surface the
/// framework depends on, so the envelope itself is not preserved in the trace.
///
/// For non-2xx responses the body is a Kubernetes `Status` object (e.g. a 404
/// for an unknown collection), so both `object` and `status_subresource` are
/// `None`, matching `make_entry`'s error shape.
fn make_list_entry(status: u16, body: Option<Value>) -> TraceEntry {
    if !(200..300).contains(&status) {
        return TraceEntry {
            status,
            object: None,
            status_subresource: None,
            resource_version: None,
            error: None,
            name: None,
        };
    }
    let mut envelope = match body {
        Some(v) => v,
        None => {
            return TraceEntry {
                status,
                object: Some(Value::Array(Vec::new())),
                status_subresource: None,
                resource_version: None,
                error: None,
                name: None,
            };
        }
    };
    let mut items = envelope
        .get_mut("items")
        .and_then(Value::as_array_mut)
        .map(std::mem::take)
        .unwrap_or_default();
    for item in items.iter_mut() {
        // Normalise the item's embedded `status` first, so the per-item
        // `object` comparison (which includes `status`) is consistent with a
        // single-object `Get` of the same resource.  A no-op for items without
        // a `status` field.
        if let Some(s) = item.get_mut("status") {
            normalise_status(s);
        }
        normalise_object(item);
    }
    // Sort by `metadata.name` so fake `HashMap` order and real-apiserver list
    // order agree.  Items without a `metadata.name` (a malformed envelope) sort
    // first by the `None`-is-less-than-`Some` rule; the conformance collection
    // always has names.
    items.sort_by(|a, b| item_name(a).cmp(&item_name(b)));
    TraceEntry {
        status,
        object: Some(Value::Array(items)),
        status_subresource: None,
        resource_version: None,
        error: None,
        name: None,
    }
}

// ── Step internals ────────────────────────────────────────────────────────────

struct Step {
    kind: StepKind,
    expected_status: Option<u16>,
    /// If set, store this step's returned `metadata.resourceVersion` into the
    /// scenario's capture map under this name, so a later
    /// [`StepKind::ApplyStatusPrecondition`] can reuse it as a precondition.
    /// See [`Scenario::capture_rv`].
    capture_rv: Option<String>,
    /// An optional name the scenario assigns to this step so a test can look the
    /// recorded [`TraceEntry`] up by name ([`Trace::entry`] / [`Trace::entry_opt`])
    /// instead of by positional index.  Set via [`Scenario::named`] on the most
    /// recently pushed step.  Positional `entries[N]` access still works; named
    /// lookups are an opt-in ergonomics layer that survives step reordering.
    name: Option<String>,
}

enum StepKind {
    Apply {
        manager: String,
        patch: Value,
        force: bool,
    },
    /// Server-Side Apply against the `/status` subresource.
    ///
    /// Sent as `application/apply-patch+yaml` to `.../status` with
    /// `fieldManager` and the given `force` flag.  This is the wire shape of
    /// the framework's `KubeHandle::patch_status`.
    ApplyStatus {
        manager: String,
        status: Value,
        force: bool,
    },
    /// SSA against `/status` carrying a `metadata.resourceVersion` precondition.
    ///
    /// Same wire shape as [`StepKind::ApplyStatus`], but the body's
    /// `metadata.resourceVersion` is set to a value captured from an earlier
    /// step (see [`Step::capture_rv`]).  This is the optimistic-concurrency
    /// pattern: a real controller GETs an object, remembers its
    /// `resourceVersion`, then patches with it; the apiserver 409s if an
    /// intervening write made the rv stale.  The fake does not enforce the
    /// precondition, so a stale rv that real rejects with 409 is silently
    /// accepted (200) by the fake — the stale-resourceVersion divergence.
    ///
    /// `rv_ref` names a prior capture slot populated by [`Step::capture_rv`].
    ApplyStatusPrecondition {
        manager: String,
        status: Value,
        force: bool,
        rv_ref: String,
    },
    /// GET the conformance object.
    ///
    /// Used to observe what the server actually persisted after a write —
    /// the only way a content-aware trace can catch silent pruning.
    Get,
    /// GET an arbitrary API path.
    ///
    /// Unlike [`StepKind::Get`] (which targets the scenario's conformance object
    /// at [`Scenario::object_path`]), this observes a non-parent resource — a
    /// child resource a [`StepKind::RunReconcile`] effect applied via
    /// [`KubeHandle::apply`], a sibling object, or any other namespaced or
    /// cluster-scoped path.  It is the observation primitive for multi-object
    /// scenarios: the `RunReconcile` effects apply and delete child resources
    /// through the framework's own `KubeHandle` code path, and this step
    /// independently observes the child's lifecycle on both backends so the
    /// equivalence assertion rides on the observable surface, not just on
    /// "the framework returned `Ok`".  The path is sent verbatim; the caller is
    /// responsible for the correct GVK path.
    GetPath { path: String },
    /// LIST a collection at an arbitrary API path, optionally filtered by a
    /// `labelSelector`.
    ///
    /// This is the collection analogue of [`StepKind::GetPath`]: `GetPath`
    /// observes a single named resource; `List` observes the collection.  It is
    /// the wire shape of the framework's `KubeHandle::list` / `list_all` (a
    /// plain `GET` against the collection path, no `fieldManager`, no body),
    /// which the runtime's reflector seeds from before switching to watch.
    ///
    /// The optional `label_selector` is sent verbatim as a `?labelSelector=`
    /// query parameter.  It exists so a scenario can isolate its own object
    /// under parallel real-cluster runs: the conformance suite runs test
    /// functions in parallel against a shared throwaway cluster, so a bare
    /// `List` of the conformance collection would return every scenario's
    /// object on the real arm but only the one object the fake holds — breaking
    /// equivalence.  A scenario that labels its object with a unique value and
    /// lists with `label_selector = "<that label>"` gets exactly its own object
    /// on both backends, and the equivalence assertion rides on the normalised
    /// items content.  The fake models only the `key=value` equality form of the
    /// selector grammar (see `matches_label_selector` in `kubo-test-fakeapi`);
    /// the harness never uses the richer forms.
    ///
    /// The recorded [`TraceEntry`] carries the normalised `items` array in
    /// `object` (each item normalised by [`normalise_object`] /
    /// [`normalise_status``], then the array sorted by `metadata.name` so fake
    /// `HashMap` iteration order and real-apiserver list order agree).  There is
    /// no `status` subresource at the List envelope level, so `status_subresource`
    /// is `None`.  The framework's `list()` discards the List envelope's
    /// `metadata.resourceVersion` and `continue` and returns `items` —
    /// equivalence on the normalised items is the surface the framework depends
    /// on.
    List {
        path: String,
        label_selector: Option<String>,
    },
    /// Replace `metadata.finalizers` via a merge-patch.
    ///
    /// Mirrors the framework's `KubeHandle::patch_finalizers`: PATCH with
    /// `application/merge-patch+json`, body `{"metadata":{"finalizers":[...]}}`,
    /// no `fieldManager`.  Merge-patch bypasses SSA ownership — it overwrites
    /// the finalizer list without claiming or releasing field ownership — which
    /// is the merge-patch-clears-SSA-owned-finalizer divergence surface (a
    /// merge-patch that clears a finalizer another manager SSA-owns silently
    /// succeeds on both fake and real, but the ownership record in
    /// `managedFields` may still reference the removed finalizer on real).
    PatchFinalizers { finalizers: Vec<String> },
    /// Plain DELETE of the conformance object.
    ///
    /// Mirrors the framework's `KubeHandle::delete`: DELETE with no body and
    /// no preconditions.  On both fake and real, if the object has finalizers
    /// the server sets `deletionTimestamp` and retains the object (finalizer-aware
    /// delete); if not, the object is removed immediately.
    Delete,
    /// Reinstall the `ConformanceObject` CRD with a new `openAPIV3Schema`.
    ///
    /// Real-cluster-only control step used by the structural-pruning scenario
    /// to swap the CRD's status schema mid-scenario: DELETE the CRD, wait for
    /// it to be gone, POST a new CRD whose `spec.versions[0].schema
    /// .openAPIV3Schema` is the given value, and wait for establishment.  On
    /// the fake path this is a no-op recorded as a synthetic `200` entry — the
    /// fake has no schema and no CRD, so reinstalling changes nothing it can
    /// observe.  The field carries the *full* `openAPIV3Schema` object (the
    /// value of `spec.versions[0].schema.openAPIV3Schema`), so the caller
    /// controls both the spec and status stanzas; the helper wraps it in the
    /// rest of the CRD envelope.
    ReinstallCrd { open_api_v3_schema: Value },
    /// Drive a real `Reconcile::execute` against the conformance object.
    ///
    /// This is the framework-level harness: unlike the raw-HTTP steps, it
    /// constructs a [`kubo::reconcile::Reconcile`] pipeline from a
    /// caller-supplied closure and runs it through the framework's own
    /// finalizer management, status-patch cadence, and SSA parameters against
    /// both backends.  It is the only step kind that exercises the framework
    /// itself, not just the wire protocol.
    ///
    /// The closure receives a [`kubo::environment::KubeHandle`] (built by the
    /// harness from the shared fake server on the fake path, or the real
    /// cluster's client on the real path) and the current
    /// [`ConformanceObject``] (GET'd by the harness before the reconcile), and
    /// returns a fully-built [`ReadyReconcile`] (pipeline + status builder).  The
    /// harness calls `.execute(finalizer)` and records a synthetic `200` on
    /// `Ok` (any [`Action`](kube::runtime::controller::Action) — `await_change`,
    /// `requeue`, etc. — is a successful reconcile).  On `Err` it records a trace
    /// entry carrying the error class (`"conflict"` for a `kube::Error::Api`
    /// with HTTP 409, `"other: <Display>"` otherwise) instead of panicking —
    /// a reconcile failure is itself a divergence signal, and panicking would
    /// discard both the comparison and the error class.
    ///
    /// The step records no object or status content: a reconcile pass may end on
    /// a finalizer clear that the fake garbage-collects synchronously, so a
    /// post-reconcile GET would race the real cluster's async GC.  The
    /// scenario observes the resulting `pendingCleanup` progression and
    /// finalizer state through explicit [`Get`](StepKind::Get) steps, which give
    /// the real GC controller time to settle — the same separation the
    /// finalizer scenario's "real GC async; fake GC sync" row relies on.
    ///
    /// Scope: the harness drives a single reconcile pass per step.  A scenario
    /// orchestrates the lifecycle by interleaving `RunReconcile` with
    /// `Delete`/`Get` steps; it does not simulate time or fault injection
    /// (fake conveniences with no real-cluster analogue).
    RunReconcile {
        field_manager: String,
        finalizer: String,
        pipeline: RunReconcilePipeline,
    },
}

/// The pipeline builder a [`StepKind::RunReconcile`] step runs each pass.
///
/// Higher-rank-bounded so the closure can hand the harness a `Reconcile` that
/// borrows the `KubeHandle` for exactly the duration of the pass, while the
/// closure itself (and the effect recorders it captures) stays `'static`.
pub type RunReconcilePipeline = Box<
    dyn for<'a> Fn(
            &'a kubo::environment::KubeHandle,
            Arc<ConformanceObject>,
        ) -> ReadyReconcile<'a, ConformanceObject, ConformanceFields>
        + Send
        + Sync,
>;

// ── Scenario ──────────────────────────────────────────────────────────────────

/// A replayable sequence of API operations with optional per-step assertions.
///
/// Build with the fluent API, then call [`run_against_fake`](Scenario::run_against_fake)
/// to execute against the in-process fake server.
///
/// Every scenario operates on its own conformance object, named after the
/// scenario (see [`Scenario::object_name`]).  The object lives at
/// `/apis/conformance.kubo.dev/v1/namespaces/default/conformanceobjects/<name>`
/// and is created by the scenario's first apply and destroyed (on the real
/// arm) by [`reset_conformance_object`] before the scenario runs.  Per-scenario
/// objects are what unlocks real-side parallelism and multi-object scenarios;
/// the one shared resource that remains is the CRD itself, which `ReinstallCrd`
/// steps delete wholesale — see that step's docs for the serialisation caveat.
pub struct Scenario {
    name: String,
    steps: Vec<Step>,
}

impl Scenario {
    /// Create a new empty scenario with the given name.
    ///
    /// The name doubles as the Kubernetes object name for the scenario (after
    /// slugification), so it must be unique across the whole suite — two
    /// scenarios sharing a name would operate on the same real-cluster object
    /// and collide under a parallel runner.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            steps: Vec::new(),
        }
    }

    /// The Kubernetes object name for this scenario, derived by slugifying
    /// [`Scenario::name`].  Scenario names in the suite are already lowercase
    /// and hyphen-separated; slugification is a safety net so a future name
    /// with an uppercase character or trailing space cannot produce an invalid
    /// object name silently.
    fn object_name(&self) -> String {
        slugify(&self.name)
    }

    /// The full API path of this scenario's conformance object.
    fn object_path(&self) -> String {
        format!(
            "/apis/conformance.kubo.dev/v1/namespaces/default/conformanceobjects/{}",
            self.object_name()
        )
    }

    /// Append a Server-Side Apply operation with `force = false`.
    ///
    /// The patch is sent as `application/apply-patch+yaml`.  With `force =
    /// false`, a conflict with an existing field owner returns 409.
    pub fn apply(mut self, manager: &str, patch: Value) -> Self {
        self.steps.push(Step {
            kind: StepKind::Apply {
                manager: manager.to_string(),
                patch,
                force: false,
            },
            expected_status: None,
            capture_rv: None,
            name: None,
        });
        self
    }

    /// Append a Server-Side Apply operation with `force = true`.
    ///
    /// Force-apply steals field ownership from any conflicting manager,
    /// mirroring `PatchParams::apply(manager).force()` in kube-rs.
    pub fn apply_force(mut self, manager: &str, patch: Value) -> Self {
        self.steps.push(Step {
            kind: StepKind::Apply {
                manager: manager.to_string(),
                patch,
                force: true,
            },
            expected_status: None,
            capture_rv: None,
            name: None,
        });
        self
    }

    /// Append a Server-Side Apply against the `/status` subresource with
    /// `force = false`.
    ///
    /// This is the wire shape of the framework's `KubeHandle::patch_status`:
    /// non-force SSA against `/status` (the framework contract).  With `force =
    /// false`, a real apiserver returns 409 if another manager owns a
    /// conflicting status field; the fake does not model status-subresource
    /// ownership and accepts the patch.  This is the step the framework's own
    /// status write rides on.
    pub fn apply_status(mut self, manager: &str, status: Value) -> Self {
        self.steps.push(Step {
            kind: StepKind::ApplyStatus {
                manager: manager.to_string(),
                status,
                force: false,
            },
            expected_status: None,
            capture_rv: None,
            name: None,
        });
        self
    }

    /// Append a Server-Side Apply against the `/status` subresource with
    /// `force = true`.
    ///
    /// This is the force variant the framework does **not** use on status:
    /// `KubeHandle::patch_status` is non-force by contract, and no force
    /// opt-in is offered on status.  This step exists to characterise the real
    /// apiserver's force-on-status semantics — the ownership-steal row in the
    /// status-subresource scenario family — as a wire-level characterisation,
    /// not a framework contract.  It steals ownership of any conflicting
    /// status field on a real apiserver; the fake does not model
    /// status-subresource ownership and accepts the patch.
    pub fn apply_status_force(mut self, manager: &str, status: Value) -> Self {
        self.steps.push(Step {
            kind: StepKind::ApplyStatus {
                manager: manager.to_string(),
                status,
                force: true,
            },
            expected_status: None,
            capture_rv: None,
            name: None,
        });
        self
    }

    /// Append an SSA against `/status` carrying a `metadata.resourceVersion`
    /// precondition captured from a prior step.
    ///
    /// `rv_ref` names a capture slot populated by [`Scenario::capture_rv`] on an
    /// earlier step (typically a GET).  The captured value is injected into the
    /// apply body's `metadata.resourceVersion`; the apiserver 409s if an
    /// intervening write made it stale.  The fake does not enforce the
    /// precondition, so it returns 200 — the stale-resourceVersion divergence.
    pub fn apply_status_precondition(mut self, manager: &str, status: Value, rv_ref: &str) -> Self {
        self.steps.push(Step {
            kind: StepKind::ApplyStatusPrecondition {
                manager: manager.to_string(),
                status,
                force: true,
                rv_ref: rv_ref.to_string(),
            },
            expected_status: None,
            capture_rv: None,
            name: None,
        });
        self
    }

    /// Append a GET of the conformance object.
    ///
    /// The returned object is recorded in the trace (normalised), so a
    /// scenario can observe what the server persisted after a prior write —
    /// the only way to catch a field that a write returned `200` for but the
    /// server silently pruned.
    pub fn get(mut self) -> Self {
        self.steps.push(Step {
            kind: StepKind::Get,
            expected_status: None,
            capture_rv: None,
            name: None,
        });
        self
    }

    /// Append a GET against an arbitrary API path.
    ///
    /// Unlike [`get`](Scenario::get) (which targets the scenario's conformance
    /// object), this observes a non-parent resource — a child resource an effect
    /// applied, a sibling object, etc.  It is the observation primitive for
    /// multi-object scenarios: the `RunReconcile` effects apply and delete
    /// child resources through [`KubeHandle::apply`] / [`KubeHandle::delete`],
    /// and this step independently observes the child's lifecycle on both
    /// backends so the equivalence assertion is on the observable surface, not
    /// just on "the framework returned `Ok`".  The path is sent verbatim; the
    /// caller is responsible for the correct GVK path (e.g.
    /// `/api/v1/namespaces/default/configmaps/<name>` for a core-v1 ConfigMap).
    pub fn get_path(mut self, path: impl Into<String>) -> Self {
        self.steps.push(Step {
            kind: StepKind::GetPath { path: path.into() },
            expected_status: None,
            capture_rv: None,
            name: None,
        });
        self
    }

    /// Append a LIST against an arbitrary collection path with no label selector.
    ///
    /// This is the collection analogue of [`get_path`](Scenario::get_path).
    /// It mirrors the framework's `KubeHandle::list` / `list_all` wire shape:
    /// a plain `GET` against the collection path.  The recorded entry carries
    /// the normalised `items` array (sorted by `metadata.name`) in `object`.
    ///
    /// **Real-arm caveat.**  The conformance suite runs test functions in
    /// parallel against a shared throwaway cluster, so a bare `list` of a
    /// collection other scenarios also write returns every scenario's object
    /// on the real arm but only the one object the fake holds — equivalence
    /// breaks.  Prefer [`Scenario::list_labeled`] for any collection shared
    /// across scenarios; reserve the bare `list` for collections a single
    /// scenario owns outright.
    pub fn list(mut self, path: impl Into<String>) -> Self {
        self.steps.push(Step {
            kind: StepKind::List {
                path: path.into(),
                label_selector: None,
            },
            expected_status: None,
            capture_rv: None,
            name: None,
        });
        self
    }

    /// Append a LIST against an arbitrary collection path filtered by a
    /// `labelSelector`.
    ///
    /// The selector is sent verbatim as a `?labelSelector=` query parameter.
    /// A scenario that labels its own object with a unique value and lists with
    /// `label_selector = "<that label>"` isolates its object on both backends,
    /// so the equivalence assertion rides on the normalised items content rather
    /// than on the full collection (which would be polluted by parallel
    /// scenarios on the real arm).  The fake models only the `key=value`
    /// equality form (comma-separated terms ANDed); see [`StepKind::List`].
    pub fn list_labeled(
        mut self,
        path: impl Into<String>,
        label_selector: impl Into<String>,
    ) -> Self {
        self.steps.push(Step {
            kind: StepKind::List {
                path: path.into(),
                label_selector: Some(label_selector.into()),
            },
            expected_status: None,
            capture_rv: None,
            name: None,
        });
        self
    }

    /// Append a merge-patch of `metadata.finalizers` with the given list.
    ///
    /// Mirrors `KubeHandle::patch_finalizers`: `application/merge-patch+json`,
    /// no `fieldManager`.  An empty list clears the finalizers — on a terminating
    /// object (one with `deletionTimestamp` set from a prior DELETE) this is the
    /// teardown signal: the apiserver's GC controller removes the object once the
    /// last finalizer is gone.
    pub fn patch_finalizers(mut self, finalizers: &[&str]) -> Self {
        self.steps.push(Step {
            kind: StepKind::PatchFinalizers {
                finalizers: finalizers.iter().map(|s| s.to_string()).collect(),
            },
            expected_status: None,
            capture_rv: None,
            name: None,
        });
        self
    }

    /// Append a plain DELETE of the conformance object.
    ///
    /// Mirrors `KubeHandle::delete`: no body, no preconditions.  If the object
    /// has finalizers, the server sets `deletionTimestamp` and retains it; if
    /// not, the object is removed immediately.
    pub fn delete(mut self) -> Self {
        self.steps.push(Step {
            kind: StepKind::Delete,
            expected_status: None,
            capture_rv: None,
            name: None,
        });
        self
    }

    /// Append a CRD reinstall with the given `openAPIV3Schema`.
    ///
    /// Real-cluster-only control step (see [`StepKind::ReinstallCrd`]): swaps
    /// the `ConformanceObject` CRD's schema mid-scenario.  On the fake path this
    /// is a no-op (the fake has no schema).  The `open_api_v3_schema` is the
    /// full value of `spec.versions[0].schema.openAPIV3Schema` — the caller owns
    /// both the spec and status stanzas.  Used by the structural-pruning
    /// scenario to toggle whether `pendingCleanup` is a declared status field,
    /// and to restore the preserve-unknown default at the end so subsequent
    /// scenarios in the same cluster are not poisoned.
    pub fn reinstall_crd(mut self, open_api_v3_schema: Value) -> Self {
        self.steps.push(Step {
            kind: StepKind::ReinstallCrd { open_api_v3_schema },
            expected_status: None,
            capture_rv: None,
            name: None,
        });
        self
    }

    /// Append a framework-level reconcile pass.
    ///
    /// The harness GETs the scenario's object as a typed [`ConformanceObject`], builds a
    /// [`kubo::environment::KubeHandle`] with `field_manager`, calls `pipeline`
    /// to construct the [`ReadyReconcile`] (effects + status builder), and runs
    /// `.execute(finalizer)`.  The object must exist before this step — seed it
    /// with an [`apply`](Scenario::apply) (or rely on a prior step's retention).
    ///
    /// The closure is `'static` (capture `Arc`-based recorders by clone); the
    /// harness reuses it for every `RunReconcile` step in the scenario, so any
    /// per-run mutable state it carries (e.g. a retry counter) must be reset
    /// between [`run_against_fake`](Scenario::run_against_fake) and
    /// [`run_against_kube_apiserver`](Scenario::run_against_kube_apiserver) by the
    /// caller.
    pub fn run_reconcile<F>(mut self, field_manager: &str, finalizer: &str, pipeline: F) -> Self
    where
        F: for<'a> Fn(
                &'a kubo::environment::KubeHandle,
                Arc<ConformanceObject>,
            ) -> ReadyReconcile<'a, ConformanceObject, ConformanceFields>
            + Send
            + Sync
            + 'static,
    {
        self.steps.push(Step {
            kind: StepKind::RunReconcile {
                field_manager: field_manager.to_string(),
                finalizer: finalizer.to_string(),
                pipeline: Box::new(pipeline),
            },
            expected_status: None,
            capture_rv: None,
            name: None,
        });
        self
    }

    /// Assert that the most recently added step produces the given HTTP status.
    ///
    /// If the actual status differs, [`run_against_fake`](Scenario::run_against_fake)
    /// panics with a descriptive message that includes the scenario name and
    /// step index.
    pub fn expect_last_status(mut self, code: u16) -> Self {
        if let Some(last) = self.steps.last_mut() {
            last.expected_status = Some(code);
        }
        self
    }

    /// Capture the most recently added step's returned `metadata.resourceVersion`
    /// into a named slot, so a later [`Scenario::apply_status_precondition`] can
    /// reuse it as an optimistic-concurrency precondition.
    ///
    /// The capture happens after the step runs: the slot is filled with the
    /// `resourceVersion` the server returned for that step (extracted before
    /// normalisation strips it).  Typically applied to a GET — the controller
    /// pattern is "GET to observe the rv, then patch with it".
    pub fn capture_rv(mut self, name: &str) -> Self {
        if let Some(last) = self.steps.last_mut() {
            last.capture_rv = Some(name.to_string());
        }
        self
    }

    /// Name the most recently added step, so a test can look its recorded
    /// [`TraceEntry`] up by name with [`Trace::entry`] / [`Trace::entry_opt`]
    /// instead of by positional index.
    ///
    /// Positional indices (`trace.entries[N]`) shift whenever a step is inserted
    /// or removed earlier in the scenario, and a mis-indexed assertion is a
    /// false proof — the worst failure mode a conformance test can have. Named
    /// lookups survive reordering. Like [`expect_last_status`] / [`capture_rv`],
    /// this operates on the last pushed step, so it chains after a builder:
    /// `.apply(...).named("seed")`. Names are not required to be unique, but a
    /// lookup that matches more than one entry returns the first; prefer unique
    /// names so the lookup is unambiguous.
    pub fn named(mut self, name: &str) -> Self {
        if let Some(last) = self.steps.last_mut() {
            last.name = Some(name.to_string());
        }
        self
    }

    /// Execute the scenario against the in-process fake API server.
    ///
    /// Steps run in order.  Steps with an `expected_status` assertion panic if
    /// the actual status code does not match.  Returns the full content-aware
    /// [`Trace`] so the caller can inspect individual entries or pass it to
    /// [`assert_equivalent`].
    pub fn run_against_fake(&self) -> Trace {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("conformance: failed to build tokio runtime");
        rt.block_on(self.execute(FakeApiServer::new()))
    }

    /// Execute the scenario against a real Kubernetes cluster.
    ///
    /// Reads credentials from the default kubeconfig (or `KUBECONFIG`).  On
    /// first use, installs the `ConformanceObject` CRD into the cluster via a
    /// plain POST, ignoring 409 if it already exists.  Each step is sent as a
    /// raw HTTP request so the wire format is identical to the fake path,
    /// letting [`assert_equivalent`] do a meaningful comparison.
    ///
    /// The patch body is enriched with `apiVersion`, `kind`, and
    /// `metadata.name` before dispatch — required by the real kube-apiserver
    /// for SSA but not enforced by the fake.
    ///
    /// # Panics
    ///
    /// Panics if the cluster is unreachable or if CRD installation fails with
    /// an error other than 409 Conflict.  Also panics before any mutation if the
    /// cluster has not been explicitly opted in via `KUBO_CONFORMANCE_CLUSTER`
    /// (see [`verify_cluster_identity`]).
    #[cfg(feature = "k3d")]
    pub fn run_against_kube_apiserver(&self) -> Trace {
        // Each scenario operates on its own object (named after the scenario),
        // so concurrent scenarios no longer interleave on a shared object.  The
        // one shared resource that remains is the CRD itself: a `ReinstallCrd`
        // step deletes the CRD wholesale and garbage-collects every instance of
        // the type, so scenarios that reinstall the CRD are not safe to run
        // concurrently with other scenarios against the same cluster — see
        // [`StepKind::ReinstallCrd`].  `cargo test` runs test binaries
        // sequentially, so the suite is safe today; a parallel runner (nextest)
        // would need to serialise the CRD-reinstalling binaries.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("conformance: failed to build tokio runtime");
        rt.block_on(self.execute_against_cluster())
    }

    #[cfg(feature = "k3d")]
    async fn execute_against_cluster(&self) -> Trace {
        let config = kube::Config::infer()
            .await
            .expect("k3d: cannot load kubeconfig");
        verify_cluster_identity(&config);
        let client =
            kube::Client::try_from(config).expect("k3d: cannot build kube client from kubeconfig");

        install_conformance_crd(&client).await;

        let path = self.object_path();
        let object_name = self.object_name();

        // The fake path starts each scenario on a fresh in-process server; the
        // real path shares a single cluster across every scenario in the
        // process.  Without a reset, status fields a prior scenario wrote to this
        // object would linger (real SSA merges, it never drops fields), so a
        // later scenario reusing the name would compare a clean fake object
        // against a polluted real one and fail for harness reasons rather than
        // real divergences.  Delete this scenario's object and wait for it to be
        // gone so the real path starts as clean as the fake.  Reset now fails
        // hard (panics) if the object does not converge to gone: a stuck reset
        // used to carry on regardless and surface later as an equivalence
        // failure with the evidence gone (the original anti-pattern that this
        // reset was tightened to eliminate).
        reset_conformance_object_strict(&client, &path).await;

        let mut entries = Vec::new();
        let mut captures: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for (i, step) in self.steps.iter().enumerate() {
            let mut entry = run_k3d_step(step, &client, &path, &object_name, &captures).await;
            entry.name = step.name.clone();
            if let Some(expected) = step.expected_status {
                assert_eq!(
                    entry.status, expected,
                    "scenario {:?}: step {i} expected HTTP {expected} but got {}",
                    self.name, entry.status
                );
            }
            if let Some(name) = &step.capture_rv
                && let Some(rv) = &entry.resource_version
            {
                captures.insert(name.clone(), rv.clone());
            }
            entries.push(entry);
        }
        // Teardown: remove this scenario's object so it does not linger.  With
        // per-scenario names no later scenario re-reset's this object, so a
        // finalizer scenario that leaves its object Terminating with an
        // unmanaged finalizer would pollute the cluster and block any later
        // `ReinstallCrd` (the CRD's cleanup finalizer waits for every instance to
        // drain).  Best-effort with a warning rather than a panic — the scenario
        // already passed; a slow GC should not turn a green test red.
        destroy_conformance_object(&client, &path).await;
        Trace { entries }
    }

    async fn execute(&self, server: FakeApiServer) -> Trace {
        let path = self.object_path();
        let object_name = self.object_name();

        let mut entries = Vec::new();
        let mut captures: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for (i, step) in self.steps.iter().enumerate() {
            let mut entry = run_step(step, &path, &object_name, &server, &captures).await;
            entry.name = step.name.clone();
            if let Some(expected) = step.expected_status {
                assert_eq!(
                    entry.status, expected,
                    "scenario {:?}: step {i} expected HTTP {expected} but got {}",
                    self.name, entry.status
                );
            }
            if let Some(name) = &step.capture_rv
                && let Some(rv) = &entry.resource_version
            {
                captures.insert(name.clone(), rv.clone());
            }
            entries.push(entry);
        }
        Trace { entries }
    }
}

// ── Step execution ────────────────────────────────────────────────────────────

/// Reduce a scenario name to a valid Kubernetes object name.
///
/// Kubernetes object names are lowercase RFC 1123 subdomains: lowercase
/// alphanumeric and `-`, starting and ending alphanumeric, max 253 chars.
/// Scenario names in the suite are already in that shape; this is a guard so a
/// stray uppercase or space fails loudly at the slug boundary instead of
/// producing an object name the apiserver rejects mid-scenario.
fn slugify(input: &str) -> String {
    let mut out: String = input
        .trim()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    // Collapse runs of `-` and strip leading/trailing `-`.
    while out.contains("--") {
        out = out.replace("--", "-");
    }
    out = out.trim_matches('-').to_string();
    if out.is_empty() {
        panic!("conformance: scenario name {input:?} slugifies to an empty object name");
    }
    if out.len() > 253 {
        out.truncate(253);
    }
    out
}

/// Enrich an SSA patch body with the conformance object's identity fields.
///
/// The real kube-apiserver requires `apiVersion`, `kind`, and `metadata.name`
/// in every SSA request body; the fake doesn't enforce it.  Applying the same
/// enrichment on both paths keeps the wire format — and therefore the stored
/// and returned object — identical, which is what the content-aware trace
/// compares against.
fn enrich_apply_patch(patch: &Value, object_name: &str) -> Value {
    let mut full = patch.clone();
    if let Some(obj) = full.as_object_mut() {
        obj.entry("apiVersion")
            .or_insert_with(|| serde_json::json!("conformance.kubo.dev/v1"));
        obj.entry("kind")
            .or_insert_with(|| serde_json::json!("ConformanceObject"));
        // Merge `name` / `namespace` into the patch's `metadata` rather than
        // `or_insert`-ing the whole object: a patch that carries its own
        // `metadata` (e.g. `metadata.labels` for the List scenario's isolation
        // label) must still get `name` / `namespace` from the path, exactly as
        // the real apiserver injects them from the URL on create.  The previous
        // `or_insert_with` form silently dropped `name` for any patch that
        // supplied its own `metadata`, leaving the fake's stored object
        // nameless where the real apiserver would have named it.
        let meta = obj
            .entry("metadata")
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
            .expect("conformance: apply patch metadata is not an object");
        meta.entry("name")
            .or_insert_with(|| serde_json::json!(object_name));
        meta.entry("namespace")
            .or_insert_with(|| serde_json::json!("default"));
    }
    full
}

async fn run_step(
    step: &Step,
    path: &str,
    object_name: &str,
    server: &FakeApiServer,
    captures: &std::collections::HashMap<String, String>,
) -> TraceEntry {
    match &step.kind {
        StepKind::Apply {
            manager,
            patch,
            force,
        } => {
            // The real kube-apiserver requires apiVersion, kind, and
            // metadata.name in every SSA request body.  The fake doesn't
            // enforce it, but we enrich both paths identically so the wire
            // format — and therefore the stored/returned object — matches.
            let full_patch = enrich_apply_patch(patch, object_name);
            let body_bytes =
                serde_json::to_vec(&full_patch).expect("conformance: patch serialization failed");
            let force_str = if *force { "true" } else { "false" };
            let uri = format!("{}?fieldManager={}&force={}", path, manager, force_str);

            let req = Request::builder()
                .method(Method::PATCH)
                .uri(&uri)
                .header("content-type", "application/apply-patch+yaml")
                .body(Full::new(Bytes::from(body_bytes)))
                .expect("conformance: failed to build request");

            let resp = {
                let mut svc = server.clone();
                svc.call(req)
                    .await
                    .expect("conformance: fake API server returned an error")
            };

            let status = resp.status().as_u16();
            let body = read_fake_body(resp.into_body()).await;
            make_entry(status, body)
        }
        StepKind::ApplyStatus {
            manager,
            status,
            force,
        } => {
            // Wire shape mirrors `KubeHandle::patch_status`: SSA against the
            // `/status` subresource with `fieldManager` and `force`.  The
            // fake ignores all three (it replaces the `status` field
            // verbatim), but we send them anyway so the wire format matches
            // the real path.
            let body = serde_json::json!({
                "apiVersion": "conformance.kubo.dev/v1",
                "kind": "ConformanceObject",
                "metadata": { "name": object_name, "namespace": "default" },
                "status": status,
            });
            let body_bytes =
                serde_json::to_vec(&body).expect("conformance: status patch serialization failed");
            let force_str = if *force { "true" } else { "false" };
            let uri = format!(
                "{}/status?fieldManager={}&force={}",
                path, manager, force_str
            );

            let req = Request::builder()
                .method(Method::PATCH)
                .uri(&uri)
                .header("content-type", "application/apply-patch+yaml")
                .body(Full::new(Bytes::from(body_bytes)))
                .expect("conformance: failed to build status request");

            let resp = {
                let mut svc = server.clone();
                svc.call(req)
                    .await
                    .expect("conformance: fake API server returned an error")
            };

            let status = resp.status().as_u16();
            let body = read_fake_body(resp.into_body()).await;
            make_entry(status, body)
        }
        StepKind::ApplyStatusPrecondition {
            manager,
            status,
            force,
            rv_ref,
        } => {
            // Same wire shape as `ApplyStatus`, but the body's
            // `metadata.resourceVersion` is set from a prior capture.  The fake
            // does not enforce the precondition, so it returns 200 even on a
            // stale rv — the stale-resourceVersion divergence surface.  A
            // missing `rv_ref` is a programming error (the scenario forgot a
            // `capture_rv` on an earlier step), not a server behaviour, so it
            // panics loudly rather than degrading to a silent 200.
            let rv = captures.get(rv_ref).unwrap_or_else(|| {
                panic!("conformance: ApplyStatusPrecondition references uncaptured rv_ref {rv_ref}")
            });
            let body = serde_json::json!({
                "apiVersion": "conformance.kubo.dev/v1",
                "kind": "ConformanceObject",
                "metadata": {
                    "name": object_name,
                    "namespace": "default",
                    "resourceVersion": rv,
                },
                "status": status,
            });
            let body_bytes = serde_json::to_vec(&body)
                .expect("conformance: precondition status patch serialization failed");
            let force_str = if *force { "true" } else { "false" };
            let uri = format!(
                "{}/status?fieldManager={}&force={}",
                path, manager, force_str
            );

            let req = Request::builder()
                .method(Method::PATCH)
                .uri(&uri)
                .header("content-type", "application/apply-patch+yaml")
                .body(Full::new(Bytes::from(body_bytes)))
                .expect("conformance: failed to build precondition status request");

            let resp = {
                let mut svc = server.clone();
                svc.call(req)
                    .await
                    .expect("conformance: fake API server returned an error")
            };

            let status = resp.status().as_u16();
            let body = read_fake_body(resp.into_body()).await;
            make_entry(status, body)
        }
        StepKind::Get => {
            let req = Request::builder()
                .method(Method::GET)
                .uri(path)
                .body(Full::new(Bytes::new()))
                .expect("conformance: failed to build GET request");

            let resp = {
                let mut svc = server.clone();
                svc.call(req)
                    .await
                    .expect("conformance: fake API server returned an error")
            };

            let status = resp.status().as_u16();
            let body = read_fake_body(resp.into_body()).await;
            make_entry(status, body)
        }
        StepKind::GetPath { path: target } => {
            // Same shape as `Get`, but against an arbitrary path — the
            // observation primitive for multi-object scenarios.  The fake's
            // `parse_route` is GVK-agnostic, so a core-group path like
            // `/api/v1/namespaces/<ns>/configmaps/<name>` routes through the
            // same object handler as the conformance CRD's path.
            let req = Request::builder()
                .method(Method::GET)
                .uri(target)
                .body(Full::new(Bytes::new()))
                .expect("conformance: failed to build GET request");

            let resp = {
                let mut svc = server.clone();
                svc.call(req)
                    .await
                    .expect("conformance: fake API server returned an error")
            };

            let status = resp.status().as_u16();
            let body = read_fake_body(resp.into_body()).await;
            make_entry(status, body)
        }
        StepKind::List {
            path: target,
            label_selector,
        } => {
            // The collection analogue of `GetPath`.  A plain `GET` against
            // the collection path, optionally with a `?labelSelector=` query
            // for real-arm isolation under parallel runs.  The fake's
            // `handle_collection` serves the `List` envelope and applies the
            // selector server-side; `make_list_entry` normalises each item
            // and sorts by `metadata.name` so fake `HashMap` order and
            // real-apiserver list order agree.
            let uri = match label_selector {
                Some(sel) => format!("{target}?labelSelector={sel}"),
                None => target.clone(),
            };
            let req = Request::builder()
                .method(Method::GET)
                .uri(&uri)
                .body(Full::new(Bytes::new()))
                .expect("conformance: failed to build LIST request");

            let resp = {
                let mut svc = server.clone();
                svc.call(req)
                    .await
                    .expect("conformance: fake API server returned an error")
            };

            let status = resp.status().as_u16();
            let body = read_fake_body(resp.into_body()).await;
            make_list_entry(status, body)
        }
        StepKind::PatchFinalizers { finalizers } => {
            // Wire shape mirrors `KubeHandle::patch_finalizers`:
            // `application/merge-patch+json`, no `fieldManager`, body is a bare
            // `{"metadata":{"finalizers":[...]}}`.  Merge-patch does not go
            // through the fake's SSA ownership tracking — it overwrites the
            // finalizer list directly — which is the
            // merge-patch-clears-SSA-owned-finalizer divergence surface.  The
            // fake's `merge_patch` models finalizer-triggered GC: if the
            // object is terminating (deletionTimestamp set from a prior
            // DELETE) and finalizers become empty, the object is removed from
            // the store so the next GET returns 404.
            let body = serde_json::json!({ "metadata": { "finalizers": finalizers } });
            let body_bytes = serde_json::to_vec(&body)
                .expect("conformance: finalizer patch serialization failed");
            let req = Request::builder()
                .method(Method::PATCH)
                .uri(path)
                .header("content-type", "application/merge-patch+json")
                .body(Full::new(Bytes::from(body_bytes)))
                .expect("conformance: failed to build finalizer patch request");

            let resp = {
                let mut svc = server.clone();
                svc.call(req)
                    .await
                    .expect("conformance: fake API server returned an error")
            };

            let status = resp.status().as_u16();
            let body = read_fake_body(resp.into_body()).await;
            make_entry(status, body)
        }
        StepKind::Delete => {
            // Wire shape mirrors `KubeHandle::delete`: plain DELETE, no body, no
            // preconditions.  The fake's `finalizer_delete` is finalizer-aware:
            // if `metadata.finalizers` is non-empty it sets `deletionTimestamp`
            // (stable sentinel) and retains the object, broadcasting MODIFIED;
            // otherwise it removes the object and broadcasts DELETED.
            //
            // The DELETE response body is a Kubernetes `Status` object on a real
            // apiserver (not the resource); the framework discards it
            // (`KubeHandle::delete` returns `Ok(())`).  Record only the status
            // code so the trace does not assert on apiserver serialisation the
            // framework does not depend on.  Retention vs removal is pinned by
            // the subsequent GET's status code (200 vs 404).
            let req = Request::builder()
                .method(Method::DELETE)
                .uri(path)
                .body(Full::new(Bytes::new()))
                .expect("conformance: failed to build DELETE request");

            let resp = {
                let mut svc = server.clone();
                svc.call(req)
                    .await
                    .expect("conformance: fake API server returned an error")
            };

            let status = resp.status().as_u16();
            make_entry(status, None)
        }
        StepKind::ReinstallCrd { open_api_v3_schema } => {
            // The fake has no CRD and no schema — reinstalling changes nothing
            // it can observe.  The schema payload is carried only for the real
            // path's [`reinstall_conformance_crd`]; the fake drops it here.
            // Record a synthetic 200 so the trace stays shape-compatible with the
            // real path.  The structural-pruning scenario does not call
            // [`assert_equivalent`], so this entry is never compared for
            // content; it exists only to keep the step indices aligned.
            let _ = open_api_v3_schema;
            make_entry(200, None)
        }
        StepKind::RunReconcile {
            field_manager,
            finalizer,
            pipeline,
        } => {
            // Build a real kube-rs client backed by the same in-memory store the
            // raw-HTTP steps address (the `FakeApiServer` is Arc-backed, so
            // `server.clone()` shares the store).  This is the same wiring
            // `World::builder().build()` uses, inlined here so the harness owns
            // the `KubeHandle` directly without depending on the fake's
            // higher-level `World` simulation.
            let client = kube::Client::new(server.clone(), "default");
            run_reconcile_pass(client, field_manager, finalizer, object_name, pipeline).await
        }
    }
}

/// Shared body of the `RunReconcile` step for both backends.
///
/// GETs the scenario's conformance object as a typed [`ConformanceObject`],
/// builds a `KubeHandle` with `field_manager`, calls `pipeline` to construct
/// the [`ReadyReconcile`], and runs `.execute(finalizer)`.  Records a synthetic
/// `200` on `Ok` (any `Action` is a successful reconcile).  On `Err` it records a
/// trace entry carrying the error class instead of panicking: a reconcile
/// failure is itself a divergence signal — the fake succeeding where the real
/// apiserver 409s is exactly what the harness exists to catch — and panicking
/// would discard both the comparison and the error class.  The error is
/// classified as `"conflict"` (synthetic status 409) when the underlying cause
/// is a `kube::Error::Api` with HTTP 409 — the apiserver rejected an SSA patch
/// on ownership/precondition grounds — or `"other: <Display>"` (synthetic
/// status 500) for any other failure (an effect returning `Err`, a
/// serialisation failure, a missing input).  The synthetic status mirrors the
/// wire status the conflict case would have returned so the existing status
/// comparison rides; the `error` field carries the class for the new
/// equivalence comparison.  No object/status content is recorded: a reconcile
/// may end on a finalizer clear that the fake GCs synchronously, so a
/// post-reconcile GET would race the real cluster's async GC — the scenario
/// observes results through explicit `Get` steps.
async fn run_reconcile_pass(
    client: kube::Client,
    field_manager: &str,
    finalizer: &str,
    object_name: &str,
    pipeline: &RunReconcilePipeline,
) -> TraceEntry {
    let ops = kubo::environment::KubeHandle::new(client.clone(), field_manager);
    let obj: Arc<ConformanceObject> = Arc::new(
        kube::Api::namespaced(client.clone(), "default")
            .get(object_name)
            .await
            .unwrap_or_else(|_| {
                panic!("conformance: RunReconcile: object {object_name} not found — apply it first")
            }),
    );
    let rec = pipeline(&ops, obj);
    match rec.execute(finalizer).await {
        Ok(_) => make_entry(200, None),
        Err(e) => reconcile_error_trace(&e),
    }
}

/// Classify a [`ReconcileError`] into a [`TraceEntry`] for the `RunReconcile`
/// `Err` arm.
///
/// A reconcile failure is a divergence signal, not a panic.  The error is
/// walked to its underlying `kube::Error::Api(Box<Status>)` cause; if that
/// carries HTTP 409 (the apiserver rejected an SSA patch on ownership or
/// `resourceVersion` grounds) the entry is classed `"conflict"` with a
/// synthetic `409` status — the wire status the apiserver would have returned
/// for the same patch sent raw.  Any other failure (an effect returning `Err`,
/// a serialisation failure, a missing pipeline input, a finalizer-patch
/// failure that is not a 409) is classed `"other: <Display>"` with a synthetic
/// `500` status — there is no wire call to mirror, so 500 is the harness's
/// convention for "a reconcile failure that did not reach the wire as a 409".
///
/// The `error` string is what `assert_equivalent` compares; the synthetic
/// `status` lets the existing status comparison ride.  Both `object` and
/// `status_subresource` are `None` — a failed reconcile has no returned object.
fn reconcile_error_trace(e: &kubo::reconcile::ReconcileError) -> TraceEntry {
    if let Some(status) = underlying_api_status(e) {
        if status.code == 409 {
            return TraceEntry {
                status: 409,
                object: None,
                status_subresource: None,
                resource_version: None,
                error: Some("conflict".to_string()),
                name: None,
            };
        }
    }
    TraceEntry {
        status: 500,
        object: None,
        status_subresource: None,
        resource_version: None,
        error: Some(format!("other: {e}")),
        name: None,
    }
}

/// Walk a [`ReconcileError`] to its underlying `kube::Error::Api` `Status`, if
/// any.  `ReconcileError` wraps either a [`ReconcilerError`] (which itself wraps
/// an [`EnvironmentError`], which wraps a `kube::Error`) or a finalizer-level
/// [`EnvironmentError`] (which also wraps a `kube::Error`).  Both paths are
/// walked; the first `Api` variant found wins.
fn underlying_api_status(e: &kubo::reconcile::ReconcileError) -> Option<&kube::core::Status> {
    use kubo::ReconcilerError;
    use kubo::environment::EnvironmentError;
    use kubo::reconcile::ReconcileError;
    let kube_err = match e {
        ReconcileError::Reconciler(ReconcilerError::Env(env)) => env,
        ReconcileError::Finalizer(env) => env,
        _ => return None,
    };
    match kube_err {
        EnvironmentError::Kube(kube::Error::Api(status)) => Some(status.as_ref()),
        _ => None,
    }
}

/// Read a fake API server response body to completion and parse it as JSON.
///
/// Returns `None` for empty bodies or unparseable payloads, so that error
/// `Status` bodies that fail to round-trip degrade gracefully to `None`
/// rather than panicking the scenario.
async fn read_fake_body(body: kubo_test_fakeapi::ResponseBody) -> Option<Value> {
    let bytes = body.collect().await.ok()?.to_bytes();
    if bytes.is_empty() {
        return None;
    }
    serde_json::from_slice(&bytes).ok()
}

// ── k3d helpers ──────────────────────────────────────────────────────────────

/// Refuse to mutate a real apiserver the caller has not explicitly opted into.
///
/// The conformance suite installs a CRD and creates/deletes objects on the
/// cluster it points at.  An accidental run against a production cluster —
/// the developer's default kubeconfig context — is the worst failure mode a
/// conformance harness can have, so the real arm refuses to start unless the
/// caller names the cluster's server endpoint in the
/// `KUBO_CONFORMANCE_CLUSTER` environment variable and that name matches the
/// kubeconfig's current context.  Refusal names the cluster it refused.
#[cfg(feature = "k3d")]
fn verify_cluster_identity(config: &kube::Config) {
    let actual = config.cluster_url.to_string();
    let Some(expected) = std::env::var("KUBO_CONFORMANCE_CLUSTER").ok() else {
        panic!(
            "conformance: refusing to run against a real apiserver without an explicit opt-in.\n  \
             The current kubeconfig context points at:\n    {actual}\n  \
             Set KUBO_CONFORMANCE_CLUSTER to that server URL to opt this cluster in."
        );
    };
    // Compare via parsed URIs so cosmetic differences (trailing slash, case)
    // don't refuse a legitimate opt-in.  Fall back to a string compare if the
    // opt-in value is not a parseable URI.
    let matches = match http::Uri::try_from(&expected) {
        Ok(e) => e == config.cluster_url,
        Err(_) => expected == actual,
    };
    if !matches {
        panic!(
            "conformance: refusing to run — cluster identity mismatch.\n  \
             KUBO_CONFORMANCE_CLUSTER = {expected}\n  \
             kubeconfig server URL   = {actual}\n  \
             The real arm mutates the cluster it points at; the opt-in must name \
             the same cluster."
        );
    }
}

/// Delete this scenario's conformance object and wait for it to be gone.
///
/// Each real-cluster scenario runs on its own object (named after the
/// scenario).  Real SSA merges status and never drops fields a prior manager
/// wrote, so without a reset a later scenario reusing the name would inherit
/// accumulated state and break equivalence against the fresh fake server.  A
/// 404 on the DELETE (object already absent) is fine; we then poll GET until the
/// object is gone.  Returns whether the object converged to gone within ~2s; the
/// caller decides whether non-convergence is fatal (start-of-scenario reset —
/// see [`reset_conformance_object_strict`]) or a hygiene warning
/// (end-of-scenario teardown — see [`destroy_conformance_object`]).
///
/// The finalizer-clear step is what makes per-scenario objects viable: a
/// finalizer scenario leaves its object Terminating with an unmanaged
/// finalizer, and with per-scenario names no later scenario re-reset's that
/// object, so the teardown must clear the finalizer itself or the object lingers
/// and blocks any later `ReinstallCrd` (the CRD's `customresourcecleanup`
/// finalizer waits for every instance to drain).
#[cfg(feature = "k3d")]
async fn reset_conformance_object(client: &kube::Client, path: &str) -> bool {
    // Clear any finalizers first.  A prior finalizer scenario may have left the
    // object with finalizers, in which case a plain DELETE sets
    // `deletionTimestamp` and retains the object — the GET-poll below would
    // never see 404 and the reset would hang to its ~2s cap.  Merge-patching
    // finalizers to [] clears them: on a terminating object this triggers the
    // apiserver's GC controller; on a live object it lets the subsequent DELETE
    // remove immediately.  A 404 here (object already gone) is fine.
    let clear_patch = serde_json::json!({ "metadata": { "finalizers": [] } });
    let clear_body =
        serde_json::to_vec(&clear_patch).expect("k3d: reset finalizer-clear serialization failed");
    let clear_req = http::Request::builder()
        .method("PATCH")
        .uri(path)
        .header("content-type", "application/merge-patch+json")
        .header("accept", "application/json")
        .body(clear_body)
        .expect("k3d: failed to build reset finalizer-clear patch");
    let _ = client.request::<serde_json::Value>(clear_req).await;

    let del = http::Request::builder()
        .method("DELETE")
        .uri(path)
        .header("accept", "application/json")
        .body(Vec::<u8>::new())
        .expect("k3d: failed to build reset DELETE");
    let _ = client.request::<serde_json::Value>(del).await;

    // Poll until the object is gone.  Capped at ~2s so a stuck deletion fails
    // loudly rather than hanging the whole run; the caller decides whether
    // non-convergence is fatal.
    for _ in 0..20 {
        let req = http::Request::builder()
            .method("GET")
            .uri(path)
            .header("accept", "application/json")
            .body(Vec::<u8>::new())
            .expect("k3d: failed to build reset GET");
        match client.request::<serde_json::Value>(req).await {
            Err(kube::Error::Api(ref e)) if e.code == 404 => return true,
            Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            _ => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    false
}

/// Start-of-scenario reset: refuse to run on a polluted object.
///
/// A stuck reset used to carry on regardless and surface later as an
/// equivalence failure with the evidence gone; it now fails hard.
#[cfg(feature = "k3d")]
async fn reset_conformance_object_strict(client: &kube::Client, path: &str) {
    if !reset_conformance_object(client, path).await {
        panic!(
            "conformance: reset did not converge — object at {path} still present after ~2s; \
             refusing to start the scenario on a polluted object"
        );
    }
}

/// End-of-scenario teardown: remove the scenario's object so it does not linger.
///
/// With per-scenario objects, no later scenario re-reset's this object, so a
/// finalizer scenario that leaves its object Terminating with an unmanaged
/// finalizer would pollute the cluster and block any later `ReinstallCrd`.
/// Best-effort with a visible warning rather than a panic: the scenario already
/// passed its assertions, and a slow GC should not turn a green test red — but a
/// leftover object is a real harness problem the developer needs to hear about.
#[cfg(feature = "k3d")]
async fn destroy_conformance_object(client: &kube::Client, path: &str) {
    if !reset_conformance_object(client, path).await {
        eprintln!(
            "conformance: WARNING — teardown did not converge: object at {path} still present \n             after ~2s. It may block later ReinstallCrd steps in the same run. Delete the \
             cluster or clear the object's finalizers manually."
        );
    }
}

/// Install the `ConformanceObject` CRD into the cluster.
///
/// Uses a plain POST to `apiextensions.k8s.io/v1/customresourcedefinitions`.
/// A 409 Conflict response is silently ignored (already installed).  On
/// success, waits 3 seconds for the API server to establish the new resource.
#[cfg(feature = "k3d")]
async fn install_conformance_crd(client: &kube::Client) {
    // The `openAPIV3Schema` is derived from `ConformanceObject`'s schemars output
    // (see [`default_open_api_v3_schema`]) so a new framework status field
    // automatically enters the conformance surface.  The envelope (group,
    // names, scope, per-version `subresources`) is kept hand-written and shared
    // with [`reinstall_conformance_crd`]'s shape — only the schema is
    // derived.  `subresources.status` is per-version in CRD v1; the derive
    // emits it too, but the envelope carries it explicitly so the install and
    // reinstall paths stay shape-compatible.
    let open_api_v3_schema = default_open_api_v3_schema();
    let crd = serde_json::json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {
            "name": "conformanceobjects.conformance.kubo.dev"
        },
        "spec": {
            "group": "conformance.kubo.dev",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": { "openAPIV3Schema": open_api_v3_schema },
                // Expose the `/status` subresource for this version.  Without
                // this, a PATCH to `.../status` returns 404 — the
                // status-subresource scenarios (Phase 2) and the
                // structural-pruning scenarios (Phase 3) all depend on it.
                // `subresources` is per-version in CRD v1, not at
                // `spec.subresources`.
                "subresources": {
                    "status": {}
                }
            }],
            "scope": "Namespaced",
            "names": {
                "plural": "conformanceobjects",
                "singular": "conformanceobject",
                "kind": "ConformanceObject"
            }
        }
    });

    let body = serde_json::to_vec(&crd).expect("CRD serialization failed");
    let req = http::Request::builder()
        .method("POST")
        .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
        .header("content-type", "application/json")
        .body(body)
        .expect("k3d: failed to build CRD install request");

    match client.request::<serde_json::Value>(req).await {
        Ok(_) => {
            // Give the apiserver time to establish the new resource type.
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
        Err(kube::Error::Api(ref e)) if e.code == 409 => {
            // CRD already installed — nothing to do.
        }
        Err(e) => panic!("k3d: failed to install ConformanceObject CRD: {e}"),
    }
}

/// Reinstall the `ConformanceObject` CRD with a new `openAPIV3Schema`.
///
/// DELETEs the existing CRD, polls until it is gone (in Kubernetes 1.22+ the
/// CRD has a `customresourcecleanup` finalizer and stays Terminating until all
/// instances are garbage-collected, so waiting for 404 also waits for
/// every instance of the type to be gone), then POSTs a new CRD whose
/// `spec.versions[0].schema.openAPIV3Schema` is `open_api_v3_schema`, and waits
/// for the `Established` condition before returning.
///
/// CRD v1 does not allow removing fields from a stored version's schema
/// in-place, so a delete-and-recreate is the only way to tighten the status
/// schema — the conformance object is throwaway.  `subresources.status` stays
/// inside the version block (see [`install_conformance_crd`]'s comment).
#[cfg(feature = "k3d")]
async fn reinstall_conformance_crd(client: &kube::Client, open_api_v3_schema: &serde_json::Value) {
    const CRD_NAME: &str = "conformanceobjects.conformance.kubo.dev";
    const CRD_PATH: &str = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/conformanceobjects.conformance.kubo.dev";

    let del = http::Request::builder()
        .method("DELETE")
        .uri(CRD_PATH)
        .header("accept", "application/json")
        .body(Vec::<u8>::new())
        .expect("k3d: failed to build CRD delete");
    let _ = client.request::<serde_json::Value>(del).await;

    // Poll until the CRD is gone.  In 1.22+ the CRD lingers in Terminating
    // while its cleanup finalizer drains the instances; 404 means both the
    // CRD and every CR of the type are gone.  Capped at ~30s — CRD cleanup is
    // slower than a single-object reset because it waits for *every* instance
    // of the type to drain, and on a shared cluster a concurrent scenario may
    // have left a Terminating object with an unmanaged finalizer (the
    // per-scenario teardown in [`destroy_conformance_object`] is what prevents
    // that, but a parallel runner can still race it — see [`Scenario`]'s docs).
    // The cap is a hard panic, not a silent fall-through: a fall-through used
    // to POST into a still-Terminating CRD and surface as a confusing 409
    // `AlreadyExists` instead of a clear "CRD didn't delete in time" failure
    // (same anti-pattern the object reset was tightened to eliminate).
    let mut crd_gone = false;
    for _ in 0..300 {
        let req = http::Request::builder()
            .method("GET")
            .uri(CRD_PATH)
            .header("accept", "application/json")
            .body(Vec::<u8>::new())
            .expect("k3d: failed to build CRD get");
        match client.request::<serde_json::Value>(req).await {
            Err(kube::Error::Api(ref e)) if e.code == 404 => {
                crd_gone = true;
                break;
            }
            _ => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    if !crd_gone {
        panic!(
            "k3d: CRD {CRD_NAME} did not delete within ~30s — it is likely stuck Terminating \
             on a lingering instance with an unmanaged finalizer (a concurrent scenario may \
             have left one; see destroy_conformance_object and Scenario docs)"
        );
    }

    let crd = serde_json::json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": CRD_NAME },
        "spec": {
            "group": "conformance.kubo.dev",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": { "openAPIV3Schema": open_api_v3_schema },
                // `subresources` is per-version in CRD v1 — keep `status`
                // inside the version block or `/status` silently 404s (see
                // [`install_conformance_crd`]).
                "subresources": { "status": {} }
            }],
            "scope": "Namespaced",
            "names": {
                "plural": "conformanceobjects",
                "singular": "conformanceobject",
                "kind": "ConformanceObject"
            }
        }
    });

    let body = serde_json::to_vec(&crd).expect("k3d: CRD serialization failed");
    let req = http::Request::builder()
        .method("POST")
        .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
        .header("content-type", "application/json")
        .body(body)
        .expect("k3d: failed to build CRD reinstall request");
    match client.request::<serde_json::Value>(req).await {
        Ok(_) => {}
        Err(e) => panic!("k3d: failed to reinstall ConformanceObject CRD: {e}"),
    }

    // Wait for the new resource type to be Established before returning, so
    // the next apply does not 404 on a not-yet-established type.  Polling the
    // condition is more reliable than a fixed sleep, especially after a
    // delete/recreate where establishment latency is variable.
    for _ in 0..50 {
        let req = http::Request::builder()
            .method("GET")
            .uri(CRD_PATH)
            .header("accept", "application/json")
            .body(Vec::<u8>::new())
            .expect("k3d: failed to build CRD establishment poll");
        if let Ok(v) = client.request::<serde_json::Value>(req).await {
            if let Some(conds) = v.pointer("/status/conditions").and_then(|c| c.as_array()) {
                if conds.iter().any(|c| {
                    c.get("type").and_then(|t| t.as_str()) == Some("Established")
                        && c.get("status").and_then(|s| s.as_str()) == Some("True")
                }) {
                    return;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    // Establishment did not converge in ~5s; carry on — the next apply will
    // 404 loudly if the type is still not ready, which is a clearer failure
    // than panicking here.
}

/// Execute a single step against the real kube-apiserver.
///
/// Enriches the patch body with `apiVersion`, `kind`, and `metadata.name`
/// before sending — required by the real apiserver for SSA.  The returned
/// object (on 2xx) is fed through [`make_entry`] so the real trace is
/// normalised identically to the fake trace.
#[cfg(feature = "k3d")]
async fn run_k3d_step(
    step: &Step,
    client: &kube::Client,
    path: &str,
    object_name: &str,
    captures: &std::collections::HashMap<String, String>,
) -> TraceEntry {
    match &step.kind {
        StepKind::Apply {
            manager,
            patch,
            force,
        } => {
            let full_patch = enrich_apply_patch(patch, object_name);
            let body = serde_json::to_vec(&full_patch).expect("k3d: patch serialization failed");
            let force_str = if *force { "true" } else { "false" };
            let uri = format!("{}?fieldManager={}&force={}", path, manager, force_str);

            let req = http::Request::builder()
                .method("PATCH")
                .uri(&uri)
                .header("content-type", "application/apply-patch+yaml")
                .header("accept", "application/json")
                .body(body)
                .expect("k3d: failed to build apply request");

            match client.request::<serde_json::Value>(req).await {
                Ok(v) => make_entry(200, Some(v)),
                Err(kube::Error::Api(ref e)) => make_entry(e.code, None),
                Err(e) => panic!("k3d: unexpected error during apply: {e}"),
            }
        }
        StepKind::ApplyStatus {
            manager,
            status,
            force,
        } => {
            // Wire shape mirrors the fake path's `ApplyStatus`: SSA against
            // the `/status` subresource with `fieldManager` and `force`, body
            // carrying apiVersion/kind/metadata/status.  The real apiserver
            // honours fieldManager and force on the status subresource (unlike
            // the fake, which ignores both), so this is the arm that surfaces
            // the status-subresource-ownership divergences.
            let body_value = serde_json::json!({
                "apiVersion": "conformance.kubo.dev/v1",
                "kind": "ConformanceObject",
                "metadata": { "name": object_name, "namespace": "default" },
                "status": status,
            });
            let body =
                serde_json::to_vec(&body_value).expect("k3d: status patch serialization failed");
            let force_str = if *force { "true" } else { "false" };
            let uri = format!(
                "{}/status?fieldManager={}&force={}",
                path, manager, force_str
            );

            let req = http::Request::builder()
                .method("PATCH")
                .uri(&uri)
                .header("content-type", "application/apply-patch+yaml")
                .header("accept", "application/json")
                .body(body)
                .expect("k3d: failed to build status request");

            match client.request::<serde_json::Value>(req).await {
                Ok(v) => make_entry(200, Some(v)),
                Err(kube::Error::Api(ref e)) => make_entry(e.code, None),
                Err(e) => panic!("k3d: unexpected error during status apply: {e}"),
            }
        }
        StepKind::Get => {
            let req = http::Request::builder()
                .method("GET")
                .uri(path)
                .header("accept", "application/json")
                .body(Vec::<u8>::new())
                .expect("k3d: failed to build GET request");

            match client.request::<serde_json::Value>(req).await {
                Ok(v) => make_entry(200, Some(v)),
                Err(kube::Error::Api(ref e)) => make_entry(e.code, None),
                Err(e) => panic!("k3d: unexpected error during get: {e}"),
            }
        }
        StepKind::GetPath { path: target } => {
            // Same shape as `Get`, but against an arbitrary path — the
            // observation primitive for multi-object scenarios.  Used to
            // independently observe a child resource an effect
            // applied/deleted through the framework's `KubeHandle` code
            // path, so the equivalence assertion rides on the observable
            // child lifecycle rather than on "the framework returned `Ok`".
            let req = http::Request::builder()
                .method("GET")
                .uri(target)
                .header("accept", "application/json")
                .body(Vec::<u8>::new())
                .expect("k3d: failed to build GET request");

            match client.request::<serde_json::Value>(req).await {
                Ok(v) => make_entry(200, Some(v)),
                Err(kube::Error::Api(ref e)) => make_entry(e.code, None),
                Err(e) => panic!("k3d: unexpected error during get: {e}"),
            }
        }
        StepKind::List {
            path: target,
            label_selector,
        } => {
            // The collection analogue of `GetPath`.  The real apiserver
            // serves the same `List` envelope shape as the fake;
            // `make_list_entry` normalises each item and sorts by
            // `metadata.name` so the equivalence assertion rides on the
            // normalised items content the framework's `list()` depends on.
            // The `labelSelector` query is what isolates a scenario's own
            // object under parallel real-cluster runs (every parallel
            // scenario's object would otherwise appear).
            let uri = match label_selector {
                Some(sel) => format!("{target}?labelSelector={sel}"),
                None => target.clone(),
            };
            let req = http::Request::builder()
                .method("GET")
                .uri(&uri)
                .header("accept", "application/json")
                .body(Vec::<u8>::new())
                .expect("k3d: failed to build LIST request");

            match client.request::<serde_json::Value>(req).await {
                Ok(v) => make_list_entry(200, Some(v)),
                Err(kube::Error::Api(ref e)) => make_list_entry(e.code, None),
                Err(e) => panic!("k3d: unexpected error during list: {e}"),
            }
        }
        StepKind::ApplyStatusPrecondition {
            manager,
            status,
            force,
            rv_ref,
        } => {
            // Same wire shape as `ApplyStatus`, but the body's
            // `metadata.resourceVersion` is set from a prior capture.  The
            // real apiserver enforces the precondition: 409 if an
            // intervening write made the rv stale, 200 if it is still
            // current.  This is the arm that surfaces the
            // stale-resourceVersion divergence (fake ignores the
            // precondition and returns 200).
            let rv = captures.get(rv_ref).unwrap_or_else(|| {
                panic!("k3d: ApplyStatusPrecondition references uncaptured rv_ref {rv_ref}")
            });
            let body_value = serde_json::json!({
                "apiVersion": "conformance.kubo.dev/v1",
                "kind": "ConformanceObject",
                "metadata": {
                    "name": object_name,
                    "namespace": "default",
                    "resourceVersion": rv,
                },
                "status": status,
            });
            let body = serde_json::to_vec(&body_value)
                .expect("k3d: precondition status patch serialization failed");
            let force_str = if *force { "true" } else { "false" };
            let uri = format!(
                "{}/status?fieldManager={}&force={}",
                path, manager, force_str
            );

            let req = http::Request::builder()
                .method("PATCH")
                .uri(&uri)
                .header("content-type", "application/apply-patch+yaml")
                .header("accept", "application/json")
                .body(body)
                .expect("k3d: failed to build precondition status request");

            match client.request::<serde_json::Value>(req).await {
                Ok(v) => make_entry(200, Some(v)),
                Err(kube::Error::Api(ref e)) => make_entry(e.code, None),
                Err(e) => panic!("k3d: unexpected error during precondition status apply: {e}"),
            }
        }
        StepKind::PatchFinalizers { finalizers } => {
            // Wire shape mirrors `KubeHandle::patch_finalizers`:
            // `application/merge-patch+json`, no `fieldManager`, body is a bare
            // `{"metadata":{"finalizers":[...]}}`.  Merge-patch does not
            // require apiVersion/kind — only SSA does — so the body is sent
            // unenriched.  On a terminating object, clearing the last finalizer
            // triggers the apiserver's GC controller; the patch itself returns
            // 200 with the updated object (finalizers empty), and the next GET
            // returns 404.
            let body_value = serde_json::json!({ "metadata": { "finalizers": finalizers } });
            let body =
                serde_json::to_vec(&body_value).expect("k3d: finalizer patch serialization failed");
            let req = http::Request::builder()
                .method("PATCH")
                .uri(path)
                .header("content-type", "application/merge-patch+json")
                .header("accept", "application/json")
                .body(body)
                .expect("k3d: failed to build finalizer patch request");

            match client.request::<serde_json::Value>(req).await {
                Ok(v) => make_entry(200, Some(v)),
                Err(kube::Error::Api(ref e)) => make_entry(e.code, None),
                Err(e) => panic!("k3d: unexpected error during finalizer patch: {e}"),
            }
        }
        StepKind::Delete => {
            // Wire shape mirrors `KubeHandle::delete`: plain DELETE, no body, no
            // preconditions.  If the object has finalizers, the apiserver sets
            // `deletionTimestamp` and retains it; otherwise it removes the object
            // immediately.  The response body is a Kubernetes `Status` object
            // (not the resource) and the framework discards it
            // (`KubeHandle::delete` returns `Ok(())`), so only the status code is
            // recorded.  Retention vs removal is pinned by the subsequent GET.
            let req = http::Request::builder()
                .method("DELETE")
                .uri(path)
                .header("accept", "application/json")
                .body(Vec::<u8>::new())
                .expect("k3d: failed to build DELETE request");

            match client.request::<serde_json::Value>(req).await {
                Ok(_v) => make_entry(200, None),
                Err(kube::Error::Api(ref e)) => make_entry(e.code, None),
                Err(e) => panic!("k3d: unexpected error during delete: {e}"),
            }
        }
        StepKind::ReinstallCrd { open_api_v3_schema } => {
            // Swap the CRD's schema mid-scenario.  Deleting the CRD also
            // garbage-collects every instance of the type (this scenario's own
            // object included), so the scenario must re-apply the spec after
            // this step to recreate the object before the next status apply.
            // Because the CRD is shared across all scenarios, a `ReinstallCrd`
            // step is not safe to run concurrently with other scenarios against
            // the same cluster — see the caveat in [`Scenario`]'s docs.
            // Recorded as a synthetic 200/None entry to stay shape-compatible
            // with the fake path's no-op; the structural-pruning scenario
            // does not compare this entry for content.
            reinstall_conformance_crd(client, open_api_v3_schema).await;
            make_entry(200, None)
        }
        StepKind::RunReconcile {
            field_manager,
            finalizer,
            pipeline,
        } => {
            // Real-cluster arm of the framework-level harness: drive a real
            // `Reconcile::execute` against the live apiserver.  The shared
            // `run_reconcile_pass` body is identical to the fake path's —
            // only the `kube::Client` differs (live cluster vs
            // fake-backed) — so the framework's own SSA parameters,
            // status-patch cadence, and finalizer management run against a
            // real apiserver here.
            run_reconcile_pass(
                client.clone(),
                field_manager,
                finalizer,
                object_name,
                pipeline,
            )
            .await
        }
    }
}

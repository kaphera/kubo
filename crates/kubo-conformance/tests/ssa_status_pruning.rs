//! Structural-schema pruning: the ledger killer.
//!
//! The fake stores every status field verbatim (`Store::status_patch` does a
//! full `status` replace); it has no OpenAPI schema to prune with.  The real
//! apiserver prunes any status field the CRD's `openAPIV3Schema` does not
//! declare — silently, returning `200`.  This is the bug class that would drop
//! `pendingCleanup` entries from the ledger if a CRD forgot to declare the
//! field: the controller writes it, the apiserver accepts it, the next GET
//! shows it gone, and the controller never learns why.
//!
//! This scenario is real-cluster-only and deliberately asymmetric: the fake
//! and real assertions differ, and [`assert_equivalent`] is *not* called.  It
//! asserts the *boundary* of equivalence — which status fields a CRD must
//! declare for the framework to work — and iterates the framework-managed
//! status fields derived from `ConformanceObject`'s schemars output, so a new
//! framework status field automatically enters this surface without a manual
//! update.
//!
//! Procedure:
//! 1. Reinstall the CRD with a status schema that *declares* every derived
//!    framework status field (`conditions`, `observedGeneration`,
//!    `pendingCleanup`, …).
//! 2. `apply` the spec, `apply_status` writes every derived field, `get` —
//!    present on both fake (verbatim) and real (declared, so preserved).
//! 3. Reinstall the CRD with a status schema that *omits* every derived field.
//! 4. Recreate the object (deleting the CRD garbage-collected it), same
//!    `apply_status`, `get` — fake still present (no schema), real absent
//!    (pruned).  The divergence is the signal.
//! 5. Reinstall the CRD with the derived default so subsequent scenarios in
//!    the same cluster are not poisoned by the tightened schema.

use kubo_conformance::{
    DivId, Scenario, assert_known_divergence, default_open_api_v3_schema, derived_status_fields,
};
use serde_json::{Map, Value, json};

/// The CRD's `openAPIV3Schema` with a structured `status` stanza that declares
/// every framework-managed status field derived from `ConformanceObject`'s
/// schemars output.  The top-level object keeps
/// `x-kubernetes-preserve-unknown-fields: true` so undeclared *spec* fields
/// still pass; the `status` node has its own structured schema (each field
/// declared with a minimal `{ "type": <type> }`, no `preserve-unknown`), so the
/// apiserver prunes any status field the schema does not declare.
///
/// This is hand-written by construction: the pruning mechanism requires a
/// schema the derivation would not produce — a status node with no
/// `x-kubernetes-preserve-unknown-fields` so pruning fires.  The field *set*
/// is derived, so a new framework status field auto-enters.
fn schema_declaring_all_derived_fields() -> Value {
    let mut status_props = Map::new();
    for f in derived_status_fields() {
        // Minimal declaration per field: just the top-level `type`, plus the
        // sub-schema fragments Kubernetes structural-schema validation makes
        // mandatory — `items` for arrays (an empty `items` is rejected; a
        // preserve-unknown items accepts any value, which is all the pruning
        // test needs) and preserve-unknown for objects.  This is enough to
        // keep the apiserver from pruning the field while accepting the
        // representative value; it is deliberately NOT the full schemars
        // sub-schema (that would force valid Condition values and couple the
        // test to the framework's condition shape).
        let mut field = Map::new();
        field.insert("type".to_string(), Value::String(f.type_keyword.clone()));
        match f.type_keyword.as_str() {
            "array" => {
                field.insert(
                    "items".to_string(),
                    json!({ "x-kubernetes-preserve-unknown-fields": true }),
                );
            }
            "object" => {
                field.insert(
                    "x-kubernetes-preserve-unknown-fields".to_string(),
                    Value::Bool(true),
                );
            }
            _ => {}
        }
        status_props.insert(f.name, Value::Object(field));
    }
    json!({
        "type": "object",
        "x-kubernetes-preserve-unknown-fields": true,
        "properties": {
            "status": {
                "type": "object",
                "properties": status_props
            }
        }
    })
}

/// The CRD's `openAPIV3Schema` with a `status` stanza that declares no
/// properties.  The `status` node is still a structured schema (no
/// `preserve-unknown`), so every framework status field is pruned on write.
fn schema_omitting_all_derived_fields() -> Value {
    json!({
        "type": "object",
        "x-kubernetes-preserve-unknown-fields": true,
        "properties": {
            "status": { "type": "object", "properties": {} }
        }
    })
}

/// A status body that writes every framework-managed status field with a
/// representative value valid under the minimal `{ "type": <type> }`
/// declaration [`schema_declaring_all_derived_fields`] emits.
fn status_writing_all_derived_fields() -> Value {
    let mut status = Map::new();
    for f in derived_status_fields() {
        status.insert(f.name, f.representative_value);
    }
    Value::Object(status)
}

/// A status field the CRD declares is preserved; the same field the CRD omits
/// is pruned on real and preserved on fake.
///
/// Two passes share one scenario, separated by a `reinstall_crd` step that
/// swaps the CRD's status schema mid-run.  Deleting the CRD garbage-collects
/// every instance of the type, so each pass re-applies the spec to recreate the
/// scenario's object before the status write.  The fake path no-ops the reinstall
/// steps (it has no schema); the real path does the delete/wait/recreate.  The
/// assertions iterate the derived framework status fields, so a new field added
/// added to `ResourceStatus` or `ConformanceFields` is exercised here automatically.
#[test]
fn ssa_status_pruning_is_the_ledger_killer() {
    let status_write = status_writing_all_derived_fields();
    let scenario = Scenario::new("ssa-status-pruning")
        // Pass 1: schema declares every derived framework status field.
        .reinstall_crd(schema_declaring_all_derived_fields())
        .apply("kubo-reconcile", json!({ "spec": { "marker": "x" } }))
        .apply_status("kubo-reconcile", status_write.clone())
        .get()
        .named("get-pass-1")
        // Pass 2: schema omits every derived framework status field.
        .reinstall_crd(schema_omitting_all_derived_fields())
        .apply("kubo-reconcile", json!({ "spec": { "marker": "x" } }))
        .apply_status("kubo-reconcile", status_write.clone())
        .get()
        .named("get-pass-2")
        // Restore the derived default so later scenarios in the same cluster
        // are not poisoned by the tightened status schema.
        .reinstall_crd(default_open_api_v3_schema());

    let _ = assert_known_divergence(DivId::SsaStatusPruning);

    let fake_trace = scenario.run_against_fake();

    // The fake has no schema, so every derived field is present in both passes
    // — confirming the framework *wrote* the field in both cases (the divergence
    // is what the real server does with it, not whether the controller sent it).
    let fake_pass1 = &fake_trace.entry("get-pass-1").status_subresource;
    let fake_pass2 = &fake_trace.entry("get-pass-2").status_subresource;
    for f in derived_status_fields() {
        assert_eq!(
            fake_pass1.as_ref().and_then(|s| s.get(&f.name)),
            Some(&f.representative_value),
            "fake must persist {} verbatim in pass 1 (no schema to prune with)",
            f.name
        );
        assert_eq!(
            fake_pass2.as_ref().and_then(|s| s.get(&f.name)),
            Some(&f.representative_value),
            "fake must still persist {} in pass 2 — it has no schema, so reinstalling changes nothing.  This is the silent-fallback the real apiserver does NOT do.",
            f.name
        );
    }

    #[cfg(feature = "k3d")]
    {
        let real_trace = scenario.run_against_kube_apiserver();

        let real_pass1 = &real_trace.entry("get-pass-1").status_subresource;
        let real_pass2 = &real_trace.entry("get-pass-2").status_subresource;
        for f in derived_status_fields() {
            // Pass 1: the schema declares the field, so the real apiserver
            // preserves it.
            assert_eq!(
                real_pass1.as_ref().and_then(|s| s.get(&f.name)),
                Some(&f.representative_value),
                "real must preserve {} when the CRD schema declares it",
                f.name
            );
            // Pass 2: the schema omits the field, so the real apiserver prunes
            // it — silently, with a 200 on the write.  This is the ledger
            // killer: the controller wrote the field, the apiserver accepted
            // it, and the next GET shows it gone.  The fake preserves it (no
            // schema), so the two diverge on content with the same status code
            // — exactly the content-aware trace's reason for existing.
            assert!(
                real_pass2.as_ref().and_then(|s| s.get(&f.name)).is_none(),
                "real must prune {} when the CRD schema omits it — the silent-fallback bug class.  If this fails, the status stanza likely still has `x-kubernetes-preserve-unknown-fields: true` (it must be off the status node for pruning to fire).",
                f.name
            );
        }
    }
}

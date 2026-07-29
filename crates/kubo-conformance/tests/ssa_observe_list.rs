//! Cover the observe path: List.
//!
//! The write path (apply / status / finalizers / delete) is verified end to end
//! against both backends; the observe path was not. The `List` capability was
//! originally planned but never landed, and the runtime's reflector seeds
//! from a List before switching to watch — a fake whose `Store::list` returned
//! a different normalised content than the real apiserver would let a
//! framework test pass against the fake and fail silently in production, and
//! no conformance test would notice.
//!
//! This scenario pins the wire shape of the framework's `KubeHandle::list` /
//! `list_all`: a plain `GET` against the collection path returning a `List`
//! envelope, whose `items` (normalised, sorted by `metadata.name`) match fake
//! and real.  It exercises the three states a controller's reflector cares
//! about: the object present after a create, the object reflecting a spec
//! mutation, and the object absent after a delete.
//!
//! ## Real-arm isolation
//!
//! The conformance suite runs test functions in parallel against a shared
//! throwaway cluster, so a bare `List` of the conformance collection would
//! return every parallel scenario's object on the real arm but only the one
//! object the fake holds — equivalence would break on the items count alone.
//! The scenario therefore labels its own object with a unique label
//! (`conformance.kubo.dev/scenario: <scenario-name>`) and lists with that
//! `labelSelector`, so both backends return exactly the scenario's own object.
//! This is the standard Kubernetes isolation mechanism, not a harness
//! convenience: a real operator lists its own resources the same way.  The
//! fake models only the `key=value` equality form of the selector grammar
//! (see `matches_label_selector` in `kubo-test-fakeapi`); the harness never
//! uses the richer forms.
//!
//! ## Watch is deferred
//!
//! The observe path also covers watch-event assertions in principle. The
//! framework has no watch primitive on `KubeHandle` — watch is a
//! `kube::runtime::Controller` concern, set up at the `Runtime` / `Listener`
//! layer, not a `KubeHandle` surface the conformance harness pins.  The
//! harness is sequential (one step at a time, no concurrent driver), so it
//! cannot reproduce a real watch session's interleaving of events with
//! intervening mutations — the same structural limitation that left the
//! two-rapid-sequential-status-patches test `#[ignore]`d.  The fake's
//! broadcast behaviour (`ADDED` / `MODIFIED` / `DELETED` on every mutation)
//! is exercised by `kubo`'s own `World` tests (`crates/kubo/src/testing/world.rs`
//! drives a real `Controller` watcher against the fake); the conformance
//! layer's job is the differential, and a sequential harness cannot
//! differentially reproduce a streaming watch against a real apiserver.
//! Watch is recorded as a known uncovered surface rather than half-asserted
//! here.

use kubo_conformance::Scenario;
use serde_json::{Value, json};

/// The scenario name doubles as the parent object name (slugified) and as the
/// unique label value that isolates the object under parallel real-cluster
/// runs.
const SCENARIO_NAME: &str = "ssa-observe-list";

/// The label key applied to the scenario's object and used in the
/// `labelSelector`.  A `kubo.dev` domain keeps it in the operator's namespace.
const LABEL_KEY: &str = "conformance.kubo.dev/scenario";

/// The collection path for namespaced `ConformanceObject` instances in
/// `default`.  The framework's `KubeHandle::list` issues a `GET` against this
/// path (cluster-scoped `list_all` would hit the cluster-scoped variant).
const COLLECTION_PATH: &str = "/apis/conformance.kubo.dev/v1/namespaces/default/conformanceobjects";

/// The fake's `Store::list` returns the same normalised `items` content as a
/// real apiserver's LIST across the create / mutate / delete lifecycle.
///
/// Procedure:
/// 1. Apply the conformance object with a unique label and a `marker` spec
///    field.
/// 2. `list_labeled` the collection with the scenario's label — observe the
///    object is present with the applied spec.
/// 3. Re-apply with a different `marker` value (a spec mutation).
/// 4. `list_labeled` again — observe the items reflect the mutated spec.
/// 5. `delete` the object (no finalizers, so the apiserver removes it
///    immediately).
/// 6. `list_labeled` again — observe the items are empty.
///
/// The whole trace is equivalent on both backends.  The List entries carry the
/// normalised `items` array (sorted by `metadata.name`) in `object`; the
/// `labelSelector` isolates the items to the scenario's own object on both
/// backends, so the equivalence assertion rides on the normalised content the
/// framework's `list()` depends on, not on the full collection.
#[test]
fn observe_list_matches_fake_and_real_across_lifecycle() {
    let label_selector = format!("{LABEL_KEY}={SCENARIO_NAME}");

    let scenario = Scenario::new(SCENARIO_NAME)
        // 0. Create the object with a unique label and a marker spec field.
        .apply(
            "kubo-conformance",
            json!({
                "metadata": { "labels": { LABEL_KEY: SCENARIO_NAME } },
                "spec": { "marker": "first" },
            }),
        )
        // 1. List the collection filtered to this scenario's object.
        .list_labeled(COLLECTION_PATH, &label_selector)
        .named("list-after-create")
        // 2. Mutate the spec.
        .apply(
            "kubo-conformance",
            json!({
                "metadata": { "labels": { LABEL_KEY: SCENARIO_NAME } },
                "spec": { "marker": "second" },
            }),
        )
        // 3. List again — items reflect the mutation.
        .list_labeled(COLLECTION_PATH, &label_selector)
        .named("list-after-mutate")
        // 4. Delete (no finalizers → immediate removal).
        .delete()
        // 5. List again — items empty.
        .list_labeled(COLLECTION_PATH, &label_selector)
        .named("list-after-delete");

    let fake_trace = scenario.run_against_fake();

    // After the create, the list returns exactly the one object with the
    // applied spec.
    let fake_list_after_create = fake_trace.entry("list-after-create");
    assert_eq!(
        fake_list_after_create.status, 200,
        "fake: list after create must return 200"
    );
    let fake_items_after_create = fake_list_after_create
        .object
        .as_ref()
        .and_then(Value::as_array)
        .expect("fake: list entry object must be the items array");
    assert_eq!(
        fake_items_after_create.len(),
        1,
        "fake: list after create must contain exactly the scenario's object"
    );
    assert_eq!(
        fake_items_after_create[0]
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(Value::as_str),
        Some(SCENARIO_NAME),
        "fake: the listed object must be the scenario's own"
    );
    assert_eq!(
        fake_items_after_create[0]
            .get("spec")
            .and_then(|s| s.get("marker")),
        Some(&json!("first")),
        "fake: the listed object must carry the first marker"
    );

    // After the mutation, the list reflects the new spec value.
    let fake_list_after_mutate = fake_trace.entry("list-after-mutate");
    let fake_items_after_mutate = fake_list_after_mutate
        .object
        .as_ref()
        .and_then(Value::as_array)
        .expect("fake: list entry object must be the items array");
    assert_eq!(
        fake_items_after_mutate.len(),
        1,
        "fake: list after mutate must still contain exactly the scenario's object"
    );
    assert_eq!(
        fake_items_after_mutate[0]
            .get("spec")
            .and_then(|s| s.get("marker")),
        Some(&json!("second")),
        "fake: the listed object must reflect the mutated marker"
    );

    // After the delete, the list is empty.
    let fake_list_after_delete = fake_trace.entry("list-after-delete");
    let fake_items_after_delete = fake_list_after_delete
        .object
        .as_ref()
        .and_then(Value::as_array)
        .expect("fake: list entry object must be the items array (possibly empty)");
    assert!(
        fake_items_after_delete.is_empty(),
        "fake: list after delete must contain no items"
    );

    #[cfg(feature = "k3d")]
    {
        let real_trace = scenario.run_against_kube_apiserver();

        // The whole trace is equivalent: the two applies (200, normalised
        // object), the three lists (200, normalised items array — isolated to
        // the scenario's object by the label selector on both backends), and
        // the delete (200, no body).  The label selector is what makes the
        // list differential hold under parallel real-cluster runs: without it
        // the real arm would return every parallel scenario's object.
        kubo_conformance::assert_equivalent(&fake_trace, &real_trace);

        // Belt-and-braces: the equivalence check already compares the list
        // entries, but assert the lifecycle expectations explicitly so a
        // failure names the observe path rather than dumping the full trace.
        let real_items_after_delete = real_trace
            .entry("list-after-delete")
            .object
            .as_ref()
            .and_then(Value::as_array)
            .expect("real: list entry object must be the items array");
        assert!(
            real_items_after_delete.is_empty(),
            "real: list after delete must contain no items (label selector must isolate)"
        );
    }
}

//! Step 2 gate: typed objects round-trip through the fake API server.
//!
//! Every assertion goes from `kube::Api<DummyResource>` → hyper request →
//! `tower::Service` → in-memory store → hyper response → deserialized struct.
//! Nothing between the test and the bytes on the wire is mocked.

use kube::{
    Api, CustomResourceExt,
    api::{Patch, PatchParams},
};
use kubo_test::fixtures::DummyResource;
use kubo_test::{World, fixtures::dummy};

#[tokio::test]
async fn get_opt_returns_none_for_missing_object() {
    let world = World::builder()
        .install_crd(DummyResource::crd())
        .build()
        .await;

    let api: Api<DummyResource> = Api::namespaced(world.client().clone(), "default");

    let result = api.get_opt("missing").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn round_trips_a_typed_object() {
    let world = World::builder()
        .install_crd(DummyResource::crd())
        .build()
        .await;

    let api: Api<DummyResource> = Api::namespaced(world.client().clone(), "default");

    // Object does not exist yet.
    assert!(api.get_opt("first").await.unwrap().is_none());

    // Apply creates it.
    let created = api
        .patch(
            "first",
            &PatchParams::apply("kubo-test"),
            &Patch::Apply(&dummy("first", "default", "value-a")),
        )
        .await
        .unwrap();

    assert_eq!(created.spec.field, "value-a");
    assert_eq!(created.metadata.name.as_deref(), Some("first"));

    // GET reflects the stored object.
    let fetched = api.get("first").await.unwrap();
    assert_eq!(fetched.spec.field, "value-a");
    // resourceVersion was injected by the store.
    assert!(fetched.metadata.resource_version.is_some());
}

#[tokio::test]
async fn apply_overwrites_existing_object() {
    let world = World::builder().build().await;
    let api: Api<DummyResource> = Api::namespaced(world.client().clone(), "default");

    api.patch(
        "obj",
        &PatchParams::apply("kubo-test"),
        &Patch::Apply(&dummy("obj", "default", "first")),
    )
    .await
    .unwrap();

    let updated = api
        .patch(
            "obj",
            &PatchParams::apply("kubo-test"),
            &Patch::Apply(&dummy("obj", "default", "second")),
        )
        .await
        .unwrap();

    assert_eq!(updated.spec.field, "second");
    assert_eq!(api.get("obj").await.unwrap().spec.field, "second");
}

#[tokio::test]
async fn delete_removes_the_object() {
    let world = World::builder().build().await;
    let api: Api<DummyResource> = Api::namespaced(world.client().clone(), "default");

    api.patch(
        "to-delete",
        &PatchParams::apply("kubo-test"),
        &Patch::Apply(&dummy("to-delete", "default", "v")),
    )
    .await
    .unwrap();

    assert!(api.get_opt("to-delete").await.unwrap().is_some());

    api.delete("to-delete", &Default::default()).await.unwrap();

    assert!(api.get_opt("to-delete").await.unwrap().is_none());
}

#[tokio::test]
async fn resource_version_increments_on_each_write() {
    let world = World::builder().build().await;
    let api: Api<DummyResource> = Api::namespaced(world.client().clone(), "default");

    let v1 = api
        .patch(
            "rv-test",
            &PatchParams::apply("kubo-test"),
            &Patch::Apply(&dummy("rv-test", "default", "a")),
        )
        .await
        .unwrap();

    let v2 = api
        .patch(
            "rv-test",
            &PatchParams::apply("kubo-test"),
            &Patch::Apply(&dummy("rv-test", "default", "b")),
        )
        .await
        .unwrap();

    let rv1: u64 = v1
        .metadata
        .resource_version
        .as_deref()
        .unwrap()
        .parse()
        .unwrap();
    let rv2: u64 = v2
        .metadata
        .resource_version
        .as_deref()
        .unwrap()
        .parse()
        .unwrap();

    assert!(rv2 > rv1);
}

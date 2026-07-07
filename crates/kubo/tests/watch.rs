//! `kube::runtime::watcher` observes object lifecycle through the
//! fake API server's streaming watch support.
//!
//! This proves that:
//! - LIST paths return a Kubernetes List with the correct `resourceVersion`.
//! - WATCH paths upgrade to a streaming NDJSON response.
//! - Store mutations (PATCH → ADDED, DELETE → DELETED) are broadcast via the
//!   per-GVK channel and forwarded to the watcher.
//! - `kube::runtime::watcher` correctly emits `Init`, `InitDone`, `Apply`, and
//!   `Delete` events against the fake.

use futures::StreamExt;
use kube::{Api, runtime::watcher};
use kubo::testing::{
    World,
    fixtures::{DummyResource, dummy},
};
use tokio::sync::mpsc;

/// Verify that a [`kube::runtime::watcher`] observes an object being created
/// and deleted through the fake API server.
///
/// ## Sequencing rationale
///
/// The watcher drives LIST → WATCH internally.  With a single async task we
/// cannot both poll the stream (to open the WATCH connection) and call
/// `world.create()` at the same time — `stream.next().await` would block
/// until the first event, which can't arrive until after `create()` runs.
///
/// We solve this by running the watcher in a **spawned task** that forwards
/// each event over an mpsc channel.  This lets the test task and the stream
/// task progress cooperatively:
///
/// 1. Spawned stream task drives `Init` → `InitDone`, then immediately queues
///    the WATCH HTTP request to Tower's Buffer worker.
/// 2. Test task receives `Init` + `InitDone` and calls `world.create()`, which
///    queues a PATCH request to the same Buffer worker.
/// 3. Tower's Buffer processes WATCH first (it arrived first), so `subscribe()`
///    and the forwarder task are in place before the PATCH is processed and
///    `ADDED` is broadcast.
/// 4. The forwarder delivers the event to the stream task, which emits `Apply`.
#[tokio::test]
async fn watcher_observes_lifecycle() {
    let world = World::builder().build().await;
    let api: Api<DummyResource> = Api::all(world.client().clone());

    // Forward watcher events over an mpsc channel so the test task can
    // receive them without having to poll the stream directly.
    let (ev_tx, mut ev_rx) =
        mpsc::channel::<Result<watcher::Event<DummyResource>, watcher::Error>>(16);

    tokio::spawn(async move {
        let mut stream = watcher(api, watcher::Config::default()).boxed();
        while let Some(ev) = stream.next().await {
            if ev_tx.send(ev).await.is_err() {
                break;
            }
        }
    });

    // ── Initial list phase ────────────────────────────────────────────────────
    // Empty store → Init, then InitDone (no InitApply events).

    let ev0 = ev_rx.recv().await.unwrap().unwrap();
    assert!(
        matches!(ev0, watcher::Event::Init),
        "expected Init, got {ev0:?}"
    );

    let ev1 = ev_rx.recv().await.unwrap().unwrap();
    assert!(
        matches!(ev1, watcher::Event::InitDone),
        "expected InitDone, got {ev1:?}"
    );

    // ── Create and observe Apply ──────────────────────────────────────────────
    // After T receives InitDone, the stream task has already queued the WATCH
    // request to Tower's Buffer worker (stream task doesn't yield between
    // sending InitDone and calling the next stream.next()).  When T calls
    // world.create(), it queues its PATCH after the WATCH.  The Buffer worker
    // processes WATCH first, so subscribe() happens before the broadcast.

    world.create(dummy("obj-1", "ns", "hello")).await;

    let ev2 = ev_rx.recv().await.unwrap().unwrap();
    assert!(
        matches!(ev2, watcher::Event::Apply(_)),
        "expected Apply after create, got {ev2:?}"
    );

    // ── Delete and observe Delete ─────────────────────────────────────────────
    // DummyResource has no finalizers, so DELETE removes it immediately and
    // broadcasts DELETED.

    world.delete::<DummyResource>("ns", "obj-1").await;

    let ev3 = ev_rx.recv().await.unwrap().unwrap();
    assert!(
        matches!(ev3, watcher::Event::Delete(_)),
        "expected Delete after delete, got {ev3:?}"
    );
}

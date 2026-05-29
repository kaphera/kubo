//! Conformance harness for the fake Kubernetes API server.
//!
//! A [`Scenario`] describes a sequence of API operations and their expected
//! HTTP status outcomes.  Call [`Scenario::run_against_fake`] to execute
//! operations against the in-process [`FakeApiServer`] and record the status
//! code for each step.
//!
//! When the `k3d` feature is enabled, `run_against_kube_apiserver` can run the
//! same scenario against a real cluster so that [`assert_equivalent`] can
//! verify protocol equivalence.

use bytes::Bytes;
use http::{Method, Request};
use http_body_util::Full;
use kubo_test_fakeapi::FakeApiServer;
use serde_json::Value;
use tower::Service;

// ── Trace ─────────────────────────────────────────────────────────────────────

/// The HTTP status code recorded for a single scenario step.
#[derive(Debug, Clone, PartialEq)]
pub struct TraceEntry {
    /// HTTP status code returned by the API server for this step.
    pub status: u16,
}

/// The recorded outcome of running a [`Scenario`].
#[derive(Debug, Clone, PartialEq)]
pub struct Trace {
    /// One entry per step, in execution order.
    pub entries: Vec<TraceEntry>,
}

/// Assert that two [`Trace`]s are equivalent.
///
/// Compares status codes entry-by-entry.  Panics on any mismatch so the
/// failing step is clearly visible in the test output.
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
    }
}

// ── Step internals ────────────────────────────────────────────────────────────

struct Step {
    kind: StepKind,
    expected_status: Option<u16>,
}

enum StepKind {
    Apply {
        manager: String,
        patch: Value,
        force: bool,
    },
}

// ── Scenario ──────────────────────────────────────────────────────────────────

/// A replayable sequence of API operations with optional per-step assertions.
///
/// Build with the fluent API, then call [`run_against_fake`](Scenario::run_against_fake)
/// to execute against the in-process fake server.
///
/// Every scenario operates on a single fixed conformance object:
/// `/apis/conformance.kubo.dev/v1/namespaces/default/conformanceobjects/test-obj`
pub struct Scenario {
    name: String,
    steps: Vec<Step>,
}

impl Scenario {
    /// Create a new empty scenario with the given name.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            steps: Vec::new(),
        }
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

    /// Execute the scenario against the in-process fake API server.
    ///
    /// Steps run in order.  Steps with an `expected_status` assertion panic if
    /// the actual status code does not match.  Returns the full [`Trace`] so
    /// the caller can inspect individual entries or pass it to
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
    /// an error other than 409 Conflict.
    #[cfg(feature = "k3d")]
    pub fn run_against_kube_apiserver(&self) -> Trace {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("conformance: failed to build tokio runtime");
        rt.block_on(self.execute_against_cluster())
    }

    #[cfg(feature = "k3d")]
    async fn execute_against_cluster(&self) -> Trace {
        let client = kube::Client::try_default()
            .await
            .expect("k3d: cannot build kube client from default config");

        install_conformance_crd(&client).await;

        const PATH: &str =
            "/apis/conformance.kubo.dev/v1/namespaces/default/conformanceobjects/test-obj";

        let mut entries = Vec::new();
        for (i, step) in self.steps.iter().enumerate() {
            let status = run_k3d_step(step, &client, PATH).await;
            entries.push(TraceEntry { status });
            if let Some(expected) = step.expected_status {
                assert_eq!(
                    status, expected,
                    "scenario {:?}: step {i} expected HTTP {expected} but got {status}",
                    self.name
                );
            }
        }
        Trace { entries }
    }

    async fn execute(&self, server: FakeApiServer) -> Trace {
        // Fixed object path shared by all conformance scenarios.
        const PATH: &str =
            "/apis/conformance.kubo.dev/v1/namespaces/default/conformanceobjects/test-obj";

        let mut entries = Vec::new();
        for (i, step) in self.steps.iter().enumerate() {
            let status = run_step(step, PATH, &server).await;
            entries.push(TraceEntry { status });
            if let Some(expected) = step.expected_status {
                assert_eq!(
                    status, expected,
                    "scenario {:?}: step {i} expected HTTP {expected} but got {status}",
                    self.name
                );
            }
        }
        Trace { entries }
    }
}

// ── Step execution ────────────────────────────────────────────────────────────

async fn run_step(step: &Step, path: &str, server: &FakeApiServer) -> u16 {
    match &step.kind {
        StepKind::Apply {
            manager,
            patch,
            force,
        } => {
            let body_bytes =
                serde_json::to_vec(patch).expect("conformance: patch serialization failed");
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

            resp.status().as_u16()
        }
    }
}

// ── k3d helpers ──────────────────────────────────────────────────────────────

/// Install the `ConformanceObject` CRD into the cluster.
///
/// Uses a plain POST to `apiextensions.k8s.io/v1/customresourcedefinitions`.
/// A 409 Conflict response is silently ignored (already installed).  On
/// success, waits 3 seconds for the API server to establish the new resource.
#[cfg(feature = "k3d")]
async fn install_conformance_crd(client: &kube::Client) {
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
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "x-kubernetes-preserve-unknown-fields": true
                    }
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

/// Execute a single step against the real kube-apiserver.
///
/// Enriches the patch body with `apiVersion`, `kind`, and `metadata.name`
/// before sending — required by the real apiserver for SSA.  Extracts the
/// HTTP status code from the kube-rs response or error so the result is
/// directly comparable to the fake-path trace.
#[cfg(feature = "k3d")]
async fn run_k3d_step(step: &Step, client: &kube::Client, path: &str) -> u16 {
    match &step.kind {
        StepKind::Apply {
            manager,
            patch,
            force,
        } => {
            // The real kube-apiserver requires apiVersion, kind, and
            // metadata.name in every SSA request body.  The fake doesn't
            // enforce this, so we enrich here rather than baking it into the
            // scenario definition.
            let mut full_patch = patch.clone();
            if let Some(obj) = full_patch.as_object_mut() {
                obj.entry("apiVersion")
                    .or_insert_with(|| serde_json::json!("conformance.kubo.dev/v1"));
                obj.entry("kind")
                    .or_insert_with(|| serde_json::json!("ConformanceObject"));
                obj.entry("metadata")
                    .or_insert_with(|| serde_json::json!({"name": "test-obj"}));
            }

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
                Ok(_) => 200,
                Err(kube::Error::Api(ref e)) => e.code,
                Err(e) => panic!("k3d: unexpected error during apply: {e}"),
            }
        }
    }
}

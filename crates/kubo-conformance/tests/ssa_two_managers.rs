//! Step 12 gate: two field managers conflict on the same spec field.
//!
//! Verifies that the fake API server returns 409 Conflict when a second field
//! manager (with `force = false`) tries to claim a field already owned by a
//! different manager.
//!
//! With `--features k3d` the same scenario also runs against a real cluster
//! and the two traces are asserted to be equivalent.

use kubo_conformance::Scenario;
#[cfg(feature = "k3d")]
use kubo_conformance::assert_equivalent;
use serde_json::json;

#[test]
fn ssa_two_managers_conflict_matches_real_apiserver() {
    let scenario = Scenario::new("ssa-two-managers")
        .apply("manager-a", json!({"spec": {"field": "a"}}))
        .apply("manager-b", json!({"spec": {"field": "b"}}))
        .expect_last_status(409);

    let fake_trace = scenario.run_against_fake();

    #[cfg(feature = "k3d")]
    {
        let real_trace = scenario.run_against_kube_apiserver();
        assert_equivalent(&fake_trace, &real_trace);
    }

    // Without k3d: assert the trace shape directly.
    assert_eq!(fake_trace.entries.len(), 2, "scenario has two steps");
    assert!(
        fake_trace.entries[0].status < 300,
        "manager-a's initial apply must succeed (got {})",
        fake_trace.entries[0].status
    );
    // entry[1] status=409 is already guaranteed by expect_last_status above.
}

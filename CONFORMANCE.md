# Conformance divergence registry

The known fake-vs-real divergences for the `kubo-test-fakeapi` fake API server.
This table is the rendered form of `REGISTRY` in
`crates/kubo-conformance/src/divergence.rs`; the
`divergence_table_matches_conformance_md` test asserts this file's table matches
the registry render, so the two cannot drift. The registry is the source of
truth; this file renders from it.

<!-- BEGIN DIVERGENCE REGISTRY -->
| ID | Section | Kind | Summary | Fake | Real | Pinned by |
|---|---|---|---|---|---|---|
| `DivId::SsaStatusSecondManagerConflict` | status SSA — second manager conflict | known divergence | Second manager claims a status field it does not own, force=false | 200 — fake does not model status-subresource ownership | 409 — the expected guard with non-force status; regression net for force on status | ssa_status::ssa_status_second_manager_conflict_is_known_fake_divergence |
| `DivId::SsaStatusCrossManagerOmit` | status SSA — cross-manager omit | known divergence | Second manager omits a field the first manager owns | 200, field removed — full `status` replace | 200, field persists — SSA ownership forbids releasing another manager's field by omission | ssa_status::ssa_status_cross_manager_omit_is_known_fake_divergence |
| `DivId::SsaStatusPruning` | status SSA — structural pruning | known divergence | Structural-schema pruning — the ledger killer | 200, undeclared status field preserved (no schema) | 200, undeclared status field pruned silently; absent on next GET | ssa_status_pruning::ssa_status_pruning_is_the_ledger_killer |
| `DivId::MergePatchClearsSsaOwnedFinalizer` | finalizers — merge-patch clears SSA-owned | characterisation | Merge-patch clears a finalizer another manager SSA-owns | Overwrites finalizer list; no ownership tracking on merge-patch | Overwrites the value; leaves the `managedFields` ownership record — re-apply by the original manager re-adds it (200 on k3d) | ssa_finalizers::merge_patch_clearing_ssa_owned_finalizer_is_known_fake_divergence |
| `DivId::StaleResourceVersionStatusPatch` | concurrency — stale resourceVersion status patch | known divergence | Status patch carrying a stale resourceVersion | 200 — fake does not enforce the resourceVersion precondition | 409 — precondition enforced; intervening write's status left in place | ssa_concurrency::stale_resource_version_status_patch_is_known_fake_divergence |
| `DivId::TwoRapidSequentialStatusPatches` | concurrency — two rapid sequential status patches | ignored gap | Two rapid sequential status patches (ledger append then strike) | Both accepted in order — harness is sequential, no race | Both accepted if no race; 409 if racy — the harness cannot reproduce a real race | ssa_concurrency::two_rapid_sequential_status_patches_cannot_be_raced_in_harness |
| `DivId::AsyncGcAfterFinalizerClear` | reconcile — async GC after finalizer clear | known divergence | Async GC after finalizer clear | Synchronous GC on last-finalizer clear — 404 on next GET | Asynchronous GC — 200 window with no finalizer, then 404 | ssa_reconcile::reconcile_ledger_progression_matches_fake_and_real |
<!-- END DIVERGENCE REGISTRY -->

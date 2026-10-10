# F3 implementation plan

Scope: this isolated worktree only. No commits, push, merge, deployment, service
restarts, or changes to production/dev checkouts or live workflow records.

## Required invariants

- Only the control-loop lifecycle writer changes authoritative bridge state.
- Detached I/O receives copied inputs, never live items/state or writer callbacks.
- Stable request IDs and immutable arguments survive retries and restart.
- Launch authorization, reservations and pending intent are durable before launch.
- Timeout/cancellation/absence are not proof that an external action did not happen.
- Stale results cannot reopen independent-review, Chief, owner or publication gates.
- Uncertain requests retain repository ownership; effects reconcile by stable ID.
- Transient observations and idle polls do not rewrite authoritative state.
- Unknown migration/protocol states fail closed. No production-readiness claim
  without measured evidence.

## Sequential implementation

1. [x] Establish baseline and resumable transition contracts. Add versioned
   validation and pure planning/reducer tests; no new external launch path yet.
   Baseline: 552 PASS; pure protocol/durability: 14 PASS.
2. [x] Worker request protocol: inspect/fence, versioned review binding, immutable
   receipt identity, compatibility with existing pending/prepared requests.
   Focused worker/protocol/adapter/recovery validation: 102 PASS.
3. [x] Reviewer launch phases: prepare, authorize, reconcile, verify, drain;
   scheduler holds, invalidation/archive handling and atomic effect enqueue.
   Writer coordinator and guard tests PASS; runtime activation belongs to stage 4.
4. [x] Control-loop I/O driver and responsive command intake. Bounded detached
   operations, writer-only application, cancellation supervision, reserved control
   capacity, and worker transport concurrency checks.
5. [x] Remaining launch/monitor paths: initial coding, follow-up, autopilot,
   review-format recovery, stale drains, runtime reads and Chief preparation.
6. [x] Outbox and publication transitions: move provider waits off the writer;
   preserve effect uncertainty, publication journals and fresh boundary checks.
7. [x] Startup/migration/restart coverage and semantic durability audit. Exercise
   every checkpoint, ambiguous worker window and supported rollback boundary.
8. [x] Full regression, compilation, diff checks, self-review and evidence report.
   Final code: 633 PASS in 202.713s, no skips, real MCP 2.3.0 test included.
   Compilation/diff checks PASS; self-review fixes have regression coverage.

Each stage requires focused tests before advancing. Failures are repaired and
retested. Update PROGRESS.md with commands, evidence, decisions and next steps.
If a concrete architecture contradiction prevents safe progress, stop and explain
it rather than bypassing a gate or claiming completion.

## Completion evidence

- Deterministic slow-RPC and cancellation tests prove control polling continues.
- Executor isolation tests prove no shared lifecycle state reaches workers.
- Crash/restart tests prove no duplicate launches and no lost uncertain ownership.
- Gate/effect tests cover reordered, stale and duplicated completions.
- Idle/stage-observation tests prove no authoritative rewrite.
- Full regression PASS, compilation PASS, diff check PASS, self-review complete.

## Independent review correction: observed closure at mutation boundaries

The previous completion evidence predates the independent review blocker.

1. [x] Reproduce delayed owner preflight/repository-read closure and identify all
   mutation boundaries, including commands waiting for dispatcher capacity.
2. [x] Apply sticky closure on the sole writer; recheck issue/quality authority
   before new publication and coding/reviewer launch dispatches.
3. [x] Preserve accepted/uncertain journals, repository ownership and closed-issue
   restart reconciliation without blind mutation replay.
4. [x] Targeted fault/regression tests, full regression, self-review and evidence.
   Post-audit targeted: 74 PASS. Final full regression: 650 PASS in 225.884s,
   no skips; compilation/diff checks PASS. Seventeen new closure/recovery tests.

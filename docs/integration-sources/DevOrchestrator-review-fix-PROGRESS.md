# F3 progress

## Latest recovery checkpoint

Independent review correction complete: the 633-test completion below predates
the reproduced CLOSED-issue publication race. The active plan is PLAN.md's
"Independent review correction" section. No production/dev/live state changes or
Git history operations are authorized. Preserve the existing uncommitted work.

Final correction evidence: **650 PASS in 225.884s, no skips**, including the real
MCP 2.3.0 concurrency test. Final log:
`/tmp/devorchestrator-f3-closure-full-final.log`. Post-audit targeted: 74 PASS in
35.363s. Compilation of all source/test modules and `git diff --check` passed.
All correction plan stages and self-review are complete; no test process remains
running. Seventeen new tests cover delayed owner preflight/repository reads,
capacity waits, fresh issue reads, quality revocation, all four publication effects,
accepted/unacknowledged restart recovery, duplicate/repeated observations,
coding/reviewer launch closure, later repository holds and unrelated polling.

Correction implemented: control observations apply sticky archive immediately,
including while a frame is suspended. Publication takes a fresh issue read on the
effect lane before each new commit object/ref, push, PR POST or merge PUT, and checks
the current approval stamp. Commands carry copied authorization data; the sole
writer rechecks it at dispatch, including capacity waits. No callback/state reaches
an executor. In-flight operations are never cancelled merely because closure arrives.

COMMIT/PUSH primary sends retain dispatch_state SENDING (durable transmission
intent, not outcome proof). PR/merge reuse existing send/uncertainty receipts;
responses use existing outcome checkpoints, without an intermediate RETURNED write.
Scheduler retains/repairs their repository holds.
Closed recovery uses only existing journals: matching Git/remote/PR/merge receipts
can complete an already accepted action, while any new primary mutation is denied.
COMMIT index completion and bound merge convergence are allowed after acceptance.
Unaccepted/prepared/unknown authority remains held. Closed initial starts are fenced
or adopted from bound worker receipts without replay; historical uncertain
continuations have no matching inspection protocol and remain held for inspection.

Validation so far: initial compatibility 56 PASS in 11.517s; closure regressions
12 PASS in 18.848s; expanded loop/publication/scheduler/driver/durability targeted set
134 PASS in 85.834s (`/tmp/devorchestrator-f3-closure-targeted.log`); final dispatch/
closure/driver/I/O/scheduler set 50 PASS in 22.627s
(`/tmp/devorchestrator-f3-closure-final-target.log`). The first closure-test fixture
misread empty repository argument lists and could wait for command consumption
after archive; both fixture errors were fixed and the tests rerun. Test assertions
were retained, including unchanged Git HEAD and zero new publication effects.

The admission/accepted-MERGE audit passed 73 tests in 28.412s
(`/tmp/devorchestrator-f3-closure-final-target-2.log`), and a late hold/no-state-churn
test passed separately. A full regression started at `/tmp/devorchestrator-f3-closure-full.log`;
it predates the removal of redundant RETURNED writes. Final verification must use
the post-audit targeted/full logs. Compile and diff checks passed.

Post-audit targeted validation: 74 PASS in 35.363s
(`/tmp/devorchestrator-f3-closure-post-audit.log`). Final complete regression passed
on the unchanged final implementation:

```sh
.venv/bin/python -m unittest discover -s tests -v > /tmp/devorchestrator-f3-closure-full-final.log 2>&1
```

The preceding full run completed: 650 PASS in 231.042s, no skips
(`/tmp/devorchestrator-f3-closure-full.log`). It predates the journal-write reduction;
use closure-full-final as the final evidence. No new code changes are planned.

Self-review completed: checked fresh-read and queued-dispatch boundaries; closure
and quality revocation; in-flight ownership; accepted COMMIT/PUSH/PR/MERGE recovery;
coding/reviewer launch fencing; index/merge-convergence continuation; immutable
IDs/arguments; polling capacity; semantic saves. The final audit removed redundant
response observations, reused PR/merge journals, retained legacy ambiguous records
and fenced previously reserved launches behind later repository holds. The remaining
provider TOCTOU and operator holds are documented in docs/resumable-transitions.md.

## Historical F3 completion before independent review

2026-10-09: all eight stages complete. Final full regression: **633 PASS in
202.713s, no skips**, using real MCP 2.3.0. Command and complete temporary log:

```sh
.venv/bin/python -m unittest discover -s tests -v > /tmp/devorchestrator-f3-full-final-3.log 2>&1
```

The previous complete run passed 623 tests in 188.667s, with the real MCP 2.3.0
SDK and no skips (`/tmp/devorchestrator-f3-full-final.log`). It predates the final
closure/repository-binding/persistence regressions; full-final-3 is the completion
evidence. Compilation and `git diff --check` also passed on the final implementation.
No production/dev checkout, live state, service or Git history was changed.
Logs are temporary evidence and may not survive a machine restart.

Recovery status: implementation and verification are complete; there is no pending
coding stage or running test process. Changes remain uncommitted for independent
review. Do not reset changes or live records, or infer authorization to deploy.
The historical sections below describe earlier checkpoints, not remaining work.

Final self-review additionally found that control/discovery reducers need the same
fatal persistence boundary as suspended lifecycle frames. ControlLoop.step now sets
the writer context for the entire serial reducer pass and resets it on exit. A disk
failure escapes the generic retry handler before any new dispatch. A deterministic
idle-control save-failure test passed; the combined loop/I/O/coordinator/driver/
migration suite passed 43 tests in 6.194s (`/tmp/devorchestrator-f3-writer-failure.log`).
The isolated fault test was rerun after simplifying its fixture (1 PASS in .016s).
This change postdates the start of full-final-2; full-final-3 passed on the final
code. Compilation and `git diff --check` passed again after this change.
Full-final-2 completed with 632 PASS in 190.573s and no skips.

Self-review completed: checked all driven RPC/repository boundaries, JSON isolation,
single-writer reducers, durable authorization before dispatch, immutable request
replay, reservation/cleanup fencing, stale source/tree controls, Chief/owner gates,
uncertain effect recovery and migration. The closing-preflight, repository-binding,
stale-answer and outer-writer persistence findings were corrected with regression
tests. The remaining limitations below are operational holds, not unsafe fallbacks.

## Starting point

The worktree already contains uncommitted F1/F2 hardening and F3 characterization
tests from the preceding task. Preserve these changes. The preceding full run
passed 552 tests; rerun a baseline for this implementation.

Existing code inspection identifies these dependencies:

- ControlLoop.step awaits a complete stateful transition and its cancellation.
- control_pass, GitHub effects, project identity and publication also contain
  synchronous I/O; reviewer launch alone is not the full responsiveness fix.
- observe_coding_turn removes invalidated review pending_rpc; detached launch
  requires retaining that authority until request-bound reconciliation.
- Worker request receipts precede launch, but PENDING/prepared states remain
  ambiguous and must preserve their existing recovery behavior.
- process_owner_actions synchronously acquires worker.lock and publishes using
  a mutable item/persist callback. It cannot simply run in a background thread.

## Historical stages 1–3

Stage 1 complete: `review_transitions.py` now defines pure versioned PREPARE/LAUNCH/VERIFY/
DRAIN/HELD records, the unchanged request-ID algorithm, immutable pending-envelope
binding, invalidation and late acceptance. It is not connected to runtime paths.
`tests/test_review_transitions.py` covers round allocation, input isolation,
checkpoint JSON round trips, stale acceptance, malformed state and source changes.

Validation: `PYTHONPATH=tests:. python3 -m unittest test_review_transitions
test_durable -v` passed 14 tests, then passed again after self-review tightened
phase-type validation and prevented duplicate acceptance from replacing an already
bound runtime generation.

Baseline: `python3 -m unittest discover -s tests -v` passed 552 tests in 226.417s;
log `/tmp/devorchestrator-f3-baseline.log`. It started before new tests were added.

Stage 2 complete. `worker_runtime.inspect_start_request` acquires worker/request
locks nonblockingly, reports ABSENT/BUSY/UNKNOWN without negative launch claims,
and fences only absent starts without metadata/directories/leases. FENCED receipts
block delayed launches before preparation. Existing PENDING/STARTED receipts are
never fenced or relaunched by inspection. CLI/MCP capabilities and request inspection
are wired. Bound reviews use signature version 3; versions 1/2 retain their existing
signature behavior. `validate_review_source` checks coding generation/turn, successful
runtime, registry/catalog identity, canonical repository and snapshot under the
worker admission lock. Prepared-request replay revalidates without claiming a
definitive no-launch failure. `_run_agent` preserves binding/request metadata.

Self-review separated confirmed SourceChanged from failed snapshots and UNKNOWN
runtime observations: transient failures are not falsely labeled STALE_INPUT.
Existing request signatures cannot be upgraded in place; orphan v3 metadata is held.

Validation: `PYTHONPATH=tests:. python3 -m unittest test_request_protocol
test_recovery test_worker_integration -q` passed 32 tests in 1.247s;
log `/tmp/devorchestrator-f3-protocol.log`. `git diff --check` passed.

Final stage 2 validation: `PYTHONPATH=tests:. python3 -m unittest test_review_binding
test_request_protocol test_review_transitions test_execution_profiles test_mcp_adapter
test_recovery test_worker_integration -q` passed 102 tests in 2.350s;
log `/tmp/devorchestrator-f3-worker-stage.log`. Compilation and diff checks passed
before the final source-classification refinement; repeat them at the next checkpoint.

Stage 3 complete (not activated in ControlLoop yet): ReviewCoordinator owns pure
writer-side phase application. Authorization atomically saves reservation, lock,
pending_rpc and phase before returning a copied launch command. Acceptance is
receipt/request/generation bound. Stale launches drain only with matching terminal
cleanup generation. Adoption and outbox enqueue share one checkpoint. A persistence
failure raises DurabilityFailure (BaseException) so no dirty-memory dispatch is
possible. Archive/coding-turn invalidation preserves authorized envelopes. Scheduler
honors transition ownership even if repo_locks is missing, and repairs without
stealing conflicting locks. Chief approval rejects all active transitions.

Validation: initial coordinator/protocol/scheduler/binding run: 48 PASS; bridge
compatibility/Chief/retry run: 63 PASS in 9.131s. Final coordinator/guard/scheduler/
protocol/Chief run: 58 PASS in .095s (`/tmp/devorchestrator-f3-stage3.log`). Added
archive, coding-generation invalidation, missing/conflicting lock and malformed
schema tests after self-review. Next: stage 4 detached I/O driver and activation;
other serial paths remain uncorrected, so F3 is not yet complete.

## Historical activation checkpoint

Stage 4 runtime integration is now active in this worktree: `control_loop.ControlLoop`
replaces the serial class. `step()` performs no provider awaits; WriterContinuation
advances stateful frames only on the writer and accepts only frozen JSON Commands.
Dispatcher retains timed-out/cancelling operations until actual completion and
reserves separate control, targeted RPC, work, effects and notification capacity.
Worker MCP handlers are explicitly async with separate bounded control/work pools.
At this checkpoint no MCP SDK was installed; handler concurrency was tested
through an isolated registration adapter. Stage 7 below supersedes that limitation.

Necessary dependencies for activation brought forward parts of stages 5/6: existing
coding/continuation/Chief RPC and repository I/O now cross explicit boundaries;
publication and outbox use generator cores with synchronous compatibility adapters.
Mutable journal and gate updates remain on the writer. Worker-lock acquisition is
nonblocking in driven lifecycle code. Slow Telegram I/O is separate from owner
publication and its SENDING receipt is reconciled by the writer.

Validation checkpoints: I/O isolation/cancellation primitives 6 PASS; reviewer
driver/coordinator initial run 42 PASS (included inherited/imported test duplicates,
subsequently removed); publication compatibility 15 PASS; outbox/durability 20 PASS;
active loop/driver/owner flow 23 PASS; hardened effects/publication/loop 32 PASS;
MCP handler capacity/I/O/loop 20 PASS; rewritten ControlTests plus MCP adapter
40 PASS in 61.761s (`/tmp/devorchestrator-f3-control-regression.log`). The earlier
I/O boundary suite had 141 passing tests plus a misspelled nonexistent test module;
the correct owner suite was included in the later passing run.

Self-review caught two integration hazards: command intake must not consume owner
actions against transient gates inside a suspended Chief frame; and a REJECTED
replay cannot settle an older delayed transmission. The former now defers same-
issue lifecycle application until its frame completes. The latter retains DRAIN
authority until worker fencing or generation-bound terminal cleanup. The negative
fence/coordinator tests passed 22 cases. Authoritative save now excludes timing
samples and heartbeat-only observation fields; a real-loop no-rewrite test is
running with retry regressions (`/tmp/devorchestrator-f3-semantic-loop.log`).

At that checkpoint still required: legacy pending-state migration, further restart/cancellation and
publication/effect uncertainty tests, completion-comment send journaling, audit of
generic pending RPC negative replies, complete regression/compilation/self-review,
documentation of rollout and unresolved deployment evidence. Do not declare F3
complete or production-ready yet.

Read PLAN.md and this file, inspect the uncommitted diff, and continue the current
stage. Never reset existing changes, live requests, locks or queues. Do not enable
a partially migrated production path or infer approval to deploy.

## Completed stages 4–7

Stage 4: ControlLoop now dispatches immutable JSON I/O and applies results only on
the lifecycle writer. Per-issue continuations are never asyncio Tasks. Dedicated
control and owner/Chief RPC capacity remain available during slow work, effects and
notification I/O. Deadlines and cancellation retain in-flight ownership through
cleanup. Tests prove polling before blocked RPC/cleanup release and no second launch.

Stage 5: initial coding, follow-ups, autopilot, format correction, stale drains,
monitoring and Chief checks use explicit suspension boundaries. Actual owner state
application waits for a safe boundary; polling does not consume commands against
temporarily cleared gates. New coding starts bind the repository/configuration with
worker signature v4; review starts use v3 source/tree/policy bindings. Original
signatures, IDs and envelopes are retained on retries. A close observed during
preflight prevents a new unsent launch. Old ANSWER and empty follow-up comments do
not invalidate a pending reviewer; actionable follow-ups do.

Stage 6: publication and outbox share generator cores with synchronous compatibility
entry points. Only I/O is detached; journals, gates, save callbacks and lock ownership
remain with the writer. Comments/PR creation/completion sends journal their uncertain
boundary before mutation and require positive reconciliation after ambiguity. An
empty query never authorizes duplicate non-idempotent sends. Generic rejected starts
must fence the request before dropping pending authority; rejected continuations
remain uncertain. A Telegram wait cannot block owner publication.

Stage 7: legacy reviewer pending requests enroll into versioned INSPECT/DRAIN/HELD
recovery without changing envelopes. Absent starts are fenced; accepted runtimes need
bound terminal cleanup. Prepared, generation-less and unknown records stay held.
Scheduler recovery preserves missing/conflicting ownership and archived uncertain
requests. Remote cleanup proof exposes the actual remote generation. Restart tests
cover reviewer phases, worker prepared/receipt windows, legacy migration and a real
temporary Git COMMIT applied before its response: restart adopts the same commit.
Semantic saves exclude timings/heartbeat-only samples; a real loop performs 120
unchanged polls with zero durable write_json calls and unchanged inode/mtime.

Focused validation, after fixing failures at each checkpoint:

- Semantic loop/retry: 12 PASS in 15.179s; `/tmp/devorchestrator-f3-semantic-loop.log`.
- Negative reply fencing/profiles: 50 PASS; `/tmp/devorchestrator-f3-prelaunch-fencing.log`.
- Completion/recovery: 39 PASS; `/tmp/devorchestrator-f3-completion.log`.
- Migration/driver/scheduler/loop: 34 PASS; `/tmp/devorchestrator-f3-migration.log`.
- Final restart/MCP/recovery: 38 PASS in 5.883s; `/tmp/devorchestrator-f3-restart-final.log`.
- Closure/preflight boundary: 49 PASS in 5.523s; `/tmp/devorchestrator-f3-latest-boundary.log`.
- Repository binding + actual HTTP concurrency: 69 PASS in 7.460s;
  `/tmp/devorchestrator-f3-repository-boundary.log`.
- Final binding/follow-up/driver/protocol/adapter regressions: 51 PASS in 7.572s;
  `/tmp/devorchestrator-f3-boundary-final.log`.

The first new full run (616 tests) exposed three old effect-delivery expectations.
They were corrected to assert uncertainty retention/exact receipt matching, with
negative late-receipt/legacy tests added, then the full 623-test run passed. The last
binding fixture initially tried two tasks against its own occupied capacity; separate
tests now cover immutable v4 and non-retrofittable v2 requests. No failing tests were
removed or skipped to obtain the passing runs.

The ignored worktree `.venv` contains mcp==2.3.0. The real SDK concurrency test opens
only an ephemeral loopback HTTP listener with fake devctl; two blocked starts leave a
report RPC responsive. No actual Cursor/SSH provider or production endpoint is used.
Reproduction commands, schema and rollout/migration restrictions are documented in
docs/resumable-transitions.md.

## Operational limits retained deliberately

No hard real-time promise for local CPU/fsync or arbitrary deployment faults.
Shutdown drains submitted operations before releasing locks and may wait for slow
transport cleanup. An uncertain prepared stale request, legacy generation-less
runtime or unqueryable external acceptance can require operator inspection; no
reset/deletion/unsafe resend bypass is provided. Rollout requires a worker/MCP upgrade
before bridge activation and independent review. Real provider soak and live
rollout/rollback have not been exercised. Production readiness and the original
production exception are not proven by this work.

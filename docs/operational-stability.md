# Operational stabilization

The existing bridge state remains the sole lifecycle authority. GitHub and
Telegram are projections; neither labels nor successful delivery authorizes a
coding turn, quality gate, or publication.

## Operator status

Use `./devctl.py status 39 --issue` for the concise view, or add `--json` for
ChatGPT/operator tooling. It reports project and lifecycle, coding configuration,
task/session/turn, independent reviewer/round, Chief bundle/state, current local
runtime evidence, queue/lock ownership, publication stage, next action, projection
health and consistency diagnostics. This view reads atomic records and probes
local process identities without writing state, starting work, contacting SSH,
sending messages, or exposing prompts/credentials. Remote evidence is explicitly
uncertain when a live probe is unavailable. Normal bridge discovery/report reads
perform reconciliation automatically.

## Reproduced transition paths

* Closed #6: `reconcile_tick` previously fetched an issue absent from the open
  poll, then unconditionally called `process_tracked`. A terminal coding report
  reached `process_chief_review`; an old independent PASS was enrolled in the
  Chief gate, whose repository snapshot comparison called `stale_chief`. The
  subsequent review-hold path emitted FRESH_REVIEW_REQUIRED. The local #6 record
  inspected during implementation contains that exact drift diagnostic. The
  regression test closes an old PASS, changes the repository, and runs repeated
  recovery ticks while asserting no Chief backfill, hold or notification.
* #39 start projection: launch/RPC completion and scheduler ownership were
  durably saved before label/comment effects; effects were flushed only after
  serial reconciliation of all tracked tasks, in insertion order, at most five
  per batch. Slow recovery or an older effect backlog therefore left a locally
  started task externally queued. Current #39 receipts are DONE, so they cannot
  establish the historical duration or relative contribution of those delays.
  Tests reproduce both the backlog and the crash between STARTED persistence
  and effect enqueue. Launch projection now runs before subsequent recovery,
  labels/start receipts are prioritized, and durable desired state is repaired
  even during worker-discovery outages.
* Terminal residue: the supervisor records a durable direct-child exit even if
  sandbox descendants remain in its session. `task_state` returned DONE/FAILED
  without persisting that status in `meta.json`. Worker leases were replaced
  only on later admission; scheduler recovery released only external-owner
  locks. Real isolated-process tests reproduce completed work with surviving
  children and stale RUNNING metadata/leases.

## Historical fence and reopen policy

Observed closure is a sticky archival fence. MERGED, explicit completion,
supersession, or a durable `dev:done` state is also excluded. Recovery observes
closure before coding-turn invalidation, malformed-state quarantine, Chief
backfill, follow-up, review/correction, or owner-command processing. Undelivered
lifecycle effects are suppressed. Only archival diagnostics and runtime/lease
reconciliation may change excluded records; closure never proves a process dead.
Unreserved queued intent is cancelled as archival reconciliation, so an inert
historical FIFO entry cannot block new tasks. Reserved or uncertain launches
retain ownership until terminal proof.

Reopening an observed closed issue leaves its old generation archived, even if
`work:queued` or stale commands remain. Create a new task issue for new work.
COMMITTED/PUSHED/PR_CREATED retain their existing explicit owner-command policy
and remain excluded from retroactive Chief enrollment. No quality-gate binding
or publication authorization has been relaxed.

## Projection and notifications

Recovery compares durable desired labels against current GitHub observations.
Delivery re-reads the issue/labels, rejects superseded targets, repairs only the
lifecycle labels, and confirms the resulting projection. Stable comment effect
markers reconcile an accepted POST after a crash without duplicate comments.
Startup can reconstruct a missing launch comment receipt. GitHub failures have
bounded exponential backoff; repeated failures produce one concise diagnostic
and ERROR health until repair succeeds. Pending comments/evidence count as
projection drift as well as labels.

New Telegram events carry a semantic task/turn/review/Chief/owner fingerprint.
Delivery checks it again, re-reads issue closure, derives the next action from
current durable state, and drops superseded or non-actionable events. Legacy
lifecycle messages are regenerated from current state or dropped. A no-delta
completion has no owner action and is visible in status without a Telegram
alert. Projection repair itself emits no Telegram alert. Existing SENDING and
UNCERTAIN Telegram receipts still suppress uncertain resends.

## Owned groups, metadata and leases

Each isolated supervisor records task, role/agent, turn/generation, wrapper PID,
boot/start identity, PGID/SID and identities of observed live group members in
`process_group.json`. Completed generation receipts are retained in
`runtime_audit.json` before a resume. Identity recording failure never stops
exit/heartbeat supervision; missing proof prevents a signal.

Observed child process groups in the original isolated session are recorded
separately. Each needs its own live identity anchor. An unrecorded group in that
session prevents cleanup and lease release rather than being killed by a
numeric session match.

Cleanup requires terminal evidence, current generation binding, an isolated
wrapper-owned group/session, and a live recorded anchor. It checks ownership
before SIGTERM, waits at most one second, rechecks before SIGKILL, then confirms
absence. A reused leader, changed session, missing anchor, corrupt receipt,
active turn or uncertain remote worker gets no signal. Executable names and
silence/age are never cleanup criteria. Legacy groups without bound evidence
are left alone when live or ambiguous.

Discovery/report reconciliation runs under the existing worker admission lock
with a bounded sweep. It durably records effective DONE/FAILED and cleanup
results, preserving timestamps/session/original exit evidence. A proven absent
or cleaned terminal runtime releases its worker lease. Ambiguous runtime keeps
capacity and repo ownership; missing queue/lease proof is diagnosed without
stealing ownership. Reviewer/correction intent retains its existing lock.

Worker completion only frees agent capacity: a tracked issue retains repository
ownership through the completion-to-review/correction handoff, including recovery
before the next RPC intent is recorded. Recovery releases a proven terminal lock
after a durable terminal review/report outcome, publication milestone, or archival
fence. Independent PASS retains the repository until bound Chief approval is
complete, including restart before Chief backfill and normal waiting ticks. The
shared release path enforces this hold; existing published historical records
retain their Chief enrollment exemption. Pending RPC/reviewer intent overrides
older terminal markers, and uncertain
runtime ownership still prevents release. Older queued issues cannot take the
repository between an owner's coding turn and its next review/correction phase.
On archival, a cancelled next-phase entry permits release only with proof that
it never launched: the archival cancellation marker, no task ID, no pending
launch and no cleanup uncertainty. Completed turns still need terminal proof.
The missing-queue-ownership diagnostic clears as soon as matching jobs prove
ownership, even when review/Chief intent deliberately retains the lock; a lock
that still has no matching jobs remains diagnostic and held.

Remote cleanup uses the same code on the registered worker and returns a
validated generation-bound receipt; WORK never signals a remote numeric PID.
Recovery skips live SSH ownership probes by default. Skipped/failed probes and
present directories not visited within the sweep budget are unclassified: they
retain leases and existing diagnostics without manufacturing an orphan warning.
A complete directory inventory can prove a missing task; visited corrupt
metadata or a bound ambiguous cleanup receipt can diagnose uncertain ownership.
Fresh report/discovery observations reconcile leases using their existing probe,
under the admission lock and only while the metadata snapshot is current. A
healthy observation clears an older lease warning only for the matching machine;
terminal release still requires bound absent/cleaned runtime evidence.

### Offline cleanup attestation for historical local runtimes

A completed local runtime without `process_group.json` remains AMBIGUOUS even
when its direct PIDs are gone. Exit codes, null agent identities and missing
receipts cannot prove that escaped descendants are absent. Normal startup and
reconciliation never migrate those tasks automatically.

After an operator independently verifies that the owning systemd MCP control
group has been completely stopped/reset, an offline migration can explicitly call
`devctl.certify_local_absent(task_id, expected_runtime_generation,
control_group_reset_verified=True)`. Keep MCP and TaskBridge stopped during the
migration. The helper uses the worker admission lock, re-reads metadata, refuses
remote or non-terminal tasks and rejects a changed/missing generation. The flag
asserts external reset evidence; the helper does not verify systemd itself.

It durably writes `cleanup_state=ABSENT`, `cleanup_generation=runtime_generation`
and `cleanup_proof=systemd_control_group_reset` without signalling historical
PIDs. Terminal local cleanup trusts ABSENT/CLEANED only for that exact generation;
active/prepared turns cannot use it. Normal successful local cleanup also saves
its generation. A subsequent continue/resume creates a different generation and
cannot reuse the old certification. Valid certification allows capacity admission
and lease reconciliation to converge while retaining task records and original
runtime evidence. Remote cleanup keeps its separate generation contract.

## Verification

Run `python3 -m unittest discover -s tests`, `python3 -m py_compile *.py tests/*.py`,
and `git diff --check`. Focused regressions are in
`tests/test_operational_stability.py`; existing Chief, publication, review,
liveness and recovery suites remain required. Process tests launch only isolated
dummy Python groups in temporary workspaces. No production process or state
cleanup is performed by this implementation task.

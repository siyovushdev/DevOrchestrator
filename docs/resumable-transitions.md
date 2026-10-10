# Resumable lifecycle transitions

Implementation is integrated in the isolated integration worktree. This document authorizes
no deployment, restart, state reset or rollback. Production readiness requires
separate operational review; the original production exception is not proven.

## Execution model

`task_bridge.main` uses `control_loop.ControlLoop`. Its `step()` collects completed
I/O, applies state changes serially, advances eligible writer continuations and
dispatches commands. `wait()` wakes for an I/O completion or polling deadline.
`transition_io.WriterContinuation` advances coroutine frames only when called by
the writer. It accepts only `Command` suspension points, not arbitrary asyncio
Futures. Frames are never scheduled as Tasks. Executor commands and results cross
a JSON copy boundary; no live item, coordinator, scheduler or save callback crosses
it. Executor tasks start with an empty Context.

One lifecycle frame owns each issue. Different issues can advance serially between
I/O waits. Provider polling remains independent; sticky archive/closure revocation
is applied immediately on the same writer, even while a frame is suspended. Other
commands remain unconsumed until that issue can apply them safely. This avoids
acting on transient Chief/owner fields inside a suspended frame. Reviewer launch
has an explicit reducer, allowing archive, changed input and follow-up invalidation
while a start is outstanding. Telegram has separate durable send receipts and does
not retain ownership of an issue's lifecycle frame.

Dispatcher capacity: two control reads, two general operations, one targeted
Chief/owner RPC, one publication/effect operation and two notification operations.
Separate thread pools prevent synchronous provider calls from exhausting command
polling capacity. MCP tools register async wrappers with distinct control and work
pools; worker admission still holds worker.lock. A deadline is diagnostic, not a
negative acknowledgement. Expired or cancelling operations retain their slots and
keys until actual completion. No retry is dispatched behind a still-live operation
with the same key. Shutdown drains submitted I/O before closing writer frames and
releasing publication locks; it can wait for a slow transport.

## Reviewer checkpoint schema (version 1)

`issues[number].review_transition` contains:

| Field | Meaning |
| --- | --- |
| schema_version, namespace, issue | Version and workflow ownership |
| sequence, operation, request_id | Monotonic attempt; `review:N`; unchanged SHA256 ID algorithm |
| phase, invalidated | PREPARE, LAUNCH, VERIFY, DRAIN or fail-closed HELD |
| source | Coding task ID, turn, generation and semantic report/input digests |
| binding | Registered machine, canonical repository digest, registry digest, snapshot |
| request_digest | Digest of the immutable pending_rpc envelope, or null before authorization |
| accepted_task | Request-bound reviewer ID and runtime generation, or null |

`review_launch_sequence` retains the high-water mark even if an unlaunched
preparation is discarded. `pending_rpc` retains the existing name/operation/arguments
shape and unchanged request ID algorithm. New bound reviewer starts use worker
signature version 3; historical signatures 1/2 are not rewritten. The worker guard additionally
binds the catalog digest. Successful stage observations, dispatch status and timing
are not persisted. There is no DISPATCHED flag claiming that transmission occurred.

| Phase | Writer action and next I/O |
| --- | --- |
| PREPARE | Persist logical attempt; collect capabilities, source, policy, tree and preflight |
| LAUNCH | Atomically save pending_rpc + reservation + lock + launch_pending, then send copied start |
| LAUNCH after restart | Inspect receipt first; replay exact saved request only if still current |
| VERIFY | Bind accepted worker identity; freshly verify source, issue and tree before adoption |
| DRAIN | Never resume stale preparation; inspect/fence or observe bound terminal cleanup |
| HELD | Keep authority and gates closed; do not guess an unsupported state |

`ReviewCoordinator.adopt` atomically records reviewer identity and logical outbox
effects, removes pending authority, and keeps review/Chief/owner gates closed until
normal independent review succeeds. A late launch completion is reconciled even if
the logical attempt is stale. It cannot be adopted into a newer coding turn/tree.
Conflicting accepted generations fail closed. Cleanup must match the accepted
generation, including remote cleanup attestations. Persistence failure raises a
fatal DurabilityFailure before another command can be dispatched from dirty memory.

## Worker receipts and fencing

`get_transition_capabilities` advertises support. `inspect_start_request` takes
worker.lock and the request lock nonblockingly. BUSY and UNKNOWN are observations;
ABSENT is not a no-launch proof. `fence=True` can write a durable FENCED tombstone
only when no receipt, metadata, directory/archive or lease can represent accepted
work. Delayed starts check that tombstone before preparation. Existing PENDING or
STARTED receipts are never fenced or silently reset.

A REJECTED/STALE_INPUT answer covers one invocation. It does not rule out an older
delayed transmission. An authorized reviewer therefore enters DRAIN and retains
its request until a fence or bound terminal cleanup settles ownership. Generic
authorized starts also require a fence before releasing a rejected pending request.
Uncertain continuations retain their existing request; they are not treated as
permanently rejected merely because a later preflight failed.

Current prepared v3 requests can resume the original request after revalidating the
source under worker.lock. Invalidated prepared requests cannot safely be resumed
and remain held for inspection. No automatic metadata/lease deletion is introduced.

New initial coding starts use signature version 4, binding machine, canonical
repository and registry digest. The writer stores the original binding in task_spec
and pending_rpc before sending; the worker validates it under its admission lock.
Queue identity drift cannot move an existing request to another repository. An
accepted receipt replays unchanged; a prepared replay rechecks the original binding
and retains uncertain ownership if it changed. Versions 1/2 are never retrofitted.
Both v3 and v4 reject orphan metadata without overwriting it. Worker/MCP must support
both binding arguments before activation; an older tool's rejection cannot trigger
an unbound fallback.

## Other lifecycle paths and external effects

`mcp_call`, coding starts/continuations, stale review drains, format recovery and
Chief processing now yield RPC/repository commands. Their existing pending request
IDs and worker receipts remain the recovery authority. Worker-lock acquisition in
driven code is nonblocking. Publication retains its owner command and journal;
`publication` generator cores contain the shared synchronous/resumable logic.
Detached Git operations use a copied registered configuration and reject registry
drift. The writer owns all journal updates, gate decisions and persistence.

`bridge_effects` has one generator core for both interfaces. GitHub comment POSTs
record SENDING before transmission. Uncertain sends are queried for the exact
payload/marker and never re-POSTed merely because a read is empty. Chief evidence
keeps its stronger exact bundle checks. Telegram uncertainties suppress resend.
CREATE_PR journals its send boundary; uncertain/legacy intent requires positive
PR reconciliation. Completion-comment receipt, label and close recovery remain
bound to durable merge/convergence proof. These protocols reconcile logical effects;
they do not promise that a non-idempotent provider can deliver an unqueryable effect
exactly once under every failure. Safety can require an operator hold.

New publication mutations additionally fetch the issue freshly on the effect lane
(without using polling capacity), check the current Chief/independent/owner approval
stamp, and carry immutable authorization data to the dispatcher. The writer checks
that stamp and the sticky archive again immediately before dispatch, including after
waiting for I/O capacity. This covers commit-tree/update-ref, push, PR creation and
merge PUT. A closed cached issue, a freshly closed issue or revoked quality gates
cannot authorize a new send. Coding/continuation sends also recheck closure and
repository holds at dispatch; reviewer prepare/launch observes the same archive.

COMMIT/PUSH intents record dispatch_state SENDING before submission. PR/merge
reuse their existing send/uncertainty receipts. Responses use the existing bound
outcome checkpoints; no intermediate RETURNED observation is written. Send intents
are not acceptance proofs or cancellation claims.
Their unfinished journals keep repository ownership. Closed-issue restart recovery
queries the bound journal without replaying a new primary effect: a matching commit
HEAD, remote SHA, exact PR or merged PR can settle the actual outcome. An accepted
COMMIT may finish its index update; a proven merge may finish bound convergence.
Missing/contradictory proof remains held. Historical uncertain continuations lack a
request-inspection protocol and are retained for operator inspection. Repeated
closure observations and unchanged receipt reads use semantic saves and add no
authoritative rewrites.

Issue reads and external mutations are separate provider operations; GitHub offers
no atomic issue-state precondition for Git or PR/merge effects. Closure after the
writer has dispatched a send can race external acceptance. Such an operation stays
uncertain until its actual outcome is reconciled; this implementation promises no
new dispatch after an observed revocation, not atomic cancellation at GitHub.

## Migration and rollout order

The shared semantic writer also implements [semantic durability](semantic-durability.md).
It removes only root timings and the named volatile runtime-health fields from
authoritative serialization. Phase records, input bindings, request IDs/envelopes,
generation/cleanup evidence, retry deadlines, locks and publication/send receipts
remain durable. Authoritative persistence failures still raise fatal
DurabilityFailure in every driven reducer before further dispatch. Optional
telemetry is saved afterwards and is never loaded for recovery or gate decisions.
Telegram payloads are immutable event snapshots in both outbox interfaces;
duration/title/action rendering occurs on the writer only after eligibility and
backoff checks, before persisting SENDING and yielding copied transport I/O.
Combined migration/restart/heartbeat/failure regressions are in
`tests/test_resumable_semantic_durability.py`.

1. Review code/tests and inspect deployment compatibility separately. Deploying or
   restarting is outside this task's authorization.
2. When approved, upgrade worker/MCP receipt and binding support before enabling
   the new bridge. Missing capabilities never trigger an unsafe legacy fallback.
3. Existing stable workflows and completed gates keep their interpretation. Existing
   coding/continuation pending requests replay their unchanged envelopes/receipts.
4. Existing pre-phase reviewer starts get `review_migration` version 1 with immutable
   request digest, INSPECT/DRAIN/HELD phase and optional accepted runtime identity.
   Arguments/signatures are unchanged. They are never adopted as newly bound review
   evidence or resumed by migration: absent starts are fenced; accepted runtimes
   drain with matching cleanup. The old sequence is retained, then a fresh review
   uses a new round. Prepared or generation-less legacy runtimes remain held.
5. Unknown schema, changed envelope, missing queue ownership or conflicting identity
   stays closed and retains repository ownership. Missing queue evidence blocks
   admission; recovery never steals a conflicting lock. Archived requests still
   reconcile. Fenced cancellations release only after all other owned work is proven
   terminal.
6. Legacy pending ordinary comments without a send journal are reconciled, not
   blindly resent. Legacy uncertain PR/completion sends may similarly need inspection.
7. Do not roll back to a bridge unaware of phase records/v3/v4 signatures while these
   records or uncertain requests exist. Do not delete phase records, receipts,
   locks or queues to make an old binary run. No automatic downgrade is supplied.

## Verification and limits

Deterministic tests cover stalled reviewer/initial RPCs, cancellation cleanup,
reserved polling capacity, single-writer completion application, same-ID retries,
worker fencing, prepared resume, late/reordered source observations, generation
checks, legacy migration, restart at PREPARE/LAUNCH/VERIFY/DRAIN and atomic adoption.
The real ControlLoop exercises coding → independent review → Chief → owner COMMIT.
A real temporary Git repository checks polling during an applied-but-unacknowledged
COMMIT and restart reconciliation without a second commit. Idle polling tests inspect
actual file replacements. Real MCP 2.3.0 HTTP tests use an ephemeral loopback listener
and fake devctl I/O; two blocked starts do not block a report RPC.

Independent-review closure regressions additionally cover delayed owner preflight,
repository reads, capacity waits, fresh issue checks and Chief revocation. All four
new publication actions are blocked after observed closure; accepted-but-unacknowledged
COMMIT/PUSH/CREATE_PR/MERGE outcomes reconcile after closed restart without a second
primary effect. Coding launch fencing, reviewer preparation, repeated observations
without writes, later ownership holds and unrelated polling are exercised too.

Local fsync, CPU scheduling and unbounded deployment-level faults are not hard
real-time guarantees. Supported ambiguity holds (prepared stale requests,
generation-less legacy runtimes, unqueryable external acceptance) intentionally
trade automatic progress for safety. Real Cursor/SSH/network-failure soak and live
rollout/rollback validation have not been performed. No production-ready claim or
claim that the original production exception was reproduced follows from these tests.

Reproduce the complete test environment within this worktree:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install 'mcp==2.3.0'
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m py_compile *.py tests/*.py
git diff --check
```

Without the MCP SDK the HTTP concurrency test reports a skip; that is not the
complete verification used for this implementation. See PROGRESS.md for run results.

# Operational control loop (resumable transitions)

`task_bridge_state.json` remains the lifecycle authority, protected by the existing
bridge process lock. `ControlLoop.step()` performs short serial state application
and schedules copied I/O commands; it does not await an issue transition or its
cancellation. Control reads have separate capacity from reconciliation, targeted
owner/Chief RPCs, publication and Telegram sends. Completion wakes the loop without
waiting for the next ten-second polling interval.

Only the loop advances lifecycle continuations. Suspended frames, live items,
Scheduler instances, persistence callbacks and WORK_CONTEXT never run as detached
Tasks or in threads. Executor operations accept frozen JSON commands. Unknown
await boundaries fail closed. Blocking worker-lock acquisition is replaced by
nonblocking acquisition and retry in driven lifecycle code. Shutdown waits for
submitted I/O before releasing writer-held publication locks; it is deliberately
not a force-cancellation mechanism.

Independent reviewer starts use versioned PREPARE, LAUNCH, VERIFY and DRAIN records.
The launch envelope, reservation and repository ownership are saved atomically
before dispatch. Restart inspects the worker request receipt before replay; a
current request reuses its exact ID and arguments. Stale requests are fenced or
drained, never replayed to resume stale preparation. New review bindings verify
coding generation/turn, repository, execution policy and tree under worker.lock.
See [resumable-transitions.md](resumable-transitions.md) for schema, migration,
recovery boundaries, test coverage and limits.

Initial coding, continuations, review-format recovery and Chief checks yield at
explicit RPC/repository boundaries. Publication and outbox reducers yield I/O while
the writer retains their existing journals and gate checks. Independent review,
bound Chief PASS and authorized owner commands are still required for publication.
Fresh coding turn, repository identity, reviewed tree, worker ownership and cleanup
proof are checked at the existing publication boundary. `list_tasks(control=True,
project=...)` reads ownership without historical cleanup sweeps. Uncertain runtimes
and missing/incorrect cleanup attestations continue to block publication.

Command polling continues during long operations. One suspended lifecycle frame
owns an issue until its next safe completion. Observed closure is applied immediately
by the same writer and revokes new mutation dispatches. Other commands remain in the
provider and are checked against fresh gates when applied. Resumable reviewer
launches additionally accept archive/follow-up invalidation while I/O is outstanding.
Other issues can progress independently, subject to repository locks and capacity.
This establishes responsiveness to slow provider calls, not a hard real-time bound
on local CPU, filesystem fsync or arbitrary deployment conditions.

Publication fetches the issue before each new COMMIT/PUSH/CREATE_PR/MERGE mutation
and rechecks current approval at dispatch, including capacity waits. These reads
use publication capacity, leaving polling slots available. Closing does not cancel
already dispatched I/O or erase journals. Closed recovery can adopt bound accepted
outcomes and complete their index/convergence cleanup; it cannot issue a new primary
publication or replay a closed coding/reviewer launch. Uncertain publication intents
retain repository ownership. GitHub issue reads and mutations are not one atomic
transaction, so an already dispatched action may still be accepted after closure.

The compatibility `tick` entry point is still a serial full pass for tests and
explicit single-pass tools; production `main` uses the resumable ControlLoop.
Reconciliation stages remain attempt-local ContextVar observations. Only actual
failures persist bounded diagnostics: known stage, exception class, limited local
source locations and retry action; never arbitrary exception payloads.
Authoritative saves exclude timing samples and heartbeat-only fields. Pending
requests, receipts, gates, transition records and scheduler ownership remain
verbatim. Tests measure actual atomic-file writes during unchanged polling; this
narrow projection shares the integrated #47 semantic writer and optional telemetry.
These isolated tests do not prove the original production exception.

Telegram SENDING is durable before detached delivery. The writer applies its
outcome; restart suppresses an uncertain resend. GitHub comments also record the
send boundary and require an exact receipt after uncertain acceptance. Legacy
unproven pending comments are reconciled without issuing another POST. GitHub PR
creation similarly retains uncertain intent and does not re-POST solely because a
query is empty. Operator inspection may be required when acceptance cannot be
proven. Completion comments have their own durable send receipt before label/close
reconciliation.

## Chief wake-up

A complete, current, bound Chief bundle emits `CHIEF_REVIEW_REQUIRED`, keyed by
its immutable bundle ID. It identifies the issue/project, agents, round, bundle
and task link and tells the operator to open/continue ChatGPT. ChatGPT Chief is
an external dependency, not an autonomous daemon. Incomplete, replaced, archived
and superseded bundles cannot send. Delivery rechecks the current tree and issue
and suppresses wake-ups when an authorized verdict has arrived. Chief PASS still
emits the separate `READY_FOR_OWNER` notification. Heartbeats and projection
repairs do not change the bundle notification identity.

## Normal merge convergence

Configure `long_lived_branch: "dev/self-development"` on the registered project
(as shown in `projects.example.json`). An explicit owner `MERGE` with
`method: "merge"` retains that exact method. After GitHub confirms the merged
PR/head/base and exact merge commit, the executor fetches master, proves ancestry,
updates the checked-out development ref using its expected old head, and safely
updates its index/worktree. The remote update is a normal non-force push with a
private pre-push hook that verifies the advertised old head against the durable
expected head. Git's receive-pack then compares that advertised head atomically.
The hook authorizes only the one explicit ref; follow-tags and user hooks cannot
expand the push. No reset, rebase or forced update is used.

Final local HEAD, remote development and remote master must equal the exact merge
commit. `post_merge_convergence` persists the result and fixed, secret-free failure
diagnostic. A failure leaves the PR legitimately merged, exposes
`POST_MERGE_BLOCKED` / `INSPECT_POST_MERGE_CONVERGENCE`, and alerts the operator;
it does not record task completion. A new explicit owner MERGE command with the
original action payload can retry from the expected old/target refs. Concurrent
movement or dirty state fails closed.
On retry, GitHub may report the branch tip as the recorded merge commit rather
than the original reviewed commit. That tip is accepted only for the durably
bound merged PR number, URL, head branch and base branch, then local and remote
convergence are freshly proven. Any other head remains blocked; recovery does
not issue another merge or rewrite the reviewed commit identity.

MERGE reacquires a durable repository hold before the external action. Its
convergence assessment is persisted before attempting the merge, so an uncertain
result also retains ownership. Recovery and direct release keep this hold while
convergence is pending or blocked, including a failure after the local ref has
moved but before the index/worktree update completes. Archiving the issue does
not release an unproven workspace. `DONE` (or an explicit `NOT_REQUIRED`
assessment for a new publication outside this convergence policy) clears a
known merge's hold.

During this hold, follow-ups and owner ANSWER comments remain unconsumed and cannot
start another coding turn. Both control and recovery passes continue processing
owner publication commands so merge proof and convergence retries remain reachable.

Before merge success is known, pending results expose `MERGE_UNCERTAIN` /
`INSPECT_MERGE_RESULT`. A bound observation of the original PR closed without
merging records `NOT_MERGED`, releases the merge hold, and exposes
`MERGE_NOT_COMPLETED` for operator inspection. Early publication errors still
perform this read-only check; failed reads, changed identities and previously
proven merges retain ownership. A fresh explicit GitHub refusal may also prove
non-merge while the PR remains open; it cannot settle an older in-flight uncertain
request. A new authorized retry reinstates `PENDING` before its external call.
The merge PUT captures GitHub's HTTP response envelope: a non-zero `gh` exit
with HTTP 405/409 can resolve a fresh attempt only after a separate bound PR
read proves it remains unmerged. Transport failures, missing response headers,
server errors and failed PR reads retain the hold. Only the numeric refusal
status is retained; raw response headers, bodies and errors are not persisted.
Positive merge responses and bound `MERGED` observations are persisted before
later reads or checks, so a contradictory observation cannot erase merge proof.
`NOT_MERGED` never authorizes completion or issue closure.

A same-session correction drops a proven `NOT_MERGED` assessment without a master
SHA while preserving its historical journal. Pending and blocked assessments
remain fenced. A fresh reviewed correction may build on the exact chain of
executor-created commits ending in proven non-merges; it does not adopt unrelated
HEAD movement or replace the original clean task baseline. Each correction still
requires new independent review, Chief approval and structured owner commands.

Only a durably completed merge/convergence projects `dev:done`, posts an idempotent
completion receipt and closes the task issue. Interrupted closure resumes through
the same outbox; closing never changes Git history. Existing historical merges
retain their archival interpretation.
An absent convergence record cannot create a completion receipt, change labels
to `dev:done`, or close an issue; it is never backfilled with `NOT_REQUIRED` just
because a historical PR is already merged.

## Timing diagnostics

Fixed-schema monotonic timings are bounded to 64 samples in process memory and
an optional sidecar. Authoritative saves exclude those timings and the six named
volatile runtime-health fields. `devctl status --issue` reads bounded telemetry
without writing and derives heartbeat age/lease state at read time. Runtime samples
must match the task identity and complete semantic health digest. Recovery never
reads the sidecar; runtime classification, generation, terminal/cleanup evidence,
retry state and actual errors remain durable. See
[semantic-durability.md](semantic-durability.md) for schema and measurements.

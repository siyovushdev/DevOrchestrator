# Operational control loop (V2-M2I)

`task_bridge_state.json` remains the only lifecycle authority, protected by the
existing bridge process lock. The production `ControlLoop` runs a serial control
pass on a monotonic ten-second deadline, rather than sleeping ten seconds after
an entire global recovery tick. Read-only worker discovery, registered repository
identity resolution, report collection and Chief evidence extraction run in
background observations with detached arguments. Only the loop applies their
results to the existing scheduler, lifecycle and outbox. There is no second
journal or concurrent lifecycle writer.

Each control pass handles authorized Chief verdicts, owner publication commands,
same-session follow-ups/answers, current labels, launch receipts and actionable
outbox effects. Publication commands submitted before the quality gates are open
receive a durable rejection receipt. Heavy discovery failures do not stop these
controls. Background recovery results cannot authorize publication: the boundary
still rechecks coding turn, Chief identity, reviewed content, registered repository,
scheduler ownership and fresh worker metadata while holding the worker lock.
`list_tasks(control=True, project=...)` reads ownership for the registered machine
without historical cleanup sweeps; uncertain process receipts, missing leases,
unknown metadata and surviving terminal children block publication.
Terminal runtimes missing process-group receipts remain uncertain unless a local
cleanup attestation matches the current generation. Control reads honor that
attestation without probing or signalling; a new generation cannot reuse it.

Legacy independent PASS with `READY_FOR_OWNER` but no Chief record stays enrolled
for heavy evidence backfill. The control pass closes the Chief approval gate and
acknowledges premature publication commands without claiming Chief ownership.
Only a built, bound Chief record transfers this phase to the control path.

Recovery retains a fair issue queue across discovery sweeps, bounds background
issue observations to two, and applies one issue slice per cycle with a five-second async RPC budget;
interrupted launches retain the existing durable request/pending-operation identity.
Expensive report/evidence reads remain in flight across cycles. Normal local/GitHub
operation should process a visible verdict or owner command within one control
cadence plus targeted reads (normally within twenty seconds). This is a service
objective, not a promise during provider/SSH failure or slow publication operations.
The compatibility `tick` entry point remains a deterministic full recovery pass for
tests and explicit single-pass tooling; production `main` uses `ControlLoop`.

Telegram sends also run as detached transport calls. The outbox durably records
`SENDING` before dispatch; the single lifecycle writer later applies acceptance.
A restart with no in-process acceptance result converts `SENDING` to `UNCERTAIN`
and suppresses automatic resend. Known rejection retains bounded retry semantics.
Slow Telegram callbacks therefore cannot hold the control loop behind a send.

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

`devctl.py status <issue> --issue` (or `--json`) exposes timings and convergence
without writing state. The bridge retains at most 64 fixed-schema numeric samples
using monotonic durations: control total, GitHub command reads, targeted controls,
heavy reconciliation, worker discovery/reconciliation, tracked issue processing,
Chief evidence extraction, outbox flush, Telegram callback and publication action.
Timings never store exception text, prompts, tokens, URLs or provider payloads.

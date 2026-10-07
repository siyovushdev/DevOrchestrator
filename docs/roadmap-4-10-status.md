# DevOrchestrator roadmap 4–10

All seven phases are implemented in the development worktree. The 2026-10-06
pre-rollout audit passed, but controlled rollouts #18 and #25 exposed MCP transport
and optional-worker health defects and safely rolled back. The development repairs
and next rollout acceptance gate are documented below. This repair changes no live checkout,
service, task bus, Telegram chat, or publication.

| Phase | Status | Implementation / verification |
|---|---|---|
| 4 Owner Gates | Implemented | Deterministic executor, strict transitions, owner identity and review epoch, exact reviewed-tree fingerprint, baseline/branch/HEAD checks, durable action journal; temporary Git and fake GitHub tests |
| 5 Telegram | Implemented | Concise terminal context, explicit next action, concrete questions, bounded transport retries and durable outbox; retry/format tests |
| 6 NEEDS_OWNER | Implemented | Coding and reviewer contracts, owner-needed labels, structured ANSWER, same-session continuation, approval invalidation, new review; whole-bridge local tests |
| 7 Queue | Implemented | Durable oldest-eligible FIFO, explicit global/per-machine/coding/review capacities; ordering, mixed workload and worker admission tests |
| 8 Repo Lock | Implemented | Registered canonical Git worktree identity, durable bridge ownership and worker leases, process identity/group checks; same/different repo and stale-lock tests |
| 9 Recovery | Implemented | Atomic fsynced JSON, singleton bridge, RPC intent/receipts, external-effects outbox, tracked-issue reconciliation, detached SSH worker with durable logs; restart/outage tests |
| 10 Cleanup | Implemented | Dry-run-first CLI, explicit retention, idempotent close/archive, preserved history, unknown-file protection, WORK enrollment/host binding; cleanup/guard tests |

## Decisions and configuration

Helpers live in `publication.py`, `scheduler.py`, `durable.py`, `worker_runtime.py`,
`remote_worker.py`, `bridge_effects.py`, `notifications.py`, `redaction.py`, and
`lifecycle.py`. There are no new third-party or paid API dependencies. The existing
MCP runtime remains required for the real MCP server/bridge transport; local tests
use a deterministic replacement and do not connect to the live service.

Use `bridge.env.example` for explicit configuration. Default capacities are one
coding task, one review, two agents total, one agent per WORK/FIREFLY machine.
Apply the same limits to the bridge and MCP service environments: worker admission
also enforces these limits for direct MCP/CLI starts. An older eligible request
wins; an issue blocked by another repository's lock does not stall unrelated work.
Issues and comments are paginated so older issues/owner answers cannot fall beyond
a fixed fetch limit. The bridge retains repository ownership across coding, queued review and review
corrections. It releases on PASS, NEEDS_OWNER or failure only after active/uncertain
work has ended. Reviews therefore cannot race mutable tasks. Codex reviews use
`read-only` sandbox; Cursor uses its existing sandbox and an explicit read-only
review contract. Any repository change during either review rejects PASS.

The registered project registry is the only source of machine/workspace paths.
Aliases and registered subdirectories of one Git worktree share a lock. Projects
can optionally set `github_repo: "owner/repository"` to pin publication identity.
Publication additionally requires an unchanged, uncredentialed GitHub origin,
non-default branch for PUSH/PR/MERGE, exact authorized SHA and matching PR identity.
No force push, automatic tag publication, push hooks, auto merge, admin bypass, branch
deletion or agent publication prompt is used. Coding prompts explicitly reserve all
publication for the bridge executor, including manual issues and owner answers.
Pending pre-rollout RPCs keep their original arguments/signature; newly admitted
requests freeze their task spec so editing a cloud issue cannot rewrite a retry.
MERGE uses GitHub CLI `gh api` with the [SHA-conditional merge endpoint](https://docs.github.com/en/rest/pulls/pulls#merge-a-pull-request),
so a protected/queued branch safely refuses instead of enabling delayed auto-merge. COMMIT uses a temporary index, `commit-tree`, compare-and-swap `update-ref`
and index reconciliation; it deliberately bypasses hooks/signing so hooks cannot
silently alter reviewed content. Existing registered repositories need configured
Git author identity. Automatic publication refuses submodule repositories.

An independently reviewed snapshot includes HEAD, branch, origin, complete final
Git tree (tracked/untracked non-ignored content, deletions, file modes and symlinks),
status and actual index digest. Review start and PASS must match, then COMMIT must
match that PASS snapshot and the original clean baseline. Dirty or unknown/adopted
baselines, detached HEAD and unexpected HEAD/branch movement block COMMIT. Ignored
files are excluded because they cannot enter the automatic commit. A coding follow-up
invalidates approval before RPC admission and preserves previous snapshots/journals
in `publication_history`; publication authorization must be posted
after the current PASS. A task resumed after publication cannot silently rebase its
original Git safety baseline; unexpected HEAD movement remains a safe refusal.

State defaults remain under `~/.local/share/devorchestrator/`. Legacy issue/task
metadata is explicitly reconciled as described below; corrupt JSON fails closed. Older
PASS records without a valid owner state, reviewed-tree fingerprint or approval timestamp
enter a durable fresh-review hold before publication commands are read. Older active
tasks are imported into capacity/lock accounting.
Workspace lookup failures are isolated per task and retain a durable UNKNOWN hold.
Recovery retries the registered identity on subsequent ticks, including after restart;
unrelated machines continue when capacity permits. Until resolution, admissions and
owner publication on the affected machine are blocked because the workspace may alias
another registered repository. An unknown machine holds all repository mutations.
A definitive DONE/FAILED report releases the hold without requiring a workspace lookup.
RPC receipts bind request IDs to argument signatures. An ambiguous local spawn gap
holds UNKNOWN rather than launching twice. PID identity includes boot ID/start time;
an existing PID with a mismatched identity fails safely even if it is a session leader.
Remaining descendants retain the worker lease only after the leader PID is gone.
A known local reboot/dead process can fail safely. New SSH tasks launch detached on
FIREFLY with per-turn receipts,
status and logs under `~/.local/share/devorchestrator/remote-tasks/`; WORK reconnects
and incrementally retrieves logs. Remote reboot becomes FAILED; network loss becomes
UNKNOWN and retains admission/locks. Legacy SSH tasks with uncertain transport state
are held for inspection. Do not remove uncertain receipts/metadata to force a retry.
The bridge's MCP task listing uses `tasks --json --cached`, with no SSH probes.
Cached RUNNING/UNKNOWN records retain capacity; tracked reports refresh individually
with per-issue backoff. An external/direct SSH task needs an explicit report refresh
to release a stale cached hold after completion/reboot. Corrupt local task metadata
appears as UNKNOWN rather than silently disappearing from admission/publication checks.
Continuation saves a byte offset before launch and also recognizes legacy continuation
markers: prior-turn PASS/results/errors cannot stand in for the new turn's output.

GitHub comments have stable effect markers. An accepted comment followed by a bridge
crash is detected before reposting. Undelivered older labels are superseded by the
latest desired state. Returning to a previous state after an outage reactivates
its label delivery; Telegram's terminal receipt remains deduplicated. Uncertain
worker notices use INSPECT_WORKER and preserve the actual coding question/ANSWER epoch.
Publication journals reconcile Git/GitHub before another
explicit command can retry an interrupted action. Telegram retries known transient
rejections at most three times per delivery with 0.5/1-second backoff, 10-second socket
timeouts, and a killable 35-second wall-clock transport limit. Known undelivered outbox
entries get at most five delivery attempts (spacing 2/4/8/16 seconds before exhaustion;
shared outbox backoff caps at 300 seconds). Each flush starts at most five effects and
stops starting new effects after five seconds; an already-started effect retains its
transport bound. Telegram does not support querying acceptance by an idempotency key.
The bridge durably saves SENDING before a POST, records ambiguous timeouts/crashes as
UNCERTAIN, and suppresses automatic resend on restart/replay. This intentionally favors
avoiding duplicates: a notice can be lost if a crash occurs before sending or receiving
an acceptance receipt. GitHub/state remain the durable task record; inspect UNCERTAIN
and FAILED outbox entries during rollout and contact the owner manually if needed.
Notification failures never change task correctness or stop reconciliation. Known credentials and
credential-shaped strings are redacted at report/notification boundaries.

## Owner contracts

Only comments authored by configured `DEVORCHESTRATOR_OWNERS` are accepted. Commands
must follow this sequence, one explicit authorization per stage:

```text
<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"COMMIT","message":"Describe the reviewed change"} -->
<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"PUSH"} -->
<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"CREATE_PR","title":"Reviewed change","body":"Problem, solution and validation"} -->
<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"MERGE","method":"squash"} -->
```

COMMIT without `message` uses `DevOrchestrator: task <id>`. MERGE defaults to squash;
allowed methods are squash/merge/rebase. Replayed completed stages are no-ops. A reported
rejected/failed action requires a new explicit comment with the original command fields.
A crash or transport failure before recording the command outcome can reconcile the
same authorized command on restart; the journal prevents repeating the mutation.
An owner answer uses:

```text
<!-- DEVORCHESTRATOR_OWNER_V1 {"action":"ANSWER","message":"Concrete decision or answer"} -->
```

Legacy `<!-- DEVORCHESTRATOR_FOLLOWUP_V1 -->` followed by text remains supported for
trusted owners and resumes the same session. ANSWER requires an outstanding question.
ANSWER must be newer than the current question epoch, and only an actual coding or
review NEEDS_OWNER accepts it. Old/unsolicited answers are consumed and cannot answer
a future question. Legacy questions without an epoch acquire a fresh epoch and need
a fresh answer. Neither contract permits arbitrary workspace paths or shell publication commands.

## Pre-rollout audit and legacy compatibility

Audit result: **ready for owner rollout review; deployment has not been performed**.
The actual owner executor, reviewed-tree snapshot, resume/admission paths, worker
receipts, PID/boot checks, remote runner, outbox, cleanup and complete development
diff were inspected. Fixes and deterministic regressions cover:

- Old tracked issues modeled on #5/#6/#8 (`task_id`, `mode`, follow-up/report fields,
  no `project`, policy-v1 result markers), durable migration, quarantine and restart.
- Issue #8's stored GraphQL `IC_kwDO…` follow-up IDs matched against REST `node_id`
  and numeric IDs, with durable aliases and no `continue_task` on restart/replay;
  processed owner commands receive the same compatibility protection.
- Historical PASS records modeled on #9–#11 without owner approval evidence,
  and #12's RUNNING reviewer without a start-tree snapshot: fresh-review holds,
  accurate diagnostics, no COMMIT invitation, no consumed publication commands,
  and no duplicate reviewer or coding continuation while review is active.
- Missing reports with bounded attempts, missing/closed issue-fetch failures,
  corrupt task metadata and active tasks lacking registered identity.
- Old ANSWER replay, current-turn result isolation, frozen initial RPC arguments,
  agent publication guards for manual/AutoPilot tasks, fresh direct-worker alias checks
  under the publication lock, and suppression of Git push hooks/extra tags.
- Coding DONE before review, migration saved before review, review PASS saved before
  notification, COMMIT completed before bridge state save, and PUSH/PR/MERGE replay.
- GitHub accepted-comment deduplication/label failure and Telegram rejection,
  timeout, exception, send/crash uncertainty, retry exhaustion and flush bounds.

Before replaying a tracked RPC, follow-up, review or publication, the bridge validates
the task report's ID and exact `(machine, workspace)` against the local project
registry. A missing `project` is saved durably as `compatibility_status=MIGRATED`
with version/source metadata when the report supplies a matching registered name or
has exactly one matching registry entry. Existing project names must match too.
No cloud issue body, shell text, task-ID prefix, reported unregistered path or
unverified canonical path is used to invent a workspace or project. Ambiguous aliases
without a verified preferred name are held; migration grants no new publication approval.

An unresolvable/mismatched identity becomes `compatibility_status=QUARANTINED`.
The item and one stable diagnostic comment/`dev:needs-owner` label are saved together;
subsequent ticks skip its task RPCs and review loop. Report access failure retries after
10 then 20 seconds and quarantines a project-less item on its third failed attempt.
Other transport failures use durable per-issue backoff capped at 300 seconds and log
only the first failure in a consecutive failure streak. A malformed non-object item
is preserved as `legacy_value` and held rather than relaunched. Per-item failures do
not abort the rest of the tick. Healthy admission/reconciliation continues where
capacity and repository exclusion permit it.

Quarantine never means a worker stopped. The scheduler independently retains active/
uncertain tasks, including missing-project tasks; an unknown repository holds its
known machine, and an unknown machine holds all mutation admissions. Definitive
DONE/FAILED reports release these holds. Terminal unresolvable legacy items do not
occupy worker capacity. History, task IDs, receipts and publication journals are kept.
REST comment identity matching preserves stored GraphQL IDs, adds numeric/URL aliases
durably, and leaves existing RPC operation IDs/signatures unchanged. Merely changing
the API adapter therefore cannot replay an already delivered owner follow-up.

Historical PASS requires `owner_state`, `owner_approved_at` and a `reviewed_tree`
fingerprint before REVIEW_PASSED or COMMIT is advertised. Missing evidence becomes
`review_status=FRESH_REVIEW_REQUIRED` with a saved `review_hold`, one stable diagnostic,
`dev:needs-owner`, and a deduplicated Telegram notice. Publication commands are left
unconsumed during the hold; no historical tree or approval is inferred/backfilled.
Pending obsolete COMMIT notifications are superseded before outbox delivery, including
when publication has already advanced to COMMITTED (the next action is PUSH).

A reviewer missing `review_start_tree` cannot approve the current tree: the bridge
reports the missing snapshot rather than claiming the repository changed. An active/
uncertain reviewer remains RUNNING and monitored with its capacity/repository hold;
follow-ups wait until a definitive terminal report. Then the item enters the fresh-review
hold. A trusted `DEVORCHESTRATOR_FOLLOWUP_V1` resumes the same coding session, archives
the prior hold/evidence, and obtains a fresh snapshot and independent review. Old owner
commands still cannot authorize publication after the new approval epoch.

To recover a quarantined item, stop the bridge in an authorized maintenance window,
back up its current state, inspect local task metadata/report and the registry, set
only the verified registered project, clear its compatibility hold/diagnostic and
reconciliation retry fields, then resume monitoring. Never clear worker receipts,
UNKNOWN leases or task IDs to force a new launch. If identity cannot be verified,
leave the item held for operator inspection.

## Cleanup policy and inspection

Run from the installed checkout, during an authorized maintenance window with the
bridge stopped (the command refuses concurrent bridge/worker mutation):

```bash
python lifecycle.py                         # dry-run JSON plan; no archive/close
python lifecycle.py --retention-days 60 --issue-days 14
python lifecycle.py --retention-days 60 --issue-days 14 --apply
```

Default policy closes MERGED (or explicit `dev:done`) task-bus issues after seven
days and moves known terminal local task directories to `tasks/archives/<id>` after
30 days. Failed issues and tasks awaiting owner decisions/publication remain open.
Referenced unfinished tasks, unknown files, symlinks, unregistered task metadata and
missing exit markers are protected. Reports/events/session IDs and bridge publication
history are retained; no unknown user file is deleted. Cleanup is an explicit operator
command, not an automatic destructive step during task ticks. `devctl tasks --all`
and `devctl report <id> --json` inspect archived history; continuation reactivates an
archived task. Remote histories and RPC/effect receipts are retained for recovery.

## Validation

### Coding/review turn isolation (2026-10-07)

Review eligibility now requires the registered coding task's current report to be
`DONE`, with exit code 0 and a positive current-turn number, as well as the valid
READY contract. RUNNING, STARTING and UNKNOWN reports cannot start, continue, accept
or finalize a review, even when their payload still contains an older READY trailer.
Each independent review persists `review_coding_turn`; coding continuation persists
the expected next turn before its RPC. An older terminal report cannot authorize
review for that new turn.

Coding resumption, a changed turn discovered after polling/restart, or an active
legacy reviewer lacking a turn binding invalidates the prior review task identity,
start/PASS snapshots and owner approval. Invalidated reviewer IDs are preserved in
`stale_review_task_ids` and history. Their processes are logically cancelled as
approval sources and monitored until definitively terminal; their output is never
parsed as a new verdict. They retain capacity/repository exclusion while active or
uncertain. A fresh review begins only after the new coding turn succeeds and these
old reviewers drain. No process is killed by this reconciliation.

Pending review RPCs from an invalidated turn are abandoned rather than replayed.
Queued launches are cancelled; possibly accepted launches are marked abandoned and
reconciled against the actual reviewer report. A terminal report drains the abandoned
reservation even if the requested format-correction turn never launched. Active or
UNKNOWN reports retain capacity and repository exclusion. Cancellation and observed
job status are persisted; old `cancelled_review_turn` records receive the same recovery.
The invalidated request consumes its old review round, so a new turn gets a distinct
request ID. Missing worker evidence retains the hold and durable backoff.
Historical PASS without a coding-turn binding holds publication for fresh review.
Legacy reviewer intents without an explicit review role are quarantined for inspection.

The bridge refreshes coding status before reviewer RPCs and after fetching reviewer
output. PASS snapshot/approval persistence also runs under the shared worker
admission lock with a fresh coding report. Worker leases/admission continue to prevent
mutable coding and read-only review from sharing a repository concurrently. Outbox
delivery normally follows reconciliation and also runs on tick failure; cached
active/new-turn observations invalidate approvals during per-issue backoff. A pending
COMMIT invitation is suppressed when coding is mutable or uncertain.

The live reviewer `72624bcb32db` is **not authoritative** for the resumed coding task
`cbec4608fcc6`. A fresh independent review is required after the coding turn containing
this fix has completed successfully. This development task has not edited live task
state, stopped the reviewer, or changed a live service.

Fourteen new deterministic regressions cover mutable reports with stale READY,
unsuccessful/older terminal reports, late reviewer PASS after coding resumes, resumption
during reviewer RPCs, format-RPC suppression, pending-launch cancellation with fresh
request identity, draining stale reviewers with locks held, restart/no-resurrection,
exactly one new review, unbound legacy reviewer rejection, stale notification suppression,
malformed turn/ID isolation, and cancellation recovery.
The initial turn-isolation verification passed **176 tests**, Python compilation
and `git diff --check`; the recovery corrections below supersede its cancellation hold.

### Independent-review recovery corrections (2026-10-07)

Pre-rollout result: ready for owner rollout review after fixing both fresh review
findings. Abandoned pending review-format continuations no longer require an
imaginary future reviewer turn to release their reservation. The persisted cancelled
job is reconciled against the actual task: DONE/FAILED drains it, RUNNING/UNKNOWN
retains exclusion, and no cancelled RPC is replayed. A fresh reviewer starts once
for the successful new coding turn. Older persisted cancellation records recover
without local state edits; obsolete uncertainty diagnostics are cleared durably.
Reviewer output from the abandoned coding turn remains invalid for publication.

The bounded outbox flush now runs in the tick's `finally` path, including MCP task-list
transport/decode and GitHub discovery failures. Already queued comments, labels and
ordinary Telegram notices continue delivery/backoff independently of task discovery.
Pending REVIEW_PASSED/COMMIT invitations and ready-for-review labels are deferred
until successful reconciliation, so an outage cannot advertise stale coding approval.
Existing comment markers, Telegram SENDING/UNCERTAIN receipts, attempt limits and
flush budgets still apply; deferral consumes no delivery attempt.

Six deterministic regressions replace the earlier future-turn hold assertion and
cover cancellation before format-RPC dispatch, an accepted but active/UNKNOWN reviewer,
pre-upgrade cancellation state, task-list transport/decode outages with durable effect
replay, successful approval delivery after recovery, and coding resumption during the
outage without a stale COMMIT invitation. Full verification: **181 tests passed**,
`python -m py_compile *.py tests/*.py` and `git diff --check` passed. Final self-review
covers the complete development diff, persisted recovery transitions and effect safety.
No live checkout/state/service or publication action was changed by this repair.

Remaining live E2E checks: interrupt a disposable review-format RPC before acceptance,
resume coding and restart the bridge; verify the terminal old reviewer drains and
exactly one fresh review starts. Repeat with an actually active/UNKNOWN reviewer and
verify the lock stays held until terminal. During a task-list outage, queue a diagnostic
comment/Telegram notice and verify delivery continues, while approval invitations wait
for recovery. Follow the exact authorized rollout/rollback sequence below; this repair
does not change service ordering or authorize deployment.

### Reviewer format recovery (2026-10-07)

The fresh-review failure caused by two malformed Cursor results is treated as a
review transport/format defect. Accepted verdicts still require the strict final
`DEVORCHESTRATOR_REVIEW` JSON object: invalid JSON, extra trailing data, missing
trailers and invalid verdict types cannot approve publication.

For each completed coding turn, the bridge allows one same-session format correction.
If that result is still malformed, it automatically queues one fresh independent
reviewer session. The fresh session also gets one format correction. Thus at most
two reviewer sessions/four malformed outputs are allowed for the unchanged coding
turn. Exhaustion produces stable `review_status=FAILED`/`dev:failed` with one terminal
failure notification and an infrastructure diagnostic; a formatting error does not
produce an owner/product decision. A valid CHANGES_REQUIRED result follows the
existing bounded coding-correction loop. A new coding turn resets the format-fallback
budget, including when its continuation RPC is recovered after a restart.

The fallback counter (`review_format_restarts`) and RUNNING intent with no reviewer
task ID are saved before a fresh launch. The bridge keeps the repository lock and
the original `review_start_tree`, then uses the next durable review round's request
ID and existing pending-RPC/worker receipts. Restart before launch, during an accepted
but interrupted RPC, or before the completion-state save reuses that request rather
than launching a second reviewer. Active/uncertain reviewers continue to be monitored.
The fresh reviewer retains the independent review role, read-only contract/sandbox,
and strict verdict contract. Formatting recovery leaves the coding session unchanged,
and a changed repository still prevents PASS even after a fresh session.

Ten new deterministic regressions cover malformed JSON/trailing data and invalid
verdict types, same-session correction, missing trailers after correction, durable
fresh-session intent, pending/running bridge restarts, accepted-RPC and completion-save
crash boundaries, final PASS, bounded failure/deduplicated notification, original-tree
protection, and budget reset after recovered coding correction. Verification:
**162 tests passed** (152 existing plus 10 new), Python compilation and
`git diff --check` passed. Self-review and full verification were repeated after fixing
the terminal failure notification's inconsistent event identity. This change was
made only in the development worktree; live Cursor/bridge fallback E2E remains an
authorized rollout check.

### Controlled rollout #18: installed MCP task collection repair

Rollout #18 (2026-10-07) passed all 134 release tests and the real WORK startup
guard, then failed TaskBridge recovery on three polling cycles. Installed MCP 2.3.0
emits `list_tasks` as separate text content blocks, each holding one JSON task
object. Read-only production inspection found 27 blocks. The old transport returned
only the first object, so `Scheduler.recover` iterated its string keys and raised
`AttributeError: 'str' object has no attribute 'get'`. The rollout safely restored
live master to `55d37668101da7f4071e0ebf39285634f4195d17`; the local audited release
`2fc29deefde491ca34d5cde8db6c9953202123df` was not pushed. Evidence is retained under
`/home/siovush/.local/share/devorchestrator/rollout-2026-10-07-030426-833686-UTC`:
`transport-failure.json`, `transport-failure.traceback.log`, and
`deployed-bridge-journal.log`.

The uncommitted development repair decodes every content block and explicitly
distinguishes collection tools from single-result tools. `list_tasks` accepts one
JSON list or one JSON task object per block, preserving the entire ordered list.
SDK source inspection and offline execution confirm an empty Python list produces
zero content blocks; this is valid only for the collection tool. Empty single-result
responses, empty/non-text/malformed blocks, mixed or nested collection shapes,
invalid task id/status fields, duplicate task IDs, error-flagged responses and
conflicting structured collections fail closed with payload-free diagnostics.
No malformed block is skipped and no fixed task count is assumed.

Caller audit: recovery in `tick` and the owner-publication worker exclusion check
both consume `list_tasks` as a list of task objects. `get_task_report`, `start_task`
and `continue_task` callers consume one object for status, receipts, task IDs and
review/coding transitions. `list_projects`, `adopt_task` and `orchestrator_health`
also return one object in `mcp_server.py`; none is flattened or wrapped as a task
collection. The smoke command now uses the same decoder for the project registry
and task listing, rather than assuming `structured_content` is populated.

Regression coverage in `tests/test_mcp_transport.py` reproduces the observed block
semantics, recovers active/uncertain tasks after the first block, checks 1/2/27/31
elements, empty and JSON-list encodings, all object tools, scalar JSON results,
malformed responses and the smoke PASS/failure gate. Full verification: **152 tests
passed** (134 existing plus 18 new) with `python -m unittest discover -s tests -v`,
also run under the installed SDK interpreter without live service calls. Offline
calls to the installed SDK's actual registered server serializer verify 0/1/3/27/31
blocks through the decoder and in-memory scheduler, plus a single-object report.
Python compilation and `git diff --check` passed. Final self-review covers the
complete delta from `2fc29dee`, including the new regression file and rollout order.

**Next rollout gate:** start MCP first while TaskBridge remains stopped. Run
`python mcp_smoke.py` with the installed dependency interpreter and deployed code;
require exit status 0, `WORK_HEALTH: PASS (READY)` or `WORK_HEALTH: PASS (DEGRADED)`,
and `TASK_LIST_TRANSPORT: PASS`, checking decoded task count
and identities against the read-only inventory. DEGRADED is acceptable only with
WORK/local control-plane OK and optional remote worker unavailability explicitly
reported with a diagnostic. Health/project-list PASS alone is
insufficient. A missing/failed task-list transport smoke keeps TaskBridge stopped
and admissions frozen. Only after task-list smoke PASS may the authorized operator
start TaskBridge and verify recovery. This development task performs no deployment,
service restart, commit, push or merge.

### Controlled rollout #25: optional remote worker health repair

Rollout #25 deployed merge `9c2f2d4707ac160da410f1ec548c16e2f33d0e74` and passed
181 tests, compilation, diff checks, MCP startup and
`TASK_LIST_TRANSPORT: PASS (35 tasks, 35 content blocks)`. TaskBridge started on
`work:queued` without duplicate launches/publication. Acceptance then failed because
FIREFLY's SSH doctor probe timed out and the MCP health wrapper had an even shorter
timeout. Safe rollback restored live baseline `55d3766`, with both DevOrchestrator
services stopped and disabled. Durable post-rollout state was retained; backup:
`/home/siovush/rollout-backups/devorchestrator-20261007T060636Z`.

The development repair adds `devctl doctor --json` and a shared health contract:

| Field | Healthy WORK and FIREFLY | Healthy WORK, unavailable FIREFLY | Failed WORK/local probe |
|---|---|---|---|
| `local_control_plane` | `OK` | `OK` | `FAILED` |
| `machines.work.status` | `OK` | `OK` | `FAILED` |
| `machines.firefly.status` | `AVAILABLE` | `UNAVAILABLE` | Independently reported |
| `state` | `READY` | `DEGRADED` | `FAILED` |
| `ok` / doctor exit | `true` / 0 | `true` / 0 | `false` / 1 |

Local machines are required; SSH workers are optional. Probe exit failures, missing
executables and timeouts are isolated per machine, with concise diagnostics and
agent path/version output. Host/user lookup failures are fatal locally; agent
discovery/version output remains informational. Local probes run first. Each probe
has at most five seconds, SSH uses a three-second connection limit, one connection
attempt and bounded keepalives, and the whole doctor has a 20-second probe budget
inside the MCP wrapper's 30-second timeout. Timeout cleanup kills the probe process
group, including local children holding output pipes open. Exhausted-budget machines
are explicitly reported as unprobed/unavailable or failed, according to their role.

`orchestrator_health` returns the same structured object even for local failure.
Doctor execution/timeout errors, invalid JSON, malformed/inconsistent health records
and disagreement between the doctor exit code and health state return structured
`FAILED` / `ok=false`, never a success fallback. A successful health RPC alone does
not establish MCP/project identity or task-list transport health; those remain
independent required rollout checks. `mcp_smoke.py` now enforces WORK health,
permits READY or DEGRADED, prints remote outage diagnostics and still requires full
task-list transport PASS. An MCP error, WORK failure or failed/malformed task-list
response fails the smoke and keeps the bridge stopped and admissions frozen.

Regression tests cover CLI and MCP results for READY/DEGRADED/FAILED, independent
local/remote probe failures, tight timeout options and aggregate budget, probe-group
cleanup, malformed subprocess results, and smoke acceptance/rejection. Verification:
**193 tests passed** with `python -m unittest discover -s tests -v`;
`python -m py_compile *.py tests/*.py` and `git diff --check` passed. Self-review
checked fail-closed local/subprocess errors, independent remote diagnostics, probe
timeout/child cleanup and the required MCP/task-list rollout gates. This repair
is development-only: no live checkout changes, service starts/enables, commit,
push, PR, merge or deployment.

### Controlled rollout retry #16: dedicated WORK queue startup fix

Retry #16 stopped safely during pre-mutation checks because the development startup
guard incorrectly required `dev:queued`. The actual primary WORK bridge intentionally
uses `work:queued` to avoid the legacy HOME bridge's queue. The guard now requires
explicit WORK enrollment, exact host binding and the dedicated `work:queued` label.
The exact production acceptance condition is all of:

```text
DEVORCHESTRATOR_WORKER=work
DEVORCHESTRATOR_WORK_HOST=siovush-Thin-15-B12UC
DEVORCHESTRATOR_QUEUE_LABEL=work:queued
socket.gethostname() == "siovush-Thin-15-B12UC"
```

Missing/empty enrollment, host binding or queue label fails closed. A mismatched
hostname, HOME/unknown worker role, legacy `dev:queued` or any other queue label also
fails; this bridge has no supported non-WORK enrollment role. WORK cannot opt into
the legacy queue. The environment example now specifies `work:queued` explicitly.

Caller audit: TaskBridge startup invokes the guard before loading state or polling;
its admission and state-label transition set share the guard's dedicated WORK queue
constant, so environment/import timing cannot redirect admission to a legacy or
arbitrary queue. New untracked issues labeled only `dev:queued`, `home:queued` or an
unrelated queue are not admitted. Existing tracked-task reconciliation remains
unchanged. The existing `dev:running`, `dev:done` and review/owner status labels are
retained; they are not admission queues. MCP delegates registered-project execution
to `devctl` and does not poll GitHub queue labels.

Regression coverage reproduces the exact production configuration, tests missing
enrollment, HOME/unknown roles, hostname mismatches and missing/legacy/arbitrary
queues, and runs one deterministic bridge startup/tick to verify dedicated admission
and queue-label removal. Invalid startup is checked before state loading or task
admission. This is a development-only blocker fix; no live code, services or rollout
state were changed.

Fix verification: **134 automated tests passed** (the existing 123 plus 11 new tests)
with `python -m unittest discover -s tests -v`. Python compilation with
`python -m py_compile *.py tests/*.py` and `git diff --check` passed. Final self-review
covered the scoped diff, guard callers, admission labels and real label transitions;
the full suite was rerun after correcting a regression-test harness mistake.

The full suite is run with `python -m unittest discover -s tests -v`. It uses temporary
Git repositories, fake GitHub/Telegram/MCP, dummy local subprocesses, and the actual
remote-runner script executed locally under a temporary HOME. No real coding agent,
GitHub publication, Telegram delivery or live TaskBridge is involved.

Pre-rollout development verification: **123 automated tests passed** with the full command
above. `python -m py_compile *.py tests/*.py` and `git diff --check` passed. Self-review
covered the complete tracked diff and all new modules/tests. It found and corrected
resume-intent crash windows, continuation capacity accounting, stale owner approvals,
SSH transport/worker lifetime confusion, push-URL mismatch, merge-queue side effects,
missing pagination, stale label delivery and report credential redaction. Relevant
checks were rerun after those fixes.

Independent-review corrections cover reused session-leader PIDs with same-boot
start-time mismatches in local leases and the actual remote report script, zombie
versus gone-leader child scanning, per-task workspace recovery failures, durable
retry/terminal release, and uninterrupted bridge reconciliation/admission. Final
self-review also verified that unresolved holds block owner publication and that
resolved external work remains protected if another task's repository lock releases.
The fresh compatibility review additionally covered REST/GraphQL comment replay,
historical PASS re-entry, missing review-start snapshots, hold recovery, malformed
hold isolation, and queued stale COMMIT notices. Full verification was rerun after
these corrections.

Production-only validation intentionally deferred: actual Codex/Cursor sessions,
live installed MCP transport, FIREFLY SSH/network/reboot behavior, real Telegram delivery,
GitHub permissions/protections, and authorized publication to a disposable test repo.
The exact checklist below covers each of these without claiming live E2E evidence.

## Exact authorized rollout and production E2E checklist

These are post-review/post-deployment procedures, not actions performed by this task.

1. Obtain separate deployment authorization. Back up the live project registry,
   bridge state, task metadata, worker leases and request receipts. Record active
   coding/review task IDs, sessions, branches and HEADs. Inspect both WORK and HOME
   service definitions/environment and queue labels; identify the actual legacy HOME
   bridge unit. Quiesce launches before updating code. Do not kill active agents.
2. Install the reviewed files into `~/DevOrchestrator` using the existing deployment
   process. Preserve `projects.json`, secrets and durable state. Install/use the
   existing working MCP dependency environment. Apply `bridge.env.example` settings
   to WORK bridge and MCP, replacing `DEVORCHESTRATOR_WORK_HOST` with WORK's exact
   `hostname`, and requiring `DEVORCHESTRATOR_QUEUE_LABEL=work:queued` (never
   `dev:queued`). Keep HOME unenrolled and disable its identified legacy bridge unit
   only with deployment authorization. Confirm HOME's startup guard rejects both
   missing enrollment and WORK's hostname. Verify registered `devorchestrator` still
   points to the isolated development worktree, not the live checkout.
3. Run the full unittest suite, `python -m py_compile *.py tests/*.py`, and
   `git diff --check` on the deployed code before the authorized service restart.
   Start MCP using its actual installed unit while WORK TaskBridge remains stopped.
   Run `python mcp_smoke.py` with the installed interpreter and require exit status 0
   and `TASK_LIST_TRANSPORT: PASS`; compare task count/identities with the inventory.
   Only after this PASS start the WORK bridge; inspect startup logs
   for registry errors, corrupt state and enrollment mismatch. Confirm old RUNNING
   coding and reviewer task IDs are monitored without new launches. Existing old PASS
   records must refuse publication until refreshed review. Inspect labels/outbox.
4. In a registered disposable test repository with clean baseline, a non-default
   branch, configured author and a dedicated GitHub test origin, submit an AutoPilot
   issue. Confirm coding DONE → independent review → READY_FOR_OWNER, one meaningful
   Telegram notice containing project/agents/model/effort/round/branch/verification
   and issue URL. Confirm no publication happens without a structured owner command.
5. Test coding NEEDS_OWNER with a deterministic prompt requiring a real choice. Check
   `dev:needs-owner` and the exact question in Telegram. Post an authorized ANSWER,
   verify the SAME coding task/session ID and incremented turn count, then normal
   AutoPilot/review/PASS. Repeat with a reviewer NEEDS_OWNER scenario. Post an ordinary
   follow-up after PASS; confirm approval/snapshot cleared before resumption and a new
   review round. An owner command from before that PASS must not publish.
6. Separately test CHANGES_REQUIRED → same-session correction → fresh reviewer → PASS.
   With authorized disposable fixtures, resume coding while its old READY payload
   remains visible. Confirm no review starts while coding is RUNNING/STARTING/UNKNOWN.
   Let an old reviewer finish: its result must be ignored, including after bridge
   restart. After the new coding turn is DONE with exit 0 and READY, confirm exactly
   one fresh read-only review and a new approval epoch. Drain any invalidated reviewer
   before the fresh review; retain locks for uncertain workers. Do not reuse reviewer
   `72624bcb32db` as rollout publication evidence.
   With separately authorized disposable review fixtures, exercise malformed output
   twice in the first reviewer, then one fresh independent session. Restart the bridge
   while fallback is pending and running: confirm a single fresh reviewer, unchanged
   coding task/session and read-only review. Confirm a valid final PASS is announced
   once; if both sessions and their corrections are malformed, confirm stable FAILED
   with no publication approval or further reviewer launches.
   Edit a reviewed file silently and attempt COMMIT; confirm refusal and unchanged
   HEAD. Repeat with dirty pre-task baseline, adopted/unknown baseline, detached HEAD,
   changed branch and changed HEAD. Use independent disposable tasks for these cases.
7. On a fresh clean reviewed test task, explicitly authorize COMMIT, then inspect the
   exact commit tree/message/SHA. Restart the authorized test bridge before PUSH;
   replay COMMIT and confirm SHA unchanged. Attempt CREATE_PR before PUSH and MERGE
   before PR_CREATED; confirm rejected skips. Explicitly authorize PUSH, CREATE_PR and
   MERGE separately, inspecting remote SHA, PR URL/head/base and final merge state.
   Replay each completed command; verify no extra commit/push/PR/merge and no force
   push. Do not use the DevOrchestrator production repository for this test.
8. Queue at least three disposable tasks: two aliases of the same repository and one
   different repository/machine. Verify oldest-eligible admission, configured coding/
   review/global/machine limits, repo exclusion across review, and slot/lock release
   after failure. Keep a task demonstrably alive while reconciling/restarting the
   bridge; its lease must remain. Check direct MCP starts obey worker capacity too.
9. With disposable work only, simulate WORK bridge restart during coding RUNNING,
   reviewer RUNNING, coding DONE before review, PASS before owner action and COMMITTED
   before PUSH. Compare task/session IDs and publication journals before/after. During
   a FIREFLY task, interrupt SSH monitoring/network then restore it; detached remote
   work must be observed without a second launch and final logs must synchronize.
   Test an authorized WORK reboot/remote reboot only in a suitable maintenance window;
   inspect dead/UNKNOWN tasks and leases before any manual recovery. Inject temporary
   GitHub/Telegram unavailability; confirm durable comments/labels eventually converge,
   backoff and no token-bearing logs. Check that Telegram's ambiguous acceptance is
   held UNCERTAIN and never automatically resent; inspect any missed notice manually.
   Interrupt MCP task-list discovery with durable diagnostic effects already pending;
   confirm comments/Telegram still flush within their bounds and approval invitations
   wait for successful reconciliation. Cancel a saved review-format continuation by
   resuming coding; restart the bridge, verify a terminal old reviewer drains without
   waiting for an unlaunched turn, and an active/UNKNOWN old reviewer retains its lock.
10. Run cleanup dry-run with a short explicit retention in an isolated state fixture.
    Inspect every planned archive/issue close. Include an unknown file and unfinished
    publication; both must be excluded. Apply once, repeat, and inspect archived
    reports/events with `tasks --all`/`report`. Schedule future cleanup only under the
    documented maintenance policy. Record the production E2E evidence before release.

11. Inspect live legacy #5/#6/#8 against their local reports. Confirm each has either
    a durably verified project or one quarantine diagnostic, with no recurring
    `Review loop requires project in task state` exceptions. Restart the authorized
    bridge again; migration/holds must persist. Confirm a healthy disposable issue
    still reconciles/admits where capacity permits, and stale ANSWER comments do not
    resume a later owner question. Exercise a direct FIREFLY task with SSH offline:
    the cached listing must complete and other tracked issues must still be processed.
    Confirm #8's stored `IC_kwDO…` comments migrate to numeric aliases without any
    continuation of `05a69938c097` on `work_test`. Check #9–#11 hold for missing approval
    evidence with no REVIEW_PASSED/COMMIT invitation and #12 reports its missing start
    snapshot while monitoring the existing reviewer to completion. In a disposable
    equivalent, authorize a fresh follow-up, confirm a new independent review and
    approval epoch, then test publication only with new owner commands.

## Safe rollout and rollback ordering

Only the owner/operator may execute this sequence after separate rollout approval.
Unit names, service scopes and environment-file paths are installation-specific and
absent from this repository; discover and record their actual values, rather than
guessing a systemd unit or issuing a blanket restart. Do not deploy with unresolved
worker identity or an unaccounted in-flight publication.

1. Freeze new task submissions, direct MCP starts and owner publication commands.
   Inventory the actual WORK bridge/MCP and legacy HOME unit names, scopes, ExecStart,
   environment files and KillMode/cgroup membership. Record branch/HEAD/session/task
   IDs and any in-flight action journal. Let active local/remote agents and automatic
   coding/review corrections drain; confirm no RUNNING/UNKNOWN/RESERVED launch remains
   with fresh reports on each worker. If that cannot be proved, keep deployment held
   and inspect the worker; never kill an uncertain agent to satisfy this precondition.
2. Stop only the identified WORK bridge, then its MCP service after draining. Verify
   neither unit can launch work and no other bridge writer is running. Back up the
   current live code, registry, secret/environment files, actual unit definitions
   and the entire local state directory (including tasks/archives, bridge state,
   requests, locks/leases and outbox). Back up FIREFLY remote task metadata/logs/receipts
   too. Record backup paths and file hashes. Preserve these snapshots for rollback.
3. Install the exact owner-reviewed code/artifact via the existing deployment process
   into the live checkout; preserve registry, secrets and state. Confirm the registry's
   `devorchestrator` entry still points to `~/DevOrchestrator-dev`. Apply WORK enrollment,
   exact WORK hostname, dedicated `work:queued` queue and identical bridge/MCP capacity
   settings. Disable only the identified HOME legacy consumer with explicit approval;
   retain its prior settings
   in the backup. Keep HOME unenrolled. Do not start a second bridge.
4. In the installed runtime, run the full unittest suite, compilation and diff check.
   Start the identified MCP service first, verify health/registered project identity,
   and run `python mcp_smoke.py` with the installed interpreter while TaskBridge is
   stopped. Require `local_control_plane=OK`, `ok=true`, and state READY or DEGRADED.
   DEGRADED with only FIREFLY unavailable is acceptable; record its diagnostic and
   keep FIREFLY-dependent work subject to the existing worker availability/hold rules.
   WORK failure, MCP failure or invalid health output is fatal. Require exit status 0,
   `WORK_HEALTH: PASS` and `TASK_LIST_TRANSPORT: PASS`, reconciling the
   decoded task count/identities with the inventory, before starting the WORK bridge.
   Failure keeps the bridge stopped and admissions frozen. Check migration/quarantine
   outcomes, PID holds, request receipts, labels and outbox; confirm no duplicate
   agents/publication/terminal notices.
   Keep ordinary admissions frozen until startup/reconciliation is healthy.
5. Run the disposable production E2E checklist above with separate authorization for
   each publication/reboot step. Record evidence and the current state snapshot.
   Reopen ordinary submissions only after acceptance. A failed check triggers the
   rollback ordering below; no release is implied by this audit.

Rollback after any code has run:

1. Freeze submissions/direct starts/owner commands again. Account for any launched
   workers and Git/GitHub mutations using fresh reports and action journals. Drain
   work, then stop the identified bridge and MCP as above. Snapshot the **post-rollout**
   state and remote receipts before changing code. If a worker/action is uncertain,
   keep the system stopped/held for inspection rather than replaying it on old code.
2. Restore the saved code/runtime and original environment/unit definitions while
   stopped; keep HOME disabled/unenrolled. Preserve post-rollout task metadata,
   receipts, journals and outbox. Do not rewind state after any agent/publication has
   started: a pre-rollout snapshot could erase the only duplicate-protection receipt.
3. Check whether the saved runtime understands all current receipts, UNKNOWN holds,
   quarantine and publication state. An older bridge that lacks these safety features
   must stay stopped; rollback to that code is safe only as a disabled installation
   until forward repair or a reviewed offline reconciliation is available. Do not
   hand migrated state to an unverified legacy consumer.
4. If deployment failed before **any** new code executed and no workers/external
   actions ran since the backup, restoring that matching code/state snapshot together
   is safe. Otherwise reconcile forward from the post-rollout snapshot, preserving
   completed Git/GitHub actions; code rollback does not undo commits/pushes/PRs/merges.
5. After state/runtime compatibility is verified, start MCP and require WORK/local
   health OK (READY or DEGRADED with explicit remote diagnostic) and the installed
   task-list transport smoke PASS before starting the single WORK bridge. Re-run
   health/reconciliation and disposable checks before reopening
   submissions. Keep all backups and audit evidence. No automatic cleanup, receipt
   deletion, branch reset, inverse publication or live release belongs to rollback.

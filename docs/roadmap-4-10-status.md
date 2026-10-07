# DevOrchestrator roadmap 4–10

All seven phases are implemented in the development worktree. The 2026-10-06
pre-rollout audit passes after the hardening described below. No live checkout,
service, task bus, Telegram chat, or publication was changed during development.

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
installed MCP transport, FIREFLY SSH/network/reboot behavior, real Telegram delivery,
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
   Start the MCP/WORK bridge using their actual installed units; inspect startup logs
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
   then start the WORK bridge. Check migration/quarantine outcomes, PID holds, request
   receipts, labels and outbox; confirm no duplicate agents/publication/terminal notices.
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
5. Start MCP then the single WORK bridge only after state/runtime compatibility is
   verified. Re-run health/reconciliation and disposable checks before reopening
   submissions. Keep all backups and audit evidence. No automatic cleanup, receipt
   deletion, branch reset, inverse publication or live release belongs to rollback.

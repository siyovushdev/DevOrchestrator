# Automation AI V1 — release candidate integration report

**READY_FOR_INDEPENDENT_REVIEW** for the isolated engineering candidate.
No production-readiness claim. Full independent release acceptance and owner
publication/deployment gates remain open.

Candidate: `/home/siovush/DevOrchestrator-v1-rc-20261010` — separate Git/object
store, detached, no remotes, no new source commit. Baseline HEAD `adab547` is
context only: the required Safety Latch working-tree files are not in that commit.
Use the complete candidate tree manifest and its SHA256 in
[evidence/verification.json](evidence/verification.json) as delivery identity.

## Integrated scope and source provenance

| Source | Inspected HEAD/content | V1 disposition |
| --- | --- | --- |
| Production | `6de216192084b9902c73f5a81e1965d1d9b5c7a1`, clean | Read-only baseline; previously committed cleanup/no-op fixes inherited through ancestry. |
| Integration | `d447ebc6e88531dc003665874ad9cfd498d5d0b4`, clean | F1/F2/F3, #47 semantic durability, worker/request recovery, TaskBridge/resumable single writer, Chief delivery fix and tests retained. |
| Acceptance | `adab54726e7427d83cf42d9241c570ec619dceec` plus 16 tracked edits and 21 untracked files | Exact 124-file frozen snapshot; all 17 guarded hashes verified; Safety Latch retained. |
| Other worktrees | dev semantic durability dirty source, review-fix F1/F2/F3 dirty source, clean P0 cleanup/write branches | Inventoried and preserved. Relevant completed changes already represented in integration/acceptance; no older overlapping source replay or V2 work. |

Acceptance already contains every integration file and Chief fix; 87/103 tracked
integration files match byte-for-byte, and the other 16 contain additive Safety
Latch/config-ignore changes. Therefore integration used exact accepted content,
not a speculative merge or cherry-pick. Source attribution: 35 frozen files equal
production baseline, 52 equal committed integration/Chief content, 37 are Safety
Latch remediation/evidence. Detailed per-file hashes and tracked/untracked origin:
[source-change manifest](evidence/source-change-manifest.json).

All 21 untracked inputs were copied: `external_effects.py`; seven acceptance test
modules (`test_acceptance_archived_merge`, `effect_gate`, `effect_latch`, `fsync`,
`migration_probe`, `report_latch`, `test_safety_latch_final`); and 13 acceptance
report/plan/probe/evidence files under `docs/acceptance/`. The manifest enumerates
exact names, including those whose abbreviated test names above share the
`test_acceptance_` prefix. No required untracked file was omitted.

Runtime intersects Safety Latch in bridge_effects, control_loop, devctl, mcp_server,
mcp_smoke, notifications, operational_state, publication, remote_worker,
runtime_liveness, scheduler, task_bridge, telegram_setup, transition_executor and
worker_runtime. All these files and the new external_effects module retain frozen
accepted bytes. github_transport is unchanged and remains in the guarded manifest.

Candidate-specific changes: `.gitignore` excludes `.rc-lab/`; current PLAN/PROGRESS
replace historical copies archived under docs/v1-rc; `tests/test_operational_stability.py`
fixes two explicit cleanup fixture premises; new docs/v1-rc contains reports,
independent review/evidence and reproducible verification tooling. 120/124 frozen
files retain exact bytes. Runtime code was neither redesigned nor rewritten.

## Exact verification results

| Check | Final result |
| --- | --- |
| Complete candidate regression | **737 PASS / 277.412s**, exit 0; 737 unique IDs match discovery; 0 failures/errors/skips. |
| Expanded targeted regression | **136 PASS / 53.459s**, exit 0; 136 unique IDs; 0 failures/errors/skips. |
| Independent Chief + Safety Latch integration review | **106 PASS / 58.012s**, ACCEPTED; 0 failures/errors/skips. |
| Independent final fixture + runner review | **14 PASS / 2.418s**, ACCEPTED; 0 failures/errors/skips. |
| Compilation/static checks | 90 Python files compile; tracked/untracked whitespace and conflict checks PASS. |
| Guarded source identity | 17/17 exact accepted SHA256 matches. |
| 60-second idle measurement | 60.016s, 17 writer steps, 1 discovery/5 reports; authoritative replacements/bytes **0/0**. |
| 60-second deferred-notification measurement | 60.019s, same steps/reads; authoritative **0/0**; PENDING notification/payload unchanged. |

These suites overlap; their counts are not added. Final runs use Python 3.12.3,
MCP 2.3.0 and 29 pinned installed distributions in dependency-lock.txt.
[Full evidence](evidence/full-final/result.json),
[targeted evidence](evidence/targeted-final/result.json),
[independent Chief decision](evidence/independent-chief/decision.json),
[independent follow-up decision](evidence/independent-chief/followup/decision.json).
Complete raw logs, command argv, inventories and isolation proofs accompany them.

Tests run with read-only source, private filesystem/PID/network namespaces,
initially empty HOME/TMPDIR and mocked external transports. Production/config/state,
credentials, machine registry and host MCP are absent. Real MCP concurrency uses
an ephemeral listener inside private loopback; real processes and Git commits are
only disposable test fixtures. No real GitHub/Telegram/provider publication.

Both measurement state files retained content/inode/mtime. Telemetry writes were
10 replacements/7,055 bytes and 16/16,435 bytes; process write_bytes were 40,960
and 65,536. These are integrated lab measurements, not production measurements.

Historical acceptance: 737 PASS / 320.331s and 55 targeted PASS / 9.773s are
backed by verified archived logs. Controlled smoke: 9 PASS / 1.559s, verified
execution/result artifacts. Historical 173 independent PASS and final
SMOKE_ACCEPTED are owner-attested: no separate reviewer record is present.
They are not substituted for the new independent Chief review or RC regression.

## Investigated failures and minimal corrections

First complete run: 737 tests / 225.764s, 735 PASS and two failures, no errors/skips.
The isolated filesystem omitted `/etc/alternatives/awk`; the real fingerprint
index pipeline returned 127 (`awk: command not found`). Runner now exposes only
the resolved awk binary read-only at that target. Application code is unchanged.

The unrecorded-survivor test assumed an exit marker meant the wrapper was gone.
The marker is durable before supervisor exit, and a live matching wrapper is a
valid ownership anchor. That case now waits/reaps only its exact disposable
wrapper before removing receipt members, retaining AMBIGUOUS/no-signal assertions.
The identity-mismatch subcase explicitly models a current matching leader for the
exact fixture PID; other PID reads and real orphan/subgroup cleanup remain real.
After leader exit an exact recorded child still proves ownership under the
accepted contract. No worker cleanup runtime change was justified or made.

An intermediate broad shared-fixture correction failed the live-leader premise
in independent and targeted review; it was superseded by the narrow corrections.
Second complete run on the superseded fixture: 737 / 305.105s, one failed identity
assertion. Initial/intermediate failures, 118/120-case exploratory passes and
135/136-case failed targeted run are retained as history. Final 136/737 PASS and
independent 14 PASS alone validate final changed test bytes. Nothing was hidden,
excluded or skipped. [Architectural review](architectural-review.md) records why
these were verification defects rather than application safety failures.

## Safety, preservation and remaining risks

Single writer, fatal durable checkpoints, immutable request IDs/arguments,
generation-bound worker lifecycle/cleanup, uncertain ownership, restart recovery,
exact receipt idempotency, fresh Chief/owner/publication gates, sticky issue holds
and effect quarantine remain intact. Removing the environment latch does not
release holds/tombstones; historical SENDING/UNCERTAIN is never reset for retry.

Original seven worktrees retain HEAD/status/file sets/hashes and branch/worktree
inventories. All 3,290 production files including Git metadata and both config
files remain unchanged. Of 1,307 live-state files, 1,306 are byte-identical; the
existing active production bridge changed only task_bridge_state.json during this
task. Read-only service/script/lock evidence identifies that preexisting writer,
started Oct 9; RC test namespaces cannot access its files. No writes to live state,
service restart, worker signals or live transports were performed by this task.
Whole-interval byte immutability of an actively running live state is **not**
claimed; the literal hash drift is preserved in the audit, never labelled an
unchanged state or a consistent recovery backup.
[Preservation audit](evidence/preservation-final.json).

No confirmed release-blocking integration defect remains. Known nonblocking F4
stale discovery cache ordering remains separate; durable closure/archive and fresh
launch/publication boundaries still fence effects. Real-provider/SSH/network soak,
MCP/worker rollout compatibility, operational handling of old ambiguous holds and
full independent production acceptance remain outside this engineering task.
Any hold/quarantine recovery requires explicit per-item owner authority.

## Remaining gates and exact next release action

Next: independent **full-candidate release acceptance** of the manifest-identified
workspace/artifact, full diff plus untracked inputs, logs and safety invariants.
Chief fix already has genuine independent technical approval. After acceptance,
explicit owner authorization is still required for source commit, push, PR, merge,
production acceptance window, deployment/restart and admission/hold recovery.
No one of these approvals implies the others. No source commit or remote operation
was performed, and no unrelated V2 milestone was started.

[Release/rollback instructions](release-and-rollback.md) include reproducible
isolated verification, exact SHA/hash checks, fresh compatible recovery backups
and conservative receipt/state handling. Stop here: do not publish or deploy.

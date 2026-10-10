# V1 RC progress — 2026-10-10

## Inventory and candidate construction

- Production HEAD `6de216192084b9902c73f5a81e1965d1d9b5c7a1`, clean;
  integration HEAD `d447ebc6e88531dc003665874ad9cfd498d5d0b4`, clean;
  acceptance HEAD `adab54726e7427d83cf42d9241c570ec619dceec`, 16 tracked edits
  and 21 individual untracked files. Seven existing worktrees recorded in
  `docs/v1-rc/evidence/baseline.json`. No applicable AGENTS.md found.
- Acceptance contains every integration file. Exactly 16 tracked paths differ;
  Chief tests and other integration files are identical. Chief implementation
  remains in both changed runtime paths; independent reviewer invoked.
- Acceptance and frozen copy match all 124 hashes/file set and 17 guarded hashes.
  Preparation artifacts and historical logs verified. Historical full: 737 PASS /
  320.331s; targeted: 55 PASS / 9.773s; smoke: 9 PASS / 1.559s. Independent
  173 PASS and final SMOKE_ACCEPTED are owner-attested; separate reviewer record
  is not present. Historical results are not new candidate verification.
- Created independent Git clone/object store, no remotes, detached at acceptance
  HEAD. Copied exact frozen files including untracked modules/tests/docs.
  No source worktree/index/branch changes. Prior integration PLAN/PROGRESS archived.
- Baseline includes production files/Git metadata, live state and local config.
  Saved only hashes/path metadata, never secret/state content. Tests use synthetic
  fixtures. Copied acceptance-only venv into candidate.

Next: isolated targeted/full tests, independent review, architectural review,
final preservation and release handoff. These engineering actions are authorized.

## Targeted verification and independent review

- Fresh RC targeted run: **118 PASS / 48.058s**, exit 0, no skips/errors/failures;
  118 discovered and unique executed IDs. Read-only source, private PID/network
  namespaces, empty synthetic HOME/TMPDIR, production invisible, host MCP
  unreachable. Python 3.12.3, MCP 2.3.0. Complete logs/inventory/isolation evidence
  captured in `.rc-lab/targeted-9a58100c/evidence` and will be archived for handoff.
- Independent read-only reviewer **ACCEPTED** Chief fix and combined Safety Latch:
  **106 PASS / 58.012s**, exit 0, zero skips/errors/failures. 82 independently
  compared runtime/test/Chief-doc files match acceptance and remain unchanged.
  Additional adversarial checks cover lock release after POST, tree-check refusal
  and tampered quarantined Chief status after latch removal. Review was performed by
  the separate reviewer agent `/root/independent_chief_review`; no claim of owner
  release acceptance. Reviewer-owned artifacts copied under
  `docs/v1-rc/evidence/independent-chief/`.
- No confirmed runtime integration defect. Exact source composition requires no
  code rewrite. All frozen Python file hashes still match; compilation of 85
  source/test/verification files passed before importing reviewer artifacts.
- `.gitignore` now excludes candidate-only `.rc-lab/`; only engineering docs and
  reproducible verification tooling differ from the frozen input.
- Full 737-test run is in progress. Two isolated final-candidate 60-second idle
  and deferred-notification measurements started; authoritative zero-write
  assertions retained from the source measurement tool.

## Full-run findings and minimal correction

- First complete RC run: 737 executed / 225.764s; 735 PASS, two failures, zero
  errors/skips. Preserved the whole run under `evidence/full-initial-failed/`.
- Git baseline failure root cause: sandbox exposes `/usr/bin/awk` symlink but
  not its `/etc/alternatives/awk` target. Captured real snapshot index pipeline
  returning 127 with `awk: command not found` in `environment-root-cause.json`.
  Runner now binds the resolved awk binary read-only at that one target; no host
  configuration directory or production path is exposed. Runtime code unchanged.
- Cleanup failure root cause: orphan fixture waited for durable exit_code, which
  precedes actual supervisor termination. A still-live generation-bound wrapper
  is a valid ownership anchor even after fixture clears receipt members, so a
  legitimate SIGTERM contradicts the test's orphan premise. Fixture now waits/reaps
  only its exact disposable wrapper and asserts absence before testing unrecorded
  survivors. All no-signal assertions and cleanup runtime logic remain intact.
- Only accepted Python diff is the eight-line synchronization correction in
  `tests/test_operational_stability.py`; new candidate-python-manifest records
  both acceptance and candidate hashes. Runtime and all 17 guards are unchanged.
  Independent reviewer requested to verify root cause and changed fixture.
- Targeted rerun expanded from 118 to 120 cases, including both failed cases;
  complete rerun will follow final correction. No failure is excluded or skipped.
- Preservation audit: production's 3,290 files (including Git metadata), two
  config files and every original worktree HEAD/status/source hash are unchanged.
  1,306/1,307 live-state files are unchanged; task_bridge_state.json drifted while
  the preexisting production bridge continues running. Read-only systemctl, exact
  script cmdline and bridge.lock ownership identify PID 1541746, started Oct 9.
  No source/state/service mutation by this task; test namespaces have no host-state
  mount. Literal whole-interval byte immutability of active live state is not
  asserted. Evidence: preservation.json, background-writer-observation.json.

### Independent fixture review correction

The independent reviewer found that applying wrapper reaping to every orphan
fixture changes another test's live-leader identity-mismatch premise. A recorded
surviving child remains a valid ownership anchor after leader exit; this is the
accepted runtime contract, not a new cleanup bug. Moved exact wrapper wait/reap
into only `test_unrecorded_survivors_fail_closed`, which specifically needs no
live leader anchor. Other fixtures remain as accepted. The no-signal assertion
is intact. Intermediate verification against the broader helper correction is
historical; final focused/full runs must validate this narrower test-only diff.

The identity-mismatch case also relied on the wrapper still being observable.
Made only its wrapper_identity subcase deterministic with a matching current
leader observation for the exact fixture PID, delegating all other PID reads to
the real implementation. A mismatching receipt must still return AMBIGUOUS and
never signal; real orphan/no-anchor and subgroup cleanup tests remain unchanged.
This expresses the accepted documented live-anchor contract, rather than treating
a valid surviving child anchor as unknown. Reviewer independently identified this
fixture precondition. No worker/lifecycle runtime change was made.

## Final fixture and targeted verification

- Final expanded targeted suite: **136 PASS / 53.459s**,
  exit 0, no skips/errors/failures. Includes all 10 OwnedCleanupTests, all eight
  WorkerIntegrationTests and the prior latch/Chief/recovery/request/publication
  suites. Exact inventory and complete log archived in evidence/targeted-final.
- Fixture changes are limited to the unrecorded-survivor wrapper termination
  precondition and deterministic live-leader observation for wrapper-identity
  mismatch. Runtime and guarded hashes remain identical to accepted Safety Latch.
- Third full candidate run started on these final test bytes. The second run
  began on the broader, superseded fixture and is retained as intermediate
  diagnostic evidence, not candidate validation. No failure/skip is omitted.
- Installed dependency inventory: 29 distributions saved in dependency-lock.txt.

## Revised independent approval

- Independent reviewer accepted final cleanup fixture corrections and awk mount:
  **14 PASS / 2.418s**, exit 0, zero errors/failures/skips. Includes all ten real
  OwnedCleanupTests, baseline fingerprint and three reviewer-owned anchor probes.
  Earlier 106-case Chief/Safety Latch approval remains valid: all runtime bytes
  unchanged. 81/82 original compared files remain identical; the sole altered
  test file hash is a58f2e7221a4260e4a674a74dc278271c487d55882ec76fcc99156a1dfe4bf1b.
- Updated independent evidence copied under independent-chief/followup. Earlier
  rejected broad fixture experiment is preserved in that evidence with its
  root cause and correction; final approval is explicit, not agent self-review.

## Final complete regression and engineering delivery

- Final full suite: **737 PASS / 277.412s**, exit 0, 0 failures/errors/skips.
  All 737 unique executed IDs match discovery; full captured log, result, exact
  bwrap command and isolation proof are in evidence/full-final. Python 3.12.3,
  MCP 2.3.0; real private-loopback MCP concurrency and disposable dummy-worker
  execution/recovery tests ran. Runtime/test hashes unchanged throughout final run.
- Final targeted: 136 PASS / 53.459s, 0 failures/errors/skips. Independent
  Chief/latch 106 PASS / 58.012s and final fixture/runner 14 PASS / 2.418s, ACCEPTED.
- Both final-candidate measurements: 60.016s idle and 60.019s deferred, 17 writer
  steps, 1 discovery/5 reports each, authoritative writes/bytes 0/0; content, inode,
  mtime unchanged. Deferred effect stayed PENDING with identical payload. Telemetry
  10/7,055 bytes and 16/16,435 bytes; process writes 40,960 and 65,536 bytes.
- Compilation: 90 Python files PASS; whitespace/conflict/git diff checks PASS.
  All 17 guard hashes retained. 120/124 frozen input files preserve exact bytes;
  only .gitignore, current PLAN/PROGRESS and reviewed cleanup test changed. All
  21 required untracked acceptance inputs present. Historical plans archived.
- Original seven worktrees' HEAD/status/file sets/hashes, original branch/worktree
  inventories, production 3,290 files including Git metadata and two config files
  unchanged. Known external drift is only live task_bridge_state.json from the
  preexisting active bridge; remaining 1,306 state files unchanged. No live state
  writer, transport, service or worker operation was invoked by this task.
- Source change manifest, independent evidence, failed/intermediate/final logs,
  focused architectural review, release/rollback instructions and final report
  prepared. Candidate identity is the frozen file manifest, not baseline HEAD.
- No confirmed release-blocking integration defect remains. Full independent
  release acceptance and owner commit/push/PR/merge/production activation gates
  remain. No source commit, publication, restart or deployment performed.

Engineering task complete; stop after delivering candidate and evidence.

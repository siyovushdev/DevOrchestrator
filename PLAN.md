# Integration plan: F1/F2/F3 + semantic durability (#47)

Scope: this integration worktree only. Sources are read-only. No production,
live state, service/configuration changes, commit, push, PR, merge or restart.

## Invariants

- One lifecycle writer; detached I/O only receives copied commands/results.
- Preserve resumable phases, immutable request IDs/arguments, receipts, locks,
  sandbox, independent review, Chief, owner and publication boundary gates.
- Durable checkpoints before dispatch; ambiguous acceptance retains ownership.
- Runtime observations/timings and deferred notification display do not rewrite
  authoritative state; optional telemetry cannot influence recovery or gates.
- Legacy migration is fail-closed; restart reconciles without duplicate launches.

## Stages

1. [x] Inspect both tracked diffs and all untracked files; map intersections.
2. [x] Apply F1/F2/F3 and transfer #47 with manual integration at intersections.
3. [x] Run transferred focused tests; fix compatibility and combined-path defects.
4. [x] Add focused F3 + semantic durability regressions, including migration,
   writer failures, suspended transitions, send receipts and restart deduplication.
5. [x] Run full regression without skips, compile and diff checks; reproduce
   wall-clock write amplification on final tree, baseline and fixed.
6. [x] Self-review final diff, verify sources unchanged, document evidence,
   complete change inventory and remaining operational risks.

Record commands, results and decisions in PROGRESS.md at each checkpoint.
No production-ready claim; acceptance/rollout is a separate task.

Completed: 669 tests PASS in 308.263s, no skips. Four final-tree measurement
windows and source invariants are recorded in docs/integration-evidence.
Ready for independent review; full inventory and remaining risks are in
docs/integration-report.md. The follow-up below supersedes the initial handoff.

## Independent review follow-up: Chief delivery lock contention

1. [x] Reproduce/analyze SENDING -> UNCERTAIN after pre-dispatch lock refusal.
2. [x] Add explicit no-send proof at lock acquisition; preserve ambiguity handling.
3. [x] Add real flock, retry/restart, receipt and semantic-durability regressions.
4. [x] Run focused and complete regressions; fix findings and self-review.
5. [x] Record final evidence/report; stop for independent verification.

Follow-up complete: 108 focused PASS; 677 full PASS in 263.395s, no skips.
Current-tree idle/deferred measurements both retain zero authoritative writes.
Ready for independent verification; docs/chief-delivery-fix-report.md records
the no-send boundary, legacy uncertain holds, evidence and remaining limits.
No additional test run or operational action is planned.

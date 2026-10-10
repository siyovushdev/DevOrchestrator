# V1 RC release and rollback instructions

This engineering handoff authorizes no publication or production operation.
The candidate is a dirty detached independent repository, not a release commit.
Its HEAD `adab54726e7427d83cf42d9241c570ec619dceec` lacks the Safety Latch
working-tree files. Never deploy HEAD alone or use its SHA as candidate identity.

## Exact next action: independent full-candidate acceptance

Review `/home/siovush/DevOrchestrator-v1-rc-20261010`,
`evidence/candidate-tree-manifest.json`, `evidence/source-change-manifest.json`,
complete regression logs/inventories and `evidence/independent-chief/decision.json`.
Verify manifest SHA256 against `evidence/verification.json`, then verify every
listed file before reviewing the full production-to-candidate diff and untracked
files. Reproduce the anchored read-only check with:

```sh
python3 docs/v1-rc/verify_candidate.py --expected-manifest-sha256 <handoff-manifest-sha256>
```

Use the exact manifest SHA256 in evidence/verification.json or the artifact
checksum sidecar; this command writes nothing. The Chief fix has independent technical approval; full V1 release
acceptance remains pending. An owner may request another read-only reviewer
without enabling any effect or changing production.

Authorized synthetic replay from candidate root:

```sh
python3 docs/v1-rc/run_verification.py targeted
python3 docs/v1-rc/run_verification.py full
python3 docs/v1-rc/run_verification.py idle
python3 docs/v1-rc/run_verification.py deferred
```

The runner uses copied acceptance dependencies (Python 3.12.3, MCP 2.3.0), bwrap,
private PID/network/filesystem namespaces, read-only code, empty HOME/TMPDIR and
mocked transports. No production paths or registry are mounted. Full tests use
an ephemeral private loopback MCP listener and disposable dummy processes/Git
repositories. Fixture commits are synthetic test behavior, not source commits.
The runner fails on missing dependencies, skips, source mismatch or timeout.
Evidence is written only under ignored `.rc-lab/`; replay does not release holds.

## Owner gates after independent acceptance

1. Explicit owner authorization to create an RC commit in this candidate only.
   Review all source-change-manifest paths, including the test-only cleanup fixture correction and 21 previously untracked
   acceptance files. Include the new guard module, seven acceptance test modules,
   acceptance docs/evidence, PLAN/PROGRESS and V1 handoff docs. If raw evidence
   logs are included in the commit, explicitly stage them despite `*.log` ignore.
   Keep venv, `.rc-lab`, state, credentials and machine registry out of the commit.
   Record the resulting commit and its committed tree; compare code/guard hashes
   again. Candidate manifest intentionally excludes itself and final verification
   summary to avoid recursive hashes; their external SHA256 anchors are recorded.
2. Separate explicit owner authorization for push, PR creation and merge. None
   is implied by engineering completion or by authorization for a commit.
3. Independent acceptance of the exact published SHA and an owner-approved
   production acceptance/rollout window. Validate observation endpoints and
   request/capability contracts on every deployed MCP/worker component, dependency
   compatibility and current workflow-state migration using copied state first.
4. Explicit owner deployment/restart authorization. Follow the reviewed
   `docs/acceptance/ROLLOUT_PLAN.md` with its final Safety Latch addendum and exact
   guarded hashes. The older PENDING-for-new-smoke statement is superseded:
   newly latch-origin effects remain QUARANTINED, issue holds are sticky.
5. Admit normal work or declare production readiness only after independent
   production acceptance and owner's approval. Any issue-hold/quarantine recovery
   needs a separate per-item authority audit and explicit approval. Environment
   latch removal, polling, effect status reset and lock release do not supply it.

## Rollback preparation and execution gate

No rollback is performed by this task. The existing production source remains
`6de216192084b9902c73f5a81e1965d1d9b5c7a1`; local configuration/live data remain
outside the candidate. At the future authorized rollout window, take fresh exact
backups of code, registry/config, authoritative state, request/worker receipts,
locks and publication/outbox journals before any operator action.

If acceptance fails before deployment, reject the candidate and leave production
as it is. Preserve candidate/evidence for repair; no cleanup or Git reset is
needed. After a deployment, rollback requires owner authorization and the reviewed
operator procedure: pause admissions through the sanctioned operational controls,
retain ownership for all active/unknown workers, inspect dispatched request IDs,
exact receipts and journals, then restore the approved compatible code artifact.
Do not blindly restore old state, clear SENDING/UNCERTAIN or remove observation
holds/quarantines. Accepted external operations may have occurred after a backup;
stale state can replay them. State restoration or backward schema migration needs
independent review; if compatibility cannot be proven, keep effects held and use
forward repair. Never signal or restart live workers as an inferred rollback step.

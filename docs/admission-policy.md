# Durable admission v1

Global `DEVORCHESTRATOR_BLOCK_EXTERNAL_EFFECTS=1` is an unconditional emergency stop. No grant, owner quality decision, issue, generation or request ID overrides it. Removing this environment latch grants no admission, publication or maintenance rights.

`DEVORCHESTRATOR_ADMISSION_POLICY` selects a private canonical JSON policy (0600, controller UID). It has exactly `schema_version: 1`, `state_dir`, `grants_dir`, `workspace_root`, and `registry_file`. Directories must be private owned directories; the registry is a private owned file. State, policy, grants and registry cannot overlap the mutable workspace root. Native runtime must use the same state/workspace roots and keep runtime scratch and authentication outside all admission resources. MCP and worker CLI load the same registry. No live configuration is migrated by importing this module.

Default is DENY when policy/grant is absent, malformed, duplicated, noncanonical, ambiguous, expired, not yet valid, or identity/generation does not match. Allowed roles are exactly native local Codex coding and independent Codex review (`gpt-6.1-sol/high`, codex-high/codex-review). Explicit auto/model/agent fallback, continuation, adoption, arbitrary shell, legacy launches and bulk/remote maintenance have no grant. Historical receipts and metadata cannot acquire permission without the original admission claim.

## Owner-file contract

A grant is a private controller-owned `<coding-request-id>.json` under grants_dir. Its exact fields are enforced in `admission_policy.grant_for`. It binds workflow_id, namespace/issue, project/machine, exact canonical workspace, standalone Git identity (root, .git device/inode/UID and HEAD), registry digest, transformed coding prompt SHA256, exact coding/review request IDs, roles/agents, finite limits/expiration, and explicit owner provenance `{kind: owner_local_file, reference: ..., approved: true}`. Both request IDs use the existing native algorithm (`initial`, `review:1`); the coding task ID is coding_request_id[:24]. No wildcard permission exists.

Owner provenance means an explicitly reviewed artifact installed by the trusted controller owner. A text reference is **not** a cryptographic signature or independent proof of ChatGPT origin. No issue, MCP endpoint, agent, worker or test promotes a draft to an approval. This assumes a trusted host/controller UID; it does not isolate mutually hostile unsandboxed processes running as that UID.

`admissionctl.py` creates only a private **UNAPPROVED draft**, outside state/grants/workspaces, from an exact private native issue JSON and the shared policy. It never sets approved=true, installs a grant, creates a task, changes a service or launches a worker:

```bash
python3 -I -B admissionctl.py \
  --policy /ABSOLUTE/PRIVATE/admission-policy.json \
  --issue-file /ABSOLUTE/PRIVATE/issue.json \
  --namespace OWNER/EXACT_TASK_REPOSITORY \
  --owner-reference PENDING_OWNER_GO \
  --output /ABSOLUTE/PRIVATE/DRAFTS/admission-draft.json
```

The owner reviews the complete scope, transformed prompt, native issue and file hash before a separately authorized artifact installation. Do not use illustrative paths/references as real authority. `-I` requires running this CLI as the script from its installed directory; Python includes the script directory for direct script imports only when -I is not used, so an installed wrapper must set its own trusted module path (this script does so).

## Filesystem boundary

The approved workspace is strictly below an explicit private root, canonical, owner-controlled and not group/other-writable. It must be a standalone initialized Git repository with a private local baseline, no remotes, no linked/shared .git, no symlinks/special files/hardlinks, no active hooks or executable Git configuration. Native coding has workspace write and .git read; independent review uses a separate immutable generation-bound snapshot with native read-only permissions. Tools have network disabled, root denied and minimal system reads; credentials, grants, controller state and runtime remain inaccessible. Codex itself uses existing ChatGPT authentication; tokens are neither copied nor exported to tools.

## Durable lifecycle

Admission checks run before bridge queue reservation, detached launch dispatch/transport, MCP start, worker launch/receipt and native preparation. A per-request RESERVED claim is fsynced before preparation. A generation-bound DISPATCHED claim precedes supervisor creation; the supervisor atomically consumes it to EXECUTING before allowing exec. The native pidfd gate performs a fresh authorization check before opening the exec gate. Duplicate supervisors cannot consume the same claim. The exec gate and each watchdog callback compare the newly validated full ticket to the consumed ticket and recheck EXECUTING status, generation, boot, grant digest and the original deadline; a changed valid grant is not interchangeable authority. Claims retain boot identity and a monotonic overall deadline; review inherits the coding overall deadline. Cleanup reserve is included inside the hard budget. Limits are at most 480s per role/1200s overall, existing contract/validation limits remain 30s/60s, TERM/KILL grace 5s each, policy/worker claim locks have a finite 5s acquisition bound. Mutable repository traversal is lazy and shares a 3s validation budget with all Git probes; native supervisor validation is additionally clamped to the role deadline.

Crash after reservation/dispatch/consumption, missing receipt, changed contract, boot/generation conflict or UNKNOWN cleanup never permits an automatic second execution. Receipt versions remain coding v4/review v3; existing signature and request replay contracts are unchanged. Only current authorized native terminal generation may update its metadata/lease from proven pidfd cleanup. Boolean admission predicates return DENY when latched, including startup with populated legacy state and admitted-task observation. The retired runtime_liveness shell supervisor, archive/unarchive and lifecycle administration cannot execute or mutate state under this two-role grant. Reads never trigger bulk legacy reconciliation; busy nested worker-lock reads do not wait or grant authority. Legacy queues/holds/quarantine/decisions remain denied and retained.

## Publication and Chief/owner boundaries

Execution admission carries **zero** delivery/publication permissions. This candidate intentionally denies all automatic GitHub mutation, Telegram delivery and commit/push/PR/merge transport, including old pending journal/outbox effects and direct transport entrypoints. Pending effects are retained; admission and existing Chief/owner decisions cannot reauthorize them. Implementing or enabling a separate publication/delivery capability is outside this change and requires separate engineering/authorization. Read-only repository snapshot/evidence extraction is separate from publication.

The existing manual ChatGPT Chief identity/verdict parser and owner quality gates are preserved. A completed independent review stops at native AWAITING_CHIEF_REVIEW, with local evidence represented in the Chief bundle/outbox. No delivery receipt or complete Chief proof is forged to bypass publication denial. Owner-mediated handoff/intake must use genuine source/provenance under the existing contract; synthetic intake tests are explicitly fixtures, not real decisions. APPROVE/REJECT remains a quality disposition without commit/push/merge permissions.

Deploy only with emergency latch ON. Verify empty/default-deny grants and protected state before granting any new scope; changing latch state and executing a real task require separate owner gates. No task is enrolled by migration or service startup.


Repository observation and review preparation reject trees above 2048 entries, 8 MiB per file or 32 MiB aggregate (including ignored files and .git). Hashing/copying is streamed and identity-checked, with a 30-second preparation/observation deadline clamped to the existing admission budget. Git probes use a minimal environment and absolute executable, disable ambient configuration/hooks/fsmonitor and reject executable local filters/includes/diff helpers. Preparation failures preserve UNKNOWN and a durable stop fence; no STARTED review receipt or recovery launch is created.

Capacity and leases of unrelated historical tasks are observational only. Unadmitted uncertain cleanup occupies capacity; even a terminal historical lease cannot be replaced by a new execution grant. Owner forward repair/resource assessment is a separate prerequisite where these reservations block new work. Current native-generation reconciliation and liveness waits are finite and generation-bound. Command configuration probes inherit preparation deadlines and reserve their own identity-verified cleanup time.

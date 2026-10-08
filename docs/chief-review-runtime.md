# Chief Engineer runtime gate

Autopilot independent review PASS records the existing `reviewed_tree`, coding
turn, and review round. It then waits in `AWAITING_CHIEF_REVIEW`. Only a current
Chief PASS makes the task `READY_FOR_OWNER`; the existing structured owner
commands, authorization, timestamps, baseline checks, worker locks, and
publication journal still govern COMMIT, PUSH, CREATE_PR, and MERGE.

The bridge backfills pending independent-PASS tasks with valid current turn/tree
bindings before processing owner actions. No coding or independent review is
relaunched. Historical COMMITTED/PUSHED/PR_CREATED/MERGED tasks and tasks with an
already-started publication journal retain their existing recovery path. The
now-merged #34 therefore remains historical; pending tasks with its former
pre-Chief shape are upgraded.

## Evidence

The manifest comment exposes the deterministic `bundle_id`, task/session,
project/repository, coding turn, independent round/verdict/summary, reviewed-tree
fingerprint, verification summary, changed-file count, ordered chunk checksums,
and checksum of all evidence. Its chunks contain a changed-file manifest with
per-file diff stats and the complete textual patch relative to the captured HEAD.
Reassemble the text inside each chunk code fence in numbered order, preserving
newlines and excluding the fence lines and appended effect marker. Verify each
payload SHA-256 and the checksum of their concatenation.

Evidence comes from the immutable Git objects created by the existing snapshot,
including nonignored untracked files. Extraction uses the registered project
workspace and literal paths from the tree. Issue-supplied paths are never read.
Binary files expose hashes, modes, and sizes only. Credential paths are excluded
from the patch, and credential values are redacted. Snapshot equality is checked
before extraction, after extraction, and immediately before and after each
comment POST under the existing worker lock.

Comments stay below 50,000 UTF-8 bytes, with 40,000-byte payload chunks. Limits
are 128 chunks, 1,000 changed files, 2 MiB per textual blob, and 8 MiB of extracted
blob contents. Oversized textual evidence holds the gate rather than truncating
its patch. Large binaries retain Git object hash/size metadata. The additive
`dev:awaiting-chief-review` label is provisioned when absent, without overwriting
existing label settings.

The existing durable outbox records every comment effect. Complete evidence
requires exactly one exact-body receipt for every manifest/chunk, with matching
effect identity. A timeout or interrupted POST is uncertain and never
blindly retried. Later exact receipts reconcile it. Missing, edited, duplicate,
or uncertain evidence blocks verdict acceptance. Persisted bundles are reused
across restart without publishing duplicate comments.

## Verdicts and correction

An authorized GitHub account in the existing `DEVORCHESTRATOR_OWNERS` allowlist
must post one complete structured comment, using the visible current bundle ID:

```text
<!-- DEVORCHESTRATOR_CHIEF_REVIEW_V1 {"bundle_id":"<current 64-character bundle_id>","verdict":"PASS","summary":"Reviewed engineering evidence","findings":[]} -->
```

```text
<!-- DEVORCHESTRATOR_CHIEF_REVIEW_V1 {"bundle_id":"<current 64-character bundle_id>","verdict":"CHANGES_REQUIRED","summary":"Correction needed","findings":["Concrete actionable finding"]} -->
```

Only these two verdicts are accepted. Unknown/duplicate JSON fields, stale IDs,
free-form text, missing durable comment IDs, and malformed payloads fail closed.
Summary and each finding are limited to 2,000 characters; at most 32 findings
are accepted. PASS requires no findings; CHANGES_REQUIRED requires findings.
The bridge persists the verdict source, content revision, receipt timestamp,
author, bounded sanitized data, and bundle publication revision/effect IDs.
Identical verdicts are idempotent. A conflicting verdict, or an edited/deleted
accepted source, blocks approval and requires fresh review.

CHANGES_REQUIRED uses the existing durable `continue_task` machinery with a
bundle-specific correction operation and the same coding task/session. The old
Chief record is retained as history. Quality gates are invalidated before the
coding RPC; interruption recovers its original request identity. Completion must
pass a fresh independent review, capture a new reviewed tree, and publish a new
bundle before Chief approval can be granted again. New coding turns and new
independent review rounds invalidate earlier Chief evidence and approval.

Evidence comments and Chief verdicts never authorize publication. No new shell
or filesystem capability is exposed to ChatGPT and no OpenAI API is called.

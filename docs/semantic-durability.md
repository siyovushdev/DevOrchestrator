# TaskBridge semantic durability

`task_bridge_state.json` persists workflow transitions immediately through the
existing atomic, fsynced `write_json_if_changed` path. Its serialization now
excludes the following observation fields at their specific schema locations:

| Location | Volatile fields | Replacement |
| --- | --- | --- |
| Root `timings` | Bounded samples appended by control, discovery, reconciliation, evidence, outbox, Telegram and publication measurements | In-memory samples and optional telemetry sidecar |
| Each issue's coding/review `runtime_health` | `observed_at`, `last_heartbeat_at`, `heartbeat_sequence`, `lease_expires_at` | Latest observations in the telemetry sidecar |
| Each issue's coding/review `runtime_health` | `heartbeat_age_seconds`, `lease_state` | Derived at status-read time; absent from both files |
| Pending Telegram outbox entries | Regenerated `Duration` text based on `notification_report.created_at` and current time | Rendered only in the transient Telegram send buffer |

No global timestamp stripping is used. Runtime classifications, reasons,
generation, phase, process ownership, session/worker identity, start time,
transition sequence and transition time remain durable. Unknown health fields
remain durable conservatively. Repository locks, queue sequences, reservations,
launch receipts, pending RPC arguments, retry deadlines, coding/review/Chief
receipts, owner gates, publication journals and outbox delivery receipts retain
their existing persistence behavior. Retry deadlines and transition timestamps
are required workflow data, rather than idle telemetry.

Final reports and transition alerts may contain a frozen observation as evidence
of that event. They are generated/deduplicated by the existing workflow identity;
new heartbeat observations do not generate new alerts or authoritative writes.

Telegram suppression and eligibility checks are read-only. New queued payloads
omit clock-derived duration, and existing snapshots (including legacy duration
text) remain immutable through backoff and retries. After backoff and fresh
boundary checks, a transient buffer is rendered with the current title, action
and duration for synchronous sends or asynchronous dispatch. SENDING remains
durable before any send; crash uncertainty handling is unchanged. Invitation
freshness gates use semantic workflow state rather than a stale payload header.

`task_bridge_telemetry.json`, beside the authoritative file, holds at most 64
fixed-schema numeric timing samples and 128 latest coding/review runtime
observations. It replaces its snapshot rather than appending history. Issue keys
and numeric values are bounded; runtime identity is a fixed-size SHA-256 digest,
so prompts and provider payloads never enter the sidecar. Status rejects files
over 128 KiB and tolerates missing/corrupt telemetry. Runtime samples are applied
only when their task identity and complete semantic health digest match the
authoritative observation. A cached sample never changes classification,
refreshes `observed_at`, or grants an action. Fresh local read-only worker probes
still take precedence in `devctl status --issue`.

Workflow is persisted before optional telemetry. Telemetry IO failure does not
turn a completed durable transition into a failed RPC or publication operation.
Authoritative persistence errors still propagate. Worker `liveness.json` leases,
generation fences and runtime health transitions are unchanged; those worker
records and fresh probes, not the telemetry sidecar, supply ownership evidence.

Recovery loads only authoritative workflow state and discards legacy volatile
fields in memory. The first subsequent save migrates an old authoritative file
once; subsequent idle saves compare equal. No recovery decision depends on
whether telemetry survived a crash. Timings resume as a fresh bounded in-memory
window after restart.

## Integrated F3 + #47 measurement (2026-10-10)

The final shared writer retains F3's fatal checkpoint boundary and uses this
semantic projection for both load/migration and save. Both synchronous and
resumable outbox interfaces render transient Telegram text after eligibility
and backoff, retaining the original payload and durable SENDING receipt.

`tests/measure_semantic_durability.py` now settles F3 continuations before counting
and uses `ControlLoop.wait()` to wake on I/O completion. It retains the normal
10-second control and 30-second discovery cadence. Each 60-second final window
performed 17 writer steps, one discovery and five fresh runtime reports.

| Scenario | Authoritative replacements / serialized bytes | Telemetry replacements / serialized bytes | Process write_bytes / syscw |
| --- | ---: | ---: | ---: |
| Previous whole-state save | 10 / 22,822,915 | 0 / 0 | 22,855,680 / 30 |
| Integrated semantic save | 0 / 0 | 10 / 7,049 | 40,960 / 10 |
| Previous deferred payload regeneration with semantic filtering | 4 / 9,134,368 | 15 / 14,767 | 9,203,712 / 27 |
| Integrated save + deferred notification | 0 / 0 | 15 / 14,739 | 61,440 / 15 |

Both fixed windows kept authoritative content, inode and mtime unchanged. Deferred
notifications remained PENDING and were never sent; the fixed payload remained
unchanged. All fixtures use a 2.28 MB payload and fake provider/worker reads.
Raw JSON is saved in [integration-evidence](integration-evidence/measurement-fixed.json).
This measures the local final loop, not deployed production or its original
107 MiB/min observation. Baseline modes patch only the behavior under comparison
in memory; they do not run an old bridge, change the checkout or live state.

Reproduce outside a sandbox that blocks thread-to-asyncio socket wakeups:

```sh
.venv/bin/python tests/measure_semantic_durability.py --mode baseline --seconds 60 --output /tmp/baseline.json
.venv/bin/python tests/measure_semantic_durability.py --mode fixed --seconds 60 --output /tmp/fixed.json
.venv/bin/python tests/measure_semantic_durability.py --mode review-baseline --deferred-notification --seconds 60 --output /tmp/deferred-baseline.json
.venv/bin/python tests/measure_semantic_durability.py --mode fixed --deferred-notification --seconds 60 --output /tmp/deferred-fixed.json
```

Combined checkpoint/legacy/restart/failure tests are in
`tests/test_resumable_semantic_durability.py`. The transferred idle-loop tests now
drive real F3 commands rather than the previous `loop.discover` method.

## Historical #47 source measurements

The following results were obtained in the independent #47 source worktree before
integration. They describe the previous loop and are not final-tree evidence.

Reproduce the isolated wall-clock measurements from the repository root:

```sh
python3 tests/measure_semantic_durability.py --mode baseline --seconds 60
python3 tests/measure_semantic_durability.py --mode fixed --seconds 60
```

The baseline restores the previous `save_state` body without changing the
checkout. Both runs use the production `ControlLoop`, scheduler, atomic writer
and a 2.28 MB fixture, with external provider/worker edges replaced by local
readers. Initial workflow/schema transitions settle before counters start. The
window uses the normal ten-second control cadence and thirty-second discovery
cadence. It counts authoritative/telemetry replacements and bytes, records
mtime/inode/content stability, and samples `/proc/self/io`. A sixty-second run
must exercise fresh heavy runtime reports. This is a local reproduction, not a
measurement of a deployed production process.

Running-idle local measurement (seven ticks, 60.060 seconds baseline / 60.044 seconds
fixed, one discovery and one fresh heavy report in each measured window):

| Evidence | Previous save behavior | Fixed |
| --- | ---: | ---: |
| Authoritative file size | 2,287,038 bytes | 2,281,689 bytes |
| Authoritative replacements | 28 | 0 |
| Authoritative bytes serialized to replacements | 64,037,069 | 0 |
| Authoritative mtime / inode / content | Changed | All unchanged |
| Small telemetry replacements / bytes | 0 / 0 | 28 / 153,503 |
| Process `write_bytes` delta | 64,110,592 | 229,376 |
| Process `syscw` delta | 84 | 28 |

The local process wrote about 61.14 MiB before versus 224 KiB after, including
optional telemetry. The owner's production observation was about 107 MiB/min
and 12.5k write syscalls/min for a roughly 2.28 MB file. This simplified fixture
reproduces continuous multi-megabyte rewrites but does not reproduce the full
production state's shape, syscall count or external traffic. The decisive local
result is zero authoritative replacements with changing timing and runtime
observations. Production services were not restarted or modified.

Independent-review reproduction with a deferred PENDING NEEDS_OWNER notification:

```sh
python3 tests/measure_semantic_durability.py --mode review-baseline --deferred-notification --seconds 60
python3 tests/measure_semantic_durability.py --mode fixed --deferred-notification --seconds 60
```

`review-baseline` restores suppression-time payload regeneration while keeping
the first fix's authoritative telemetry filter. Both windows also include the
healthy running fixture's discovery and fresh runtime read. The deferred issue
is held for local inspection, with its notification retry scheduled beyond the
measurement window. Neither run sends a notification. Seven normal-cadence ticks
took 60.060 seconds before and 60.088 seconds after the review fix:

| Evidence | Before review fix | Fixed |
| --- | ---: | ---: |
| Authoritative file size | 2,283,534 bytes | 2,283,514 bytes |
| Authoritative replacements / bytes | 6 / 13,701,209 | 0 / 0 |
| Authoritative mtime / inode / content | Changed | All unchanged |
| Deferred notification status | PENDING | PENDING |
| Deferred notification payload | Changed duration | Unchanged |
| Process `write_bytes` delta including telemetry | 14,000,128 | 286,720 |
| Process `syscw` delta | 53 | 35 |

The moving-clock control-loop regression covers both metadata-bearing and legacy
notification snapshots, including restart. Send-boundary tests verify fresh
duration and title in synchronous/asynchronous buffers, durable SENDING receipts,
uncertain acceptance after restart, and freshness gating of regenerated legacy
COMMIT invitations. Deferred checks never call the display formatter.

Regression coverage is in `tests/test_semantic_durability.py`: empty and running
idle loops, heavy observations, restart stability, all six volatile health
fields, immediate workflow/health transitions, uncertain-worker notification
deduplication, retained reservations, legacy migration, status observability,
stale/corrupt/missing telemetry, bounded storage, telemetry IO failure, and the
deferred-notification/send-boundary cases above.

Full verification commands:

```sh
python3 -m unittest discover -s tests -v
python3 -m py_compile *.py tests/*.py
git diff --check
```

# Runtime liveness (V2-M2D)

GitHub labels and future ClickUp statuses are projections. They cannot establish
whether a task is executing. Use `devctl report TASK --json` / MCP
`get_task_report` and inspect the additive, transport-neutral `liveness` object.
`devctl status TASK` prints the classification and lease evidence too.

Every newly launched coding/review turn has a detached Python supervisor on its
registered machine. It observes the agent command's exit and refreshes a durable
lease every **5 seconds**, with a **30 second** expiry, even with no stdout.
`liveness.json` contains task/project/machine/agent/session identity, a per-turn
generation, supervisor and agent PIDs with Linux boot/start identities, phase,
start/heartbeat/expiry timestamps, heartbeat sequence, exit code and reason.
Writes use the existing atomic JSON/fsync primitive and a per-task lock. Turn
generation fencing rejects writes from an old runner. An observed session is
persisted without changing the lease or heartbeat sequence.

The watchdog runs when status is read, including the bridge's periodic task
listing. It reloads the record and rechecks ownership every time; a saved healthy
classification is never reused as proof. Classification transitions, their
sequence and time are persisted in `runtime_health.json`. Duplicate observations
do not rewrite it; heartbeat age is calculated for each report.

| Runtime classification | Required evidence | Existing task status |
| --- | --- | --- |
| `RUNNING_HEALTHY` | Fresh lease, at least one heartbeat, supervisor and agent command owned/alive | `RUNNING` |
| `SUSPECTED_STALL` | Stale lease with a provable live process, or fresh lease with uncertain ownership/communication | `UNKNOWN` for new records; legacy status remains compatible |
| `LOST_ORPHANED` | Expired lease without any proven live responsible process | `FAILED` when both identities are verified absent or the responsible machine confirms no worker ever registered; otherwise `UNKNOWN` to retain capacity |
| `TERMINAL` | Durable observed exit; completed V1 tasks retain their existing exit semantics | `DONE`/`FAILED` |

This is runtime evidence, not another coding/review/owner workflow. Suspected
work keeps its scheduler/workspace reservation. Lost work with ambiguous
ownership also retains capacity. A stale lease never kills or retries a live
agent. No missing-output timeout is used. A new worker's zero exit without an
agent result becomes `FAILED` with the durable reason
`process_exited_without_final_report` on the shared status path, including
`task_state`, `status`, `list_tasks` and the final report. Existing
structured coding/review contract validation remains in the bridge.

On restart, status reloads the records and verifies boot/start identities; a PID
alone or a saved `RUNNING` field does not prove health. Missing, malformed,
duplicate-key or mismatched-generation records fail closed and are preserved
for inspection. A surviving recorded agent process after supervisor death is
suspected even after expiry, not falsely declared absent. Pre-milestone live
tasks without heartbeats report `heartbeat_not_registered` and cannot be
classified healthy; their existing process/exit handling and running projection
remain in use. Missing registration alone does not trigger a stall notification
or owner hold for these legacy tasks.

A local launch whose lease expires before any heartbeat or process identity was
registered is `FAILED` with reason `lease_expired_before_worker_registered`,
freeing its capacity through the existing failure path. A provably live launch
PID receipt still keeps the task suspected and reserved. An unsynchronized remote
mirror remains uncertain until the responsible machine can be checked; lack of
communication cannot establish that the remote worker never registered.

SSH workers use the same runtime code, bundled with the existing registered
worker RPC. The supervisor persists and refreshes on the worker independently
of SSH connectivity. A successful report carries the source record and process
ownership evidence. WORK validates and mirrors it durably. Cached MCP
`list_tasks` performs no SSH probes and **cannot prove remote health**; a fresh
remote lease is suspected without a current ownership probe, and an expired
unverified lease is lost/ambiguous. Use `get_task_report` to obtain fresh remote
evidence. A durable remote exit remains terminal. Incomplete final-log download
still holds the existing report-processing barrier before review/publication.

The normalized report exposes heartbeat age, lease state, phase, worker/process
ownership (`true`, `false`, or `null` for uncertainty), task/session/generation,
exit status, reason and last classification transition. It contains no commands,
credentials or environment. A healthy lease establishes runtime ownership and
responsiveness, not proof of useful semantic progress in the model's reasoning.

The bridge projects suspected work as `dev:needs-owner`, and lost work as
`dev:failed`, with a concise evidence comment and Telegram notification through
the existing durable outbox. Coding and review workers both use this projection.
An unavailable report RPC can still project cached lost/suspected evidence.
Incident signatures suppress duplicate comments/notifications; label
reconciliation corrects a stale external `dev:running`. Future transport
adapters and hourly watchers can consume the same `liveness` payload without
owning task state. This milestone adds no ClickUp polling, automatic
kill/restart/retry, or owner publication authorization.

Verification: `python3 -m unittest discover -s tests -v`,
`python3 -m py_compile *.py tests/*.py`, and `git diff --check`.

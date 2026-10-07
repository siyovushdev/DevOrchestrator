# V2-M2A local control foundation

`control_plane.py` defines version-1 normalized envelopes and a separate local
JSON receipt/event journal. Nothing in the production runtime imports it. V1
TaskBridge state remains authoritative for task lifecycle, scheduling, coding,
independent review and owner publication gates. Chief Review M1 is unchanged.
There is no transport adapter, polling, network I/O, execution callback, migration,
delivery outbox or second task state machine in this foundation.

## Envelopes

`ControlCommand` requires `source_transport`, `source_id`, `kind`, a JSON object
`payload`, a JSON object `actor`, and `created_at`. Optional fields are `logical_id`,
`task_ref`, `project_ref` and `source_order`. `schema_version` defaults to 1 and
`command_id` is derived. `ControlEvent` requires `task_ref`, `kind`, positive
`revision`, a JSON object `payload` and `created_at`; it also carries version 1
and a derived `event_id`.

References are opaque nonblank strings of at most 512 characters. They are never
resolved as shell commands, paths, projects or authorization. Kinds use lowercase
letters, digits, underscores, dots and hyphens; transports use lowercase letters,
digits, underscores and hyphens. Timestamps must be explicit UTC RFC3339 strings
ending in `Z`, with optional fractional seconds up to six digits. Source order is
an optional nonnegative integer; revisions are positive integers, both bounded
by a signed 64-bit maximum. Payloads and actor metadata accept only strict JSON
values with string object keys and finite numbers. Envelopes snapshot their input
into deeply immutable values. Use `to_dict()` for detached JSON serialization;
`from_dict()` requires every serialized field, including explicit null optional
fields, rejects unknown fields, and verifies the derived identity.

## Identity and replay

Command IDs are SHA-256 hashes of a versioned canonical JSON identity: either
the transport/source pair or the shared logical ID. Source IDs must be stable and
namespaced by the adapter, for example including repository/account, task and
comment/update ID. An edited command must receive a distinct source revision ID;
reusing an existing source ID with changed normalized content fails closed.
Text never supplies command identity.

A shared logical ID deduplicates across transports only if version, kind, payload,
actor, task reference and project reference match as canonical JSON. Adapters must
normalize actor identity and target references consistently. Actor metadata does
not confer owner authority. Transport, source ID, timestamp and source order are
provenance and are excluded from the content comparison. Source replay cannot add,
remove or change its logical ID. Logical or source content conflicts raise
`ValueError` before writing.

Event IDs bind the version, task reference and revision. Event content, including
its timestamp, cannot change on replay. Each task starts at revision 1 and advances
contiguously; an event describing a later observation uses the next revision.
Revisions describe journal observations, not a new lifecycle transition engine.

## Local journal

A trusted local caller supplies a separate file path to `ControlJournal`; there is
no default deployment path or automatic initialization in V1. `record_command()`
and `append_event()` return a `JournalResult(identity, sequence, duplicate)` only
after successful persistence. `snapshot()` returns detached, validated JSON.

The file contains only `schema_version`, `receipts` and `events`. Both collections
have contiguous append sequences starting at 1. Receipt sequence orders first
sightings of transport sources. An unseen cross-transport alias of an existing
logical command is retained as a receipt, but returns `duplicate=True` with the
first command's identity and receipt sequence. Replaying a known source writes
nothing. Events have their own global append sequence, in addition to per-task
revision; exact event replay writes nothing. Source timestamps/order do not reorder
already recorded entries.

Read, validation and mutation occur under `durable.exclusive`; writes use
`durable.write_json` with atomic replacement and file/directory synchronization.
Every read rebuilds identity indexes and validates all records, versions, identities,
sequences and logical aliases. The additive optional `object_pairs_hook` argument
to `durable.read_json` lets this journal reject duplicate JSON keys, including
nested payload keys; V1 callers retain their existing decoder behavior. A missing
file represents an empty journal. Invalid JSON, malformed records and contradictory
history fail closed; I/O errors propagate. No corrupt journal is reset or repaired
automatically. Passing an existing V1 state file fails schema validation and leaves
it unchanged.

A receipt is evidence of durable receipt, not evidence of successful execution or
delivery. Callers must not treat replay as permission to execute again. Crash
reconciliation and actual effects must later use the existing execution pipeline;
this milestone adds no dispatch or exactly-once execution claim.

## Local verification

```bash
python3 -m unittest discover -s tests -p 'test_control_plane.py' -v
PYTHONPATH=tests:. python3 -m unittest test_recovery test_boundaries test_scheduler test_chief_review -v
python3 -m unittest discover -s tests -v
python3 -m py_compile control_plane.py durable.py tests/test_control_plane.py
git diff --check
```

The focused tests cover versioned identity encoding, detached immutable envelopes,
source and logical replay, content conflicts, event ordering, concurrent writers,
failed persistence, corrupt JSON/history, duplicate keys and unchanged legacy V1
state/task parsing. Tests use temporary local files and no external services.

# V2-M2C GitHub runtime bridge

`transports.py` defines the I/O-only contract above the M2A envelopes.
`github_transport.py` now owns the GitHub task-bus I/O used by TaskBridge.
GitHub is the only active transport. The existing bridge polling loop, CLI runner
and timeouts, workflow, durable state and outbox remain authoritative. No new
configuration, credentials, dispatcher or lifecycle engine is introduced.

`task_bridge.open_issues()` calls `GitHubTransport.poll_commands()` once, then
obtains V1 issue views of that validated batch through `issue_view()`. Discovery
therefore passes through `ExternalTask` and `ControlCommand` before V1 consumes
task text or metadata. Commands contain only unparsed title/body/display URL and
the external issue address; provider labels and other metadata stay in the
adapter's ephemeral compatibility views. The views preserve V1 fields, nulls,
URL precedence and issue numbers. They are neither new reads nor lifecycle state.
A failed poll clears the batch and exposes no partial or stale views.

Discovery commands use kind `task.discovered`, an empty descriptive actor and the
GitHub creation timestamp (an epoch sentinel for missing legacy fixture metadata).
They do not claim a local task/project reference or owner authority. No M2A journal
writes or dispatch are added: edited task text can still follow V1 recovery rules
without journal conflicts, and V1 issue-number keys and RPC identities are intact.

## Existing GitHub responsibilities

| Responsibility | V1 workflow seam | M2C boundary |
| --- | --- | --- |
| Open issue discovery, pagination, PR filtering | `task_bridge.open_issues`, `gh_pages` | `poll_commands`, `issue_view`, `pages`; one complete validated poll, no duplicate discovery. |
| Issue title/body/labels and tracked closed-issue fetch | `reconcile_tick`, `fetch_issue`, `label_names` | Adapter `fetch_issue` validates through `ExternalTask`, binds the requested issue reference and returns a V1 compatibility view. `fetch_task` exposes the normalized task directly. |
| Task marker parsing and execution options | `parse_task`, `start_queued` | Remain local; adapter preserves unparsed body. |
| Comments and author/time normalization | `issue_comments` | Adapter compatibility method retains V1 author/time/URL precedence and all dedupe aliases. Comment commands are not newly dispatched. |
| Owner/follow-up markers, trusted authors, question freshness | `parse_owner_command`, `authorized_comment`, `process_followup`, `process_owner_actions` | All authorization, freshness and workflow decisions remain local. |
| Report/review comments, desired labels | `format_report`, `add_comment`, `transition_issue` | Existing durable outbox uses adapter `post_comment`, `fetch_labels` and `transition_issue` via compatibility wrappers; bodies, markers and label arguments are unchanged. |
| Durable inbound dedupe and launch identity | `processed_followups`, `processed_owner_actions`, `comment_already_processed`, pending RPC, scheduler/worker receipts | Unchanged. M2A source identity is additive, not a migration of V1 operation keys. |
| Durable external delivery/recovery | `bridge_effects` outbox and `flush_effects` | Unchanged, including comment effect markers, latest desired labels and bounded batches. Ambiguous adapter writes raise `TransportError(uncertain=True)`; V1 re-reads markers/current labels before retrying. |
| Telegram terminal notifications | `notifications`, V1 outbox | Unchanged and outside this milestone. |

GitHub publication repository calls in `publication.py` are not task-bus transport
operations. Publication policy and merge method selection remain unchanged.

## Contract

`Transport` is a structural Protocol with three I/O methods and a scope:

- `poll_commands() -> tuple[ControlCommand, ...]`: a complete, validated batch of
  normalized commands, including task candidates or message observations.
- `fetch_task(ExternalRef) -> ExternalTask`: unparsed task text and display URL.
- `publish_event(ExternalRef, ControlEvent) -> None`: project an authoritative
  local observation as externally visible text or state. A visibility event may
  carry a normalized state; mapping that state to provider labels is adapter-owned.
- `scope -> TransportScope`: stable provider kind and account/repository/list
  instance namespace. Consumers can keep multiple instances without global state.

This contract has no execution callbacks, mutable local state, scheduler, review
or owner authorization methods. `ExternalRef` is an external routing address,
distinct from M2A's local `task_ref`/`project_ref`. The local pipeline must resolve
those references and validate/journal/authorize commands before executing them.
Actor metadata, command kinds, issue text and visible labels grant no authority.
Provider objects, alias resolution, pagination and cursors belong inside adapters.

Adapters raise `TransportError` on unavailable, malformed, unsupported or failed
operations. They must not return an empty batch after a failed poll or claim a
successful write after ambiguous acceptance. `uncertain=True` tells the caller to
reconcile before retrying. Error operation/codes must be static and secret-free;
provider exception text and credentials must not leak. Targets outside an
adapter's scope must be rejected. A Protocol defines this contract; it does not
sandbox arbitrary adapter code or itself implement delivery/retry validation.

GitHub I/O uses injected CLI functions that receive no mutable lifecycle state.
Mapping-only instances without CLI functions still fail explicitly with
`not_implemented`. `publish_event` remains explicitly unsupported in M2C: the
authoritative outbound path is the V1 outbox through the adapter compatibility
methods, with its existing delivery keys and comment markers. Generic V2 event
rendering would require a later migration of delivery semantics. There is no
parallel polling or effect-delivery loop.

## Source identity and GitHub mapping

`ExternalRef.source_id(revision=None)` hashes compact UTF-8 JSON of
`["transport-source", 1, kind, instance, external_id, revision]` with SHA-256 and
prefixes it with `v1:`. Use it as `ControlCommand.source_id` together with
`ref.scope.kind` as `source_transport`. The versioned source includes instance
scope, so identical external IDs in different repositories/lists cannot collide.
It is independent of message text, credentials, polling time and process lifetime.
Adapters must preserve namespaces/encoding across restarts. An explicit provider
revision may identify an edit; without one M2A rejects changed normalized content
at the same source. M2A schema, journal format and command-ID encoding are unchanged.
Shared logical IDs retain M2A cross-transport semantic binding/deduplication.

The GitHub namespace is `github.com/owner/repository` in lowercase. Issue addresses
use `issue:<number>`; comment addresses use `issue:<number>/comment:<numeric-id>`.
REST numeric IDs, numeric strings and GraphQL comments with the corresponding
GitHub web URL converge on the same comment reference. Node-only comments must be
resolved to a numeric ID before ingestion; no timestamp/body fallback is allowed.
V1 still retains its original ID priority and alias backfill, untouched. Mapping
preserves issue number, title, body and `html_url`-or-`url` display precedence and
rejects PR objects. It does not parse task, follow-up or owner markers.

## Offline verification

`tests/test_transports.py` checks the normalized contract, distinct implementations
(including a test-only second-provider double), multiple same-provider instances,
stable and revised sources, explicit/uncertain failures, GitHub reference aliases,
M2A replay/conflicts, runtime discovery/fetch/label/comment compatibility and
explicit failures. The whole-bridge harness now fakes provider I/O while keeping
GitHubTransport active, so existing integration/recovery/owner-flow cases exercise
the migrated path. Additional cases check poll/comment failures, same-session
follow-ups, all publication command gates, ambiguous-write marker reconciliation,
duplicate polls and read-only/no-op completion. No ClickUp API, polling/webhooks,
credentials or configuration are added.

```bash
python3 -m unittest discover -s tests -p 'test_transports.py' -v
PYTHONPATH=tests:. python3 -m unittest test_control_plane test_bridge_integration test_boundaries test_recovery test_owner_flow -v
python3 -m unittest discover -s tests -v
python3 -m py_compile transports.py github_transport.py task_bridge.py tests/test_transports.py tests/test_bridge_integration.py
git diff --check
```

# V2-M2B transport boundary

`transports.py` is an additive, I/O-only contract above the M2A envelopes.
`github_transport.py` is a pure GitHub mapping skeleton. Neither is imported by
production TaskBridge, MCP, notifications or publication code. No network calls,
credentials, new configuration, polling implementation or lifecycle engine are
introduced. Runtime integration belongs to M2C.

## Existing GitHub responsibilities

| Responsibility | Current V1 owner | M2B treatment |
| --- | --- | --- |
| Open issue discovery, pagination, PR filtering | `task_bridge.open_issues`, `gh_pages` | Contract can poll normalized commands; skeleton only maps issue references/text. |
| Issue title/body/labels and tracked closed-issue fetch | `reconcile_tick`, `gh_json`, `label_names` | `ExternalTask` exposes reference/title/body/display URL; provider metadata and labels stay adapter-owned. |
| Task marker parsing and execution options | `parse_task`, `start_queued` | Remain local; skeleton preserves unparsed body. |
| Comments and author/time normalization | `issue_comments` | Future adapter supplies normalized `ControlCommand`; no GitHub comment object in the protocol. |
| Owner/follow-up markers, trusted authors, question freshness | `parse_owner_command`, `authorized_comment`, `process_followup`, `process_owner_actions` | All authorization, freshness and workflow decisions remain local. |
| Report/review comments, desired labels | `format_report`, `add_comment`, `transition_issue` | `publish_event` can render a local event as a comment or visible state/labels; never changes local owner/lifecycle state. |
| Durable inbound dedupe and launch identity | `processed_followups`, `processed_owner_actions`, `comment_already_processed`, pending RPC, scheduler/worker receipts | Unchanged. M2A source identity is additive, not a migration of V1 operation keys. |
| Durable external delivery/recovery | `bridge_effects` outbox and `flush_effects` | Unchanged, including comment effect markers, latest desired labels and bounded batches. |
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

The GitHub skeleton explicitly raises `not_implemented` for all I/O, including
outbound events. Its pure mapping helpers are usable offline; it is deliberately
not a production delivery implementation. M2C must define supported event
rendering and retain durable dedupe/recovery before integrating it. Event IDs can
key delivery markers, but are not proof of successful delivery.

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
M2A replay/conflicts and unchanged local owner/lifecycle state. No ClickUp API,
polling/webhooks, credentials or configuration are added.

```bash
python3 -m unittest discover -s tests -p 'test_transports.py' -v
PYTHONPATH=tests:. python3 -m unittest test_control_plane test_bridge_integration test_boundaries test_recovery test_owner_flow -v
python3 -m unittest discover -s tests -v
python3 -m py_compile transports.py github_transport.py tests/test_transports.py
git diff --check
```

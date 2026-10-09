# Agent execution profiles (schema version 1)

`execution_models.json` is the execution policy boundary. No model inventory is
embedded in the bridge, MCP adapter, or UI. New models, supported option values,
Cursor variant mappings, defaults, and named profiles are configuration edits.
There is no billing integration or automatic routing heuristic.

## Configuration locations and validation

The bundled catalog is the default. Set `DEVORCHESTRATOR_MODEL_CATALOG` for a
global replacement, or `execution_catalog` in a registered project in
`projects.json` for a project replacement. Relative paths resolve from the
DevOrchestrator directory, not the task workspace. Catalogs replace rather than
merge, so their schema and default selection remain unambiguous. They contain
no credentials; keep API keys and tokens in existing runtime authentication.

A project can set `execution_profile` for coding defaults and
`review_execution_profiles` as an agent-to-profile map for independent review.
Review uses its review profile separately from coding defaults. The bundled
Codex review profile retains the previous `gpt-6.1-sol` / `high` policy. The
bundled coding Codex default pins WORK's existing `gpt-6.1-sol` / `xhigh` settings;
set a project catalog/profile to retain different host-specific defaults.
Cursor's default catalog model `cursor-auto` maps to runtime `auto`, explicitly
recorded as `runtime_model`. Select provider Auto explicitly with `--model cursor-auto`;
`--model auto` selects the configured profile/default. Catalog `model_id: auto`
is rejected because `auto` is reserved for automatic selection.
Runtime `auto` invokes Cursor's provider Auto policy; DevOrchestrator's `auto`
means configured selection, without new task/risk heuristics.

Run read-only validation/inventory:

```bash
python devctl.py execution-catalog
python devctl.py execution-catalog --project devorchestrator --discover --agent cursor
```

MCP exposes the same operation as `get_execution_catalog(project, discover,
agent)`. This validates the entire catalog and optionally reads the runtime
version/model list on the project host. It never creates a chat or agent turn.
The runtime probes use bounded timeouts and never change authentication or
services. Invalid schema versions, duplicate JSON keys/models/variant mappings,
unknown execution fields, incorrect types, invalid defaults/profiles, and
unsupported adapter mappings fail closed.

## Catalog structure

The root requires `schema_version: 1`, `models` (array), `profiles` (object), and
`defaults` (object). Defaults specify `agent`, `models` for both agents, and
optionally `review_profiles` mapping agents to profile names.

Each model requires `agent`, `model_id`, `display_name`, boolean `enabled`,
`supported_options` (option name to allowed scalar values), and `defaults`
(option values). Option values are identifiers or booleans; `auto` is reserved
for task default selection. Codex's current adapter supports `effort` as a
reasoning-effort name. Cursor accepts generic option names, but every resolved
nonempty option combination needs an exact `variants` mapping to a verified
runtime model ID. Each variant requires `options` and `runtime_model_id`.

Optional model fields are `constraints.allowed_combinations` (array of exact
resolved option objects), descriptive `capabilities`, and descriptive `metadata`
(for example cost/limit notes). Metadata and capabilities never affect billing,
CLI flags, or routing. Disabled models cannot start new tasks, including through
a profile or default. Unknown options and missing combination mappings are
errors, never silently discarded.

A named profile requires `agent`, `model`, and `options`. For example the bundled
Cursor fast profile resolves this model:

```json
{
  "agent": "cursor",
  "model_id": "composer-2.5",
  "display_name": "Composer 2.5",
  "enabled": true,
  "supported_options": {"fast": [false, true]},
  "defaults": {"fast": false},
  "variants": [
    {"options": {"fast": false}, "runtime_model_id": "composer-2.5"},
    {"options": {"fast": true}, "runtime_model_id": "composer-2.5-fast"}
  ]
}
```

Add another model by adding an entry, its verified variant mappings, and
optionally a profile. No orchestration changes are required. If a combination
is unavailable (for example GPT-5.4 Low Fast in the verified installed list),
omit its mapping and selection will fail before launch.

## Verified Cursor adapter behavior

On WORK, `agent --version` returned `2026.10.01-e373342` on 2026-10-09.
`agent --help` advertises `--model <model>` and bracket parameter syntax;
`agent models` lists account-specific concrete variants including
`composer-2.5-fast` and GPT-5.4 effort/fast variants. The adapter passes verified
variants using the existing `--model` flag. This agrees with Cursor's documented
[CLI model selection](https://prod.cursor.com/docs/cli/overview).

The CLI inventory does not expose a machine-readable per-model bracket-parameter
schema. Recognizing bracket syntax alone cannot prove a particular model's
allowed combinations, so this milestone uses listed concrete variants. It does
not invent `--fast`, `--context`, or arbitrary CLI flags. Context/quality/other
options can be configured when they map to an available variant; unverified
bracket combinations and flags remain rejected. Model IDs in the bundled catalog
are an editable snapshot, not protocol constants.

Before a new launch, the adapter checks version/help and the chosen Cursor
variant against `agent models` on the execution host, including SSH workers.
Unavailable variants and failed discovery produce actionable errors. Codex
keeps `-m MODEL` and `-c 'model_reasoning_effort="EFFORT"'`; its effort allowlist
is catalog policy. This does not call OpenAI APIs or require a network model
catalog for Codex. Read-only review/sandbox arguments are preserved.

## Task selection and precedence

Issue task metadata remains `DEVORCHESTRATOR_TASK_V1`:

```html
<!-- DEVORCHESTRATOR_TASK_V1 {"project":"work_test","agent":"cursor","model":"composer-2.5","execution_profile":"cursor-fast","execution_options":{"fast":false}} -->
Implement the requested change.
```

CLI and MCP support `agent`, `model`, `effort`, `execution_profile`, and
`execution_options`:

```bash
python devctl.py run-project work_test 'Implement the requested change' \
  --agent cursor --execution-profile cursor-fast
python devctl.py run-project work_test 'Implement the requested change' \
  --agent cursor --model gpt-5.4 --effort high --execution-options '{"fast":true}'
```

Explicit agent/model/option values win over profile and model defaults. An
explicit agent overrides an incompatible project default coding profile. An
explicitly named profile with a conflicting explicit agent is an actionable
configuration error; choose a matching profile. Project review profile mappings
must pair each agent with a profile for that agent; mismatches are errors rather
than fallback to coding defaults. A model override retains profile
options and validates them against the new model; incompatible options cause an
error. Conflicting `effort` and `execution_options.effort` values cause an error.

`agent:auto`, `model:auto`, and option `auto` use the configured profile/default
path. An automatic option requires a supported option with a configured value.
There is no opaque fallback to another model. Options not requested use the
selected model defaults overlaid with profile options.

## Durable execution identity and observability

New task metadata saves `execution` containing resolved `agent`, logical `model`,
`options`, concrete `runtime_model`, selected profile, per-field
`decision_sources` (`explicit`, `profile`, `default`, `auto`), adapter version,
runtime version, and catalog path/schema/hash. The same object is exposed in
reports, task listings, issue status, and bridge result comments. Reports keep
the resolved logical model and provide `observed_model` separately when runtime
logs report a concrete variant/display name.

```bash
python devctl.py report TASK_ID --json
python devctl.py status TASK_ID --json
python devctl.py status ISSUE_NUMBER --issue --json
```

Continuation accepts no execution overrides: it reuses the saved agent/model/
options and revalidates the saved Cursor variant against the current runtime.
Catalog/profile edits, including disabling a model, affect new launches and do
not silently reroute an existing session. If the runtime no longer supports the
saved variant, continuation fails closed. Adopted/legacy sessions retain their
existing continuation behavior; legacy Cursor metadata carrying unverified
model/options is rejected. Start RPC replays use their durable receipt and saved
identity rather than re-resolving changed configuration. Version 2 start request
signatures include the agent exactly as requested, including omission, and
profile/options. An explicit default agent cannot replay an omitted-agent request
that selected a different profile agent. Historical receipts retain their
original signatures, with saved execution provenance checked where available.

Before reserving a repository or persisting launch intent, the bridge calls
read-only MCP `preflight_task` to validate role, profiles, and installed runtime.
Runtime probes share a three-second deadline within the control loop's issue
budget. A confirmed prelaunch rejection carries its actionable error in a
structured result, terminates the coding queue or review loop, and releases the
repository when existing safety holds permit. Locked worker preparation repeats
validation to catch changes after preflight. Existing receipts replay without
reapplying catalog policy; network errors, corrupt receipts, and uncertain
worker ownership retain their reservations for reconciliation.

Publication, owner, Chief, post-merge convergence, runtime cleanup, and control
loop gates are unchanged. Editing a catalog does not authorize publication or
restart services.

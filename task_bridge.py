#!/usr/bin/env python3

import asyncio
import json
import math
import os
import re
import subprocess
import time
from contextlib import ExitStack
from contextvars import ContextVar
from pathlib import Path

import notifications
import bridge_effects
import bridge_telemetry
import chief_review
import review_transitions
from transition_io import DRIVING, call as io_call
from bridge_timing import measure, record as record_timing
from operational_state import exclusion_reason, lifecycle_state, next_action, notification_fingerprint
from github_transport import GitHubTransport
from lifecycle import WORK_QUEUE_LABEL, worker_guard
from datetime import datetime, timezone
from durable import read_json, write_json_if_changed, exclusive
from scheduler import Scheduler, QueueWait, project_identity, registered_project
import hashlib
from redaction import redact, redact_data
from execution_profiles import PreflightRejected, check_preflight

from publication import Repository, PublicationError, execute as publish

# Import MCP lazily so local tests and cleanup do not need a running MCP service.


REPO = "siyovushdev/dev-orchestrator-tasks"
MCP_URL = "http://127.0.0.1:8787/mcp"
POLL_SECONDS = 10
# Includes fresh report checks, repository identity/snapshot, read-only preflight,
# and the worker's locked launch validation. Five seconds is only a read slice,
# not a viable budget for this complete durable transition.
ISSUE_RECONCILIATION_TIMEOUT = 60
# Startup validates explicit enrollment, host binding and this dedicated queue.
QUEUE_LABEL = WORK_QUEUE_LABEL

STATE_DIR = Path.home() / ".local/share/devorchestrator"
STATE_FILE = STATE_DIR / "task_bridge_state.json"

STATE_LABELS = {
    QUEUE_LABEL,
    "dev:running",
    "dev:done",
    "dev:ready-for-review",
    "dev:awaiting-chief-review",
    "dev:needs-owner",
    "dev:failed",
}

TASK_RE = re.compile(
    r"<!--\s*DEVORCHESTRATOR_TASK_V1\s*(\{.*?\})\s*-->",
    re.S,
)

FOLLOWUP_MARKER = "<!-- DEVORCHESTRATOR_FOLLOWUP_V1 -->"

AUTOPILOT_MODE = "autopilot"
AUTOPILOT_READY_MARKER = "DEVORCHESTRATOR_READY_FOR_REVIEW"
AUTOPILOT_OWNER_MARKER = "DEVORCHESTRATOR_NEEDS_OWNER"
MAX_AUTOPILOT_CORRECTIONS = 2

AUTOPILOT_POLICY_VERSION = 2
AUTOPILOT_RESULT_PREFIX = "DEVORCHESTRATOR_RESULT:"

REVIEW_POLICY_VERSION = 1
REVIEW_RESULT_PREFIX = "DEVORCHESTRATOR_REVIEW:"

OWNER_COMMAND_RE = re.compile(
    r"<!--\s*DEVORCHESTRATOR_OWNER_V1\s+(\{.*?\})\s*-->",
    re.DOTALL,
)

OWNER_STATE_READY = "READY_FOR_OWNER"
OWNER_STATE_COMMITTED = "COMMITTED"
OWNER_STATE_PUSHED = "PUSHED"
OWNER_STATE_PR_CREATED = "PR_CREATED"
OWNER_STATE_MERGED = "MERGED"

OWNER_STATE_SEQUENCE = (
    OWNER_STATE_READY,
    OWNER_STATE_COMMITTED,
    OWNER_STATE_PUSHED,
    OWNER_STATE_PR_CREATED,
    OWNER_STATE_MERGED,
)

OWNER_ACTION_RULES = {
    "COMMIT": (
        OWNER_STATE_READY,
        OWNER_STATE_COMMITTED,
    ),
    "PUSH": (
        OWNER_STATE_COMMITTED,
        OWNER_STATE_PUSHED,
    ),
    "CREATE_PR": (
        OWNER_STATE_PUSHED,
        OWNER_STATE_PR_CREATED,
    ),
    "MERGE": (
        OWNER_STATE_PR_CREATED,
        OWNER_STATE_MERGED,
    ),
}

MAX_REVIEW_CORRECTIONS = 2
MAX_REVIEW_FORMAT_CORRECTIONS = 1
MAX_REVIEW_FORMAT_RESTARTS = 1


def gh(*args):
    from external_effects import guard_github
    guard_github(args)
    result = subprocess.run(
        ["gh", *args],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"GitHub CLI operation failed (exit {result.returncode})"
        )

    return result.stdout


def gh_json(*args):
    out = gh(*args)
    return json.loads(out) if out.strip() else None



def send_telegram(text):
    return notifications.send(text)


def notify(status, issue, item, report=None, blocker=None):
    if WORK_CONTEXT and observation_barrier(WORK_CONTEXT[0], WORK_CONTEXT[1], report):
        return
    if exclusion_reason(item, issue):
        return
    text = redact(notifications.terminal_text(status, issue, item, report, blocker,
                                            include_duration=not bool(WORK_CONTEXT)))
    if WORK_CONTEXT:
        state, number, _ = WORK_CONTEXT
        # Keep only bounded, redacted display data. Delivery recomputes the
        # action and checks the durable semantic identity, never trusts text.
        item['notification_report'] = redact_data({k: (report or {}).get(k) for k in
            ('id', 'agent', 'model', 'reasoning_effort', 'execution', 'execution_options', 'session_id', 'git_baseline', 'created_at', 'updated_at')})
        fingerprint = (item['chief_review']['identity']['bundle_id'] if status == 'CHIEF_REVIEW_REQUIRED'
                       else notification_fingerprint(item))
        event = [status, fingerprint, blocker]
        key = bridge_effects.enqueue(state, number, "telegram", text, event)
        state['outbox'][key]['notification'] = dict(status=status, fingerprint=fingerprint,
            issue={k: issue.get(k) for k in ('number', 'title', 'url')}, blocker=redact(blocker) if blocker else None)
        save_state(state)
    else:
        send_telegram(text)


def autopilot_contract():
    return f"""
The LAST non-empty line of your final response MUST be exactly one
machine-readable line beginning with:

{AUTOPILOT_RESULT_PREFIX}

followed by one JSON object.

Allowed state values:
- READY_FOR_REVIEW
- NEEDS_OWNER
- CONTINUE

Required JSON fields:
- state
- implementation
- verification
- self_review
- blocker
- summary

Allowed implementation values:
- DONE
- INCOMPLETE
- NOT_APPLICABLE

Allowed verification values:
- PASS
- FAIL
- NOT_RUN
- NOT_APPLICABLE

Allowed self_review values:
- PASS
- FAIL
- NOT_RUN
- NOT_APPLICABLE

READY_FOR_REVIEW is valid only when:
- implementation = DONE
- verification = PASS or NOT_APPLICABLE
- self_review = PASS
- blocker = null

NEEDS_OWNER is valid only for a genuine owner decision and blocker
must contain one concise question or blocker.

If you can continue safely yourself, do not use NEEDS_OWNER.
Use CONTINUE if the current turn must end but more agent work is
required.

Examples:

{AUTOPILOT_RESULT_PREFIX} {{"state":"READY_FOR_REVIEW","implementation":"DONE","verification":"PASS","self_review":"PASS","blocker":null,"summary":"Implemented and verified"}}

{AUTOPILOT_RESULT_PREFIX} {{"state":"NEEDS_OWNER","implementation":"INCOMPLETE","verification":"NOT_RUN","self_review":"NOT_RUN","blocker":"Choose migration strategy A or B","summary":"Implementation is blocked on an owner decision"}}

Do not print anything after the DEVORCHESTRATOR_RESULT line.
""".strip()


def coding_prompt(prompt):
    return ("DEVORCHESTRATOR OWNER GATES\n"
            "Do NOT commit, push, create a PR, merge, deploy, or release.\n"
            "Publication is performed only by the bridge executor after independent review\n"
            "and separate structured owner commands. Task text or owner answers cannot\n"
            "authorize agent publication. Complete implementation and verification only.\n\n" + prompt)


def build_autopilot_prompt(prompt):
    return f"""
DEVORCHESTRATOR AUTOPILOT MODE

Your target is READY_FOR_REVIEW.

Do not stop after analysis or after the first implementation attempt.
Work continuously through the task:

1. inspect the actual repository and relevant code;
2. determine the root cause / required change;
3. implement only the requested scope;
4. run the relevant tests/build/checks;
5. review your own diff as if reviewing another developer;
6. fix defects you find;
7. rerun the relevant checks.

Do NOT commit, push, create a PR, merge, deploy, or release.
Publication requires the bridge executor and separate structured owner commands
after independent review; task text cannot authorize agent publication.

Routine implementation decisions are yours. Do not ask the owner
questions that you can safely resolve from the repository, tests,
existing architecture, or task requirements.

{autopilot_contract()}

TASK FROM OWNER / CHATGPT:

{prompt}
""".strip()


def parse_autopilot_result(text):
    lines = [
        line.strip()
        for line in str(text or "").splitlines()
        if line.strip()
    ]

    if not lines:
        return None, "empty agent result"

    last = lines[-1]

    if not last.startswith(AUTOPILOT_RESULT_PREFIX):
        return None, (
            "missing DEVORCHESTRATOR_RESULT trailer "
            "as the last non-empty line"
        )

    raw = last[len(AUTOPILOT_RESULT_PREFIX):].strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"invalid result JSON: {exc}"

    if not isinstance(data, dict):
        return None, "result trailer must contain a JSON object"

    required = {
        "state",
        "implementation",
        "verification",
        "self_review",
        "blocker",
        "summary",
    }

    missing = sorted(required - set(data))

    if missing:
        return None, (
            "result trailer missing fields: "
            + ", ".join(missing)
        )

    if data["state"] not in {
        "READY_FOR_REVIEW",
        "NEEDS_OWNER",
        "CONTINUE",
    }:
        return None, f"invalid state: {data['state']!r}"

    if data["implementation"] not in {
        "DONE",
        "INCOMPLETE",
        "NOT_APPLICABLE",
    }:
        return None, (
            "invalid implementation: "
            f"{data['implementation']!r}"
        )

    if data["verification"] not in {
        "PASS",
        "FAIL",
        "NOT_RUN",
        "NOT_APPLICABLE",
    }:
        return None, (
            "invalid verification: "
            f"{data['verification']!r}"
        )

    if data["self_review"] not in {
        "PASS",
        "FAIL",
        "NOT_RUN",
        "NOT_APPLICABLE",
    }:
        return None, (
            "invalid self_review: "
            f"{data['self_review']!r}"
        )

    blocker = data["blocker"]

    if blocker is not None and (
        not isinstance(blocker, str)
        or not blocker.strip()
    ):
        return None, "blocker must be null or a non-empty string"

    if (
        not isinstance(data["summary"], str)
        or not data["summary"].strip()
    ):
        return None, "summary must be a non-empty string"

    return data, None


def autopilot_decision(report, item=None):
    if report.get("status") == "FAILED":
        return "FAILED", "agent process failed", None

    if not item or item.get("mode") != AUTOPILOT_MODE:
        return "READY_FOR_REVIEW", "manual task", None

    result = str(report.get("result") or "")

    # Compatibility for tasks created before policy v2.
    version = int(
        item.get("autopilot_policy_version") or 1
    )

    if version < AUTOPILOT_POLICY_VERSION:
        if AUTOPILOT_OWNER_MARKER in result:
            return "NEEDS_OWNER", "legacy owner marker", None

        if AUTOPILOT_READY_MARKER in result:
            return (
                "READY_FOR_REVIEW",
                "legacy ready marker",
                None,
            )

        return "CONTINUE", "legacy result incomplete", None

    data, error = parse_autopilot_result(result)

    if error:
        return "CONTINUE", error, None

    state = data["state"]

    if state == "NEEDS_OWNER":
        if not data["blocker"]:
            return (
                "CONTINUE",
                "NEEDS_OWNER requires a blocker",
                data,
            )

        return "NEEDS_OWNER", data["blocker"], data

    if state == "CONTINUE":
        return "CONTINUE", data["summary"], data

    problems = []

    if data["implementation"] != "DONE":
        problems.append("implementation is not DONE")

    if data["verification"] not in {
        "PASS",
        "NOT_APPLICABLE",
    }:
        problems.append("verification did not pass")

    if data["self_review"] != "PASS":
        problems.append("self-review did not pass")

    if data["blocker"] is not None:
        problems.append("READY_FOR_REVIEW has a blocker")

    if problems:
        return "CONTINUE", "; ".join(problems), data

    return "READY_FOR_REVIEW", data["summary"], data


def terminal_state(report, item=None):
    decision, _, _ = autopilot_decision(report, item)

    if decision == "READY_FOR_REVIEW":
        return "READY_FOR_REVIEW", "dev:ready-for-review"

    if decision == "NEEDS_OWNER":
        return "NEEDS_OWNER", "dev:needs-owner"

    if decision == "FAILED":
        return "FAILED", "dev:failed"

    # CONTINUE can reach here only after automatic correction attempts
    # have been exhausted.
    return "FAILED", "dev:failed"





def parse_owner_command(body):
    match = OWNER_COMMAND_RE.search(
        str(body or "")
    )

    if not match:
        return None

    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid owner command JSON: {exc}"
        ) from exc

    if not isinstance(data, dict):
        raise ValueError(
            "Owner command must be a JSON object"
        )

    allowed_fields = {
        "action",
        "message",
        "title",
        "body",
        "method",
    }

    extra = sorted(
        set(data) - allowed_fields
    )

    if extra:
        raise ValueError(
            "Unknown owner command fields: "
            + ", ".join(extra)
        )

    action = str(
        data.get("action") or ""
    ).strip().upper()

    if action not in {*OWNER_ACTION_RULES, "ANSWER"}:
        raise ValueError(
            f"Unsupported owner action: {action!r}"
        )

    data["action"] = action
    if action == "ANSWER" and not isinstance(data.get("message"), str):
        raise ValueError("ANSWER requires a non-empty message")

    for name in (
        "message",
        "title",
        "body",
        "method",
    ):
        value = data.get(name)

        if value is not None and (
            not isinstance(value, str)
            or not value.strip()
        ):
            raise ValueError(
                f"Owner command field {name!r} "
                "must be a non-empty string"
            )

    return data


def owner_action_plan(owner_state, action):
    action = str(
        action or ""
    ).strip().upper()

    if action not in OWNER_ACTION_RULES:
        return {
            "status": "REJECTED",
            "action": action,
            "current_state": owner_state,
            "next_state": owner_state,
            "reason": (
                f"Unsupported owner action: "
                f"{action!r}"
            ),
        }

    required_state, target_state = (
        OWNER_ACTION_RULES[action]
    )

    if owner_state not in OWNER_STATE_SEQUENCE:
        return {
            "status": "REJECTED",
            "action": action,
            "current_state": owner_state,
            "next_state": owner_state,
            "reason": (
                "Independent review has not "
                "reached READY_FOR_OWNER"
            ),
        }

    positions = {
        state: index
        for index, state
        in enumerate(OWNER_STATE_SEQUENCE)
    }

    current_pos = positions[owner_state]
    target_pos = positions[target_state]

    # Important for retries / duplicate owner commands:
    # never perform the same or an earlier publication action twice.
    if current_pos >= target_pos:
        return {
            "status": "ALREADY_DONE",
            "action": action,
            "current_state": owner_state,
            "next_state": owner_state,
            "reason": (
                f"{action} gate has already been passed"
            ),
        }

    if owner_state != required_state:
        return {
            "status": "REJECTED",
            "action": action,
            "current_state": owner_state,
            "next_state": owner_state,
            "reason": (
                f"{action} requires "
                f"{required_state}, "
                f"current state is {owner_state}"
            ),
        }

    return {
        "status": "ALLOWED",
        "action": action,
        "current_state": owner_state,
        "next_state": target_state,
        "reason": None,
    }


def invalidate_quality_gates(item):
    """
    Any new coding turn invalidates previous review/owner approval.

    Keep review_round as historical telemetry, but force a fresh
    independent review before publication is allowed again.
    """
    if item.get('review_transition') is not None:
        item['review_transition'] = review_transitions.invalidate(
            item['review_transition'], item.get('pending_rpc'))
    if item.get("review_task_id"):
        item['stale_review_task_ids'] = sorted(set(item.get('stale_review_task_ids') or []) | {item['review_task_id']})
    if item.get("owner_state") or item.get("reviewed_tree") or item.get("publication_journal") or item.get("review_hold") or item.get('review_task_id'):
        item.setdefault("publication_history", []).append({
            "owner_state": item.get("owner_state"), "review_round": item.get("review_round"),
            "approved_at": item.get("owner_approved_at"), "reviewed_tree": item.get("reviewed_tree"),
            "journal": item.get("publication_journal"), "commit_sha": item.get("commit_sha"),
            "pr_url": item.get("pr_url"),
            "review_hold": item.get("review_hold"), "review_task_id": item.get("review_task_id"),
            "coding_turn": item.get("review_coding_turn"),
            "post_merge_convergence": (item.get('post_merge_convergence') or {}).copy(),
        })
    chief_review.invalidate(item)
    item["autopilot_corrections"] = 0

    item["review_task_id"] = None
    item["reviewer_agent"] = None
    item["review_status"] = None
    item["review_format_corrections"] = 0
    item["review_format_restarts"] = 0
    item["review_corrections"] = 0

    item["owner_state"] = None
    item["owner_approved_at"] = None
    item["owner_blocker"] = None
    item["owner_question_at"] = None
    item["reviewed_tree"] = None
    item["review_start_tree"] = None
    item.pop('review_coding_turn', None)
    item['last_report_signature'] = None
    item.pop("review_hold", None)
    item.pop("execution_failure", None)
    item["publication_journal"] = {}
    convergence = item.get('post_merge_convergence') or {}
    if convergence.get('status') == 'NOT_MERGED' and not convergence.get('master_sha'):
        item.pop('post_merge_convergence', None)


def coding_report_successful(report, item):
    turn = report.get('turns')
    return (not item.get('safety_latch_hold') and report.get('observation_confidence') != 'READ_ONLY' and
            report.get('id') == item.get('task_id') and report.get('status') == 'DONE' and
            type(report.get('exit_code')) is int and report['exit_code'] == 0 and
            type(turn) is int and turn > 0 and turn == item.get('coding_turn', turn))


def observe_coding_turn(item, state, report):
    """Invalidate reviews on mutable/new coding turns, never adopt old review evidence."""
    number = next((n for n, tracked in state['issues'].items() if tracked is item), None)
    if number is not None and observation_barrier(state, number, report):
        return
    if exclusion_reason(item) or report.get('id') != item.get('task_id'):
        return
    turn = report.get('turns')
    bound = item.get('review_coding_turn')
    has_review = any(item.get(field) for field in ('review_task_id', 'review_start_tree', 'review_status',
                                                  'reviewed_tree', 'owner_state')) or item.get('review_transition') is not None
    changed_turn = type(turn) is int and turn > 0 and (
        (bound is not None and bound != max(turn, item.get('coding_turn', turn))) or turn > item.get('coding_turn', turn))
    unbound_review = bool(item.get('review_task_id') and bound is None and item.get('review_start_tree'))
    transition = item.get('review_transition')
    changed_source = transition is not None and (
        transition.get('source', {}).get('turn') != turn or
        transition.get('source', {}).get('runtime_generation') != report.get('runtime_generation'))
    if has_review and (report.get('status') in ('RUNNING', 'STARTING') or changed_turn or unbound_review or changed_source):
        pending = item.get('pending_rpc')
        if pending and pending['arguments'].get('role') == 'review' and transition is None and item.get('review_migration') is None:
            round_match = re.fullmatch(r'review:([0-9]+)', pending['operation'])
            if round_match:
                item['review_round'] = max(int(item.get('review_round') or 0), int(round_match[1]))
            number = next((str(number) for number, tracked in state.get('issues', {}).items() if tracked is item), '')
            key = hashlib.sha256(f"{REPO}:{number}:{pending['name']}:{pending['operation']}".encode()).hexdigest()
            job = state.get('work_queue', {}).get(key)
            if job:
                job['cancelled'] = True
                if job['status'] == 'QUEUED':
                    job.update(status='FAILED', launch_pending=False)
                elif job.get('task_id'):
                    item['stale_review_task_ids'] = sorted(set(item.get('stale_review_task_ids') or []) | {job['task_id']})
            item.pop('pending_rpc', None)
        invalidate_quality_gates(item)
    if type(turn) is int and turn > 0:
        item['coding_turn'] = max(item.get('coding_turn', 0), turn)
    item['coding_status'] = report.get('status') if report.get('status') in ('RUNNING', 'STARTING', 'UNKNOWN', 'DONE', 'FAILED') else 'UNKNOWN'
    if report.get('session_id'):
        item['coding_session_id'] = report['session_id']
    save_state(state)


async def drain_stale_reviews(item, state):
    if review_transitions.holds_repository(item):
        return False  # Only request-bound phase reconciliation may settle this launch.
    for task_id in list(item.get('stale_review_task_ids') or []):
        report = await mcp_call('get_task_report', {'task_id': task_id})
        cancelled_observed = False
        for job in state.get('work_queue', {}).values():
            if job.get('task_id') == task_id and (job.get('cancelled') or job.get('cancelled_review_turn')):
                # This RPC will never be replayed. Its requested turn may not exist;
                # drain the actual reviewer, including persisted pre-upgrade cancellations.
                status = report.get('status')
                job.update(cancelled=True, launch_pending=False,
                           status=status if status in ('DONE', 'FAILED', 'RUNNING') else 'UNKNOWN')
                job.pop('cancelled_review_turn', None)
                cancelled_observed = True
        if cancelled_observed:
            save_state(state)
        uncertain_launch = any(job.get('task_id') == task_id and job.get('launch_pending')
                               for job in state.get('work_queue', {}).values())
        if report.get('status') in ('DONE', 'FAILED') and uncertain_launch:
            item['stale_review_diagnostic'] = 'Invalidated reviewer launch is uncertain; the old terminal report does not prove the requested turn ended. Inspect worker receipts before recovery.'
            save_state(state)
            raise RuntimeError('Invalidated reviewer launch is uncertain')
        if report.get('status') in ('DONE', 'FAILED') and not uncertain_launch:
            item['stale_review_task_ids'].remove(task_id)
            save_state(state)
    if not item.get('stale_review_task_ids') and item.pop('stale_review_diagnostic', None) is not None:
        save_state(state)
    return not item.get('stale_review_task_ids')


def reviewer_for(coding_agent, project=None):
    """Resolve project review policy, preserving the legacy opposite-agent default."""
    if project is not None and "review_agent" in project:
        reviewer = project["review_agent"]
        if reviewer not in ("codex", "cursor"):
            raise ValueError(
                f"Invalid project review_agent: {reviewer!r}; "
                'set review_agent to "codex" or "cursor" in projects.json, '
                "or remove the field to use the opposite coding agent"
            )
        return reviewer

    if coding_agent == "codex":
        return "cursor"

    if coding_agent == "cursor":
        return "codex"

    raise ValueError(
        f"Unsupported coding agent for review: {coding_agent}"
    )


def review_contract():
    return f"""
The LAST non-empty line of the review MUST begin with:

{REVIEW_RESULT_PREFIX}

followed by one JSON object.

Required fields:
- verdict
- summary
- findings
- blocker

Allowed verdict values:
- PASS
- CHANGES_REQUIRED
- NEEDS_OWNER

Rules:

PASS:
- findings must be []
- blocker must be null

CHANGES_REQUIRED:
- findings must contain one or more concrete actionable findings
- blocker must be null

NEEDS_OWNER:
- blocker must contain one concise owner question/blocker

Do not print anything after the DEVORCHESTRATOR_REVIEW line.
""".strip()


def parse_review_result(text):
    lines = [
        line.strip()
        for line in str(text or "").splitlines()
        if line.strip()
    ]

    if not lines:
        return None, "empty reviewer result"

    last = lines[-1]

    if not last.startswith(REVIEW_RESULT_PREFIX):
        return None, (
            "missing DEVORCHESTRATOR_REVIEW trailer "
            "as the last non-empty line"
        )

    raw = last[len(REVIEW_RESULT_PREFIX):].strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"invalid review JSON: {exc}"

    if not isinstance(data, dict):
        return None, "review trailer must contain a JSON object"

    required = {
        "verdict",
        "summary",
        "findings",
        "blocker",
    }

    missing = sorted(required - set(data))

    if missing:
        return None, (
            "review trailer missing fields: "
            + ", ".join(missing)
        )

    verdict = data["verdict"]

    if not isinstance(verdict, str) or verdict not in {
        "PASS",
        "CHANGES_REQUIRED",
        "NEEDS_OWNER",
    }:
        return None, f"invalid review verdict: {verdict!r}"

    if (
        not isinstance(data["summary"], str)
        or not data["summary"].strip()
    ):
        return None, "review summary must be a non-empty string"

    findings = data["findings"]

    if not isinstance(findings, list):
        return None, "review findings must be a list"

    if any(
        not isinstance(value, str) or not value.strip()
        for value in findings
    ):
        return None, (
            "every review finding must be a non-empty string"
        )

    blocker = data["blocker"]

    if blocker is not None and (
        not isinstance(blocker, str)
        or not blocker.strip()
    ):
        return None, (
            "review blocker must be null or a non-empty string"
        )

    if verdict == "PASS":
        if findings:
            return None, "PASS cannot contain findings"

        if blocker is not None:
            return None, "PASS cannot contain a blocker"

    elif verdict == "CHANGES_REQUIRED":
        if not findings:
            return None, (
                "CHANGES_REQUIRED requires actionable findings"
            )

        if blocker is not None:
            return None, (
                "CHANGES_REQUIRED cannot contain an owner blocker"
            )

    elif verdict == "NEEDS_OWNER":
        if blocker is None:
            return None, (
                "NEEDS_OWNER requires an owner blocker/question"
            )

    return data, None


def load_state():
    state = read_json(STATE_FILE, {"issues": {}})
    if not isinstance(state, dict) or not isinstance(state.get("issues"), dict):
        raise ValueError("Invalid bridge state; refusing to start duplicate tasks")
    return bridge_telemetry.durable_state(state)


def observation_barrier(state, number, report=None):
    """One writer boundary for all read-only/cached observation consumers.

    A sticky hold prevents later reconciliation from regenerating effects from
    smoke state. It is cleared only by an audited, explicit owner recovery,
    never by removing the environment latch or by a later automatic poll.
    """
    from external_effects import blocked
    number = str(number)
    if blocked() or (report or {}).get('observation_confidence') == 'READ_ONLY':
        bridge_effects.hold_observation(state, number)
        save_state(state)  # Must precede any lifecycle application or dispatch.
        return True
    return bridge_effects.observation_held(state, number)


async def observe_latched_poll(state):
    """No lifecycle reducers, scheduler recovery or command consumption."""
    global WORK_CONTEXT
    WORK_CONTEXT = None
    for number, item in state['issues'].items():
        if not str(number).isdigit() or not isinstance(item, dict):
            continue
        # Hold before I/O, including failed reads and legacy smoke projections.
        if item.get('task_id') or item.get('review_task_id'):
            observation_barrier(state, number)
        for task_id in {item.get('task_id'), item.get('review_task_id')} - {None}:
            try:
                await rpc_io('get_task_report', {'task_id': task_id})
            except Exception:
                pass  # Failed observations cannot grant workflow authority.


def save_state(state):
    # Semantic workflow first; optional display telemetry is never recovery
    # authority. A failed checkpoint must stop the resumable lifecycle writer.
    try:
        changed = write_json_if_changed(STATE_FILE, bridge_telemetry.durable_state(state))
    except Exception as exc:
        if DRIVING.get():
            from review_coordinator import DurabilityFailure
            raise DurabilityFailure('Lifecycle checkpoint failed; writer must stop') from exc
        raise
    bridge_telemetry.save(STATE_FILE, state)
    return changed


def archive_issue(issue, item, state):
    reason = exclusion_reason(item, issue)
    if not reason:
        return False
    before = json.dumps(item, sort_keys=True)
    if item.get('review_transition') is not None:
        item['review_transition'] = review_transitions.invalidate(
            item['review_transition'], item.get('pending_rpc'))
    item.setdefault('runtime_exclusion', reason)
    if str(issue.get('state', '')).upper() == 'CLOSED':
        item['issue_closed'] = True
    number = str(issue['number'])
    task_ids = {t for t in (item.get('task_id'), item.get('review_task_id')) if t}
    jobs = [j for j in state.get('work_queue', {}).values() if str(j.get('owner')) == number or j.get('task_id') in task_ids]
    queue_changed = False
    for job in jobs:
        if str(job.get('owner')) == number and job.get('status') == 'QUEUED' and not job.get('launch_pending'):
            # No launch crossed the reservation boundary. Keep its receipt for
            # audit, but do not let archived FIFO intent block new admissions.
            job.update(status='CANCELLED', cancelled='archived_issue')
            queue_changed = True
    active = any(j.get('status') in ('RESERVED', 'RUNNING', 'UNKNOWN') or j.get('launch_pending') for j in jobs)
    unproven = not jobs and (item.get('coding_status') in ('RUNNING', 'STARTING', 'UNKNOWN') or item.get('pending_rpc'))
    if active or unproven:
        item.setdefault('consistency_diagnostics', {})['archival'] = 'Archived issue has active or uncertain runtime; ownership retained until terminal proof'
    else:
        item.setdefault('consistency_diagnostics', {}).pop('archival', None)
    if item.get('issue_closed') and str(issue.get('state', '')).upper() == 'OPEN':
        item.setdefault('consistency_diagnostics', {})['reopen'] = 'Reopened historical issue remains archived; create a new task issue'
    if json.dumps(item, sort_keys=True) != before or queue_changed:
        save_state(state)
    return True


def reconcile_projection(issue, item, state):
    if observation_barrier(state, issue['number']):
        return
    if exclusion_reason(item, issue):
        return
    global WORK_CONTEXT
    WORK_CONTEXT = (state, str(issue['number']), item)
    target = item.get('desired_label')
    current = lifecycle_state(item)
    if current == 'RUNNING':
        target = 'dev:running'
    elif current == 'AWAITING_CHIEF_REVIEW':
        target = 'dev:awaiting-chief-review'
    if target:
        if label_names(issue) & STATE_LABELS != {target}:
            if item.get('projection', {}).get('health') != 'ERROR':
                item['projection'] = {'health': 'DRIFT', 'target': target, 'failures': 0}
            transition_issue(issue, target)
        elif item.get('projection', {}).get('health') != 'ERROR':
            item['projection'] = {'health': 'SYNCED', 'target': target, 'failures': 0}
    if item.get('queue_status') == 'STARTED' and item.get('task_id'):
        add_comment(issue['number'], start_comment(item))
    save_state(state)


def start_comment(item):
    machine = item.get('coding_machine')
    if not machine:
        import devctl
        machine = devctl.load_projects().get(item.get('project'), {}).get('machine')
    return '\n'.join(['<!-- DEVORCHESTRATOR_BRIDGE_V1 -->', '## DevOrchestrator', '',
        f"Task started: `{item['task_id']}`", f"Project: `{item.get('project')}`",
        f"Agent: `{item.get('coding_agent')}`", f"Machine: `{machine}`"])


TERMINAL_NOTIFICATION_STATUSES = frozenset({'NEEDS_OWNER', 'FAILED', 'FRESH_REVIEW_REQUIRED',
    'READY_FOR_REVIEW', 'REVIEW_PASSED / READY_FOR_OWNER', 'REVIEW_PASSED / COMPLETE',
    'COMMITTED', 'PUSHED', 'PR_CREATED', 'MERGED'})


def prepare_notification(entry, item, issue=None):
    """Read-only eligibility fence, including persisted pre-upgrade events."""
    notification = entry.get('notification')
    if not notification and entry['payload'].split('\n', 1)[0] not in TERMINAL_NOTIFICATION_STATUSES:
        return True  # Existing independent transport/recovery diagnostics.
    if notification and notification['status'] == 'CHIEF_REVIEW_REQUIRED':
        record = item.get('chief_review') or {}
        try:
            identity = chief_review.validate_identity(record['identity'], item)
        except (ValueError, KeyError, TypeError):
            return False
        if (exclusion_reason(item, issue) or not record.get('complete') or record.get('verdict') or
                item.get('chief_status') != 'AWAITING_CHIEF_REVIEW' or
                notification['fingerprint'] != identity.bundle_id):
            return False
        return True
    if notification and notification['fingerprint'] != notification_fingerprint(item):
        return False
    return next_action(item) not in ('NONE', 'CHIEF_VERDICT')


def render_notification(entry, item, issue=None):
    """Render a transient send buffer after backoff and eligibility checks."""
    notification = entry.get('notification')
    if not notification and entry['payload'].split('\n', 1)[0] not in TERMINAL_NOTIFICATION_STATUSES:
        return entry['payload']
    if notification and notification['status'] == 'CHIEF_REVIEW_REQUIRED':
        return redact(notifications.terminal_text('CHIEF_REVIEW_REQUIRED', issue or notification['issue'], item,
            current_action='OPEN_CHATGPT_FOR_CHIEF_REVIEW'))
    status = notification['status'] if notification else lifecycle_state(item)
    if status == 'READY_FOR_OWNER':
        status = 'REVIEW_PASSED / READY_FOR_OWNER'
    view = issue or (notification or {}).get('issue') or {'number': entry['number']}
    return redact(notifications.terminal_text(status, view, item, item.get('notification_report'),
        item.get('owner_blocker') or (notification or {}).get('blocker'), current_action=next_action(item)))


def ready_owner_notification(entry, item):
    """Compute the invitation's freshness gate without rendering display text."""
    notification = entry.get('notification')
    if not notification and entry['payload'].split('\n', 1)[0] not in TERMINAL_NOTIFICATION_STATUSES:
        return entry['payload'].startswith('REVIEW_PASSED / READY_FOR_OWNER')
    status = notification['status'] if notification else lifecycle_state(item)
    return (status in ('READY_FOR_OWNER', 'REVIEW_PASSED / READY_FOR_OWNER') and
            notifications.review_approval_complete(item) and item.get('owner_state') == OWNER_STATE_READY and
            notifications.publication_delta_present(item, item.get('notification_report', {})) is not False)


def label_names(issue):
    return GitHubTransport.label_names(issue)


def github_task_transport():
    # One GitHub runtime, using the existing CLI/timeouts and V1 label mapping.
    # These callbacks perform I/O only; the adapter receives no lifecycle state.
    return GitHubTransport(REPO, run=gh, read_json=gh_json, state_labels=STATE_LABELS)


def raw_transition_issue(issue, target):
    return github_task_transport().transition_issue(issue, target)


def raw_add_comment(number, body):
    adapter = github_task_transport()
    return adapter.post_comment(adapter.issue_ref(number), body)




def transition_issue(issue, target):
    if not WORK_CONTEXT:
        return raw_transition_issue(issue, target)
    state, number, item = WORK_CONTEXT
    if observation_barrier(state, number):
        return
    if exclusion_reason(item, issue):
        return
    event = [target, item.get("task_id"), item.get("review_task_id"), item.get("review_round"),
             item.get("autopilot_corrections"), item.get("last_report_signature"),
             item.get("processed_followups")]
    changed = item.get("desired_label") != target
    item["desired_label"] = target
    key = bridge_effects.enqueue(state, number, "transition", {"issue": issue, "target": target}, event)
    entry = state["outbox"][key]
    if entry["status"] == "DONE" and (changed or (label_names(issue) & STATE_LABELS) != {target}):
        # Returning to an earlier desired state must not inherit its old delivery receipt.
        entry.update(status="PENDING", attempts=0, next_attempt=0, payload={"issue": issue, "target": target})
    save_state(state)


def add_comment(number, body):
    body = redact(body)
    if not WORK_CONTEXT:
        return raw_add_comment(number, body)
    state, _, item = WORK_CONTEXT
    if observation_barrier(state, number):
        return
    if exclusion_reason(item):
        return
    bridge_effects.enqueue(state, number, "comment", body)
    save_state(state)



def restore_merged_state(item, state):
    """Replay a persisted executor result without repeating any Git operation."""
    journal = (item.get('publication_journal') or {}).get('MERGE') or {}
    if (item.get('merged') is True and (journal.get('command') or {}).get('action') == 'MERGE' and
            item.get('owner_state') != 'MERGED'):
        item['owner_state'] = 'MERGED'
        journal['done'] = True
        save_state(state)


def merge_complete(item):
    from operational_state import merge_convergence_complete
    return item.get('owner_state') == 'MERGED' and item.get('merged') and merge_convergence_complete(item)


def project_merge(issue, item, state):
    if observation_barrier(state, issue['number']):
        return
    """Completion is a durable projection only, independent of Git history."""
    if not merge_complete(item):
        return
    item.setdefault('completed_at', datetime.now(timezone.utc).isoformat())
    item['desired_label'] = 'dev:done'
    bridge_effects.enqueue(state, issue['number'], 'completion', {'number': issue['number']},
                          ['merged', item.get('task_id'), item.get('commit_sha')])
    save_state(state)


def flush_effects(state, reconciled=True, control_fresh=None, telegram_dispatch=None, telegram_inflight=()):
    from transition_io import run_inline
    return run_inline(flush_effects_async(state, reconciled, control_fresh, telegram_dispatch, telegram_inflight))


async def flush_effects_async(state, reconciled=True, control_fresh=None, telegram_dispatch=None, telegram_inflight=(), *, owners=None):
    from external_effects import blocked
    if blocked():
        for number, item in state['issues'].items():
            if (isinstance(item, dict) and (item.get('task_id') or item.get('review_task_id')) and
                    (owners is None or str(number) in owners)):
                observation_barrier(state, number)
    telegram_issues = {}  # Fresh display views; never installed in the outbox.
    suppressed = False
    for entry in state.get('outbox', {}).values():
        if entry.get('status') == 'QUARANTINED' or entry.get('effect_origin') in ('safety_latch', 'legacy_unverified'):
            continue
        if owners is not None and str(entry['number']) not in owners:
            continue
        item = state.get('issues', {}).get(str(entry['number']), {})
        if not isinstance(item, dict):
            item = {}
        if entry['kind'] == 'completion' and merge_complete(item):
            continue
        if entry['kind'] == 'telegram' and entry['status'] == 'SENDING':
            continue
        if entry['status'] not in ('DONE', 'UNCERTAIN', 'FAILED') and exclusion_reason(item):
            entry.update(status='DONE', diagnostic='Suppressed: archived task')
            suppressed = True
            continue
        if entry['kind'] == 'telegram' and entry['status'] == 'PENDING' and not prepare_notification(entry, item):
            entry.update(status='DONE', diagnostic='Notification superseded before send')
            item.setdefault('consistency_diagnostics', {})['notification'] = 'Superseded notification dropped'
            suppressed = True
            continue
        if (entry['kind'] == 'telegram' and entry['status'] == 'PENDING' and
                ready_owner_notification(entry, item) and
                (not notifications.review_approval_complete(item) or item.get('owner_state') != OWNER_STATE_READY)):
            entry.update(status='DONE', diagnostic='Superseded: task has no current READY_FOR_OWNER approval')
            suppressed = True
    if suppressed:
        save_state(state)
    async def transition(issue, target):
        # Read current labels, since outbox payload may predate a restart or another transition.
        adapter = github_task_transport()
        item = state.get('issues', {}).get(str(issue['number']), {})
        if exclusion_reason(item) or item.get('desired_label', target) != target:
            return
        try:
            current = (await io_call('issue', dict(number=issue['number']), lane='effect'))
            if archive_issue(current, item, state):
                return
            (await io_call('transition', dict(issue=current, target=target), lane='effect'))
            observed = (await io_call('labels', dict(number=issue['number']), lane='effect'))
            if label_names(observed) & STATE_LABELS != {target}:
                raise ValueError('Projection not confirmed')
            item['projection'] = {'health': 'SYNCED', 'target': target, 'failures': 0}
            item.setdefault('consistency_diagnostics', {}).pop('projection', None)
        except Exception:
            failures = item.get('projection', {}).get('failures', 0) + 1
            item['projection'] = {'health': 'ERROR', 'target': target, 'failures': min(failures, 3)}
            if failures >= 3:
                item.setdefault('consistency_diagnostics', {})['projection'] = 'GitHub projection failed repeatedly; automatic retry pending'
            raise
    async def eligible(entry):
        item = state.get('issues', {}).get(str(entry['number']), {})
        if entry['kind'] == 'completion':
            return bool(merge_complete(item))
        if entry['kind'] == 'telegram' and telegram_dispatch is not None and len(telegram_inflight) >= 4:
            return False
        if exclusion_reason(item):
            return False
        if entry['kind'] == 'transition' and item.get('desired_label', entry['payload']['target']) != entry['payload']['target']:
            entry.update(status='DONE', diagnostic='Superseded projection')
            save_state(state)
            return False
        if entry['kind'] == 'chief_comment':
            record = item.get('chief_review') or {}
            if not isinstance(record, dict) or chief_review.malformed_runtime(item):
                return False
            current_effect = any(state.get('outbox', {}).get(key) is entry and
                chief_effect_valid(state, entry['number'], record, index)
                for index, key in enumerate(record.get('effects', [])))
            if not current_effect:
                if entry['status'] == 'PENDING':
                    entry.update(status='DONE', diagnostic='Chief evidence superseded before send')
                    save_state(state)
                return False
        if control_fresh is not None and str(entry['number']) not in control_fresh and (
                entry['kind'] == 'chief_comment' or
                (entry['kind'] == 'telegram' and (entry.get('notification') or {}).get('status') in
                    ('CHIEF_REVIEW_REQUIRED', 'REVIEW_PASSED / READY_FOR_OWNER')) or
                (entry['kind'] == 'telegram' and ready_owner_notification(entry, item)) or
                (entry['kind'] == 'transition' and entry['payload']['target'] == 'dev:ready-for-review')):
            return False
        if entry['kind'] in ('telegram', 'comment', 'chief_comment'):
            # A fresh issue observation at the send boundary prevents a close
            # during a long coding/review tick from leaking an owner alert.
            try:
                current = (await io_call('issue', dict(number=entry['number']), lane='effect'))
            except Exception:
                entry['attempts'] += 1
                entry['next_attempt'] = time.time() + min(300, 2 ** min(entry['attempts'], 8))
                entry['diagnostic'] = 'Issue observation unavailable; delivery deferred'
                save_state(state)
                return False
            if not current or archive_issue(current, item, state):
                return False
            if entry['kind'] == 'telegram' and (entry.get('notification') or {}).get('status') == 'CHIEF_REVIEW_REQUIRED':
                # A verdict may arrive between polling and delivery. Suppress the
                # wake-up without granting approval from this transport read.
                identity = (item.get('chief_review') or {}).get('identity') or {}
                if not (await chief_tree_current_async(item, state, str(entry['number']))):
                    return False
                try:
                    for comment in (await io_call('comments', dict(number=entry['number']), lane='effect')):
                        if not authorized_comment(comment) or chief_review.is_evidence_comment(comment.get('body')):
                            continue
                        try:
                            verdict = chief_review.parse_verdict(comment.get('body'))
                        except ValueError:
                            continue
                        if verdict and verdict['bundle_id'] == identity.get('bundle_id'):
                            entry.update(status='DONE', diagnostic='Chief verdict arrived before wake-up delivery')
                            save_state(state)
                            return False
                except Exception:
                    return False
            if entry['kind'] == 'telegram' and not prepare_notification(entry, item, current):
                entry.update(status='DONE', diagnostic='Notification superseded before send')
                item.setdefault('consistency_diagnostics', {})['notification'] = 'Superseded notification dropped'
                save_state(state)
                return False
            if entry['kind'] == 'telegram':
                telegram_issues[str(entry['number'])] = current
        if entry['kind'] == 'chief_comment':
            item = state.get('issues', {}).get(str(entry['number']), {})
            record = item.get('chief_review') or {}
            if not reconciled or chief_review.malformed_runtime(item):
                return False
            for index, key in enumerate(record.get('effects', [])):
                if state.get('outbox', {}).get(key) is entry:
                    return (chief_effect_valid(state, entry['number'], record, index) and
                            (await chief_tree_current_async(item, state, str(entry['number']))))
            return False
        return reconciled or not (
            (entry['kind'] == 'telegram' and ready_owner_notification(entry, item)) or
            (entry['kind'] == 'transition' and entry['payload']['target'] == 'dev:ready-for-review'))

    async def post(number, body):
        match = re.search(r'<!-- DEVORCHESTRATOR_EFFECT_V1 ([0-9a-f]{64}) -->$', body)
        entry = state.get('outbox', {}).get(match[1]) if match else None
        if entry and entry['kind'] == 'chief_comment':
            from devctl import DATA_DIR
            with ExitStack() as locks:
                try:
                    locks.enter_context(exclusive(DATA_DIR.parent / 'worker.lock', blocking=False))
                except BlockingIOError as exc:
                    # Only acquisition failed: no POST command has been yielded.
                    # Do not catch errors from tree checks, POST or lock release.
                    raise bridge_effects.CommentNotDispatched() from exc
                item = state.get('issues', {}).get(str(number), {})
                if not (await chief_tree_current_async(item, state, str(number))):
                    raise PublicationError('Chief tree changed before comment publication')
                (await io_call('post_comment', dict(number=number, body=body), lane='effect'))
                if not (await chief_tree_current_async(item, state, str(number))):
                    raise PublicationError('Chief tree changed during comment publication')
        else:
            (await io_call('post_comment', dict(number=number, body=body), lane='effect'))

    async def complete(number, key):
        item = state['issues'][str(number)]
        entry = state['outbox'][key]
        if not merge_complete(item):
            raise ValueError('Completion has no durable convergence proof')
        current = (await io_call('issue', dict(number=number), lane='effect'))
        marker = '<!-- DEVORCHESTRATOR_MERGE_COMPLETE ' + hashlib.sha256(
            json.dumps([str(number), item.get('task_id'), item.get('commit_sha')]).encode()).hexdigest() + ' -->'
        matches = [c for c in (await io_call('comments', dict(number=number), lane='effect'))
                   if marker in (c.get('body') or '') and not chief_review.is_evidence_comment(c.get('body'))]
        if len(matches) > 1:
            raise PublicationError('Completion comment receipt is ambiguous')
        if not matches:
            if entry.get('completion_comment') in ('SENDING', 'UNCERTAIN', 'DONE') or entry.get('delivery_version') != 2:
                entry['completion_comment'] = 'UNCERTAIN'
                save_state(state)
                raise PublicationError('Completion comment acceptance uncertain; reconcile existing receipt')
            from external_effects import blocked, ExternalEffectBlocked
            if blocked():
                raise ExternalEffectBlocked('github')
            entry['completion_comment'] = 'SENDING'
            save_state(state)
            (await io_call('post_comment', dict(number=number, body='Owner action MERGE: ALLOWED; state MERGED. Workflow completed.\n\n' + marker), lane='effect'))
        entry['completion_comment'] = 'DONE'
        save_state(state)
        (await io_call('transition', dict(issue=current, target='dev:done'), lane='effect'))
        if str(current.get('state', '')).upper() != 'CLOSED':
            (await io_call('github', dict(args=['issue', 'close', str(number), '-R', REPO]), lane='effect'))
        observed = (await io_call('issue', dict(number=number), lane='effect'))
        if str(observed.get('state', '')).upper() != 'CLOSED' or label_names(observed) & STATE_LABELS != {'dev:done'}:
            raise ValueError('Completion projection not confirmed')
        item['projection'] = {'health': 'SYNCED', 'target': 'dev:done', 'failures': 0}
    async def timed_send(text):
        with measure(state, 'telegram_callback'):
            return (await io_call('telegram', dict(text=text), lane='effect'))
    def render_telegram(entry):
        number = str(entry['number'])
        return render_notification(entry, state['issues'].get(number, {}), telegram_issues.get(number))
    with measure(state, 'outbox_flush'):
        await bridge_effects.flush_async(state, lambda: save_state(state), comments_io,
                            post, transition, timed_send, eligible=eligible, complete=complete,
                            telegram_dispatch=telegram_dispatch, telegram_inflight=telegram_inflight,
                            render_telegram=render_telegram,
                            selection={key for key, entry in state.get('outbox', {}).items()
                                       if owners is None or str(entry['number']) in owners})
    for number, item in state.get('issues', {}).items():
        if owners is not None and str(number) not in owners:
            continue
        if not isinstance(item, dict) or exclusion_reason(item):
            continue
        if bridge_effects.observation_held(state, number):
            continue
        pending = [e for e in state.get('outbox', {}).values() if str(e['number']) == str(number) and
                   e['kind'] in ('transition', 'comment', 'chief_comment') and e['status'] != 'DONE']
        if pending:
            from external_effects import blocked
            repeated = any(e.get('attempts', 0) >= 3 or e['status'] in ('UNCERTAIN', 'FAILED') for e in pending)
            # A smoke withhold is not a failed delivery. UNCERTAIN receipts still count.
            if not (blocked() and all(e.get('status') == 'PENDING' for e in pending)):
                item.setdefault('projection', {})['health'] = 'ERROR' if repeated else 'DRIFT'
                if repeated:
                    item.setdefault('consistency_diagnostics', {})['projection'] = 'External projection incomplete after repeated delivery failures'
        elif item.get('desired_label') and item.get('projection', {}).get('target') == item['desired_label']:
            item['projection']['health'] = 'SYNCED'
            item.setdefault('consistency_diagnostics', {}).pop('projection', None)
    save_state(state)


def parse_task(issue):
    body = issue.get("body") or ""
    match = TASK_RE.search(body)

    if not match:
        raise ValueError(
            "Missing DEVORCHESTRATOR_TASK_V1 metadata"
        )

    meta = json.loads(match.group(1))
    if not isinstance(meta, dict):
        raise ValueError("Task metadata must be an object")

    project = meta.get("project")
    agent = meta.get("agent")
    mode = meta.get("mode", "manual")
    model = meta.get("model")
    effort = meta.get("effort")

    if not isinstance(project, str) or not project:
        raise ValueError("Task metadata requires a registered project")
    if mode not in ("manual", AUTOPILOT_MODE) or agent not in (None, "codex", "cursor", "auto"):
        raise ValueError("Unsupported task mode/agent")
    for name, value in (("model", model), ("effort", effort)):
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise ValueError(f"Task {name} must be a non-empty string")
    if effort and not re.fullmatch(r"[a-z]+", effort):
        raise ValueError("Effort must be a simple reasoning-effort name")

    task_execution_request(issue)
    prompt = TASK_RE.sub("", body, count=1).strip()

    if not prompt:
        raise ValueError("Task prompt is empty")

    return project, agent, mode, model, effort, prompt


def task_execution_request(issue):
    """New execution fields stay out of the legacy six-value parse contract."""
    from execution_profiles import token, options
    match = TASK_RE.search(issue.get('body') or '')
    meta = json.loads(match.group(1)) if match else {}
    extra = {}
    if 'execution_profile' in meta:
        extra['execution_profile'] = token(meta['execution_profile'], 'execution_profile')
    if 'execution_options' in meta:
        extra['execution_options'] = options(meta['execution_options'], 'execution_options')
    return extra


async def transport_call(name, arguments):
    from external_effects import guard_worker_rpc
    guard_worker_rpc(name, arguments)
    return await asyncio.wait_for(_transport_call(name, arguments), timeout=120)


async def rpc_io(name, arguments):
    lane = 'work'
    if WORK_CONTEXT and name in ('get_task_report', 'list_tasks'):
        item = WORK_CONTEXT[2]
        if item.get('chief_review') or item.get('chief_required') or item.get('owner_state'):
            lane = 'control_rpc'
    result = await io_call('rpc', dict(name=name, arguments=arguments), lane=lane)
    if WORK_CONTEXT and (name in ('get_task_report', 'get_task_observation') or
                         (isinstance(result, dict) and result.get('observation_confidence') == 'READ_ONLY')):
        state, number, _ = WORK_CONTEXT
        if observation_barrier(state, number, result):
            from external_effects import ExternalEffectBlocked
            raise ExternalEffectBlocked('observation')
    return result


async def comments_io(number):
    return await io_call('comments', dict(number=number), lane='effect')


async def repository_io(repository, method, *args):
    return await io_call('repository', dict(project=repository.project, report=repository.report,
        configuration=repository.cfg, method=method, args=args))


def publication_authority(item):
    return review_transitions.digest({key: item.get(key) for key in (
        'task_id', 'coding_turn', 'coding_status', 'review_task_id', 'review_status',
        'review_coding_turn', 'reviewed_tree', 'review_hold', 'owner_approved_at',
        'owner_state', 'chief_required', 'chief_status', 'chief_review',
        'worker_uncertain', 'review_uncertain', 'owner_decision')})


def authorize_dispatch(number, command, state, issue):
    """Writer-only last check, including commands waiting for I/O capacity."""
    item = state['issues'][str(number)]
    if observation_barrier(state, number):
        from external_effects import ExternalEffectBlocked
        raise ExternalEffectBlocked('observation')
    if issue and str(issue.get('state', '')).upper() == 'CLOSED':
        archive_issue(issue, item, state)
    if command.authorization is not None:
        guard = json.loads(command.authorization)
        if (not issue or str(issue.get('state', 'OPEN')).upper() != 'OPEN' or
                guard.get('issue') != str(number) or guard.get('kind') != 'publication' or
                exclusion_reason(item, issue) or guard.get('reconcile_only') or
                publication_authority(item) != guard.get('quality') or
                not chief_review.approval_complete(item) or not __import__('owner_decision').publication_allowed(item) or item.get('review_hold') or
                owner_action_plan(item.get('owner_state'), guard.get('action'))['status'] != 'ALLOWED' or
                (item.get('review_status') == 'PASS' and not notifications.review_approval_complete(item))):
            raise PublicationError('Current issue and quality approval are required before a new publication mutation')
    elif command.name == 'rpc' and command.arguments().get('name') in ('start_task', 'continue_task'):
        if exclusion_reason(item, issue):
            raise QueueWait()  # Keep the original request for receipt reconciliation.
        from operational_state import publication_repository_hold, post_merge_repository_hold
        scheduler = Scheduler(state)
        job = scheduler.jobs.get(command.arguments()['arguments'].get('request_id'))
        if (publication_repository_hold(item) or post_merge_repository_hold(item) or
                (job and scheduler.repository_busy(str(number), job['machine'], job['repo']))):
            raise QueueWait()  # A later hold also fences an unsent reserved launch.


async def publish_resumable(repository, item, report, command, persist, *, reconcile_only=False):
    if DRIVING.get():
        from publication import execute_async
        reconcile_only = reconcile_only or (command['action'] == 'MERGE' and bool(item.get('merged')))
        state, number, _ = WORK_CONTEXT
        guard = dict(kind='publication', issue=str(number), action=command['action'], quality=publication_authority(item),
                     reconcile_only=reconcile_only)
        if not reconcile_only:
            from transition_io import Command, LATEST_ISSUE
            authorize_dispatch(number, Command.create('repository', {}, authorization=guard), state, LATEST_ISSUE.get())
        async def before_mutation():
            from transition_io import Command, LATEST_ISSUE
            boundary = Command.create('repository', {}, authorization=guard)
            authorize_dispatch(number, boundary, state, LATEST_ISSUE.get())
            # Publication reads cannot consume the slots reserved for polling.
            current = await io_call('issue', dict(number=number, purpose='publication_boundary'), lane='effect')
            if not current or str(current.get('number')) != str(number):
                raise PublicationError('Fresh issue state unavailable before publication mutation')
            archive_issue(current, item, state)
            authorize_dispatch(number, boundary, state, LATEST_ISSUE.get())
        return await execute_async(repository, item, report, command, persist,
            before_mutation=before_mutation, authorization=guard, reconcile_only=reconcile_only)
    return publish(repository, item, report, command, persist)


def archived_reconciliation_pending(item):
    return bool(item.get('pending_rpc') or any(not entry.get('done') for entry in
        (item.get('publication_journal') or {}).values() if isinstance(entry, dict)))


async def reconcile_archived(issue, item, state):
    """Reconcile existing authority; closed issues never authorize a new send."""
    number = str(issue['number'])
    if observation_barrier(state, number):
        return
    pending = item.get('pending_rpc')
    if pending:
        if pending.get('name') != 'start_task':
            # Historical uncertain continuations have no start-inspection proof.
            # Keep the same receipt/ownership; do not replay a closed generation.
            return
        key = pending['arguments']['request_id']
        proof = await rpc_io('inspect_start_request', dict(request_id=key, fence=True))
        if (proof.get('schema_version') != 1 or proof.get('request_id') != key or
                proof.get('task_id') != key[:24]):
            raise RuntimeError('Unbound archived request observation')
        job = state['work_queue'][key]
        if str(job.get('owner')) != number:
            raise RuntimeError('Archived request lost its original queue ownership')
        if proof.get('state') == 'FENCED' and proof.get('launch_started') is False:
            job.update(status='CANCELLED', launch_pending=False, no_launch_proof='FENCED')
            item.pop('pending_rpc')
            save_state(state)
        elif proof.get('state') in ('PENDING', 'STARTED'):
            meta = proof.get('metadata') or {}
            if (meta.get('launch_prepared') or meta.get('resume_prepared') or
                    meta.get('launch_request_id') != key or not meta.get('runtime_generation')):
                return
            report = await rpc_io('get_task_report', dict(task_id=key[:24]))
            if (report.get('id') != key[:24] or report.get('launch_request_id') != key or
                    report.get('runtime_generation') != meta['runtime_generation'] or
                    report.get('role', 'coding') != pending['arguments'].get('role', 'coding')):
                raise RuntimeError('Unbound archived launch receipt')
            Scheduler(state).started(key, report['id'], report.get('status'))
            Scheduler(state).observe(report['id'], report.get('status'), report.get('cleanup_state'))
            complete_rpc(item, pending['operation'], dict(report, task_id=report['id']))
            if pending['operation'] == 'initial':
                item.update(task_id=report['id'], project=pending['arguments']['project'],
                    coding_agent=report.get('agent'), coding_machine=report.get('machine'),
                    coding_status=report.get('status'), coding_turn=report.get('turns'), queue_status='STARTED')
            item.pop('pending_rpc')
            save_state(state)
        return
    for action, entry in (item.get('publication_journal') or {}).items():
        if not isinstance(entry, dict) or entry.get('done') or not entry.get('command'):
            continue
        try:
            report = await rpc_io('get_task_report', dict(task_id=item['task_id']))
            machine, repo_key = await io_call('project_identity', dict(project=item['project']))
            if Scheduler(state).repository_busy(number, machine, repo_key):
                return
            from devctl import DATA_DIR
            with exclusive(DATA_DIR.parent / 'worker.lock', blocking=False):
                await publish_resumable(Repository(item['project'], report), item, report,
                    entry['command'], lambda: save_state(state), reconcile_only=True)
            if entry.get('done'):
                item['owner_state'] = OWNER_ACTION_RULES[action][1]
                item.get('consistency_diagnostics', {}).pop('archived_publication', None)
                save_state(state)
        except (PublicationError, BlockingIOError):
            item.setdefault('consistency_diagnostics', {})['archived_publication'] = (
                'Archived publication receipt retained; no new mutation authorized. Inspect bound Git/GitHub outcome.')
            save_state(state)
        return


async def _transport_call(name, arguments):
    from mcp import Client
    from external_effects import blocked
    tool_name = name
    if blocked():
        # A distinct tool name fails closed on old MCP servers. Unknown mode
        # arguments can be silently dropped by an older SDK's input model.
        tool_name = {'get_task_report': 'get_task_observation',
                     'list_tasks': 'list_task_observations'}.get(name, name)
    async with Client(MCP_URL) as client:
        result = await client.call_tool(tool_name, arguments)

    return decode_mcp_result(name, result)


# Return contracts from mcp_server.py. A project registry is an object even though
# it contains multiple projects; only list_tasks has collection-valued output.
MCP_COLLECTION_TOOLS = frozenset({"list_tasks", "list_task_observations"})
MCP_OBJECT_TOOLS = frozenset({"list_projects", "get_task_report", "start_task",
                              "continue_task", "adopt_task", "orchestrator_health", "get_execution_catalog", "preflight_task",
                              "get_transition_capabilities", "inspect_start_request", "get_task_observation"})


def decode_mcp_result(name, result):
    """Decode every SDK content block before exposing a result to bridge callers.

    MCP 2.3.0 unrolls a Python list into one JSON text block per element, including
    zero blocks for an empty list. Older transports can instead emit one JSON list.
    Single-result tools must never silently select one of several blocks.
    """
    def fail(reason):
        # Report shape/location only: tool payloads can contain prompts or secrets.
        raise RuntimeError(f"MCP tool {name}: {reason}")

    if getattr(result, "is_error", False) or getattr(result, "isError", False):
        fail("returned an error result")
    content = getattr(result, "content", None)
    if not isinstance(content, (list, tuple)):
        fail("expected a content block sequence")

    values = []
    for index, part in enumerate(content, 1):
        text = getattr(part, "text", None)
        if getattr(part, "type", None) != "text" or not isinstance(text, str) or not text.strip():
            fail(f"content block {index} must contain JSON text")
        try:
            values.append(json.loads(text))
        except json.JSONDecodeError:
            fail(f"content block {index} contains malformed JSON")

    if name in MCP_COLLECTION_TOOLS:
        if len(values) == 1 and isinstance(values[0], list):
            tasks = values[0]
        elif all(isinstance(value, dict) for value in values):
            tasks = values
        else:
            fail("expected one JSON collection or JSON task objects in every block")
        # Validate the whole list before recovery can change capacity/lock state.
        # Legacy tasks may lack project/workspace; recovery already holds these.
        ids = set()
        for index, task in enumerate(tasks, 1):
            if not isinstance(task, dict) or any(
                    not isinstance(task.get(key), str) or not task[key].strip()
                    for key in ("id", "status")):
                fail(f"task element {index} requires non-empty string id/status")
            if task["id"] in ids:
                fail(f"task element {index} has a duplicate id")
            ids.add(task["id"])
        structured = getattr(result, "structured_content", None)
        if structured is not None:
            if (not isinstance(structured, dict) or set(structured) != {"result"}
                    or structured["result"] != tasks):
                fail("structured collection disagrees with content blocks")
        return tasks

    if len(values) != 1:
        fail("expected exactly one JSON result block")
    value = values[0]
    if name in MCP_OBJECT_TOOLS and not isinstance(value, dict):
        fail("expected a JSON object result")
    return value


# Context is set by the serial bridge loop; admission is durably recorded before RPC.
WORK_CONTEXT = None
# Attempt-local observations never belong to the authoritative item. A caller
# may share this small dict with wait_for's child to diagnose a timeout after
# cancellation; it carries no lifecycle state or external payloads.
RECONCILIATION_PROGRESS = ContextVar('reconciliation_progress', default=None)


def observe_reconciliation_stage(stage):
    progress = RECONCILIATION_PROGRESS.get()
    if progress is not None:
        progress['stage'] = stage


def complete_rpc(item, operation, result):
    if operation.startswith("followup:"):
        cid = operation.split(":", 1)[1]
        item["processed_followups"] = sorted(set(item.get("processed_followups") or []) | {cid})
        item["last_report_signature"] = None
    elif operation.startswith("autopilot:"):
        item["autopilot_corrections"] = int(operation.rsplit(":", 1)[1])
        item["last_report_signature"] = None
    elif operation.startswith("review-correction:"):
        item.update(review_corrections=int(operation.rsplit(":", 1)[1]), review_task_id=None,
                    reviewer_agent=None, review_status=None, review_start_tree=None,
                    review_format_corrections=0, review_format_restarts=0,
                    autopilot_corrections=0, last_report_signature=None)
        item.pop('review_coding_turn', None)
    elif operation.startswith("review-format:"):
        item["review_format_corrections"] = int(operation.rsplit(":", 1)[1])
        item["review_status"] = "RUNNING"
    elif operation.startswith("review:"):
        chief_review.invalidate(item)
        item.update(owner_state=None, owner_approved_at=None)
        item.update(review_task_id=result.get("task_id") or result["id"],
                    reviewer_agent=result.get("agent"), review_round=int(operation.split(":")[1]),
                    review_status="RUNNING", review_format_corrections=0)
        item['review_task_turn'] = result.get('turns', 1)


async def mcp_call(name, arguments):
    observe_reconciliation_stage(name)
    if not WORK_CONTEXT or name not in ("start_task", "continue_task"):
        result = await rpc_io(name, arguments)
        if WORK_CONTEXT and name == "get_task_report":
            state, _, _ = WORK_CONTEXT
            Scheduler(state).observe(arguments["task_id"], result.get("status"), result.get('cleanup_state'))
            save_state(state)
        return result
    from external_effects import blocked, ExternalEffectBlocked
    if blocked():
        raise ExternalEffectBlocked('worker')
    state, number, item = WORK_CONTEXT
    if observation_barrier(state, number):
        raise ExternalEffectBlocked('observation')
    from operational_state import post_merge_repository_hold
    if post_merge_repository_hold(item) or review_transitions.holds_repository(item):
        # Keep the original quality gates and merge journal intact for retry.
        # No coding/review turn may start on a possibly half-updated workspace.
        raise QueueWait()
    arguments = dict(arguments)
    operation = arguments.pop("_operation", "initial")
    kind = arguments.get("role", "coding")
    if operation.startswith(('review:', 'review-format:')) and kind != 'review':
        raise ValueError('Review RPC requires the read-only review role')
    if kind == 'review':
        current = await mcp_call('get_task_report', {'task_id': item['task_id']})
        observe_coding_turn(item, state, current)
        if not coding_report_successful(current, item) or item.get('review_coding_turn') != current.get('turns'):
            raise QueueWait()
    pending = item.get('pending_rpc')
    if pending and (pending.get('name') != name or pending.get('operation') != operation or
                    {k: v for k, v in pending.get('arguments', {}).items() if k != 'request_id'} !=
                    {k: v for k, v in arguments.items() if k != 'request_id'}):
        raise RuntimeError('Pending launch intent differs from this RPC; existing ownership must be reconciled')
    observe_reconciliation_stage('project_identity')
    project = arguments.get('project') or item['project']
    if DRIVING.get() and name == 'start_task' and kind == 'coding' and not pending:
        binding = await io_call('project_identity', dict(project=project, binding=True))
        machine, repo = binding['machine'], binding['repository']
        arguments.setdefault('repository_binding', binding)
        # Keep this first envelope across the acceptance-to-item-adoption crash window.
        if operation == 'initial' and item.get('task_spec'):
            item['task_spec']['arguments'].setdefault('repository_binding', arguments['repository_binding'])
    else:
        machine, repo = await io_call('project_identity', dict(project=project))
    key = hashlib.sha256(f"{REPO}:{number}:{name}:{operation}".encode()).hexdigest()
    if pending and pending.get('arguments', {}).get('request_id', key) != key:
        raise RuntimeError('Pending launch request ID changed; existing ownership must be reconciled')
    scheduler = Scheduler(state)
    job = scheduler.enqueue(key, number, kind, machine, repo)
    if (job.get('owner'), job.get('kind'), job.get('machine'), job.get('repo')) != (number, kind, machine, repo):
        raise RuntimeError('Registered repository changed; original request ownership retained')
    if job['status'] == 'QUEUED' and not scheduler.available(job):
        raise QueueWait()
    async def rejected(exc):
        # Only a worker's confirmed no-launch result reaches this path. Generic
        # MCP/network errors and ambiguous receipts retain their reservations.
        if item.get('pending_rpc') or job.get('launch_pending'):
            from execution_profiles import RuntimeUnavailable
            if name != 'start_task':
                raise RuntimeUnavailable('Rejected invocation cannot settle an older uncertain continuation') from exc
            # The error concerns this invocation. A delayed earlier invocation
            # may still launch; only a worker tombstone settles the whole ID.
            fence = await rpc_io('inspect_start_request', {'request_id': key, 'fence': True})
            if (fence.get('schema_version') != 1 or fence.get('request_id') != key or
                    fence.get('task_id') != key[:24] or fence.get('state') != 'FENCED' or
                    fence.get('launch_started') is not False):
                raise RuntimeUnavailable('Uncertain original start retained pending request reconciliation') from exc
        job.update(status='FAILED', launch_pending=False, preflight_error=redact(str(exc))[:512])
        item.pop('pending_rpc', None)
        scheduler.release(number)
        save_state(state)
        exc.role = kind
    try:
        observe_reconciliation_stage('preflight_task')
        preflight = await rpc_io('preflight_task', {
            'operation': name, 'arguments': {**arguments, 'request_id': key}})
        check_preflight(preflight)
    except PreflightRejected as exc:
        await rejected(exc)
        raise
    if DRIVING.get():
        from transition_io import LATEST_ISSUE
        latest = LATEST_ISSUE.get()
        if latest and str(latest.get('number')) == str(number) and archive_issue(latest, item, state):
            raise QueueWait()  # A close observed during preflight cannot authorize a new launch.
    if not item.get("pending_rpc"):
        item["pending_rpc"] = {"name": name, "arguments": {**arguments, 'request_id': key}, "operation": operation}
        if kind == "coding" and name == "continue_task":
            invalidate_quality_gates(item)
            item['coding_status'] = 'STARTING'
            if item.get('coding_turn'):
                item['coding_turn'] += 1
        save_state(state)
    scheduler.reserve(key)
    job["task_id"] = arguments.get("task_id") or key[:24]
    job["launch_pending"] = True
    arguments["request_id"] = key
    save_state(state)
    try:
        observe_reconciliation_stage(name)
        result = await rpc_io(name, arguments)
        if result.get('status') in ('REJECTED', 'UNAVAILABLE', 'STALE_INPUT', 'FENCED'):
            check_preflight(result)
    except PreflightRejected as exc:
        # Capability/catalog changes between preflight and the worker's locked
        # preparation still report a definitive prelaunch failure.
        await rejected(exc)
        raise
    scheduler.started(key, result.get("task_id") or arguments.get("task_id") or result["id"], result.get("status"))
    complete_rpc(item, operation, result)
    if kind == 'coding' and type(result.get('turns')) is int:
        item['coding_turn'] = max(item.get('coding_turn', 0), result['turns'])
    item.pop("pending_rpc", None)
    save_state(state)
    return result


def gh_pages(endpoint):
    return github_task_transport().pages(endpoint)


def open_issues():
    adapter = github_task_transport()
    return [adapter.issue_view(command) for command in adapter.poll_commands()]


def fetch_issue(number):
    adapter = github_task_transport()
    return adapter.fetch_issue(adapter.issue_ref(number))


def issue_comments(number):
    adapter = github_task_transport()
    return adapter.issue_comments(adapter.issue_ref(number))


def comment_id(comment):
    return str(
        comment.get("id")
        or comment.get("url")
        or (
            str(comment.get("createdAt", ""))
            + ":"
            + (comment.get("body") or "")[:100]
        )
    )


def comment_already_processed(comment, processed):
    # REST numeric IDs and GraphQL node IDs describe the same GitHub comment.
    # Keep the original RPC operation ID intact, but backfill aliases durably.
    aliases = {comment_id(comment)} | {str(comment[field]) for field in ('id', 'node_id', 'url', 'html_url')
                                      if comment.get(field)}
    if processed & aliases:
        processed.update(aliases)
        return True
    return False


def format_report(report):
    result = report.get("result")
    error = report.get("error")

    text = [
        "<!-- DEVORCHESTRATOR_REPORT_V1 -->",
        "## DevOrchestrator report",
        "",
        f"- **Task:** `{report.get('id')}`",
        f"- **Status:** `{report.get('status')}`",
        f"- **Machine:** `{report.get('machine')}`",
        f"- **Agent:** `{report.get('agent')}`",
        f"- **Turns:** `{report.get('turns')}`",
    ]

    if report.get("session_id"):
        text.append(
            f"- **Session:** `{report['session_id']}`"
        )

    health = report.get('liveness')
    if isinstance(health, dict):
        text.extend(runtime_evidence_lines(health))

    if report.get('execution'):
        text.append('- **Execution configuration:** `' + json.dumps(report['execution'], sort_keys=True) + '`')
    if report.get("model"):
        text.append(
            f"- **Model:** `{report['model']}`"
        )

    if report.get("reasoning_effort"):
        text.append(
            f"- **Effort:** `{report['reasoning_effort']}`"
        )

    if result:
        text += [
            "",
            "### Result",
            "",
            str(result)[:40000],
        ]

    if error:
        text += [
            "",
            "### Error",
            "",
            str(error)[:10000],
        ]

    return "\n".join(text)


def runtime_evidence_lines(health):
    return [
        f"- **Runtime:** `{health.get('state')}` — {health.get('reason')}",
        f"- **Phase:** `{health.get('phase')}`; **lease:** `{health.get('lease_state')}`",
        f"- **Heartbeat age (seconds):** `{health.get('heartbeat_age_seconds')}`",
        f"- **Worker owned/alive:** `{health.get('worker_alive')}`; **agent process owned/alive:** `{health.get('process_alive')}`",
        f"- **Runtime generation:** `{health.get('generation')}`; **transition:** `{health.get('transition_sequence')}`",
    ]


def project_runtime_health(issue, item, state, report, slot='coding'):
    """Project evidence through the existing outbox, never advance a workflow.

    Suspected/ambiguous workers retain scheduler ownership. No retry, kill,
    review or publication is authorized by these observations. External labels
    cannot override the durable local report, including on duplicate ticks.
    """
    if observation_barrier(state, issue['number'], report):
        return True
    if exclusion_reason(item, issue):
        return False
    health = report.get('liveness')
    if not isinstance(health, dict):
        return False  # V1/test transports without the additive contract.
    item.setdefault('runtime_health', {})[slot] = health
    if report.get('cleanup_state') == 'AMBIGUOUS':
        item.setdefault('consistency_diagnostics', {})['runtime_' + slot] = 'Terminal runtime ownership ambiguous; capacity and publication held'
    elif report.get('cleanup_state') in ('ABSENT', 'CLEANED'):
        item.setdefault('consistency_diagnostics', {}).pop('runtime_' + slot, None)
    if health.get('reason') == 'heartbeat_not_registered':
        return False  # Legacy tasks retain their existing process/exit handling.
    runtime_state = health.get('state')
    if runtime_state not in ('SUSPECTED_STALL', 'LOST_ORPHANED'):
        if runtime_state == 'RUNNING_HEALTHY':
            item.setdefault('runtime_observations', {}).pop(slot, None)
        return False
    lost = runtime_state == 'LOST_ORPHANED'
    item['review_uncertain' if slot == 'review' else 'worker_uncertain'] = True
    transition_issue(issue, 'dev:failed' if lost else 'dev:needs-owner')
    signature = [report.get('id'), health.get('generation'), health.get('transition_sequence'),
                 runtime_state, health.get('reason')]
    observations = item.setdefault('runtime_observations', {})
    if observations.get(slot) != signature:
        diagnostic = f"{slot.capitalize()} task {report.get('id')}: {runtime_state}: {health.get('reason')}. No automatic recovery is scheduled."
        add_comment(issue['number'], '\n'.join([
            '<!-- DEVORCHESTRATOR_RUNTIME_V1 -->', diagnostic, *runtime_evidence_lines(health)]))
        notify('FAILED' if lost else 'NEEDS_OWNER', issue, item, report, diagnostic)
        observations[slot] = signature
        save_state(state)
    return report.get('status') not in ('DONE', 'FAILED')


async def start_queued(issue, state, *, progress=None):
    number = str(issue["number"])
    global WORK_CONTEXT
    WORK_CONTEXT = (state, number, state["issues"].setdefault(number, {"queue_status": "QUEUED"}))
    if archive_issue(issue, state['issues'][number], state):
        return
    from external_effects import blocked
    if blocked():
        return

    progress = progress if progress is not None else {}
    token = RECONCILIATION_PROGRESS.set(progress)
    observe_reconciliation_stage('tracked_issue')
    try:
        queued = state["issues"][number]
        spec = queued.get("task_spec")
        pending = queued.get("pending_rpc")
        if not spec and pending and pending.get("name") == "start_task":
            # Preserve pre-rollout request signatures, including their exact prompt.
            args = dict(pending["arguments"])
            spec = {"arguments": args, "mode": AUTOPILOT_MODE if
                    args.get("prompt", "").startswith("DEVORCHESTRATOR AUTOPILOT MODE") else "manual"}
        if not spec:
            project, agent, mode, model, effort, prompt = parse_task(issue)
            if mode == AUTOPILOT_MODE:
                prompt = build_autopilot_prompt(prompt)
            args = {"project": project, "prompt": coding_prompt(prompt)}
            for name, value in (("agent", agent), ("model", model), ("effort", effort)):
                if value:
                    args[name] = value
            args.update(task_execution_request(issue))
            spec = {"arguments": args, "mode": mode}
        args = dict(spec["arguments"])
        project, mode = args["project"], spec["mode"]
        model, effort = args.get("model"), args.get("effort")
        projects = __import__("devctl").load_projects()
        if project not in projects:
            raise ValueError("Unknown registered project")
        if not pending:
            # Pure configuration validation before admission. An uncertain RPC
            # must replay its original request and saved identity after config edits.
            __import__('execution_profiles').resolve(
                projects[project], agent=args.get('agent'), model=model, effort=effort,
                execution_profile=args.get('execution_profile'), execution_options=args.get('execution_options'))
        queued["task_spec"] = spec

        result = await mcp_call("start_task", args)

        task_id = result["task_id"]

        queued = state["issues"][number]
        queued.pop("reconciliation_retry", None)
        queued.get('consistency_diagnostics', {}).pop('reconciliation', None)
        state["issues"][number] = {
            **queued,
            "queue_status": "STARTED",
            "task_id": task_id,
            "project": project,
            "coding_agent": result.get("agent"),
            "coding_machine": result.get("machine"),
            "coding_status": result.get('status', 'UNKNOWN'),
            "coding_session_id": result.get('session_id'),
            "mode": mode,
            "autopilot_policy_version": (
                AUTOPILOT_POLICY_VERSION
                if mode == AUTOPILOT_MODE
                else None
            ),
            "execution": result.get("execution"),
            "execution_options": result.get("execution_options", {}),
            "model": result.get('model') or model,
            "effort": result.get('reasoning_effort') or effort,
            "autopilot_corrections": 0,
            "review_policy_version": REVIEW_POLICY_VERSION,
            "review_task_id": None,
            "review_round": 0,
            "review_corrections": 0,
            "review_format_corrections": 0,
            "review_format_restarts": 0,
            "review_status": None,
            "reviewer_agent": None,
            "owner_state": None,
            "processed_owner_actions": [],
            "commit_sha": None,
            "pushed_branch": None,
            "pr_url": None,
            "merged": False,
            "processed_followups": [],
            "last_report_signature": None,
        }

        WORK_CONTEXT = (state, number, state["issues"][number])
        save_state(state)

        transition_issue(issue, "dev:running")

        add_comment(issue['number'], start_comment(state['issues'][number]))
        flush_effects(state)

        print(
            f"[bridge] issue #{number} -> task {task_id}",
            flush=True,
        )

    except QueueWait:
        return
    except ValueError as exc:
        item = state["issues"][number]
        item["queue_status"] = "FAILED"
        item["owner_blocker"] = str(exc)
        Scheduler(state).release(number)
        transition_issue(issue, "dev:failed")
        notify("FAILED", issue, item, blocker=str(exc))
        save_state(state)
    except Exception as exc:
        from external_effects import ExternalEffectBlocked
        if isinstance(exc, ExternalEffectBlocked):
            return
        # An RPC failure may mean the agent started. Keep its reservation and retry the same request ID.
        defer_issue(state["issues"][number], state, number, exc, stage=progress.get('stage'))
    finally:
        RECONCILIATION_PROGRESS.reset(token)



def authorized_comment(comment):
    # Structured commands are not authorization by themselves. GitHub supplies author identity.
    return (comment.get("author") or {}).get("login", "").lower() in owner_logins()


def owner_logins():
    return {login.strip().lower() for login in os.environ.get(
        "DEVORCHESTRATOR_OWNERS", REPO.split("/")[0]).split(",") if login.strip()}


def stale_chief(item, state, number, diagnostic):
    """Revoke quality gates, retaining the old Chief receipt as historical evidence."""
    if exclusion_reason(item):
        return
    chief_review.invalidate(item)
    item.update(owner_state=None, owner_approved_at=None, reviewed_tree=None,
                review_status='FRESH_REVIEW_REQUIRED',
                review_hold={'diagnostic': diagnostic, 'previous_status': 'PASS'})
    item['desired_label'] = 'dev:failed'
    bridge_effects.enqueue(state, number, 'transition',
        {'issue': {'number': int(number)}, 'target': 'dev:failed'},
        ['chief-stale', item.get('task_id'), item.get('coding_turn'), item.get('review_round')])
    save_state(state)


def chief_tree_current(item, state, number):
    if exclusion_reason(item):
        return False
    from external_effects import ExternalEffectBlocked
    try:
        chief_review.validate_identity(item['chief_review']['identity'], item)
        if Repository(item['project']).snapshot() != item['reviewed_tree']:
            stale_chief(item, state, number, 'Repository changed after independent PASS; fresh independent review and Chief bundle required.')
            return False
        return True
    except (PublicationError, ExternalEffectBlocked):
        return False  # Unavailable evidence cannot authorize a send or verdict.
    except (ValueError, KeyError, TypeError):
        stale_chief(item, state, number, 'Chief identity is stale; fresh independent review and Chief bundle required.')
        return False


async def chief_tree_current_async(item, state, number):
    if exclusion_reason(item):
        return False
    from external_effects import ExternalEffectBlocked
    try:
        chief_review.validate_identity(item['chief_review']['identity'], item)
        snapshot = await repository_io(Repository(item['project']), 'snapshot')
        if snapshot != item['reviewed_tree']:
            stale_chief(item, state, number, 'Repository changed after independent PASS; fresh independent review and Chief bundle required.')
            return False
        return True
    except (PublicationError, ExternalEffectBlocked):
        return False  # Unavailable evidence cannot authorize a send or verdict.
    except (ValueError, KeyError, TypeError):
        stale_chief(item, state, number, 'Chief identity is stale; fresh independent review and Chief bundle required.')
        return False


def chief_effect_key(number, bundle_id, index, body):
    event = ['chief-bundle', bundle_id, index, hashlib.sha256(body.encode()).hexdigest()]
    return hashlib.sha256(json.dumps([str(number), 'chief_comment', event], sort_keys=True).encode()).hexdigest()


def chief_effect_valid(state, number, record, index):
    key = record['effects'][index]
    entry = state.get('outbox', {}).get(key)
    return (isinstance(entry, dict) and entry.get('kind') == 'chief_comment' and
            str(entry.get('number')) == str(number) and isinstance(entry.get('payload'), str) and
            len(entry['payload'].encode()) <= 50000 and
            chief_effect_key(number, record['identity']['bundle_id'], index, entry['payload']) == key)


def chief_backfill_pending(item):
    """Unpublished independent PASS stays on the heavy path until evidence exists."""
    return (not exclusion_reason(item) and item.get('review_status') == 'PASS' and
            not item.get('chief_review') and not item.get('publication_journal') and
            item.get('owner_state') not in OWNER_STATE_SEQUENCE[1:])


async def process_chief_review(issue, item, state, report, *, allow_evidence=True, evidence=None):
    """Backfill pending independent PASS before the existing owner flow executes.

    Reuse its task/turn/round/tree; historical or interrupted publication is not
    retroactively enrolled. Return True while Chief review owns this task.
    """
    if review_transitions.holds_repository(item):
        return True
    if exclusion_reason(item, issue):
        return False
    if item.get('owner_state') in OWNER_STATE_SEQUENCE[1:] or item.get('publication_journal'):
        return False
    if item.get('review_status') != 'PASS':
        return False
    if not allow_evidence and not item.get('chief_review'):
        # Legacy independent approval cannot authorize publication during the
        # evidence gap. Only the heavy reader can build the missing Chief record;
        # there is no Chief ownership for the control caller to act on yet.
        item['chief_required'] = True
        save_state(state)
        return False
    current = await mcp_call('get_task_report', {'task_id': item['task_id']})
    observe_coding_turn(item, state, current)
    if not coding_report_successful(current, item) or item.get('review_status') != 'PASS':
        return True
    report = current
    try:
        identity = chief_review.build_identity(item)
    except ValueError:
        return False  # Existing historical-review holds handle missing evidence.
    item.update(chief_required=True, owner_state=None, owner_approved_at=None)
    record = item.get('chief_review')
    if record and record['identity'] != identity.to_dict():
        chief_review.invalidate(item)
        record = None
    if not record:
        item['chief_status'] = 'AWAITING_CHIEF_REVIEW'
        transition_issue(issue, 'dev:awaiting-chief-review')
    save_state(state)

    from devctl import DATA_DIR
    from external_effects import ExternalEffectBlocked
    with exclusive(DATA_DIR.parent / 'worker.lock', blocking=not DRIVING.get()):
        repository = Repository(item['project'], report)
        try:
            snapshot = await repository_io(repository, 'snapshot')
        except ExternalEffectBlocked:
            snapshot = None
        if snapshot is None:
            if not record:
                return True
        elif snapshot != item['reviewed_tree']:
            stale_chief(item, state, str(issue['number']), 'Repository changed after independent PASS; fresh review required.')
            return True
        if snapshot is not None and not record:
            coding_data, _ = parse_autopilot_result(report.get('result'))
            if coding_data:
                verification = {name: coding_data[name] for name in ('implementation', 'verification', 'self_review', 'state')}
                verification['summary'] = chief_review.sanitize(coding_data['summary'])[:500]
                item['chief_verification_summary'] = json.dumps(verification, ensure_ascii=False)
            else:
                item['chief_verification_summary'] = chief_review.sanitize(str(report.get('result') or ''))[:2000]
            if not item.get('independent_review_summary') and item.get('review_task_id'):
                # Backfill reads an existing completed review; never relaunches it.
                try:
                    review = await mcp_call('get_task_report', {'task_id': item['review_task_id']})
                    data, _ = parse_review_result(review.get('result'))
                except ExternalEffectBlocked:
                    raise
                except Exception:
                    data = None  # Durable PASS/tree evidence does not depend on log retention.
                item['independent_review_summary'] = chief_review.sanitize(data['summary'])[:2000] if (
                    data and data['verdict'] == 'PASS') else 'Independent PASS recorded in durable bridge state; reviewer summary unavailable.'
            if evidence is None:
                with measure(state, 'chief_evidence'):
                    try:
                        evidence = await repository_io(repository, 'review_evidence', snapshot)
                    except ExternalEffectBlocked:
                        return True
            bodies = chief_review.evidence_comments(identity, item['project'],
                repository.cfg.get('github_repo') or snapshot.get('origin') or '(local registered repository)',
                item, report, evidence)
            try:
                current_snapshot = await repository_io(repository, 'snapshot')
            except ExternalEffectBlocked:
                return True
            if current_snapshot != snapshot:
                stale_chief(item, state, str(issue['number']), 'Repository changed during Chief evidence extraction; fresh review required.')
                return True
            keys = [bridge_effects.enqueue(state, issue['number'], 'chief_comment', body,
                ['chief-bundle', identity.bundle_id, index, hashlib.sha256(body.encode()).hexdigest()])
                for index, body in enumerate(bodies)]
            record = item['chief_review'] = dict(identity=identity.to_dict(), revision=1,
                                                effects=keys, complete=False, verdict=None)
            save_state(state)

    comments = (await io_call('comments', dict(number=issue['number'])))
    complete = True
    for index, key in enumerate(record['effects']):
        if not chief_effect_valid(state, issue['number'], record, index):
            complete = False
            break
        entry = state['outbox'][key]
        expected = entry['payload'] + '\n\n' + f'<!-- DEVORCHESTRATOR_EFFECT_V1 {key} -->'
        matches = [c for c in comments if c.get('body') == expected]
        if len(matches) != 1:
            complete = False
        else:
            entry['status'] = 'DONE'  # Prove acceptance across timeout/crash.
    record['complete'] = complete
    if not complete:
        item['chief_status'] = 'AWAITING_CHIEF_REVIEW'
        save_state(state)
        transition_issue(issue, 'dev:awaiting-chief-review')
        return True

    processed = set(item.get('processed_chief_comments') or [])
    accepted_source = record.get('verdict')
    if accepted_source:
        sources = [c for c in comments if comment_id(c) == accepted_source['source']]
        if (len(sources) != 1 or hashlib.sha256((sources[0].get('body') or '').encode()).hexdigest() != accepted_source['source_revision']):
            record['conflict'] = True
            item['chief_status'] = 'CONFLICT'
            add_comment(issue['number'], 'Accepted Chief verdict was edited or removed; fresh review required.')
    for comment in comments:
        if chief_review.is_evidence_comment(comment.get('body')) or not authorized_comment(comment):
            continue
        cid = comment_id(comment)
        if comment_already_processed(comment, processed):
            continue
        try:
            data = chief_review.parse_verdict(comment.get('body'))
            if data is None:
                continue
            if not re.fullmatch(r'[1-9][0-9]*', str(comment.get('id'))):
                raise ValueError('Chief command requires a stable GitHub comment id')
            if data['bundle_id'] != identity.bundle_id:
                raise ValueError('Chief verdict rejected: stale or wrong bundle_id')
            if record.get('conflict'):
                raise ValueError('Conflicting Chief verdicts; fresh review required')
            previous = record.get('verdict')
            if previous:
                if previous['data'] != data:
                    record['conflict'] = True
                    item['chief_status'] = 'CONFLICT'
                    raise ValueError('Conflicting Chief verdict; approval blocked pending fresh review')
            else:
                with exclusive(DATA_DIR.parent / 'worker.lock', blocking=not DRIVING.get()):
                    current = await mcp_call('get_task_report', {'task_id': item['task_id']})
                    observe_coding_turn(item, state, current)
                    if not coding_report_successful(current, item) or not (await chief_tree_current_async(item, state, str(issue['number']))):
                        return True
                    record['verdict'] = dict(data=data, source=cid, revision=1,
                        timestamp=datetime.now(timezone.utc).isoformat(),
                        source_revision=hashlib.sha256(comment['body'].encode()).hexdigest(),
                        source_timestamp=chief_review.sanitize(str(comment.get('createdAt') or ''))[:128],
                        actor=chief_review.sanitize((comment.get('author') or {}).get('login', ''))[:128])
                    save_state(state)
            processed.add(cid)
        except ValueError as exc:
            processed.add(cid)
            add_comment(issue['number'], str(exc))
    item['processed_chief_comments'] = sorted(processed)
    save_state(state)
    if record.get('conflict'):
        item['chief_status'] = 'CONFLICT'
        transition_issue(issue, 'dev:failed')
        return True
    accepted = record.get('verdict')
    if not accepted:
        item['chief_status'] = 'AWAITING_CHIEF_REVIEW'
        transition_issue(issue, 'dev:awaiting-chief-review')
        notify('CHIEF_REVIEW_REQUIRED', issue, item, report)
        return True
    if accepted['data']['verdict'] == 'PASS':
        item.update(chief_status='PASS', owner_state=OWNER_STATE_READY,
                    owner_approved_at=accepted['timestamp'])
        save_state(state)
        transition_issue(issue, 'dev:ready-for-review')
        notify('REVIEW_PASSED / READY_FOR_OWNER', issue, item, report)
        return False

    item['chief_status'] = 'CHANGES_REQUIRED'
    save_state(state)
    data = accepted['data']
    prompt = ('CHIEF ENGINEER: CHANGES REQUIRED\n\n' + data['summary'] + '\n\n' +
              '\n'.join('- ' + finding for finding in data['findings']) +
              '\n\nContinue in the SAME coding session. Fix these findings, run relevant checks, '
              'self-review and rerun verification. A fresh independent review and Chief bundle are required.\n\n' +
              autopilot_contract())
    await mcp_call('continue_task', {'task_id': item['task_id'],
        '_operation': 'chief-correction:' + identity.bundle_id, 'prompt': coding_prompt(prompt)})
    transition_issue(issue, 'dev:running')
    return True


async def process_owner_actions(issue, item, state, report):
    if exclusion_reason(item, issue):
        return
    if not chief_review.approval_complete(item):
        return
    if report.get("status") not in ("DONE", "FAILED") or item.get("review_status") == "RUNNING":
        return
    if item.get('review_hold'):
        return
    if item.get('review_status') == 'PASS' and not coding_report_successful(report, item):
        return
    if (item.get('review_status') == 'PASS' and item.get('review_coding_turn') != report.get('turns') and
            item.get('owner_state') and item.get('owner_approved_at') and item.get('reviewed_tree')):
        hold_review_for_refresh(issue, item, state,
            'Independent review has no verified coding-turn binding. Send a trusted follow-up for a fresh independent review before publication.', report)
        return
    if item.get('review_status') == 'PASS' and not notifications.review_approval_complete(item):
        hold_review_for_refresh(issue, item, state,
            'Historical review PASS lacks owner approval or reviewed-tree evidence. Send a trusted follow-up for a fresh independent review before publication.', report)
        return
    if item.get("owner_state") in OWNER_STATE_SEQUENCE[1:]:
        if item.get('merged') and not merge_complete(item):
            notify('POST_MERGE_BLOCKED', issue, item, report, item.get('owner_blocker'))
        else:
            notify(item["owner_state"], issue, item, report)
    processed = set(item.get("processed_owner_actions") or [])
    for comment in (await io_call('comments', dict(number=issue["number"]))):
        if chief_review.is_evidence_comment(comment.get('body')):
            continue
        cid = comment_id(comment)
        if comment_already_processed(comment, processed) or not authorized_comment(comment):
            continue
        try:
            import owner_decision
            decision = owner_decision.parse(comment.get('body'))
            if decision is not None:
                approved = item.get('owner_approved_at')
                created = comment.get('createdAt')
                if not approved or not created or datetime.fromisoformat(created.replace('Z', '+00:00')) <= datetime.fromisoformat(approved):
                    raise PublicationError('Owner decision must follow current Chief PASS')
                owner_decision.apply(item, comment['body'], cid)
                processed.add(cid)
                item['processed_owner_actions'] = sorted(processed)
                save_state(state)
                continue  # Quality decision alone performs no publication/notification.
            command = parse_owner_command(comment.get("body"))
            if not command:
                continue
            if command["action"] == "ANSWER":
                # Defer the answer to process_followup so it follows the coding resume path.
                continue
            if not owner_decision.publication_allowed(item):
                raise PublicationError('Owner rejected or decision binding changed; publication remains blocked')
            plan = owner_action_plan(item.get("owner_state"), command["action"])
            if command['action'] == 'MERGE' and item.get('merged') and not merge_complete(item):
                plan = {'status': 'ALLOWED', 'next_state': 'MERGED'}
            if plan["status"] == "REJECTED":
                raise PublicationError(plan["reason"])
            if plan["status"] == "ALLOWED":
                approved = item.get("owner_approved_at")
                created = comment.get("createdAt")
                if not approved or not created or datetime.fromisoformat(created.replace("Z", "+00:00")) <= datetime.fromisoformat(approved):
                    raise PublicationError("Owner action must be issued after the current independent review PASS")
                machine, repo_key = (await io_call('project_identity', dict(project=item["project"])))
                if Scheduler(state).repository_busy(str(issue["number"]), machine, repo_key):
                    raise PublicationError("Repository has another active or uncertain task")
                from devctl import DATA_DIR
                with exclusive(DATA_DIR.parent / "worker.lock", blocking=not DRIVING.get()):
                    if item.get('chief_required'):
                        current_report = await mcp_call('get_task_report', {'task_id': item['task_id']})
                        observe_coding_turn(item, state, current_report)
                        if not coding_report_successful(current_report, item) or not chief_review.approval_complete(item):
                            raise PublicationError('Coding turn changed before owner publication; fresh quality gates required')
                    repository = Repository(item["project"], report)
                    tasks = await rpc_io("list_tasks", {"control": True, "project": item["project"]})
                    for task in tasks:
                        if (task.get("status") not in ("RUNNING", "UNKNOWN", "STARTING") and
                                task.get('cleanup_state') != 'AMBIGUOUS'):
                            continue
                        if task.get("machine") not in (None, "-", machine):
                            continue
                        try:
                            active_project = registered_project(task)
                            active_machine, active_repo = (await io_call('project_identity', dict(project=active_project)))
                        except Exception:
                            raise PublicationError("Worker repository identity is uncertain")
                        if (active_machine, active_repo) == (machine, repo_key):
                            raise PublicationError("Registered workspace has active or uncertain worker work")
                    with measure(state, 'publication_action'):
                        if command['action'] == 'MERGE':
                            Scheduler(state).hold_publication_repository(str(issue['number']), machine, repo_key)
                            save_state(state)
                        (await publish_resumable(repository, item, report, command, lambda: save_state(state)))
                item["owner_state"] = plan["next_state"]
                if item["owner_state"] == "MERGED":
                    if merge_complete(item):
                        project_merge(issue, item, state)
                    else:
                        item['owner_blocker'] = (item.get('post_merge_convergence') or {}).get('diagnostic') or 'PR merged; explicit post-merge convergence result unavailable'
                        notify('POST_MERGE_BLOCKED', issue, item, report, item['owner_blocker'])
            processed.add(cid)
            item["processed_owner_actions"] = sorted(processed)
            save_state(state)
            if plan["status"] == "ALLOWED" and not (item.get("merged") and not merge_complete(item)):
                notify(item["owner_state"], issue, item, report)
            add_comment(issue["number"], f"Owner action {command['action']}: {plan['status']}; state {item['owner_state']}")
        except (ValueError, PublicationError) as exc:
            # Retry interrupted actions only on a new explicit owner command; keep the journal.
            processed.add(cid)
            item["processed_owner_actions"] = sorted(processed)
            save_state(state)
            add_comment(issue["number"], f"Owner action rejected/failed: {exc}")
            notify("FAILED", issue, item, report, str(exc))
    if sorted(processed) != item.get('processed_owner_actions'):
        item['processed_owner_actions'] = sorted(processed)
        save_state(state)


async def process_followup(issue, item, state, report):
    if exclusion_reason(item, issue):
        return report
    if review_transitions.holds_repository(item):
        return report  # Intake records intent; no command is consumed while launch is unsettled.
    from operational_state import post_merge_repository_hold
    if post_merge_repository_hold(item):
        # Defer without consuming comments or invalidating gates. Owner commands
        # must still run to resolve an uncertain merge or retry convergence.
        return report
    if report.get("status") not in ("DONE", "FAILED") or item.get("review_status") == "RUNNING":
        return report

    processed = set(item.get("processed_followups") or [])

    for comment in (await io_call('comments', dict(number=issue["number"]))):
        body = comment.get("body") or ""

        if 'DEVORCHESTRATOR_OWNER_DECISION_V1' in body or chief_review.is_evidence_comment(body) or not authorized_comment(comment):
            continue
        cid = comment_id(comment)
        if comment_already_processed(comment, processed):
            continue
        try:
            command = parse_owner_command(body)
        except ValueError:
            continue
        if command and command["action"] == "ANSWER":
            outstanding = (item.get("review_status") == "NEEDS_OWNER" or
                           terminal_state(report, item)[0] == "NEEDS_OWNER")
            question_at = item.get("owner_question_at")
            created = comment.get("createdAt")
            if not outstanding or not item.get("owner_blocker"):
                processed.add(cid)
                continue
            if not question_at:
                # Legacy questions get a fresh epoch; old answers cannot authorize a new turn.
                item["owner_question_at"] = datetime.now(timezone.utc).isoformat()
                question_at = item["owner_question_at"]
            try:
                fresh = bool(created) and datetime.fromisoformat(created.replace("Z", "+00:00")) > datetime.fromisoformat(question_at)
            except (TypeError, ValueError):
                fresh = False
            if not fresh:
                processed.add(cid)
                continue
            prompt = "OWNER ANSWER TO: " + item["owner_blocker"] + "\n\n" + command["message"]
        elif FOLLOWUP_MARKER in body:
            prompt = body.split(FOLLOWUP_MARKER, 1)[1].strip()
        else:
            continue
        if item.get("mode") == AUTOPILOT_MODE:
            prompt = build_autopilot_prompt(prompt)

        if not prompt:
            processed.add(cid)
            continue
        prompt = coding_prompt(prompt)

        result = await mcp_call(
            "continue_task",
            {
                "task_id": item["task_id"],
                "_operation": "followup:" + cid,
                "prompt": prompt,
            },
        )

        processed.add(cid)
        item["processed_followups"] = sorted(processed)
        item["last_report_signature"] = None

        invalidate_quality_gates(item)

        save_state(state)
        transition_issue(issue, "dev:running")

        add_comment(
            issue["number"],
            "\n".join([
                "<!-- DEVORCHESTRATOR_BRIDGE_V1 -->",
                "Follow-up sent to the same agent session.",
                "",
                f"Task: `{item['task_id']}`",
            ]),
        )

        print(
            f"[bridge] follow-up -> {item['task_id']}",
            flush=True,
        )

        return result

    item["processed_followups"] = sorted(processed)
    save_state(state)
    return report



async def maybe_continue_autopilot(
    issue,
    item,
    state,
    report,
):
    if exclusion_reason(item, issue):
        return report, False
    if item.get("mode") != AUTOPILOT_MODE or item.get("execution_failure"):
        return report, False

    if report.get("status") not in ('DONE', 'FAILED'):
        return report, False

    decision, reason, _ = autopilot_decision(
        report,
        item,
    )

    if decision in {
        "READY_FOR_REVIEW",
        "NEEDS_OWNER",
        "FAILED",
    }:
        return report, False

    corrections = int(
        item.get("autopilot_corrections") or 0
    )

    if corrections >= MAX_AUTOPILOT_CORRECTIONS:
        print(
            f"[bridge] autopilot corrections exhausted "
            f"for {item['task_id']}: {reason}",
            flush=True,
        )
        return report, False

    attempt = corrections + 1

    followup = f"""
DEVORCHESTRATOR AUTOPILOT CORRECTIVE ITERATION
{attempt}/{MAX_AUTOPILOT_CORRECTIONS}

Your previous turn was NOT accepted by the AutoPilot policy.

Reason:
{reason}

Continue from the SAME repository and SAME agent session.
Do not restart the task from scratch.

Inspect the actual current state and previous changes.

If work is incomplete or verification/self-review failed:
- finish or correct the implementation;
- run relevant tests/build/checks;
- review the resulting diff;
- fix defects;
- rerun checks.

Do not commit, push, create a PR, merge, deploy, or release.
Publication requires separate structured owner commands after independent review.

{autopilot_contract()}
""".strip()

    new_report = await mcp_call(
        "continue_task",
        {
            "task_id": item["task_id"],
            "_operation": f"autopilot:{report.get('turns')}:{attempt}",
            "prompt": coding_prompt(followup),
        },
    )

    item["autopilot_corrections"] = attempt
    item["last_report_signature"] = None
    save_state(state)

    transition_issue(issue, "dev:running")

    add_comment(
        issue["number"],
        "\n".join([
            "<!-- DEVORCHESTRATOR_AUTOPILOT_V2 -->",
            (
                f"AutoPilot corrective iteration "
                f"{attempt}/{MAX_AUTOPILOT_CORRECTIONS} started."
            ),
            "",
            f"Reason: {reason}",
            f"Task: `{item['task_id']}`",
        ]),
    )

    print(
        f"[bridge] autopilot continue {item['task_id']} "
        f"{attempt}/{MAX_AUTOPILOT_CORRECTIONS}: {reason}",
        flush=True,
    )

    return new_report, True




def build_review_prompt(issue, coding_report):
    body = issue.get("body") or ""
    owner_task = TASK_RE.sub("", body, count=1).strip()

    coding_result = str(
        coding_report.get("result") or ""
    )

    return f"""
DEVORCHESTRATOR INDEPENDENT REVIEW

You are a fresh independent reviewer.

IMPORTANT:
- Do NOT implement fixes.
- Do NOT edit source files.
- Do NOT commit, push, merge, deploy, or create a PR.
- Inspect the actual repository/workspace, current diff and relevant code.
- Verify the coding agent's claims against the repository.
- Look for correctness bugs, regressions, missing edge cases,
  architecture violations, unsafe behavior, and insufficient tests.
- Do not invent issues just to produce findings.
- If the implementation is correct and sufficiently verified, PASS it.
- If concrete fixable problems exist, use CHANGES_REQUIRED.
- Use NEEDS_OWNER only for a genuine owner/product/architecture decision.

ORIGINAL TASK:

{owner_task}

CODING AGENT FINAL REPORT:

{coding_result}

{review_contract()}
""".strip()


def format_independent_review(review_report, review_data=None, error=None):
    text = [
        "<!-- DEVORCHESTRATOR_REVIEW_V1 -->",
        "## Independent review",
        "",
        f"- **Review task:** `{review_report.get('id')}`",
        f"- **Reviewer:** `{review_report.get('agent')}`",
    ]

    if review_report.get("model"):
        text.append(
            f"- **Model:** `{review_report.get('model')}`"
        )

    if review_report.get("reasoning_effort"):
        text.append(
            f"- **Effort:** `{review_report.get('reasoning_effort')}`"
        )

    if review_data:
        text += [
            f"- **Verdict:** `{review_data.get('verdict')}`",
            "",
            "### Summary",
            "",
            str(review_data.get("summary") or ""),
        ]

        findings = review_data.get("findings") or []

        if findings:
            text += [
                "",
                "### Findings",
                "",
            ]
            text += [
                f"- {finding}"
                for finding in findings
            ]

        if review_data.get("blocker"):
            text += [
                "",
                "### Owner blocker",
                "",
                str(review_data["blocker"]),
            ]

    elif error:
        text += [
            "- **Verdict:** `INVALID_REVIEW_RESULT`",
            "",
            "### Error",
            "",
            str(error),
        ]

    return "\n".join(text)


async def start_independent_review(
    issue,
    item,
    state,
    coding_report,
):
    if exclusion_reason(item, issue):
        return
    if not coding_report_successful(coding_report, item):
        return
    if DRIVING.get():
        from review_coordinator import ReviewCoordinator
        from review_driver import inputs
        ReviewCoordinator(state, save_state, REPO).begin(str(issue['number']), coding_report, inputs(issue, item))
        return
    project = item.get("project")

    if not project:
        raise RuntimeError(
            "Review loop requires project in task state"
        )

    coding_agent = (
        item.get("coding_agent")
        or coding_report.get("agent")
    )

    project_config = __import__("devctl").load_projects()[project]
    try:
        reviewer = reviewer_for(coding_agent, project_config)
    except ValueError as exc:
        rejection = PreflightRejected(f"Review configuration for project {project}: {exc}")
        rejection.role = "review"
        raise rejection from exc

    round_number = int(item.get("review_round") or 0) + 1
    args = {
        "project": project,
        "agent": reviewer,
        "role": "review",
        "_operation": f"review:{round_number}",
        "prompt": build_review_prompt(
            issue,
            coding_report,
        ),
    }

    chief_review.invalidate(item)
    item["owner_state"] = None
    item["owner_approved_at"] = None
    if not item.get("review_start_tree"):
        observe_reconciliation_stage('review_snapshot')
        item["review_start_tree"] = await repository_io(Repository(project, coding_report), 'snapshot')
    item['review_coding_turn'] = coding_report['turns']
    save_state(state)
    result = await mcp_call(
        "start_task",
        args,
    )

    item["review_task_id"] = result["task_id"]
    item["reviewer_agent"] = reviewer
    item["review_round"] = round_number
    item["review_format_corrections"] = 0
    item["review_status"] = "RUNNING"

    save_state(state)
    transition_issue(issue, "dev:running")

    # Preserve the coding result in the GitHub history before review.
    add_comment(
        issue["number"],
        format_report(coding_report),
    )

    add_comment(
        issue["number"],
        "\n".join([
            "<!-- DEVORCHESTRATOR_REVIEW_START_V1 -->",
            "## Independent review started",
            "",
            f"- Coding task: `{item['task_id']}`",
            f"- Coding agent: `{coding_agent}`",
            f"- Review task: `{result['task_id']}`",
            f"- Reviewer: `{reviewer}`",
            f"- Review round: `{item['review_round']}`",
        ]),
    )

    print(
        f"[bridge] review started "
        f"{result['task_id']} for {item['task_id']} "
        f"with {reviewer}",
        flush=True,
    )


async def fail_review_loop(
    issue,
    item,
    state,
    message,
    coding_report,
):
    item["review_status"] = "FAILED"
    item["owner_blocker"] = message
    save_state(state)

    transition_issue(issue, "dev:failed")

    add_comment(
        issue["number"],
        "\n".join([
            "<!-- DEVORCHESTRATOR_REVIEW_FAILURE_V1 -->",
            "## Review loop failed",
            "",
            str(message),
        ]),
    )

    notify("FAILED", issue, item, coding_report, message)


def hold_review_for_refresh(issue, item, state, diagnostic, coding_report=None, active=False):
    if exclusion_reason(item, issue):
        return
    if item.get('review_hold') is None:
        item['review_hold'] = {'diagnostic': diagnostic, 'previous_status': item.get('review_status'),
                              'review_task_id': item.get('review_task_id')}
    if not active:
        item['review_status'] = 'FRESH_REVIEW_REQUIRED'
    else:
        item['review_status'] = 'RUNNING'
    save_state(state)
    transition_issue(issue, 'dev:needs-owner')
    add_comment(issue['number'], item['review_hold']['diagnostic'])
    notify('FRESH_REVIEW_REQUIRED', issue, item, coding_report, item['review_hold']['diagnostic'])



async def process_review_loop(
    issue,
    item,
    state,
    coding_report,
):
    if exclusion_reason(item, issue):
        return False
    if item.get("mode") != AUTOPILOT_MODE:
        return False
    if item.get("execution_failure"):
        return True

    observe_coding_turn(item, state, coding_report)
    if not coding_report_successful(coding_report, item):
        return coding_report.get('status') not in ('DONE', 'FAILED')
    coding_report = await mcp_call('get_task_report', {'task_id': item['task_id']})
    observe_coding_turn(item, state, coding_report)
    if not coding_report_successful(coding_report, item):
        return True
    if not await drain_stale_reviews(item, state):
        return True

    decision, _, _ = autopilot_decision(
        coding_report,
        item,
    )

    if decision != "READY_FOR_REVIEW":
        return False

    if (item.get('review_status') == 'PASS' and item.get('review_coding_turn') != coding_report.get('turns') and
            item.get('owner_state') and item.get('owner_approved_at') and item.get('reviewed_tree')):
        hold_review_for_refresh(issue, item, state,
            'Independent review has no verified coding-turn binding. Send a trusted follow-up for a fresh independent review before publication.', coding_report)
        return True
    if item.get('review_status') == 'PASS' and not notifications.review_approval_complete(item):
        hold_review_for_refresh(issue, item, state,
            'Historical review PASS lacks owner approval or reviewed-tree evidence. Send a trusted follow-up for a fresh independent review before publication.', coding_report)
        return True

    review_task_id = item.get('review_task_id')
    if item.get('review_hold') or (review_task_id and not item.get('review_start_tree') and
                                  item.get('review_status') not in ('PASS', 'NEEDS_OWNER', 'FAILED')):
        active = False
        if review_task_id and item.get('review_status') not in ('PASS', 'NEEDS_OWNER', 'FAILED', 'FRESH_REVIEW_REQUIRED'):
            report = await mcp_call('get_task_report', {'task_id': review_task_id})
            active = report.get('status') not in ('DONE', 'FAILED')
        hold_review_for_refresh(issue, item, state,
            'Independent review has no recorded start-tree snapshot; its result cannot approve publication. '
            'The existing reviewer is monitored until terminal. Then send a trusted follow-up for a fresh independent review.',
            coding_report, active=active)
        return True

    # A terminal review state must not be processed repeatedly.
    if item.get("review_status") in {"PASS", "NEEDS_OWNER", "FAILED"}:
        status = item["review_status"]
        transition_issue(issue, "dev:ready-for-review" if status == "PASS" else
                         "dev:needs-owner" if status == "NEEDS_OWNER" else "dev:failed")
        notification_status = ('REVIEW_PASSED / READY_FOR_OWNER' if item.get('owner_state') == OWNER_STATE_READY
                               else item.get('owner_state')) if status == 'PASS' else status
        notify(notification_status,
               issue, item, coding_report, item.get("owner_blocker"))
        return True

    review_task_id = item.get("review_task_id")

    if not review_task_id:
        await start_independent_review(
            issue,
            item,
            state,
            coding_report,
        )
        return True

    review_report = await mcp_call(
        "get_task_report",
        {"task_id": review_task_id},
    )

    current = await mcp_call('get_task_report', {'task_id': item['task_id']})
    observe_coding_turn(item, state, current)
    if (not coding_report_successful(current, item) or item.get('review_task_id') != review_task_id or
            item.get('review_coding_turn') != current.get('turns')):
        return True
    coding_report = current

    review_process_status = review_report.get("status")
    if type(review_report.get('turns')) is int:
        item['review_task_turn'] = review_report['turns']

    if project_runtime_health(issue, item, state, review_report, 'review'):
        item['review_uncertain'] = True
        return True

    if review_process_status in {"RUNNING", "UNKNOWN"}:
        if review_process_status == "UNKNOWN":
            item["review_uncertain"] = True
            diagnostic = "Reviewer state is uncertain; inspect the worker before recovering. No duplicate reviewer will start."
            transition_issue(issue, "dev:needs-owner")
            notify("NEEDS_OWNER", issue, item, coding_report, diagnostic)
        elif item.pop("review_uncertain", False):
            transition_issue(issue, "dev:running")
        return True
    item.pop("review_uncertain", None)

    if review_process_status == "FAILED":
        await fail_review_loop(
            issue,
            item,
            state,
            (
                "Independent reviewer process failed: "
                + str(review_report.get("error") or "unknown error")
            ),
            coding_report,
        )
        return True

    review_data, review_error = parse_review_result(
        review_report.get("result")
    )

    if review_error:
        retries = int(
            item.get("review_format_corrections") or 0
        )

        if retries < MAX_REVIEW_FORMAT_CORRECTIONS:
            retries += 1

            await mcp_call(
                "continue_task",
                {
                    "task_id": review_task_id,
                    "role": "review",
                    "_operation": f"review-format:{review_task_id}:{retries}",
                    "prompt": f"""
Your review result was rejected because:

{review_error}

Do not change repository files.

Return the review again using the required machine-readable contract.

{review_contract()}
""".strip(),
                },
            )

            item["review_format_corrections"] = retries
            item["review_status"] = "RUNNING"
            save_state(state)

            add_comment(
                issue["number"],
                "\n".join([
                    "<!-- DEVORCHESTRATOR_REVIEW_RETRY_V1 -->",
                    (
                        "Reviewer result format retry "
                        f"{retries}/{MAX_REVIEW_FORMAT_CORRECTIONS}"
                    ),
                    "",
                    f"Reason: {review_error}",
                ]),
            )

            return True

        add_comment(
            issue["number"],
            format_independent_review(
                review_report,
                error=review_error,
            ),
        )

        restarts = int(item.get("review_format_restarts") or 0)
        if restarts < MAX_REVIEW_FORMAT_RESTARTS:
            # Persist the fallback intent before any launch. The next tick derives
            # review:<round+1>, and pending_rpc/worker receipts retain that identity
            # across an interrupted start. Keep the original start-tree snapshot;
            # a fresh reviewer must not silently approve changes by the old one.
            item.update(review_format_restarts=restarts + 1, review_task_id=None,
                        reviewer_agent=None, review_format_corrections=0,
                        review_status="RUNNING", last_report_signature=None)
            save_state(state)
            transition_issue(issue, "dev:running")
            add_comment(issue["number"], "\n".join([
                "<!-- DEVORCHESTRATOR_REVIEW_FORMAT_RESTART_V1 -->",
                f"Reviewer `{review_task_id}` still returned malformed output after format correction.",
                f"Fresh independent reviewer session queued ({restarts + 1}/{MAX_REVIEW_FORMAT_RESTARTS}).",
                "The coding session is unchanged; publication remains blocked.",
                "",
                f"Reason: {review_error}",
            ]))
            return True

        await fail_review_loop(
            issue,
            item,
            state,
            (
                f"Reviewer formatting failed after {restarts + 1} independent sessions "
                f"and {MAX_REVIEW_FORMAT_CORRECTIONS} format correction(s) per session: "
                + review_error
            ),
            coding_report,
        )
        return True

    add_comment(
        issue["number"],
        format_independent_review(
            review_report,
            review_data=review_data,
        ),
    )

    verdict = review_data["verdict"]

    if verdict == "PASS":
        from devctl import DATA_DIR
        with exclusive(DATA_DIR.parent / 'worker.lock', blocking=not DRIVING.get()):
            current = await mcp_call('get_task_report', {'task_id': item['task_id']})
            observe_coding_turn(item, state, current)
            if (not coding_report_successful(current, item) or item.get('review_task_id') != review_task_id or
                    item.get('review_coding_turn') != current.get('turns')):
                return True
            reviewed = await repository_io(Repository(item['project'], current), 'snapshot')
            if reviewed != item.get("review_start_tree"):
                await fail_review_loop(issue, item, state,
                                       "Repository changed during independent review; fresh review required", current)
                return True
            item["reviewed_tree"] = reviewed
            item["review_status"] = "PASS"
            item["chief_required"] = True
            item["chief_status"] = "AWAITING_CHIEF_REVIEW"
            item["independent_review_summary"] = chief_review.sanitize(review_data["summary"])[:2000]
            item["owner_state"] = None
            item["owner_approved_at"] = None
            item["owner_blocker"] = None
            item["owner_question_at"] = None
            save_state(state)

        await process_chief_review(issue, item, state, current)

        print(
            f"[bridge] independent review PASS "
            f"for {item['task_id']}",
            flush=True,
        )

        return True

    if verdict == "NEEDS_OWNER":
        item["owner_blocker"] = review_data["blocker"]
        item["owner_question_at"] = datetime.now(timezone.utc).isoformat()
        item["review_status"] = "NEEDS_OWNER"
        save_state(state)

        transition_issue(
            issue,
            "dev:needs-owner",
        )

        notify("NEEDS_OWNER", issue, item, coding_report, review_data["blocker"])

        return True

    # CHANGES_REQUIRED
    corrections = int(
        item.get("review_corrections") or 0
    )

    if corrections >= MAX_REVIEW_CORRECTIONS:
        await fail_review_loop(
            issue,
            item,
            state,
            (
                "Independent review still requires changes after "
                f"{MAX_REVIEW_CORRECTIONS} correction rounds."
            ),
            coding_report,
        )
        return True

    findings = review_data.get("findings") or []

    findings_text = "\n".join(
        f"- {finding}"
        for finding in findings
    )

    correction_prompt = f"""
INDEPENDENT REVIEW: CHANGES REQUIRED

A fresh independent reviewer found these concrete issues:

{findings_text}

Reviewer summary:
{review_data['summary']}

Continue in the SAME coding session.

Fix the review findings and any directly related regressions.
Then:
- run the relevant tests/build/checks;
- self-review the final diff;
- fix anything else you find;
- rerun verification.

Do not commit, push, create a PR, merge, deploy, or release.
Publication requires separate structured owner commands after independent review.

{autopilot_contract()}
""".strip()

    await mcp_call(
        "continue_task",
        {
            "task_id": item["task_id"],
            "_operation": f"review-correction:{item.get('review_round')}:{corrections + 1}",
            "prompt": coding_prompt(correction_prompt),
        },
    )

    corrections += 1

    item["review_corrections"] = corrections
    item["review_start_tree"] = None
    item["reviewed_tree"] = None
    item["owner_state"] = None
    item["review_task_id"] = None
    item["reviewer_agent"] = None
    item["review_format_corrections"] = 0
    item["review_format_restarts"] = 0
    item["review_status"] = None
    item["autopilot_corrections"] = 0
    item["last_report_signature"] = None

    save_state(state)
    transition_issue(issue, "dev:running")

    add_comment(
        issue["number"],
        "\n".join([
            "<!-- DEVORCHESTRATOR_REVIEW_CORRECTION_V1 -->",
            "## Review changes sent back to coding agent",
            "",
            (
                f"Correction round: "
                f"{corrections}/{MAX_REVIEW_CORRECTIONS}"
            ),
            f"Coding task: `{item['task_id']}`",
            "",
            findings_text,
        ]),
    )

    print(
        f"[bridge] review findings returned to "
        f"{item['task_id']} "
        f"{corrections}/{MAX_REVIEW_CORRECTIONS}",
        flush=True,
    )

    return True


def quarantine_item(issue, item, state, diagnostic):
    """Persist a deterministic hold and its effects together, before continuing the tick."""
    if item.get("compatibility_status") == "QUARANTINED":
        return
    item.update(compatibility_status="QUARANTINED", compatibility_diagnostic=diagnostic,
                owner_blocker=diagnostic, desired_label="dev:needs-owner")
    number = str(issue["number"])
    bridge_effects.enqueue(state, number, "transition", {"issue": issue, "target": "dev:needs-owner"},
                           ["legacy-quarantine", item.get("task_id")])
    bridge_effects.enqueue(state, number, "comment", diagnostic,
                           ["legacy-quarantine", item.get("task_id")])
    save_state(state)
    print(f"[bridge] issue #{number} quarantined: {diagnostic}", flush=True)


def reconcile_project(issue, item, state, report):
    if item.get("compatibility_status") == "QUARANTINED":
        return False
    try:
        if report.get("id") != item["task_id"]:
            raise ValueError("Task report identity mismatch")
        project = registered_project(report, item.get("project"))
    except ValueError:
        quarantine_item(issue, item, state,
            "Tracked task has no verified registered project. Held for local metadata/registry inspection; cloud issue paths are ignored.")
        return False
    if not item.get("project"):
        item.update(project=project, compatibility_status="MIGRATED",
                    compatibility_version=1, compatibility_source="registered-task-report")
        # Publication cannot acquire a historical approval from migration.
        save_state(state)
    return True


def defer_issue(item, state, number, exc=None, *, stage=None):
    retry = item.setdefault("reconciliation_retry", {"attempts": 0})
    retry["attempts"] = retry.get("attempts", 0) + 1
    delay = min(300, 10 * 2 ** min(retry["attempts"] - 1, 5))
    retry["next_attempt"] = time.time() + delay
    # Payload-free evidence: arbitrary exceptions may embed prompts, CLI output,
    # URLs, and credentials. Keep only known stages and local source locations.
    stages = MCP_OBJECT_TOOLS | {'list_tasks', 'project_identity', 'review_snapshot', 'tracked_issue'}
    stage = stage if isinstance(stage, str) and stage in stages else 'tracked_issue'
    error = re.sub('[^A-Za-z0-9_]', '', type(exc).__name__)[:64] if exc else 'Unavailable'
    locations = []
    tb = exc.__traceback__ if exc else None
    while tb:
        filename = Path(tb.tb_frame.f_code.co_filename)
        if filename.parent == Path(__file__).resolve().parent:
            locations.append(f'{filename.name}:{tb.tb_lineno}')
        tb = tb.tb_next
    pending = item.get('pending_rpc')
    action = ('Replay the existing durable request; retain receipts and repository locks.' if pending else
              'Inspect the failed stage; no launch intent is recorded. Retain queue ownership.')
    diagnostic = redact(f'{stage}: {error}; attempt {retry["attempts"]}; retry in {delay}s; '
                        f'locations={",".join(locations[-4:]) or "unavailable"}. {action}')[:512]
    retry['diagnostic'] = diagnostic
    item.setdefault('consistency_diagnostics', {})['reconciliation'] = diagnostic
    save_state(state)
    if retry["attempts"] in (1, 3) or retry["attempts"] % 10 == 0:
        print(f"[bridge] issue #{number} reconciliation unavailable: {diagnostic}", flush=True)


def malformed_item(item):
    """Validate fields before the tick or resume path can consume them."""
    task_id = item.get("task_id")
    if item.get('review_transition') is not None:
        try:
            review_transitions.validate(item['review_transition'], item.get('pending_rpc'))
        except (ValueError, TypeError, KeyError):
            return True
    if task_id is not None and (not isinstance(task_id, str) or not task_id or
                               not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", task_id) or ".." in task_id):
        return True
    if not task_id and item.get("queue_status") not in ("QUEUED", "FAILED"):
        return True
    if item.get("project") is not None and not isinstance(item["project"], str):
        return True
    for field in ("processed_followups", "processed_owner_actions", "stale_review_task_ids", "processed_chief_comments"):
        values = item.get(field)
        if values is not None and (not isinstance(values, list) or any(not isinstance(v, str) for v in values)):
            return True
    for task_id in [item.get('review_task_id'), *(item.get('stale_review_task_ids') or [])]:
        if task_id is not None and (not isinstance(task_id, str) or
                                   not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', task_id) or '..' in task_id):
            return True
    for field in ("pending_rpc", "publication_journal", "reviewed_tree", "review_start_tree", "task_spec", "review_hold"):
        if item.get(field) is not None and not isinstance(item[field], dict):
            return True
    hold = item.get('review_hold')
    if hold is not None and (not isinstance(hold.get('diagnostic'), str) or not hold['diagnostic']):
        return True
    retry = item.get("reconciliation_retry", {})
    if (not isinstance(retry, dict) or not isinstance(retry.get("attempts", 0), int) or
            retry.get("attempts", 0) < 0 or not isinstance(retry.get("next_attempt", 0), (int, float)) or
            not math.isfinite(retry.get("next_attempt", 0)) or retry.get("next_attempt", 0) < 0):
        return True
    for field in ("autopilot_policy_version", "autopilot_corrections", "review_round",
                  "review_corrections", "review_format_corrections", "review_format_restarts"):
        try:
            if int(item.get(field) or 0) < 0:
                return True
        except (ValueError, TypeError, OverflowError):
            return True
    for field in ('coding_turn', 'review_coding_turn', 'review_task_turn'):
        if field in item and (type(item[field]) is not int or item[field] < 1):
            return True
    pending = item.get("pending_rpc")
    if pending and (pending.get("name") not in ("start_task", "continue_task") or
                    not isinstance(pending.get("arguments"), dict) or not isinstance(pending.get("operation"), str)):
        return True
    if (pending and pending['operation'].startswith(('review:', 'review-format:')) and
            pending['arguments'].get('role') != 'review'):
        return True
    spec = item.get("task_spec")
    if spec and (spec.get("mode") not in ("manual", AUTOPILOT_MODE) or
                 not isinstance(spec.get("arguments"), dict) or
                 not isinstance(spec["arguments"].get("project"), str) or
                 not isinstance(spec["arguments"].get("prompt"), str)):
        return True
    return chief_review.malformed_runtime(item)


async def process_tracked(issue, item, state, runtime_reports=None, *, initial_report=None, chief_evidence=None,
                          progress=None):
    if observation_barrier(state, issue['number'], initial_report):
        return
    restore_merged_state(item, state)
    if merge_complete(item):
        project_merge(issue, item, state)
    if archive_issue(issue, item, state):
        return
    if review_transitions.holds_repository(item):
        return  # The resumable coordinator, never legacy inline replay, owns this intent.
    global WORK_CONTEXT
    WORK_CONTEXT = (state, str(issue["number"]), item)
    failed = False
    attempted = False
    progress = progress if progress is not None else {}
    token = RECONCILIATION_PROGRESS.set(progress)
    try:
        if item.get("compatibility_status") == "QUARANTINED":
            return
        if item.get("reconciliation_retry", {}).get("next_attempt", 0) > time.time():
            return
        attempted = True
        observe_reconciliation_stage('tracked_issue')
        report = initial_report or await mcp_call("get_task_report", {"task_id": item["task_id"]})
        if not reconcile_project(issue, item, state, report):
            return
        observe_coding_turn(item, state, report)
        pending = item.get("pending_rpc")
        if pending:
            args = {**pending["arguments"], "_operation": pending["operation"]}
            await mcp_call(pending["name"], args)
            transition_issue(issue, "dev:running")
            report = await mcp_call("get_task_report", {"task_id": item["task_id"]})
            observe_coding_turn(item, state, report)

        if project_runtime_health(issue, item, state, report):
            item['worker_uncertain'] = True
            return
        if report.get("status") == "UNKNOWN":
            item["worker_uncertain"] = True
            diagnostic = "Worker state is uncertain. Inspect the registered machine and task metadata before recovering this task; no duplicate worker will start."
            transition_issue(issue, "dev:needs-owner")
            notify("NEEDS_OWNER", issue, item, report, diagnostic)
            return
        if (report.get('status') not in ('DONE', 'FAILED') or
                (type(report.get('turns')) is int and report['turns'] < item.get('coding_turn', report['turns']))):
            if report.get('status') == 'RUNNING' and item.pop('worker_uncertain', False):
                save_state(state)
            transition_issue(issue, 'dev:running')
            return
        if not await drain_stale_reviews(item, state):
            transition_issue(issue, 'dev:running')
            return
        if item.pop("worker_uncertain", False) and item.get("last_report_signature"):
            # Reconcile the existing terminal result after a transport outage.
            user_status, label = terminal_state(report, item)
            transition_issue(issue, label)

        if item.get('merged'):
            await process_owner_actions(issue, item, state, report)
            return
        report = await process_followup(
            issue, item, state, report
        )

        observe_coding_turn(item, state, report)
        if report.get("status") not in ('DONE', 'FAILED'):
            return
        if await process_chief_review(issue, item, state, report, evidence=chief_evidence):
            return
        await process_owner_actions(issue, item, state, report)

        report, autopilot_continued = await maybe_continue_autopilot(
            issue, item, state, report
        )

        if autopilot_continued:
            return

        review_handled = await process_review_loop(
            issue,
            item,
            state,
            report,
        )

        if review_handled:
            return

        status = report.get("status")
        turns = report.get("turns")
        exit_code = report.get("exit_code")

        signature = f"{status}:{turns}:{exit_code}"

        if (
            status in ("DONE", "FAILED")
            and item.get("last_report_signature") != signature
        ):
            add_comment(
                issue["number"],
                format_report(report),
            )

            user_status, label = terminal_state(report, item)

            transition_issue(issue, label)

            _, blocker, _ = autopilot_decision(report, item)
            if user_status == "NEEDS_OWNER":
                item["owner_blocker"] = blocker
                item["owner_question_at"] = datetime.now(timezone.utc).isoformat()
                save_state(state)
            notify(user_status, issue, item, report,
                   blocker if user_status == "NEEDS_OWNER" else None)
            item["last_report_signature"] = signature
            save_state(state)

            print(
                f"[bridge] task {item['task_id']} -> {user_status}",
                flush=True,
            )

    except PreflightRejected as exc:
        if getattr(exc, 'role', 'coding') == 'review':
            await fail_review_loop(issue, item, state, str(exc), report)
        else:
            invalidate_quality_gates(item)
            item['execution_failure'] = str(exc)
            item['owner_blocker'] = str(exc)
            transition_issue(issue, 'dev:failed')
            add_comment(issue['number'], 'Execution validation failed before launch: ' + str(exc))
            notify('FAILED', issue, item, report, str(exc))
            save_state(state)
        Scheduler(state).release(str(issue['number']))
    except QueueWait:
        pass
    except asyncio.CancelledError:
        # wait_for cancellation is not success. Its caller records the timeout;
        # preserve retry history and any pending RPC/reservation for that caller.
        failed = True
        raise
    except Exception as exc:
        from external_effects import ExternalEffectBlocked
        if isinstance(exc, ExternalEffectBlocked):
            failed = True
            return
        failed = True
        # Cached observations can still expose expired/uncertain leases when
        # the worker report RPC is unavailable. Do not leave a stale running
        # label as the only evidence, or infer recovery from a failed refresh.
        for slot, key in (('coding', 'task_id'), ('review', 'review_task_id')):
            cached = (runtime_reports or {}).get(item.get(key))
            if cached:
                project_runtime_health(issue, item, state, cached, slot)
        defer_issue(item, state, issue['number'], exc, stage=progress.get('stage'))
        if not item.get("project") and item["reconciliation_retry"]["attempts"] >= 3:
            quarantine_item(issue, item, state,
                "Legacy task report unavailable after three bounded attempts. Held for local metadata/registry inspection; no task will be relaunched.")
    finally:
        RECONCILIATION_PROGRESS.reset(token)
        if attempted and not failed:
            diagnostics = item.get('consistency_diagnostics', {})
            recovered = 'reconciliation' in diagnostics or 'reconciliation_retry' in item
            diagnostics.pop('reconciliation', None)
            item.pop('reconciliation_retry', None)
            if recovered:
                save_state(state)
        if (item.get("review_status") in ("PASS", "NEEDS_OWNER", "FAILED", "FRESH_REVIEW_REQUIRED") or item.get("last_report_signature") or
                item.get("compatibility_status") == "QUARANTINED"):
            Scheduler(state).release(str(issue["number"]))
            save_state(state)



async def tick(state):
    from external_effects import blocked
    if blocked():
        await observe_latched_poll(state)
        return
    global WORK_CONTEXT
    reconciled = False
    try:
        await reconcile_tick(state)
        reconciled = True
    finally:
        WORK_CONTEXT = None
        if not reconciled:
            # Worker discovery failure does not disable repair of durable
            # desired projections. Approval sends retain the freshness gate.
            started = time.monotonic()
            for number, item in state.get('issues', {}).items():
                if time.monotonic() - started >= 5:
                    break
                if not str(number).isdigit() or not isinstance(item, dict) or exclusion_reason(item) or malformed_item(item):
                    continue
                if bridge_effects.observation_held(state, number):
                    continue
                try:
                    issue = (await io_call('issue', dict(number=number)))
                    if issue and not archive_issue(issue, item, state):
                        reconcile_projection(issue, item, state)
                except Exception:
                    continue
            WORK_CONTEXT = None
        # Outbox recovery must not depend on task discovery being available.
        # Approval invitations still require successful coding-status reconciliation.
        flush_effects(state, reconciled=reconciled)


async def reconcile_tick(state):
    from external_effects import blocked
    if blocked():
        await observe_latched_poll(state)
        return
    global WORK_CONTEXT
    WORK_CONTEXT = None
    issues = open_issues()

    by_number = {
        str(issue["number"]): issue
        for issue in issues
    }

    with measure(state, 'worker_reconciliation'):
        tasks = await mcp_call("list_tasks", {})
        Scheduler(state).recover(tasks)
    reports = {task['id']: task for task in tasks}
    save_state(state)
    # GitHub lists newest first; issue number provides stable oldest-first admission.
    for issue in sorted(issues, key=lambda i: i["number"]):
        number = str(issue["number"])
        if QUEUE_LABEL in label_names(issue) and number not in state["issues"]:
            state["issues"][number] = {"queue_status": "QUEUED"}
    save_state(state)
    for number, item in sorted(list(state["issues"].items()),
                               key=lambda pair: (not str(pair[0]).isdigit(), int(pair[0]) if str(pair[0]).isdigit() else str(pair[0]))):
        WORK_CONTEXT = None
        if bridge_effects.observation_held(state, number):
            continue
        if not isinstance(item, dict):
            item = state["issues"][number] = {"legacy_value": item}
        if item.get("compatibility_status") == "QUARANTINED":
            continue
        if not str(number).isdigit():
            item.update(compatibility_status="QUARANTINED", compatibility_diagnostic="Invalid tracked issue identity")
            save_state(state)
            continue
        try:
            issue = by_number.get(number)
            if not issue:
                # Closed/missing from list is not proof a process stopped. Fetch tracked issues explicitly.
                issue = (await io_call('issue', dict(number=number)))
        except Exception as exc:
            defer_issue(item, state, number, exc)
            continue
        if not issue:
            continue
        restore_merged_state(item, state)
        if merge_complete(item):
            project_merge(issue, item, state)
        if archive_issue(issue, item, state):
            continue
        if "legacy_value" in item or malformed_item(item):
            quarantine_item(issue, item, state, 'Malformed tracked state held for local inspection.')
            Scheduler(state).release(number)
            save_state(state)
            continue
        if item.get('task_id') in reports:
            observe_coding_turn(item, state, reports[item['task_id']])
        reconcile_projection(issue, item, state)
        if item.get('reconciliation_retry', {}).get('next_attempt', 0) > time.time():
            continue
        if not item.get("task_id"):
            if item.get("queue_status") != "FAILED" and issue.get("state", "OPEN").upper() == "OPEN":
                await start_queued(issue, state)
        else:
            with measure(state, 'tracked_issue'):
                await process_tracked(issue, item, state, reports)


def reject_unready_commands(issue, item, state, comments=None):
    """Acknowledge publication requests while external quality gates are closed."""
    if chief_review.approval_complete(item) and item.get('owner_state'):
        return
    processed = set(item.get('processed_owner_actions') or [])
    for comment in comments if comments is not None else issue_comments(issue['number']):
        if not authorized_comment(comment) or chief_review.is_evidence_comment(comment.get('body')):
            continue
        cid = comment_id(comment)
        if comment_already_processed(comment, processed):
            continue
        try:
            command = parse_owner_command(comment.get('body'))
        except ValueError:
            continue
        if not command or command['action'] == 'ANSWER':
            continue
        processed.add(cid)
        item['processed_owner_actions'] = sorted(processed)
        add_comment(issue['number'], 'Owner action ' + command['action'] +
                    ': REJECTED; current independent review and bound Chief PASS are required.')


async def control_pass(state, *, telegram_dispatch=None, telegram_inflight=()):
    """Only the caller mutates lifecycle state. No global recovery prerequisite."""
    global WORK_CONTEXT
    from external_effects import blocked
    if blocked():
        await observe_latched_poll(state)
        return
    fresh = set()
    with measure(state, 'control_total'):
        for number, item in list(state['issues'].items()):
            WORK_CONTEXT = None
            if not str(number).isdigit() or not isinstance(item, dict):
                continue
            if bridge_effects.observation_held(state, number):
                continue
            restore_merged_state(item, state)
            if merge_complete(item):
                project_merge({'number': int(number)}, item, state)
                continue
            if exclusion_reason(item) or malformed_item(item):
                continue
            try:
                with measure(state, 'github_command_read'):
                    issue = (await io_call('issue', dict(number=number)))
                if not issue or archive_issue(issue, item, state):
                    continue
                reconcile_projection(issue, item, state)
                WORK_CONTEXT = (state, number, item)
                comments = (await io_call('comments', dict(number=issue['number'])))
                item.setdefault('consistency_diagnostics', {}).pop('control', None)
                has_followup = False
                for comment in comments:
                    if not authorized_comment(comment) or chief_review.is_evidence_comment(comment.get('body')):
                        continue
                    if comment_already_processed(comment, set(item.get('processed_followups') or [])):
                        continue
                    try:
                        command = parse_owner_command(comment.get('body'))
                    except ValueError:
                        command = None
                    if FOLLOWUP_MARKER in (comment.get('body') or '') or (command and command['action'] == 'ANSWER'):
                        has_followup = True
                if not item.get('chief_review'):
                    reject_unready_commands(issue, item, state, comments)
                if not item.get('task_id') or not (item.get('chief_review') or item.get('owner_state') or has_followup):
                    continue  # Running/recovery observations belong to the heavy reader.
                with measure(state, 'targeted_control'):
                    report = await mcp_call('get_task_report', {'task_id': item['task_id']})
                    if not reconcile_project(issue, item, state, report):
                        continue
                    observe_coding_turn(item, state, report)
                    if project_runtime_health(issue, item, state, report):
                        continue
                    if not item.get('merged'):
                        report = await process_followup(issue, item, state, report)
                        observe_coding_turn(item, state, report)
                    if not coding_report_successful(report, item):
                        reject_unready_commands(issue, item, state)
                        continue
                    fresh.add(number)
                    if not item.get('merged') and await process_chief_review(
                            issue, item, state, report, allow_evidence=False):
                        reject_unready_commands(issue, item, state)
                        continue
                    reject_unready_commands(issue, item, state)
                    await process_owner_actions(issue, item, state, report)
            except Exception:
                # No failed read supplies approval; a later control poll retries.
                item.setdefault('consistency_diagnostics', {})['control'] = 'Targeted control observation unavailable'
            else:
                item.setdefault('consistency_diagnostics', {}).pop('control', None)
        WORK_CONTEXT = None
        # Targeted fresh checks authorize eligible effects even if discovery fails.
        flush_effects(state, reconciled=True, control_fresh=fresh,
                      telegram_dispatch=telegram_dispatch, telegram_inflight=telegram_inflight)
    save_state(state)


from control_loop import ControlLoop


async def main():
    worker_guard()
    state = load_state()

    print(
        f"[bridge] watching {REPO} every {POLL_SECONDS}s",
        flush=True,
    )

    loop = ControlLoop(state)
    try:
        while True:
            try:
                await loop.step()
            except Exception as exc:
                print(f"[bridge] loop error: {type(exc).__name__}; will retry", flush=True)
            await loop.wait(POLL_SECONDS)
    finally:
        await loop.close()


if __name__ == "__main__":
    with exclusive(STATE_DIR / "bridge.lock"):
        asyncio.run(main())

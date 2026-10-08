#!/usr/bin/env python3

import asyncio
import json
import math
import os
import re
import subprocess
import time
from pathlib import Path

import notifications
import bridge_effects
from github_transport import GitHubTransport
from lifecycle import WORK_QUEUE_LABEL, worker_guard
from datetime import datetime, timezone
from durable import read_json, write_json, exclusive
from scheduler import Scheduler, QueueWait, project_identity, registered_project
import hashlib
from redaction import redact

from publication import Repository, PublicationError, execute as publish

# Import MCP lazily so local tests and cleanup do not need a running MCP service.


REPO = "siyovushdev/dev-orchestrator-tasks"
MCP_URL = "http://127.0.0.1:8787/mcp"
POLL_SECONDS = 10
# Startup validates explicit enrollment, host binding and this dedicated queue.
QUEUE_LABEL = WORK_QUEUE_LABEL

STATE_DIR = Path.home() / ".local/share/devorchestrator"
STATE_FILE = STATE_DIR / "task_bridge_state.json"

STATE_LABELS = {
    QUEUE_LABEL,
    "dev:running",
    "dev:done",
    "dev:ready-for-review",
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

REVIEWER_CODEX_MODEL = "gpt-6.1-sol"
REVIEWER_CODEX_EFFORT = "high"


def gh(*args):
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
    text = redact(notifications.terminal_text(status, issue, item, report, blocker))
    if WORK_CONTEXT:
        state, number, _ = WORK_CONTEXT
        event = [status, item.get("task_id"), item.get("review_round"),
                 (report or {}).get("turns"), blocker]
        bridge_effects.enqueue(state, number, "telegram", text, event)
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
        })
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
    item["publication_journal"] = {}


def coding_report_successful(report, item):
    turn = report.get('turns')
    return (report.get('id') == item.get('task_id') and report.get('status') == 'DONE' and
            type(report.get('exit_code')) is int and report['exit_code'] == 0 and
            type(turn) is int and turn > 0 and turn == item.get('coding_turn', turn))


def observe_coding_turn(item, state, report):
    """Invalidate reviews on mutable/new coding turns, never adopt old review evidence."""
    if report.get('id') != item.get('task_id'):
        return
    turn = report.get('turns')
    bound = item.get('review_coding_turn')
    has_review = any(item.get(field) for field in ('review_task_id', 'review_start_tree', 'review_status',
                                                  'reviewed_tree', 'owner_state'))
    changed_turn = type(turn) is int and turn > 0 and (
        (bound is not None and bound != max(turn, item.get('coding_turn', turn))) or turn > item.get('coding_turn', turn))
    unbound_review = bool(item.get('review_task_id') and bound is None and item.get('review_start_tree'))
    if has_review and (report.get('status') in ('RUNNING', 'STARTING') or changed_turn or unbound_review):
        pending = item.get('pending_rpc')
        if pending and pending['arguments'].get('role') == 'review':
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
    save_state(state)


async def drain_stale_reviews(item, state):
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


def reviewer_for(coding_agent):
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
    return state


def save_state(state):
    write_json(STATE_FILE, state)


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
    state, _, _ = WORK_CONTEXT
    bridge_effects.enqueue(state, number, "comment", body)
    save_state(state)


def flush_effects(state, reconciled=True):
    suppressed = False
    for entry in state.get('outbox', {}).values():
        item = state.get('issues', {}).get(str(entry['number']), {})
        if not isinstance(item, dict):
            item = {}
        if (entry['kind'] == 'telegram' and entry['status'] == 'PENDING' and
                entry['payload'].startswith('REVIEW_PASSED / READY_FOR_OWNER') and
                (not notifications.review_approval_complete(item) or item.get('owner_state') != OWNER_STATE_READY)):
            entry.update(status='DONE', diagnostic='Superseded: task has no current READY_FOR_OWNER approval')
            suppressed = True
    if suppressed:
        save_state(state)
    def transition(issue, target):
        # Read current labels, since outbox payload may predate a restart or another transition.
        adapter = github_task_transport()
        current = adapter.fetch_labels(adapter.issue_ref(issue["number"]))
        raw_transition_issue(current, target)
    bridge_effects.flush(state, lambda: save_state(state), issue_comments,
                        raw_add_comment, transition, send_telegram,
                        eligible=lambda entry: reconciled or not (
                            (entry['kind'] == 'telegram' and entry['payload'].startswith('REVIEW_PASSED / READY_FOR_OWNER')) or
                            (entry['kind'] == 'transition' and entry['payload']['target'] == 'dev:ready-for-review')))


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
    if mode not in ("manual", AUTOPILOT_MODE) or agent not in (None, "codex", "cursor"):
        raise ValueError("Unsupported task mode/agent")
    for name, value in (("model", model), ("effort", effort)):
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise ValueError(f"Task {name} must be a non-empty string")
    if effort and not re.fullmatch(r"[a-z]+", effort):
        raise ValueError("Effort must be a simple reasoning-effort name")

    prompt = TASK_RE.sub("", body, count=1).strip()

    if not prompt:
        raise ValueError("Task prompt is empty")

    return project, agent, mode, model, effort, prompt


async def transport_call(name, arguments):
    return await asyncio.wait_for(_transport_call(name, arguments), timeout=120)


async def _transport_call(name, arguments):
    from mcp import Client
    async with Client(MCP_URL) as client:
        result = await client.call_tool(name, arguments)

    return decode_mcp_result(name, result)


# Return contracts from mcp_server.py. A project registry is an object even though
# it contains multiple projects; only list_tasks has collection-valued output.
MCP_COLLECTION_TOOLS = frozenset({"list_tasks"})
MCP_OBJECT_TOOLS = frozenset({"list_projects", "get_task_report", "start_task",
                              "continue_task", "adopt_task", "orchestrator_health"})


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
        item.update(review_task_id=result.get("task_id") or result["id"],
                    reviewer_agent=result.get("agent"), review_round=int(operation.split(":")[1]),
                    review_status="RUNNING", review_format_corrections=0)
        item['review_task_turn'] = result.get('turns', 1)


async def mcp_call(name, arguments):
    if not WORK_CONTEXT or name not in ("start_task", "continue_task"):
        result = await transport_call(name, arguments)
        if WORK_CONTEXT and name == "get_task_report":
            state, _, _ = WORK_CONTEXT
            Scheduler(state).observe(arguments["task_id"], result.get("status"))
            save_state(state)
        return result
    state, number, item = WORK_CONTEXT
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
    machine, repo = project_identity(arguments.get("project") or item["project"])
    key = hashlib.sha256(f"{REPO}:{number}:{name}:{operation}".encode()).hexdigest()
    scheduler = Scheduler(state)
    job = scheduler.enqueue(key, number, kind, machine, repo)
    if not item.get("pending_rpc"):
        item["pending_rpc"] = {"name": name, "arguments": arguments, "operation": operation}
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
    result = await transport_call(name, arguments)
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
    health = report.get('liveness')
    if not isinstance(health, dict):
        return False  # V1/test transports without the additive contract.
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


async def start_queued(issue, state):
    number = str(issue["number"])
    global WORK_CONTEXT
    WORK_CONTEXT = (state, number, state["issues"].setdefault(number, {"queue_status": "QUEUED"}))

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
            spec = {"arguments": args, "mode": mode}
        args = dict(spec["arguments"])
        project, mode = args["project"], spec["mode"]
        model, effort = args.get("model"), args.get("effort")
        if project not in __import__("devctl").load_projects():
            raise ValueError("Unknown registered project")
        queued["task_spec"] = spec

        result = await mcp_call("start_task", args)

        task_id = result["task_id"]

        queued = state["issues"][number]
        queued.pop("reconciliation_retry", None)
        state["issues"][number] = {
            **queued,
            "queue_status": "STARTED",
            "task_id": task_id,
            "project": project,
            "coding_agent": result.get("agent"),
            "mode": mode,
            "autopilot_policy_version": (
                AUTOPILOT_POLICY_VERSION
                if mode == AUTOPILOT_MODE
                else None
            ),
            "model": model,
            "effort": effort,
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

        add_comment(
            issue["number"],
            "\n".join([
                "<!-- DEVORCHESTRATOR_BRIDGE_V1 -->",
                "## DevOrchestrator",
                "",
                f"Task started: `{task_id}`",
                f"Project: `{project}`",
                f"Agent: `{result.get('agent')}`",
                f"Machine: `{result.get('machine')}`",
            ]),
        )

        print(
            f"[bridge] issue #{number} -> task {task_id}",
            flush=True,
        )

    except QueueWait:
        return
    except ValueError as exc:
        item = state["issues"][number]
        item["queue_status"] = "FAILED"
        transition_issue(issue, "dev:failed")
        notify("FAILED", issue, item, blocker=str(exc))
        save_state(state)
    except Exception:
        # An RPC failure may mean the agent started. Keep its reservation and retry the same request ID.
        defer_issue(state["issues"][number], state, number)



def authorized_comment(comment):
    # Structured commands are not authorization by themselves. GitHub supplies author identity.
    return (comment.get("author") or {}).get("login", "").lower() in owner_logins()


def owner_logins():
    return {login.strip().lower() for login in os.environ.get(
        "DEVORCHESTRATOR_OWNERS", REPO.split("/")[0]).split(",") if login.strip()}


async def process_owner_actions(issue, item, state, report):
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
        notify(item["owner_state"], issue, item, report)
    processed = set(item.get("processed_owner_actions") or [])
    for comment in issue_comments(issue["number"]):
        cid = comment_id(comment)
        if comment_already_processed(comment, processed) or not authorized_comment(comment):
            continue
        try:
            command = parse_owner_command(comment.get("body"))
            if not command:
                continue
            if command["action"] == "ANSWER":
                # Defer the answer to process_followup so it follows the coding resume path.
                continue
            plan = owner_action_plan(item.get("owner_state"), command["action"])
            if plan["status"] == "REJECTED":
                raise PublicationError(plan["reason"])
            if plan["status"] == "ALLOWED":
                approved = item.get("owner_approved_at")
                created = comment.get("createdAt")
                if not approved or not created or datetime.fromisoformat(created.replace("Z", "+00:00")) <= datetime.fromisoformat(approved):
                    raise PublicationError("Owner action must be issued after the current independent review PASS")
                machine, repo_key = project_identity(item["project"])
                if Scheduler(state).repository_busy(str(issue["number"]), machine, repo_key):
                    raise PublicationError("Repository has another active or uncertain task")
                from devctl import DATA_DIR
                with exclusive(DATA_DIR.parent / "worker.lock", blocking=True):
                    repository = Repository(item["project"], report)
                    tasks = await transport_call("list_tasks", {})
                    for task in tasks:
                        if task.get("status") not in ("RUNNING", "UNKNOWN", "STARTING"):
                            continue
                        if task.get("machine") not in (None, "-", machine):
                            continue
                        try:
                            active_project = registered_project(task)
                            active_machine, active_repo = project_identity(active_project)
                        except Exception:
                            raise PublicationError("Worker repository identity is uncertain")
                        if (active_machine, active_repo) == (machine, repo_key):
                            raise PublicationError("Registered workspace has active or uncertain worker work")
                    publish(repository, item, report, command, lambda: save_state(state))
                item["owner_state"] = plan["next_state"]
                if item["owner_state"] == "MERGED":
                    item["completed_at"] = datetime.now(timezone.utc).isoformat()
            processed.add(cid)
            item["processed_owner_actions"] = sorted(processed)
            save_state(state)
            if plan["status"] == "ALLOWED":
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
    if report.get("status") not in ("DONE", "FAILED") or item.get("review_status") == "RUNNING":
        return report

    processed = set(item.get("processed_followups") or [])

    for comment in issue_comments(issue["number"]):
        body = comment.get("body") or ""

        if not authorized_comment(comment):
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
    if item.get("mode") != AUTOPILOT_MODE:
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
    if not coding_report_successful(coding_report, item):
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

    reviewer = reviewer_for(coding_agent)

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

    if reviewer == "codex":
        args["model"] = REVIEWER_CODEX_MODEL
        args["effort"] = REVIEWER_CODEX_EFFORT

    if not item.get("review_start_tree"):
        item["review_start_tree"] = Repository(project, coding_report).snapshot()
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
    if item.get("mode") != AUTOPILOT_MODE:
        return False

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
        with exclusive(DATA_DIR.parent / 'worker.lock', blocking=True):
            current = await mcp_call('get_task_report', {'task_id': item['task_id']})
            observe_coding_turn(item, state, current)
            if (not coding_report_successful(current, item) or item.get('review_task_id') != review_task_id or
                    item.get('review_coding_turn') != current.get('turns')):
                return True
            reviewed = Repository(item["project"], current).snapshot()
            if reviewed != item.get("review_start_tree"):
                await fail_review_loop(issue, item, state,
                                       "Repository changed during independent review; fresh review required", current)
                return True
            item["reviewed_tree"] = reviewed
            item["review_status"] = "PASS"
            item["owner_state"] = OWNER_STATE_READY
            item["owner_approved_at"] = datetime.now(timezone.utc).isoformat()
            item["owner_blocker"] = None
            item["owner_question_at"] = None
            save_state(state)

        transition_issue(
            issue,
            "dev:ready-for-review",
        )

        notify("REVIEW_PASSED / READY_FOR_OWNER", issue, item, coding_report)

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


def defer_issue(item, state, number):
    retry = item.setdefault("reconciliation_retry", {"attempts": 0})
    retry["attempts"] = retry.get("attempts", 0) + 1
    retry["next_attempt"] = time.time() + min(300, 10 * 2 ** min(retry["attempts"] - 1, 5))
    save_state(state)
    if retry["attempts"] == 1:
        print(f"[bridge] issue #{number} reconciliation unavailable; durable retry with backoff", flush=True)


def malformed_item(item):
    """Validate fields before the tick or resume path can consume them."""
    task_id = item.get("task_id")
    if task_id is not None and (not isinstance(task_id, str) or not task_id or
                               not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", task_id) or ".." in task_id):
        return True
    if not task_id and item.get("queue_status") not in ("QUEUED", "FAILED"):
        return True
    if item.get("project") is not None and not isinstance(item["project"], str):
        return True
    for field in ("processed_followups", "processed_owner_actions", "stale_review_task_ids"):
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
    return False


async def process_tracked(issue, item, state, runtime_reports=None):
    global WORK_CONTEXT
    WORK_CONTEXT = (state, str(issue["number"]), item)
    failed = False
    try:
        if item.get("compatibility_status") == "QUARANTINED":
            return
        if item.get("reconciliation_retry", {}).get("next_attempt", 0) > time.time():
            return
        report = await mcp_call("get_task_report", {"task_id": item["task_id"]})
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

        report = await process_followup(
            issue, item, state, report
        )

        observe_coding_turn(item, state, report)
        if report.get("status") not in ('DONE', 'FAILED'):
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

    except QueueWait:
        pass
    except Exception:
        failed = True
        # Cached observations can still expose expired/uncertain leases when
        # the worker report RPC is unavailable. Do not leave a stale running
        # label as the only evidence, or infer recovery from a failed refresh.
        for slot, key in (('coding', 'task_id'), ('review', 'review_task_id')):
            cached = (runtime_reports or {}).get(item.get(key))
            if cached:
                project_runtime_health(issue, item, state, cached, slot)
        defer_issue(item, state, issue['number'])
        if not item.get("project") and item["reconciliation_retry"]["attempts"] >= 3:
            quarantine_item(issue, item, state,
                "Legacy task report unavailable after three bounded attempts. Held for local metadata/registry inspection; no task will be relaunched.")
    finally:
        if not failed and item.pop("reconciliation_retry", None) is not None:
            save_state(state)
        if (item.get("review_status") in ("PASS", "NEEDS_OWNER", "FAILED", "FRESH_REVIEW_REQUIRED") or item.get("last_report_signature") or
                item.get("compatibility_status") == "QUARANTINED"):
            Scheduler(state).release(str(issue["number"]))
            save_state(state)



async def tick(state):
    global WORK_CONTEXT
    reconciled = False
    try:
        await reconcile_tick(state)
        reconciled = True
    finally:
        WORK_CONTEXT = None
        # Outbox recovery must not depend on task discovery being available.
        # Approval invitations still require successful coding-status reconciliation.
        flush_effects(state, reconciled=reconciled)


async def reconcile_tick(state):
    global WORK_CONTEXT
    WORK_CONTEXT = None
    issues = open_issues()

    by_number = {
        str(issue["number"]): issue
        for issue in issues
    }

    tasks = await mcp_call("list_tasks", {})
    Scheduler(state).recover(tasks)
    reports = {task['id']: task for task in tasks}
    for item in state['issues'].values():
        if (isinstance(item, dict) and item.get('compatibility_status') != 'QUARANTINED' and
                not malformed_item(item) and item.get('task_id') in reports):
            observe_coding_turn(item, state, reports[item['task_id']])
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
        if not isinstance(item, dict):
            item = state["issues"][number] = {"legacy_value": item}
        if item.get("compatibility_status") == "QUARANTINED":
            continue
        if not str(number).isdigit():
            item.update(compatibility_status="QUARANTINED", compatibility_diagnostic="Invalid tracked issue identity")
            save_state(state)
            continue
        if "legacy_value" in item or malformed_item(item):
            issue = by_number.get(number) or {"number": int(number), "labels": []}
            quarantine_item(issue, item, state, "Malformed tracked state held for local inspection.")
            Scheduler(state).release(number)
            save_state(state)
            continue
        if item.get("reconciliation_retry", {}).get("next_attempt", 0) > time.time():
            continue
        try:
            issue = by_number.get(number)
            if not issue:
                # Closed/missing from list is not proof a process stopped. Fetch tracked issues explicitly.
                issue = fetch_issue(number)
        except Exception:
            defer_issue(item, state, number)
            continue
        if not issue:
            continue
        if not item.get("task_id"):
            if item.get("queue_status") != "FAILED" and issue.get("state", "OPEN").upper() == "OPEN":
                await start_queued(issue, state)
        else:
            await process_tracked(issue, item, state, reports)


async def main():
    worker_guard()
    state = load_state()

    print(
        f"[bridge] watching {REPO} every {POLL_SECONDS}s",
        flush=True,
    )

    while True:
        try:
            await tick(state)
        except Exception as exc:
            print(f"[bridge] loop error: {type(exc).__name__}; will retry", flush=True)

        await asyncio.sleep(POLL_SECONDS)


if __name__ == "__main__":
    with exclusive(STATE_DIR / "bridge.lock"):
        asyncio.run(main())

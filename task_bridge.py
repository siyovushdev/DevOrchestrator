#!/usr/bin/env python3

import asyncio
import json
import os
import re
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path

from mcp import Client


REPO = "siyovushdev/dev-orchestrator-tasks"
MCP_URL = "http://127.0.0.1:8787/mcp"
POLL_SECONDS = 10
QUEUE_LABEL = os.environ.get(
    "DEVORCHESTRATOR_QUEUE_LABEL",
    "dev:queued",
)

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

REVIEWER_CODEX_MODEL = "gpt-6.1-sol"
REVIEWER_CODEX_EFFORT = "high"


def gh(*args):
    result = subprocess.run(
        ["gh", *args],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"gh {' '.join(args)} failed: {result.stderr.strip()}"
        )

    return result.stdout


def gh_json(*args):
    out = gh(*args)
    return json.loads(out) if out.strip() else None



def send_telegram(text):
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print("[bridge] telegram not configured", flush=True)
        return

    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": chat_id,
            "text": text,
        }).encode()

        request = urllib.request.Request(url, data=data)

        with urllib.request.urlopen(request, timeout=15) as response:
            response.read()

    except Exception as exc:
        # Telegram никогда не должен ломать сам TaskBridge.
        print(
            f"[bridge] telegram error: {exc}",
            flush=True,
        )


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

Do NOT commit, push, merge, deploy, or release unless the task
explicitly authorizes it.

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

    if action not in OWNER_ACTION_RULES:
        raise ValueError(
            f"Unsupported owner action: {action!r}"
        )

    data["action"] = action

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
    item["autopilot_corrections"] = 0

    item["review_task_id"] = None
    item["reviewer_agent"] = None
    item["review_status"] = None
    item["review_format_corrections"] = 0
    item["review_corrections"] = 0

    item["owner_state"] = None


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

    if verdict not in {
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
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    if not STATE_FILE.exists():
        return {"issues": {}}

    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {"issues": {}}


def save_state(state):
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(
        json.dumps(state, indent=2, ensure_ascii=False)
    )
    tmp.replace(STATE_FILE)


def label_names(issue):
    result = set()

    for label in issue.get("labels") or []:
        if isinstance(label, dict):
            name = label.get("name")
        else:
            name = str(label)

        if name:
            result.add(name)

    return result


def transition_issue(issue, target):
    current = label_names(issue)

    args = [
        "issue", "edit", str(issue["number"]),
        "-R", REPO,
    ]

    changed = False

    for label in STATE_LABELS:
        if label in current and label != target:
            args += ["--remove-label", label]
            changed = True

    if target not in current:
        args += ["--add-label", target]
        changed = True

    if changed:
        gh(*args)


def add_comment(number, body):
    gh(
        "issue", "comment", str(number),
        "-R", REPO,
        "--body", body,
    )


def parse_task(issue):
    body = issue.get("body") or ""
    match = TASK_RE.search(body)

    if not match:
        raise ValueError(
            "Missing DEVORCHESTRATOR_TASK_V1 metadata"
        )

    meta = json.loads(match.group(1))

    project = meta.get("project")
    agent = meta.get("agent")
    mode = meta.get("mode", "manual")
    model = meta.get("model")
    effort = meta.get("effort")

    if not project:
        raise ValueError("Task metadata requires project")

    prompt = TASK_RE.sub("", body, count=1).strip()

    if not prompt:
        raise ValueError("Task prompt is empty")

    return project, agent, mode, model, effort, prompt


async def mcp_call(name, arguments):
    async with Client(MCP_URL) as client:
        result = await client.call_tool(name, arguments)

    for part in result.content:
        text = getattr(part, "text", None)
        if not text:
            continue

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

    raise RuntimeError(
        f"MCP tool {name} returned no JSON result"
    )


def issue_comments(number):
    data = gh_json(
        "issue", "view", str(number),
        "-R", REPO,
        "--json", "comments",
    )

    return (data or {}).get("comments") or []


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


async def start_queued(issue, state):
    number = str(issue["number"])

    try:
        project, agent, mode, model, effort, prompt = parse_task(issue)

        if mode == AUTOPILOT_MODE:
            prompt = build_autopilot_prompt(prompt)

        args = {
            "project": project,
            "prompt": prompt,
        }

        if agent:
            args["agent"] = agent

        if model:
            args["model"] = model

        if effort:
            args["effort"] = effort

        result = await mcp_call("start_task", args)

        task_id = result["task_id"]

        state["issues"][number] = {
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

    except Exception as exc:
        transition_issue(issue, "dev:failed")

        add_comment(
            issue["number"],
            "\n".join([
                "<!-- DEVORCHESTRATOR_BRIDGE_V1 -->",
                "## TaskBridge error",
                "",
                f"`{exc}`",
            ]),
        )

        print(
            f"[bridge] failed issue #{number}: {exc}",
            flush=True,
        )


async def process_followup(issue, item, state, report):
    if report.get("status") == "RUNNING":
        return report

    processed = set(item.get("processed_followups") or [])

    for comment in issue_comments(issue["number"]):
        body = comment.get("body") or ""

        if FOLLOWUP_MARKER not in body:
            continue

        cid = comment_id(comment)

        if cid in processed:
            continue

        prompt = body.split(FOLLOWUP_MARKER, 1)[1].strip()

        if not prompt:
            processed.add(cid)
            continue

        result = await mcp_call(
            "continue_task",
            {
                "task_id": item["task_id"],
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
    return report



async def maybe_continue_autopilot(
    issue,
    item,
    state,
    report,
):
    if item.get("mode") != AUTOPILOT_MODE:
        return report, False

    if report.get("status") == "RUNNING":
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

Do not commit, push, merge, deploy, or release unless explicitly
authorized by the original task.

{autopilot_contract()}
""".strip()

    new_report = await mcp_call(
        "continue_task",
        {
            "task_id": item["task_id"],
            "prompt": followup,
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

    args = {
        "project": project,
        "agent": reviewer,
        "prompt": build_review_prompt(
            issue,
            coding_report,
        ),
    }

    if reviewer == "codex":
        args["model"] = REVIEWER_CODEX_MODEL
        args["effort"] = REVIEWER_CODEX_EFFORT

    result = await mcp_call(
        "start_task",
        args,
    )

    item["review_task_id"] = result["task_id"]
    item["reviewer_agent"] = reviewer
    item["review_round"] = (
        int(item.get("review_round") or 0) + 1
    )
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
):
    item["review_status"] = "FAILED"
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

    send_telegram(
        "\n".join([
            "❌ REVIEW_FAILED",
            "",
            issue.get("title") or f"Task #{issue['number']}",
            f"Coding agent: {item.get('coding_agent')}",
            f"Reviewer: {item.get('reviewer_agent')}",
            f"Task: {item['task_id']}",
            "",
            str(message),
        ])
    )


async def process_review_loop(
    issue,
    item,
    state,
    coding_report,
):
    if item.get("mode") != AUTOPILOT_MODE:
        return False

    decision, _, _ = autopilot_decision(
        coding_report,
        item,
    )

    if decision != "READY_FOR_REVIEW":
        return False

    # A terminal review state must not be processed repeatedly.
    if item.get("review_status") in {
        "PASS",
        "NEEDS_OWNER",
        "FAILED",
    }:
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

    review_process_status = review_report.get("status")

    if review_process_status in {
        "RUNNING",
        "UNKNOWN",
    }:
        return True

    if review_process_status == "FAILED":
        await fail_review_loop(
            issue,
            item,
            state,
            (
                "Independent reviewer process failed: "
                + str(review_report.get("error") or "unknown error")
            ),
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

        await fail_review_loop(
            issue,
            item,
            state,
            (
                "Reviewer did not return a valid structured result: "
                + review_error
            ),
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
        item["review_status"] = "PASS"
        item["owner_state"] = OWNER_STATE_READY
        save_state(state)

        transition_issue(
            issue,
            "dev:ready-for-review",
        )

        send_telegram(
            "\n".join([
                "✅ REVIEW_PASSED",
                "",
                issue.get("title") or f"Task #{issue['number']}",
                f"Coding agent: {item.get('coding_agent')}",
                *(
                    [f"Model: {coding_report.get('model')}"]
                    if coding_report.get("model")
                    else []
                ),
                *(
                    [f"Effort: {coding_report.get('reasoning_effort')}"]
                    if coding_report.get("reasoning_effort")
                    else []
                ),
                f"Reviewer: {item.get('reviewer_agent')}",
                f"Review round: {item.get('review_round')}",
                f"Task: {item['task_id']}",
                "",
                "Открой нужный ChatGPT-чат и напиши: Проверь результат.",
            ])
        )

        print(
            f"[bridge] independent review PASS "
            f"for {item['task_id']}",
            flush=True,
        )

        return True

    if verdict == "NEEDS_OWNER":
        item["review_status"] = "NEEDS_OWNER"
        save_state(state)

        transition_issue(
            issue,
            "dev:needs-owner",
        )

        send_telegram(
            "\n".join([
                "⚠️ NEEDS_OWNER",
                "",
                issue.get("title") or f"Task #{issue['number']}",
                f"Reviewer: {item.get('reviewer_agent')}",
                "",
                str(review_data["blocker"]),
                "",
                "Вернись в нужный ChatGPT-чат.",
            ])
        )

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

Do not commit, push, merge, deploy, or release unless explicitly
authorized by the original owner task.

{autopilot_contract()}
""".strip()

    await mcp_call(
        "continue_task",
        {
            "task_id": item["task_id"],
            "prompt": correction_prompt,
        },
    )

    corrections += 1

    item["review_corrections"] = corrections
    item["review_task_id"] = None
    item["reviewer_agent"] = None
    item["review_format_corrections"] = 0
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


async def process_tracked(issue, item, state):
    try:
        report = await mcp_call(
            "get_task_report",
            {"task_id": item["task_id"]},
        )

        report = await process_followup(
            issue, item, state, report
        )

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

            item["last_report_signature"] = signature
            save_state(state)

            send_telegram(
                "\n".join([
                    (
                        "✅ READY_FOR_REVIEW"
                        if user_status == "READY_FOR_REVIEW"
                        else
                        "⚠️ NEEDS_OWNER"
                        if user_status == "NEEDS_OWNER"
                        else
                        "❌ FAILED"
                    ),
                    "",
                    issue.get("title") or f"Task #{issue['number']}",
                    f"Agent: {report.get('agent')}",
                    *(
                        [f"Model: {report.get('model')}"]
                        if report.get("model")
                        else []
                    ),
                    *(
                        [f"Effort: {report.get('reasoning_effort')}"]
                        if report.get("reasoning_effort")
                        else []
                    ),
                    f"Machine: {report.get('machine')}",
                    f"Task: {item['task_id']}",
                    "",
                    "Открой нужный ChatGPT-чат и напиши: Проверь результат.",
                ])
            )

            print(
                f"[bridge] task {item['task_id']} -> {user_status}",
                flush=True,
            )

    except Exception as exc:
        print(
            f"[bridge] tracked issue #{issue['number']}: {exc}",
            flush=True,
        )


async def tick(state):
    issues = gh_json(
        "issue", "list",
        "-R", REPO,
        "--state", "open",
        "--limit", "100",
        "--json", "number,title,body,labels,url",
    ) or []

    by_number = {
        str(issue["number"]): issue
        for issue in issues
    }

    # Новые задачи
    for issue in issues:
        number = str(issue["number"])
        labels = label_names(issue)

        if (
            QUEUE_LABEL in labels
            and number not in state["issues"]
        ):
            await start_queued(issue, state)

    # Уже известные задачи + follow-up
    for number, item in list(state["issues"].items()):
        issue = by_number.get(number)

        if not issue:
            continue

        await process_tracked(issue, item, state)


async def main():
    state = load_state()

    print(
        f"[bridge] watching {REPO} every {POLL_SECONDS}s",
        flush=True,
    )

    while True:
        try:
            await tick(state)
        except Exception as exc:
            print(f"[bridge] loop error: {exc}", flush=True)

        await asyncio.sleep(POLL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())

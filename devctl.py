#!/usr/bin/env python3

import argparse
import json
import os
import shlex
import subprocess
import sys
import uuid

from collections import deque
from datetime import datetime, timezone
from pathlib import Path


MACHINES = {
    "firefly": {
        "type": "ssh",
        "host": "100.91.173.108",
        "user": "firefly",
        "path_prefix": "/home/firefly/.local/bin",
        "codex_sandbox": "workspace-write",
        "agent_env": {
            "HTTP_PROXY": "http://127.0.0.1:10809",
            "HTTPS_PROXY": "http://127.0.0.1:10809",
            "http_proxy": "http://127.0.0.1:10809",
            "https_proxy": "http://127.0.0.1:10809",
            "NO_PROXY": "localhost,127.0.0.1,::1",
            "no_proxy": "localhost,127.0.0.1,::1",
        },
    },
    "work": {
        "type": "local",
        "path_prefix": "/home/siovush/.local/bin:/home/siovush/.nvm/versions/node/v24.12.0/bin",
        "codex_sandbox": "danger-full-access",
        "agent_env": {
            "HTTP_PROXY": "http://127.0.0.1:10809",
            "HTTPS_PROXY": "http://127.0.0.1:10809",
            "http_proxy": "http://127.0.0.1:10809",
            "https_proxy": "http://127.0.0.1:10809",
            "NO_PROXY": "localhost,127.0.0.1,::1",
            "no_proxy": "localhost,127.0.0.1,::1",
            "NODE_USE_ENV_PROXY": "1",
        },
    },
}

DATA_DIR = Path.home() / ".local/share/devorchestrator/tasks"
PROJECTS_FILE = Path(__file__).resolve().parent / "projects.json"


def now():
    return datetime.now(timezone.utc).isoformat()


def machine_command(machine, command):
    cfg = MACHINES[machine]

    path_prefix = cfg.get("path_prefix")
    if path_prefix:
        command = f'export PATH="{path_prefix}:$PATH"; {command}'

    if cfg["type"] == "local":
        return ["bash", "-lc", command]

    target = f'{cfg["user"]}@{cfg["host"]}'

    return [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        target,
        f"bash -lc {shlex.quote(command)}",
    ]


def capture(machine, command):
    return subprocess.run(
        machine_command(machine, command),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )



def capture_git(machine, workspace, *args):
    command = (
        "git -C "
        + shlex.quote(workspace)
        + " "
        + shlex.join(list(args))
    )

    return capture(machine, command)


def capture_git_baseline(machine, workspace):
    """
    Capture repository state BEFORE the coding agent starts.

    This is a publication safety baseline, not a task result.
    A dirty baseline is allowed for coding/review, but later
    owner COMMIT must reject automatic publication.
    """
    captured_at = now()

    repo = capture_git(
        machine,
        workspace,
        "rev-parse",
        "--is-inside-work-tree",
    )

    if (
        repo.returncode != 0
        or repo.stdout.strip() != "true"
    ):
        return {
            "captured_at": captured_at,
            "is_repo": False,
            "head": None,
            "branch": None,
            "detached": None,
            "status_porcelain": None,
            "clean": False,
            "error": (
                repo.stdout.strip()[-2000:]
                or "not a Git work tree"
            ),
        }

    # HEAD can legitimately be absent in a newly initialized
    # repository with no commits yet.
    head_result = capture_git(
        machine,
        workspace,
        "rev-parse",
        "--verify",
        "HEAD",
    )

    head = (
        head_result.stdout.strip()
        if head_result.returncode == 0
        else None
    )

    branch_result = capture_git(
        machine,
        workspace,
        "symbolic-ref",
        "--quiet",
        "--short",
        "HEAD",
    )

    branch = (
        branch_result.stdout.strip()
        if branch_result.returncode == 0
        else None
    )

    status_result = capture_git(
        machine,
        workspace,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    )

    status_porcelain = (
        status_result.stdout.rstrip("\n")
        if status_result.returncode == 0
        else None
    )

    origin_result = capture_git(
        machine,
        workspace,
        "remote",
        "get-url",
        "origin",
    )

    origin = (
        origin_result.stdout.strip()
        if origin_result.returncode == 0
        else None
    )

    return {
        "captured_at": captured_at,
        "is_repo": True,
        "head": head,
        "branch": branch,
        "detached": branch is None,
        "status_porcelain": status_porcelain,
        "clean": status_porcelain == "",
        "origin": origin,
        "error": (
            None
            if status_result.returncode == 0
            else status_result.stdout.strip()[-2000:]
        ),
    }


def doctor():
    command = """
echo "HOST=$(hostname)"
echo "USER=$(whoami)"
echo "CODEX=$(command -v codex || true)"
codex --version 2>/dev/null || true
echo "CURSOR=$(command -v agent || true)"
agent --version 2>/dev/null || true
"""

    failed = False

    for machine in MACHINES:
        print(f"\n===== {machine} =====")

        result = capture(machine, command)
        print(result.stdout.rstrip())

        if result.returncode != 0:
            failed = True

    return 1 if failed else 0



def load_projects():
    if not PROJECTS_FILE.exists():
        raise SystemExit(f"Projects registry not found: {PROJECTS_FILE}")

    projects = json.loads(PROJECTS_FILE.read_text())

    if not isinstance(projects, dict):
        raise SystemExit("projects.json must contain an object")

    return projects


def show_projects(as_json=False):
    projects = load_projects()

    if as_json:
        print(json.dumps(projects, indent=2, ensure_ascii=False))
        return 0

    for name, cfg in projects.items():
        print(
            f"{name:16} "
            f"{cfg['machine']:8} "
            f"{cfg.get('default_agent', '-'):8} "
            f"{cfg['workspace']}"
        )

    return 0


def run_project(
    project,
    prompt,
    agent_override=None,
    model=None,
    effort=None,
):
    projects = load_projects()

    if project not in projects:
        available = ", ".join(sorted(projects))
        raise SystemExit(
            f"Unknown project: {project}. Available: {available}"
        )

    cfg = projects[project]

    machine = cfg["machine"]
    workspace = cfg["workspace"]
    agent = agent_override or cfg.get("default_agent", "codex")

    if machine not in MACHINES:
        raise SystemExit(f"Unknown machine in project {project}: {machine}")

    if agent not in ("codex", "cursor"):
        raise SystemExit(f"Unsupported agent in project {project}: {agent}")

    if agent != "codex" and (model or effort):
        raise SystemExit(
            "--model/--effort are supported only for Codex"
        )

    return run_agent(
        machine,
        agent,
        workspace,
        prompt,
        model=model,
        effort=effort,
    )



def task_state(meta, directory):
    exit_path = directory / "exit_code"

    if exit_path.exists():
        try:
            code = int(exit_path.read_text().strip())
        except ValueError:
            code = -1
        return ("DONE" if code == 0 else "FAILED"), code

    if process_alive(meta.get("pid", 0)):
        return "RUNNING", None

    return "UNKNOWN", None


def project_for_task(meta):
    try:
        projects = load_projects()
    except Exception:
        return "-"

    for name, cfg in projects.items():
        if (
            cfg.get("machine") == meta.get("machine")
            and cfg.get("workspace") == meta.get("workspace")
        ):
            return name

    return "-"


def list_tasks(as_json=False, show_all=False):
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    rows = []

    for directory in DATA_DIR.iterdir():
        if not directory.is_dir():
            continue

        meta_path = directory / "meta.json"
        if not meta_path.exists():
            continue

        try:
            meta = json.loads(meta_path.read_text())
        except Exception:
            continue

        if meta.get("archived", False) and not show_all:
            continue

        state, exit_code = task_state(meta, directory)

        rows.append({
            "id": meta.get("id", directory.name),
            "project": project_for_task(meta),
            "machine": meta.get("machine", "-"),
            "agent": meta.get("agent", "-"),
            "status": state,
            "turns": meta.get("turns", 1),
            "session_id": meta.get("session_id"),
            "workspace": meta.get("workspace"),
            "created_at": meta.get("created_at", ""),
            "updated_at": meta.get("updated_at", ""),
            "exit_code": exit_code,
            "archived": bool(meta.get("archived", False)),
        })

    rows.sort(
        key=lambda x: x.get("updated_at") or x.get("created_at") or "",
        reverse=True,
    )

    if as_json:
        print(json.dumps(rows, indent=2, ensure_ascii=False))
        return 0

    print(
        f"{'TASK':20} {'PROJECT':16} {'AGENT':8} "
        f"{'STATUS':9} {'TURNS':5} {'MACHINE':8}"
    )

    for row in rows:
        print(
            f"{row['id'][:20]:20} "
            f"{row['project'][:16]:16} "
            f"{row['agent'][:8]:8} "
            f"{row['status'][:9]:9} "
            f"{str(row['turns']):5} "
            f"{row['machine'][:8]:8}"
        )

    return 0



def adopt_task(
    project,
    session_id,
    task_id=None,
    agent_override=None,
    model=None,
    effort=None,
):
    projects = load_projects()

    if project not in projects:
        available = ", ".join(sorted(projects))
        raise SystemExit(
            f"Unknown project: {project}. Available: {available}"
        )

    cfg = projects[project]
    machine = cfg["machine"]
    workspace = cfg["workspace"]
    agent = agent_override or cfg.get("default_agent", "codex")

    if agent not in ("codex", "cursor"):
        raise SystemExit(f"Unsupported agent: {agent}")

    # Для Codex проверяем, что session реально существует
    # на нужной машине и принадлежит нужному workspace.
    if agent == "codex":
        script = f"""
python3 - <<'REMOTE_PY'
from pathlib import Path
import json

sid = {session_id!r}
expected = {workspace!r}

root = Path.home() / ".codex" / "sessions"
paths = list(root.rglob(f"*{{sid}}.jsonl"))

if not paths:
    print(json.dumps({{"exists": False}}))
    raise SystemExit(0)

path = paths[0]
cwd = None

with path.open(errors="replace") as f:
    for line in f:
        try:
            obj = json.loads(line)
        except Exception:
            continue

        payload = obj.get("payload")
        if isinstance(payload, dict) and payload.get("cwd"):
            cwd = payload["cwd"]
            break

print(json.dumps({{
    "exists": True,
    "path": str(path),
    "cwd": cwd,
    "expected": expected,
}}))
REMOTE_PY
"""

        result = capture(machine, script)

        if result.returncode != 0:
            raise SystemExit(
                f"Failed to inspect Codex session on {machine}: "
                f"{result.stdout.strip()}"
            )

        lines = [
            line.strip()
            for line in result.stdout.splitlines()
            if line.strip().startswith("{")
        ]

        if not lines:
            raise SystemExit(
                f"Could not inspect Codex session {session_id}"
            )

        info = json.loads(lines[-1])

        if not info.get("exists"):
            raise SystemExit(
                f"Codex session not found on {machine}: {session_id}"
            )

        session_cwd = info.get("cwd")

        if session_cwd and session_cwd != workspace:
            raise SystemExit(
                "Session workspace mismatch:\n"
                f"  session:  {session_cwd}\n"
                f"  project:  {workspace}"
            )

    if not task_id:
        task_id = f"{project}-{session_id[-8:]}"

    directory = task_dir(task_id)

    if (directory / "meta.json").exists():
        raise SystemExit(
            f"Task already exists: {task_id}"
        )

    directory.mkdir(parents=True, exist_ok=True, mode=0o700)

    meta = {
        "id": task_id,
        "project": project,
        "machine": machine,
        "agent": agent,
        "workspace": workspace,
        "session_id": session_id,
        "model": model,
        "reasoning_effort": effort,
        "turns": 0,
        "status": "DONE",
        "pid": 0,
        "created_at": now(),
        "updated_at": now(),
        "adopted": True,
        "git_baseline": None,
        "git_baseline_reason": (
            "adopted session: repository state before "
            "original coding start is unknown"
        ),
    }

    save_meta(directory, meta)

    (directory / "exit_code").write_text("0\n")
    (directory / "events.jsonl").write_text(
        json.dumps({
            "type": "devorchestrator.adopt",
            "timestamp": now(),
            "project": project,
            "agent": agent,
            "session_id": session_id,
        }, ensure_ascii=False) + "\n"
    )
    (directory / "prompt.txt").write_text(
        f"Adopted existing {agent} session {session_id}\n"
    )

    print(task_id)
    return 0



def set_task_archived(task_id, archived=True):
    directory = DATA_DIR / task_id
    meta_path = directory / "meta.json"

    if not meta_path.exists():
        raise SystemExit(f"Unknown task: {task_id}")

    meta = json.loads(meta_path.read_text())
    meta["archived"] = archived
    meta["updated_at"] = now()

    save_meta(directory, meta)

    print(task_id)
    return 0


def task_dir(task_id):
    return DATA_DIR / task_id


def save_meta(directory, meta):
    path = directory / "meta.json"
    path.write_text(json.dumps(meta, indent=2, ensure_ascii=False))
    os.chmod(path, 0o600)


def load_meta(task_id):
    path = task_dir(task_id) / "meta.json"

    if not path.exists():
        raise SystemExit(f"Unknown task: {task_id}")

    return json.loads(path.read_text())


def build_agent_command(
    machine,
    agent,
    workspace,
    prompt,
    model=None,
    effort=None,
):
    if agent == "codex":
        args = [
            "codex",
            "exec",
            "--json",
            "--color", "never",
            "--sandbox", MACHINES[machine].get(
                "codex_sandbox", "workspace-write"
            ),
        ]

        if model:
            args += ["-m", model]

        if effort:
            args += [
                "-c",
                f'model_reasoning_effort="{effort}"',
            ]

        args += [
            "-C", workspace,
            prompt,
        ]
        command = shlex.join(args)

        env = dict(MACHINES[machine].get("agent_env", {}))
        if env:
            prefix = " ".join(
                f"{key}={shlex.quote(value)}"
                for key, value in env.items()
            )
            command = f"env {prefix} {command}"

        return command

    if agent == "cursor":
        args = [
            "agent",
            "--print",
            "--output-format", "stream-json",
            "--sandbox", "enabled",
            "--auto-review",
            "--trust",
            "--workspace", workspace,
            prompt,
        ]
        command = shlex.join(args)

        env = dict(MACHINES[machine].get("agent_env", {}))
        if machine == "firefly":
            env["NODE_USE_ENV_PROXY"] = "1"

        if env:
            prefix = " ".join(
                f"{key}={shlex.quote(value)}"
                for key, value in env.items()
            )
            command = f"env {prefix} {command}"

        return command

    raise ValueError(f"Unsupported agent: {agent}")


def run_agent(
    machine,
    agent,
    workspace,
    prompt,
    model=None,
    effort=None,
):
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    task_id = uuid.uuid4().hex[:12]
    directory = task_dir(task_id)
    directory.mkdir(mode=0o700)

    log_path = directory / "events.jsonl"
    exit_path = directory / "exit_code"
    prompt_path = directory / "prompt.txt"

    prompt_path.write_text(prompt)
    os.chmod(prompt_path, 0o600)

    # IMPORTANT: capture repository state before the coding
    # process starts. Later owner publication gates rely on it.
    git_baseline = capture_git_baseline(
        machine,
        workspace,
    )

    agent_command = build_agent_command(
        machine,
        agent,
        workspace,
        prompt,
        model=model,
        effort=effort,
    )
    actual_command = machine_command(machine, agent_command)

    meta = {
        "id": task_id,
        "machine": machine,
        "agent": agent,
        "workspace": workspace,
        "created_at": now(),
        "status": "RUNNING",
        "git_baseline": git_baseline,
    }

    if model:
        meta["model"] = model

    if effort:
        meta["reasoning_effort"] = effort

    log_file = open(log_path, "ab", buffering=0)
    os.chmod(log_path, 0o600)

    command_string = shlex.join(actual_command)
    wrapper = (
        f"{command_string}; "
        f"rc=$?; "
        f"printf '%s\\n' \"$rc\" > {shlex.quote(str(exit_path))}; "
        f"exit \"$rc\""
    )

    process = subprocess.Popen(
        ["bash", "-lc", wrapper],
        stdin=subprocess.DEVNULL,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )

    meta["pid"] = process.pid
    save_meta(directory, meta)

    print(task_id)
    return 0


def process_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def status(task_id):
    directory = task_dir(task_id)
    meta = load_meta(task_id)

    exit_path = directory / "exit_code"

    if exit_path.exists():
        try:
            exit_code = int(exit_path.read_text().strip())
        except ValueError:
            exit_code = -1

        state = "DONE" if exit_code == 0 else "FAILED"
    elif process_alive(meta["pid"]):
        exit_code = None
        state = "RUNNING"
    else:
        exit_code = None
        state = "UNKNOWN"

    print(f"id:        {task_id}")
    print(f"status:    {state}")
    print(f"machine:   {meta['machine']}")
    print(f"agent:     {meta['agent']}")
    print(f"workspace: {meta['workspace']}")
    print(f"pid:       {meta['pid']}")
    if exit_code is not None:
        print(f"exit_code: {exit_code}")

    return 0


def show_log(task_id, lines):
    directory = task_dir(task_id)
    load_meta(task_id)

    path = directory / "events.jsonl"

    if not path.exists():
        return 0

    with path.open(errors="replace") as f:
        for line in deque(f, maxlen=lines):
            print(line, end="")

    return 0



def find_codex_session_id(task_id):
    path = task_dir(task_id) / "events.jsonl"

    if not path.exists():
        return None

    session_id = None

    with path.open(errors="replace") as f:
        for line in f:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            if event.get("type") == "thread.started":
                value = event.get("thread_id")
                if value:
                    session_id = value

    return session_id



def find_cursor_session_id(task_id):
    path = task_dir(task_id) / "events.jsonl"

    if not path.exists():
        return None

    session_id = None

    with path.open(errors="replace") as f:
        for line in f:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            value = event.get("session_id")
            if value:
                session_id = value

    return session_id


def continue_task(task_id, prompt):
    directory = task_dir(task_id)
    meta = load_meta(task_id)

    exit_path = directory / "exit_code"

    if not exit_path.exists() and process_alive(meta["pid"]):
        raise SystemExit(f"Task {task_id} is still running")

    agent = meta["agent"]
    machine = meta["machine"]
    workspace = meta["workspace"]

    if agent == "codex":
        session_id = (
            meta.get("session_id")
            or find_codex_session_id(task_id)
        )

        if not session_id:
            raise SystemExit(
                f"Codex session id not found for task {task_id}"
            )

        args = [
            "codex",
            "exec",
            "--sandbox",
            MACHINES[machine].get(
                "codex_sandbox",
                "workspace-write",
            ),
            "resume",
            "--json",
        ]

        model = meta.get("model")
        if model:
            args += ["-m", model]

        effort = meta.get("reasoning_effort")
        if effort:
            args += ["-c", f'model_reasoning_effort="{effort}"']

        args += [
            session_id,
            prompt,
        ]

    elif agent == "cursor":
        session_id = (
            meta.get("session_id")
            or find_cursor_session_id(task_id)
        )

        if not session_id:
            raise SystemExit(
                f"Cursor session id not found for task {task_id}"
            )

        args = [
            "agent",
            "--print",
            "--output-format", "stream-json",
            "--sandbox", "enabled",
            "--auto-review",
            "--trust",
            "--workspace", workspace,
            "--resume", session_id,
            prompt,
        ]

    else:
        raise SystemExit(f"Unsupported agent: {agent}")

    command = shlex.join(args)

    env = dict(MACHINES[machine].get("agent_env", {}))

    if agent == "cursor" and machine == "firefly":
        env["NODE_USE_ENV_PROXY"] = "1"

    if env:
        prefix = " ".join(
            f"{key}={shlex.quote(value)}"
            for key, value in env.items()
        )
        command = f"env {prefix} {command}"

    if agent == "codex":
        command = f"cd {shlex.quote(workspace)} && {command}"

    actual_command = machine_command(machine, command)

    exit_path.unlink(missing_ok=True)

    log_path = directory / "events.jsonl"
    log_file = open(log_path, "ab", buffering=0)

    marker_event = {
        "type": "devorchestrator.continue",
        "timestamp": now(),
        "agent": agent,
        "session_id": session_id,
    }

    log_file.write(
        (json.dumps(marker_event, ensure_ascii=False) + "\n").encode()
    )

    command_string = shlex.join(actual_command)

    wrapper = (
        f"{command_string}; "
        f"rc=$?; "
        f"printf '%s\\n' \"$rc\" > {shlex.quote(str(exit_path))}; "
        f"exit \"$rc\""
    )

    process = subprocess.Popen(
        ["bash", "-lc", wrapper],
        stdin=subprocess.DEVNULL,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )

    meta["pid"] = process.pid
    meta["session_id"] = session_id
    meta["updated_at"] = now()
    meta["turns"] = meta.get("turns", 1) + 1

    save_meta(directory, meta)

    print(task_id)
    return 0



def task_report(task_id, as_json=False):
    directory = task_dir(task_id)
    meta = load_meta(task_id)

    exit_path = directory / "exit_code"
    log_path = directory / "events.jsonl"

    if exit_path.exists():
        try:
            exit_code = int(exit_path.read_text().strip())
        except ValueError:
            exit_code = -1
        state = "DONE" if exit_code == 0 else "FAILED"
    elif process_alive(meta["pid"]):
        exit_code = None
        state = "RUNNING"
    else:
        exit_code = None
        state = "UNKNOWN"

    session_id = meta.get("session_id")
    model = meta.get("model")
    reasoning_effort = meta.get("reasoning_effort")
    result = None
    last_error = None
    last_non_json = None

    if log_path.exists():
        with log_path.open(errors="replace") as f:
            for raw in f:
                line = raw.strip()
                if not line:
                    continue

                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    last_non_json = line
                    continue

                event_type = event.get("type")

                if event_type == "thread.started":
                    session_id = event.get("thread_id") or session_id

                elif event_type == "system" and event.get("subtype") == "init":
                    session_id = event.get("session_id") or session_id
                    model = event.get("model") or model

                elif event_type == "item.completed":
                    item = event.get("item") or {}

                    if item.get("type") == "agent_message":
                        text = item.get("text")
                        if text:
                            result = text.strip()

                    elif item.get("type") == "error":
                        last_error = item.get("message")

                elif event_type == "assistant":
                    message = event.get("message") or {}
                    content = message.get("content") or []

                    texts = [
                        part.get("text", "")
                        for part in content
                        if part.get("type") == "text"
                    ]

                    text = "".join(texts).strip()
                    if text:
                        result = text

                    session_id = event.get("session_id") or session_id

                elif event_type == "result":
                    value = event.get("result")
                    if isinstance(value, str) and value.strip():
                        result = value.strip()

                    session_id = event.get("session_id") or session_id

                    if event.get("is_error"):
                        last_error = value

                elif event_type == "error":
                    last_error = event.get("message")

    if state == "FAILED" and not last_error:
        last_error = last_non_json

    report = {
        "id": task_id,
        "status": state,
        "machine": meta["machine"],
        "agent": meta["agent"],
        "workspace": meta["workspace"],
        "session_id": session_id,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "turns": meta.get("turns", 1),
        "exit_code": exit_code,
        "result": result,
        "error": last_error,
    }

    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    print(f"id:        {report['id']}")
    print(f"status:    {report['status']}")
    print(f"machine:   {report['machine']}")
    print(f"agent:     {report['agent']}")
    print(f"workspace: {report['workspace']}")

    if report["session_id"]:
        print(f"session:   {report['session_id']}")

    if report["model"]:
        print(f"model:     {report['model']}")

    if report["reasoning_effort"]:
        print(f"effort:    {report['reasoning_effort']}")

    print(f"turns:     {report['turns']}")

    if report["exit_code"] is not None:
        print(f"exit_code: {report['exit_code']}")

    if report["result"]:
        print()
        print("RESULT:")
        print(report["result"])

    if report["error"]:
        print()
        print("ERROR:")
        print(report["error"])

    return 0


def exec_command(machine, command):
    return subprocess.run(
        machine_command(machine, command),
        stdin=subprocess.DEVNULL,
    ).returncode


def main():
    parser = argparse.ArgumentParser(prog="devctl")
    sub = parser.add_subparsers(dest="action", required=True)

    sub.add_parser("doctor")

    p_projects = sub.add_parser("projects")
    p_projects.add_argument("--json", action="store_true")

    p_tasks = sub.add_parser("tasks")
    p_tasks.add_argument("--json", action="store_true")
    p_tasks.add_argument("--all", dest="show_all", action="store_true")

    p_archive = sub.add_parser("archive")
    p_archive.add_argument("task_id")

    p_unarchive = sub.add_parser("unarchive")
    p_unarchive.add_argument("task_id")

    p_adopt = sub.add_parser("adopt")
    p_adopt.add_argument("project")
    p_adopt.add_argument("session_id")
    p_adopt.add_argument("--task-id", default=None)
    p_adopt.add_argument(
        "--agent",
        choices=["codex", "cursor"],
        default=None,
    )
    p_adopt.add_argument("--model", default=None)
    p_adopt.add_argument("--effort", default=None)

    p_run_project = sub.add_parser("run-project")
    p_run_project.add_argument("project")
    p_run_project.add_argument("prompt")
    p_run_project.add_argument(
        "--agent",
        choices=["codex", "cursor"],
        default=None,
    )
    p_run_project.add_argument("--model", default=None)
    p_run_project.add_argument("--effort", default=None)

    p_exec = sub.add_parser("exec")
    p_exec.add_argument("machine", choices=MACHINES.keys())
    p_exec.add_argument("command")

    p_run = sub.add_parser("run")
    p_run.add_argument("machine", choices=MACHINES.keys())
    p_run.add_argument("agent", choices=["codex", "cursor"])
    p_run.add_argument("workspace")
    p_run.add_argument("prompt")
    p_run.add_argument("--model", default=None)
    p_run.add_argument("--effort", default=None)

    p_status = sub.add_parser("status")
    p_status.add_argument("task_id")

    p_log = sub.add_parser("log")
    p_log.add_argument("task_id")
    p_log.add_argument("--lines", type=int, default=30)

    p_report = sub.add_parser("report")
    p_report.add_argument("task_id")
    p_report.add_argument("--json", action="store_true")

    p_continue = sub.add_parser("continue")
    p_continue.add_argument("task_id")
    p_continue.add_argument("prompt")

    args = parser.parse_args()

    if args.action == "doctor":
        raise SystemExit(doctor())

    if args.action == "projects":
        raise SystemExit(show_projects(args.json))

    if args.action == "tasks":
        raise SystemExit(
            list_tasks(args.json, args.show_all)
        )

    if args.action == "archive":
        raise SystemExit(
            set_task_archived(args.task_id, True)
        )

    if args.action == "unarchive":
        raise SystemExit(
            set_task_archived(args.task_id, False)
        )

    if args.action == "adopt":
        raise SystemExit(
            adopt_task(
                args.project,
                args.session_id,
                args.task_id,
                args.agent,
                args.model,
                args.effort,
            )
        )

    if args.action == "run-project":
        raise SystemExit(
            run_project(
                args.project,
                args.prompt,
                args.agent,
                args.model,
                args.effort,
            )
        )

    if args.action == "exec":
        raise SystemExit(exec_command(args.machine, args.command))

    if args.action == "run":
        raise SystemExit(
            run_agent(
                args.machine,
                args.agent,
                args.workspace,
                args.prompt,
                model=args.model,
                effort=args.effort,
            )
        )

    if args.action == "status":
        raise SystemExit(status(args.task_id))

    if args.action == "log":
        raise SystemExit(show_log(args.task_id, args.lines))

    if args.action == "report":
        raise SystemExit(
            task_report(args.task_id, args.json)
        )

    if args.action == "continue":
        raise SystemExit(
            continue_task(args.task_id, args.prompt)
        )


if __name__ == "__main__":
    main()

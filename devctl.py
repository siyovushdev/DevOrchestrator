#!/usr/bin/env python3

import argparse
import json
import os
import shlex
import subprocess
import sys
import uuid
import re
import hashlib
import time
import signal
from durable import read_json, write_json, exclusive
from health import DOCTOR_TIMEOUT, PROBE_TIMEOUT, SSH_CONNECT_TIMEOUT, health_result
import worker_runtime
import remote_worker
import runtime_liveness
from redaction import redact, redact_data

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
_CHILDREN = []


def reap_processes():
    _CHILDREN[:] = [process for process in _CHILDREN if process.poll() is None]


def spawn_detached(wrapper, log_file):
    try:
        process = subprocess.Popen(["bash", "-lc", wrapper], stdin=subprocess.DEVNULL,
                                   stdout=log_file, stderr=subprocess.STDOUT, start_new_session=True)
        _CHILDREN.append(process)
        return process
    finally:
        log_file.close()



def now():
    return datetime.now(timezone.utc).isoformat()


def machine_command(machine, command, *, connect_timeout=10, ssh_options=()):
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
        "-o", f"ConnectTimeout={connect_timeout}",
        *[arg for option in ssh_options for arg in ("-o", option)],
        target,
        f"bash -lc {shlex.quote(command)}",
    ]


def capture(machine, command, separate_stderr=False):
    return subprocess.run(
        machine_command(machine, command),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE if separate_stderr else subprocess.STDOUT,
        timeout=60,
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

    root_result = capture_git(machine, workspace, "rev-parse", "--show-toplevel")
    push_result = capture_git(machine, workspace, "remote", "get-url", "--push", "--all", "origin")

    # Capture the exact publishable worktree state without touching the user's
    # real index. This lets notification code prove that a reviewed task was
    # truly read-only/no-op even when the repository was already dirty.
    quoted_workspace = shlex.quote(workspace)

    tree_result = capture(
        machine,
        (
            "set -e; "
            'tmp=$(mktemp); rm -f "$tmp"; '
            """trap 'rm -f "$tmp"' EXIT; """
            f"if git -C {quoted_workspace} rev-parse --verify HEAD >/dev/null 2>&1; then "
            f'GIT_INDEX_FILE="$tmp" git -C {quoted_workspace} read-tree HEAD; '
            "fi; "
            f'GIT_INDEX_FILE="$tmp" git -C {quoted_workspace} add -A; '
            f'GIT_INDEX_FILE="$tmp" git -C {quoted_workspace} write-tree'
        ),
        separate_stderr=True,
    )

    index_result = capture(
        machine,
        (
            "set -o pipefail; "
            f"git -C {quoted_workspace} ls-files --stage -z "
            "| sha256sum | awk '{print $1}'"
        ),
        separate_stderr=True,
    )

    root = root_result.stdout.strip() if root_result.returncode == 0 else None
    push_urls = push_result.stdout.splitlines() if push_result.returncode == 0 else []
    worktree_tree = tree_result.stdout.strip() if tree_result.returncode == 0 else None
    index_fingerprint = index_result.stdout.strip() if index_result.returncode == 0 else None

    snapshot_fingerprint = None
    if (
        status_result.returncode == 0
        and root
        and worktree_tree
        and index_fingerprint
    ):
        snapshot = {
            "head": head,
            "branch": branch,
            "origin": origin,
            "root": root,
            "push_urls": push_urls,
            "tree": worktree_tree,
            "status": status_result.stdout,
            "index": index_fingerprint,
        }
        snapshot_fingerprint = hashlib.sha256(
            json.dumps(snapshot, sort_keys=True).encode()
        ).hexdigest()

    return {
        "captured_at": captured_at,
        "is_repo": True,
        "head": head,
        "branch": branch,
        "detached": branch is None,
        "status_porcelain": status_porcelain,
        "clean": status_porcelain == "",
        "origin": origin,
        "root": root,
        "push_urls": push_urls,
        "worktree_tree": worktree_tree,
        "index_fingerprint": index_fingerprint,
        "snapshot_fingerprint": snapshot_fingerprint,
        "error": (
            None
            if status_result.returncode == 0
            else status_result.stdout.strip()[-2000:]
        ),
    }


def doctor_probe(machine, command, timeout):
    # A timed-out local version command may have children holding stdout open.
    # Kill the entire probe group so cleanup cannot wait on those children.
    process = subprocess.Popen(
        machine_command(machine, command, connect_timeout=SSH_CONNECT_TIMEOUT,
                        ssh_options=('ConnectionAttempts=1', 'ServerAliveInterval=1',
                                     'ServerAliveCountMax=2')),
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, errors='replace', start_new_session=True,
    )
    try:
        output, _ = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.communicate()
        raise
    return subprocess.CompletedProcess(process.args, process.returncode, output)


def doctor_health():
    command = """
set -e
host=$(hostname)
user=$(whoami)
echo "HOST=$host"
echo "USER=$user"
echo "CODEX=$(command -v codex || true)"
codex --version 2>/dev/null || true
echo "CURSOR=$(command -v agent || true)"
agent --version 2>/dev/null || true
"""

    deadline = time.monotonic() + DOCTOR_TIMEOUT
    machines = {}
    # Verify required local machines before spending time on optional workers.
    ordered = sorted(MACHINES, key=lambda name: MACHINES[name]['type'] != 'local')
    for machine in ordered:
        kind = MACHINES[machine]['type']
        details = ''
        diagnostic = None
        timeout = min(PROBE_TIMEOUT, max(0, deadline - time.monotonic()))
        try:
            if timeout <= 0:
                diagnostic = 'Doctor probe budget exhausted; machine not probed'
            else:
                result = doctor_probe(machine, command, timeout)
                details = redact(result.stdout.strip())[-2000:]
                if result.returncode != 0:
                    diagnostic = f'Probe exited {result.returncode}: {details[-500:] or "no output"}'
        except subprocess.TimeoutExpired:
            diagnostic = f'{"SSH" if kind == "ssh" else "Local"} probe timed out after {timeout:g}s'
        except OSError as exc:
            diagnostic = f'Probe could not start: {redact(exc)}'
        machines[machine] = {
            'type': kind, 'required': kind == 'local',
            'status': ('FAILED' if kind == 'local' else 'UNAVAILABLE') if diagnostic else
                      ('OK' if kind == 'local' else 'AVAILABLE'),
            'diagnostic': diagnostic, 'details': details,
        }
    return health_result(machines)


def doctor(as_json=False):
    report = doctor_health()
    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"STATE={report['state']} LOCAL_CONTROL_PLANE={report['local_control_plane']}")
        for machine, item in report['machines'].items():
            print(f"\n===== {machine}: {item['status']} =====")
            if item['details']:
                print(item['details'])
            if item['diagnostic']:
                print(item['diagnostic'])
    return 0 if report['ok'] else 1



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
    request_id=None,
    role="coding",
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
        request_id=request_id,
        role=role,
    )



def task_state(meta, directory, refresh_remote=True):
    reap_processes()
    if not refresh_remote:
        meta.pop('_runtime_health', None)
    if meta.get("resume_prepared") or meta.get("launch_prepared"):
        return "UNKNOWN", None
    if meta.get('runtime_generation') or (directory / 'liveness.json').exists():
        if meta.get('remote_key') and refresh_remote:
            state, code = remote_worker.refresh(meta, directory)
            # Final log draining remains an existing control-plane barrier.
            if meta.get('remote_logs_pending'):
                return 'RUNNING', None
            if meta.get('_runtime_health'):
                return runtime_task_state(meta, directory, meta['_runtime_health'])
            return state, code
        health = runtime_liveness.watchdog(directory, meta, probe=not meta.get('remote_key'))
        return runtime_task_state(meta, directory, health)
    if meta.get("remote_key"):
        if not refresh_remote:
            exit_path = directory / "exit_code"
            if exit_path.exists():
                try:
                    code = int(exit_path.read_text().strip())
                    return ("DONE" if code == 0 else "FAILED"), code
                except ValueError:
                    return "UNKNOWN", None
            return meta.get("remote_status", "UNKNOWN"), None
        return remote_worker.refresh(meta, directory)
    exit_path = directory / "exit_code"
    if exit_path.exists():
        try:
            code = int(exit_path.read_text().strip())
        except ValueError:
            code = -1
        if meta.get("machine") == "firefly" and code == 255:
            return "UNKNOWN", None
        return ("DONE" if code == 0 else "FAILED"), code
    if worker_runtime.alive(meta):
        return "RUNNING", None
    # A local wrapper demonstrably died. For SSH, the remote child may still exist.
    if meta.get("pid", 0) > 0 and meta.get("machine") == "work":
        return "FAILED", None
    return "UNKNOWN", None


def runtime_task_state(meta, directory, health):
    """Every status consumer diagnoses completed turns using the same parser."""
    state, code = runtime_liveness.task_status(health)
    if health['state'] == 'TERMINAL' and meta.get('remote_key'):
        if meta.get('remote_logs_pending'):
            return 'RUNNING', None
        if not (directory / 'exit_code').exists():
            return 'UNKNOWN', None  # Remote final logs have not been confirmed.
    if state == 'DONE' and not task_output(meta, directory)['result']:
        runtime_liveness.final_report_missing(directory, dict(meta, runtime_generation=health['generation']))
        meta.pop('_runtime_health', None)
        return 'FAILED', code
    return state, code


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


def runtime_status(meta, directory, state=None):
    if meta.get('remote_key') and meta.get('_runtime_health'):
        return meta['_runtime_health']
    return runtime_liveness.watchdog(directory, meta, probe=not meta.get('remote_key'),
                                     terminal=state in ('DONE', 'FAILED'))


def spawn_runtime(directory, meta, command, log_file):
    meta['project'] = project_for_task(meta)
    meta.pop('runtime_generation', None)
    record = runtime_liveness.initialize(directory, meta)
    meta['runtime_generation'] = record['generation']
    save_meta(directory, meta)
    runner = shlex.join([sys.executable, str(Path(runtime_liveness.__file__).resolve()),
                         str(directory), record['generation'], command])
    return spawn_detached('exec ' + runner, log_file)


def list_tasks(as_json=False, show_all=False, cached=False):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    reconcile_workers()

    rows = []

    directories = [p for p in DATA_DIR.iterdir() if p.name != "archives"]
    if (DATA_DIR / "archives").exists():
        directories.extend((DATA_DIR / "archives").iterdir())
    for directory in directories:
        if not directory.is_dir():
            continue

        meta_path = directory / "meta.json"
        if not meta_path.exists():
            continue

        try:
            meta = json.loads(meta_path.read_text())
            if (not isinstance(meta, dict) or meta.get('id', directory.name) != directory.name or
                    not isinstance(meta.get('machine'), str) or not isinstance(meta.get('workspace'), str) or
                    meta.get('role', 'coding') not in ('coding', 'review')):
                raise ValueError('Invalid task metadata')
        except Exception:
            # Never silently erase uncertain work from bridge admission/publication checks.
            rows.append({'id': directory.name, 'project': '-', 'machine': '-', 'agent': '-',
                         'workspace': None, 'status': 'UNKNOWN', 'role': 'coding', 'turns': 0,
                         'created_at': '', 'updated_at': '', 'session_id': None,
                         'exit_code': None, 'archived': directory.parent.name == 'archives'})
            continue

        try:
            state, exit_code = task_state(meta, directory, refresh_remote=not cached)
            if meta.get('remote_key') and not cached:
                reconcile_remote_observation(directory, meta, state, exit_code)
        except Exception:
            state, exit_code = 'UNKNOWN', None
        if meta.get("archived", False) and not show_all and state in ("DONE", "FAILED"):
            continue

        rows.append({
            "id": meta.get("id", directory.name),
            "project": project_for_task(meta),
            "machine": meta.get("machine", "-"),
            "agent": meta.get("agent", "-"),
            "status": state,
            "turns": meta.get("turns", 1),
            "session_id": meta.get("session_id"),
            "workspace": meta.get("workspace"),
            "role": meta.get("role", "coding"),
            "created_at": str(meta.get("created_at") or ""),
            "updated_at": str(meta.get("updated_at") or ""),
            "exit_code": exit_code,
            "archived": bool(meta.get("archived", False)),
            "liveness": runtime_status(meta, directory, state),
            "cleanup_state": meta.get('cleanup_state'),
            "consistency_diagnostic": meta.get('consistency_diagnostic'),
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


def worker_lease_evidence(directory, meta, state, code, refresh_remote=False):
    """Called under worker.lock; return None when ownership was not classified."""
    if meta.get('launch_prepared') or meta.get('resume_prepared'):
        return None  # task_state deliberately deferred probing this launch/turn.
    remote = meta.get('remote_key') or MACHINES.get(meta.get('machine'), {}).get('type') == 'ssh'
    health = meta.get('_runtime_health') or {}
    # A skipped/failed probe is not a negative ownership probe. Bound durable
    # terminal cleanup remains usable on recovery without another SSH request.
    bound_cleanup = (meta.get('runtime_generation') and
                     meta.get('remote_cleanup_generation') == meta['runtime_generation'] and
                     meta.get('remote_cleanup_state') in ('ABSENT', 'CLEANED', 'AMBIGUOUS'))
    cached_terminal = state in ('DONE', 'FAILED') and bound_cleanup
    probed = (refresh_remote and meta.get('remote_key') and
              (any(type(health.get(k)) is bool for k in ('worker_alive', 'process_alive')) or
               (not meta.get('runtime_generation') and state == 'RUNNING')))
    if remote and not cached_terminal and not probed:
        return None
    if (remote and probed and state not in ('DONE', 'FAILED') and health.get('state') == 'TERMINAL' and
            bound_cleanup and meta['remote_cleanup_state'] == 'AMBIGUOUS'):
        # RUNNING can mean final logs are still downloading, not live ownership.
        return dict(machine=meta.get('machine'), cleanup='AMBIGUOUS')
    result = worker_runtime.reconcile_task(directory, meta, state, code)
    return dict(machine=meta.get('machine'),
                cleanup=result or ('ACTIVE' if state in ('RUNNING', 'STARTING') else 'AMBIGUOUS'))


def reconcile_remote_observation(directory, meta, state, code):
    """Use an existing fresh report probe, without repeating SSH or a stale write."""
    try:
        with exclusive(DATA_DIR.parent / 'worker.lock'):
            snapshot = {k: v for k, v in meta.items() if k != '_runtime_health'}
            if read_json(directory / 'meta.json') != snapshot:
                return  # A resume/newer observation superseded this report.
            proof = worker_lease_evidence(directory, meta, state, code, refresh_remote=True)
            if proof is not None:
                worker_runtime.reconcile_leases(DATA_DIR, {meta['id']: proof})
    except OSError:
        pass  # A busy lock/failed read leaves evidence unclassified for a later report.


def reconcile_workers(refresh_remote=False):
    """Bounded self-healing; skipped remote probes leave leases unclassified."""
    try:
        with exclusive(DATA_DIR.parent / 'worker.lock'):
            started = time.monotonic()
            directories = [p for p in DATA_DIR.iterdir() if p.name != 'archives'] if DATA_DIR.exists() else []
            if (DATA_DIR / 'archives').exists():
                directories.extend((DATA_DIR / 'archives').iterdir())
            present = {p.name for p in directories}
            terminal = {}
            for directory in directories:
                if time.monotonic() - started >= 5:
                    break  # Continue on the next discovery; never block the bridge indefinitely.
                if not directory.is_dir() or directory.is_symlink():
                    continue
                try:
                    meta = read_json(directory / 'meta.json')
                    if not isinstance(meta, dict) or meta.get('id') != directory.name:
                        raise ValueError('Task metadata identity does not match its directory')
                    state, code = task_state(meta, directory, refresh_remote=refresh_remote)
                    proof = worker_lease_evidence(directory, meta, state, code, refresh_remote)
                    if proof is not None:
                        terminal[meta['id']] = proof
                except OSError:
                    continue  # A failed read is unclassified, not negative evidence.
                except (ValueError, TypeError, KeyError):
                    # Visited corrupt metadata is evidence of unbound ownership.
                    terminal[directory.name] = dict(machine=None, cleanup='AMBIGUOUS')
            # The complete directory inventory proves absence, even if the
            # bounded classification sweep did not visit every present task.
            for lease in read_json(DATA_DIR.parent / 'worker_leases.json', {}).values():
                task_id = lease.get('task_id') if isinstance(lease, dict) else None
                if isinstance(task_id, str) and task_id not in present:
                    terminal[task_id] = dict(machine=None, cleanup='MISSING')
            worker_runtime.reconcile_leases(DATA_DIR, terminal)
    except BlockingIOError:
        pass



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
    directory = task_dir(task_id)
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
    if not isinstance(task_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", task_id) or ".." in task_id:
        raise ValueError("Invalid task ID")
    current = DATA_DIR / task_id
    archived = DATA_DIR / "archives" / task_id
    return archived if not current.exists() and archived.exists() else current


def save_meta(directory, meta):
    path = directory / "meta.json"
    # A live SSH probe is scoped to this call, never proof after a restart.
    write_json(path, {k: v for k, v in meta.items() if k != '_runtime_health'})
    os.chmod(path, 0o600)


def load_meta(task_id):
    path = task_dir(task_id) / "meta.json"

    if not path.exists():
        raise SystemExit(f"Unknown task: {task_id}")

    meta = json.loads(path.read_text())
    if isinstance(meta, dict):
        meta.pop('_runtime_health', None)
    return meta


def build_agent_command(
    machine,
    agent,
    workspace,
    prompt,
    model=None,
    effort=None,
    role="coding",
):
    if agent == "codex":
        args = [
            "codex",
            "exec",
            "--json",
            "--color", "never",
            "--sandbox", "read-only" if role == "review" else MACHINES[machine].get(
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


def run_agent(machine, agent, workspace, prompt, model=None, effort=None,
              request_id=None, role="coding"):
    worker_runtime.valid_request(request_id)
    task_id = request_id[:24] if request_id else uuid.uuid4().hex[:12]
    with exclusive(DATA_DIR.parent / "worker.lock", blocking=True):
        import scheduler
        project = next((name for name, cfg in load_projects().items()
                        if cfg["machine"] == machine and cfg["workspace"] == workspace), None)
        if not project:
            raise ValueError("Mutable tasks require a registered workspace")
        _, canonical = scheduler.project_identity(project)
        signature = hashlib.sha256(json.dumps([machine, agent, workspace, prompt, model, effort, role]).encode()).hexdigest()
        def prepare():
            worker_runtime.check_capacity(DATA_DIR, task_id, machine, role, task_state)
            worker_runtime.admit(DATA_DIR, machine, canonical, task_id, task_state)
            directory = task_dir(task_id)
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            save_meta(directory, {"id": task_id, "machine": machine, "agent": agent,
                      "workspace": workspace, "pid": 0, "status": "STARTING", "role": role,
                      "created_at": now(), "git_baseline": None, "launch_prepared": True, "prepared_request": request_id})
        def start():
            return _run_agent(machine, agent, workspace, prompt, model, effort, task_id, role)
        result = worker_runtime.launch(DATA_DIR, request_id, task_id, start, prepare, signature)
        meta = load_meta(task_id)
        if meta.get("launch_prepared") and meta.get("prepared_request") == request_id:
            _run_agent(machine, agent, workspace, prompt, model, effort, task_id, role)
            meta = load_meta(task_id)
        if meta.get("remote_key"):
            remote_worker.start(machine, meta["remote_key"], meta["remote_command"], meta)
        return result



def _run_agent(
    machine,
    agent,
    workspace,
    prompt,
    model=None,
    effort=None,
    task_id=None,
    role="coding",
):
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    task_id = task_id or uuid.uuid4().hex[:12]
    directory = task_dir(task_id)
    directory.mkdir(mode=0o700, exist_ok=True)

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
        role=role,
    )
    actual_command = machine_command(machine, agent_command)

    meta = {
        "id": task_id,
        "machine": machine,
        "agent": agent,
        "workspace": workspace,
        "created_at": now(),
        "status": "STARTING",
        "pid": 0,
        "role": role,
        "git_baseline": git_baseline,
    }

    if model:
        meta["model"] = model

    if effort:
        meta["reasoning_effort"] = effort

    save_meta(directory, meta)

    if MACHINES[machine]["type"] == "ssh":
        meta.update(remote_key=f"{task_id}-turn-1", remote_command=agent_command,
                    project=project_for_task(meta))
        record = runtime_liveness.initialize(directory, meta)
        meta['runtime_generation'] = record['generation']
        save_meta(directory, meta)
        remote_worker.start(machine, meta["remote_key"], agent_command, meta)
        print(task_id)
        return 0

    log_file = open(log_path, "ab", buffering=0)
    os.chmod(log_path, 0o600)

    command_string = shlex.join(actual_command)
    process = spawn_runtime(directory, meta, command_string, log_file)

    meta["pid"] = process.pid
    meta["process_identity"] = worker_runtime.process_identity(process.pid)
    meta["status"] = "RUNNING"
    save_meta(directory, meta)

    print(task_id)
    return 0


def process_alive(pid):
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def readonly_runtime(task_id):
    directory = task_dir(task_id)
    if not (directory / 'meta.json').exists():
        return None
    try:
        meta = load_meta(task_id)
        terminal = meta.get('status') in ('DONE', 'FAILED')
        if not meta.get('runtime_generation') and (directory / 'exit_code').exists():
            terminal = True
        health = runtime_liveness.watchdog(directory, meta, probe=not meta.get('remote_key'),
            terminal=terminal, read_only=True)
        health['session_id'] = task_output(meta, directory)['session_id']
        health['cleanup_state'] = meta.get('cleanup_state')
        health['diagnostic'] = meta.get('consistency_diagnostic')
        return health
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return {'state': 'UNKNOWN', 'reason': 'invalid_task_metadata'}


def status(task_id, as_json=False, issue=False):
    if issue:
        from operational_state import status_view
        bridge = read_json(DATA_DIR.parent / 'task_bridge_state.json', {'issues': {}})
        item = bridge.get('issues', {}).get(str(task_id), {})
        # Never probe SSH, write observations or clean processes.
        runtime = readonly_runtime(item['task_id']) if item.get('task_id') else None
        view = status_view(bridge, task_id, runtime)
        if item.get('review_task_id'):
            view['review_runtime'] = readonly_runtime(item['review_task_id']) or view['review_runtime']
        diagnostics = read_json(DATA_DIR.parent / 'worker_consistency.json', {})
        view['worker_diagnostics'] = diagnostics if diagnostics.get('leases') else {}
        if as_json:
            print(json.dumps(view, ensure_ascii=False, indent=2))
        else:
            health = view['runtime']
            print(f"Issue #{view['issue']} | {view['project']} | {view['lifecycle']}")
            print(f"Coding: {view['coding_agent']} / {view['model']} / {view['effort']} | task {view['task']} | session {view['session']} | turn {view['coding_turn']}")
            print(f"Review: {view['reviewer']} | task {view['review_task']} | round {view['review_round']} | Chief: {view['chief_state']} / {view['chief_bundle']}")
            print(f"Runtime: coding {health.get('state')} ({health.get('reason')}) | review {view['review_runtime'].get('state')} | cleanup {health.get('cleanup_state')} | queue {json.dumps(view['queue'])} | locks {len(view['locks'])}")
            print(f"Publication: {view['publication']} | next action: {view['next_action']} | projection: {view['projection'].get('health')}")
            diagnostics = [view['diagnostics'], view['scheduler_diagnostics'], view['worker_diagnostics'], health.get('diagnostic')]
            if any(diagnostics):
                print('Diagnostics: ' + json.dumps([v for v in diagnostics if v], ensure_ascii=False))
        return 0
    directory = task_dir(task_id)
    meta = load_meta(task_id)

    exit_path = directory / "exit_code"

    state, exit_code = task_state(meta, directory)

    print(f"id:        {task_id}")
    print(f"status:    {state}")
    print(f"machine:   {meta['machine']}")
    print(f"agent:     {meta['agent']}")
    print(f"workspace: {meta['workspace']}")
    print(f"pid:       {meta['pid']}")
    health = runtime_status(meta, directory, state)
    print(f"liveness:  {health['state']} ({health['reason']})")
    print(f"lease:     {health['lease_state']}; heartbeat age: {health['heartbeat_age_seconds']}")
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


def continue_task(task_id, prompt, request_id=None):
    with exclusive(DATA_DIR.parent / "worker.lock", blocking=True):
        meta = load_meta(task_id)
        project = project_for_task(meta)
        if project == "-":
            raise ValueError("Task workspace is no longer registered")
        import scheduler
        _, canonical = scheduler.project_identity(project)
        signature = hashlib.sha256(json.dumps([task_id, prompt]).encode()).hexdigest()
        def prepare():
            state, code = task_state(meta, task_dir(task_id))
            if state not in ("DONE", "FAILED"):
                raise RuntimeError("Task is active or uncertain; resume is blocked")
            if worker_runtime.reconcile_task(task_dir(task_id), meta, state, code) == 'AMBIGUOUS':
                raise RuntimeError('Previous turn runtime ownership is ambiguous; resume is blocked')
            finder = find_codex_session_id if meta["agent"] == "codex" else find_cursor_session_id
            session_id = meta.get("session_id") or finder(task_id)
            if not session_id:
                raise ValueError("Task session ID is unavailable")
            worker_runtime.check_capacity(DATA_DIR, task_id, meta["machine"], meta.get("role", "coding"), task_state)
            worker_runtime.admit(DATA_DIR, meta["machine"], canonical, task_id, task_state)
            directory = task_dir(task_id)
            if directory.parent.name == "archives":
                directory.rename(DATA_DIR / task_id)
            meta["archived"] = False
            log_path = task_dir(task_id) / "events.jsonl"
            meta["turn_log_offset"] = log_path.stat().st_size if log_path.exists() else 0
            meta.update(status="STARTING", pid=0, process_identity=None, session_id=session_id, resume_prepared=True, prepared_request=request_id)
            save_meta(task_dir(task_id), meta)
            (task_dir(task_id) / "exit_code").unlink(missing_ok=True)
        result = worker_runtime.launch(DATA_DIR, request_id, task_id,
                                       lambda: _continue_task(task_id, prompt), prepare, signature)
        meta = load_meta(task_id)
        if meta.get("resume_prepared") and meta.get("prepared_request") == request_id:
            _continue_task(task_id, prompt)
            meta = load_meta(task_id)
        if meta.get("remote_key"):
            remote_worker.start(meta["machine"], meta["remote_key"], meta["remote_command"], meta)
        return result



def _continue_task(task_id, prompt):
    directory = task_dir(task_id)
    meta = load_meta(task_id)

    exit_path = directory / "exit_code"

    if not meta.get("remote_key") and not exit_path.exists() and worker_runtime.alive(meta):
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
            "read-only" if meta.get("role") == "review" else MACHINES[machine].get(
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

    meta.update(status="STARTING", pid=0, process_identity=None)
    save_meta(directory, meta)
    exit_path.unlink(missing_ok=True)

    if MACHINES[machine]["type"] == "ssh":
        meta.update(remote_key=f"{task_id}-turn-{meta.get('turns', 1) + 1}", remote_command=command, resume_prepared=False, remote_status='UNKNOWN',
                    remote_offset=0, session_id=session_id, updated_at=now(), turns=meta.get("turns", 1) + 1)
        meta.pop("remote_local_size", None)
        meta.pop('runtime_generation', None)
        meta.pop('_runtime_health', None)
        meta['project'] = project_for_task(meta)
        record = runtime_liveness.initialize(directory, meta)
        meta['runtime_generation'] = record['generation']
        save_meta(directory, meta)
        # Keep prior log history; refresh appends this turn using a separate remote offset.
        with (directory / "events.jsonl").open("ab") as log:
            log.write((json.dumps({"type": "devorchestrator.continue", "timestamp": now()}) + "\n").encode())
        remote_worker.start(machine, meta["remote_key"], command, meta)
        print(task_id)
        return 0

    meta["resume_prepared"] = False
    save_meta(directory, meta)
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

    meta['turns'] = meta.get('turns', 1) + 1
    process = spawn_runtime(directory, meta, command_string, log_file)

    meta["pid"] = process.pid
    meta["process_identity"] = worker_runtime.process_identity(process.pid)
    meta["status"] = "RUNNING"
    meta["session_id"] = session_id
    meta["updated_at"] = now()

    save_meta(directory, meta)

    print(task_id)
    return 0



def task_output(meta, directory):
    """Parse only this coding/review turn; shared by status and full reports."""
    log_path = directory / "events.jsonl"

    session_id = meta.get("session_id")
    model = meta.get("model")
    reasoning_effort = meta.get("reasoning_effort")
    result = None
    last_error = None
    last_non_json = None

    if log_path.exists():
        with log_path.open(errors="replace") as f:
            f.seek(meta.get("turn_log_offset", 0))
            for raw in f:
                line = raw.strip()
                if not line:
                    continue

                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    last_non_json = line
                    continue

                if not isinstance(event, dict):
                    continue
                event_type = event.get("type")

                if event_type == "devorchestrator.continue":
                    # Legacy logs lack a per-turn offset but include this boundary marker.
                    result = last_error = last_non_json = None
                elif event_type == "thread.started":
                    session_id = event.get("thread_id") or session_id

                elif event_type == "system" and event.get("subtype") == "init":
                    session_id = event.get("session_id") or session_id
                    model = event.get("model") or model

                elif event_type == "item.completed":
                    item = event.get("item") or {}
                    if not isinstance(item, dict):
                        continue

                    if item.get("type") == "agent_message":
                        text = item.get("text")
                        if isinstance(text, str) and text.strip():
                            result = text.strip()

                    elif item.get("type") == "error":
                        last_error = item.get("message")

                elif event_type == "assistant":
                    message = event.get("message") or {}
                    if not isinstance(message, dict):
                        continue
                    content = message.get("content") or []
                    if not isinstance(content, list):
                        continue

                    texts = [
                        part.get("text", "")
                        for part in content
                        if isinstance(part, dict) and part.get("type") == "text" and isinstance(part.get('text', ''), str)
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

    return dict(session_id=session_id, model=model, reasoning_effort=reasoning_effort,
                result=result, last_error=last_error, last_non_json=last_non_json)


def task_report(task_id, as_json=False):
    reconcile_workers()
    directory = task_dir(task_id)
    meta = load_meta(task_id)

    state, exit_code = task_state(meta, directory)
    if meta.get('remote_key'):
        reconcile_remote_observation(directory, meta, state, exit_code)

    output = task_output(meta, directory)
    session_id = output['session_id']
    model = output['model']
    reasoning_effort = output['reasoning_effort']
    result = output['result']
    last_error = output['last_error']
    last_non_json = output['last_non_json']

    if state == "FAILED" and not last_error:
        last_error = last_non_json

    if session_id and meta.get('runtime_generation'):
        try:
            runtime_liveness.annotate_session(directory, meta, session_id)
        except (OSError, ValueError, TypeError):
            pass  # The normalized watchdog below reports invalid records closed.
    health = runtime_status(meta, directory, state)
    if session_id:
        health['session_id'] = session_id
    if state == 'FAILED' and not last_error:
        last_error = health['reason']

    report = {
        "id": task_id,
        "git_baseline": redact_data(meta.get("git_baseline")),
        "adopted": bool(meta.get("adopted")),
        "created_at": meta.get("created_at"),
        "updated_at": meta.get("updated_at"),
        "status": state,
        "machine": meta["machine"],
        "agent": meta["agent"],
        "workspace": meta["workspace"],
        "session_id": session_id,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "turns": meta.get("turns", 1),
        "exit_code": exit_code,
        "result": redact(result) if result else result,
        "error": redact(last_error) if last_error else last_error,
        "liveness": health,
        "cleanup_state": meta.get('cleanup_state'),
        "consistency_diagnostic": meta.get('consistency_diagnostic'),
    }

    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    print(f"id:        {report['id']}")
    print(f"status:    {report['status']}")
    print(f"liveness:  {health['state']} ({health['reason']})")
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

    p_doctor = sub.add_parser("doctor")
    p_doctor.add_argument("--json", action="store_true")

    p_projects = sub.add_parser("projects")
    p_projects.add_argument("--json", action="store_true")

    p_tasks = sub.add_parser("tasks")
    p_tasks.add_argument("--json", action="store_true")
    p_tasks.add_argument("--all", dest="show_all", action="store_true")
    p_tasks.add_argument("--cached", action="store_true", help="Use durable remote status without SSH probes")

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
    p_run_project.add_argument("--request-id", default=None)
    p_run_project.add_argument("--role", choices=["coding", "review"], default="coding")

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
    p_status.add_argument('--issue', action='store_true', help='Read the durable bridge/operator view by issue number')
    p_status.add_argument('--json', action='store_true')

    p_log = sub.add_parser("log")
    p_log.add_argument("task_id")
    p_log.add_argument("--lines", type=int, default=30)

    p_report = sub.add_parser("report")
    p_report.add_argument("task_id")
    p_report.add_argument("--json", action="store_true")

    p_continue = sub.add_parser("continue")
    p_continue.add_argument("task_id")
    p_continue.add_argument("prompt")
    p_continue.add_argument("--request-id", default=None)

    args = parser.parse_args()

    if args.action == "doctor":
        raise SystemExit(doctor(args.json))

    if args.action == "projects":
        raise SystemExit(show_projects(args.json))

    if args.action == "tasks":
        raise SystemExit(
            list_tasks(args.json, args.show_all, args.cached)
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
                args.request_id,
                args.role,
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
        raise SystemExit(status(args.task_id, args.json, args.issue))

    if args.action == "log":
        raise SystemExit(show_log(args.task_id, args.lines))

    if args.action == "report":
        raise SystemExit(
            task_report(args.task_id, args.json)
        )

    if args.action == "continue":
        raise SystemExit(
            continue_task(args.task_id, args.prompt, args.request_id)
        )


if __name__ == "__main__":
    main()

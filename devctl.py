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
import execution_profiles
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
    __import__('admission_policy').require_operation('legacy_launch')
    from external_effects import refuse
    refuse('worker')
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
    from external_effects import refuse
    refuse('process_read')
    return subprocess.run(
        machine_command(machine, command),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE if separate_stderr else subprocess.STDOUT,
        timeout=60,
    )



def capture_execution(machine, command, *, timeout=PROBE_TIMEOUT):
    from external_effects import refuse
    refuse('process_read')
    env = dict(MACHINES[machine].get('agent_env', {}))
    if machine == 'firefly' and command.startswith('agent '):
        env['NODE_USE_ENV_PROXY'] = '1'
    if env:
        prefix = ' '.join(f'{key}={shlex.quote(value)}' for key, value in env.items())
        command = f'env {prefix} {command}'
    return subprocess.run(machine_command(machine, command, connect_timeout=SSH_CONNECT_TIMEOUT),
                          text=True, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          timeout=timeout)


def capture_git(machine, workspace, *args):
    from repository_safety import GIT_PREAMBLE, bounded_script
    script=GIT_PREAMBLE + '\nimport sys; p=sys.argv[1]; assert_git_config(p); sys.stdout.buffer.write(safe_git(p,sys.argv[2:]))'
    return capture(machine, shlex.join(['/usr/bin/python3','-I','-B','-c',bounded_script(script),workspace,*args]))


def capture_git_baseline(machine, workspace):
    """
    Capture repository state BEFORE the coding agent starts.

    This is a publication safety baseline, not a task result.
    A dirty baseline is allowed for coding/review, but later
    owner COMMIT must reject automatic publication.
    """
    from publication import SNAPSHOT_SCRIPT
    from repository_safety import bounded_script
    result=capture(machine,shlex.join(['/usr/bin/python3','-I','-B','-c',bounded_script(SNAPSHOT_SCRIPT),workspace]),separate_stderr=True)
    if result.returncode:
        raise RuntimeError('Cannot safely capture bounded repository baseline')
    value=json.loads(result.stdout)
    return dict(captured_at=now(),is_repo=True,head=value['head'],branch=value['branch'],
        detached=value['branch'] is None,status_porcelain=value['status'].rstrip('\n'),
        clean=value['status'].rstrip('\n')=='',origin=value['origin'],root=value['root'],
        push_urls=value['push_urls'],worktree_tree=value['tree'],index_fingerprint=value['index'],
        snapshot_fingerprint=value['fingerprint'],error=None)


def doctor_probe(machine, command, timeout):
    from external_effects import refuse
    refuse('worker')
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
    from external_effects import refuse
    refuse('worker')
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
    if os.environ.get('DEVORCHESTRATOR_ADMISSION_POLICY'):
        from admission_policy import policy, strict
        return strict(policy()[1]['registry_file'])
    if not PROJECTS_FILE.exists():
        raise SystemExit(f"Projects registry not found: {PROJECTS_FILE}")

    projects = json.loads(PROJECTS_FILE.read_text())

    if not isinstance(projects, dict):
        raise SystemExit("projects.json must contain an object")

    return projects



def execution_catalog(project=None, discover=False, agent=None):
    from external_effects import blocked, refuse
    if discover:
        __import__('admission_policy').require_operation('discovery')
    cfg = load_projects()[project] if project else {}
    catalog, identity = execution_profiles.load_catalog(cfg)
    result = dict(catalog=catalog, identity=identity)
    if discover:
        if not project:
            raise ValueError('--discover requires a registered --project')
        result['runtime'] = {}
        for selected in ([agent] if agent else execution_profiles.AGENTS):
            capabilities = execution_profiles.discover(cfg['machine'], selected, capture_execution)
            result['runtime'][selected] = {k: v for k, v in capabilities.items() if k != 'help'}
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


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
    execution_profile=None,
    execution_options=None,
    review_binding=None, repository_binding=None,
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
    if machine not in MACHINES:
        raise SystemExit(f"Unknown machine in project {project}: {machine}")

    return run_agent(
        machine, agent_override, workspace, prompt, model=model, effort=effort,
        request_id=request_id, role=role, execution_profile=execution_profile,
        execution_options=execution_options, review_binding=review_binding, repository_binding=repository_binding,
    )



def stored_task_status(meta, directory):
    """Read-only completion diagnosis, using the authoritative result parser.

    Cached metadata alone cannot establish terminal completion. A generation
    mismatch invalidates even an apparently successful exit marker.
    """
    if meta.get('resume_prepared') or meta.get('launch_prepared') or meta.get('remote_logs_pending'):
        return 'UNKNOWN', None
    record = None
    try:
        record = runtime_liveness.read_record(directory)
        if record is not None or meta.get('runtime_generation'):
            runtime_liveness.validate(record, meta)
    except (OSError, ValueError, TypeError, KeyError):
        return 'UNKNOWN', None
    exit_path = directory / 'exit_code'
    if exit_path.exists():
        try:
            code = int(exit_path.read_text().strip())
        except (OSError, ValueError):
            return 'UNKNOWN', None
        if meta.get('machine') == 'firefly' and code == 255:
            return 'UNKNOWN', None
        if record and (record.get('phase') != 'EXITED' or record.get('exit_code') != code):
            return 'UNKNOWN', None
    elif record and record.get('phase') == 'EXITED':
        code = record['exit_code']
        if meta.get('remote_key'):
            return 'UNKNOWN', None  # No confirmed final remote log drain.
    else:
        return 'UNKNOWN', None
    if code != 0 or (record and record.get('terminal_reason') != 'process_exited'):
        return 'FAILED', code
    return ('DONE' if task_output(meta, directory)['result'] else 'FAILED'), code


def stored_runtime_health(directory, state):
    """Cached health is not current ownership or failure evidence."""
    return {'state': 'UNKNOWN', 'reason': 'reconciliation_blocked',
            'observation_confidence': 'READ_ONLY', 'lease_state': 'UNKNOWN',
            'heartbeat_age_seconds': None}


def task_state(meta, directory, refresh_remote=True):
    from external_effects import blocked
    if blocked() or not __import__('admission_policy').task_allowed(meta.get('id',directory.name)):
        reap_processes()
        return stored_task_status(meta, directory)
    reap_processes()
    if not refresh_remote:
        meta.pop('_runtime_health', None)
    if meta.get("resume_prepared") or meta.get("launch_prepared"):
        return "UNKNOWN", None
    if meta.get('native_codex'):
        from native_supervisor import terminal_cleanup
        result = read_json(directory / 'native-watchdog/bounded-result.json', {})
        if result.get('state') == 'UNKNOWN': return 'UNKNOWN', None
        if (directory / 'exit_code').exists():
            state, code = stored_task_status(meta, directory)
            proof = terminal_cleanup(directory, meta, state)
            return (state, code) if proof == 'ABSENT' else ('UNKNOWN', None)
        health = runtime_liveness.watchdog(directory, meta)
        if health['state'] != 'RUNNING_HEALTHY': return 'UNKNOWN', None
        return 'RUNNING', None
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
    from external_effects import blocked
    if blocked() or not __import__('admission_policy').task_allowed(meta.get('id',directory.name)):
        return stored_runtime_health(directory, state)
    if meta.get('remote_key') and meta.get('_runtime_health'):
        return meta['_runtime_health']
    return runtime_liveness.watchdog(directory, meta, probe=not meta.get('remote_key'),
                                     terminal=state in ('DONE', 'FAILED'))


def spawn_runtime(directory, meta, command, log_file):
    from external_effects import refuse
    refuse('worker')
    meta['project'] = project_for_task(meta)
    if meta.get('native_codex'):
        from native_codex import spawn
        process = spawn(directory, meta, log_file)
        _CHILDREN.append(process)
        return process
    meta.pop('runtime_generation', None)
    record = runtime_liveness.initialize(directory, meta)
    meta['runtime_generation'] = record['generation']
    save_meta(directory, meta)
    runner = shlex.join([sys.executable, str(Path(runtime_liveness.__file__).resolve()),
                         str(directory), record['generation'], command])
    return spawn_detached('exec ' + runner, log_file)


def list_tasks(as_json=False, show_all=False, cached=False, control=False, control_project=None):
    from external_effects import blocked
    if not blocked():
        DATA_DIR.mkdir(parents=True, exist_ok=True)
    # Bulk legacy reconciliation is never triggered by observational list APIs.
    if not control and not blocked() and __import__('admission_policy').operation_allowed('reconcile'):
        reconcile_workers()

    rows = []
    scope = load_projects()[control_project] if control_project else None

    directories = [p for p in DATA_DIR.iterdir() if p.name != "archives"] if DATA_DIR.exists() else []
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

        if scope and meta['machine'] != scope['machine']:
            continue
        try:
            state, exit_code = task_state(meta, directory, refresh_remote=not cached)
            if control and state in ('DONE', 'FAILED'):
                # A control read never performs historical process cleanup. It
                # blocks on surviving/uncertain ownership until heavy recovery
                # supplies cleanup proof, including terminal wrapper children.
                if not worker_runtime.publication_safe(directory, meta):
                    state, exit_code = 'UNKNOWN', None
            if meta.get('remote_key') and not cached and not blocked() and __import__('admission_policy').task_allowed(meta.get('id',directory.name)):
                reconcile_remote_observation(directory, meta, state, exit_code)
        except Exception:
            state, exit_code = 'UNKNOWN', None
        if meta.get("archived", False) and not show_all and state in ("DONE", "FAILED"):
            continue

        rows.append({
            "id": meta.get("id", directory.name),
            "observation_confidence": 'READ_ONLY' if blocked() else 'RUNTIME',
            "project": project_for_task(meta),
            "machine": meta.get("machine", "-"),
            "agent": meta.get("agent", "-"),
            "execution": meta.get("execution"),
            "model": meta.get("model"),
            "execution_options": (meta.get("execution") or {}).get("options", {}),
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

    if control:
        present = {p.name for p in directories if p.is_dir()}
        for lease in read_json(DATA_DIR.parent / 'worker_leases.json', {}).values():
            if not isinstance(lease, dict):
                raise ValueError('Worker lease identity unavailable')
            if lease.get('task_id') not in present:
                rows.append(dict(id='missing-lease-' + str(lease.get('task_id')), status='UNKNOWN',
                                 machine=lease.get('machine', '-'), project='-', workspace=None))
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
    from external_effects import blocked
    if blocked():
        return
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
    __import__('admission_policy').require_operation('reconcile')
    """Bounded self-healing; skipped remote probes leave leases unclassified."""
    from external_effects import blocked
    if blocked():
        return
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


def certify_local_absent(task_id, generation, *, control_group_reset_verified=False):
    """Offline operator attestation, only after verifying a complete MCP cgroup reset.

    The reset is external evidence asserted by the caller, not inferred from PIDs.
    No startup or reconciliation path calls this helper.
    """
    from external_effects import refuse
    refuse('worker')
    if control_group_reset_verified is not True:
        raise ValueError('A verified complete MCP control-group reset is required')
    with exclusive(DATA_DIR.parent / 'worker.lock', blocking=True):
        directory = task_dir(task_id)
        meta = load_meta(task_id)
        if not isinstance(meta, dict) or meta.get('id') != task_id:
            raise ValueError('Task metadata identity does not match its directory')
        if meta.get('remote_key') or MACHINES.get(meta.get('machine'), {}).get('type') != 'local':
            raise ValueError('Cleanup attestation requires a local task')
        if not isinstance(generation, str) or not generation or meta.get('runtime_generation') != generation:
            raise ValueError('Runtime generation changed or is missing')
        state, _ = task_state(meta, directory, refresh_remote=False)
        if state not in ('DONE', 'FAILED'):
            raise ValueError('Cleanup attestation requires a terminal task')
        meta.update(cleanup_state='ABSENT', cleanup_generation=generation,
                    cleanup_proof='systemd_control_group_reset')
        save_meta(directory, meta)
        return meta



def adopt_task(
    project,
    session_id,
    task_id=None,
    agent_override=None,
    model=None,
    effort=None,
):
    from external_effects import refuse
    refuse('worker')
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
    __import__('admission_policy').require_operation('maintenance')
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
    execution=None,
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

        if execution:
            args += execution_profiles.cli_options(execution)
        elif model:
            args += ["-m", model]

        if effort and not execution:
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
        if execution:
            args[-1:-1] = execution_profiles.cli_options(execution)
        elif effort:
            raise ValueError("Cursor effort requires a validated execution profile")
        elif model:
            args[-1:-1] = ["--model", model]
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



def start_signature(project, machine, agent, workspace, prompt, model, effort, role,
                    execution_profile, execution_options, request_id, review_binding=None, repository_binding=None):
    inputs = [machine, agent, workspace, prompt, model, effort, role, execution_profile, execution_options]
    receipt = read_json(DATA_DIR.parent / 'requests' / (request_id + '.json')) if request_id else None
    if receipt and receipt.get('status') == 'FENCED':
        raise worker_runtime.RequestFenced('Request was durably fenced before launch')
    if repository_binding is not None:
        if review_binding is not None or (receipt and receipt.get('signature_version') != 4):
            raise RuntimeError('Cannot retrofit repository binding into an existing request')
        inputs.append({'repository_binding': repository_binding})
    elif receipt and receipt.get('signature_version') == 4:
        raise RuntimeError('Bound start requires its original repository binding')
    if review_binding is not None:
        if receipt and receipt.get('signature_version') != 3:
            raise RuntimeError('Cannot retrofit review binding into an existing request')
        inputs.append(review_binding)
    elif receipt and receipt.get('signature_version') == 3:
        raise RuntimeError('Bound review request requires its original source binding')
    if receipt and receipt.get('signature_version') not in (2, 3, 4):
        # Historical receipts retain their original signatures. Execution-era
        # provenance distinguishes omitted agent from an explicit default agent.
        meta = load_meta(receipt['task_id'])
        saved = meta.get('execution')
        if saved and (saved['decision_sources']['agent'] == 'explicit') != (agent in execution_profiles.AGENTS):
            raise ValueError('Request ID reused with different requested agent')
        inputs = [machine, agent or project.get('default_agent', 'codex'), workspace, prompt, model, effort, role]
        if execution_profile is not None or execution_options is not None:
            inputs += [execution_profile, execution_options]
    return hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()


def validated_execution(project, machine, capture=None, **arguments):
    try:
        return execution_profiles.launch_identity(project, machine, capture or capture_execution, **arguments)
    except ValueError as exc:
        raise execution_profiles.PreflightRejected(str(exc)) from exc


def validate_review_source(project, machine, workspace, binding):
    """Called under worker.lock; no bridge state or owner authorization involved."""
    import review_transitions
    import scheduler
    from publication import Repository
    review_transitions.validate_launch_binding(binding)
    cfg = load_projects().get(project)
    if (not cfg or cfg.get('machine') != machine or cfg.get('workspace') != workspace or
            binding['machine'] != machine or
            review_transitions.digest(cfg) != binding['registry_digest'] or
            execution_profiles.load_catalog(cfg)[1]['sha256'] != binding['catalog_digest'] or
            scheduler.project_identity(project) != (machine, binding['repository'])):
        raise review_transitions.SourceChanged('Review repository or execution policy changed')
    meta = load_meta(binding['source_task_id'])
    if (meta.get('machine') != machine or meta.get('workspace') != workspace or
            meta.get('role', 'coding') != 'coding' or
            type(meta.get('turns', 1)) is not int or
            meta.get('turns', 1) != binding['source_turn'] or
            meta.get('runtime_generation') != binding['source_generation']):
        raise review_transitions.SourceChanged('Review coding source identity changed')
    state, code = task_state(meta, task_dir(meta['id']))
    if state == 'UNKNOWN':
        raise execution_profiles.RuntimeUnavailable('Review coding source is uncertain')
    if state != 'DONE' or type(code) is not int or code != 0:
        raise review_transitions.SourceChanged('Review coding source is no longer successful')
    if Repository(project).snapshot() != binding['snapshot']:
        raise review_transitions.SourceChanged('Review source tree changed')


def preflight_task(operation, arguments):
    """Receipt-only replay is observational; fresh runtime probes require admission."""
    request_id = arguments.get('request_id')
    worker_runtime.valid_request(request_id)
    receipt = read_json(DATA_DIR.parent / 'requests' / (request_id + '.json')) if request_id else None
    if receipt:
        return _authorized_preflight_task(operation, arguments)
    from admission_policy import execution, validate_rpc
    validate_rpc(operation, arguments)
    with execution(arguments, DATA_DIR):
        return _authorized_preflight_task(operation, arguments)


def _authorized_preflight_task(operation, arguments):
    """Read-only validation, including runtime, on the same host as launches.

    Existing durable requests are replayed without applying changed catalog
    policy. An unavailable/corrupt receipt is not proof that a launch failed.
    """
    request_id = arguments.get('request_id')
    worker_runtime.valid_request(request_id)
    receipt = read_json(DATA_DIR.parent / 'requests' / (request_id + '.json')) if request_id else None
    if receipt:
        if receipt.get('status') == 'FENCED':
            return {'status': 'FENCED', 'launch_started': False}
        return {'status': 'REPLAY'}
    from external_effects import blocked, refuse
    if blocked():
        refuse('worker')
    if request_id and (task_dir(request_id[:24]) / 'meta.json').exists() and operation == 'start_task':
        raise RuntimeError('Existing task metadata without request receipt; inspect worker ownership before retrying')
    # Bound read-only discovery independently from the full issue transition.
    # Probe timeouts share a deadline, rather than multiplying per CLI.
    deadline = time.monotonic() + 3
    def capture(machine, command):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(command, 3)
        return capture_execution(machine, command, timeout=min(PROBE_TIMEOUT, remaining))
    try:
        role = arguments.get('role', 'coding')
        if role not in ('coding', 'review'):
            raise ValueError('Execution role must be coding or review')
        if operation == 'start_task':
            project = load_projects().get(arguments.get('project'))
            if not project or project.get('machine') not in MACHINES:
                raise ValueError('Task requires a registered project on a known machine')
            execution = validated_execution(project, project['machine'], capture=capture, role=role,
                **{key: arguments.get(key) for key in ('agent', 'model', 'effort', 'execution_profile', 'execution_options')})
        elif operation == 'continue_task':
            meta = load_meta(arguments['task_id'])
            if meta.get('role', 'coding') != role:
                raise ValueError(f'Continuation role {role} does not match saved task role {meta.get("role", "coding")}')
            execution = execution_profiles.resume_identity(meta, capture)
        else:
            raise ValueError('Preflight operation must be start_task or continue_task')
        return {'status': 'READY', 'execution': execution}
    except execution_profiles.RuntimeUnavailable:
        # Transient discovery is not a confirmed invalid model/policy. No launch
        # occurs here; the bridge retains queue ownership and retries visibly.
        return {'status': 'UNAVAILABLE', 'launch_started': False}
    except ValueError as exc:
        return execution_profiles.rejection(exc)


def self_module():
    return sys.modules[__name__]


def run_agent(machine, agent, workspace, prompt, model=None, effort=None,
              request_id=None, role="coding", execution_profile=None, execution_options=None,
              review_binding=None, repository_binding=None):
    from admission_policy import execution, emergency
    emergency()
    project = next((name for name, cfg in load_projects().items()
                    if cfg.get('machine') == machine and cfg.get('workspace') == workspace), None)
    arguments = dict(project=project, agent=agent, prompt=prompt, model=model, effort=effort,
                     request_id=request_id, role=role, execution_profile=execution_profile,
                     execution_options=execution_options, review_binding=review_binding,
                     repository_binding=repository_binding)
    with execution(arguments, DATA_DIR):
        return _admitted_run_agent(machine, agent, workspace, prompt, model, effort, request_id,
                                   role, execution_profile, execution_options, review_binding, repository_binding)


def _admitted_run_agent(machine, agent, workspace, prompt, model=None, effort=None,
              request_id=None, role="coding", execution_profile=None, execution_options=None,
              review_binding=None, repository_binding=None):
    from external_effects import refuse
    refuse('worker')
    worker_runtime.valid_request(request_id)
    task_id = request_id[:24] if request_id else uuid.uuid4().hex[:12]
    with __import__("admission_policy").finite_lock(DATA_DIR.parent / "worker.lock"):
        import scheduler
        projects = load_projects()
        project = next((name for name, cfg in projects.items()
                        if cfg["machine"] == machine and cfg["workspace"] == workspace), None)
        if not project:
            raise ValueError("Mutable tasks require a registered workspace")
        if review_binding is not None and (role != 'review' or not request_id):
            raise ValueError('A bound review requires the review role and a request ID')
        if repository_binding is not None and (role != 'coding' or not request_id):
            raise ValueError('A repository-bound start requires the coding role and request ID')
        _, canonical = scheduler.project_identity(project)
        signature = start_signature(projects[project], machine, agent, workspace, prompt, model, effort,
                                    role, execution_profile, execution_options, request_id, review_binding, repository_binding)
        def validate_repository():
            from review_transitions import digest
            expected = dict(machine=machine, repository=canonical, registry_digest=digest(projects[project]))
            if repository_binding != expected:
                raise execution_profiles.StaleReviewInput('Registered repository changed before launch')
        import native_codex
        native_config = native_codex.config(DATA_DIR)
        __import__('admission_policy').native_required(native_config)
        if native_config:
            previous = read_json(task_dir(task_id) / 'meta.json', {})
            if previous and (not previous.get('native_codex') or previous.get('launch_prepared') or previous.get('resume_prepared') or
                             previous.get('cleanup_state') == 'AMBIGUOUS' or previous.get('runtime_cleanup_pending')):
                raise RuntimeError('UNKNOWN native request; no admission or resume; owner forward repair required')
        def prepare():
            if repository_binding is not None:
                validate_repository()
            execution = validated_execution(
                projects[project], machine, agent=agent, model=model, effort=effort,
                execution_profile=execution_profile, execution_options=execution_options, role=role)
            if review_binding is not None:
                from review_transitions import SourceChanged
                try:
                    validate_review_source(project, machine, workspace, review_binding)
                except SourceChanged as exc:
                    # prepare has not admitted capacity, created metadata, or
                    # crossed the worker receipt boundary at this point.
                    raise execution_profiles.StaleReviewInput('Review source changed before launch') from exc
            from admission_policy import reserve
            reserve(DATA_DIR)
            prepared_native = None
            if native_config:
                prepared_native = native_codex.prepare(self_module(), native_config, project, machine, agent,
                    workspace, prompt, execution, role, request_id, review_binding)
            worker_runtime.check_capacity(DATA_DIR, task_id, machine, role, task_state)
            worker_runtime.admit(DATA_DIR, machine, canonical, task_id, task_state)
            directory = task_dir(task_id)
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            save_meta(directory, {"id": task_id, "machine": machine, "agent": execution["agent"],
                      "execution": execution, "model": execution["model"], "reasoning_effort": execution["options"].get("effort"),
                      "workspace": workspace, "pid": 0, "status": "STARTING", "role": role,
                      "created_at": now(), "git_baseline": None, "launch_prepared": True, "prepared_request": request_id,
                      **({'native_codex': prepared_native} if prepared_native else {}),
                      'admission': __import__('admission_policy').require_worker(),
                      **({'review_binding': review_binding, 'launch_request_id': request_id}
                         if review_binding is not None else {}),
                      **({'repository_binding': repository_binding, 'launch_request_id': request_id}
                         if repository_binding is not None else {})})
        def start():
            prepared = load_meta(task_id)
            return _run_agent(machine, prepared['agent'], workspace, prompt, model, effort, task_id, role,
                              execution=prepared['execution'])
        result = worker_runtime.launch(DATA_DIR, request_id, task_id, start, prepare, signature,
                                       signature_version=4 if repository_binding is not None else 3 if review_binding is not None else 2)
        meta = load_meta(task_id)
        if meta.get("launch_prepared") and meta.get("prepared_request") == request_id:
            if repository_binding is not None:
                try:
                    validate_repository()
                except execution_profiles.StaleReviewInput as exc:
                    raise RuntimeError('Prepared repository binding changed; original request retained') from exc
            if review_binding is not None:
                # This is an existing prepared request: failure is uncertain,
                # never the definitive prelaunch StaleReviewInput contract.
                validate_review_source(project, machine, workspace, review_binding)
            execution_profiles.resume_identity(meta, capture_execution)
            _run_agent(machine, meta['agent'], workspace, prompt, model, effort, task_id, role,
                       execution=meta.get('execution'))
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
    execution=None,
):
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    task_id = task_id or uuid.uuid4().hex[:12]
    directory = task_dir(task_id)
    directory.mkdir(mode=0o700, exist_ok=True)
    prepared = read_json(directory / 'meta.json', {})

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

    if prepared.get('native_codex'):
        agent_command = ''
        actual_command = []  # Native supervisor consumes the hash-bound structured spec.
    else:
        agent_command = build_agent_command(
            machine,
            agent,
            workspace,
            prompt,
            model=model,
            effort=effort,
            role=role,
            execution=execution,
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
        **({'native_codex': prepared['native_codex']} if prepared.get('native_codex') else {}),
        **({'admission': prepared['admission']} if prepared.get('admission') else {}),
        **({key: prepared[key] for key in ('review_binding', 'launch_request_id')}
           if prepared.get('review_binding') is not None else {}),
        **({key: prepared[key] for key in ('repository_binding', 'launch_request_id')}
           if prepared.get('repository_binding') is not None else {}),
    }

    if execution:
        meta['execution'] = execution
        model = execution['model']
        effort = execution['options'].get('effort')
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
        import bridge_telemetry
        bridge_file = DATA_DIR.parent / 'task_bridge_state.json'
        bridge = read_json(bridge_file, {'issues': {}})
        item = bridge.get('issues', {}).get(str(task_id), {})
        # Never probe SSH, write observations or clean processes.
        runtime = readonly_runtime(item['task_id']) if item.get('task_id') else None
        view = status_view(bridge, task_id, runtime, bridge_telemetry.load(bridge_file))
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
            if view.get("execution"):
                print("Execution: " + json.dumps(view["execution"], sort_keys=True))
            print(f"Runtime: coding {health.get('state')} ({health.get('reason')}) | review {view['review_runtime'].get('state')} | cleanup {health.get('cleanup_state')} | queue {json.dumps(view['queue'])} | locks {len(view['locks'])}")
            print(f"Publication: {view['publication']} | next action: {view['next_action']} | projection: {view['projection'].get('health')}")
            timings = {row['stage']: row['seconds'] for row in view['timings'].get('samples', [])}
            if timings:
                print('Timings (seconds): ' + json.dumps(timings, sort_keys=True))
            if view.get('convergence'):
                print('Post-merge convergence: ' + json.dumps(view['convergence'], sort_keys=True))
            diagnostics = [view['diagnostics'], view['scheduler_diagnostics'], view['worker_diagnostics'], health.get('diagnostic')]
            if any(diagnostics):
                print('Diagnostics: ' + json.dumps([v for v in diagnostics if v], ensure_ascii=False))
        return 0
    if as_json:
        return task_report(task_id, as_json=True)
    directory = task_dir(task_id)
    meta = load_meta(task_id)

    exit_path = directory / "exit_code"

    state, exit_code = task_state(meta, directory)

    print(f"id:        {task_id}")
    print(f"status:    {state}")
    print(f"machine:   {meta['machine']}")
    print(f"agent:     {meta['agent']}")
    print(f"execution: {json.dumps(meta.get('execution'), sort_keys=True)}")
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
    from external_effects import refuse
    refuse('worker')
    with exclusive(DATA_DIR.parent / "worker.lock", blocking=True):
        meta = load_meta(task_id)
        if meta.get('native_codex'):
            raise execution_profiles.PreflightRejected('Native Codex continuation requires a fresh authorized request; automatic session resume disabled')
        project = project_for_task(meta)
        if project == "-":
            raise ValueError("Task workspace is no longer registered")
        import scheduler
        _, canonical = scheduler.project_identity(project)
        signature = hashlib.sha256(json.dumps([task_id, prompt]).encode()).hexdigest()
        execution = None
        def prepare():
            nonlocal execution
            try:
                execution = execution_profiles.resume_identity(meta, capture_execution)
            except ValueError as exc:
                raise execution_profiles.PreflightRejected(str(exc)) from exc
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
                                       lambda: _continue_task(task_id, prompt, execution), prepare, signature)
        meta = load_meta(task_id)
        if meta.get("resume_prepared") and meta.get("prepared_request") == request_id:
            _continue_task(task_id, prompt)
            meta = load_meta(task_id)
        if meta.get("remote_key"):
            remote_worker.start(meta["machine"], meta["remote_key"], meta["remote_command"], meta)
        return result



def _continue_task(task_id, prompt, execution=None):
    directory = task_dir(task_id)
    meta = load_meta(task_id)

    exit_path = directory / "exit_code"

    if not meta.get("remote_key") and not exit_path.exists() and worker_runtime.alive(meta):
        raise SystemExit(f"Task {task_id} is still running")

    agent = meta["agent"]
    machine = meta["machine"]
    workspace = meta["workspace"]
    if execution is None:
        execution = execution_profiles.resume_identity(meta, capture_execution)

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

        if execution:
            args += execution_profiles.cli_options(execution)
        else:
            if meta.get('model'):
                args += ['-m', meta['model']]
            if meta.get('reasoning_effort'):
                args += ['-c', 'model_reasoning_effort=' + json.dumps(meta['reasoning_effort'])]

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
        if execution:
            args[-1:-1] = execution_profiles.cli_options(execution)
        elif meta.get('model') or meta.get('reasoning_effort'):
            raise ValueError('Legacy Cursor task with options has no validated execution identity; adopt a new task')

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
    from external_effects import blocked
    quiet = blocked() or not __import__('admission_policy').task_allowed(task_id)
    if not quiet and __import__('admission_policy').operation_allowed('reconcile'):
        reconcile_workers()
    directory = task_dir(task_id)
    meta = load_meta(task_id)

    state, exit_code = stored_task_status(meta, directory) if quiet else task_state(meta, directory)
    if quiet and meta.get('native_codex') and read_json(directory/'native-watchdog/bounded-result.json',{}).get('state')=='UNKNOWN':
        state,exit_code='UNKNOWN',None
    if not quiet and meta.get('native_codex') and state in ('DONE','FAILED'):
        # Finalize only this admitted generation. No bulk scan, signals, remote
        # operations, or legacy metadata/lease changes are allowed here.
        from native_supervisor import terminal_cleanup
        try:
            with __import__('admission_policy').finite_lock(DATA_DIR.parent/'worker.lock',blocking=False):
                current=load_meta(task_id)
                if (current.get('runtime_generation')==meta.get('runtime_generation') and
                        __import__('admission_policy').task_allowed(task_id) and
                        terminal_cleanup(directory,current,state)=='ABSENT'):
                    worker_runtime.reconcile_task(directory,current,state,exit_code)
                    worker_runtime.reconcile_leases(DATA_DIR,{task_id:dict(machine=current['machine'],cleanup='ABSENT')})
                    meta=current
        except __import__('admission_policy').AdmissionDenied:
            pass  # A reader nested inside a writer lock never waits or grants authority.
    if meta.get('remote_key') and not quiet:
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

    if session_id and meta.get('runtime_generation') and not quiet:
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
        "observation_confidence": 'READ_ONLY' if quiet else 'RUNTIME',
        "role": meta.get('role', 'coding'),
        "runtime_generation": meta.get('runtime_generation'),
        "launch_request_id": meta.get('launch_request_id'),
        "review_binding": meta.get('review_binding'),
        "git_baseline": redact_data(meta.get("git_baseline")),
        "adopted": bool(meta.get("adopted")),
        "created_at": meta.get("created_at"),
        "updated_at": meta.get("updated_at"),
        "status": state,
        "machine": meta["machine"],
        "agent": meta["agent"],
        "workspace": meta["workspace"],
        "session_id": session_id,
        "model": (meta.get("execution") or {}).get("model", model),
        "observed_model": model,
        "reasoning_effort": reasoning_effort,
        "execution": meta.get("execution"),
        "execution_options": (meta.get("execution") or {}).get("options", {}),
        "turns": meta.get("turns", 1),
        "exit_code": exit_code,
        "result": redact(result) if result else result,
        "error": redact(last_error) if last_error else last_error,
        "liveness": health,
        "cleanup_state": meta.get('cleanup_state'),
        "cleanup_generation": (meta.get('remote_cleanup_generation') if meta.get('remote_key') else
                               meta.get('cleanup_generation')),
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

    if report.get("execution"):
        print("execution: " + json.dumps(report["execution"], sort_keys=True))
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
    __import__('admission_policy').require_operation('shell')
    from external_effects import refuse
    refuse('worker')
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

    p_preflight = sub.add_parser('preflight-task', help='Read-only execution validation before admission')
    p_preflight.add_argument('operation', choices=['start_task', 'continue_task'])
    p_preflight.add_argument('arguments', type=json.loads)

    p_request = sub.add_parser('inspect-start-request', help='Reconcile or fence a deterministic start request')
    p_request.add_argument('request_id')
    p_request.add_argument('--fence', action='store_true')

    p_models = sub.add_parser('execution-catalog', help='Validate and display model/profile configuration')
    p_models.add_argument('--project')
    p_models.add_argument('--discover', action='store_true', help='Read installed runtime version/models on the project host')
    p_models.add_argument('--agent', choices=['cursor', 'codex'])

    p_tasks = sub.add_parser("tasks")
    p_tasks.add_argument("--json", action="store_true")
    p_tasks.add_argument("--all", dest="show_all", action="store_true")
    p_tasks.add_argument("--control", action="store_true", help="Read worker ownership without recovery sweeps")
    p_tasks.add_argument("--control-project", help="Target registered repository for control safety checks")
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
        choices=["codex", "cursor", "auto"],
        default=None,
    )
    p_run_project.add_argument("--model", default=None)
    p_run_project.add_argument("--effort", default=None)
    p_run_project.add_argument("--execution-profile", default=None)
    p_run_project.add_argument("--execution-options", type=json.loads, default=None)
    p_run_project.add_argument("--request-id", default=None)
    p_run_project.add_argument("--role", choices=["coding", "review"], default="coding")
    p_run_project.add_argument('--review-binding', type=json.loads, default=None)
    p_run_project.add_argument('--repository-binding', type=json.loads, default=None)

    p_exec = sub.add_parser("exec")
    p_exec.add_argument("machine", choices=MACHINES.keys())
    p_exec.add_argument("command")

    p_run = sub.add_parser("run")
    p_run.add_argument("machine", choices=MACHINES.keys())
    p_run.add_argument("agent", choices=["codex", "cursor", "auto"])
    p_run.add_argument("workspace")
    p_run.add_argument("prompt")
    p_run.add_argument("--model", default=None)
    p_run.add_argument("--effort", default=None)
    p_run.add_argument("--execution-profile", default=None)
    p_run.add_argument("--execution-options", type=json.loads, default=None)

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

    if args.action == 'preflight-task':
        print(json.dumps(preflight_task(args.operation, args.arguments)))
        raise SystemExit(0)

    if args.action == 'inspect-start-request':
        print(json.dumps(worker_runtime.inspect_start_request(DATA_DIR, args.request_id, fence=args.fence)))
        raise SystemExit(0)

    if args.action == 'execution-catalog':
        raise SystemExit(execution_catalog(args.project, args.discover, args.agent))

    if args.action == "tasks":
        raise SystemExit(
            list_tasks(args.json, args.show_all, args.cached, **(
                {"control": True, "control_project": args.control_project} if args.control else {}))
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
                **({'execution_profile': args.execution_profile, 'execution_options': args.execution_options}
                   if args.execution_profile is not None or args.execution_options is not None else {}),
                **({'review_binding': args.review_binding} if args.review_binding is not None else {}),
                **({'repository_binding': args.repository_binding} if args.repository_binding is not None else {}),
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
                execution_profile=args.execution_profile,
                execution_options=args.execution_options,
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
    try:
        main()
    except execution_profiles.PreflightRejected as exc:
        # Only failures before preparation/receipt creation use this contract.
        print(json.dumps(execution_profiles.rejection(exc)))
        raise SystemExit(2)
    except execution_profiles.StaleReviewInput:
        print(json.dumps({'status': 'STALE_INPUT', 'launch_started': False}))
        raise SystemExit(3)
    except worker_runtime.RequestFenced:
        print(json.dumps({'status': 'FENCED', 'launch_started': False}))
        raise SystemExit(3)

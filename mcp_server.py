#!/usr/bin/env python3

import json
import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial, wraps
import subprocess
from pathlib import Path
from typing import Literal
from health import MCP_HEALTH_TIMEOUT, failed_health, validate_health
from redaction import redact
from execution_profiles import PreflightRejected, StaleReviewInput, check_preflight, rejection
from worker_runtime import RequestFenced

from mcp.server import MCPServer


ROOT = Path(__file__).resolve().parent
DEVCTL = ROOT / "devctl.py"
PROJECTS_FILE = ROOT / "projects.json"

mcp = MCPServer("Siyavush DevOrchestrator")

# Register explicitly async handlers. A synchronous SDK invocation of devctl
# must not monopolize the MCP event loop during runtime discovery or cleanup.
# Worker admission remains serialized by worker.lock, independent of these pools.
TOOL_POOLS = {name: ThreadPoolExecutor(max_workers=2, thread_name_prefix='mcp-' + name)
              for name in ('control', 'work')}


def io_tool(function):
    lane = 'work' if function.__name__ in ('start_task', 'continue_task', 'adopt_task', 'preflight_task') else 'control'
    @wraps(function)
    async def handler(*args, **kwargs):
        from external_effects import blocked
        latched = blocked()
        result = await asyncio.get_running_loop().run_in_executor(TOOL_POOLS[lane], partial(function, *args, **kwargs))
        if function.__name__ in ('get_task_report', 'list_tasks', 'get_task_observation', 'list_task_observations') and (latched or blocked() or kwargs.get('read_only')):
            # Retain dispatch provenance across environment changes in flight.
            if isinstance(result, list):
                result = [dict(row, observation_confidence='READ_ONLY') for row in result]
            else:
                result = dict(result, observation_confidence='READ_ONLY')
        return result
    mcp.tool()(handler)
    return function  # Preserve the direct, synchronous CLI-adapter test contract.


def load_projects() -> dict:
    if os.environ.get('DEVORCHESTRATOR_ADMISSION_POLICY'):
        import devctl as worker
        return worker.load_projects()
    return json.loads(PROJECTS_FILE.read_text())


def devctl_result(*args: str, timeout: int = 30, read_only: bool = False):
    from external_effects import ENV
    return subprocess.run(
        [str(DEVCTL), *args],
        cwd=ROOT,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
        **({'env': dict(os.environ, **{ENV: '1'})} if read_only else {}),
    )


def devctl(*args: str, timeout: int = 30, read_only: bool = False) -> str:
    result = devctl_result(*args, timeout=timeout, **({'read_only': True} if read_only else {}))
    output = result.stdout.strip()

    if result.returncode in (2, 3):
        try:
            payload = json.loads(output)
        except ValueError:
            payload = None
        if isinstance(payload, dict) and payload.get('status') in ('REJECTED', 'STALE_INPUT', 'FENCED'):
            check_preflight(payload)
    if result.returncode != 0:
        raise RuntimeError(
            f"devctl failed with exit code {result.returncode}: {output}"
        )

    return output


@io_tool
def list_projects() -> dict:
    """List projects available to DevOrchestrator."""
    return load_projects()


@io_tool
def get_execution_catalog(project: str | None = None, discover: bool = False,
                          agent: Literal['cursor', 'codex'] | None = None) -> dict:
    """Validate/catalogue configuration; optionally probe the installed project runtime without launching."""
    args = ['execution-catalog']
    if project is not None:
        args += ['--project', project]
    if discover:
        args += ['--discover']
    if agent is not None:
        args += ['--agent', agent]
    return json.loads(devctl(*args))


@io_tool
def preflight_task(operation: Literal['start_task', 'continue_task'], arguments: dict) -> dict:
    """Read-only role, profile and runtime validation; never starts a worker."""
    return json.loads(devctl('preflight-task', operation, json.dumps(arguments, sort_keys=True)))


@io_tool
def get_transition_capabilities() -> dict:
    """Static protocol support; absence never enables the new launch path."""
    return {'schema_version': 1, 'review_binding': 1, 'start_request_reconciliation': 1}


@io_tool
def inspect_start_request(request_id: str, fence: bool = False) -> dict:
    """Inspect a start receipt; optionally fence only a proven unaccepted request."""
    if fence:
        from external_effects import refuse
        refuse('worker')
    args = ['inspect-start-request', request_id]
    if fence:
        args.append('--fence')
    return json.loads(devctl(*args))


@io_tool
@__import__('admission_policy').endpoint
def start_task(
    project: str,
    prompt: str,
    agent: Literal["codex", "cursor", "auto"] | None = None,
    model: str | None = None,
    effort: str | None = None,
    request_id: str | None = None,
    role: Literal["coding", "review"] = "coding",
    execution_profile: str | None = None,
    execution_options: dict | None = None,
    review_binding: dict | None = None,
    repository_binding: dict | None = None,
) -> dict:
    """Start a task in a registered project.

    Uses the project's default agent unless agent is explicitly specified.
    """
    from external_effects import refuse
    refuse('worker')
    projects = load_projects()

    if project not in projects:
        raise ValueError(
            f"Unknown project: {project}. "
            f"Available: {', '.join(sorted(projects))}"
        )

    cfg = projects[project]
    selected_agent = agent or cfg.get("default_agent", "codex")

    args = [
        "run-project",
        project,
        prompt,
    ]

    if agent is not None:
        args += ["--agent", agent]

    if model is not None:
        args += ["--model", model]

    if effort is not None:
        args += ["--effort", effort]

    if execution_profile is not None:
        args += ['--execution-profile', execution_profile]
    if execution_options is not None:
        args += ['--execution-options', json.dumps(execution_options, sort_keys=True)]
    if request_id:
        args += ["--request-id", request_id]
    if review_binding is not None:
        args += ['--review-binding', json.dumps(review_binding, sort_keys=True)]
    if repository_binding is not None:
        args += ['--repository-binding', json.dumps(repository_binding, sort_keys=True)]
    args += ["--role", role]
    try:
        output = devctl(*args)
    except PreflightRejected as exc:
        return rejection(exc)
    except StaleReviewInput:
        return {'status': 'STALE_INPUT', 'launch_started': False}
    except RequestFenced:
        return {'status': 'FENCED', 'launch_started': False}
    task_id = output.splitlines()[-1].strip()

    report = json.loads(devctl("report", task_id, "--json"))
    return {**report, "task_id": task_id, "project": project,
            "machine": cfg["machine"], "agent": report.get("agent") or selected_agent, "role": role,
            "model": report.get("model") or model,
            "effort": report.get("reasoning_effort") or effort}



@io_tool
def get_task_report(task_id: str, read_only: bool = False) -> dict:
    read_only = read_only or not __import__('admission_policy').task_allowed(task_id)
    """Return task status and latest concise result."""
    return json.loads(
        devctl("report", task_id, "--json", **({'read_only': True} if read_only else {}))
    )


@io_tool
def get_task_observation(task_id: str) -> dict:
    """Always read-only, including when the MCP server itself is unlatched."""
    return get_task_report(task_id, read_only=True)


@io_tool
def continue_task(task_id: str, prompt: str, request_id: str | None = None,
                  role: Literal["coding", "review"] = "coding") -> dict:
    """Continue the same Codex or Cursor session."""
    from external_effects import refuse
    refuse('worker')
    args = ["continue", task_id, prompt]
    if request_id:
        args += ["--request-id", request_id]
    try:
        devctl(*args)
    except PreflightRejected as exc:
        return rejection(exc)

    return json.loads(
        devctl("report", task_id, "--json")
    )


@io_tool
def list_tasks(control: bool = False, project: str | None = None, read_only: bool = False) -> list:
    """Control reads target worker ownership without running recovery sweeps."""
    args = ["tasks", "--json", "--cached"]
    if control:
        args += ["--control"]
        if project:
            args += ["--control-project", project]
    return json.loads(devctl(*args, **({'read_only': True} if read_only else {})))


@io_tool
def list_task_observations(control: bool = False, project: str | None = None) -> list:
    """Always read-only; an older server cannot silently drop a mode argument."""
    return list_tasks(control, project, read_only=True)


@io_tool
def adopt_task(
    project: str,
    session_id: str,
    task_id: str | None = None,
    agent: Literal["codex", "cursor"] | None = None,
    model: str | None = None,
    effort: str | None = None,
) -> dict:
    """Register an existing agent session as a DevOrchestrator task."""
    from external_effects import refuse
    refuse('worker')
    args = [
        "adopt",
        project,
        session_id,
    ]

    if task_id:
        args += ["--task-id", task_id]

    if agent:
        args += ["--agent", agent]

    if model:
        args += ["--model", model]

    if effort:
        args += ["--effort", effort]

    new_task_id = devctl(*args).splitlines()[-1].strip()

    return json.loads(
        devctl("report", new_task_id, "--json")
    )

@io_tool
def orchestrator_health() -> dict:
    """Report required WORK health and optional worker availability independently.

    READY and DEGRADED have ok=true; FAILED has ok=false and blocks rollout.
    MCP/project identity and task-list transport still require separate checks.
    """
    from external_effects import refuse
    refuse('worker')
    try:
        result = devctl_result('doctor', '--json', timeout=MCP_HEALTH_TIMEOUT)
        report = validate_health(json.loads(result.stdout))
        expected_exit = 0 if report['ok'] else 1
        if result.returncode != expected_exit:
            return failed_health(f'Doctor exit {result.returncode} disagrees with health state')
        return report
    except subprocess.TimeoutExpired:
        return failed_health(f'Local doctor timed out after {MCP_HEALTH_TIMEOUT}s')
    except OSError as exc:
        return failed_health(f'Local doctor could not start: {redact(exc)}')
    except (ValueError, TypeError) as exc:
        return failed_health(f'Invalid local doctor result: {exc}')


if __name__ == "__main__":
    mcp.run(
        transport="streamable-http",
        host="127.0.0.1",
        port=8787,
        stateless_http=True,
        json_response=True,
    )

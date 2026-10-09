#!/usr/bin/env python3

import json
import subprocess
from pathlib import Path
from typing import Literal
from health import MCP_HEALTH_TIMEOUT, failed_health, validate_health
from redaction import redact

from mcp.server import MCPServer


ROOT = Path(__file__).resolve().parent
DEVCTL = ROOT / "devctl.py"
PROJECTS_FILE = ROOT / "projects.json"

mcp = MCPServer("Siyavush DevOrchestrator")


def load_projects() -> dict:
    return json.loads(PROJECTS_FILE.read_text())


def devctl_result(*args: str, timeout: int = 30):
    return subprocess.run(
        [str(DEVCTL), *args],
        cwd=ROOT,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
    )


def devctl(*args: str, timeout: int = 30) -> str:
    result = devctl_result(*args, timeout=timeout)
    output = result.stdout.strip()

    if result.returncode != 0:
        raise RuntimeError(
            f"devctl failed with exit code {result.returncode}: {output}"
        )

    return output


@mcp.tool()
def list_projects() -> dict:
    """List projects available to DevOrchestrator."""
    return load_projects()


@mcp.tool()
def start_task(
    project: str,
    prompt: str,
    agent: Literal["codex", "cursor"] | None = None,
    model: str | None = None,
    effort: str | None = None,
    request_id: str | None = None,
    role: Literal["coding", "review"] = "coding",
) -> dict:
    """Start a task in a registered project.

    Uses the project's default agent unless agent is explicitly specified.
    """
    projects = load_projects()

    if project not in projects:
        raise ValueError(
            f"Unknown project: {project}. "
            f"Available: {', '.join(sorted(projects))}"
        )

    cfg = projects[project]
    selected_agent = agent or cfg.get("default_agent", "codex")

    if selected_agent != "codex" and (model or effort):
        raise ValueError(
            "model/effort are supported only for Codex tasks"
        )

    args = [
        "run-project",
        project,
        prompt,
    ]

    if agent:
        args += ["--agent", agent]

    if model:
        args += ["--model", model]

    if effort:
        args += ["--effort", effort]

    if request_id:
        args += ["--request-id", request_id]
    args += ["--role", role]
    output = devctl(*args)
    task_id = output.splitlines()[-1].strip()

    report = json.loads(devctl("report", task_id, "--json"))
    return {**report, "task_id": task_id, "project": project,
            "machine": cfg["machine"], "agent": selected_agent, "role": role,
            "model": report.get("model") or model,
            "effort": report.get("reasoning_effort") or effort}



@mcp.tool()
def get_task_report(task_id: str) -> dict:
    """Return task status and latest concise result."""
    return json.loads(
        devctl("report", task_id, "--json")
    )


@mcp.tool()
def continue_task(task_id: str, prompt: str, request_id: str | None = None,
                  role: Literal["coding", "review"] = "coding") -> dict:
    """Continue the same Codex or Cursor session."""
    args = ["continue", task_id, prompt]
    if request_id:
        args += ["--request-id", request_id]
    devctl(*args)

    return json.loads(
        devctl("report", task_id, "--json")
    )


@mcp.tool()
def list_tasks(control: bool = False, project: str | None = None) -> list:
    """Control reads target worker ownership without running recovery sweeps."""
    args = ["tasks", "--json", "--cached"]
    if control:
        args += ["--control"]
        if project:
            args += ["--control-project", project]
    return json.loads(devctl(*args))


@mcp.tool()
def adopt_task(
    project: str,
    session_id: str,
    task_id: str | None = None,
    agent: Literal["codex", "cursor"] | None = None,
    model: str | None = None,
    effort: str | None = None,
) -> dict:
    """Register an existing agent session as a DevOrchestrator task."""
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

@mcp.tool()
def orchestrator_health() -> dict:
    """Report required WORK health and optional worker availability independently.

    READY and DEGRADED have ok=true; FAILED has ok=false and blocks rollout.
    MCP/project identity and task-list transport still require separate checks.
    """
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

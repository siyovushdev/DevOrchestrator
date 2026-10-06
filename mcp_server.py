#!/usr/bin/env python3

import json
import subprocess
from pathlib import Path
from typing import Literal

from mcp.server import MCPServer


ROOT = Path(__file__).resolve().parent
DEVCTL = ROOT / "devctl.py"
PROJECTS_FILE = ROOT / "projects.json"

mcp = MCPServer("Siyavush DevOrchestrator")


def load_projects() -> dict:
    return json.loads(PROJECTS_FILE.read_text())


def devctl(*args: str, timeout: int = 30) -> str:
    result = subprocess.run(
        [str(DEVCTL), *args],
        cwd=ROOT,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
    )

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

    output = devctl(*args)
    task_id = output.splitlines()[-1].strip()

    return {
        "task_id": task_id,
        "project": project,
        "machine": cfg["machine"],
        "agent": selected_agent,
        "model": model,
        "effort": effort,
        "status": "RUNNING",
    }


@mcp.tool()
def get_task_report(task_id: str) -> dict:
    """Return task status and latest concise result."""
    return json.loads(
        devctl("report", task_id, "--json")
    )


@mcp.tool()
def continue_task(task_id: str, prompt: str) -> dict:
    """Continue the same Codex or Cursor session."""
    devctl("continue", task_id, prompt)

    return json.loads(
        devctl("report", task_id, "--json")
    )


@mcp.tool()
def list_tasks() -> list:
    """List known DevOrchestrator tasks and their current status."""
    return json.loads(
        devctl("tasks", "--json")
    )


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
    """Check connectivity and available agents."""
    return {
        "ok": True,
        "details": devctl("doctor"),
    }


if __name__ == "__main__":
    mcp.run(
        transport="streamable-http",
        host="127.0.0.1",
        port=8787,
        stateless_http=True,
        json_response=True,
    )

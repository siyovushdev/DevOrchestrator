"""Fail-closed latch for a controlled bridge smoke.

Clearing this latch permits only separately admitted execution; publication
remains denied. An unset admission policy grants no new execution.
Durable observation holds and quarantined effects still require explicit
authorization recovery: removing this environment flag never clears them.
When set, worker launches, publication subprocesses, Telegram delivery and
GitHub mutations raise before the external call. GitHub reads are an exact
allowlist of the poll, comment, label and issue-view commands the bridge
actually issues. Worker reports and the other allowed MCP reads stay
observational: they do not signal process groups, start processes, SSH, or
write worker receipts, metadata or leases. A quiet log is not the control;
the process environment and these raises are.
"""
import os
import re


ENV = 'DEVORCHESTRATOR_BLOCK_EXTERNAL_EFFECTS'
WORKER_TOOLS = frozenset({'start_task', 'continue_task', 'adopt_task'})
_REPO = r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+'
_ISSUE = r'[1-9][0-9]*'
_OPEN_ISSUES = re.compile(
    r'repos/' + _REPO + r'/issues\?state=open&sort=created&direction=asc&per_page=100')
_COMMENTS = re.compile(r'repos/' + _REPO + r'/issues/' + _ISSUE + r'/comments\?per_page=100')
_LABELS = re.compile(r'repos/' + _REPO + r'/labels\?per_page=100')
_ISSUE_JSON = frozenset({
    'number,title,body,labels,url,state',
    'number,labels',
})


class ExternalEffectBlocked(RuntimeError):
    def __init__(self, kind):
        self.kind = kind
        super().__init__(f'External {kind} effect blocked')


def blocked():
    return os.environ.get(ENV) == '1'


def read_only_observation(value):
    """Copied RPC results retain provenance through nested review/discovery IO."""
    if isinstance(value, dict):
        return value.get('observation_confidence') == 'READ_ONLY' or any(
            read_only_observation(v) for v in value.values() if isinstance(v, (dict, list)))
    if isinstance(value, list):
        return any(read_only_observation(v) for v in value)
    return False


def refuse(kind):
    if blocked():
        raise ExternalEffectBlocked(kind)
    if kind == 'worker':
        from admission_policy import require_worker
        require_worker()
    elif kind in ('publication', 'github', 'telegram'):
        from admission_policy import external_denied
        external_denied()


def github_is_read(args):
    """True only for the exact read argv the task transport issues.

    ``gh api`` is not a read. Method flags, field flags and any extra argument
    fail closed, including when they are attached to an otherwise known URL.
    """
    args = tuple(args)
    if len(args) == 3 and args[0] == 'api' and args[2] == '--paginate':
        endpoint = args[1]
        return bool(_OPEN_ISSUES.fullmatch(endpoint) or _COMMENTS.fullmatch(endpoint) or
                    _LABELS.fullmatch(endpoint))
    if (len(args) == 7 and args[0] == 'issue' and args[1] == 'view' and args[3] == '-R' and
            args[5] == '--json' and args[6] in _ISSUE_JSON and
            re.fullmatch(_ISSUE, args[2]) and re.fullmatch(_REPO, args[4])):
        return True
    return False


def guard_github(args):
    if blocked() and not github_is_read(tuple(args)):
        raise ExternalEffectBlocked('github')
    if not github_is_read(tuple(args)):
        from admission_policy import external_denied
        external_denied()


def guard_worker_rpc(name, arguments):
    if not blocked():
        return
    if not isinstance(arguments, dict):
        raise ExternalEffectBlocked('worker')
    if name in ('list_projects', 'get_transition_capabilities', 'get_task_report', 'list_tasks',
                'get_task_observation', 'list_task_observations'):
        return
    if name == 'inspect_start_request' and not arguments.get('fence'):
        return
    if name == 'get_execution_catalog' and not arguments.get('discover'):
        return
    # Doctor, live preflight/discovery, unknown tools and all mutations fail
    # closed before contacting even a separately running, unlatched server.
    raise ExternalEffectBlocked('worker')

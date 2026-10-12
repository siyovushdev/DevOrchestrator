"""Durable FIFO admission, explicit capacities and repository ownership."""
from dataclasses import dataclass, field
import hashlib
import json
import os

import devctl


class QueueWait(Exception):
    """Normal admission deferral, not a task failure."""


def registered_project(task, preferred=None):
    """Resolve local task metadata only; never execute or normalize a reported path."""
    registry = devctl.load_projects()
    matches = sorted(name for name, cfg in registry.items()
                     if cfg.get('machine') in devctl.MACHINES and
                     cfg.get('machine') == task.get('machine') and
                     cfg.get('workspace') == task.get('workspace'))
    if preferred:
        if preferred in matches:
            return preferred
        raise ValueError('Task identity does not match its registered project')
    if task.get('project') in matches:
        return task['project']
    if len(matches) == 1:
        return matches[0]
    raise ValueError('Task identity has no unique registered project')


@dataclass
class Limits:
    coding: int = 1
    review: int = 1
    total: int = 2
    machines: dict = field(default_factory=lambda: {'work': 1, 'firefly': 1})

    @classmethod
    def from_env(cls):
        limits = cls(coding=int(os.environ.get('DEVORCHESTRATOR_MAX_CODING', '1')),
                     review=int(os.environ.get('DEVORCHESTRATOR_MAX_REVIEW', '1')),
                     total=int(os.environ.get('DEVORCHESTRATOR_MAX_AGENTS', '2')),
                     machines=json.loads(os.environ.get('DEVORCHESTRATOR_MACHINE_LIMITS', '{"work":1,"firefly":1}')))
        if min(limits.coding, limits.review, limits.total) < 1 or any(
                not isinstance(v, int) or v < 1 for v in limits.machines.values()):
            raise ValueError('Concurrency limits must be positive integers')
        return limits


def project_identity(project):
    cfg = devctl.load_projects().get(project)
    if not cfg or cfg.get('machine') not in devctl.MACHINES:
        raise ValueError('Task requires a registered project on a known machine')
    workspace = cfg['workspace']
    # Resolve aliases on the actual worker, never a cloud supplied path.
    import shlex
    from repository_safety import GIT_PREAMBLE, bounded_script
    script=GIT_PREAMBLE + '\nimport sys; p=os.path.realpath(sys.argv[1]); assert_git_config(p); r=safe_git(p,["rev-parse","--show-toplevel"],optional=True).decode().strip(); print(os.path.realpath(r) if r else p)'
    result = devctl.capture(cfg['machine'], shlex.join([
        '/usr/bin/python3', '-I', '-B', '-c', bounded_script(script), workspace]), separate_stderr=True)
    if result.returncode:
        raise RuntimeError('Cannot resolve registered workspace identity')
    canonical = result.stdout.strip()
    return cfg['machine'], hashlib.sha256((cfg['machine'] + '\0' + canonical).encode()).hexdigest()


class Scheduler:
    def __init__(self, state, limits=None):
        self.state = state
        self.limits = limits or Limits.from_env()
        self.jobs = state.setdefault('work_queue', {})
        self.locks = state.setdefault('repo_locks', {})

    def enqueue(self, key, owner, kind, machine, repo):
        if key not in self.jobs:
            sequence = self.state.get('queue_sequence', 0) + 1
            self.state['queue_sequence'] = sequence
            self.jobs[key] = dict(owner=owner, kind=kind, machine=machine, repo=repo,
                                  sequence=sequence, status='QUEUED', task_id=None)
        return self.jobs[key]

    def repository_busy(self, owner, machine, repo):
        # An unresolved workspace may alias any repository on its registered machine.
        # If even that machine is unknown, hold all new admissions until reconciliation.
        from operational_state import post_merge_repository_hold, publication_repository_hold
        from review_transitions import holds_repository
        for number, item in self.state.get('issues', {}).items():
            if str(number) == str(owner) or not (holds_repository(item) or publication_repository_hold(item)):
                continue
            owned = [job for job in self.jobs.values() if str(job.get('owner')) == str(number)]
            if not owned or any(not job.get('repo') for job in owned):
                return True  # Missing ownership evidence cannot imply a free repository.
        return (self.locks.get(repo, {}).get('owner', owner) != owner or
                any(j.get('repo') == repo and j['owner'] != owner and
                    (post_merge_repository_hold(self.state.get('issues', {}).get(str(j['owner']), {})) or
                     publication_repository_hold(self.state.get('issues', {}).get(str(j['owner']), {})) or
                     holds_repository(self.state.get('issues', {}).get(str(j['owner']), {})))
                    for j in self.jobs.values()) or
                any(j['status'] in ('RESERVED', 'RUNNING', 'UNKNOWN') and
                    ((j.get('identity_pending') and j.get('machine') in (None, machine)) or
                     (j['repo'] == repo and j['owner'] != owner)) for j in self.jobs.values()))

    def hold_publication_repository(self, owner, machine, repo):
        """Reacquire ownership before MERGE; this receipt is not a worker slot."""
        if self.repository_busy(owner, machine, repo):
            raise QueueWait()
        key = 'publication:' + str(owner)
        job = self.enqueue(key, owner, 'publication', machine, repo)
        if (job['owner'], job['machine'], job['repo']) != (owner, machine, repo):
            raise ValueError('Publication repository identity changed')
        job['status'] = 'DONE'
        self.locks[repo] = {'owner': owner, 'key': key}

    def available(self, job):
        from operational_state import post_merge_repository_hold, publication_repository_hold
        if (post_merge_repository_hold(self.state.get('issues', {}).get(str(job['owner']), {})) or
                publication_repository_hold(self.state.get('issues', {}).get(str(job['owner']), {}))):
            return False
        active = [j for j in self.jobs.values() if j['status'] in ('RESERVED', 'RUNNING', 'UNKNOWN')]
        limit = self.limits.coding if job['kind'] == 'coding' else self.limits.review
        return (not self.repository_busy(job['owner'], job['machine'], job['repo']) and
                len(active) < self.limits.total and
                sum(j['kind'] == job['kind'] for j in active) < limit and
                sum(j['machine'] == job['machine'] for j in active) < self.limits.machines.get(job['machine'], 1))

    def reserve(self, key):
        job = self.jobs[key]
        if job['status'] != 'QUEUED':
            return job
        # Oldest eligible request wins; blocked repositories do not stall unrelated machines.
        eligible = sorted((j for j in self.jobs.values() if j['status'] == 'QUEUED' and self.available(j)),
                          key=lambda j: j['sequence'])
        if not eligible or eligible[0] is not job:
            raise QueueWait()
        job['status'] = 'RESERVED'
        self.locks[job['repo']] = {'owner': job['owner'], 'key': key}
        return job

    def started(self, key, task_id, status="RUNNING"):
        self.jobs[key].update(status=status if status in ("RUNNING", "DONE", "FAILED") else "UNKNOWN", task_id=task_id, launch_pending=False)

    def observe(self, task_id, status, cleanup_state=None):
        matching = [j for j in self.jobs.values() if j.get('task_id') == task_id and j['status'] != 'QUEUED']
        if matching:
            job = max(matching, key=lambda j: j['sequence'])
            if cleanup_state == 'AMBIGUOUS':
                job['runtime_cleanup_pending'] = True
            elif cleanup_state in ('ABSENT', 'CLEANED'):
                for previous in matching:
                    previous.pop('runtime_cleanup_pending', None)
            if job.get('launch_pending') and status in ('DONE', 'FAILED'):
                return
            job['status'] = status if status in ('DONE', 'FAILED', 'RUNNING') else 'UNKNOWN'
            if job.get('identity_pending') and job['status'] == 'RUNNING':
                job['status'] = 'UNKNOWN'
            if status in ('DONE', 'FAILED'):
                for previous in matching:
                    if not previous.get('launch_pending'):
                        previous['status'] = status
            if job.get('runtime_cleanup_pending'):
                job['status'] = 'UNKNOWN'

    def release(self, owner):
        from operational_state import chief_repository_hold, post_merge_repository_hold, publication_repository_hold
        item = self.state.get('issues', {}).get(str(owner))
        if isinstance(item, dict) and item.get('safety_latch_hold'):
            return False
        from review_transitions import holds_repository
        if item is not None and holds_repository(item):
            return False
        if item is not None and (post_merge_repository_hold(item) or chief_repository_hold(item) or publication_repository_hold(item)):
            # Both recovery and process_tracked's finally block use this path.
            # Review/merge milestones alone cannot release an unproven workspace.
            return False
        if any(j['owner'] == owner and (j['status'] in ('RESERVED', 'RUNNING', 'UNKNOWN') or
                                       j.get('runtime_cleanup_pending')) for j in self.jobs.values()):
            return False
        for repo, lock in list(self.locks.items()):
            if lock['owner'] == owner:
                del self.locks[repo]
        return True

    def recover(self, tasks, identity_resolver=None):
        """Import active legacy/direct tasks; never steal an uncertain lock."""
        from external_effects import blocked
        if blocked():
            return  # Observations cannot settle or manufacture queue ownership.
        held_tasks = {task_id for item in self.state.get('issues', {}).values()
                      if isinstance(item, dict) and item.get('safety_latch_hold')
                      for task_id in (item.get('task_id'), item.get('review_task_id')) if task_id}
        tasks = [t for t in tasks if t.get('observation_confidence') != 'READ_ONLY' and t.get('id') not in held_tasks]
        identity_resolver = identity_resolver or project_identity
        for task in tasks:
            if not __import__('admission_policy').task_allowed(task['id']):
                continue
            project = task.get('project')
            matching = [j for j in self.jobs.values() if j.get('task_id') == task['id']]
            pending = next((j for j in matching if j.get('identity_pending')), None)
            if matching and not pending:
                self.observe(task['id'], task['status'], task.get('cleanup_state'))
            elif task['status'] in ('RUNNING', 'UNKNOWN', 'STARTING') or task.get('cleanup_state') == 'AMBIGUOUS':
                key = 'external:' + task['id']
                try:
                    if not project or project == '-':
                        project = registered_project(task)
                    machine, repo = identity_resolver(project)
                except Exception:
                    # Failure is isolated to this task, never discarded or treated as a free slot.
                    try:
                        machine = task.get('machine') or devctl.load_projects().get(project, {}).get('machine')
                        if machine not in devctl.MACHINES:
                            machine = None
                    except Exception:
                        machine = None
                    job = self.enqueue(key, key, task.get('role', 'coding'), machine, None)
                    job.update(machine=machine, status='UNKNOWN', task_id=task['id'], identity_pending=True, project=project)
                else:
                    job = self.enqueue(key, key, task.get('role', 'coding'), machine, repo)
                    job.update(machine=machine, repo=repo, identity_pending=False,
                               status='UNKNOWN' if task['status'] != 'RUNNING' else 'RUNNING', task_id=task['id'])
                    self.locks.setdefault(repo, {'owner': key, 'key': key})
        external = {t['id']: t for t in tasks}
        for key, job in self.jobs.items():
            import task_bridge
            if not __import__('admission_policy').issue_allowed(task_bridge.REPO,job['owner'],self.state.get('issues',{}).get(str(job['owner']))):
                continue
            if key.startswith('external:') and job.get('task_id') in external:
                self.observe(job['task_id'], external[job['task_id']]['status'], external[job['task_id']].get('cleanup_state'))
                if job['status'] in ('DONE', 'FAILED'):
                    self.release(job['owner'])
        diagnostics = self.state.setdefault('scheduler_diagnostics', {})
        from operational_state import post_merge_repository_hold, publication_repository_hold
        from review_transitions import holds_repository
        # Repair a previously released lock from durable queue ownership. The
        # admission predicate also honors this hold before recovery can run.
        for key, job in self.jobs.items():
            import task_bridge
            if not __import__('admission_policy').issue_allowed(task_bridge.REPO,job['owner'],self.state.get('issues',{}).get(str(job['owner']))):
                continue
            item = self.state.get('issues', {}).get(str(job['owner']), {})
            if job.get('repo') and (item.get('safety_latch_hold') or post_merge_repository_hold(item) or
                                    holds_repository(item) or publication_repository_hold(item)):
                self.locks.setdefault(job['repo'], {'owner': job['owner'], 'key': key})
        for repo, lock in list(self.locks.items()):
            import task_bridge
            if not __import__('admission_policy').issue_allowed(task_bridge.REPO,lock['owner'],self.state.get('issues',{}).get(str(lock['owner']))):
                continue
            jobs = [j for j in self.jobs.values() if j.get('repo') == repo and j.get('owner') == lock.get('owner')]
            if not jobs:
                diagnostics[repo] = 'Lock retained: missing queue ownership proof'
                continue
            if diagnostics.get(repo) == 'Lock retained: missing queue ownership proof':
                diagnostics.pop(repo)
            item = self.state.get('issues', {}).get(str(lock.get('owner')))
            if isinstance(item, dict) and item.get('safety_latch_hold'):
                continue
            from review_transitions import holds_repository
            if item is not None and holds_repository(item):
                continue
            if item is not None and publication_repository_hold(item):
                diagnostics[repo] = 'Lock retained: publication outcome is unproven'
                continue
            if diagnostics.get(repo) == 'Lock retained: publication outcome is unproven':
                diagnostics.pop(repo)
            if item is not None and post_merge_repository_hold(item):
                diagnostics[repo] = 'Lock retained: post-merge convergence is unproven'
                continue
            if diagnostics.get(repo) == 'Lock retained: post-merge convergence is unproven':
                diagnostics.pop(repo)
            from operational_state import exclusion_reason
            archived = item is not None and exclusion_reason(item)
            if item is not None and not archived:
                # A terminal worker turn is not a completed issue. Recovery runs
                # before phase processing, when review/correction intent may not
                # yet exist. Keep ownership until the bridge records its terminal
                # review/report outcome (or an existing publication milestone).
                finished = (item.get('review_status') in ('PASS', 'NEEDS_OWNER', 'FAILED', 'FRESH_REVIEW_REQUIRED') or
                            item.get('last_report_signature') or
                            item.get('compatibility_status') == 'QUARANTINED' or
                            item.get('owner_state') in ('READY_FOR_OWNER', 'COMMITTED', 'PUSHED', 'PR_CREATED', 'MERGED'))
                if not finished or item.get('review_status') == 'RUNNING' or item.get('pending_rpc'):
                    continue
            # An archival cancellation proves no launch only while its original
            # unreserved queue identity has no task or uncertain launch receipt.
            if (all((j['status'] in ('DONE', 'FAILED') or
                     (j['status'] == 'CANCELLED' and j.get('no_launch_proof') == 'FENCED') or
                     (archived and j['status'] == 'CANCELLED' and j.get('cancelled') == 'archived_issue' and
                      not j.get('task_id'))) and not j.get('launch_pending') for j in jobs) and
                    self.release(lock['owner'])):
                diagnostics.pop(repo, None)

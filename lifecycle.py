"""Conservative lifecycle policy: close merged issues; archive only known terminal tasks."""
import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import socket
import time

from durable import exclusive, read_json, write_json
import devctl
import worker_runtime

KNOWN_FILES = {'meta.json', 'events.jsonl', 'exit_code', 'prompt.txt', 'report.json', 'remote-refresh.lock',
               'liveness.json', 'liveness.lock', 'runtime_health.json', 'process_group.json', 'runtime_audit.json'}
WORK_QUEUE_LABEL = 'work:queued'


def worker_guard():
    # Explicit WORK enrollment and host binding prevent HOME's legacy bridge consuming this queue.
    if os.environ.get('DEVORCHESTRATOR_WORKER') != 'work':
        raise RuntimeError('Bridge is disabled unless DEVORCHESTRATOR_WORKER=work')
    host = os.environ.get('DEVORCHESTRATOR_WORK_HOST')
    if not host or socket.gethostname() != host:
        raise RuntimeError('Bridge requires DEVORCHESTRATOR_WORK_HOST matching this WORK host')
    if os.environ.get('DEVORCHESTRATOR_QUEUE_LABEL') != WORK_QUEUE_LABEL:
        raise RuntimeError(f'Bridge requires DEVORCHESTRATOR_QUEUE_LABEL={WORK_QUEUE_LABEL} for WORK')


def age_seconds(value, now):
    try:
        return now - datetime.fromisoformat(value).timestamp()
    except (TypeError, ValueError):
        return None


def plan(state, data_dir, registry, retention_days=30, issue_days=7, now=None):
    if retention_days < 1 or issue_days < 1:
        raise ValueError('Retention periods must be at least one day')
    now = time.time() if now is None else now
    actions = []
    protected = set()
    for number, item in state.get('issues', {}).items():
        complete = item.get('owner_state') == 'MERGED' or item.get('desired_label') == 'dev:done'
        if complete:
            age = age_seconds(item.get('completed_at'), now)
            if age is not None and age >= issue_days * 86400 and not item.get('issue_closed'):
                actions.append({'action': 'close_issue', 'number': number})
        else:
            protected.update(t for t in (item.get('task_id'), item.get('review_task_id')) if t)
    protected.update(j.get('task_id') for j in state.get('work_queue', {}).values()
                     if j.get('status') in ('QUEUED', 'RESERVED', 'RUNNING', 'UNKNOWN'))
    for lock in state.get('repo_locks', {}).values():
        owner = state.get('issues', {}).get(lock.get('owner'), {})
        protected.update(t for t in (owner.get('task_id'), owner.get('review_task_id')) if t)
    registered = {(c['machine'], c['workspace']) for c in registry.values()}
    if not data_dir.exists():
        return actions
    for directory in sorted(data_dir.iterdir()):
        if directory.name == 'archives' or directory.name in protected or directory.is_symlink() or not directory.is_dir():
            continue
        try:
            meta = read_json(directory / 'meta.json')
            if not meta or meta.get('id') != directory.name or (meta.get('machine'), meta.get('workspace')) not in registered:
                continue
            if any(p.name not in KNOWN_FILES or not p.is_file() or p.is_symlink() for p in directory.iterdir()):
                continue
            # Require a durable exit marker. Process disappearance alone never justifies archiving.
            if (not (directory / 'exit_code').exists() or meta.get('launch_prepared') or
                meta.get('resume_prepared') or worker_runtime.alive(meta)):
                continue
            int((directory / 'exit_code').read_text().strip())
            age = age_seconds(meta.get('updated_at') or meta.get('created_at'), now)
            if age is None or age < retention_days * 86400:
                continue
            if (data_dir / 'archives' / directory.name).exists():
                continue
            actions.append({'action': 'archive_task', 'task_id': directory.name})
        except (OSError, ValueError):
            continue
    return actions


def apply(state, data_dir, actions, persist, close_issue):
    for action in actions:
        if action['action'] == 'close_issue':
            number = action['number']
            if state['issues'][number].get('issue_closed'):
                continue
            close_issue(number)  # GitHub issue close is idempotent.
            state['issues'][number]['issue_closed'] = True
            persist()
        elif action['action'] == 'archive_task':
            task_id = action['task_id']
            source, dest = data_dir / task_id, data_dir / 'archives' / task_id
            if not source.exists() or source.is_symlink() or dest.exists():
                continue
            if any(p.name not in KNOWN_FILES or not p.is_file() or p.is_symlink() for p in source.iterdir()):
                continue
            if not (source / 'exit_code').exists():
                continue
            current = read_json(source / 'meta.json')
            if not current or current.get('id') != task_id or current.get('resume_prepared') or current.get('launch_prepared') or worker_runtime.alive(current):
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            meta = read_json(source / 'meta.json')
            meta['archived'] = True
            report = {'id': meta.get('id'), 'machine': meta.get('machine'), 'agent': meta.get('agent'),
                      'session_id': meta.get('session_id'), 'created_at': meta.get('created_at'),
                      'updated_at': meta.get('updated_at'), 'exit_code': (source / 'exit_code').read_text().strip()}
            if not (source / 'report.json').exists():
                write_json(source / 'report.json', report)
            write_json(source / 'meta.json', meta)
            source.rename(dest)


def main():
    parser = argparse.ArgumentParser(description='Inspect cleanup by default. --apply archives known tasks and closes completed issues.')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--retention-days', type=int, default=int(os.environ.get('DEVORCHESTRATOR_RETENTION_DAYS', '30')))
    parser.add_argument('--issue-days', type=int, default=int(os.environ.get('DEVORCHESTRATOR_ISSUE_RETENTION_DAYS', '7')))
    parser.add_argument('--state-dir', type=Path, default=devctl.DATA_DIR.parent)
    args = parser.parse_args()
    state_file = args.state_dir / 'task_bridge_state.json'
    with exclusive(args.state_dir / 'bridge.lock'), exclusive(args.state_dir / 'worker.lock'):
        state = read_json(state_file, {'issues': {}})
        data = args.state_dir / 'tasks'
        actions = plan(state, data, devctl.load_projects(), args.retention_days, args.issue_days)
        print(json.dumps({'dry_run': not args.apply, 'actions': actions}, indent=2))
        if args.apply:
            import task_bridge
            def close(number):
                task_bridge.gh('issue', 'close', str(number), '-R', task_bridge.REPO)
            apply(state, data, actions, lambda: write_json(state_file, state), close)


if __name__ == '__main__':
    main()

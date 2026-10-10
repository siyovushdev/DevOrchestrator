#!/usr/bin/env python3
"""Read-only-to-source migration dry-run.

Copies one backup into an explicit work directory, loads it with the staging
ControlLoop, and writes a redacted structural summary. It never calls GitHub,
MCP, git, SSH, or worker launch, and it refuses the live state path.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys


LIVE_STATE = Path('/home/siovush/.local/share/devorchestrator/task_bridge_state.json')
LIVE_DIR = LIVE_STATE.parent
STAGING = Path('/home/siovush/DevOrchestrator-acceptance')
PRODUCTION = Path('/home/siovush/DevOrchestrator')
FORBIDDEN_OUTPUT = (LIVE_DIR, PRODUCTION, Path('/home/siovush/.config/devorchestrator'))
VOLATILE = frozenset({
    'observed_at', 'heartbeat_age_seconds', 'last_heartbeat_at',
    'heartbeat_sequence', 'lease_expires_at', 'lease_state',
})
TOKEN = set('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_:-')


def forbid_subprocess(*_args, **_kwargs):
    raise AssertionError('migration dry-run invoked a subprocess')


def digest(value):
    payload = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(payload.encode()).hexdigest()


def token(value):
    return isinstance(value, str) and 1 <= len(value) <= 48 and set(value) <= TOKEN


def public(value, fallback='present'):
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int) and not isinstance(value, bool) and abs(value) < 1_000_000:
        return value
    if token(value):
        return value
    return fallback


def diff_paths(before, after, prefix=''):
    added, removed, changed = [], [], []
    if isinstance(before, dict) and isinstance(after, dict):
        for key in sorted(set(before) | set(after), key=str):
            path = f'{prefix}.{key}' if prefix else str(key)
            if key not in before:
                added.append(path)
            elif key not in after:
                removed.append(path)
            else:
                sub_added, sub_removed, sub_changed = diff_paths(before[key], after[key], path)
                added.extend(sub_added)
                removed.extend(sub_removed)
                changed.extend(sub_changed)
        return added, removed, changed
    if isinstance(before, list) and isinstance(after, list):
        if before != after:
            changed.append(prefix or '$')
        return added, removed, changed
    if before != after:
        changed.append(prefix or '$')
    return added, removed, changed


_VOLATILE_REMOVAL = re.compile(
    r'^issues\.[^.]+\.runtime_health\.[^.]+\.(?:' + '|'.join(sorted(VOLATILE)) + r')$'
)


def allowed_removal(path):
    """Only the root timings tree and the six volatile health leaves may disappear."""
    if path == 'timings' or path.startswith('timings.'):
        return True
    return _VOLATILE_REMOVAL.fullmatch(path) is not None


def assert_staging_target(path, label):
    resolved = path.resolve()
    for root in FORBIDDEN_OUTPUT:
        root = root.resolve()
        if resolved == root or root in resolved.parents:
            raise SystemExit(f'refusing to write {label} inside {root}')
    staging = STAGING.resolve()
    if staging not in resolved.parents:
        raise SystemExit(f'{label} must stay inside the staging checkout')


def classify(added, removed, changed):
    unexpected = [path for path in removed if not allowed_removal(path)]
    unexpected.extend(path for path in added)
    unexpected.extend(changed)
    return unexpected


def journal_view(journal):
    if not isinstance(journal, dict):
        return None
    view = {}
    for action, entry in journal.items():
        if not isinstance(entry, dict):
            view[public(action, 'invalid')] = 'invalid'
            continue
        view[public(action, 'invalid')] = {
            'done': entry.get('done') is True,
            'dispatch_state': public(entry.get('dispatch_state')),
            'create_status': public(entry.get('create_status')),
            'outcome': public(entry.get('outcome')),
        }
    return view


def issue_view(number, item, locks):
    if not isinstance(item, dict):
        return {'number': number, 'shape': 'invalid'}
    chief = item.get('chief_review') if isinstance(item.get('chief_review'), dict) else {}
    verdict = chief.get('verdict') if isinstance(chief.get('verdict'), dict) else {}
    retry = item.get('reconciliation_retry') if isinstance(item.get('reconciliation_retry'), dict) else None
    pending = item.get('pending_rpc') if isinstance(item.get('pending_rpc'), dict) else None
    return {
        'number': number,
        'project': public(item.get('project')),
        'queue_status': public(item.get('queue_status')),
        'coding_status': public(item.get('coding_status')),
        'review_status': public(item.get('review_status')),
        'owner_state': public(item.get('owner_state')),
        'chief_required': item.get('chief_required') is True,
        'chief_status': public(item.get('chief_status')),
        'chief_complete': chief.get('complete') is True,
        'chief_verdict': public(verdict.get('verdict') or verdict.get('data', {}).get('verdict')
                                if isinstance(verdict.get('data'), dict) else verdict.get('verdict')),
        'holds_repo_lock': any(isinstance(lock, dict) and str(lock.get('owner')) == number for lock in locks.values()),
        'pending_rpc': None if pending is None else {
            'name': public(pending.get('name')),
            'operation': public(pending.get('operation')),
        },
        'review_transition': item.get('review_transition') is not None,
        'review_migration': public((item.get('review_migration') or {}).get('phase'))
        if isinstance(item.get('review_migration'), dict) else None,
        'reconciliation_attempts': None if retry is None else retry.get('attempts'),
        'publication': journal_view(item.get('publication_journal')),
        'task_identity': digest([item.get('task_id'), item.get('review_task_id')]),
    }


def structural(state):
    issues = state.get('issues') if isinstance(state.get('issues'), dict) else {}
    queue = state.get('work_queue') if isinstance(state.get('work_queue'), dict) else {}
    locks = state.get('repo_locks') if isinstance(state.get('repo_locks'), dict) else {}
    statuses = {}
    for job in queue.values():
        status = job.get('status') if isinstance(job, dict) else 'invalid'
        key = status if token(str(status)) else 'other'
        statuses[key] = statuses.get(key, 0) + 1
    return {
        'top_level_keys': sorted(state.keys()) if isinstance(state, dict) else [],
        'issue_count': len(issues),
        'queue_count': len(queue),
        'queue_status': statuses,
        'pending_rpc': sum(isinstance(item, dict) and isinstance(item.get('pending_rpc'), dict)
                           for item in issues.values()),
        'review_transition': sum(isinstance(item, dict) and item.get('review_transition') is not None
                                 for item in issues.values()),
        'review_migration': sum(isinstance(item, dict) and item.get('review_migration') is not None
                                for item in issues.values()),
        'reconciliation_retry': sorted(number for number, item in issues.items()
                                       if isinstance(item, dict) and item.get('reconciliation_retry')),
        'repo_lock_owners': sorted(str(lock.get('owner')) for lock in locks.values()
                                   if isinstance(lock, dict)),
        'repo_lock_keys_are_digests': all(isinstance(key, str) and len(key) == 64 and set(key) <= set('0123456789abcdef')
                                          for key in locks),
        'issues': {number: issue_view(number, item, locks) for number, item in sorted(issues.items())},
        'workflow_digest': digest(state),
    }


def snapshot_bytes(path):
    with path.open('rb') as stream:
        return stream.read()


def write_copy(source, dest):
    assert_staging_target(dest, 'state copy')
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(snapshot_bytes(source))
    if dest.read_bytes() != snapshot_bytes(source):
        raise SystemExit('backup copy mismatch')


def run(source, work, label):
    os.environ['HOME'] = str(work / 'home')
    os.environ['TMPDIR'] = str(work / 'tmp')
    (work / 'home').mkdir(parents=True, exist_ok=True)
    (work / 'tmp').mkdir(parents=True, exist_ok=True)
    copy = work / 'state' / 'task_bridge_state.json'
    write_copy(source, copy)
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    import subprocess
    subprocess.run = forbid_subprocess
    subprocess.Popen = forbid_subprocess
    subprocess.check_output = forbid_subprocess
    subprocess.check_call = forbid_subprocess
    import task_bridge as bridge
    import bridge_telemetry
    from control_loop import ControlLoop
    if Path(bridge.STATE_FILE).resolve() == LIVE_STATE.resolve():
        raise SystemExit('staging import resolved the live state file')
    bridge.STATE_FILE = copy
    bridge.gh = forbid_subprocess
    bridge.transport_call = forbid_subprocess
    bridge.send_telegram = forbid_subprocess
    raw = json.loads(copy.read_text())
    projected = bridge_telemetry.durable_state(raw)
    before = structural(projected)
    loaded = bridge.load_state()
    load_added, load_removed, load_changed = diff_paths(projected, bridge_telemetry.durable_state(loaded))
    before_enroll = copy.read_bytes()
    loop = ControlLoop(loaded)
    enroll_wrote = copy.read_bytes() != before_enroll
    # No step and no close-time provider I/O. Constructor enrollment is the migration.
    enrolled = bridge_telemetry.durable_state(loop.state)
    enroll_added, enroll_removed, enroll_changed = diff_paths(projected, enrolled)
    first = bridge.save_state(loop.state)
    first_bytes = copy.read_bytes()
    second = bridge.save_state(loop.state)
    third = bridge.save_state(bridge.load_state())
    reloaded = bridge_telemetry.durable_state(bridge.load_state())
    final_added, final_removed, final_changed = diff_paths(projected, reloaded)
    return {
        'label': label,
        'source_bytes': source.stat().st_size,
        'source_sha256': hashlib.sha256(snapshot_bytes(source)).hexdigest(),
        'copy_sha256': hashlib.sha256(copy.read_bytes()).hexdigest(),
        'load_unexpected': classify(load_added, load_removed, load_changed),
        'enroll_unexpected': classify(enroll_added, enroll_removed, enroll_changed),
        'final_unexpected': classify(final_added, final_removed, final_changed),
        'volatile_removals': [path for path in final_removed if allowed_removal(path)],
        'enroll_wrote': enroll_wrote,
        'first_save_changed': first is True,
        'second_save_changed': second is True,
        'third_save_changed': third is True,
        'bytes_after_first_save': len(first_bytes),
        'bytes_stable_after_first_save': copy.read_bytes() == first_bytes,
        'before': before,
        'after_issue_count': structural(reloaded)['issue_count'],
        'after_queue_status': structural(reloaded)['queue_status'],
        'after_pending_rpc': structural(reloaded)['pending_rpc'],
        'after_review_transition': structural(reloaded)['review_transition'],
        'after_review_migration': structural(reloaded)['review_migration'],
        'after_retries': structural(reloaded)['reconciliation_retry'],
        'after_lock_owners': structural(reloaded)['repo_lock_owners'],
        'after_workflow_digest': structural(reloaded)['workflow_digest'],
        'focus': {number: structural(reloaded)['issues'][number]
                  for number in ('3', '13', '15', '16', '37', '47')
                  if number in structural(reloaded)['issues']},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--label', required=True)
    args = parser.parse_args()
    work = args.work.resolve()
    output = args.output.resolve()
    assert_staging_target(work, 'work directory')
    assert_staging_target(output, 'summary output')
    if output == args.source.resolve() or output == LIVE_STATE.resolve():
        raise SystemExit('refusing to overwrite the migration source')
    summary = run(args.source.resolve(), work, args.label)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, indent=2, sort_keys=True) + '\n')
    print(json.dumps({
        'label': summary['label'],
        'issue_count': summary['before']['issue_count'],
        'queue_count': summary['before']['queue_count'],
        'queue_status': summary['before']['queue_status'],
        'pending_rpc': summary['before']['pending_rpc'],
        'review_transition': summary['before']['review_transition'],
        'retries': summary['before']['reconciliation_retry'],
        'locks': summary['before']['repo_lock_owners'],
        'first_save_changed': summary['first_save_changed'],
        'second_save_changed': summary['second_save_changed'],
        'unexpected': {
            'load': summary['load_unexpected'][:20],
            'enroll': summary['enroll_unexpected'][:20],
            'final': summary['final_unexpected'][:20],
        },
        'digest_stable': summary['before']['workflow_digest'] == summary['after_workflow_digest'],
    }, indent=2))


if __name__ == '__main__':
    main()

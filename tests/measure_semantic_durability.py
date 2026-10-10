"""Isolated wall-clock measurement of the production ControlLoop write path.

Run from the repository root, for example:
  python3 tests/measure_semantic_durability.py --mode baseline --seconds 60
  python3 tests/measure_semantic_durability.py --mode fixed --seconds 60

Baseline restores the pre-fix save_state implementation. Both modes use the
same production loop, scheduler and 2.28 MB fixture. Only external read edges
are faked; no services, agents, repositories or publication are touched.
With --deferred-notification, a second idle tracked issue retains a PENDING
NEEDS_OWNER notification throughout the window. The review-baseline mode
reproduces the previous suppression check's payload regeneration while keeping
the authoritative telemetry filter enabled.
"""
import argparse
import asyncio
import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import tempfile
import time
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import devctl
import durable
from scheduler import Scheduler
import task_bridge as bridge


def process_io():
    return {key: int(value) for key, value in
            (line.split(':') for line in Path('/proc/self/io').read_text().splitlines())}


async def measure(mode, seconds, interval, deferred_notification=False):
    with tempfile.TemporaryDirectory(prefix='bridge-idle-measure-') as tmp:
        state_file = Path(tmp) / 'task_bridge_state.json'
        config = {'test': {'machine': 'work', 'workspace': tmp}}
        issue = dict(number=1, state='OPEN', title='Idle fixture', labels=['dev:running'])
        issues = {1: issue}
        comments = []
        item = dict(task_id='task', project='test', coding_status='RUNNING', coding_turn=1,
                    desired_label='dev:running', projection={'health': 'SYNCED', 'target': 'dev:running', 'failures': 0})
        state = dict(issues={'1': item}, large_payload='x' * 2_280_000)
        if deferred_notification:
            issues[2] = dict(number=2, state='OPEN', title='Deferred owner notification', labels=['dev:needs-owner'])
            state['issues']['2'] = dict(task_id='owner-task', project='test', coding_status='DONE',
                desired_label='dev:needs-owner', owner_blocker='Choose A or B',
                compatibility_status='QUARANTINED',
                notification_report={'created_at': (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()})
        scheduler = Scheduler(state)
        scheduler.enqueue('coding', '1', 'coding', 'work', 'fixture-repo')
        scheduler.reserve('coding')
        scheduler.started('coding', 'task')
        reads = {'discovery': 0, 'reports': 0}

        async def transport(name, args):
            now = time.time()
            report = dict(id='task', project='test', workspace=tmp, machine='work', agent='codex',
                status='RUNNING', role='coding', turns=1, exit_code=None,
                liveness=dict(state='RUNNING_HEALTHY', reason='lease_fresh_and_processes_owned',
                    worker_alive=True, process_alive=True, phase='CODING', generation='fixture-generation',
                    transition_sequence=1, last_transition_at=1, worker_pid=123, agent_pid=456,
                    started_at=1, observed_at=now, last_heartbeat_at=now - 2,
                    heartbeat_sequence=int(now / 5), heartbeat_age_seconds=2,
                    lease_expires_at=now + 28, lease_state='FRESH'))
            if name == 'list_tasks':
                reads['discovery'] += 1
                return [report]
            if name == 'get_task_report':
                reads['reports'] += 1
                return report
            raise AssertionError('Unexpected workflow transition: ' + name)

        def previous_save(value):
            return durable.write_json_if_changed(state_file, value)

        prepare = bridge.prepare_notification
        def previous_prepare(entry, tracked, current=None):
            # Reproduce the independently reviewed defect while retaining the
            # first fix's authoritative telemetry filter.
            allowed = prepare(entry, tracked, current)
            if allowed:
                entry['payload'] = bridge.render_notification(entry, tracked, current)
            return allowed

        def github_json(*args):
            if args[:2] == ('issue', 'view'):
                return copy.deepcopy(issues[int(args[2])])
            raise AssertionError('Unexpected GitHub read: ' + repr(args))

        def github(*args):
            if args[0] == 'api' and '/labels?' in args[1]:
                number = int(args[1].split('/issues/')[1].split('/')[0])
                return json.dumps([{'name': name} for name in issues[number]['labels']])
            raise AssertionError('Unexpected GitHub operation: ' + repr(args))

        def transition(current, target):
            if target != 'dev:running':
                raise AssertionError('Unexpected workflow transition: ' + target)

        actual_write = durable.write_json
        writes = {'authoritative': 0, 'authoritative_bytes': 0, 'telemetry': 0, 'telemetry_bytes': 0}

        def counted_write(path, value):
            actual_write(path, value)
            kind = 'authoritative' if path == state_file else 'telemetry'
            writes[kind] += 1
            writes[kind + '_bytes'] += path.stat().st_size

        patches = [patch.object(bridge, 'STATE_FILE', state_file),
            patch.object(devctl, 'load_projects', return_value=config),
            patch.object(bridge, 'project_identity', return_value=('work', 'fixture-repo')),
            patch.object(bridge, 'open_issues', side_effect=lambda: copy.deepcopy(list(issues.values()))),
            patch.object(bridge, 'fetch_issue', side_effect=lambda number: copy.deepcopy(issues[int(number)])),
            patch.object(bridge, 'issue_comments', side_effect=lambda number: copy.deepcopy(comments)),
            patch.object(bridge, 'gh_json', side_effect=github_json),
            patch.object(bridge, 'gh', side_effect=github),
            patch.object(bridge, 'raw_add_comment', side_effect=AssertionError('Unexpected comment')),
            patch.object(bridge, 'raw_transition_issue', side_effect=transition),
            patch.object(bridge, 'send_telegram', side_effect=AssertionError('Unexpected notification')),
            patch.object(bridge, 'transport_call', side_effect=transport)]
        if mode == 'baseline':
            patches.append(patch.object(bridge, 'save_state', side_effect=previous_save))
        elif mode == 'review-baseline':
            patches.append(patch.object(bridge, 'prepare_notification', side_effect=previous_prepare))
        for p in patches:
            p.start()
        loop = bridge.ControlLoop(state)
        try:
            deferred_entry = None
            if deferred_notification:
                waiting = state['issues']['2']
                with patch.object(bridge, 'WORK_CONTEXT', (state, '2', waiting)):
                    bridge.notify('NEEDS_OWNER', issues[2], waiting, waiting['notification_report'], 'Choose A or B')
                deferred_entry = next(iter(state['outbox'].values()))
                deferred_entry['next_attempt'] = time.time() + seconds + 3600
            # Settle initial schema, health and queue receipts before measuring.
            for _ in range(200):
                await loop.step()
                await asyncio.sleep(.005)
                if (item.get('runtime_health') and not loop.continuations and
                        not loop.after_work and not loop.dispatcher.flights):
                    break
            else:
                raise AssertionError('F3 warmup did not settle all writer continuations')
            loop.next_heavy = time.monotonic() + 30
            if item.get('runtime_exclusion') or item.get('reconciliation_retry') or not item.get('runtime_health'):
                raise AssertionError('Warmup did not establish a healthy running fixture')
            before = state_file.stat()
            authoritative_before = state_file.read_bytes()
            io_before = process_io()
            initial_reads = dict(reads)
            payload_before = deferred_entry['payload'] if deferred_entry else None
            ticks = 0
            started = time.monotonic()
            with patch.object(durable, 'write_json', side_effect=counted_write):
                while True:
                    await loop.step()
                    ticks += 1
                    remaining = seconds - (time.monotonic() - started)
                    if remaining <= 0:
                        break
                    await loop.wait(min(interval, remaining))
            elapsed = time.monotonic() - started
            io_after = process_io()
            after = state_file.stat()
            result = dict(mode=mode, deferred_notification=deferred_notification,
                elapsed_seconds=round(elapsed, 3), writer_steps=ticks,
                interval_seconds=interval, state_bytes=after.st_size, writes=writes,
                mtime_unchanged=before.st_mtime_ns == after.st_mtime_ns,
                inode_unchanged=before.st_ino == after.st_ino,
                authoritative_bytes_unchanged=authoritative_before == state_file.read_bytes(),
                process_write_bytes=io_after['write_bytes'] - io_before['write_bytes'],
                process_write_syscalls=io_after['syscw'] - io_before['syscw'],
                discovery_reads=reads['discovery'] - initial_reads['discovery'],
                report_reads=reads['reports'] - initial_reads['reports'])
            if deferred_entry:
                result.update(notification_status=deferred_entry['status'],
                    notification_payload_unchanged=deferred_entry['payload'] == payload_before)
                if deferred_entry['status'] != 'PENDING':
                    raise AssertionError('Deferred fixture lost its pending notification')
            if seconds >= 60 and not result['report_reads']:
                raise AssertionError('Window did not exercise heavy runtime observations')
            if mode == 'fixed' and (writes['authoritative'] or not result['mtime_unchanged']):
                raise AssertionError(result)
            return result
        finally:
            await loop.close()
            bridge.WORK_CONTEXT = None
            for p in reversed(patches):
                p.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('baseline', 'review-baseline', 'fixed'), required=True)
    parser.add_argument('--deferred-notification', action='store_true')
    parser.add_argument('--seconds', type=float, default=60)
    parser.add_argument('--interval', type=float, default=bridge.POLL_SECONDS)
    parser.add_argument('--output', type=Path, help='Also save the measured JSON evidence to this file')
    args = parser.parse_args()
    if args.seconds <= 0 or args.interval <= 0:
        parser.error('seconds and interval must be positive')
    evidence = json.dumps(asyncio.run(measure(args.mode, args.seconds, args.interval, args.deferred_notification)), indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(evidence + '\n')
    print(evidence)

"""Runtime evidence, independent of workflow states and transport projections.

Only a supervising worker refreshes the lease. Readers re-probe ownership and
persist classification transitions; they never manufacture a heartbeat. Linux
boot/start identity prevents PID reuse from proving ownership after a restart.
"""
import math
from contextlib import nullcontext
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

from durable import exclusive, read_json, write_json
from worker_runtime import process_identity

HEARTBEAT_SECONDS = 5
LEASE_SECONDS = 30
STATES = ('RUNNING_HEALTHY', 'SUSPECTED_STALL', 'LOST_ORPHANED', 'TERMINAL')


def _number(value):
    try:
        return type(value) in (int, float) and math.isfinite(value) and value >= 0
    except OverflowError:
        return False


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('Duplicate runtime field')
        result[key] = value
    return result


def read_record(directory):
    return read_json(directory / 'liveness.json', object_pairs_hook=_unique)


def _identity(value):
    return (isinstance(value, dict) and set(value) == {'boot', 'start'} and
            all(type(v) is str and v for v in value.values()))


def validate(record, meta=None):
    if not isinstance(record, dict) or type(record.get('schema_version')) is not int or record['schema_version'] != 1:
        raise ValueError('Invalid runtime record')
    for field in ('task_id', 'project', 'machine', 'worker_id', 'agent', 'generation', 'phase'):
        if type(record.get(field)) is not str or not record[field] or len(record[field]) > 512:
            raise ValueError('Invalid runtime identity')
    for field in ('worker_pid', 'agent_pid', 'heartbeat_sequence'):
        if type(record.get(field)) is not int or record[field] < 0:
            raise ValueError('Invalid runtime counter')
    for field in ('started_at', 'last_heartbeat_at', 'lease_expires_at'):
        if not _number(record.get(field)):
            raise ValueError('Invalid runtime timestamp')
    if not record['started_at'] <= record['last_heartbeat_at'] <= record['lease_expires_at']:
        raise ValueError('Invalid runtime lease ordering')
    for field in ('worker', 'agent'):
        identity = record.get(field + '_identity')
        if identity is not None and not _identity(identity):
            raise ValueError('Invalid process identity')
    for field in ('session_id', 'terminal_reason'):
        if record.get(field) is not None and (type(record[field]) is not str or len(record[field]) > 512):
            raise ValueError('Invalid runtime detail')
    if record.get('exit_code') is not None and type(record['exit_code']) is not int:
        raise ValueError('Invalid runtime exit code')
    if record.get('phase') not in ('STARTING', 'CODING', 'REVIEW', 'EXITED') or record.get('role') not in ('coding', 'review'):
        raise ValueError('Invalid runtime phase/role')
    if record.get('terminal_reason') not in (None, 'process_exited', 'process_exited_without_final_report'):
        raise ValueError('Invalid runtime terminal reason')
    if ((record['exit_code'] is None) != (record.get('terminal_reason') is None) or
            (record['exit_code'] is not None) != (record['phase'] == 'EXITED') or
            (record.get('terminal_reason') == 'process_exited_without_final_report' and record['exit_code'] != 0)):
        raise ValueError('Invalid runtime terminal evidence')
    if meta:
        for key, source in (('task_id', 'id'), ('machine', 'machine'), ('agent', 'agent'),
                            ('generation', 'runtime_generation'), ('project', 'project'), ('role', 'role')):
            if meta.get(source) is not None and record[key] != meta[source]:
                raise ValueError('Runtime identity does not match task')
    return record


def initialize(directory, meta, *, clock=None):
    now = time.time() if clock is None else clock
    record = dict(schema_version=1, task_id=meta['id'], project=meta.get('project') or '-',
                  machine=meta['machine'], worker_id=meta['machine'], agent=meta['agent'],
                  session_id=meta.get('session_id'), generation=meta.get('runtime_generation') or uuid.uuid4().hex,
                  worker_pid=0, worker_identity=None, agent_pid=0, agent_identity=None,
                  phase='STARTING', started_at=now, last_heartbeat_at=now,
                  heartbeat_sequence=0, lease_expires_at=now + LEASE_SECONDS,
                  exit_code=None, terminal_reason=None, role=meta.get('role', 'coding'))
    record['turn'] = meta.get('turns', 1)
    with exclusive(directory / 'liveness.lock', blocking=True):
        previous = read_record(directory)
        if previous and previous.get('generation') != record['generation']:
            # Preserve each completed turn's original runtime/ownership/exit
            # receipts before installing the next generation.
            validate(previous)
            audit = read_json(directory / 'runtime_audit.json', {})
            audit.setdefault(previous['generation'], dict(liveness=previous,
                process_group=read_json(directory / 'process_group.json'),
                cleanup=meta.get('cleanup_state'), session_id=meta.get('session_id')))
            write_json(directory / 'runtime_audit.json', audit)
        write_json(directory / 'liveness.json', record)
    return record


def heartbeat(directory, generation, *, worker_pid, agent_pid, phase, exit_code=None):
    """A generation fence prevents an old runner from touching a subsequent turn."""
    with exclusive(directory / 'liveness.lock', blocking=True):
        record = validate(read_record(directory))
        if record['generation'] != generation:
            raise RuntimeError('Runtime generation changed')
        now = max(time.time(), record['last_heartbeat_at'])
        record.update(worker_pid=worker_pid, worker_identity=process_identity(worker_pid),
                      agent_pid=agent_pid, agent_identity=process_identity(agent_pid),
                      phase=phase, last_heartbeat_at=now, lease_expires_at=now + LEASE_SECONDS,
                      heartbeat_sequence=record['heartbeat_sequence'] + 1)
        if exit_code is not None:
            record.update(exit_code=exit_code, terminal_reason='process_exited')
        # Record live group anchors before the supervisor exits, including
        # descendants surviving a successfully exited direct agent child.
        from worker_runtime import record_group
        if worker_pid == os.getpid():
            record_group(directory, record, worker_pid)
        write_json(directory / 'liveness.json', record)
        return record


def owned(pid, identity):
    """None is uncertainty, False is observed absence/mismatch, True is ownership.

    Do not use kill(pid, 0), an unbound PID, or inherited process groups as proof.
    The runner also records its direct agent child, so a surviving child retains
    capacity without relying on ambiguous group membership.
    """
    if type(pid) is not int or pid <= 0 or not _identity(identity):
        return None
    try:
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        if fields[0] == 'Z':
            return False
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        return identity == {'boot': boot, 'start': fields[19]}
    except (FileNotFoundError, ProcessLookupError):
        return False
    except OSError:
        return None
    except (IndexError, ValueError):
        return None


def classify(record, now, worker_alive, process_alive, *, communication=True):
    if record['exit_code'] is not None or record.get('terminal_reason'):
        return 'TERMINAL', record.get('terminal_reason') or 'process_exited'
    if now < record['last_heartbeat_at']:
        return 'SUSPECTED_STALL', 'heartbeat_clock_uncertain'
    fresh = now < record['lease_expires_at']
    if communication and fresh and record['heartbeat_sequence'] > 0 and worker_alive is True and process_alive is True:
        return 'RUNNING_HEALTHY', 'lease_fresh_and_processes_owned'
    if worker_alive is True or process_alive is True:
        return 'SUSPECTED_STALL', 'lease_stale_but_process_owned' if not fresh else 'worker_ownership_uncertain'
    if not fresh:
        if (communication and record['heartbeat_sequence'] == 0 and
                not any(record[key] for key in ('worker_pid', 'agent_pid', 'worker_identity', 'agent_identity'))):
            return 'LOST_ORPHANED', 'lease_expired_before_worker_registered'
        return 'LOST_ORPHANED', 'lease_expired_without_owned_process'
    return 'SUSPECTED_STALL', 'worker_communication_uncertain' if not communication else 'process_ownership_unproven'


def watchdog(directory, meta, *, probe=True, terminal=None, clock=None, evidence=None, read_only=False):
    """One normalized, secret-free report, also used by cached MCP/SSH status.

    Missing legacy heartbeats cannot prove health. Remote cached evidence cannot
    prove ownership; only a successful remote probe can. Corruption is preserved
    for diagnosis and classified closed, even if an exit marker also exists.
    Only semantic transitions write runtime_health.json, making repeated ticks
    idempotent; elapsed heartbeat ages are calculated at read time.
    """
    # Same variable as external_effects.ENV. The SSH worker bundle cannot import it.
    if os.environ.get('DEVORCHESTRATOR_BLOCK_EXTERNAL_EFFECTS') == '1':
        return dict(state='UNKNOWN', reason='reconciliation_blocked',
                    observation_confidence='READ_ONLY', generation=meta.get('runtime_generation'),
                    worker_alive=None, process_alive=None, phase='UNKNOWN', exit_code=None,
                    lease_state='UNKNOWN', heartbeat_age_seconds=None)
    now = time.time() if clock is None else clock
    with nullcontext() if read_only else exclusive(directory / 'liveness.lock', blocking=True):
        record = None
        worker = process = None
        try:
            record = read_record(directory)
            if record is None and meta.get('runtime_generation'):
                state, reason = 'SUSPECTED_STALL', 'invalid_runtime_record'
            elif record is None:
                state, reason = ('TERMINAL', 'legacy_process_exited') if terminal else ('SUSPECTED_STALL', 'heartbeat_not_registered')
            else:
                validate(record, meta)
                if probe:
                    worker = owned(record['worker_pid'], record['worker_identity'])
                    process = owned(record['agent_pid'], record['agent_identity'])
                    if not record['worker_pid'] and meta.get('pid') and meta.get('process_identity'):
                        # The launch PID receipt may precede the first heartbeat.
                        worker = owned(meta['pid'], meta['process_identity'])
                elif evidence is not None:
                    worker, process = evidence
                state, reason = classify(record, now, worker, process, communication=probe or evidence is not None)
        except (OSError, ValueError, TypeError, KeyError):
            record = None
            state, reason = 'SUSPECTED_STALL', 'invalid_runtime_record'
        observation = dict(state=state, reason=reason, worker_alive=worker, process_alive=process,
                           phase=record['phase'] if record else 'UNKNOWN',
                           generation=record['generation'] if record else meta.get('runtime_generation'))
        try:
            previous = read_json(directory / 'runtime_health.json', {})
            if not isinstance(previous, dict):
                previous = {}
        except (OSError, ValueError):
            previous = {}
        sequence = previous.get('transition_sequence', 0)
        if type(sequence) is not int or sequence < 0:
            sequence = 0
        if (any(previous.get(key) != value for key, value in observation.items()) or
                not _number(previous.get('last_transition_at'))):
            observation.update(transition_sequence=sequence + 1,
                               last_transition_at=now)
            if not read_only:
                write_json(directory / 'runtime_health.json', observation)
        else:
            observation.update(transition_sequence=sequence, last_transition_at=previous['last_transition_at'])
        report = dict(observation, task_id=meta.get('id', directory.name), project=meta.get('project', '-'),
                      machine=meta.get('machine'), agent=meta.get('agent'), session_id=meta.get('session_id'),
                      heartbeat_age_seconds=None, lease_state='UNKNOWN', exit_code=None, observed_at=now)
        if record:
            report.update({key: record[key] for key in ('worker_id', 'worker_pid', 'agent_pid', 'session_id',
                          'started_at', 'last_heartbeat_at', 'heartbeat_sequence', 'lease_expires_at', 'exit_code')})
            lease_state = ('UNKNOWN' if now < record['last_heartbeat_at'] else
                           'FRESH' if now < record['lease_expires_at'] else 'EXPIRED')
            report.update(project=record['project'], heartbeat_age_seconds=max(0, now - record['last_heartbeat_at']),
                          lease_state=lease_state)
        return report


def task_status(health):
    if health['state'] == 'RUNNING_HEALTHY':
        return 'RUNNING', None
    if health['state'] == 'TERMINAL':
        code = health.get('exit_code')
        return ('DONE' if code == 0 and health['reason'] == 'process_exited' else 'FAILED'), code
    # Keep ambiguous ownership occupying capacity. Known absence frees the
    # existing failure path only after lease expiry, with a durable diagnostic.
    if health['state'] == 'LOST_ORPHANED' and health['reason'] == 'lease_expired_before_worker_registered':
        return 'FAILED', None
    if health['state'] == 'LOST_ORPHANED' and health['worker_alive'] is False and health['process_alive'] is False:
        return 'FAILED', None
    return 'UNKNOWN', None


def final_report_missing(directory, meta):
    if os.environ.get('DEVORCHESTRATOR_BLOCK_EXTERNAL_EFFECTS') == '1':
        return
    with exclusive(directory / 'liveness.lock', blocking=True):
        record = validate(read_record(directory), meta)
        if record['exit_code'] == 0 and record.get('terminal_reason') == 'process_exited':
            record['terminal_reason'] = 'process_exited_without_final_report'
            write_json(directory / 'liveness.json', record)


def annotate_session(directory, meta, session_id):
    """Session discovery is activity metadata, never a heartbeat/lease refresh."""
    if os.environ.get('DEVORCHESTRATOR_BLOCK_EXTERNAL_EFFECTS') == '1':
        return
    with exclusive(directory / 'liveness.lock', blocking=True):
        record = validate(read_record(directory), meta)
        if type(session_id) is str and 0 < len(session_id) <= 512 and record['session_id'] != session_id:
            record['session_id'] = session_id
            write_json(directory / 'liveness.json', record)


def run(directory, generation, command):
    """Supervise independently of stdout; no silence-based kills/retries."""
    if os.environ.get('DEVORCHESTRATOR_BLOCK_EXTERNAL_EFFECTS') == '1':
        raise RuntimeError('External worker effect blocked')
    directory = Path(directory)
    record = validate(read_record(directory))
    if record['generation'] != generation:
        raise RuntimeError('Runtime generation changed before launch')
    from worker_runtime import record_group
    record_group(directory, record, os.getpid())
    child = subprocess.Popen(['bash', '-c', command], stdin=subprocess.DEVNULL)
    phase = 'REVIEW' if record.get('role') == 'review' else 'CODING'
    heartbeat(directory, generation, worker_pid=os.getpid(), agent_pid=child.pid, phase=phase)
    while True:
        try:
            code = child.wait(timeout=HEARTBEAT_SECONDS)
            break
        except subprocess.TimeoutExpired:
            heartbeat(directory, generation, worker_pid=os.getpid(), agent_pid=child.pid, phase=phase)
    # The locked generation check and exit marker must precede any new turn.
    heartbeat(directory, generation, worker_pid=os.getpid(), agent_pid=child.pid, phase='EXITED', exit_code=code)
    with exclusive(directory / 'liveness.lock', blocking=True):
        if read_record(directory)['generation'] == generation:
            path = directory / 'exit_code.tmp'
            with path.open('w') as stream:
                stream.write(str(code)); stream.flush(); os.fsync(stream.fileno())
            path.replace(directory / 'exit_code')
            fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
    return code


if __name__ == '__main__':
    raise SystemExit(run(*sys.argv[1:4]))

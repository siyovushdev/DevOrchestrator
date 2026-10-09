"""Local worker admission and RPC receipts; ambiguous launches are never replayed."""
import hashlib
import os
from pathlib import Path
import re
import signal
import time

from durable import exclusive, read_json, write_json


def valid_request(request_id):
    if request_id and not re.fullmatch(r'[a-f0-9]{64}', request_id):
        raise ValueError('Invalid request ID')


def boot_id():
    return Path('/proc/sys/kernel/random/boot_id').read_text().strip()


def process_identity(pid):
    if not isinstance(pid, int) or pid <= 0:
        return None
    try:
        # comm may contain spaces and parentheses; fields after its final ')' are stable.
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        if fields[0] == 'Z':
            return None
        return {'boot': boot_id(), 'start': fields[19]}
    except (OSError, IndexError):
        return None


def alive(meta):
    pid = meta.get('pid', 0)
    if not isinstance(pid, int) or pid <= 0:
        return False
    expected = meta.get('process_identity')
    if expected and expected['boot'] != boot_id():
        return False
    identity = process_identity(pid)
    if identity:
        return not expected or expected == identity
    if Path(f'/proc/{pid}').exists():
        return False
    # A killed wrapper can leave an agent child in its original session/process group.
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():
            continue
        try:
            fields = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
            if fields[0] != 'Z' and fields[2] == str(pid) and fields[3] == str(pid):
                return True
        except (OSError, IndexError):
            pass
    return False


def process_details(pid):
    """Read even zombie leaders so PID reuse cannot hide behind child scans."""
    try:
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        return dict(pid=pid, state=fields[0], pgid=int(fields[2]), sid=int(fields[3]),
                    identity={'boot': boot_id(), 'start': fields[19]})
    except (FileNotFoundError, ProcessLookupError):
        return None


def session_members(sid):
    members = []
    for path in Path('/proc').iterdir():
        if path.name.isdigit():
            details = process_details(int(path.name))
            if details and details['sid'] == sid and details['state'] != 'Z':
                members.append(details)
    return members


def group_members(pgid):
    members = []
    for path in Path('/proc').iterdir():
        if path.name.isdigit():
            details = process_details(int(path.name))
            if details and details['pgid'] == pgid and details['state'] != 'Z':
                members.append(details)
    return members


def record_group(directory, record, wrapper_pid):
    """Only the isolated local supervisor records ownership, before/after work."""
    try:
        wrapper = process_details(wrapper_pid)
        if not wrapper or wrapper['pgid'] != wrapper_pid or wrapper['sid'] != wrapper_pid:
            return False  # Inherited groups are never candidates for termination.
        members = session_members(wrapper_pid)
        groups = []
        for pgid in sorted({m['pgid'] for m in members if m['pgid'] != wrapper_pid}):
            groups.append(dict(pgid=pgid, members=[{'pid': m['pid'], 'identity': m['identity']}
                for m in members if m['pgid'] == pgid]))
        receipt = dict(task_id=record['task_id'], generation=record['generation'],
            turn=record.get('turn', 1), role=record['role'], agent=record['agent'], machine=record['machine'],
            wrapper_pid=wrapper_pid, wrapper_identity=wrapper['identity'], pgid=wrapper_pid, sid=wrapper_pid,
            members=[{'pid': m['pid'], 'identity': m['identity']} for m in members if m['pgid'] == wrapper_pid],
            groups=groups)
        write_json(directory / 'process_group.json', receipt)
        return True
    except (OSError, ValueError, IndexError):
        return False  # Ownership uncertainty must not stop exit/heartbeat supervision.


def group_ownership(receipt, group=None):
    """A surviving recorded anchor proves the group/session never became free.

    Numeric PGID/SID or age alone are not proof. If every recorded member exited
    but unknown descendants survive, leave them untouched for diagnosis.
    """
    pid = receipt['wrapper_pid']
    if type(pid) is not int or pid <= 1 or receipt['pgid'] != pid or receipt['sid'] != pid:
        return 'AMBIGUOUS'
    pgid = group['pgid'] if group is not None else pid
    if type(pgid) is not int or pgid <= 1 or pgid in (os.getpid(), os.getpgrp(), os.getsid(0)) or pid == os.getsid(0):
        return 'AMBIGUOUS'
    expected = receipt['wrapper_identity']
    if set(expected) != {'boot', 'start'} or expected['boot'] != boot_id():
        return 'AMBIGUOUS'
    leader = process_details(pid)
    if leader and (leader['identity'] != expected or leader['pgid'] != pid or leader['sid'] != pid):
        return 'AMBIGUOUS'
    members = group_members(pgid)
    if not members:
        return 'ABSENT'
    if any(m['sid'] != pid or int(m['identity']['start']) < int(expected['start']) for m in members):
        return 'AMBIGUOUS'
    anchors = {m['pid']: m['identity'] for m in (group or receipt)['members']}
    if group is None:
        anchors[pid] = expected
    if any(anchors.get(m['pid']) == m['identity'] for m in members):
        return 'OWNED'
    return 'AMBIGUOUS'


def cleanup_group(directory, meta, state, *, grace_seconds=1):
    """Terminal generation-bound proof fences cleanup and every signal."""
    if state not in ('DONE', 'FAILED') or meta.get('launch_prepared') or meta.get('resume_prepared'):
        return 'ACTIVE'
    if meta.get('remote_key'):
        # Only the registered remote worker may signal its own group. A bound
        # successful cleanup receipt is cached locally; transport loss cannot
        # manufacture one or apply it to a new turn.
        if (meta.get('remote_cleanup_generation') == meta.get('runtime_generation') and
                meta.get('remote_cleanup_state') in ('ABSENT', 'CLEANED')):
            return meta['remote_cleanup_state']
        return 'AMBIGUOUS'
    if (meta.get('machine') == 'work' and meta.get('runtime_generation') and
            meta.get('cleanup_generation') == meta['runtime_generation'] and
            meta.get('cleanup_state') in ('ABSENT', 'CLEANED')):
        return meta['cleanup_state']
    try:
        receipt = read_json(directory / 'process_group.json')
        if receipt is None:
            if meta.get('runtime_generation'):
                import runtime_liveness
                record = runtime_liveness.validate(runtime_liveness.read_record(directory), meta)
                # Only a never-registered launch can use the absence fallback.
                # Exited direct PIDs cannot rule out escaped historical children.
                if record['phase'] != 'STARTING' or record['worker_pid'] or record['agent_pid']:
                    return 'AMBIGUOUS'
            if alive(meta):
                return 'AMBIGUOUS'
            leader = process_details(meta.get('pid', 0)) if meta.get('pid', 0) else None
            if leader and leader['identity'] != meta.get('process_identity'):
                return 'AMBIGUOUS'
            return 'ABSENT'  # Legacy absent processes; never signal an unbound PID.
        for key, value in (('task_id', meta['id']), ('generation', meta.get('runtime_generation')),
                           ('turn', meta.get('turns', 1)), ('role', meta.get('role', 'coding')),
                           ('agent', meta['agent']), ('machine', meta['machine'])):
            if receipt.get(key) != value:
                return 'AMBIGUOUS'
        # A status string is insufficient for sending a signal. Require a
        # current runtime terminal receipt or a legacy durable exit marker.
        if meta.get('runtime_generation'):
            import runtime_liveness
            record = runtime_liveness.validate(runtime_liveness.read_record(directory), meta)
            if record['exit_code'] is None:
                health = runtime_liveness.watchdog(directory, meta)
                if state != 'FAILED' or runtime_liveness.task_status(health)[0] != 'FAILED':
                    return 'AMBIGUOUS'
            elif int((directory / 'exit_code').read_text().strip()) != record['exit_code']:
                return 'AMBIGUOUS'  # Do not interrupt final exit-marker persistence.
        else:
            int((directory / 'exit_code').read_text().strip())
        groups = receipt.get('groups', [])
        if not isinstance(groups, list) or len({g['pgid'] for g in groups}) != len(groups):
            return 'AMBIGUOUS'
        targets = [None, *groups]
        # Validate ALL observed groups before signalling any of them. An
        # unrecorded group in the original session is diagnostic-only.
        pgids = {receipt['pgid'], *(g['pgid'] for g in groups)}
        if any(m['pgid'] not in pgids for m in session_members(receipt['sid'])):
            return 'AMBIGUOUS'
        if any(group_ownership(receipt, g) == 'AMBIGUOUS' for g in targets):
            return 'AMBIGUOUS'
        signalled = False
        budget = time.monotonic() + grace_seconds + 1
        for group in targets:
            pgid = (group or receipt)['pgid']
            # Recheck at each send boundary, not just the all-group preflight.
            ownership = group_ownership(receipt, group)
            if ownership == 'ABSENT':
                continue
            if ownership != 'OWNED':
                return 'AMBIGUOUS'
            os.killpg(pgid, signal.SIGTERM)
            signalled = True
            deadline = min(budget, time.monotonic() + grace_seconds)
            while time.monotonic() < deadline:
                ownership = group_ownership(receipt, group)
                if ownership != 'OWNED':
                    break
                time.sleep(min(.05, max(0, deadline - time.monotonic())))
            ownership = group_ownership(receipt, group)
            if ownership == 'AMBIGUOUS':
                return 'AMBIGUOUS'
            if ownership == 'OWNED':
                os.killpg(pgid, signal.SIGKILL)
                while time.monotonic() < budget:
                    ownership = group_ownership(receipt, group)
                    if ownership != 'OWNED':
                        break
                    time.sleep(.05)
                if ownership != 'ABSENT':
                    return 'AMBIGUOUS'
        if any(m['pgid'] not in pgids for m in session_members(receipt['sid'])):
            return 'AMBIGUOUS'
        return 'CLEANED' if signalled else 'ABSENT'
    except ProcessLookupError:
        # Recheck; absence is safe, a changed numeric group is not.
        try:
            targets = [None, *receipt.get('groups', [])]
            return ('ABSENT' if all(group_ownership(receipt, g) == 'ABSENT' for g in targets)
                    and not session_members(receipt['sid']) else 'AMBIGUOUS')
        except (OSError, ValueError, TypeError, KeyError, IndexError):
            return 'AMBIGUOUS'
    except (OSError, ValueError, TypeError, KeyError, IndexError):
        return 'AMBIGUOUS'



def publication_safe(directory, meta):
    """Read-only ownership proof for terminal tasks; never signal or reconcile."""
    try:
        if meta.get('launch_prepared') or meta.get('resume_prepared'):
            return False
        if meta.get('remote_key'):
            return (meta.get('remote_cleanup_generation') == meta.get('runtime_generation') and
                    meta.get('remote_cleanup_state') in ('ABSENT', 'CLEANED'))
        if (meta.get('machine') == 'work' and meta.get('runtime_generation') and
                meta.get('cleanup_generation') == meta['runtime_generation'] and
                meta.get('cleanup_state') in ('ABSENT', 'CLEANED')):
            return True  # Same generation-bound attestation as cleanup_group.
        if meta.get('runtime_generation'):
            import runtime_liveness
            record = runtime_liveness.validate(runtime_liveness.read_record(directory), meta)
            if (record.get('exit_code') is None or record.get('phase') != 'EXITED' or
                    int((directory / 'exit_code').read_text().strip()) != record['exit_code']):
                return False
        receipt = read_json(directory / 'process_group.json')
        if receipt is None:
            if meta.get('runtime_generation'):
                # Exited direct PIDs cannot rule out escaped historical children.
                # Terminal runtimes require a group receipt or bound attestation.
                return False
            leader = process_details(meta.get('pid', 0)) if meta.get('pid', 0) else None
            return not alive(meta) and not leader
        for key, value in (('task_id', meta['id']), ('generation', meta.get('runtime_generation')),
                           ('turn', meta.get('turns', 1)), ('role', meta.get('role', 'coding')),
                           ('agent', meta['agent']), ('machine', meta['machine'])):
            if receipt.get(key) != value:
                return False
        return (group_ownership(receipt) == 'ABSENT' and
                all(group_ownership(receipt, group) == 'ABSENT' for group in receipt.get('groups', [])) and
                not session_members(receipt['sid']))
    except Exception:
        return False  # Missing/corrupt ownership proof never frees a repository.


def reconcile_task(directory, meta, state, code):
    """Called under worker.lock; preserve original receipts and audit history."""
    if state not in ('DONE', 'FAILED'):
        return None
    import copy
    before = copy.deepcopy(meta)
    result = cleanup_group(directory, meta, state)
    if meta.get('status') != state:
        meta.setdefault('terminal_reconciliation', {}).setdefault('previous_status', meta.get('status'))
        meta['status'] = state
    meta['effective_exit_code'] = code
    # Preserve a successful receipt on repeated absence observations.
    meta['cleanup_state'] = 'CLEANED' if result == 'ABSENT' and meta.get('cleanup_state') == 'CLEANED' else result
    if (meta.get('machine') == 'work' and not meta.get('remote_key') and
            meta.get('runtime_generation') and result in ('ABSENT', 'CLEANED')):
        meta['cleanup_generation'] = meta['runtime_generation']
    if result == 'AMBIGUOUS':
        meta['consistency_diagnostic'] = 'Terminal runtime ownership ambiguous; group and lease retained'
    else:
        meta.pop('consistency_diagnostic', None)
    if meta != before:
        # Keep updated_at/created_at, session and original exit_code unchanged.
        write_json(directory / 'meta.json', {k: v for k, v in meta.items() if k != '_runtime_health'})
    return result


def reconcile_leases(data_dir, terminal):
    """Apply classified evidence; unmentioned tasks retain leases and diagnostics.

    Only a bound terminal receipt with absent/cleaned runtime frees a lease.
    Missing evidence is not a missing task: callers must classify that explicitly.
    """
    path = data_dir.parent / 'worker_leases.json'
    leases = read_json(path, {})
    diagnostic_path = data_dir.parent / 'worker_consistency.json'
    report = read_json(diagnostic_path, {})
    if not isinstance(report, dict) or not isinstance(report.get('leases', {}), dict):
        raise ValueError('Invalid worker consistency diagnostics')
    diagnostics = {key: value for key, value in report.get('leases', {}).items() if key in leases}
    changed = False
    for key, lease in list(leases.items()):
        task_id = lease.get('task_id') if isinstance(lease, dict) else None
        proof = terminal.get(task_id)
        if not isinstance(proof, dict) or proof.get('cleanup') not in ('ABSENT', 'CLEANED', 'ACTIVE', 'AMBIGUOUS', 'MISSING'):
            continue  # Skipped probes, sweep limits, and incomplete classifications.
        bound = (isinstance(lease, dict) and bool(lease.get('machine')) and
                 proof.get('machine') == lease['machine'])
        if proof['cleanup'] in ('ABSENT', 'CLEANED') and bound:
            del leases[key]
            diagnostics.pop(key, None)
            changed = True
        elif proof['cleanup'] == 'ACTIVE' and bound:
            diagnostics.pop(key, None)
        else:
            diagnostics[key] = 'Lease retained: active, missing or ambiguous task ownership'
    if changed:
        write_json(path, leases)
    report['leases'] = diagnostics
    if read_json(diagnostic_path, {}) != report:
        write_json(diagnostic_path, report)


def check_capacity(data_dir, task_id, machine, role, task_state):
    from scheduler import Limits
    limits = Limits.from_env()
    active = []
    if data_dir.exists():
        directories = [p for p in data_dir.iterdir() if p.name != 'archives']
        if (data_dir / 'archives').exists():
            directories.extend((data_dir / 'archives').iterdir())
        for directory in directories:
            if not directory.is_dir() or directory.is_symlink() or directory.name == task_id:
                continue
            meta = read_json(directory / 'meta.json')
            if meta:
                state, code = task_state(meta, directory)
                cleanup = reconcile_task(directory, meta, state, code)
                if state in ('RUNNING', 'UNKNOWN', 'STARTING') or cleanup == 'AMBIGUOUS':
                    active.append(meta)
    role_limit = limits.coding if role == 'coding' else limits.review
    if (len(active) >= limits.total or
        sum(t.get('role', 'coding') == role for t in active) >= role_limit or
        sum(t.get('machine') == machine for t in active) >= limits.machines.get(machine, 1)):
        raise RuntimeError('Worker capacity is occupied by active or uncertain tasks')


def admit(data_dir, machine, canonical, task_id, task_state):
    """Called under worker.lock; only known terminal work can free a lease."""
    path = data_dir.parent / 'worker_leases.json'
    leases = read_json(path, {})
    key = hashlib.sha256((machine + '\0' + canonical).encode()).hexdigest()
    lease = leases.get(key)
    if lease and lease['task_id'] != task_id:
        directory = data_dir / lease['task_id']
        if not directory.exists():
            directory = data_dir / 'archives' / lease['task_id']
        meta = read_json(directory / 'meta.json')
        if not meta:
            raise RuntimeError('Registered workspace has an active or uncertain worker lease')
        state, code = task_state(meta, directory)
        if state not in ('DONE', 'FAILED') or reconcile_task(directory, meta, state, code) == 'AMBIGUOUS':
            raise RuntimeError('Registered workspace has an active or uncertain worker lease')
    leases[key] = {'task_id': task_id, 'machine': machine}
    write_json(path, leases)


def launch(data_dir, request_id, task_id, callback, prepare=None, signature=None):
    """Receipt is durable before launching. A crash can leave UNKNOWN, never a duplicate."""
    valid_request(request_id)
    if not request_id:
        if prepare:
            prepare()
        return callback()
    path = data_dir.parent / 'requests' / (request_id + '.json')
    with exclusive(path.with_suffix('.lock'), blocking=True):
        receipt = read_json(path)
        if receipt:
            if receipt['task_id'] != task_id or receipt.get('signature') != signature:
                raise ValueError('Request ID reused with different task arguments')
            print(task_id)
            return 0
        if prepare:
            prepare()
        write_json(path, {'task_id': task_id, 'status': 'PENDING', 'signature': signature})
        result = callback()
        write_json(path, {'task_id': task_id, 'status': 'STARTED', 'signature': signature})
        return result

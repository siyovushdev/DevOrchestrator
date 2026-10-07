"""Local worker admission and RPC receipts; ambiguous launches are never replayed."""
import hashlib
from pathlib import Path
import re

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
            if meta and task_state(meta, directory)[0] in ('RUNNING', 'UNKNOWN'):
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
        if not meta or task_state(meta, directory)[0] not in ('DONE', 'FAILED'):
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

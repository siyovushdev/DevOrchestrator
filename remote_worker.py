"""Detached SSH worker with durable remote logs/status; transport loss is not task failure."""
import base64
import json
import shlex
from pathlib import Path

import runtime_liveness
from durable import write_json

# Ship the same standard-library runtime implementation to the registered SSH
# worker, without requiring a checkout/install or a second liveness algorithm.
SOURCES = {name: Path(__file__).with_name(name + '.py').read_text()
           for name in ('durable', 'worker_runtime', 'runtime_liveness')}
BUNDLE = '''import types, sys
for module_name, source in SOURCES.items():
    module = types.ModuleType(module_name)
    sys.modules[module_name] = module
    exec(source, module.__dict__)
import runtime_liveness
'''.replace('SOURCES', repr(SOURCES), 1)

# Self-contained standard-library script, sent only to a registered machine.
SCRIPT = BUNDLE + r'''
import base64, json, os, pathlib, re, subprocess, sys, tempfile
mode, key = sys.argv[1:3]
if not re.fullmatch(r'[A-Za-z0-9_-]{1,160}', key):
    raise ValueError('Invalid remote run key')
root = pathlib.Path.home() / '.local/share/devorchestrator/remote-tasks' / key
meta_path = root / 'meta.json'
def save(data):
    fd, name = tempfile.mkstemp(dir=root)
    with os.fdopen(fd, 'w') as stream:
        json.dump(data, stream)
        stream.flush(); os.fsync(stream.fileno())
    os.replace(name, meta_path)
    directory = os.open(root, os.O_RDONLY)
    try: os.fsync(directory)
    finally: os.close(directory)
def identity(pid):
    try:
        fields = pathlib.Path('/proc/%s/stat' % pid).read_text().rsplit(')', 1)[1].split()
        if fields[0] == 'Z': return None
        return fields[19]
    except OSError: return None
boot = pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip()
if os.environ.get('DEVORCHESTRATOR_BLOCK_EXTERNAL_EFFECTS') == '1':
    if mode != 'report':
        raise RuntimeError('External worker effect blocked')
    # Direct script invocation must be observational too: no mkdir, lock,
    # watchdog write, cleanup or successful completion from cached status.
    path = root / 'events.jsonl'
    chunk = b''; more = False
    if path.exists():
        with path.open('rb') as log:
            log.seek(int(sys.argv[3])); chunk = log.read(524288); more = bool(log.read(1))
    print(json.dumps(dict(status='UNKNOWN', exit_code=None, cleanup_state='ACTIVE',
        observation_confidence='READ_ONLY', chunk=base64.b64encode(chunk).decode(), more=more)))
    raise SystemExit(0)
# Direct/delayed script invocation is not inherited launch authority.
import admission_policy
admission_policy.require_operation('remote')
root.mkdir(parents=True, exist_ok=True, mode=0o700)
# Concurrent/replayed SSH invocations must share a single launch receipt.
import fcntl
with (root / 'lock').open('a') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    if mode == 'start':
        if not meta_path.exists():
            save(dict(pid=0, boot=boot, start=None, state='STARTING'))
            command = sys.argv[3]
            context = json.loads(sys.argv[4]) if len(sys.argv) > 4 else dict(id=key, machine='remote', agent='unknown', project='-')
            record = runtime_liveness.initialize(root, context)
            runner = BUNDLE + "\nraise SystemExit(runtime_liveness.run(*sys.argv[1:4]))"
            with (root / 'events.jsonl').open('ab', buffering=0) as log:
                process = subprocess.Popen(['python3', '-c', runner, str(root), record['generation'], command],
                    stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            save(dict(pid=process.pid, boot=boot, start=identity(process.pid), state='RUNNING'))
        print(meta_path.read_text())
    elif mode == 'report':
        if not meta_path.exists():
            print(json.dumps(dict(status='UNKNOWN', absent=True)))
        else:
            meta = json.loads(meta_path.read_text())
            if (root / 'exit_code').exists():
                code = int((root / 'exit_code').read_text())
                status = 'DONE' if code == 0 else 'FAILED'
            else:
                code = None
                if meta['boot'] != boot:
                    status = 'FAILED'
                else:
                    status = 'UNKNOWN' if not meta['pid'] else 'FAILED'
                    start = identity(meta['pid']) if meta['pid'] else None
                    if start:
                        if start == meta['start']:
                            status = 'RUNNING'
                    elif meta['pid'] and not pathlib.Path('/proc/%s' % meta['pid']).exists():
                        # Only a gone leader can leave children of the original worker.
                        for proc in pathlib.Path('/proc').iterdir():
                            if not proc.name.isdigit(): continue
                            try:
                                fields = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
                                if fields[0] != 'Z' and fields[2] == str(meta['pid']) and fields[3] == str(meta['pid']):
                                    status = 'RUNNING'; break
                            except (OSError, IndexError): pass
            extra = {}
            if (root / 'liveness.json').exists():
                record = runtime_liveness.validate(runtime_liveness.read_record(root))
                context = dict(id=record['task_id'], machine=record['machine'], agent=record['agent'],
                    role=record['role'], turns=record.get('turn', 1), runtime_generation=record['generation'],
                    pid=meta['pid'], process_identity=dict(boot=meta['boot'], start=meta['start']))
                health = runtime_liveness.watchdog(root, context)
                status, code = runtime_liveness.task_status(health)
                import worker_runtime
                if os.environ.get('DEVORCHESTRATOR_BLOCK_EXTERNAL_EFFECTS') == '1':
                    cleanup = 'ACTIVE'
                else:
                    cleanup = worker_runtime.cleanup_group(root, context, status)
                extra = dict(liveness=record, runtime_health=health, cleanup_state=cleanup)
            # Observe exit before reading logs. A process that exits while this
            # chunk is being read must not make an incomplete chunk final.
            offset = int(sys.argv[3])
            path = root / 'events.jsonl'
            chunk = b''; more = False
            if path.exists():
                with path.open('rb') as log:
                    log.seek(offset); chunk = log.read(524288); more = bool(log.read(1))
            print(json.dumps(dict(status=status, exit_code=code, chunk=base64.b64encode(chunk).decode(), more=more, **extra)))
    else:
        raise ValueError('Unknown remote operation')
'''
# Available to the detached runner too; Python -c does not inherit modules.
SCRIPT = SCRIPT.replace('runner = BUNDLE +', 'runner = ' + repr(BUNDLE) + ' +')


def rpc(machine, mode, key, value, context=None):
    __import__('admission_policy').require_operation('remote')
    from external_effects import refuse
    refuse('worker')
    import devctl
    args = ['python3', '-c', SCRIPT, mode, key, str(value)]
    if context is not None:
        args.append(json.dumps(context))
    result = devctl.capture(machine, shlex.join(args), separate_stderr=True)
    if result.returncode:
        raise RuntimeError('Registered remote worker is unavailable')
    return json.loads(result.stdout)


def start(machine, key, command, context=None):
    from external_effects import refuse
    refuse('worker')
    if context is not None:
        context = {k: context[k] for k in ('id', 'machine', 'agent', 'project', 'session_id', 'role', 'runtime_generation', 'turns') if k in context}
    return rpc(machine, 'start', key, command, context)


def cached_refresh(meta, directory):
    """Share local read-only diagnosis; cached status is never authority."""
    from devctl import stored_task_status
    return stored_task_status(meta, directory)


def refresh(meta, directory):
    from external_effects import blocked
    if blocked():
        return cached_refresh(meta, directory)
    from durable import exclusive, read_json
    with exclusive(directory / 'remote-refresh.lock', blocking=True):
        meta.update(read_json(directory / 'meta.json'))
        meta.pop('_runtime_health', None)
        return _refresh(meta, directory)


def _refresh(meta, directory):
    from external_effects import blocked
    if blocked():
        return cached_refresh(meta, directory)
    from durable import exclusive, read_json
    key = meta['remote_key']
    log = directory / 'events.jsonl'
    offset = meta.get("remote_offset", 0)
    try:
        result = rpc(meta['machine'], 'report', key, offset)
        record = result.get('liveness')
        if record is None and meta.get('runtime_generation'):
            raise ValueError('Worker did not return runtime evidence')
        if record is not None:
            runtime_liveness.validate(record, meta)
            health = result.get('runtime_health')
            if (not isinstance(health, dict) or health.get('state') not in runtime_liveness.STATES or
                    health.get('generation') != record['generation'] or health.get('task_id') != record['task_id'] or
                    not runtime_liveness._number(health.get('heartbeat_age_seconds')) or
                    not runtime_liveness._number(health.get('observed_at')) or
                    any(type(health.get(k)) not in (bool, type(None)) for k in ('worker_alive', 'process_alive'))):
                raise ValueError('Invalid remote runtime evidence')
            with exclusive(directory / 'liveness.lock', blocking=True):
                previous = runtime_liveness.read_record(directory)
                if (directory / 'liveness.json').exists():
                    runtime_liveness.validate(previous, meta)
                previous = previous or {}
                if (previous.get('generation') == record['generation'] and
                        previous.get('heartbeat_sequence', 0) > record['heartbeat_sequence']):
                    raise ValueError('Worker heartbeat sequence regressed')
                if (previous.get('generation') == record['generation'] and
                        previous.get('terminal_reason') == 'process_exited_without_final_report'):
                    record['terminal_reason'] = previous['terminal_reason']
                if previous.get('generation') == record['generation'] and not record.get('session_id'):
                    record['session_id'] = previous.get('session_id')
                write_json(directory / 'liveness.json', record)
            # Recalculate from the validated source record and ownership probe,
            # never from remote_status or an external provider label.
            health = runtime_liveness.watchdog(directory, meta, probe=False,
                evidence=(health['worker_alive'], health['process_alive']),
                clock=health['observed_at'])
            result['status'], result['exit_code'] = runtime_liveness.task_status(health)
            meta['_runtime_health'] = health
            if result.get('cleanup_state') in ('ABSENT', 'CLEANED', 'AMBIGUOUS', 'ACTIVE'):
                meta['remote_cleanup_state'] = result['cleanup_state']
                meta['remote_cleanup_generation'] = record['generation']
        local_size = meta.get("remote_local_size", log.stat().st_size if log.exists() else 0)
        if log.exists() and log.stat().st_size > local_size:
            with log.open("r+b") as stream:
                stream.truncate(local_size)
        chunk = base64.b64decode(result.get('chunk', ''))
        if chunk:
            import devctl
            meta["remote_local_size"] = local_size
            devctl.save_meta(directory, meta)
            with log.open('ab') as stream:
                stream.write(chunk)
                stream.flush()
                import os
                os.fsync(stream.fileno())
            meta["remote_offset"] = offset + len(chunk)
            meta["remote_local_size"] = log.stat().st_size
            import devctl
            devctl.save_meta(directory, meta)
        status = result['status']
        # Don't expose incomplete final logs to AutoPilot/review.
        if result.get('more') and status in ('DONE', 'FAILED'):
            meta['remote_logs_pending'] = True
            meta['remote_status'] = 'RUNNING'
            import devctl
            devctl.save_meta(directory, meta)
            return 'RUNNING', None
        code = result.get('exit_code')
        meta['remote_logs_pending'] = False
        if status in ('DONE', 'FAILED') and code is not None:
            (directory / 'exit_code').write_text(str(code))
        meta['remote_status'] = status
        import devctl
        devctl.save_meta(directory, meta)
        return status, code
    except (OSError, ValueError, RuntimeError, TypeError, KeyError):
        meta.pop('_runtime_health', None)
        if meta.get('runtime_generation'):
            meta['_runtime_health'] = runtime_liveness.watchdog(directory, meta, probe=False)
        return 'UNKNOWN', None

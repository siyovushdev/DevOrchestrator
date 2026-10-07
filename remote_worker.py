"""Detached SSH worker with durable remote logs/status; transport loss is not task failure."""
import base64
import json
import shlex

# Self-contained standard-library script, sent only to a registered machine.
SCRIPT = r'''
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
root.mkdir(parents=True, exist_ok=True, mode=0o700)
# Concurrent/replayed SSH invocations must share a single launch receipt.
import fcntl
with (root / 'lock').open('a') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    if mode == 'start':
        if not meta_path.exists():
            save(dict(pid=0, boot=boot, start=None, state='STARTING'))
            command = sys.argv[3]
            runner = "import pathlib,subprocess,sys; p=pathlib.Path(sys.argv[1]); rc=subprocess.call(['bash','-c',sys.argv[2]]); (p/'exit.tmp').write_text(str(rc)); (p/'exit.tmp').replace(p/'exit_code')"
            with (root / 'events.jsonl').open('ab', buffering=0) as log:
                process = subprocess.Popen(['python3', '-c', runner, str(root), command],
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
            offset = int(sys.argv[3])
            path = root / 'events.jsonl'
            chunk = b''; more = False
            if path.exists():
                with path.open('rb') as log:
                    log.seek(offset); chunk = log.read(524288); more = bool(log.read(1))
            print(json.dumps(dict(status=status, exit_code=code, chunk=base64.b64encode(chunk).decode(), more=more)))
    else:
        raise ValueError('Unknown remote operation')
'''


def rpc(machine, mode, key, value):
    import devctl
    result = devctl.capture(machine, shlex.join(['python3', '-c', SCRIPT, mode, key, str(value)]), separate_stderr=True)
    if result.returncode:
        raise RuntimeError('Registered remote worker is unavailable')
    return json.loads(result.stdout)


def start(machine, key, command):
    return rpc(machine, 'start', key, command)


def refresh(meta, directory):
    from durable import exclusive, read_json
    with exclusive(directory / 'remote-refresh.lock', blocking=True):
        meta.update(read_json(directory / 'meta.json'))
        return _refresh(meta, directory)


def _refresh(meta, directory):
    key = meta['remote_key']
    log = directory / 'events.jsonl'
    offset = meta.get("remote_offset", 0)
    try:
        result = rpc(meta['machine'], 'report', key, offset)
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
            meta['remote_status'] = 'RUNNING'
            import devctl
            devctl.save_meta(directory, meta)
            return 'RUNNING', None
        code = result.get('exit_code')
        if status in ('DONE', 'FAILED') and code is not None:
            (directory / 'exit_code').write_text(str(code))
        meta['remote_status'] = status
        import devctl
        devctl.save_meta(directory, meta)
        return status, code
    except (OSError, ValueError, RuntimeError):
        return 'UNKNOWN', None

"""Finite local supervisor; generation-bound native liveness and pidfd cleanup."""
import os
from pathlib import Path
import shlex
import sys
import time
from contextlib import contextmanager
sys.path.insert(0,str(Path(__file__).resolve().parent))
from durable import exclusive, read_json, write_json
from native_process import run as bounded, identity
from native_deadlines import deadline, stop, LIMITS, clock
import runtime_liveness as native
import worker_runtime

@contextmanager
def bounded_lock(path,blocking=True,until=None):
    """Finite native liveness-lock waits, private supervisor process only."""
    import fcntl
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a') as stream:
        seconds=LIMITS['close'] if blocking else 0
        if until is not None:seconds=min(seconds,max(0,until-clock()))
        end=time.monotonic()+seconds
        while True:
            try:fcntl.flock(stream,fcntl.LOCK_EX|fcntl.LOCK_NB);break
            except BlockingIOError:
                if time.monotonic()>=end:raise TimeoutError('Private liveness lock deadline')
                time.sleep(.02)
        try:yield
        finally:fcntl.flock(stream,fcntl.LOCK_UN)

def terminal_cleanup(directory,meta,state):
    """Private read-only cleanup proof; never invoke native group signals."""
    directory=Path(directory)
    if state not in ('DONE','FAILED') or meta.get('launch_prepared') or meta.get('resume_prepared'):return 'ACTIVE'
    try:
        result=read_json(directory/'native-watchdog/bounded-result.json')
        receipt=read_json(directory/'process_group.json')
        native.validate(native.read_record(directory),meta)
        if not result or not result.get('cleanup_proven') or result['state']=='UNKNOWN':return 'AMBIGUOUS'
        for key,value in [('task_id',meta['id']),('generation',meta['runtime_generation']),
                          ('role',meta['role']),('agent',meta['agent']),('machine',meta['machine']),
                          ('turn',meta.get('turns',1))]:
            if receipt.get(key)!=value:return 'AMBIGUOUS'
        record=native.read_record(directory)
        if record['phase']!='EXITED' or record['exit_code'] is None or int((directory/'exit_code').read_text())!=record['exit_code']:
            return 'AMBIGUOUS'
        journal=read_json(directory/'native-watchdog/processes.json');members=journal['members']
        if journal.get('schema_version')!=2 or (journal.get('ownership_unknown') is not False
                or not members or any(row.get('exit_proven_by_pidfd') is not True for row in members)):
            return 'AMBIGUOUS'
        # Include the still-finishing native supervisor as an explicit anchor.
        from native_process import same,same_process
        for row in members:
            current=identity(row['pid'])
            comparable=same_process(row,current) if row.get('exit_proven_by_pidfd') else same(row,current)
            if current and not comparable:return 'AMBIGUOUS'
            if current and current['state'] not in ('Z','X'):return 'ACTIVE'
        current=identity(receipt['wrapper_pid'])
        if current:
            if {'boot':current['boot'],'start':current['start']}!=receipt['wrapper_identity']:return 'AMBIGUOUS'
            if current['state'] not in ('Z','X'):return 'ACTIVE'
        return 'ABSENT'
    except (OSError,ValueError,KeyError,TypeError):return 'AMBIGUOUS'

def finish(directory,generation,code):
    record=native.read_record(directory)
    native.heartbeat(directory,generation,worker_pid=os.getpid(),agent_pid=record['agent_pid'],
        phase='EXITED',exit_code=code,lock=bounded_lock,record_process_group=False)
    with bounded_lock(directory/'liveness.lock',blocking=True):
        if native.read_record(directory)['generation']!=generation:raise ValueError('Generation changed')
        path=directory/'exit_code.tmp'
        with path.open('w') as stream:
            stream.write(str(code));stream.flush();os.fsync(stream.fileno())
        path.replace(directory/'exit_code')
        fd=os.open(directory,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)

def supervise(directory,generation,spec_path):
    directory=Path(directory).resolve();spec_path=Path(spec_path).resolve()
    meta=read_json(directory/'meta.json');prepared=meta['native_codex']
    from native_codex import sha
    if str(spec_path)!=prepared['spec'] or sha(spec_path)!=prepared['spec_sha256']:
        raise ValueError('Prepared argv identity mismatch')
    spec=read_json(spec_path);run=Path(spec['flow'])
    if sha(spec['argv'][0])!=spec['binary_sha256']:
        raise ValueError('Prepared executable digest changed')
    record=native.validate(native.read_record(directory),meta)
    if record['generation']!=generation or record['role']!=spec['role'] or (run/'STOP_ADMISSION').exists():
        raise ValueError('Generation/role/admission mismatch')
    from admission_policy import supervisor
    admission_deadline=supervisor(spec['admission'],generation)
    owner=identity(os.getpid())
    if owner['pgid']!=os.getpid() or owner['sid']!=os.getpid():raise ValueError('Fresh supervisor session required')
    write_json(directory/'process_group.json',dict(task_id=record['task_id'],generation=generation,
        turn=record.get('turn',1),role=record['role'],agent=record['agent'],machine=record['machine'],
        wrapper_pid=os.getpid(),wrapper_identity={'boot':owner['boot'],'start':owner['start']},
        pgid=os.getpid(),sid=os.getpid(),members=[],groups=[]))
    role_deadline=min(spec['deadline'],deadline(run,record['role']),admission_deadline);last=[0.]
    def authorize_exec():
        from admission_policy import require_running, emergency, validation_budget
        try:
            emergency()
            with validation_budget(role_deadline):require_running(spec['admission'],generation,admission_deadline)
        except __import__('native_process').DeadlineReached:
            raise
        except Exception as exc:
            raise ValueError('Admission/emergency boundary revoked: '+type(exc).__name__) from exc
    def tick(pid):
        authorize_exec()
        if time.monotonic()-last[0]>=min(native.HEARTBEAT_SECONDS,1):
            native.heartbeat(directory,generation,worker_pid=os.getpid(),agent_pid=pid,
                phase='REVIEW' if record['role']=='review' else 'CODING',
                lock=lambda path,blocking=True:bounded_lock(path,blocking,until=role_deadline),
                record_process_group=False)
            last[0]=time.monotonic()
    result=bounded(spec['argv'],directory/'native-watchdog',deadline=role_deadline,env=spec['env'],tick=tick,before_exec=authorize_exec)
    from admission_policy import terminal
    try:terminal(spec['admission'],generation,result['cleanup_proven'] and result['state']!='UNKNOWN')
    except (OSError,ValueError,KeyError,TimeoutError) as exc:
        result={**result,'state':'UNKNOWN','returncode':None,'reason':'Admission terminal identity changed: '+type(exc).__name__}
        write_json(directory/'native-watchdog/bounded-result.json',result)
    if result['state']=='UNKNOWN':
        stop(run,'UNKNOWN worker ownership/cleanup; owner forward repair required')
        write_json(directory/'native-timeout.json',{**result,'generation':generation,'state':'UNKNOWN'})
        return 125
    if result['state']=='TIMEOUT':
        stop(run,'Worker timeout; no automatic retry')
        write_json(directory/'native-timeout.json',{**result,'generation':generation});code=124
    else:code=result['returncode']
    try:finish(directory,generation,code)
    except (OSError,RuntimeError,ValueError,TimeoutError) as exc:
        ambiguous={**result,'state':'UNKNOWN','observed_exit_code':result['returncode'],
            'returncode':None,'reason':'Native terminal commit incomplete: '+type(exc).__name__}
        write_json(directory/'native-watchdog/bounded-result.json',ambiguous)
        write_json(directory/'native-timeout.json',{**ambiguous,'generation':generation})
        stop(run,'UNKNOWN terminal commit; no retry');return 125
    return code

if __name__=='__main__':raise SystemExit(supervise(*sys.argv[1:4]))

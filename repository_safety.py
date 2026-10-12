"""Bounded local repository observation; ambient Git execution is never trusted."""
import hashlib
import os
from pathlib import Path
import stat
import time

MAX_ENTRIES = 2048
MAX_FILE = 8 * 1024 * 1024
MAX_TOTAL = 32 * 1024 * 1024
CHUNK = 64 * 1024

def clock():
    return time.clock_gettime(time.CLOCK_BOOTTIME)

def check(until):
    if clock() >= until:
        raise ValueError('Repository preparation deadline exhausted')

def entries(root, until):
    root=Path(root);pending=[root];rows=[];total=0;count=0
    while pending:
        check(until)
        with os.scandir(pending.pop()) as scan:
            for entry in scan:
                check(until);count+=1
                if count>MAX_ENTRIES:raise ValueError('Repository entry limit exceeded')
                s=entry.stat(follow_symlinks=False)
                if entry.is_symlink() or not (stat.S_ISREG(s.st_mode) or stat.S_ISDIR(s.st_mode)):
                    raise ValueError('Repository link or special file rejected')
                if s.st_uid!=os.getuid():raise ValueError('Repository ownership changed')
                if stat.S_ISDIR(s.st_mode):pending.append(Path(entry.path));continue
                if s.st_nlink!=1 or s.st_size>MAX_FILE:raise ValueError('Repository file bound/link rejected')
                total+=s.st_size
                if total>MAX_TOTAL:raise ValueError('Repository aggregate limit exceeded')
                rows.append((Path(entry.path),s))
    return rows

def stream(path, expected, until, target=None):
    check(until);fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        s=os.fstat(fd)
        identity=lambda x:(x.st_dev,x.st_ino,x.st_size,x.st_mtime_ns,x.st_ctime_ns)
        if not stat.S_ISREG(s.st_mode) or identity(s)!=identity(expected):raise ValueError('Repository file identity changed')
        h=hashlib.sha256();count=0
        while True:
            check(until);data=os.read(fd,CHUNK)
            if not data:break
            count+=len(data)
            if count>expected.st_size or count>MAX_FILE:raise ValueError('Repository file grew during preparation')
            h.update(data)
            if target is not None:target.write(data)
        check(until)
        if count!=s.st_size or identity(os.fstat(fd))!=identity(expected):raise ValueError('Repository file changed during preparation')
        return h.hexdigest()
    finally:os.close(fd)

def manifest(root, until):
    return {str(p.relative_to(root)):stream(p,s,until) for p,s in entries(root,until)}

# Self-contained trusted preamble for observations executed on the registered
# host. No imported workspace code, shell helpers, inherited config/environment,
# textconv, filters, hooks, fsmonitor or external diff commands are permitted.
GIT_PREAMBLE = r'''
import os, signal, subprocess, tempfile, time
_git_end=time.clock_gettime(time.CLOCK_BOOTTIME)+30
_git_env={'PATH':'/usr/bin:/bin','HOME':'/nonexistent','LANG':'C.UTF-8',
          'GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null','GIT_TERMINAL_PROMPT':'0'}
def safe_git(workspace, args, input=None, optional=False, env=None):
    left=_git_end-time.clock_gettime(time.CLOCK_BOOTTIME)
    if left<=0:raise ValueError('Repository observation deadline exhausted')
    child_env=dict(_git_env)
    if env is not None:
        # The only per-command setting is a controller-created temporary index.
        if 'GIT_INDEX_FILE' in env:child_env['GIT_INDEX_FILE']=env['GIT_INDEX_FILE']
    argv=['/usr/bin/git','-c','core.hooksPath=/dev/null','-c','core.fsmonitor=false',
          '-c','core.untrackedCache=false','-C',workspace,'--literal-pathspecs',*args]
    with tempfile.TemporaryFile() as output:
        p=subprocess.Popen(argv,env=child_env,stdin=subprocess.PIPE if input is not None else subprocess.DEVNULL,
                           stdout=output,stderr=subprocess.DEVNULL,close_fds=True)
        # An unreaped direct child cannot be PID-reused; pin it before any wait.
        fd=os.pidfd_open(p.pid)
        try:
            try:p.communicate(input=input,timeout=left)
            except subprocess.TimeoutExpired:
                try:signal.pidfd_send_signal(fd,signal.SIGTERM)
                except ProcessLookupError:pass
                try:p.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    try:signal.pidfd_send_signal(fd,signal.SIGKILL)
                    except ProcessLookupError:pass
                    p.wait(timeout=1)
                raise ValueError('Repository observation timed out') from None
            if p.returncode and not optional:raise RuntimeError('Repository observation failed')
            if p.returncode:return b''
            if output.tell()>8*1024*1024:raise ValueError('Repository observation output limit exceeded')
            output.seek(0);return output.read(8*1024*1024+1)
        finally:os.close(fd)
'''

GIT_PREAMBLE += r'''
def assert_git_config(workspace):
    # Legacy observational identity may describe a non-Git directory. It can
    # never pass admission, but remains resolvable without executing helpers.
    if not os.path.exists(os.path.join(workspace,'.git')):return
    raw=safe_git(workspace,['config','--local','--no-includes','--list']).decode()
    for line in raw.splitlines():
        key=line.split('=',1)[0].lower()
        if (key.startswith(('filter.','include.','includeif.','credential.')) or
            key in ('core.sshcommand','core.fsmonitor','diff.external') or
            (key.startswith('diff.') and key.endswith(('.command','.textconv')))):
            raise ValueError('Executable repository configuration rejected')
def bounded_tree(workspace):
    import stat
    pending=[workspace];count=0;total=0
    while pending:
        if time.clock_gettime(time.CLOCK_BOOTTIME)>=_git_end:raise ValueError('Repository observation deadline exhausted')
        with os.scandir(pending.pop()) as scan:
            for entry in scan:
                if time.clock_gettime(time.CLOCK_BOOTTIME)>=_git_end:raise ValueError('Repository observation deadline exhausted')
                count+=1;s=entry.stat(follow_symlinks=False)
                if count>2048:raise ValueError('Repository entry limit exceeded')
                if entry.is_symlink() or not (stat.S_ISREG(s.st_mode) or stat.S_ISDIR(s.st_mode)):
                    raise ValueError('Repository link/special file rejected')
                if stat.S_ISDIR(s.st_mode):pending.append(entry.path)
                else:
                    total+=s.st_size
                    if s.st_size>8*1024*1024 or total>32*1024*1024:raise ValueError('Repository byte limit exceeded')
'''

def until():
    """Preparation shares a consumed/reserved workflow's existing deadline."""
    end=clock()+30
    import admission_policy as a
    current=a.VALIDATION_UNTIL.get()
    if current is not None:end=min(end,current)
    ticket=a.ACTIVE.get()
    if ticket is not None:
        _,g,c=a.validate(ticket['arguments'])
        end=min(end,clock()+g['expires_at']-time.time()-12)
        key=ticket['arguments']['request_id']
        claim=a.read_json(Path(c['state_dir'])/'admissions'/key/'claim.json',{})
        if claim:
            from native_deadlines import boot
            a.need(claim['ticket']==ticket and claim['boot']==boot(),'Preparation claim identity changed')
            end=min(end,claim['work_deadline'])
    check(end);return end

def bounded_script(script, end=None):
    # Derived numeric literal, never a workspace/prompt value or environment code.
    end=until() if end is None else end
    check(end)
    return script.replace('_git_end=time.clock_gettime(time.CLOCK_BOOTTIME)+30',
        '_git_end=min(time.clock_gettime(time.CLOCK_BOOTTIME)+30,'+repr(float(end))+')',1)

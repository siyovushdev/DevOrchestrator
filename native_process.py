"""Linux pidfd watchdog, exclusively for a newly created local worker cohort.

Never enumerates /proc, searches names, signals groups, or adopts stored PIDs
after restart. Every signal targets a pinned pidfd and a rechecked identity.
The caller is a dedicated trusted process, with no other live children.
"""
import ctypes
import json
import os
from pathlib import Path
import signal
import select
import subprocess
import sys
import time

sys.path.insert(0,str(Path(__file__).resolve().parent))
from durable import read_json, write_json
from native_deadlines import boot, clock, LIMITS

class DeadlineReached(ValueError):
    """A trusted callback exhausted the same monotonic execution deadline."""

def identity(pid):
    try:
        p=Path('/proc')/str(pid)
        fields=(p/'stat').read_text().rsplit(')',1)[1].split()
        return {'pid':pid,'boot':boot(),'start':fields[19],
                'pgid':int(fields[2]),'sid':int(fields[3]),'uid':p.stat().st_uid,
                'ppid':int(fields[1]),'state':fields[0]}
    except (FileNotFoundError,ProcessLookupError):return None

def same(a,b):
    return bool(a and b and all(a[k]==b[k] for k in ('pid','boot','start','pgid','sid','uid')))

def same_process(a,b):
    """Kernel task identity; only live pidfd cohorts may accept group changes."""
    return bool(a and b and all(a[k]==b[k] for k in ('pid','boot','start','uid')))

def pidfd_exited(fd):
    poll=select.poll();poll.register(fd,select.POLLIN)
    ready=poll.poll(0)
    if any(flags & (select.POLLERR|select.POLLNVAL) for _,flags in ready):
        raise ValueError('UNKNOWN: invalid pidfd; no ownership proof')
    return any(flags & (select.POLLIN|select.POLLHUP) for _,flags in ready)

def children(pid):
    # Include children forked by any thread of this known process. No global scan.
    result=set()
    try:tasks=list((Path('/proc')/str(pid)/'task').iterdir())
    except FileNotFoundError:return result
    for task in tasks:
        try:result.update(map(int,(task/'children').read_text().split()))
        except FileNotFoundError:pass
    return result

def require_support():
    if not hasattr(os,'pidfd_open') or not hasattr(signal,'pidfd_send_signal'):
        raise ValueError('Linux pidfd required; no numeric-PID/group signal fallback')

class Cohort:
    def __init__(self,root,journal):
        self.root=root;self.journal=Path(journal);self.members={};self.fds={};self.actions=[]
        self.errors=[];self.transitions=[];self.ownership_unknown=False;self.owner=identity(os.getpid())
        self.pin(root,None)
    def pin(self,pid,parent):
        first=identity(pid)
        if not first:return
        if first['uid']!=os.getuid():raise ValueError('UNKNOWN: foreign UID')
        if parent is None and first['ppid']!=os.getpid():raise ValueError('UNKNOWN: root is not our newly spawned child')
        lineage={'discovered_parent':parent,'verified_parent':first['ppid'],'proof':'DIRECT_KNOWN_PARENT'}
        if parent is not None and first['ppid']!=parent:
            # The discovered parent can exit before pin. Re-prove adoption
            # through the dedicated subreaper or another already pinned node;
            # PPID alone and a process/group name never authorize ownership.
            adopted=first['ppid']
            known_parent=adopted==os.getpid() or (adopted in self.members and self.observe(adopted,'ADOPTION') is not None)
            if not known_parent or pid not in children(adopted):
                raise ValueError('UNKNOWN: descendant lineage changed; pid='+str(pid))
            lineage.update(proof='ADOPTION_REPROVED_BY_KNOWN_PARENT_CHILDREN')
        try:fd=os.pidfd_open(pid)
        except ProcessLookupError:return  # Proven exited/reaped before pin; never signal it.
        try:
            if pidfd_exited(fd):return  # Fast descendant already ended; no live admission.
            current=identity(pid)
            if pidfd_exited(fd):return
            if not same_process(first,current):
                raise ValueError('UNKNOWN: identity changed while pinning; pid='+str(pid))
            self.members[pid]={**first,'parent_proof':parent,'lineage':lineage,'owner':self.owner,'exit_proven_by_pidfd':False}
            self.fds[pid]=fd;fd=None
            self._update(pid,current,'PIN')
            self.save()
        finally:
            if fd is not None:os.close(fd)
    def _update(self,pid,current,stage):
        row=self.members[pid]
        changes={key:[row[key],current[key]] for key in ('pgid','sid','ppid') if row[key]!=current[key]}
        if changes:
            self.transitions.append({'pid':pid,'stage':stage,'changes':changes,
                'stable_identity':{key:row[key] for key in ('pid','boot','start','uid')},
                'proof':'LIVE_PINNED_PIDFD_AND_STABLE_KERNEL_IDENTITY'})
            row.update({key:current[key] for key in changes});self.save()
    def observe(self,pid,stage):
        if self.ownership_unknown:raise ValueError('UNKNOWN: cohort ownership already fenced')
        row=self.members[pid];fd=self.fds[pid]
        if pidfd_exited(fd):
            if not row['exit_proven_by_pidfd']:
                row['exit_proven_by_pidfd']=True;self.save()
            return None  # No numeric PID read or signal after kernel exit proof.
        current=identity(pid)
        if pidfd_exited(fd):
            row['exit_proven_by_pidfd']=True;self.save();return None
        if not same_process(row,current):
            self.ownership_unknown=True
            self.errors.append({'property':'pid/boot/start/uid','pid':pid,
                'expected':{key:row[key] for key in ('pid','boot','start','uid')},
                'actual':{key:current.get(key) for key in ('pid','boot','start','uid')} if current else None,
                'reason':'LIVE_PIDFD_KERNEL_IDENTITY_MISMATCH','stage':stage})
            self.save();raise ValueError('UNKNOWN: process identity mismatch; pid='+str(pid))
        self._update(pid,current,stage)
        return current
    def save(self):
        write_json(self.journal,{'schema_version':2,'root':self.root,'members':list(self.members.values()),
            'actions':self.actions,'errors':self.errors,'transitions':self.transitions,
            'ownership_unknown':self.ownership_unknown,'automatic_retry':False})
    def discover(self):
        if self.ownership_unknown:return False
        try:
            # The dedicated caller was child-free before launch and is a
            # subreaper: direct adopted children originate in this one cohort.
            pending=[]
            def descendants(parent):
                for pid in children(parent):
                    if pid not in self.members:pending.append((parent,pid))
                    elif self.members[pid]['exit_proven_by_pidfd']:
                        # An old zombie can remain our child. A new live child
                        # reusing its numeric PID cannot be silently skipped.
                        current=identity(pid)
                        if current and not same_process(self.members[pid],current):
                            raise ValueError('UNKNOWN: reused child PID; pid='+str(pid))
            descendants(os.getpid())
            for pid,row in list(self.members.items()):
                current=self.observe(pid,'DISCOVER')
                if current and current['state'] not in ('Z','X'):
                    descendants(pid)
            for parent,pid in pending:
                if pid not in self.members:self.pin(pid,parent)
            return True
        except (OSError,ValueError,IndexError) as exc:
            self.ownership_unknown=True
            self.errors.append({'property':'pidfd_or_lineage_metadata','expected':'proven own child/task',
                'actual':type(exc).__name__,'reason':str(exc),'stage':'DISCOVER'})
            self.save();return False
    def active(self):
        alive=[]
        for pid,row in self.members.items():
            current=self.observe(pid,'ACTIVE')
            if current:alive.append(pid)  # pidfd, not a /proc absence/Z guess, proves exit.
        return alive
    def send(self,pid,sig):
        row=self.members[pid];current=self.observe(pid,'SIGNAL')
        if not current or current['state'] in ('Z','X'):return
        # No kill/killpg fallback. An exiting/reused numeric PID cannot redirect
        # this signal because the descriptor pins the original kernel task.
        try:signal.pidfd_send_signal(self.fds[pid],sig)
        except ProcessLookupError:return
        self.actions.append({'pid':pid,'identity':{k:row[k] for k in ('boot','start','uid','pgid','sid')},
            'signal':sig.name,'verified':True,'proof':'PINNED_PIDFD_STABLE_KERNEL_IDENTITY_INITIAL_LINEAGE'});self.save()
    def quiet(self):
        # All pinned tasks ended, then rescan the subreaper after their final
        # forks/reparenting. No producer remains to create an unobserved child.
        return self.discover() and not self.active() and self.discover() and not self.active()
    def cleanup(self,term,kill):
        # Re-scan during grace to find forks and subreaper-adopted descendants.
        # Revalidate *all* known identities before each signal pass.
        try:
            for sig,grace in ((signal.SIGTERM,term),(signal.SIGKILL,kill)):
                end=clock()+grace;sent=set()
                while True:
                    if not self.discover():return False
                    active=self.active()
                    if not active:
                        if self.quiet():return True
                        if self.ownership_unknown:return False
                        active=self.active()
                    for pid in reversed(active):
                        if pid not in sent:self.send(pid,sig);sent.add(pid)
                    if clock()>=end:break
                    time.sleep(.02)
            return self.quiet()
        except (OSError,ValueError) as exc:
            self.ownership_unknown=True
            self.errors.append({'property':'cleanup_ownership','expected':'verified pidfd cohort',
                'actual':type(exc).__name__,'reason':str(exc),'stage':'CLEANUP'})
            self.save();return False
    def close(self):
        for fd in self.fds.values():os.close(fd)

def run(argv,folder,*,deadline,env=None,stdout=None,stderr=None,cwd=None,
        term_grace=None,kill_grace=None,tick=None,before_exec=None):
    require_support();folder=Path(folder);folder.mkdir(parents=True,exist_ok=True)
    journal=folder/'processes.json';result_path=folder/'bounded-result.json'
    if journal.exists() or result_path.exists():raise ValueError('Durable process fence exists; no automatic retry')
    if deadline<=clock():raise ValueError('Deadline expired before launch')
    # Dedicated caller only. Exited direct children can be reaped without signals.
    for pid in children(os.getpid()):
        row=identity(pid)
        if row and row['state'] not in ('Z','X'):raise ValueError('Caller has unrelated live children; no launch')
        try:os.waitpid(pid,os.WNOHANG)
        except ChildProcessError:pass
    libc=ctypes.CDLL(None,use_errno=True)
    if libc.prctl(36,1,0,0,0)!=0:raise OSError(ctypes.get_errno(),'PR_SET_CHILD_SUBREAPER failed')
    write_json(folder/'launch.json',{'state':'PENDING_IDENTITY','boot':boot(),'deadline':deadline,
        'automatic_retry':False})
    gate=folder/'commit.json'
    # Child blocks before exec until identity + cohort are fsynced. No argv/env
    # contents are written to lifecycle evidence; the launch spec is private.
    spec=folder/'exec.json';write_json(spec,{'argv':argv,'env':env,'cwd':str(cwd) if cwd else None,'deadline':deadline})
    try:
        child=subprocess.Popen([sys.executable,'-I','-B',__file__,'--gate',str(folder)],
            stdin=subprocess.DEVNULL,stdout=stdout,stderr=stderr,close_fds=True,env=env)
    except OSError as exc:
        result={'state':'UNKNOWN','reason':'Launch interrupted before identity commit: '+type(exc).__name__,
            'returncode':None,'cleanup_proven':False,'automatic_retry':False}
        write_json(result_path,result);return result
    cohort=None;timed_out=False;known=False;code=None;reason='';ambiguous=False
    try:
        cohort=Cohort(child.pid,journal)
        if child.pid not in cohort.members:raise ValueError('UNKNOWN: launch identity unavailable')
        if before_exec:before_exec()
        write_json(gate,{'identity':cohort.members[child.pid]})
        if tick:tick(child.pid)
        while True:
            if not cohort.discover():reason='OWNERSHIP_UNKNOWN';ambiguous=True;break
            code=child.poll()
            if code is not None:
                known=cohort.quiet();reason='PROCESS_EXITED' if known else 'LINGERING_DESCENDANTS';break
            if clock()>=deadline:timed_out=True;reason='DEADLINE';break
            if tick:tick(child.pid)
            time.sleep(.02)
        if not known:
            known=cohort.cleanup(LIMITS['term_grace'] if term_grace is None else term_grace,
                                 LIMITS['kill_grace'] if kill_grace is None else kill_grace)
            if known:code=child.wait(timeout=.2)
            if known:
                for pid in cohort.members:
                    if pid!=child.pid:
                        try:os.waitpid(pid,os.WNOHANG)
                        except ChildProcessError:pass
    except (OSError,ValueError,subprocess.TimeoutExpired) as exc:
        timed_out=isinstance(exc,DeadlineReached)
        reason='DEADLINE' if timed_out else 'UNKNOWN: '+str(exc)
        ambiguous=not timed_out;known=False
        # A failed heartbeat/receipt callback must not escape the time bound.
        # Cleanup revalidates ownership independently. Identity/access errors
        # make it return False without guessed numeric-PID/group signals.
        if cohort:
            known=cohort.cleanup(LIMITS['term_grace'] if term_grace is None else term_grace,
                                 LIMITS['kill_grace'] if kill_grace is None else kill_grace)
            if known:
                try:code=child.wait(timeout=.2)
                except subprocess.TimeoutExpired:known=False
    finally:
        result={'state':('TIMEOUT' if timed_out else 'EXITED') if known and not ambiguous else 'UNKNOWN',
            'reason':reason,'returncode':code if known and not ambiguous else None,'cleanup_proven':known,
            'deadline':deadline,'finished':clock(),'automatic_retry':False,
            'signals':cohort.actions if cohort else []}
        if known and reason=='LINGERING_DESCENDANTS':result['state']='TIMEOUT'
        if ambiguous:result['observed_exit_code']=code
        write_json(result_path,result)
        if cohort:cohort.close()
    return result

def recover(folder):
    """Offline restart fence: retain ambiguity; never signal or re-launch."""
    folder=Path(folder)
    value=read_json(folder/'bounded-result.json')
    if value:return value
    if (folder/'launch.json').exists():
        value={'state':'UNKNOWN','reason':'CRASH_WITHOUT_DURABLE_COMPLETION',
               'returncode':None,'cleanup_proven':False,'automatic_retry':False}
        write_json(folder/'bounded-result.json',value);return value
    raise ValueError('No admitted launch to recover')

def gate_main(folder):
    folder=Path(folder);spec=read_json(folder/'exec.json')
    # Kernel parent-death protection for this newly spawned child only. It
    # cannot target a reused numeric PID or any unrelated process. If the
    # watchdog crashes, durable PENDING/committed evidence remains UNKNOWN.
    parent=os.getppid();libc=ctypes.CDLL(None,use_errno=True)
    if libc.prctl(1,signal.SIGKILL,0,0,0)!=0 or os.getppid()!=parent:return 125
    while clock()<spec['deadline']:
        committed=read_json(folder/'commit.json')
        if committed:
            if not same(committed['identity'],identity(os.getpid())):return 125
            if committed['identity']['owner']['pid']!=os.getppid():return 125
            if spec['cwd']:os.chdir(spec['cwd'])
            os.execve(spec['argv'][0],spec['argv'],spec['env'] if spec['env'] is not None else dict(os.environ))
        time.sleep(.02)
    return 124

if __name__=='__main__':
    if len(sys.argv)==3 and sys.argv[1]=='--gate':raise SystemExit(gate_main(sys.argv[2]))
    raise SystemExit('Internal trusted gate only')

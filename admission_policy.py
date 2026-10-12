"""Private, durable, exact two-role admission. Never an emergency-latch override.

The trusted controller UID installs owner-approved files; agents cannot access
this directory. No RPC, issue comment or worker can mint a grant. Permissions
authorize one initial coding request and one bound independent review only.
External delivery/publication remains denied independently of admission.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
import hashlib
import inspect
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import time

from durable import exclusive, read_json, write_json
from execution_profiles import PreflightRejected
from external_effects import ExternalEffectBlocked
from review_transitions import digest, request_id

ENV = 'DEVORCHESTRATOR_ADMISSION_POLICY'
ACTIVE = ContextVar('durable_admission', default=None)
VALIDATION_UNTIL = ContextVar('admission_validation_deadline', default=None)
HASH = re.compile(r'[a-f0-9]{64}')
MAXIMUM = dict(coding=480, review=480, overall=1200)

class AdmissionDenied(PreflightRejected):
    pass

def need(value, reason):
    if not value: raise AdmissionDenied(reason)

def emergency():
    from external_effects import blocked, ExternalEffectBlocked
    if blocked(): raise ExternalEffectBlocked('worker')

def private(path, directory=False):
    p = Path(path)
    need(p.is_absolute() and p.resolve() == p, 'Noncanonical policy resource')
    s = p.lstat()
    need(not stat.S_ISLNK(s.st_mode) and s.st_uid == os.getuid() and not s.st_mode & 0o077,
         'Policy resource must be private and owned')
    need(stat.S_ISDIR(s.st_mode) if directory else stat.S_ISREG(s.st_mode), 'Invalid policy resource type')
    return p

def strict(path):
    def pairs(rows):
        out = {}
        for k,v in rows:
            need(k not in out, 'Duplicate policy field');out[k] = v
        return out
    return read_json(private(path), object_pairs_hook=pairs)

def policy():
    name = os.environ.get(ENV)
    need(name, 'Admission default DENY: no explicit policy')
    p = private(name);c = strict(p)
    need(set(c) == {'schema_version','state_dir','grants_dir','workspace_root','registry_file'} and
         type(c['schema_version']) is int and c['schema_version'] == 1, 'Unsupported admission policy')
    for key in ('state_dir','grants_dir','workspace_root'):private(c[key],directory=True)
    private(c['registry_file'])
    work = Path(c['workspace_root'])
    for other in (p, Path(c['registry_file']), Path(c['state_dir']), Path(c['grants_dir'])):
        need(not other.is_relative_to(work) and not work.is_relative_to(other), 'Policy/state overlaps writable workspace')
    return p,c

@contextmanager
def validation_budget(until):
    token=VALIDATION_UNTIL.set(until)
    try:yield
    finally:VALIDATION_UNTIL.reset(token)

def repository(workspace, root):
    # Mutable coding trees are untrusted. Lazy traversal and all Git probes
    # share one budget, additionally clamped to the native role deadline.
    from native_deadlines import clock
    until=min(clock()+3,VALIDATION_UNTIL.get() or float('inf'))
    def remaining():
        now=clock()
        if VALIDATION_UNTIL.get() is not None and now>=VALIDATION_UNTIL.get():
            from native_process import DeadlineReached
            raise DeadlineReached('Native role deadline exhausted during repository validation')
        value=until-now
        need(value>0,'Repository validation budget exhausted; no execution')
        return value
    remaining()
    p = Path(workspace);root = Path(root)
    need(p.is_absolute() and p.resolve() == p and p != root and p.is_relative_to(root), 'Workspace escapes explicit root')
    need(p.is_dir() and p.stat().st_uid == os.getuid() and not p.stat().st_mode & 0o022, 'Workspace ownership/mode mismatch')
    # Refuse symlink ancestors even when they currently resolve inside the root.
    for a in (p,*p.parents):need(not a.is_symlink(), 'Symlink workspace ancestor')
    g = p/'.git'
    need(g.is_dir() and not g.is_symlink(), 'Standalone disposable repository required')
    need(not (g/'hooks').exists() or all(f.name.endswith('.sample') for f in (g/'hooks').iterdir()),'Active Git hooks rejected')
    from repository_safety import entries
    entries(p,until)  # Pre-stat byte/count bounds include ignored coding output.
    env={'PATH':'/usr/bin:/bin','HOME':'/nonexistent','GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null','GIT_TERMINAL_PROMPT':'0'}
    def git(*args):
        try:
            return subprocess.check_output(['/usr/bin/git','-C',str(p),'-c','core.hooksPath=/dev/null',*args],env=env,stderr=subprocess.DEVNULL,timeout=remaining(),text=True).strip()
        except subprocess.TimeoutExpired:
            if VALIDATION_UNTIL.get() is not None and clock()>=VALIDATION_UNTIL.get():
                from native_process import DeadlineReached
                raise DeadlineReached('Native role deadline exhausted in repository probe') from None
            raise
    allowed={'core.repositoryformatversion','core.filemode','core.bare','core.logallrefupdates','user.name','user.email'}
    local=git('config','--local','--no-includes','--list')
    need(all(line.split('=',1)[0] in allowed for line in local.splitlines()),'Unsafe disposable Git configuration')
    need(git('rev-parse','--show-toplevel') == str(p), 'Repository root mismatch')
    need(Path(git('rev-parse','--path-format=absolute','--git-common-dir')) == g, 'Linked/shared Git repository rejected')
    need(not git('remote'), 'Disposable repository must have no remotes')
    s=g.stat()
    return dict(path=str(p),git_dev=s.st_dev,git_ino=s.st_ino,uid=s.st_uid,head=git('rev-parse','HEAD'))

def grant_for(key):
    emergency();p,c = policy()
    need(isinstance(key,str) and HASH.fullmatch(key), 'Exact request identity required')
    # A review request is indexed by its coding workflow grant, never a wildcard.
    found=[]
    for file in Path(c['grants_dir']).glob('*.json'):
        g=strict(file)
        need(set(g)=={'schema_version','workflow_id','namespace','issue','project','machine','workspace',
            'repository_identity','registry_digest','coding_prompt_sha256','requests','agents','operations',
            'not_before','expires_at','limits','owner'}, 'Invalid grant fields')
        need(type(g['schema_version']) is int and g['schema_version']==1 and HASH.fullmatch(g['workflow_id']), 'Invalid workflow identity')
        need(file.name == g['workflow_id']+'.json', 'Grant filename identity mismatch')
        need(isinstance(g['namespace'],str) and re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',g['namespace']) and
             isinstance(g['issue'],str) and re.fullmatch(r'[1-9][0-9]*',g['issue']), 'Exact issue identity required')
        expected={r:request_id(g['namespace'],g['issue'],op) for r,op in (('coding','initial'),('review','review:1'))}
        need(g['requests']==expected and g['workflow_id']==expected['coding'], 'Request/workflow mismatch')
        need(g['agents']=={'coding':'codex','review':'codex'} and g['operations']==['coding','review'], 'No fallback or extra operations')
        need(isinstance(g['owner'],dict) and set(g['owner'])=={'kind','reference','approved'} and
             g['owner']['kind']=='owner_local_file' and g['owner']['approved'] is True and
             isinstance(g['owner']['reference'],str) and bool(g['owner']['reference'].strip()), 'Explicit owner provenance required')
        need(all(type(g[k]) in (int,float) and math.isfinite(g[k]) for k in ('not_before','expires_at')) and
             g['expires_at']>g['not_before'] and g['expires_at']-g['not_before']<=86400, 'Finite approval interval required')
        need(set(g['limits'])==set(MAXIMUM) and all(type(v) is int and 1<=v<=MAXIMUM[k] for k,v in g['limits'].items()), 'Invalid finite execution bounds')
        need(g['machine']=='work' and isinstance(g['project'],str) and re.fullmatch(r'[A-Za-z0-9_-]+',g['project']) and
             isinstance(g['registry_digest'],str) and HASH.fullmatch(g['registry_digest']) and
             isinstance(g['coding_prompt_sha256'],str) and HASH.fullmatch(g['coding_prompt_sha256']), 'Invalid repository/task binding')
        if key in expected.values():found.append((p,c,g,digest(g)))
    need(len(found)==1, 'Admission default DENY: no unique exact grant')
    p,c,g,h=found[0]
    need(g['not_before']<=time.time()<g['expires_at'], 'Admission expired or not yet valid')
    return p,c,g,h

def validate_scope(name, arguments):
    """Deny before repository identity RPCs can probe an unapproved remote host."""
    need(name=='start_task', 'Only initial scoped role launches are supported')
    p,c,g,h=grant_for(arguments.get('request_id'))
    import devctl
    cfg=devctl.load_projects().get(arguments.get('project'))
    role=arguments.get('role','coding')
    need(role in ('coding','review') and arguments.get('request_id')==g['requests'][role] and
         arguments.get('project')==g['project'] and cfg, 'Task/project/role mismatch')
    need(cfg['machine']==g['machine'] and cfg['workspace']==g['workspace'] and digest(cfg)==g['registry_digest'],
         'Registry binding changed before identity RPC')
    need(repository(g['workspace'],c['workspace_root'])==g['repository_identity'], 'Repository identity changed')


def validate(arguments, data_dir=None):
    emergency()
    a=dict(arguments);key=a.get('request_id');p,c,g,h=grant_for(key)
    if data_dir is not None:need(Path(data_dir).parent.resolve()==Path(c['state_dir']), 'Admission state identity mismatch')
    import devctl
    projects=devctl.load_projects();cfg=projects.get(a.get('project'))
    role=a.get('role','coding')
    need(role in ('coding','review') and key==g['requests'][role] and a.get('project')==g['project'] and cfg,
         'Task/project/role mismatch')
    need(cfg['machine']==g['machine'] and cfg['workspace']==g['workspace'] and digest(cfg)==g['registry_digest'], 'Registry binding changed')
    need(repository(g['workspace'],c['workspace_root'])==g['repository_identity'], 'Repository identity changed')
    need(a.get('agent') in (None,'codex') and a.get('model') in (None,'gpt-6.1-sol') and a.get('effort') in (None,'high'), 'Automatic role/model fallback forbidden')
    need(cfg.get('default_agent')=='codex' and cfg.get('review_agent')=='codex','Codex-to-Codex registry policy required')
    resolved=__import__('execution_profiles').resolve(cfg,role=role,
        **{k:a.get(k) for k in ('agent','model','effort','execution_profile','execution_options')})
    need(resolved['agent']=='codex' and resolved['model']=='gpt-6.1-sol' and resolved['options']=={'effort':'high'} and resolved['execution_profile']==('codex-high' if role=='coding' else 'codex-review'),'Only pinned native Codex roles')
    need(isinstance(a.get('prompt'),str) and a['prompt'], 'Exact task prompt required')
    if role=='coding':
        need(hashlib.sha256(a['prompt'].encode()).hexdigest()==g['coding_prompt_sha256'], 'Coding prompt changed')
        binding=dict(machine=g['machine'],repository=hashlib.sha256((g['machine']+'\0'+g['workspace']).encode()).hexdigest(),registry_digest=g['registry_digest'])
        need(a.get('repository_binding')==binding,'Coding v4 repository binding required')
    else:
        b=a.get('review_binding') or {}
        source=read_json(Path(c['state_dir'])/'tasks'/g['requests']['coding'][:24]/'meta.json',{})
        ledger=read_json(Path(c['state_dir'])/'admissions'/g['requests']['coding']/ 'claim.json',{})
        need(b.get('source_task_id')==g['requests']['coding'][:24] and b.get('source_turn')==1 and
             b.get('source_generation') and b.get('source_generation')==source.get('runtime_generation')==ledger.get('generation') and
             ledger.get('grant_digest')==h and ledger.get('status') in ('EXECUTING','TERMINAL'), 'Review source generation is not authorized')
    claim=Path(c['state_dir'])/'admissions'/key/'claim.json'
    historical=(Path(c['state_dir'])/'requests'/(key+'.json')).exists() or (Path(c['state_dir'])/'tasks'/key[:24]/'meta.json').exists()
    need(not historical or claim.exists(), 'Historical receipt/metadata cannot acquire admission')
    ticket=dict(policy=str(p),policy_digest=digest(c),grant_digest=h,arguments=a)
    return ticket,g,c

@contextmanager
def execution(arguments, data_dir=None):
    ticket,_,_=validate(arguments,data_dir)
    token=ACTIVE.set(ticket)
    try:yield ticket
    finally:ACTIVE.reset(token)

def require_worker():
    emergency();ticket=ACTIVE.get()
    need(ticket is not None, 'Admission default DENY: no execution context')
    now,_,_=validate(ticket['arguments'])
    need(now==ticket, 'Admission contract changed before operation')
    return ticket

def reserve(data_dir):
    t=require_worker();_,g,c=validate(t['arguments'],data_dir)
    key=t['arguments']['request_id'];path=Path(c['state_dir'])/'admissions'/key/'claim.json'
    # Called under worker.lock; an existing reservation with no receipt is UNKNOWN.
    with finite_lock(path.with_suffix('.lock')):
        need(not path.exists(), 'Admission already reserved; no automatic relaunch')
        from native_deadlines import boot,clock
        end=clock()+min(g['limits']['overall'],g['expires_at']-time.time())-12
        if t['arguments'].get('role')=='review':
            coding=strict(Path(c['state_dir'])/'admissions'/g['requests']['coding']/'claim.json')
            need(coding['boot']==boot(),'Coding admission boot changed')
            end=min(end,coding['work_deadline'])
        need(end>clock(),'No safe execution budget remains')
        write_json(path,dict(status='RESERVED',ticket=t,grant_digest=t['grant_digest'],boot=boot(),
            work_deadline=end,
            role_limit=g['limits'][t['arguments'].get('role','coding')],generation=None))
    return t

def dispatched(data_dir,generation):
    t=require_worker();_,_,c=validate(t['arguments'],data_dir)
    path=Path(c['state_dir'])/'admissions'/t['arguments']['request_id']/'claim.json'
    with finite_lock(path.with_suffix('.lock')):
        value=strict(path)
        need(value['ticket']==t and value['status']=='RESERVED' and value['generation'] is None, 'Consumed/conflicting admission')
        value.update(status='DISPATCHED',generation=generation);write_json(path,value)

def supervisor(ticket,generation):
    emergency();t,g,c=validate(ticket['arguments'])
    need(t==ticket, 'Supervisor authorization changed')
    path=Path(c['state_dir'])/'admissions'/t['arguments']['request_id']/'claim.json'
    from native_deadlines import boot,clock
    with finite_lock(path.with_suffix('.lock')):
        value=strict(path)
        need(value['ticket']==t and value['status']=='DISPATCHED' and value['generation']==generation and
             value['boot']==boot() and value['work_deadline']>clock(), 'Supervisor consumed/generation/budget mismatch')
        end=min(value['work_deadline'],clock()+value['role_limit'],clock()+g['expires_at']-time.time()-12)
        need(end>clock(),'No safe execution/cleanup budget remains')
        value.update(status='EXECUTING',execution_deadline=end)
        write_json(path,value)  # Crash after this point is UNKNOWN, never a fresh execution.
    return end

def require_running(ticket,generation,until):
    """Recheck the exact consumed authorization and claim at exec/heartbeat."""
    t,_,c=validate(ticket['arguments'])
    need(t==ticket,'Consumed authorization contract changed')
    value=strict(Path(c['state_dir'])/'admissions'/ticket['arguments']['request_id']/'claim.json')
    from native_deadlines import boot
    need(value['ticket']==ticket and value['grant_digest']==ticket['grant_digest'] and
         value['generation']==generation and value['status']=='EXECUTING' and value['boot']==boot() and
         value['execution_deadline']==until,'Executing admission claim/generation changed')


def terminal(ticket,generation,cleanup_proven):
    _,c=policy()
    path=Path(c['state_dir'])/'admissions'/ticket['arguments']['request_id']/'claim.json'
    with finite_lock(path.with_suffix('.lock')):
        value=strict(path)
        need(value['ticket']==ticket and value['generation']==generation and value['status']=='EXECUTING', 'Terminal admission identity mismatch')
        value['status']='TERMINAL' if cleanup_proven else 'UNKNOWN'
        write_json(path,value)

def issue_allowed(namespace, number, item=None):
    try:
        _,c,g,h=grant_for(request_id(namespace,str(number),'initial'))
        need(g['namespace']==namespace and g['issue']==str(number),'Issue scope mismatch')
        if item:
            need(not item.get('safety_latch_hold') and item.get('compatibility_status')!='QUARANTINED' and
                 not item.get('runtime_exclusion') and not item.get('review_migration'),'Legacy/sticky scope held')
            need(item.get('project',g['project'])==g['project'],'Issue project changed')
            if item.get('task_id'):
                need(item['task_id']==g['requests']['coding'][:24],'Legacy task cannot acquire issue admission')
                claim=strict(Path(c['state_dir'])/'admissions'/g['requests']['coding']/'claim.json')
                need(claim['grant_digest']==h,'Issue generation belongs to different approval')
            pending=item.get('pending_rpc')
            if pending:
                need(pending.get('name')=='start_task' and pending['arguments'].get('request_id') in g['requests'].values(),'Legacy pending intent held')
        return True
    except (AdmissionDenied,ExternalEffectBlocked,OSError,ValueError,TypeError,KeyError):return False

def available():
    try:emergency();policy();return True
    except (AdmissionDenied,ExternalEffectBlocked,OSError,ValueError,TypeError,KeyError):return False

def endpoint(function):
    @wraps(function)
    def wrapped(*args,**kwargs):
        values=inspect.signature(function).bind(*args,**kwargs);values.apply_defaults()
        with execution(values.arguments):return function(*args,**kwargs)
    return wrapped

def external_denied():
    # This release adds execution admission, not a transport/publication grant.
    # Existing Chief/owner decisions and journals are never transport authority.
    raise AdmissionDenied('External effect default DENY: separate publication/delivery permission required')

def native_required(config):
    need(config is not None, 'Admission requires native isolated Codex runtime')

def task_allowed(task):
    if not isinstance(task,str) or not re.fullmatch(r'[a-f0-9]{24}',task):return False
    try:
        _,c=policy()
        meta=read_json(Path(c['state_dir'])/'tasks'/task/'meta.json',{})
        key=meta.get('launch_request_id')
        if not key:return False
        claim=read_json(Path(c['state_dir'])/'admissions'/key/'claim.json',{})
        t,_,_=validate(claim['ticket']['arguments'])
        return t==claim['ticket'] and meta.get('runtime_generation')==claim.get('generation')
    except (AdmissionDenied,ExternalEffectBlocked,OSError,ValueError,TypeError,KeyError):return False

def reconciliation_allowed(directory,meta):
    """Execution authority is never maintenance authority for historical work."""
    try:
        need(meta.get('native_codex') and meta.get('runtime_generation') and
             not meta.get('safety_latch_hold') and meta.get('compatibility_status')!='QUARANTINED',
             'Only current native generations may reconcile')
        need(task_allowed(meta.get('id')),'Task generation has no current admission')
        _,c=policy();key=meta['launch_request_id']
        need(Path(directory)==Path(c['state_dir'])/'tasks'/meta['id'],'Reconciliation state path changed')
        claim=strict(Path(c['state_dir'])/'admissions'/key/'claim.json')
        return (claim['generation']==meta['runtime_generation'] and
                claim['ticket']==meta.get('admission') and claim['status'] in ('DISPATCHED','EXECUTING','TERMINAL'))
    except (AdmissionDenied,ExternalEffectBlocked,OSError,ValueError,KeyError,TypeError):return False


def require_request(data_dir,key,task_id,prepared=False):
    t=require_worker()
    need(t['arguments']['request_id']==key and key[:24]==task_id,'Deep request/task identity mismatch')
    _,_,c=validate(t['arguments'],data_dir)
    if prepared:
        claim=strict(Path(c['state_dir'])/'admissions'/key/'claim.json')
        need(claim['ticket']==t and claim['status']=='RESERVED','Durable admission reservation required before receipt')

@contextmanager
def finite_lock(path,blocking=True,until=None):
    from native_deadlines import clock
    bound=VALIDATION_UNTIL.get()
    if until is not None:bound=min(until,bound) if bound is not None else until
    end=time.monotonic()+min(5,max(0,bound-clock())) if bound is not None else time.monotonic()+5
    while True:
        if bound is not None and clock()>=bound:raise AdmissionDenied('Admission lock deadline exhausted; no dispatch')
        lock=exclusive(path,blocking=False)
        try:
            lock.__enter__()
            break
        except BlockingIOError:
            if not blocking or time.monotonic()>=end:raise AdmissionDenied('Admission lock unavailable; no dispatch')
            time.sleep(.02)
    try:yield
    finally:lock.__exit__(None,None,None)

def validate_rpc(name,arguments):
    emergency()
    need(name=='start_task','Continuation/adoption require separate admission; unsupported')
    return validate(arguments)

def operation_allowed(operation):
    try:
        t=require_worker();_,g,_=validate(t['arguments'])
        return operation in g['operations']
    except (AdmissionDenied,ExternalEffectBlocked,OSError,ValueError,TypeError,KeyError):return False

def require_operation(operation):
    emergency()
    need(operation_allowed(operation),'Operation outside exact admission scope')

"""Opt-in local Codex execution through the native admission/receipt lifecycle.

No shell, remote launcher, provider fallback, credential copying or reconciliation
of historical processes. A configured controller serves only its private state.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
from durable import exclusive, read_json, write_json
from execution_profiles import PreflightRejected
from native_deadlines import initialize, deadline, fence_unknown
from native_permissions import overrides, profile

CONFIG_ENV = 'DEVORCHESTRATOR_NATIVE_CODEX_CONFIG'

def require(value, reason):
    if not value: raise PreflightRejected(reason)

def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda:stream.read(64*1024),b''):h.update(chunk)
    return h.hexdigest()

def config(data_dir):
    name=os.environ.get(CONFIG_ENV)
    if not name:return None
    path=Path(name)
    require(path.is_absolute() and path.resolve()==path and not path.is_symlink(), 'Explicit canonical runtime config required')
    st=path.stat()
    require(st.st_uid==os.getuid() and st.st_mode & 0o077==0,'Runtime config must be private and owned')
    cfg=read_json(path)
    require(cfg.get('schema_version')==1 and cfg.get('enabled') is True,'Native runtime disabled or unsupported')
    for key in ('state_dir','runtime_root','workspace_root','binary','auth_home'):
        p=Path(cfg[key]);require(p.is_absolute() and p.resolve()==p,'Canonical '+key+' required')
    require(Path(cfg['state_dir'])==Path(data_dir).parent.resolve(),'Private state identity mismatch')
    root=Path(cfg['runtime_root']);work=Path(cfg['workspace_root']);auth=Path(cfg['auth_home'])
    for a,b in ((root,work),(auth,work),(Path(cfg['state_dir']),work)):
        require(not a.is_relative_to(b) and not b.is_relative_to(a),'Runtime/state/auth must be disjoint from mutable workspaces')
    require(cfg.get('model')=='gpt-6.1-sol' and cfg.get('effort')=='high','Pinned Codex model required')
    require(sha(cfg['binary'])==cfg.get('binary_sha256'),'Codex binary changed; new acceptance required')
    require(cfg.get('projects') and cfg.get('machine')=='work','Explicit local project allowlist required')
    st=(auth/'auth.json').stat()
    require(st.st_uid==os.getuid() and st.st_mode & 0o077==0 and not (auth/'auth.json').is_symlink(),'Private existing ChatGPT auth metadata required')
    return cfg

def snapshot(source,target,until=None):
    """Bounded streaming freeze; all bytes share the inherited preparation budget."""
    from repository_safety import entries,stream,manifest,check,until as budget
    source=Path(source);target=Path(target);until=budget() if until is None else until
    require(not target.exists(),'Review snapshot already exists; no re-admission')
    rows=entries(source,until)  # Reject size/count violations before reading/copying.
    files={};target.mkdir(mode=0o700)
    for p,identity in rows:
        check(until);relative=p.relative_to(source);dest=target/relative
        dest.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        with dest.open('xb') as out:files[str(relative)]=stream(p,identity,until,out)
    require(files==manifest(target,until),'Snapshot copy changed')
    require(files==manifest(source,until),'Coding source changed during snapshot')
    for p in target.rglob('*'):
        check(until)
        if p.is_file():p.chmod(0o444)
    for p in sorted((p for p in target.rglob('*') if p.is_dir()),reverse=True):
        check(until);p.chmod(0o555)
    check(until);target.chmod(0o555)
    return files


def build_command(cfg, role, repo, prompt, private):
    argv=[cfg['binary'],'exec','--json','--color','never','--ephemeral','--ignore-user-config','--ignore-rules',
          '-m',cfg['model'],'-C',str(repo)]
    for value in overrides(role,repo,cfg['binary'],private/'home',private/'tmp'):argv+=['-c',value]
    argv.append(prompt)
    from native_configuration import validate_with_cli
    from repository_safety import until
    validate_with_cli(argv,role,private/'configuration',until=min(deadline(private.parent,'contract'),until()),private=private)
    return argv

def _prepare(d,cfg,project,machine,agent,workspace,prompt,execution,role,request_id,review_binding):
    from admission_policy import require_worker, policy
    ticket=require_worker()
    require(Path(cfg['workspace_root'])==Path(policy()[1]['workspace_root']),'Native/admission workspace root mismatch')
    pc=policy()[1]
    for protected in (pc['state_dir'],pc['grants_dir'],pc['registry_file'],ticket['policy'],cfg['auth_home']):
        a,b=Path(cfg['runtime_root']),Path(protected)
        require(not a.is_relative_to(b) and not b.is_relative_to(a),'Private admission/auth overlaps role runtime')
    require(project in cfg['projects'] and machine==cfg['machine'] and d.MACHINES[machine]['type']=='local', 'Local registered runtime scope required')
    require(agent=='codex' and execution['agent']=='codex' and execution['model']==cfg['model']
            and execution['options'].get('effort')==cfg['effort'] and role in ('coding','review'),'Codex-only pinned role; no fallback')
    require(request_id and len(request_id)==64,'Durable request identity required')
    workspace=Path(workspace)
    require(workspace.resolve()==workspace and workspace.is_dir() and workspace.is_relative_to(cfg['workspace_root']), 'Workspace outside runtime allowlist')
    require(not workspace.is_symlink(),'Symlink workspace forbidden')
    from native_process import require_support
    require_support()
    if role=='coding':
        flow=Path(cfg['runtime_root'])/request_id
        require(not flow.exists(),'Existing native attempt requires forward repair; no replay execution')
        flow.mkdir(parents=True,mode=0o700);initialize(flow)
        repo=workspace
    else:
        require(review_binding is not None,'Bound review required')
        meta=d.load_meta(review_binding['source_task_id']);directory=d.task_dir(meta['id'])
        from native_supervisor import terminal_cleanup
        require(meta.get('native_codex') and meta['runtime_generation']==review_binding['source_generation']
                and d.stored_task_status(meta,directory)==('DONE',0)
                and terminal_cleanup(directory,meta,'DONE')=='ABSENT','Review needs proven terminal coding generation')
        flow=Path(meta['native_codex']['flow'])
        require(flow.parent==Path(cfg['runtime_root']),'Review workflow root mismatch')
        fence_unknown(flow);deadline(flow,role)
        repo=flow/'review/repo'
    private=flow/role
    require(not private.exists(),'Role already prepared; no automatic retry')
    private.mkdir(mode=0o700)
    for leaf in ('home','tmp'):(private/leaf).mkdir(mode=0o700)
    if role=='review':
        files=snapshot(workspace,repo)
        write_json(flow/'review-snapshot.json',dict(files=files,source_workspace=str(workspace),execution_workspace=str(repo),
                   source_task_id=meta['id'],source_generation=meta['runtime_generation']))
    argv=build_command(cfg,role,repo,prompt,private)
    require(isinstance(argv,list) and argv and argv[0]==cfg['binary'] and sha(argv[0])==cfg['binary_sha256'],'Final executable differs from pinned runtime')
    if role=='review':
        d.validate_review_source(project,machine,str(workspace),review_binding)
        from repository_safety import manifest,until
        require(files==manifest(workspace,until()),'Coding source changed during snapshot/configuration')
    env={'HOME':str(private/'home'),'CODEX_HOME':cfg['auth_home'],'PATH':'/usr/bin:/bin','TMPDIR':str(private/'tmp'),'LANG':'C.UTF-8'}
    spec={'admission':ticket,'schema_version':1,'role':role,'request_id':request_id,'flow':str(flow),'argv':argv,'env':env,
          'source_workspace':str(workspace),'execution_workspace':str(repo),'deadline':deadline(flow,role),
          'binary_sha256':cfg['binary_sha256']}
    path=private/'launch.json';write_json(path,spec)
    return dict(spec=str(path),spec_sha256=sha(path),flow=str(flow),execution_workspace=str(repo),
                source_workspace=str(workspace),permission_profile=profile(role,repo,cfg['binary'],private/'tmp'))


def prepare(d,cfg,project,machine,agent,workspace,prompt,execution,role,request_id,review_binding):
    from repository_safety import until
    from admission_policy import validation_budget,ACTIVE,strict,finite_lock
    try:
        end=until()
        with validation_budget(end):
            return _prepare(d,cfg,project,machine,agent,workspace,prompt,execution,role,request_id,review_binding)
    except Exception:
        # No STARTED receipt or metadata has been created yet. Preserve partial
        # private output and consumption; recovery can never restart preparation.
        from native_deadlines import stop
        flow=Path(cfg['runtime_root'])/request_id
        if role=='review' and review_binding:
            meta=d.load_meta(review_binding['source_task_id'])
            flow=Path(meta.get('native_codex',{}).get('flow',flow))
        if flow.exists():
            stop(flow,'Bounded native preparation rejected; owner forward repair required')
        ticket=ACTIVE.get()
        if ticket:
            path=Path(cfg['state_dir'])/'admissions'/request_id/'claim.json'
            with finite_lock(path.with_suffix('.lock')):
                if path.exists():
                    row=strict(path)
                    if row.get('ticket')==ticket and row.get('status')=='RESERVED':
                        row.update(status='UNKNOWN',diagnostic='PREPARATION_REJECTED_NO_WORKER_LAUNCH');write_json(path,row)
        if flow.exists():
            # Publish completion evidence only after the durable claim fence.
            write_json(flow/'preparation-failure.json',dict(role=role,request_id=request_id,
                state='UNKNOWN',reason='PREPARATION_REJECTED',automatic_retry=False,worker_launched=False))
        raise


def spawn(directory,meta,log_file):
    import subprocess
    import runtime_liveness as native
    from admission_policy import finite_lock,ACTIVE,strict
    from repository_safety import until
    process=None
    try:
        prepared=meta['native_codex'];path=Path(prepared['spec'])
        require(sha(path)==prepared['spec_sha256'],'Prepared native argv changed')
        bound=until()
        record=native.initialize(directory,meta,lock=lambda path,blocking=True:finite_lock(path,blocking,until=bound))
        meta['runtime_generation']=record['generation']
        from admission_policy import dispatched
        dispatched(Path(directory).parent,record['generation'])
        write_json(Path(directory)/'meta.json',meta)
        argv=[sys.executable,'-I','-B',str(Path(__file__).with_name('native_supervisor.py')),str(directory),record['generation'],str(path)]
        process=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=log_file,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True)
        return process
    except Exception:
        # No automatic attempt can reuse a pre-dispatch failure. UNKNOWN is
        # retained, even if no supervisor was created, rather than a false exit.
        from native_deadlines import stop
        prepared=meta.get('native_codex',{})
        if prepared.get('flow'):stop(prepared['flow'],'Pre-dispatch lock/identity failure; owner forward repair required')
        ticket=ACTIVE.get()
        if ticket:
            claim=Path(directory).parent.parent/'admissions'/ticket['arguments']['request_id']/'claim.json'
            with finite_lock(claim.with_suffix('.lock')):
                row=strict(claim)
                if row.get('ticket')==ticket and row.get('status') in ('RESERVED','DISPATCHED'):
                    row.update(status='UNKNOWN',diagnostic='PRE_DISPATCH_REJECTED');write_json(claim,row)
        write_json(Path(directory)/'native-watchdog/pre-dispatch-failure.json',dict(state='UNKNOWN',
            automatic_retry=False,worker_launched=process is not None,reason='PRE_DISPATCH_REJECTED'))
        raise
    finally:log_file.close()

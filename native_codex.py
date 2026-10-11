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
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

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

def snapshot(source,target):
    """Freeze a complete independent snapshot; links/special files fail closed."""
    source=Path(source);target=Path(target)
    require(not target.exists(),'Review snapshot already exists; no re-admission')
    files={}
    for p in source.rglob('*'):
        require(not p.is_symlink() and (p.is_file() or p.is_dir()),'Snapshot contains link or special file')
        if p.is_file():files[str(p.relative_to(source))]=sha(p)
    shutil.copytree(source,target)
    require(files=={str(p.relative_to(target)):sha(p) for p in target.rglob('*') if p.is_file()},'Snapshot copy changed')
    for p in target.rglob('*'):
        if p.is_file():p.chmod(0o444)
    for p in sorted((p for p in target.rglob('*') if p.is_dir()),reverse=True):p.chmod(0o555)
    target.chmod(0o555)
    return files

def build_command(cfg, role, repo, prompt, private):
    argv=[cfg['binary'],'exec','--json','--color','never','--ephemeral','--ignore-user-config','--ignore-rules',
          '-m',cfg['model'],'-C',str(repo)]
    for value in overrides(role,repo,cfg['binary'],private/'home',private/'tmp'):argv+=['-c',value]
    argv.append(prompt)
    from native_configuration import validate_with_cli
    validate_with_cli(argv,role,private/'configuration',until=deadline(private.parent,'contract'),private=private)
    return argv

def prepare(d,cfg,project,machine,agent,workspace,prompt,execution,role,request_id,review_binding):
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
        require(files=={str(p.relative_to(workspace)):sha(p) for p in workspace.rglob('*') if p.is_file()},'Coding source changed during snapshot/configuration')
    env={'HOME':str(private/'home'),'CODEX_HOME':cfg['auth_home'],'PATH':'/usr/bin:/bin','TMPDIR':str(private/'tmp'),'LANG':'C.UTF-8'}
    spec={'schema_version':1,'role':role,'request_id':request_id,'flow':str(flow),'argv':argv,'env':env,
          'source_workspace':str(workspace),'execution_workspace':str(repo),'deadline':deadline(flow,role),
          'binary_sha256':cfg['binary_sha256']}
    path=private/'launch.json';write_json(path,spec)
    return dict(spec=str(path),spec_sha256=sha(path),flow=str(flow),execution_workspace=str(repo),
                source_workspace=str(workspace),permission_profile=profile(role,repo,cfg['binary'],private/'tmp'))

def spawn(directory,meta,log_file):
    import subprocess
    import runtime_liveness as native
    prepared=meta['native_codex'];path=Path(prepared['spec'])
    require(sha(path)==prepared['spec_sha256'],'Prepared native argv changed')
    record=native.initialize(directory,meta);meta['runtime_generation']=record['generation']
    write_json(Path(directory)/'meta.json',meta)
    argv=[sys.executable,'-I','-B',str(Path(__file__).with_name('native_supervisor.py')),str(directory),record['generation'],str(path)]
    try:return subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=log_file,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True)
    finally:log_file.close()

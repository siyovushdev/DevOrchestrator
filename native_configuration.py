"""Shared, bounded CLI configuration gate. No real exec/session/provider calls."""
import hashlib
import json
import os
from pathlib import Path
import tomllib
import native_process as bp
from durable import read_json,write_json
from execution_profiles import PreflightRejected

def configuration(argv,role,private=None):
    from native_permissions import overrides
    if role not in ('coding','review') or len(argv)<13 or argv[1:8]!=['exec','--json','--color','never','--ephemeral','--ignore-user-config','--ignore-rules']:
        raise PreflightRejected('Invalid pinned offline-verifiable final Codex argv')
    if argv[8:11]!=['-m','gpt-6.1-sol','-C']:raise PreflightRejected('Model or workspace argv changed')
    repo=Path(argv[11]);private=Path(private) if private is not None else repo.parent
    if private.name!=role or not repo.is_absolute() or repo.resolve()!=repo:
        raise PreflightRejected('Private role repository path mismatch')
    items={};values=[]
    for index in range(12,len(argv)-1,2):
        if index+1>=len(argv)-1 or argv[index]!='-c':raise PreflightRejected('Unexpected final argv option')
        value=argv[index+1]
        try:key,raw=value.split('=',1);parsed=tomllib.loads('value='+raw)['value']
        except (ValueError,tomllib.TOMLDecodeError):raise PreflightRejected('Invalid config TOML override') from None
        if key in items:raise PreflightRejected('Duplicate config override')
        if key=='model_providers' or key.startswith('model_providers.'):
            raise PreflightRejected('Custom/built-in provider table overrides forbidden; use builtin openai')
        items[key]=parsed;values.append(value)
    expected=overrides(role,repo,argv[0],private/'home',private/'tmp')
    if values!=expected:raise PreflightRejected('Final argv differs from frozen role security configuration')
    return {'items':items,'values':values,'repo':repo,'private':private}

def validate_with_cli(argv,role,folder,*,until,private=None):
    from native_deadlines import clock
    cfg=configuration(argv,role,private)
    binary=argv[0]
    folder=Path(folder);folder.mkdir(mode=0o700,parents=True)
    work=cfg['private']/'tmp/config-preflight';work.mkdir(mode=0o700)
    for name in ('empty-codex','scratch'):(work/name).mkdir(mode=0o700)
    # Empty CODEX_HOME reproduces --ignore-user-config without opening auth.
    # System/ancestor policy files must be absent; project config cannot hide a
    # provider override or replace this exact role permission profile.
    for path in [Path('/etc/codex/config.toml'),Path('/etc/codex/requirements.toml'),
                 *(p/'.codex/config.toml' for p in [cfg['repo'],*cfg['repo'].parents])]:
        if path==Path.home()/'.codex/config.toml':continue
        if path.exists():raise PreflightRejected('Unexpected system/project Codex configuration layer')
    helper=work/'entry.py';helper.write_bytes(Path(__file__).with_name('native_configuration_probe.py').read_bytes())
    env={'HOME':str(work),'CODEX_HOME':str(work/'empty-codex'),'TMPDIR':str(work/'scratch'),
         'PATH':'/usr/bin:/bin','LANG':'C.UTF-8'}
    common=[]
    for value in cfg['values']+['model="gpt-6.1-sol"']:common+=['-c',value]
    commands={'features':[binary,'-C',str(cfg['repo']),*common,'features','list'],
              'argv-help':[*argv,'--help']}
    host_ns={name:os.readlink('/proc/self/ns/'+name) for name in ('user','pid','mnt','net')}
    results={};outputs={}
    for mode,command in commands.items():
        spec={'mode':mode,'host_ns':host_ns,'argv':command,'offline_env':env}
        probe=[binary,'sandbox',*common,'--permission-profile','simple-'+role,'-C',str(cfg['repo']),'--',
               '/usr/bin/python3','-I','-B',str(helper),json.dumps(spec)]
        with (folder/(mode+'.stdout')).open('w') as out,(folder/(mode+'.stderr')).open('w') as err:
            result=bp.run(probe,folder/(mode+'-watchdog'),deadline=min(until,clock()+10),env=env,stdout=out,stderr=err)
        results[mode]=result
        if result['state']!='EXITED' or not result['cleanup_proven']:
            write_json(folder/'result.json',{'status':'UNKNOWN_OR_TIMEOUT','mode':mode,'watchdog':result,'automatic_retry':False})
            raise RuntimeError('Configuration probe timeout/unknown; preserve evidence and fence admission')
        output=(folder/(mode+'.stdout')).read_text();outputs[mode]=output
        if result['returncode']!=0 or not output.startswith('OFFLINE_NATIVE_CONFIG_ISOLATION_PASS '):
            write_json(folder/'result.json',{'status':'REJECTED','mode':mode,'exit_code':result['returncode'],'worker_created':False,'automatic_retry':False})
            raise PreflightRejected('Native Codex CLI rejected effective configuration/argv; see private config-preflight evidence')
    features={line.split()[0]:line.split()[-1]=='true' for line in outputs['features'].splitlines()[1:] if len(line.split())>=3}
    for key,value in cfg['items'].items():
        if key.startswith('features.') and features.get(key[9:]) is not value:
            raise PreflightRejected('Effective Codex feature restriction mismatch: '+key)
    result={'status':'PASS','role':role,'configuration':cfg['items'],'final_argv_sha256':hashlib.sha256(json.dumps(argv).encode()).hexdigest(),
        'argv_without_prompt':argv[:-1],'prompt_sha256':hashlib.sha256(argv[-1].encode()).hexdigest(),
        'actual_cli_config_load':'FEATURES_LIST_PASS','actual_final_argv_parse':'EXEC_HELP_PASS','effective_features':features,
        'probe_credentials':'EMPTY_CODEX_HOME_NO_AUTH','native_namespaces_and_kernel_assertions':'PASS',
        'inference_requests':0,'worker_created':False,'watchdogs':results,'builtin_provider_transport_retries':'CLI_DEFAULTS_NOT_OVERRIDDEN',
        'automatic_worker_retry':False,'configuration_layers':'ONLY_FROZEN_OVERRIDES_SYSTEM_AND_PROJECT_ABSENT_USER_IGNORED'}
    write_json(folder/'result.json',result);return result

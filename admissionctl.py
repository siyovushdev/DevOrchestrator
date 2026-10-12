"""Prepare an UNAPPROVED owner-review draft; never install grants or admit work."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import time
import sys
sys.path.insert(0,str(Path(__file__).resolve().parent))
import admission_policy as a
from durable import write_json
from review_transitions import digest,request_id

def draft(policy_file,issue_file,namespace,reference,seconds,output):
    # Per-process selection only; this does not change service configuration.
    os.environ[a.ENV]=str(a.private(policy_file))
    _,pc=a.policy()
    issue=a.strict(issue_file)
    import devctl,task_bridge
    project,agent,mode,model,effort,prompt=task_bridge.parse_task(issue)
    cfg=devctl.load_projects()[project]
    a.need(cfg['machine']=='work' and cfg['default_agent']=='codex' and cfg['review_agent']=='codex','Codex-to-Codex local registry required')
    a.need(type(issue['number']) is int and issue['number']>0 and isinstance(namespace,str) and
           a.re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',namespace),'Exact native issue identity required')
    a.need(type(seconds) is int and 60<=seconds<=86400,'Finite draft expiration required')
    if mode==task_bridge.AUTOPILOT_MODE:prompt=task_bridge.build_autopilot_prompt(prompt)
    prompt=task_bridge.coding_prompt(prompt)
    number=str(issue['number']);requests={r:request_id(namespace,number,op) for r,op in [('coding','initial'),('review','review:1')]}
    output=Path(output)
    a.private(output.parent,directory=True)
    a.need(output.is_absolute() and output.resolve()==output and not output.exists(),'New canonical private draft file required')
    for protected in ('workspace_root','state_dir','grants_dir'):
        a.need(not output.is_relative_to(pc[protected]),'Draft must remain outside active state/grants/workspaces')
    now=time.time()
    result=dict(schema_version=1,workflow_id=requests['coding'],namespace=namespace,issue=number,project=project,
        machine='work',workspace=cfg['workspace'],repository_identity=a.repository(cfg['workspace'],pc['workspace_root']),
        registry_digest=digest(cfg),coding_prompt_sha256=hashlib.sha256(prompt.encode()).hexdigest(),requests=requests,
        agents={'coding':'codex','review':'codex'},operations=['coding','review'],not_before=now,expires_at=now+seconds,
        limits=a.MAXIMUM.copy(),owner={'kind':'owner_local_file','reference':reference,'approved':False})
    write_json(output,result)
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for flag in ('policy','issue-file','namespace','owner-reference','output'):p.add_argument('--'+flag,required=True)
    p.add_argument('--seconds',type=int,default=3600)
    args=p.parse_args()
    value=draft(args.policy,args.issue_file,args.namespace,args.owner_reference,args.seconds,args.output)
    print(json.dumps({'status':'UNAPPROVED_DRAFT_ONLY','workflow_id':value['workflow_id'],'output':args.output,'admission_installed':False}))

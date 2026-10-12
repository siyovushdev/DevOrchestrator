"""Actual ControlLoop/MCP/devctl/native supervisor/pidfd path with inert roles."""
import asyncio
from contextlib import ExitStack,redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import threading
import time
import tempfile
import unittest
from unittest.mock import patch,Mock

import admission_policy as a
import devctl as d
import mcp_server as mcp
import native_codex
import native_process
import task_bridge as b
from control_loop import ControlLoop
from durable import read_json,write_json
from test_admission_policy import PolicyFixture
from test_bridge_integration import Harness

class ScopedLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.f=PolicyFixture(self.root)
        # Harness provides only synthetic issue/comment observations, never launches.
        self.h=object.__new__(Harness)
        self.h.repo=self.f.repo;self.h.root=self.root
        self.h.issues={1:self.h.issue(1)};self.h.comments={1:[]};self.h.state={'issues':{}}
        project,agent,mode,model,effort,prompt=b.parse_task(self.h.issues[1])
        self.f.grant['namespace']=b.REPO
        from review_transitions import request_id
        self.f.grant['requests']={role:request_id(b.REPO,'1',op) for role,op in [('coding','initial'),('review','review:1')]}
        self.f.grant['workflow_id']=self.f.grant['requests']['coding']
        self.f.file.unlink();self.f.file=self.f.file.parent/(self.f.grant['workflow_id']+'.json')
        text=b.coding_prompt(b.build_autopilot_prompt(prompt))
        self.f.grant['coding_prompt_sha256']=hashlib.sha256(text.encode()).hexdigest();self.f.save()
        self.f.args.update(prompt=text,request_id=self.f.grant['workflow_id'])
        self.stack=ExitStack();self.addCleanup(self.stack.close);self.cli_lock=threading.Lock()
        self.stack.enter_context(patch.dict(os.environ,{**self.f.env(),'DEVORCHESTRATOR_OWNERS':'siyovushdev'}))
        self.stack.enter_context(patch.object(d,'DATA_DIR',self.root/'state/tasks'))
        self.stack.enter_context(patch.object(b,'STATE_FILE',self.root/'state/task_bridge_state.json'))
        self.stack.enter_context(patch.object(b,'gh_json',self.h.gh_json))
        self.github_calls=Mock(wraps=self.h.gh)
        self.stack.enter_context(patch.object(b,'gh',self.github_calls))
        self.stack.enter_context(patch.object(d,'capture_execution',return_value=subprocess.CompletedProcess([],0,'--model --config','')))
        self.stack.enter_context(patch('execution_profiles.discover',return_value={'version':'fixture','help':'--model --config'}))
        self.modes={'coding':'normal','review':'normal'}
        def command(cfg,role,repo,prompt,private):
            return [cfg['binary'],'-I','-B',str(Path(__file__).with_name('admission_dummy.py')),role,str(repo),self.modes[role]]
        self.stack.enter_context(patch.object(native_codex,'build_command',side_effect=command))
        self.stack.enter_context(patch.object(mcp,'devctl_result',side_effect=self.cli))
        async def transport(name,args):
            return await asyncio.to_thread(getattr(mcp,name),**args)
        self.stack.enter_context(patch.object(b,'_transport_call',side_effect=transport))
        self.loop=ControlLoop(self.h.state)
    def cli(self,*args,**kwargs):
        # CLI serialization is an adapter for the actual functions; enforcement,
        # launch/receipt code, subprocess spawn and supervisor are never mocked.
        with self.cli_lock:
            out=io.StringIO()
            def value(flag):return args[args.index(flag)+1] if flag in args else None
            with redirect_stdout(out):
                if args[0]=='run-project':
                    kw={k:json.loads(value(flag)) for flag,k in [('--review-binding','review_binding'),('--repository-binding','repository_binding'),('--execution-options','execution_options')] if value(flag)}
                    kw.update({k:value(flag) for flag,k in [('--model','model'),('--effort','effort'),('--execution-profile','execution_profile'),('--request-id','request_id'),('--role','role')] if value(flag)})
                    d.run_project(args[1],args[2],agent_override=value('--agent'),**kw)
                elif args[0]=='report':d.task_report(args[1],as_json=True)
                elif args[0]=='tasks':d.list_tasks(as_json=True,cached=True,control=True)
                elif args[0]=='preflight-task':print(json.dumps(d.preflight_task(args[1],json.loads(args[2]))))
                elif args[0]=='inspect-start-request':print(json.dumps(d.worker_runtime.inspect_start_request(d.DATA_DIR,args[1],fence='--fence' in args)))
                else:raise AssertionError(args[0])
            return subprocess.CompletedProcess(args,0,out.getvalue(),'')
    async def pump(self,predicate,seconds=25):
        until=time.monotonic()+seconds
        while time.monotonic()<until:
            self.loop.next_heavy=0;self.loop.next_control.clear()
            await self.loop.step();await asyncio.sleep(.015)
            if predicate():return
        raise AssertionError('Scoped ControlLoop deadline; state='+json.dumps(self.h.state)[:1800])
    async def asyncTearDown(self):
        await self.loop.close()
        for child in list(d._CHILDREN):
            if str(self.root) in str(child.args):await asyncio.to_thread(child.wait,15)
        d.reap_processes();b.WORK_CONTEXT=None
        evidence=Path(os.environ['HOME'])/'lifecycle-evidence';evidence.mkdir(mode=0o700,exist_ok=True)
        records={str(p.relative_to(self.root)):read_json(p) for p in self.root.rglob('*.json') if p.name in ('claim.json','bounded-result.json','processes.json','review-snapshot.json') or p.parent.name=='requests'}
        write_json(evidence/(self._testMethodName+'.json'),dict(state=self.h.state,records=records,real_inference=False,production_access=False))
    def item(self):return self.h.state['issues'].get('1',{})
    async def test_actual_control_loop_pair_receipts_recovery_replay(self):
        await self.pump(lambda:self.item().get('chief_status')=='AWAITING_CHIEF_REVIEW' and bool(self.item().get('chief_review')))
        receipts={}
        for role,key in self.f.grant['requests'].items():
            row=read_json(d.DATA_DIR.parent/'requests'/(key+'.json'))
            self.assertEqual(row['signature_version'],4 if role=='coding' else 3)
            self.assertEqual(row['status'],'STARTED');receipts[role]=row
            meta=d.load_meta(row['task_id'])
            result=read_json(d.DATA_DIR/row['task_id']/'native-watchdog/bounded-result.json')
            self.assertEqual(result['state'],'EXITED');self.assertTrue(result['cleanup_proven']);self.assertEqual(result['signals'],[])
            self.assertEqual(read_json(d.DATA_DIR.parent/'admissions'/key/'claim.json')['status'],'TERMINAL')
        self.assertEqual(self.item()['review_status'],'PASS');self.assertNotEqual(self.item().get('owner_state'),'READY_FOR_OWNER')
        self.assertFalse(self.item().get('owner_approved_at'));self.assertFalse(self.item().get('chief_review',{}).get('verdict'))
        # DONE also denotes purely local notification supersession, never delivery.
        for entry in self.h.state.get('outbox',{}).values():
            if entry['kind']!='transition' and entry['status']=='DONE':
                self.assertEqual(entry.get('diagnostic'),'Notification superseded before send')
                self.assertEqual(entry['attempts'],0)
        self.assertTrue(all(__import__('external_effects').github_is_read(call.args) for call in self.github_calls.call_args_list), [call.args[:3] for call in self.github_calls.call_args_list if not __import__('external_effects').github_is_read(call.args)])
        before=list(d.DATA_DIR.glob('*/native-watchdog/launch.json'))
        for key in self.f.grant['requests'].values():
            args=read_json(d.DATA_DIR.parent/'admissions'/key/'claim.json')['ticket']['arguments']
            await asyncio.to_thread(mcp.start_task,**args)
        await self.loop.close();self.h.state=b.load_state();self.loop=ControlLoop(self.h.state)
        for _ in range(5):await self.loop.step();await asyncio.sleep(.02)
        self.assertEqual(list(d.DATA_DIR.glob('*/native-watchdog/launch.json')),before)
        snapshot=read_json(next((self.root/'runtime').glob('*/review-snapshot.json')))
        self.assertEqual(snapshot['source_generation'],d.load_meta(receipts['coding']['task_id'])['runtime_generation'])
    async def test_latch_on_valid_grant_never_launches(self):
        with patch.dict(os.environ,{'DEVORCHESTRATOR_BLOCK_EXTERNAL_EFFECTS':'1'}):
            await self.loop.step()
        self.assertFalse(list(d.DATA_DIR.glob('*/meta.json')))
    async def test_timeout_identity_cleanup_no_review_or_relaunch(self):
        self.modes['coding']='hang';self.f.grant['limits']['coding']=1;self.f.save()
        await self.pump(lambda:bool(list(d.DATA_DIR.glob('*/native-watchdog/bounded-result.json'))),seconds=20)
        result=read_json(next(d.DATA_DIR.glob('*/native-watchdog/bounded-result.json')))
        self.assertEqual(result['state'],'TIMEOUT');self.assertTrue(result['cleanup_proven'])
        self.assertTrue(result['signals']);self.assertTrue(all(s['verified'] for s in result['signals']))
        self.assertFalse((d.DATA_DIR/self.f.grant['requests']['review'][:24]).exists())
        self.assertEqual(len(list(d.DATA_DIR.glob('*/meta.json'))),1)
    async def test_crash_receipt_recovery_never_second_spawn(self):
        self.modes['coding']='crash'
        await self.pump(lambda:bool(list(d.DATA_DIR.glob('*/native-watchdog/bounded-result.json'))))
        await self.loop.close();self.h.state=b.load_state();self.loop=ControlLoop(self.h.state)
        for _ in range(5):await self.loop.step();await asyncio.sleep(.02)
        self.assertEqual(len(list(d.DATA_DIR.glob('*/native-watchdog/launch.json'))),1)

    async def test_synthetic_chief_owner_intake_no_publication(self):
        await self.pump(lambda:self.item().get('chief_status')=='AWAITING_CHIEF_REVIEW' and bool(self.item().get('chief_review')))
        item=self.item();record=item['chief_review']
        # Synthetic observations, not real delivery/provenance or a real decision.
        for key in record['effects']:
            entry=self.h.state['outbox'][key]
            self.h.comments[1].append(dict(id='fixture-'+key,author={'login':'bridge'},body=entry['payload']+'\n\n<!-- DEVORCHESTRATOR_EFFECT_V1 '+key+' -->'))
        decision=dict(bundle_id=record['identity']['bundle_id'],verdict='PASS',summary='SYNTHETIC CHIEF CONTRACT TEST ONLY',findings=[])
        self.h.comment('<!-- DEVORCHESTRATOR_CHIEF_REVIEW_V1 '+json.dumps(decision)+' -->')
        await self.pump(lambda:self.item().get('owner_state')=='READY_FOR_OWNER')
        import owner_decision
        body='<!-- DEVORCHESTRATOR_OWNER_DECISION_V1 '+json.dumps(dict(decision='APPROVE',bundle_id=record['identity']['bundle_id']))+' -->'
        self.assertEqual(owner_decision.apply(item,body,'SYNTHETIC_OWNER_1'),'RECORDED')
        self.assertEqual(owner_decision.apply(item,body,'SYNTHETIC_OWNER_1'),'REPLAY')
        self.assertEqual(item['owner_decision']['publication_permissions'],[])
        body=body.replace('APPROVE','REJECT');self.assertEqual(owner_decision.apply(item,body,'SYNTHETIC_OWNER_2'),'RECORDED')
        self.assertFalse(owner_decision.publication_allowed(item))
        from external_effects import refuse
        for action in ('publication','github','telegram'):
            with self.assertRaises(a.AdmissionDenied):refuse(action)
        b.save_state(self.h.state);restored=b.load_state()
        self.assertEqual(restored['issues']['1']['owner_state'],'READY_FOR_OWNER')
        self.assertEqual(restored['issues']['1']['owner_decision'],item['owner_decision'])

    async def test_concurrent_real_cli_mcp_same_request_one_launch(self):
        def independent_cli(*args,**kwargs):
            return subprocess.run(['/usr/bin/python3','-I','-B',str(Path(__file__).with_name('admission_dummy_cli.py')),*args],
                capture_output=True,text=True,stdin=subprocess.DEVNULL,timeout=20)
        with patch.object(mcp,'devctl_result',side_effect=independent_cli):
            first,second=await asyncio.gather(asyncio.to_thread(mcp.start_task,**self.f.args),asyncio.to_thread(mcp.start_task,**self.f.args))
        self.assertEqual(first['task_id'],second['task_id'])
        key=self.f.grant['workflow_id'];task=d.DATA_DIR/key[:24]
        until=time.monotonic()+10
        while time.monotonic()<until and not (task/'native-watchdog/bounded-result.json').exists():await asyncio.sleep(.03)
        result=read_json(task/'native-watchdog/bounded-result.json')
        self.assertEqual(result['state'],'EXITED');self.assertTrue(result['cleanup_proven'])
        self.assertEqual(len(list(d.DATA_DIR.glob('*/native-watchdog/launch.json'))),1)
        self.assertEqual(read_json(d.DATA_DIR.parent/'requests'/(key+'.json'))['signature_version'],4)
        self.assertEqual(read_json(d.DATA_DIR.parent/'admissions'/key/'claim.json')['status'],'TERMINAL')

    async def test_admitted_mcp_observation_when_latched_readonly(self):
        await self.pump(lambda:self.item().get('chief_status')=='AWAITING_CHIEF_REVIEW')
        paths=list(d.DATA_DIR.rglob('meta.json'))+list((d.DATA_DIR.parent/'requests').glob('*.json'))
        before={str(p):p.read_bytes() for p in paths}
        with patch.dict(os.environ,{'DEVORCHESTRATOR_BLOCK_EXTERNAL_EFFECTS':'1'}):
            self.assertFalse(a.task_allowed(self.f.grant['workflow_id'][:24]))
            report=await asyncio.to_thread(mcp.get_task_report,self.f.grant['workflow_id'][:24])
            self.assertIn(report['status'],('DONE','UNKNOWN'))
        self.assertEqual(before,{str(p):p.read_bytes() for p in paths})

    async def revoke_running(self,claim_change):
        self.modes['coding']='hang';self.f.grant['limits']['coding']=15;self.f.save()
        await self.pump(lambda:bool(list(d.DATA_DIR.glob('*/native-watchdog/commit.json'))))
        await asyncio.sleep(.1)
        if claim_change:
            path=d.DATA_DIR.parent/'admissions'/self.f.grant['workflow_id']/'claim.json'
            value=read_json(path);self.assertEqual(value['status'],'EXECUTING');value['status']='UNKNOWN';write_json(path,value)
        else:
            self.f.grant['owner']['reference']='CHANGED_VALID_OWNER_CONTRACT';self.f.save()
        await self.pump(lambda:bool(list(d.DATA_DIR.glob('*/native-watchdog/bounded-result.json'))),seconds=20)
        result=read_json(next(d.DATA_DIR.glob('*/native-watchdog/bounded-result.json')))
        self.assertEqual(result['state'],'UNKNOWN');self.assertIsNone(result['returncode']);self.assertTrue(result['cleanup_proven'])
        self.assertTrue(result['signals']);self.assertTrue(all(s['verified'] for s in result['signals']))
        self.assertEqual(len(list(d.DATA_DIR.glob('*/native-watchdog/launch.json'))),1)
        self.assertFalse((d.DATA_DIR/self.f.grant['requests']['review'][:24]).exists())
        await self.loop.close();self.h.state=b.load_state();self.loop=ControlLoop(self.h.state)
        for _ in range(3):await self.loop.step()
        self.assertEqual(len(list(d.DATA_DIR.glob('*/native-watchdog/launch.json'))),1)
    async def test_changed_valid_contract_during_real_worker_unknown_cleanup(self):
        await self.revoke_running(False)
    async def test_changed_executing_claim_during_real_worker_unknown_cleanup(self):
        await self.revoke_running(True)

    async def test_preparation_deadline_failure_fenced_no_review_launch_recovery(self):
        original=native_codex.snapshot
        def expired(source,target,until=None):
            return original(source,target,until=__import__('repository_safety').clock()-1)
        with patch.object(native_codex,'snapshot',side_effect=expired):
            await self.pump(lambda:bool(list((self.root/'runtime').glob('*/preparation-failure.json'))))
        failure=read_json(next((self.root/'runtime').glob('*/preparation-failure.json')))
        self.assertEqual(failure['state'],'UNKNOWN');self.assertFalse(failure['worker_launched'])
        self.assertEqual(len(list(d.DATA_DIR.glob('*/native-watchdog/launch.json'))),1)
        key=self.f.grant['requests']['review']
        self.assertFalse((d.DATA_DIR.parent/'requests'/(key+'.json')).exists())
        self.assertEqual(read_json(d.DATA_DIR.parent/'admissions'/key/'claim.json')['status'],'UNKNOWN')
        await self.loop.close();self.h.state=b.load_state();self.loop=ControlLoop(self.h.state)
        for _ in range(5):await self.loop.step()
        self.assertEqual(len(list(d.DATA_DIR.glob('*/native-watchdog/launch.json'))),1)

    async def test_oversized_ignored_coding_output_never_reviews_or_relaunches(self):
        self.modes['coding']='oversize'
        await self.pump(lambda:bool(list(d.DATA_DIR.glob('*/native-watchdog/bounded-result.json'))))
        for _ in range(5):await self.loop.step()
        self.assertEqual(len(list(d.DATA_DIR.glob('*/native-watchdog/launch.json'))),1)
        key=self.f.grant['requests']['review']
        self.assertFalse((d.DATA_DIR.parent/'requests'/(key+'.json')).exists())
        await self.loop.close();self.h.state=b.load_state();self.loop=ControlLoop(self.h.state)
        for _ in range(5):await self.loop.step()
        self.assertEqual(len(list(d.DATA_DIR.glob('*/native-watchdog/launch.json'))),1)

    async def test_legacy_capacity_and_leases_preserved_alongside_real_pair(self):
        import copy
        records=[]
        for name,extra in (('legacy-unknown',{}),('legacy-held',{'safety_latch_hold':{'reason':'legacy'}}),
                           ('legacy-quarantine',{'compatibility_status':'QUARANTINED'}),('legacy-terminal',{})):
            directory=d.DATA_DIR/name
            write_json(directory/'meta.json',dict(id=name,machine='work',agent='codex',role='coding',pid=0,status='UNKNOWN',**extra))
            if name=='legacy-terminal':(directory/'exit_code').write_text('1')
            records.extend(p for p in directory.iterdir() if p.is_file())
        leases={'legacy-key':{'task_id':'legacy-unknown','machine':'work'}}
        write_json(d.DATA_DIR.parent/'worker_leases.json',leases)
        before={str(p):p.read_bytes() for p in records}
        with patch.dict(os.environ,{'DEVORCHESTRATOR_MAX_CODING':'10','DEVORCHESTRATOR_MAX_AGENTS':'10','DEVORCHESTRATOR_MACHINE_LIMITS':'{"work":10}'}),patch.object(os,'killpg',side_effect=AssertionError('No legacy signals')):
            await self.pump(lambda:self.item().get('chief_status')=='AWAITING_CHIEF_REVIEW')
        self.assertEqual({str(p):p.read_bytes() for p in records},before)
        self.assertEqual(read_json(d.DATA_DIR.parent/'worker_leases.json')['legacy-key'],leases['legacy-key'])
        self.assertEqual(len(list(d.DATA_DIR.glob('*/native-watchdog/launch.json'))),2)

    async def test_native_initialization_contended_lock_is_finite_and_no_relaunch(self):
        import runtime_liveness
        from durable import exclusive
        from native_deadlines import clock
        original=runtime_liveness.initialize;failed=[]
        def contested(directory,meta,**kwargs):
            with exclusive(directory/'liveness.lock'),a.validation_budget(clock()+.15):
                try:return original(directory,meta,**kwargs)
                except a.AdmissionDenied:failed.append(True);raise
        with patch.object(runtime_liveness,'initialize',side_effect=contested):
            await self.pump(lambda:bool(list(d.DATA_DIR.glob('*/native-watchdog/pre-dispatch-failure.json'))),seconds=10)
        self.assertTrue(failed);self.assertFalse(list(d.DATA_DIR.glob('*/native-watchdog/launch.json')))
        key=self.f.grant['workflow_id']
        self.assertEqual(read_json(d.DATA_DIR.parent/'requests'/(key+'.json'))['status'],'PENDING')
        self.assertEqual(read_json(d.DATA_DIR.parent/'admissions'/key/'claim.json')['status'],'UNKNOWN')
        await self.loop.close();self.h.state=b.load_state();self.loop=ControlLoop(self.h.state)
        for _ in range(5):await self.loop.step()
        self.assertFalse(list(d.DATA_DIR.glob('*/native-watchdog/launch.json')))

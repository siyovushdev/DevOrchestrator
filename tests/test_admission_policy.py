"""Real enforcement; private Git fixtures, no production/credentials/providers."""
import asyncio
import copy
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch,Mock

import admission_policy as a
import devctl as d
import external_effects as e
from durable import read_json,write_json
from review_transitions import digest,request_id

class PolicyFixture:
    def __init__(self,root):
        self.root=root
        for leaf in ('settings','settings/grants','state','workspaces','runtime','auth'):
            (root/leaf).mkdir(mode=0o700)
        self.repo=root/'workspaces/repo';self.repo.mkdir(mode=0o700)
        def git(*args):return subprocess.check_output(['git','-C',str(self.repo),*args],stderr=subprocess.DEVNULL,text=True).strip()
        git('init','-b','pilot');git('config','user.name','Fixture');git('config','user.email','test@example.invalid')
        (self.repo/'file').write_text('baseline');git('add','.');git('commit','-m','Fixture baseline')
        self.cfg=dict(machine='work',workspace=str(self.repo),default_agent='codex',review_agent='codex',
            execution_profile='codex-high',review_execution_profiles={'codex':'codex-review'},chief_required=True)
        self.registry=root/'settings/registry.json';write_json(self.registry,{'test':self.cfg})
        self.policy=root/'settings/policy.json'
        self.pc=dict(schema_version=1,state_dir=str(root/'state'),grants_dir=str(root/'settings/grants'),
            workspace_root=str(root/'workspaces'),registry_file=str(self.registry))
        write_json(self.policy,self.pc)
        key=request_id('owner/test','1','initial')
        self.grant=dict(schema_version=1,workflow_id=key,namespace='owner/test',issue='1',project='test',
            machine='work',workspace=str(self.repo),repository_identity=a.repository(self.repo,root/'workspaces'),
            registry_digest=digest(self.cfg),coding_prompt_sha256=hashlib.sha256(b'fixture coding').hexdigest(),
            requests={'coding':key,'review':request_id('owner/test','1','review:1')},agents={'coding':'codex','review':'codex'},
            operations=['coding','review'],not_before=time.time()-1,expires_at=time.time()+3600,
            limits=dict(coding=10,review=10,overall=60),owner=dict(kind='owner_local_file',reference='SYNTHETIC_TEST_ONLY',approved=True))
        self.file=root/'settings/grants'/(key+'.json');write_json(self.file,self.grant)
        self.binding=dict(machine='work',repository=hashlib.sha256(('work\0'+str(self.repo)).encode()).hexdigest(),registry_digest=digest(self.cfg))
        self.args=dict(project='test',agent='codex',prompt='fixture coding',role='coding',request_id=key,repository_binding=self.binding)
        binary=Path('/usr/bin/python3').resolve()
        write_json(root/'auth/auth.json',{'SYNTHETIC_NO_AUTH':True})
        self.native=root/'settings/native.json'
        write_json(self.native,dict(schema_version=1,enabled=True,state_dir=str(root/'state'),runtime_root=str(root/'runtime'),
            workspace_root=str(root/'workspaces'),auth_home=str(root/'auth'),binary=str(binary),binary_sha256=a.hashlib.sha256(binary.read_bytes()).hexdigest(),
            machine='work',projects=['test'],model='gpt-6.1-sol',effort='high'))
    def save(self):write_json(self.file,self.grant)
    def env(self):return {a.ENV:str(self.policy),e.ENV:'0','DEVORCHESTRATOR_NATIVE_CODEX_CONFIG':str(self.native)}

class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.f=PolicyFixture(Path(self.tmp.name));self.stack=ExitStack();self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.dict(os.environ,self.f.env()))
        self.stack.enter_context(patch.object(d,'DATA_DIR',self.f.root/'state/tasks'))
    def deny(self,args=None):
        with self.assertRaises((a.AdmissionDenied,ValueError,OSError)):a.validate(args or self.f.args,d.DATA_DIR)
    def test_valid_exact_coding(self):self.assertEqual(a.validate(self.f.args,d.DATA_DIR)[1],self.f.grant)
    def test_latch_always_precedes_valid_grant(self):
        with patch.dict(os.environ,{e.ENV:'1'}),self.assertRaises(e.ExternalEffectBlocked):a.validate(self.f.args)
    def test_default_deny_without_policy(self):
        with patch.dict(os.environ,{a.ENV:''}):self.deny()
    def test_default_deny_without_grant(self):self.f.file.unlink();self.deny()
    def test_other_request_denied(self):self.deny(dict(self.f.args,request_id='f'*64))
    def test_prompt_changed_denied(self):self.deny(dict(self.f.args,prompt='different'))
    def test_workspace_symlink_denied(self):
        (self.f.repo/'escape').symlink_to(self.f.root/'settings');self.deny()
    def test_parent_symlink_denied(self):
        link=self.f.root/'alias';link.symlink_to(self.f.root/'workspaces');self.f.grant['workspace']=str(link/'repo');self.f.save();self.deny()
    def test_root_itself_denied(self):
        self.f.pc['workspace_root']=str(self.f.repo);write_json(self.f.policy,self.f.pc);self.deny()
    def test_repository_replaced_denied(self):
        self.f.grant['repository_identity']['git_ino']+=1;self.f.save();self.deny()
    def test_registry_changed_denied(self):
        cfg=dict(self.f.cfg,chief_required=False);write_json(self.f.registry,{'test':cfg});self.deny()
    def test_cursor_and_model_fallback_denied(self):
        for fields in ({'agent':'cursor'},{'model':'gpt-5.6-sol'},{'effort':'low'}):self.deny(dict(self.f.args,**fields))
    def test_missing_v4_binding_denied(self):self.deny(dict(self.f.args,repository_binding=None))
    def test_remote_and_wildcard_grants_denied(self):
        for key,value in [('machine','firefly'),('project','*'),('operations',['coding','review','push']),('agents',{'coding':'auto','review':'codex'})]:
            old=copy.deepcopy(self.f.grant);self.f.grant[key]=value;self.f.save();self.deny();self.f.grant=old
    def test_expiration_denied(self):self.f.grant.update(not_before=time.time()-100,expires_at=time.time()-1);self.f.save();self.deny()
    def test_future_grant_denied(self):self.f.grant.update(not_before=time.time()+10);self.f.save();self.deny()
    def test_unbounded_grant_denied(self):self.f.grant['limits']['coding']=481;self.f.save();self.deny()
    def test_private_mode_required(self):self.f.file.chmod(0o644);self.deny()
    def test_duplicate_json_denied(self):
        self.f.file.write_text(self.f.file.read_text()[:-1]+',"schema_version":1}');self.deny()
    def test_missing_owner_provenance_denied(self):self.f.grant['owner']['reference']='';self.f.save();self.deny()
    def test_grant_mutation_revokes_context(self):
        with a.execution(self.f.args):
            self.f.grant['owner']['reference']='CHANGED';self.f.save()
            with self.assertRaises(a.AdmissionDenied):e.refuse('worker')
    def test_legacy_started_never_acquires_permission(self):
        path=d.DATA_DIR.parent/'requests'/(self.f.args['request_id']+'.json');write_json(path,{'status':'STARTED','task_id':self.f.args['request_id'][:24]})
        before=path.read_bytes();self.deny();self.assertEqual(path.read_bytes(),before)
    def test_reservation_crash_is_not_relaunchable(self):
        with a.execution(self.f.args):
            a.reserve(d.DATA_DIR)
            with self.assertRaises(a.AdmissionDenied):a.reserve(d.DATA_DIR)
    def test_supervisor_generation_and_single_execution(self):
        with a.execution(self.f.args) as ticket:
            a.reserve(d.DATA_DIR);a.dispatched(d.DATA_DIR,'generation')
        with self.assertRaises(a.AdmissionDenied):a.supervisor(ticket,'WRONG')
        end=a.supervisor(ticket,'generation');self.assertGreater(end,0)
        with self.assertRaises(a.AdmissionDenied):a.supervisor(ticket,'generation')
    def test_latch_reenabled_denies_reserved_execution(self):
        with a.execution(self.f.args) as ticket:a.reserve(d.DATA_DIR);a.dispatched(d.DATA_DIR,'generation')
        with patch.dict(os.environ,{e.ENV:'1'}),self.assertRaises(e.ExternalEffectBlocked):a.supervisor(ticket,'generation')
    def test_no_execution_context_denied(self):
        with self.assertRaises(a.AdmissionDenied):e.refuse('worker')
    def test_approval_not_publication_permission(self):
        with a.execution(self.f.args):
            for kind in ('publication','github','telegram'):
                with self.assertRaises(a.AdmissionDenied):e.refuse(kind)
    def test_pending_effect_unchanged_no_callbacks(self):
        import bridge_effects
        state={'issues':{'1':{'owner_state':'READY_FOR_OWNER','owner_approved_at':'synthetic'}},'outbox':{'old':dict(number=1,kind='telegram',payload='x',status='PENDING',attempts=0,next_attempt=0)}}
        before=copy.deepcopy(state);callbacks=Mock()
        bridge_effects.flush(state,callbacks,callbacks,callbacks,callbacks,callbacks)
        self.assertEqual(state,before);callbacks.assert_not_called()
    def test_unsafe_git_config_denied(self):
        subprocess.check_call(['git','-C',str(self.f.repo),'config','core.fsmonitor','malicious']);self.deny()
    def test_second_writer_claim_only_once(self):
        from concurrent.futures import ThreadPoolExecutor
        def claim():
            with a.execution(self.f.args):
                try:a.reserve(d.DATA_DIR);return 'RESERVED'
                except a.AdmissionDenied:return 'DENIED'
        with ThreadPoolExecutor(2) as pool:results=list(pool.map(lambda _:claim(),range(2)))
        self.assertEqual(sorted(results),['DENIED','RESERVED'])

    def test_active_hooks_denied(self):
        (self.f.repo/'.git/hooks/post-index-change').write_text('malicious')
        self.deny()
    def test_grant_state_binding_denied(self):
        with self.assertRaises(a.AdmissionDenied):a.validate(self.f.args,self.f.root/'other/tasks')
    def test_generation_stale_review_denied(self):
        key=self.f.grant['requests']['review']
        self.deny(dict(self.f.args,role='review',request_id=key,review_binding={'source_generation':'stale'}))
    def test_direct_worker_boundary_requires_reservation(self):
        import worker_runtime
        called=Mock()
        with a.execution(self.f.args),self.assertRaises((a.AdmissionDenied,OSError)):
            worker_runtime.launch(d.DATA_DIR,self.f.args['request_id'],self.f.args['request_id'][:24],called,signature='fixture')
        called.assert_not_called();self.assertFalse(list((self.f.root/'state/requests').glob('*.json')))
    def test_deep_request_identity_denied(self):
        with a.execution(self.f.args),self.assertRaises(a.AdmissionDenied):a.require_request(d.DATA_DIR,'f'*64,'f'*24)
    def test_supervisor_expired_grant_no_exec(self):
        with a.execution(self.f.args) as ticket:a.reserve(d.DATA_DIR);a.dispatched(d.DATA_DIR,'g')
        with patch.object(a.time,'time',return_value=self.f.grant['expires_at']+1),self.assertRaises(a.AdmissionDenied):a.supervisor(ticket,'g')
    def test_missing_owner_decision_cannot_unblock_effect(self):
        from publication import Repository
        with self.assertRaises(a.AdmissionDenied):Repository('test').git('commit','-m','denied')
    def test_reconciliation_never_launches_legacy_worker(self):
        import remote_worker
        with self.assertRaises(a.AdmissionDenied):remote_worker.start('firefly','legacy','unused')

    def test_explicit_auto_fallback_rejected(self):
        for field in ('agent','model','effort'):
            self.deny(dict(self.f.args,**{field:'auto'}))
    def test_continuation_rpc_cannot_reuse_start_grant(self):
        with self.assertRaises(a.AdmissionDenied):a.validate_rpc('continue_task',self.f.args)
    def test_second_task_remains_blocked_with_valid_first(self):
        self.assertTrue(a.issue_allowed('owner/test','1'));self.assertFalse(a.issue_allowed('owner/test','2'))

    def test_hardlink_escape_denied(self):
        external=self.f.root/'settings/private-fixture';external.write_text('not-a-real-secret')
        os.link(external,self.f.repo/'hardlink');self.deny()

    def test_execution_grant_not_bulk_maintenance_permission(self):
        with a.execution(self.f.args):
            self.assertTrue(a.operation_allowed('coding'))
            for op in ('reconcile','legacy_launch','shell','remote','push'):
                self.assertFalse(a.operation_allowed(op))
                with self.assertRaises(a.AdmissionDenied):a.require_operation(op)
    def test_default_deny_legacy_list_read_does_not_reconcile(self):
        with patch.object(d,'reconcile_workers') as reconcile:
            with __import__('contextlib').redirect_stdout(__import__('io').StringIO()):d.list_tasks(as_json=True)
            reconcile.assert_not_called()

    def test_valid_new_grant_never_enrolls_legacy_issue_task(self):
        self.assertFalse(a.issue_allowed('owner/test','1',{'task_id':'old-coding','project':'test'}))
        self.assertFalse(a.issue_allowed('owner/test','1',{'safety_latch_hold':{'reason':'sticky'}}))
        self.assertFalse(a.issue_allowed('owner/test','1',{'compatibility_status':'QUARANTINED'}))
        self.assertFalse(a.issue_allowed('owner/test','1',{'pending_rpc':{'name':'continue_task','arguments':{}}}))

    def test_owner_draft_cannot_grant_or_install_admission(self):
        import admissionctl
        from test_bridge_integration import Harness
        issue=self.f.root/'settings/issue.json';write_json(issue,Harness.issue(None,1))
        directory=self.f.root/'drafts';directory.mkdir(mode=0o700)
        before=list((self.f.root/'settings/grants').iterdir())
        result=admissionctl.draft(self.f.policy,issue,'owner/test','PENDING_SYNTHETIC_OWNER',3600,directory/'draft.json')
        self.assertFalse(result['owner']['approved']);self.assertEqual(list((self.f.root/'settings/grants').iterdir()),before)
        with self.assertRaises(a.AdmissionDenied):admissionctl.draft(self.f.policy,issue,'owner/test','PENDING',3600,self.f.file)

    def test_unapproved_preflight_never_probes_runtime(self):
        self.f.file.unlink()
        with patch.object(d,'capture_execution') as probe,self.assertRaises(a.AdmissionDenied):
            d.preflight_task('start_task',self.f.args)
        probe.assert_not_called()

    def test_discovery_not_implied_by_execution_grant(self):
        with a.execution(self.f.args),patch.object(d,'capture_execution') as probe,self.assertRaises(a.AdmissionDenied):
            d.execution_catalog('test',discover=True)
        probe.assert_not_called()

    def test_preflight_latch_precedes_runtime_probe(self):
        with patch.dict(os.environ,{e.ENV:'1'}),patch.object(d,'capture_execution') as probe,self.assertRaises(e.ExternalEffectBlocked):
            d.preflight_task('start_task',self.f.args)
        probe.assert_not_called()

    def test_historical_preflight_replay_observational_only(self):
        write_json(d.DATA_DIR.parent/'requests'/(self.f.args['request_id']+'.json'),{'status':'STARTED'})
        self.f.file.unlink()
        with patch.object(d,'capture_execution') as probe:
            self.assertEqual(d.preflight_task('start_task',self.f.args),{'status':'REPLAY'})
        probe.assert_not_called()

    def test_bridge_scope_denied_before_project_identity_rpc(self):
        import task_bridge as b
        item={'project':'test'}
        with patch.object(b,'WORK_CONTEXT',({},'1',item)),patch.object(b,'REPO','owner/test'),\
             patch.object(b,'observation_barrier',return_value=False),patch.object(b,'io_call') as io,\
             patch.object(b.review_transitions,'holds_repository',return_value=False),\
             self.assertRaises(a.AdmissionDenied):
            asyncio.run(b.mcp_call('start_task',dict(self.f.args,project='not-approved')))
        io.assert_not_called()

    def test_valid_scope_allows_exact_local_identity_only(self):
        a.validate_scope('start_task',self.f.args)
        self.f.cfg['machine']='firefly';write_json(self.f.registry,{'test':self.f.cfg})
        with self.assertRaises(a.AdmissionDenied):a.validate_scope('start_task',self.f.args)

    def test_repository_walk_budget_fails_closed(self):
        import native_deadlines
        start=native_deadlines.clock()
        with patch.object(native_deadlines,'clock',side_effect=[start,start,start+4]),self.assertRaises(a.AdmissionDenied):
            a.repository(self.f.repo,self.f.root/'workspaces')

    def test_repository_validation_clamped_to_role_deadline(self):
        import native_deadlines
        with a.validation_budget(native_deadlines.clock()-1),self.assertRaises(__import__('native_process').DeadlineReached):
            a.repository(self.f.repo,self.f.root/'workspaces')

    def test_repository_git_probes_share_finite_budget(self):
        seen=[]
        original=a.subprocess.check_output
        def probe(*args,**kwargs):
            seen.append(kwargs.get('timeout'))
            return original(*args,**kwargs)
        with patch.object(a.subprocess,'check_output',side_effect=probe):a.repository(self.f.repo,self.f.root/'workspaces')
        self.assertEqual(len(seen),5)
        self.assertTrue(all(0<x<=3 for x in seen))
        self.assertEqual(seen,sorted(seen,reverse=True))

    def test_deadline_callback_before_exec_no_inference_cleanup(self):
        import native_process,native_deadlines
        folder=self.f.root/'runtime/deadline-probe'
        def expired():raise native_process.DeadlineReached('synthetic deadline boundary')
        result=native_process.run(['/usr/bin/python3','-c','raise AssertionError("must not execute")'],folder,
            deadline=native_deadlines.clock()+5,env={'PATH':'/usr/bin:/bin'},before_exec=expired)
        self.assertEqual(result['state'],'TIMEOUT');self.assertTrue(result['cleanup_proven'])
        self.assertTrue(all(s['verified'] for s in result['signals']))

    def test_repository_probe_role_timeout_distinguished_from_unknown(self):
        import native_deadlines,native_process
        start=native_deadlines.clock()
        with patch.object(native_deadlines,'clock',return_value=start) as timer:
            def timeout(*args,**kwargs):
                timer.return_value=start+2
                raise subprocess.TimeoutExpired(args[0],kwargs['timeout'])
            with a.validation_budget(start+1),patch.object(a.subprocess,'check_output',side_effect=timeout),self.assertRaises(native_process.DeadlineReached):
                a.repository(self.f.repo,self.f.root/'workspaces')

    def test_unknown_callback_never_becomes_timeout_or_success(self):
        import native_process,native_deadlines
        def unknown():raise ValueError('synthetic identity uncertainty')
        result=native_process.run(['/usr/bin/python3','-c','raise AssertionError("must not execute")'],self.f.root/'runtime/unknown-probe',
            deadline=native_deadlines.clock()+5,env={'PATH':'/usr/bin:/bin'},before_exec=unknown)
        self.assertEqual(result['state'],'UNKNOWN');self.assertIsNone(result['returncode'])
        self.assertTrue(result['cleanup_proven']);self.assertTrue(all(s['verified'] for s in result['signals']))

    def test_boolean_predicates_deny_when_latched_without_raising(self):
        with patch.dict(os.environ,{e.ENV:'1'}):
            self.assertFalse(a.issue_allowed('owner/test','1'))
            self.assertFalse(a.available());self.assertFalse(a.task_allowed(self.f.grant['workflow_id'][:24]))
            self.assertFalse(a.operation_allowed('coding'))

    def test_consumed_contract_and_claim_rechecked_at_real_exec_gate(self):
        import native_process,native_deadlines
        with a.execution(self.f.args) as ticket:a.reserve(d.DATA_DIR);a.dispatched(d.DATA_DIR,'g')
        until=a.supervisor(ticket,'g')
        marker=self.f.repo/'must-not-execute'
        def changed():
            self.f.grant['owner']['reference']='CHANGED_VALID_CONTRACT';self.f.save()
            a.require_running(ticket,'g',until)
        result=native_process.run(['/usr/bin/python3','-c',"from pathlib import Path;Path("+repr(str(marker))+ ").write_text('bad')"],self.f.root/'runtime/contract-gate',
            deadline=native_deadlines.clock()+5,env={'PATH':'/usr/bin:/bin'},before_exec=changed)
        self.assertEqual(result['state'],'UNKNOWN');self.assertTrue(result['cleanup_proven']);self.assertFalse(marker.exists())
        self.assertTrue(all(s['verified'] for s in result['signals']))

    def test_consumed_generation_status_and_budget_cannot_change(self):
        with a.execution(self.f.args) as ticket:a.reserve(d.DATA_DIR);a.dispatched(d.DATA_DIR,'g')
        until=a.supervisor(ticket,'g');path=d.DATA_DIR.parent/'admissions'/self.f.grant['workflow_id']/'claim.json'
        original=a.strict(path)
        for k,v in [('generation','other'),('status','TERMINAL'),('execution_deadline',until+10),('grant_digest','f'*64)]:
            write_json(path,dict(original,**{k:v}))
            with self.assertRaises(a.AdmissionDenied):a.require_running(ticket,'g',until)
            write_json(path,original)
        a.require_running(ticket,'g',until)

    def test_retired_legacy_supervisor_and_delayed_dispatch_deny(self):
        import runtime_liveness as legacy
        directory=self.f.root/'state/tasks/legacy';write_json(directory/'liveness.json',{'generation':'historical'})
        marker=self.f.repo/'legacy-executed'
        command='/usr/bin/python3 -c '+__import__('shlex').quote("from pathlib import Path;Path("+repr(str(marker))+ ").write_text('bad')")
        for mode in ('no-policy','execution-grant'):
            with patch.dict(os.environ,{a.ENV:''} if mode=='no-policy' else self.f.env()):
                context=__import__('contextlib').nullcontext() if mode=='no-policy' else a.execution(self.f.args)
                with context,patch('worker_runtime.record_group') as launch,self.assertRaises(a.AdmissionDenied):legacy.run(directory,'historical',command)
                launch.assert_not_called()
                result=subprocess.run(['/usr/bin/python3','-B',str(Path(legacy.__file__)),str(directory),'historical',command],capture_output=True,text=True,timeout=5)
                self.assertNotEqual(result.returncode,0);self.assertFalse(marker.exists())
        self.assertEqual(a.strict(directory/'liveness.json'),{'generation':'historical'})
        self.assertFalse((directory/'process_group.json').exists())

    def test_archive_unarchive_and_lifecycle_never_gain_maintenance(self):
        import lifecycle
        directory=d.DATA_DIR/'legacy';write_json(directory/'meta.json',{'id':'legacy','status':'UNKNOWN','archived':False})
        before=(directory/'meta.json').read_bytes()
        for mode in ('latched','no-grant','execution-grant'):
            with patch.dict(os.environ,{e.ENV:'1'} if mode=='latched' else {e.ENV:'0'}):
                context=a.execution(self.f.args) if mode=='execution-grant' else __import__('contextlib').nullcontext()
                with context:
                    for operation in (lambda:d.set_task_archived('legacy',True),lambda:d.set_task_archived('legacy',False),
                        lambda:lifecycle.apply({},d.DATA_DIR,[{'action':'archive_task','task_id':'legacy'}],Mock(),Mock())):
                        with self.assertRaises((a.AdmissionDenied,e.ExternalEffectBlocked)):operation()
                    for args in (['--apply'],[]):
                        with patch.object(__import__('sys'),'argv',['lifecycle',*args,'--state-dir',str(d.DATA_DIR.parent)]),self.assertRaises((a.AdmissionDenied,e.ExternalEffectBlocked)):lifecycle.main()
            self.assertEqual((directory/'meta.json').read_bytes(),before)
            self.assertFalse((d.DATA_DIR.parent/'bridge.lock').exists());self.assertFalse((d.DATA_DIR.parent/'worker.lock').exists())
            self.assertFalse((d.DATA_DIR/'archives').exists())

    def test_direct_remote_script_denied_before_state_or_child_creation(self):
        import remote_worker
        home=self.f.root/'remote-home';home.mkdir(mode=0o700)
        for mode in ('start','report'):
            env=dict(os.environ,HOME=str(home))
            result=subprocess.run(['/usr/bin/python3','-c',remote_worker.SCRIPT,mode,'denied-legacy','true' if mode=='start' else '0'],env=env,capture_output=True,text=True,timeout=5)
            self.assertNotEqual(result.returncode,0)
            self.assertFalse((home/'.local/share/devorchestrator/remote-tasks').exists())

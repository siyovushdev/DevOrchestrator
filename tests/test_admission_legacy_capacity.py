"""Admission never grants legacy maintenance, lease replacement or signalling."""
from contextlib import ExitStack
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock,patch

import admission_policy as a
import devctl as d
from durable import read_json,write_json,exclusive
import worker_runtime as w
from test_admission_policy import PolicyFixture

class LegacyCapacityTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.f=PolicyFixture(Path(self.tmp.name));self.stack=ExitStack();self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.dict(os.environ,self.f.env()))
        self.stack.enter_context(patch.object(d,'DATA_DIR',self.f.root/'state/tasks'))
    def legacy(self,name,status='UNKNOWN',exit_code=None,**extra):
        directory=d.DATA_DIR/name
        meta=dict(id=name,machine='work',agent='codex',pid=0,status=status,role='coding',**extra)
        write_json(directory/'meta.json',meta)
        if exit_code is not None:(directory/'exit_code').write_text(str(exit_code))
        return directory,meta
    def protected(self):return {str(p):p.read_bytes() for p in (self.f.root/'state').rglob('*') if p.is_file()}
    def test_capacity_legacy_unknown_quarantine_hold_terminal_are_observational(self):
        rows=[self.legacy('unknown'),self.legacy('quarantine',compatibility_status='QUARANTINED'),
              self.legacy('held',safety_latch_hold={'reason':'historical'}),self.legacy('terminal','DONE',0)]
        write_json(self.f.root/'state/worker_leases.json',{'old':{'task_id':'unknown','machine':'work'}})
        before=self.protected();state=Mock(side_effect=AssertionError('No mutable legacy status reader'))
        with a.execution(self.f.args),patch.object(os,'killpg',side_effect=AssertionError('No legacy signals')):
            for directory,meta in rows:
                self.assertFalse(a.reconciliation_allowed(directory,meta))
                w.reconcile_task(directory,meta,'FAILED',1);w.cleanup_group(directory,meta,'FAILED')
            with patch.dict(os.environ,{'DEVORCHESTRATOR_MAX_CODING':'10','DEVORCHESTRATOR_MAX_AGENTS':'10','DEVORCHESTRATOR_MACHINE_LIMITS':'{"work":10}'}):
                w.check_capacity(d.DATA_DIR,self.f.grant['workflow_id'][:24],'work','coding',state)
            w.reconcile_leases(d.DATA_DIR,{n:{'machine':'work','cleanup':'ABSENT'} for n in ('unknown','terminal','quarantine','held')})
        self.assertEqual(self.protected(),before);state.assert_not_called()
    def test_even_terminal_unadmitted_same_repository_lease_is_never_replaced(self):
        directory,meta=self.legacy('terminal','DONE',0)
        import hashlib
        key=hashlib.sha256(('work\0'+self.f.binding['repository']).encode()).hexdigest()
        write_json(self.f.root/'state/worker_leases.json',{key:{'task_id':'terminal','machine':'work'}})
        before=self.protected()
        with a.execution(self.f.args),patch.object(w,'reconcile_task',side_effect=AssertionError('No legacy reconciliation')):
            with self.assertRaisesRegex(RuntimeError,'unadmitted'):w.admit(d.DATA_DIR,'work',self.f.binding['repository'],self.f.grant['workflow_id'][:24],Mock())
        self.assertEqual(self.protected(),before)
    def test_unproved_legacy_cleanup_occupies_capacity_without_writes(self):
        self.legacy('terminal','DONE',0);before=self.protected()
        with a.execution(self.f.args),self.assertRaisesRegex(RuntimeError,'capacity'):w.check_capacity(d.DATA_DIR,self.f.grant['workflow_id'][:24],'work','coding',Mock())
        self.assertEqual(self.protected(),before)
    def test_finite_lock_respects_inherited_preparation_deadline(self):
        import time
        from native_deadlines import clock
        path=self.f.root/'state/contended.lock'
        with exclusive(path),a.validation_budget(clock()+.15):
            start=time.monotonic()
            with self.assertRaises(a.AdmissionDenied):
                with a.finite_lock(path):self.fail('Lock unexpectedly acquired')
            self.assertLess(time.monotonic()-start,.5)

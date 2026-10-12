"""Coherent legacy backup copies: observation cannot mint launch authority."""
import asyncio
import copy
import hashlib
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch,AsyncMock
import admission_policy as a
import devctl as d
import external_effects as e
import task_bridge as b
import worker_runtime as w
from control_loop import ControlLoop
from durable import read_json

class MigrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_legacy_queues_decisions_holds_quarantine_preserved(self):
        original=Path(os.environ['HOME'])/'compat-samples'
        self.assertTrue(original.is_dir(),'Verified coherent backup fixture required; cannot skip compatibility')
        state=read_json(original/'task_bridge_state.json');before=copy.deepcopy(state)
        with patch.dict(os.environ,{a.ENV:'',e.ENV:'0'}),patch.object(b,'observe_latched_poll',new_callable=AsyncMock) as observe:
            loop=ControlLoop(state)
            await loop.step();await loop.close()
            observe.assert_awaited_once()
        self.assertEqual(state,before)
        for key in ('issues','work_queue','repo_locks','outbox'):self.assertEqual(state[key],before[key])
    async def test_all_legacy_receipts_readonly_unknown_no_reexecution(self):
        original=Path(os.environ['HOME'])/'compat-samples'
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);shutil.copytree(original/'tasks',root/'tasks');shutil.copytree(original/'requests',root/'requests')
            paths=list((root/'requests').glob('*.json'))+list((root/'tasks').rglob('meta.json'))
            before={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
            counts={}
            with patch.dict(os.environ,{a.ENV:'',e.ENV:'0'}),patch.object(d,'DATA_DIR',root/'tasks'):
                for p in (root/'requests').glob('*.json'):
                    observed=w.inspect_start_request(root/'tasks',p.stem,fence=False)
                    counts[observed['state']]=counts.get(observed['state'],0)+1
                    with self.assertRaises(a.AdmissionDenied):a.grant_for(p.stem)
            self.assertEqual(len(list((root/'requests').glob('*.json'))),88)
            self.assertEqual(counts,{'STARTED':55,'UNKNOWN':33})
            self.assertEqual(before,{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths})
            self.assertFalse((root/'admissions').exists())

    async def test_latched_populated_legacy_control_loop_startup_no_mutation(self):
        state=read_json(Path(os.environ['HOME'])/'compat-samples/task_bridge_state.json');before=copy.deepcopy(state)
        with patch.dict(os.environ,{a.ENV:'',e.ENV:'1'}),patch.object(b,'observe_latched_poll',new_callable=AsyncMock) as observe:
            loop=ControlLoop(state);await loop.step();await loop.close();observe.assert_awaited_once()
        self.assertEqual(state,before)

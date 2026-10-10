"""Authoritative fsync failures stop the writer before another external dispatch.

File fsync happens before rename, so a failure leaves the previous checkpoint.
Directory fsync happens after rename; the new checkpoint may already be visible,
but the same step must still not submit further work. DurabilityFailure is a
BaseException, so the production retry handler does not continue from dirty memory.
"""
import unittest
from unittest.mock import patch

import durable
import task_bridge as bridge
import test_bridge_integration as integration
from control_loop import ControlLoop
from review_coordinator import DurabilityFailure
from transition_io import Command


class AcceptanceFsyncTests(unittest.IsolatedAsyncioTestCase):
    setUp = integration.BridgeIntegrationTests.setUp

    def prepare(self):
        self.h.state['issues']['1'] = {'queue_status': 'QUEUED'}
        bridge.save_state(self.h.state)
        self.loop = ControlLoop(self.h.state)
        self.addAsyncCleanup(self.loop.close)
        self.before = bridge.STATE_FILE.read_bytes()
        self.completed = ('control:1', Command.create('control_read', {'number': '1'}, lane='control'),
                          dict(issue=dict(self.h.issues[1], state='CLOSED'), comments=[]), None)

    async def step_with(self, fsync):
        with (patch.object(self.loop.dispatcher, 'collect', return_value=[self.completed]),
              patch.object(self.loop.dispatcher, 'submit') as submit,
              patch.object(durable.os, 'fsync', side_effect=fsync)):
            with self.assertRaises(DurabilityFailure):
                await self.loop.step()
            submit.assert_not_called()
        return submit

    async def test_file_fsync_failure_keeps_checkpoint_and_blocks_dispatch(self):
        self.prepare()
        real = durable.os.fsync

        def fail_file(fd):
            raise OSError('file fsync failed')

        await self.step_with(fail_file)
        self.assertEqual(bridge.STATE_FILE.read_bytes(), self.before)
        self.assertIs(real, durable.os.fsync)

    async def test_directory_fsync_failure_after_replace_still_blocks_dispatch(self):
        self.prepare()
        real = durable.os.fsync
        calls = {'n': 0}

        def fail_directory(fd):
            calls['n'] += 1
            if calls['n'] >= 2:
                raise OSError('directory fsync failed')
            return real(fd)

        await self.step_with(fail_directory)
        self.assertGreaterEqual(calls['n'], 2)
        self.assertNotEqual(bridge.STATE_FILE.read_bytes(), self.before)
        restored = bridge.load_state()
        self.assertEqual(restored['issues']['1'].get('runtime_exclusion'), 'ISSUE_CLOSED')

    def test_durability_failure_is_not_retried_as_exception(self):
        self.assertTrue(issubclass(DurabilityFailure, BaseException))
        self.assertFalse(issubclass(DurabilityFailure, Exception))

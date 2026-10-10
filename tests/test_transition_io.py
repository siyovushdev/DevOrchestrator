import asyncio
from contextvars import ContextVar
import unittest

from transition_io import Command, Dispatcher, WriterContinuation, call


class TransitionIOTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_explicit_writer_advance_can_apply_completion(self):
        state = {'phase': 'PREPARE'}
        source = {'number': 1, 'nested': {'value': 'original'}}
        entered, release = asyncio.Event(), asyncio.Event()
        async def execute(command):
            self.assertEqual(command.arguments()['nested']['value'], 'original')
            entered.set()
            await release.wait()
            return {'id': 'accepted'}
        async def lifecycle():
            result = await call('slow', source)
            state.update(phase='ACCEPTED', task=result['id'])
        writer = WriterContinuation(lifecycle())
        dispatcher = Dispatcher(execute)
        command = writer.advance()
        source['nested']['value'] = 'later mutation'
        self.assertTrue(dispatcher.submit('request', command))
        await entered.wait()
        for _ in range(20):
            self.assertEqual(dispatcher.collect(), [])
            self.assertEqual(state, {'phase': 'PREPARE'})
        release.set()
        await asyncio.sleep(0)
        completed = dispatcher.collect()
        self.assertEqual(state, {'phase': 'PREPARE'})
        self.assertEqual(len(completed), 1)
        writer.advance(completed[0][2])
        self.assertTrue(writer.done)
        self.assertEqual(state, {'phase': 'ACCEPTED', 'task': 'accepted'})

    async def test_slow_cancellation_retains_slot_but_control_capacity_works(self):
        cancelling, release, entered = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async def execute(command):
            if command.lane == 'control':
                return {'polled': True}
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelling.set()
                await release.wait()
        dispatcher = Dispatcher(execute, limits={'work': 1, 'control': 1, 'effect': 1})
        self.assertTrue(dispatcher.submit('launch', Command.create('rpc', {})))
        await entered.wait()
        dispatcher.cancel('launch')
        await cancelling.wait()
        for count in range(10):
            self.assertFalse(dispatcher.submit('launch', Command.create('rpc', {})))
            self.assertFalse(dispatcher.submit('duplicate', Command.create('rpc', {})))
            self.assertTrue(dispatcher.submit('poll', Command.create('issue', {}, 'control')))
            await asyncio.sleep(0)
            self.assertEqual([r[0] for r in dispatcher.collect()], ['poll'])
        self.assertFalse(await dispatcher.close())
        release.set()
        await asyncio.sleep(0)
        result = dispatcher.collect()
        self.assertEqual(result[0][0], 'launch')
        self.assertIsInstance(result[0][3], asyncio.CancelledError)
        self.assertTrue(await dispatcher.close())

    async def test_deadline_does_not_free_slot_or_start_a_second_attempt(self):
        now = [0]
        release = asyncio.Event()
        async def execute(command):
            await release.wait()
            return None
        dispatcher = Dispatcher(execute, clock=lambda: now[0])
        command = Command.create('launch', {})
        dispatcher.submit('stable-id', command, timeout=60)
        now[0] = 120
        self.assertEqual(dispatcher.expire(), ['stable-id'])
        self.assertEqual(dispatcher.expire(), [])
        self.assertFalse(dispatcher.submit('stable-id', command))
        self.assertEqual(dispatcher.collect(), [])
        release.set()
        await asyncio.sleep(0)
        self.assertEqual(len(dispatcher.collect()), 1)

    async def test_executor_cannot_inherit_writer_context(self):
        secret = ContextVar('live_state', default=None)
        secret.set({'mutable': True})
        async def execute(command):
            self.assertIsNone(secret.get())
            return None
        dispatcher = Dispatcher(execute)
        dispatcher.submit('read', Command.create('read', {}))
        await asyncio.sleep(0)
        self.assertIsNone(dispatcher.collect()[0][3])

    async def test_arbitrary_background_await_in_lifecycle_fails_closed(self):
        async def invalid():
            await asyncio.sleep(1)
        writer = WriterContinuation(invalid())
        with self.assertRaisesRegex(RuntimeError, 'outside'):
            writer.advance()
        self.assertTrue(writer.done)

    def test_commands_reject_callbacks_and_non_json_state(self):
        with self.assertRaises(TypeError):
            Command.create('bad', {'persist': lambda: None})


if __name__ == '__main__':
    unittest.main()

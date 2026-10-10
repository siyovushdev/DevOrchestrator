"""Real SDK/HTTP test on an ephemeral loopback listener, with fake devctl I/O.

Install the repository's documented mcp==2.3.0 into the isolated .venv to run.
No production port, agent, registry, state directory or service is contacted.
"""
import asyncio
import importlib.util
import json
from pathlib import Path
import socket
import threading
import unittest
from unittest.mock import patch

try:
    from mcp import Client
    import uvicorn
except ImportError:
    Client = None


@unittest.skipIf(Client is None, 'Requires isolated mcp==2.3.0 test environment')
class MCPHTTPConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_two_blocked_launches_leave_real_http_report_rpc_responsive(self):
        spec = importlib.util.spec_from_file_location('http_test_mcp_adapter',
            Path(__file__).resolve().parents[1] / 'mcp_server.py')
        adapter = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(adapter)
        entered, release = threading.Event(), threading.Event()
        starts = []
        def devctl(*args, **kwargs):
            if args[0] == 'run-project':
                starts.append(args)
                if len(starts) == 2:
                    entered.set()
                if not release.wait(10):
                    raise TimeoutError('Test release was not signalled')
                return 'test-task'
            if args[0] == 'report':
                return json.dumps(dict(id='test-task', status='RUNNING', role='review'))
            raise AssertionError('Unexpected test I/O')
        sock = socket.socket()
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
        app = adapter.mcp.streamable_http_app()
        server = uvicorn.Server(uvicorn.Config(app, log_level='critical', lifespan='on'))
        running = asyncio.create_task(server.serve(sockets=[sock]))
        launches = []
        try:
            for _ in range(200):
                if server.started:
                    break
                await asyncio.sleep(.005)
            self.assertTrue(server.started)
            with patch.object(adapter, 'load_projects', return_value={'test': {'machine': 'work', 'default_agent': 'codex'}}), \
                 patch.object(adapter, 'devctl', side_effect=devctl):
                async with Client(f'http://127.0.0.1:{port}/mcp') as client:
                    launches = [asyncio.create_task(client.call_tool('start_task', dict(project='test', prompt='fixture',
                        request_id=str(index) * 64, role='review'))) for index in (1, 2)]
                    await asyncio.wait_for(asyncio.to_thread(entered.wait, 3), 4)
                    self.assertTrue(entered.is_set())
                    report = await asyncio.wait_for(client.call_tool('get_task_report', {'task_id': 'test-task'}), 2)
                    import task_bridge
                    self.assertEqual(task_bridge.decode_mcp_result('get_task_report', report)['id'], 'test-task')
                    self.assertTrue(all(not task.done() for task in launches))
                    release.set()
                    await asyncio.gather(*launches)
        finally:
            release.set()
            if launches:
                await asyncio.gather(*launches, return_exceptions=True)
            server.should_exit = True
            await asyncio.wait_for(running, 5)
            sock.close()
            for pool in adapter.TOOL_POOLS.values():
                pool.shutdown()


if __name__ == '__main__':
    unittest.main()

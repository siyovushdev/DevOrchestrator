"""Copied I/O commands and a cooperative, single-writer continuation driver.

Only WriterContinuation advances lifecycle code. Executor tasks receive a frozen
JSON command, never a coroutine frame, item, scheduler or persistence callback.
An expired/cancelling task keeps its slot until it actually finishes. Cancellation
is an observation, never evidence that a mutating request did not reach a worker.
"""
import asyncio
from contextvars import ContextVar, Context, copy_context
from dataclasses import dataclass
import json
import time


DRIVING = ContextVar('lifecycle_writer_driving', default=False)
LATEST_ISSUE = ContextVar('latest_writer_issue_observation', default=None)


@dataclass(frozen=True)
class Command:
    name: str
    payload: str
    lane: str = 'work'
    authorization: str | None = None

    @classmethod
    def create(cls, name, arguments, lane='work', authorization=None):
        if lane not in ('control', 'work', 'effect', 'notification', 'control_rpc'):
            raise ValueError('Unknown I/O lane')
        return cls(name, json.dumps(arguments, sort_keys=True, allow_nan=False), lane,
                   json.dumps(authorization, sort_keys=True, allow_nan=False) if authorization is not None else None)

    def arguments(self):
        return json.loads(self.payload)

    def __await__(self):
        result = yield self
        return result


async def call(name, arguments, *, lane='work', authorization=None):
    command = Command.create(name, arguments, lane, authorization)
    if DRIVING.get():
        return await command
    # Compatibility callers still execute serially; production uses the driver.
    from transition_executor import execute
    return await execute(command)


class WriterContinuation:
    """Suspended frames stay on the writer; they are never scheduled as Tasks."""
    def __init__(self, coroutine):
        self.coroutine = coroutine
        self.context = copy_context()
        self.context.run(DRIVING.set, True)
        self.command = None
        self.done = False
        self.result = None

    def advance(self, value=None, error=None):
        if self.done:
            raise RuntimeError('Completed lifecycle continuation cannot resume')
        try:
            yielded = self.context.run(self.coroutine.throw, error) if error is not None else self.context.run(
                self.coroutine.send, value)
        except StopIteration as exc:
            self.done, self.result, self.command = True, exc.value, None
            return None
        except BaseException:
            self.done = True
            raise
        if not isinstance(yielded, Command):
            # Do not allow a Future/Task to secretly resume live lifecycle code.
            self.done = True
            self.context.run(self.coroutine.close)
            raise RuntimeError('Lifecycle code awaited outside the detached I/O boundary')
        self.command = yielded
        return yielded


def run_inline(coroutine):
    """Synchronous compatibility for tools/tests using the same writer logic."""
    from transition_executor import synchronous
    writer = WriterContinuation(coroutine)
    value, error = None, None
    while not writer.done:
        command = writer.advance(value, error)
        if command is None:
            break
        try:
            value, error = synchronous(command.name, command.arguments()), None
        except Exception as exc:
            value, error = None, exc
    return writer.result


@dataclass
class Flight:
    command: Command
    task: asyncio.Task
    deadline: float
    expired: bool = False


class Dispatcher:
    def __init__(self, execute, *, clock=time.monotonic, limits=None):
        self.execute = execute
        self.clock = clock
        self.limits = limits or {'control': 2, 'work': 2, 'effect': 1, 'notification': 2, 'control_rpc': 1}
        self.flights = {}
        self.closed = False

    def submit(self, key, command, *, timeout=60):
        if self.closed or key in self.flights:
            return False
        if sum(f.command.lane == command.lane for f in self.flights.values()) >= self.limits[command.lane]:
            return False
        # Empty context prevents WORK_CONTEXT-like context variables crossing
        # into workers. execute is a static I/O adapter, not a writer closure.
        task = asyncio.create_task(self.execute(command), context=Context())
        self.flights[key] = Flight(command, task, self.clock() + timeout)
        return True

    def collect(self):
        """Writer consumes only finished operations; no awaiting cancellation."""
        results = []
        for key, flight in list(self.flights.items()):
            if not flight.task.done():
                continue
            del self.flights[key]
            try:
                value = flight.task.result()
                # Executor results cannot leak references to other callers.
                value = json.loads(json.dumps(value, allow_nan=False))
                results.append((key, flight.command, value, None))
            except BaseException as exc:
                results.append((key, flight.command, None, exc))
        return results

    def expire(self):
        """Return new deadline observations; retain ownership until completion.

        Transport timeouts perform their own cancellation. Cancelling to_thread
        here would lose track of still-running subprocess/SSH work.
        """
        expired = []
        for key, flight in self.flights.items():
            if not flight.expired and self.clock() >= flight.deadline:
                flight.expired = True
                expired.append(key)
        return expired

    def cancel(self, key):
        # Explicit cancellation is supported for cooperative transports only.
        # Even a slow cancellation cleanup keeps its slot and request ownership.
        flight = self.flights.get(key)
        if flight:
            flight.task.cancel()

    async def close(self, *, grace=0):
        self.closed = True
        tasks = [flight.task for flight in self.flights.values()]
        # Do not cancel threaded operations and pretend that they stopped.
        if tasks and grace:
            await asyncio.wait(tasks, timeout=grace)
        return not any(not task.done() for task in tasks)

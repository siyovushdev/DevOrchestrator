"""Event-driven control polling with one lifecycle writer and detached I/O.

Each issue has at most one live writer continuation. Other issues and command
polling remain runnable while it waits. Reviewer launch uses durable phases;
legacy worker requests and publication use their existing journals on restart.
"""
import asyncio
import time
from copy import deepcopy

from review_driver import ReviewDriver
from review_migration import LegacyReviewRecovery
from transition_io import Command, Dispatcher, WriterContinuation, LATEST_ISSUE, DRIVING
from transition_executor import execute


class ControlLoop:
    def __init__(self, state):
        import task_bridge as bridge
        self.state = state
        self.dispatcher = Dispatcher(execute)
        self.reviews = ReviewDriver(state, bridge.save_state, bridge.REPO, self.dispatcher)
        self.legacy_reviews = LegacyReviewRecovery(state, bridge.save_state, bridge.REPO, self.dispatcher)
        self.legacy_reviews.enroll()
        self.next_heavy = 0
        self.next_control = {}
        self.pending = []
        self.issues = {}
        self.continuations = {}
        self.waiting = {}
        self.ready = {}
        self.after_work = set()
        self.telegram_tasks = {}
        self.poll_cursor = 0
        self.writer_cursor = 0
        self.closing = False

    def dispatch_telegram(self, key, text):
        # Called by the writer after the SENDING receipt is durable. Queued I/O
        # remains represented by that receipt if the bridge stops before send.
        self.telegram_tasks[key] = Command.create('telegram', dict(text=text), lane='notification')

    def _telegram_completion(self, key, result, error):
        import task_bridge as bridge
        self.telegram_tasks.pop(key, None)
        entry = self.state['outbox'][key]
        if error is None and result is True:
            entry['status'] = 'DONE'
        elif error is None and result is False:
            entry['attempts'] += 1
            entry.update(status='FAILED' if entry['attempts'] >= 5 else 'PENDING',
                         next_attempt=time.time() + min(300, 2 ** min(entry['attempts'], 8)))
        else:
            entry.update(status='UNCERTAIN', diagnostic='Telegram acceptance uncertain; automatic resend suppressed')
        bridge.save_state(self.state)

    def _context(self, number):
        import task_bridge as bridge
        bridge.WORK_CONTEXT = (self.state, number, self.state['issues'][number])

    def _observe_control(self, number, observation):
        import task_bridge as bridge
        if bridge.observation_barrier(self.state, number):
            return
        issue = observation['issue']
        if not issue:
            return
        self.issues[number] = issue
        item = self.state['issues'][number]
        self._context(number)
        # Closure is a sticky revocation, applied by the same lifecycle writer
        # even while its frame is suspended. It does not cancel in-flight I/O.
        bridge.archive_issue(issue, item, self.state)
        # A suspended ordinary lifecycle frame owns its item until completion.
        # Intake still runs; fresh comments remain in the provider and cache.
        if number in self.continuations:
            return
        bridge.reject_unready_commands(issue, item, self.state, observation['comments'])
        if item.get('review_transition') is not None:
            bridge.archive_issue(issue, item, self.state)
            from review_driver import inputs
            from review_transitions import digest
            if digest(inputs(issue, item)) != item['review_transition']['source']['input_digest']:
                self.reviews.coordinator.invalidate(number)
            pending = []
            for comment in observation['comments']:
                if not bridge.authorized_comment(comment) or bridge.chief_review.is_evidence_comment(comment.get('body')):
                    continue
                if bridge.comment_already_processed(comment, set(item.get('processed_followups') or [])):
                    continue
                try:
                    command = bridge.parse_owner_command(comment.get('body'))
                except ValueError:
                    continue
                # A review starts only from successful coding, with no open
                # owner question. Old ANSWER commands cannot invalidate it.
                body = comment.get('body') or ''
                if (not command and bridge.FOLLOWUP_MARKER in body and
                        body.split(bridge.FOLLOWUP_MARKER, 1)[1].strip()):
                    pending.append(bridge.comment_id(comment))
            if pending:
                item['pending_followup'] = sorted(set(pending))
                self.reviews.coordinator.invalidate(number)
            bridge.save_state(self.state)
        if number not in self.pending:
            self.pending.append(number)

    def _apply_discovery(self, observation):
        import task_bridge as bridge
        issues, tasks, identities = observation['issues'], observation['tasks'], observation['identities']
        def resolved(project):
            import devctl
            config, identity = identities[project]
            if config != devctl.load_projects().get(project):
                raise ValueError('Registry changed since discovery')
            return tuple(identity)
        bridge.Scheduler(self.state).recover(tasks, identity_resolver=resolved)
        for issue in issues:
            number = str(issue['number'])
            if bridge.QUEUE_LABEL in bridge.label_names(issue) and number not in self.state['issues']:
                self.state['issues'][number] = {'queue_status': 'QUEUED'}
            if number in self.state['issues']:
                if bridge.bridge_effects.observation_held(self.state, number):
                    continue
                self.issues[number] = issue
                if number not in self.pending:
                    self.pending.append(number)
        self.state.pop('recovery_diagnostic', None)
        bridge.save_state(self.state)

    def _completion(self, key, command, value, error):
        import task_bridge as bridge
        from external_effects import read_only_observation
        # Delayed detached results cannot revive a writer held during smoke.
        if key in self.reviews.operations:
            number = self.reviews.operations[key][0]
        elif key in self.legacy_reviews.operations:
            number = self.legacy_reviews.operations[key]
        elif key.startswith(('control:', 'writer:')):
            number = key.split(':', 1)[1]
        else:
            number = None
        if read_only_observation(value):
            targets = [number] if number is not None else [
                n for n, item in self.state['issues'].items() if isinstance(item, dict) and item.get('task_id')]
            for target in targets:
                bridge.observation_barrier(self.state, target, {'observation_confidence': 'READ_ONLY'})
            return
        if number is not None and bridge.observation_barrier(self.state, number):
            return
        if key == 'discovery':
            self.next_heavy = time.monotonic() + 30
            if error is None:
                self._apply_discovery(value)
            else:
                self.state['recovery_diagnostic'] = 'Heavy discovery unavailable; targeted controls retain fresh boundary checks'
                bridge.save_state(self.state)
        elif key.startswith('control:'):
            number = key.split(':', 1)[1]
            self.next_control[number] = time.monotonic() + bridge.POLL_SECONDS
            if error is None:
                item = self.state['issues'][number]
                if isinstance(item, dict):
                    item.get('consistency_diagnostics', {}).pop('control', None)
                self._observe_control(number, value)
            else:
                item = self.state['issues'][number]
                if isinstance(item, dict):
                    item.setdefault('consistency_diagnostics', {})['control'] = 'Targeted control observation unavailable'
                    bridge.save_state(self.state)
        elif key in self.reviews.operations:
            number = self.reviews.operations[key][0]
            self._context(number)
            self.reviews.complete(key, value, error, self.issues[number])
            if self.state['issues'][number].get('review_transition') is None:
                self.after_work.add(number)
        elif key in self.legacy_reviews.operations:
            number = self.legacy_reviews.operations[key]
            self._context(number)
            self.legacy_reviews.complete(key, value, error)
            if self.state['issues'][number].get('review_migration') is None and number not in self.pending:
                self.pending.append(number)
        elif key.startswith('writer:'):
            number = key.split(':', 1)[1]
            self.waiting.pop(number, None)
            self.ready[number] = (value, error)
        elif key.startswith('telegram:'):
            self._telegram_completion(key.split(':', 1)[1], value, error)

    def _advance(self, number):
        import task_bridge as bridge
        writer = self.continuations[number]
        self._context(number)
        writer.context.run(LATEST_ISSUE.set, deepcopy(self.issues.get(number)))
        value, error = self.ready.pop(number, (None, None))
        try:
            command = writer.advance(value, error)
        except (Exception, asyncio.CancelledError) as exc:
            from external_effects import ExternalEffectBlocked
            if not isinstance(exc, ExternalEffectBlocked):
                bridge.defer_issue(self.state['issues'][number], self.state, number, exc)
            command = None
        if writer.done:
            del self.continuations[number]
            self.waiting.pop(number, None)
            return
        self.waiting[number] = command

    async def step(self):
        """No provider await here. All state application happens in this call."""
        import task_bridge as bridge
        if self.closing:
            return
        from external_effects import blocked
        if blocked():
            await bridge.observe_latched_poll(self.state)
            return
        # Control/discovery reducers are lifecycle writers too. A failed save
        # must escape the generic retry handler before any further dispatch.
        token = DRIVING.set(True)
        try:
            completed = self.dispatcher.collect()
            completed.sort(key=lambda result: not result[0].startswith('control:'))
            for result in completed:
                try:
                    self._completion(*result)
                except Exception as exc:
                    key = result[0]
                    if key.startswith(('review:', 'legacy-review:')):
                        # Invalid/unbound results keep their durable ownership.
                        field = 'review_migration' if key.startswith('legacy-review:') else 'review_transition'
                        number = next((n for n, i in self.state['issues'].items() if isinstance(i, dict) and
                            isinstance(i.get(field), dict) and i[field].get('request_id') == key.split(':', 1)[1]), None)
                        if number:
                            bridge.defer_issue(self.state['issues'][number], self.state, number, exc)
                    elif key == 'discovery':
                        self.state['recovery_diagnostic'] = 'Heavy discovery could not be safely applied; existing ownership retained'
                        bridge.save_state(self.state)
                    else:
                        raise
            for key in self.dispatcher.expire():
                if key.startswith('review:') and key in self.reviews.operations:
                    number = self.reviews.operations[key][0]
                    bridge.defer_issue(self.state['issues'][number], self.state, number, TimeoutError(), stage='start_task')
            now = time.monotonic()
            if now >= self.next_heavy:
                self.dispatcher.submit('discovery', Command.create('discovery', {}), timeout=120)
            numbers = [number for number in self.state['issues'] if number.isdigit()]
            if numbers:
                start = self.poll_cursor % len(numbers)
                for number in numbers[start:] + numbers[:start]:
                    if now >= self.next_control.get(number, 0):
                        self.dispatcher.submit('control:' + number,
                            Command.create('control_read', dict(number=number), lane='control'))
                self.poll_cursor += 1
            for number, item in list(self.state['issues'].items()):
                if not isinstance(item, dict) or not number.isdigit():
                    continue
                if bridge.bridge_effects.observation_held(self.state, number):
                    continue
                if item.get('review_migration') is not None:
                    try:
                        self.legacy_reviews.schedule(number)
                    except (ValueError, KeyError, TypeError):
                        item.setdefault('consistency_diagnostics', {})['review_migration'] = 'Legacy review envelope held for local inspection'
                        bridge.save_state(self.state)
                    continue
                if item.get('review_transition') is not None and number in self.issues:
                    self._context(number)
                    if not bridge.malformed_item(item):
                        bridge.archive_issue(self.issues[number], item, self.state)
                        self.reviews.schedule(number, self.issues[number])
                    continue
                if number in self.continuations or number not in self.issues:
                    continue
                if number in self.after_work:
                    self.after_work.remove(number)
                    self.continuations[number] = WriterContinuation(
                        bridge.flush_effects_async(self.state, owners={number},
                            telegram_dispatch=self.dispatch_telegram, telegram_inflight=self.telegram_tasks))
                elif number in self.pending:
                    self.pending.remove(number)
                    issue = self.issues[number]
                    self._context(number)
                    if bridge.archive_issue(issue, item, self.state):
                        if bridge.archived_reconciliation_pending(item):
                            self.continuations[number] = WriterContinuation(
                                bridge.reconcile_archived(issue, item, self.state))
                        continue
                    if bridge.malformed_item(item):
                        bridge.quarantine_item(issue, item, self.state, 'Malformed tracked state held for local inspection.')
                        continue
                    if item.get('reconciliation_retry', {}).get('next_attempt', 0) > time.time():
                        continue
                    bridge.reconcile_projection(issue, item, self.state)
                    if item.get('pending_followup'):
                        item.pop('pending_followup')  # Provider commands remain unconsumed for process_followup.
                    coroutine = (bridge.process_tracked(issue, item, self.state) if item.get('task_id') else
                                 bridge.start_queued(issue, self.state))
                    self.continuations[number] = WriterContinuation(coroutine)
                    self.after_work.add(number)
            ready_numbers = list(self.continuations)
            if ready_numbers:
                offset = self.writer_cursor % len(ready_numbers)
                ready_numbers = ready_numbers[offset:] + ready_numbers[:offset]
                self.writer_cursor += 1
            for number in ready_numbers:
                if bridge.bridge_effects.observation_held(self.state, number):
                    continue
                if number not in self.waiting or number in self.ready:
                    self._advance(number)
                command = self.waiting.get(number)
                key = 'writer:' + number
                if command is not None and key not in self.dispatcher.flights:
                    try:
                        bridge.authorize_dispatch(number, command, self.state, self.issues.get(number))
                    except (bridge.PublicationError, bridge.QueueWait) as exc:
                        self.waiting.pop(number, None)
                        self.ready[number] = (None, exc)
                    else:
                        self.dispatcher.submit(key, command)
            for key, command in self.telegram_tasks.items():
                self.dispatcher.submit('telegram:' + key, command)
        finally:
            bridge.WORK_CONTEXT = None
            DRIVING.reset(token)

    async def wait(self, timeout):
        tasks = [flight.task for flight in self.dispatcher.flights.values() if not flight.task.done()]
        if tasks:
            await asyncio.wait(tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        else:
            await asyncio.sleep(timeout)

    async def close(self):
        # Shutdown cannot release a writer-held publication lock while detached
        # I/O is still executing. Finish submitted I/O, then close frames without
        # dispatching another operation. Restart uses durable requests/journals.
        import task_bridge as bridge
        self.closing = True
        self.dispatcher.closed = True
        tasks = [flight.task for flight in self.dispatcher.flights.values()]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for number, writer in list(self.continuations.items()):
            self._context(number)
            try:
                writer.context.run(writer.coroutine.throw, asyncio.CancelledError())
            except (asyncio.CancelledError, StopIteration):
                pass
            finally:
                writer.context.run(writer.coroutine.close)
        self.continuations.clear()
        bridge.WORK_CONTEXT = None


async def discover():
    """Copied discovery observations; never receives a ControlLoop reference."""
    issues = await execute(Command.create('issues', {}))
    tasks = await execute(Command.create('rpc', dict(name='list_tasks', arguments={})))
    identities = await execute(Command.create('discovery_identities', dict(tasks=tasks)))
    return dict(issues=issues, tasks=tasks, identities=identities)

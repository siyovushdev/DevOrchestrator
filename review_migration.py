"""Conservative reconciliation of pre-phase reviewer start envelopes.

Legacy requests keep their exact signature/arguments. They are never upgraded
into fresh v3 authorization or adopted as current review evidence. Fence absent
requests; drain positively bound runtimes; hold prepared/unverifiable records.
"""
from copy import deepcopy
import re
import time

import review_transitions as protocol
from review_coordinator import DurabilityFailure
from scheduler import Scheduler
from transition_io import Command


def validate(item, namespace, number):
    record, pending = item['review_migration'], item.get('pending_rpc')
    if (not isinstance(record, dict) or set(record) != {
            'schema_version', 'request_id', 'request_digest', 'phase', 'accepted'} or
            type(record['schema_version']) is not int or record['schema_version'] != 1 or
            record['phase'] not in ('INSPECT', 'DRAIN', 'HELD') or
            not isinstance(pending, dict) or pending.get('name') != 'start_task' or
            not isinstance(pending.get('operation'), str) or not re.fullmatch(r'review:[1-9][0-9]*', pending['operation']) or
            not isinstance(pending.get('arguments'), dict) or pending['arguments'].get('role') != 'review' or
            record['request_id'] != protocol.request_id(namespace, number, pending['operation']) or
            pending['arguments'].get('request_id') != record['request_id'] or
            protocol.digest(pending) != record['request_digest']):
        raise protocol.InvalidTransition('Legacy reviewer envelope cannot be verified')
    accepted = record['accepted']
    if accepted is not None and (not isinstance(accepted, dict) or set(accepted) != {
            'id', 'runtime_generation', 'turns', 'machine', 'workspace', 'role'} or
            accepted['id'] != record['request_id'][:24] or accepted['role'] != 'review' or
            not isinstance(accepted['runtime_generation'], str) or not accepted['runtime_generation'] or
            type(accepted['turns']) is not int or accepted['turns'] < 1 or
            any(not isinstance(accepted[key], str) or not accepted[key] for key in ('machine', 'workspace'))):
        raise protocol.InvalidTransition('Legacy reviewer acceptance is unbound')
    if (record['phase'] == 'DRAIN') != (accepted is not None):
        raise protocol.InvalidTransition('Legacy reviewer phase disagrees with its acceptance')
    return record


class LegacyReviewRecovery:
    def __init__(self, state, persist, namespace, dispatcher):
        self.state, self.persist, self.namespace, self.dispatcher = state, persist, namespace, dispatcher
        self.operations, self.next_attempt = {}, {}

    def save(self):
        try:
            self.persist(self.state)
        except Exception as exc:
            raise DurabilityFailure('Legacy reconciliation checkpoint failed') from exc

    def enroll(self):
        import task_bridge as bridge
        changed = False
        for number, item in self.state['issues'].items():
            from admission_policy import issue_allowed
            if not issue_allowed(self.namespace,number,item):
                continue
            if not isinstance(item, dict) or item.get('review_transition') is not None or item.get('review_migration') is not None:
                continue
            pending = item.get('pending_rpc')
            if (not isinstance(pending, dict) or pending.get('name') != 'start_task' or
                    not isinstance(pending.get('operation'), str) or not pending['operation'].startswith('review:')):
                continue
            key = protocol.request_id(self.namespace, number, pending['operation'])
            item['review_migration'] = dict(schema_version=1, request_id=key,
                request_digest=protocol.digest(pending), phase='INSPECT', accepted=None)
            # Install the hold before invalidating old approval. Never alter the
            # pending RPC to fit a new policy or request signature.
            try:
                validate(item, self.namespace, number)
                sequence = int(pending['operation'].split(':')[1])
                item['review_launch_sequence'] = max(item.get('review_launch_sequence', 0), sequence)
                bridge.invalidate_quality_gates(item)
            except (ValueError, KeyError, TypeError):
                item['review_migration']['phase'] = 'HELD'
                item.setdefault('consistency_diagnostics', {})['review_migration'] = 'Legacy review envelope held for local inspection'
            changed = True
        if changed:
            self.save()

    def schedule(self, number):
        item = self.state['issues'][number]
        record = validate(item, self.namespace, number)
        key = 'legacy-review:' + record['request_id']
        if key in self.operations or time.monotonic() < self.next_attempt.get(key, 0):
            return
        if record['phase'] == 'DRAIN':
            command = Command.create('rpc', dict(name='get_task_report', arguments={'task_id': record['accepted']['id']}))
        else:
            command = Command.create('legacy_review_inspect', dict(request_id=record['request_id']))
        if self.dispatcher.submit(key, command):
            self.operations[key] = number

    def finish(self, number, *, report=None):
        import task_bridge as bridge
        item = self.state['issues'][number]
        record = validate(item, self.namespace, number)
        key = record['request_id']
        job = self.state.get('work_queue', {}).get(key)
        if not job or str(job.get('owner')) != number or job.get('kind') != 'review':
            raise protocol.InvalidTransition('Legacy review queue ownership is unavailable')
        if report is None:
            job.update(status='CANCELLED', launch_pending=False, no_launch_proof='FENCED')
        else:
            Scheduler(self.state).started(key, report['id'], report['status'])
            Scheduler(self.state).observe(report['id'], report['status'], report['cleanup_state'])
            item['stale_review_task_ids'] = [task for task in item.get('stale_review_task_ids', []) if task != report['id']]
        item.pop('pending_rpc')
        item.pop('review_migration')
        item.get('consistency_diagnostics', {}).pop('review_migration', None)
        # No legacy review result can reopen a quality gate.
        self.save()

    def complete(self, key, value, error):
        import task_bridge as bridge
        number = self.operations.pop(key)
        item = self.state['issues'][number]
        record = validate(item, self.namespace, number)
        self.next_attempt[key] = time.monotonic() + 1
        if error is not None:
            bridge.defer_issue(item, self.state, number, error, stage='inspect_start_request')
            self.next_attempt[key] = time.monotonic() + 10
            return
        if record['phase'] == 'DRAIN':
            accepted = record['accepted']
            if any(value.get(field) != expected for field, expected in accepted.items()):
                raise protocol.InvalidTransition('Legacy reviewer runtime identity changed')
            if (value.get('status') in ('DONE', 'FAILED') and value.get('cleanup_state') in ('ABSENT', 'CLEANED') and
                    value.get('cleanup_generation') == accepted['runtime_generation']):
                self.finish(number, report=value)
            return
        if (value.get('schema_version') != 1 or value.get('request_id') != record['request_id'] or
                value.get('task_id') != record['request_id'][:24]):
            raise protocol.InvalidTransition('Unbound legacy request observation')
        if value.get('state') == 'FENCED' and value.get('launch_started') is False:
            self.finish(number)
            return
        if value.get('state') not in ('PENDING', 'STARTED'):
            return
        meta = value.get('metadata') or {}
        if (value.get('signature_version') not in (1, 2) or meta.get('id') != record['request_id'][:24] or
                meta.get('role') != 'review'):
            raise protocol.InvalidTransition('Legacy receipt has incompatible launch identity')
        if meta.get('launch_prepared') or meta.get('resume_prepared') or not meta.get('runtime_generation'):
            item['review_migration']['phase'] = 'HELD'
            item.setdefault('consistency_diagnostics', {})['review_migration'] = 'Legacy prepared or unbound runtime retained; no automatic launch or approval'
            self.save()
            return
        accepted = {field: deepcopy(meta.get(field)) for field in ('id', 'runtime_generation', 'turns', 'machine', 'workspace', 'role')}
        changed = dict(record, phase='DRAIN', accepted=accepted)
        validate(dict(item, review_migration=changed), self.namespace, number)
        item['review_migration'] = changed
        self.save()

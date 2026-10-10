"""Writer-only reviewer phase application; this module never executes I/O.

The persist callback is called synchronously by the sole lifecycle writer. No
coordinator, live state, item or callback may be handed to an I/O executor.
"""
from copy import deepcopy

import bridge_effects
import chief_review
import review_transitions as protocol
from scheduler import QueueWait, Scheduler


class DurabilityFailure(BaseException):
    """Stop the writer after an uncertain save; never dispatch from dirty memory."""


class ReviewCoordinator:
    def __init__(self, state, persist, namespace):
        self.state = state
        self.persist = persist
        self.namespace = namespace

    def save(self):
        try:
            self.persist(self.state)
        except Exception as exc:
            raise DurabilityFailure('Review checkpoint persistence failed; writer must stop') from exc

    def record(self, number):
        item = self.state['issues'][str(number)]
        record = item['review_transition']
        protocol.validate(record, item.get('pending_rpc'))
        if record['issue'] != str(number) or record['namespace'] != self.namespace:
            raise protocol.InvalidTransition('Review transition belongs to a different issue or namespace')
        return item, record

    def begin(self, number, report, inputs):
        item = self.state['issues'][str(number)]
        record = protocol.begin(self.namespace, number, item, report, inputs)
        if item.get('review_task_id') or item.get('review_status') in ('PASS', 'NEEDS_OWNER', 'FAILED'):
            raise protocol.InvalidTransition('Existing review must be reconciled before creating a launch')
        item['review_launch_sequence'] = record['sequence']
        item['review_transition'] = record
        chief_review.invalidate(item)
        item.update(owner_state=None, owner_approved_at=None)
        self.save()
        return deepcopy(record)

    def current(self, item, record, report, inputs):
        from operational_state import exclusion_reason, post_merge_repository_hold
        return (not exclusion_reason(item) and not post_merge_repository_hold(item) and
                item.get('task_id') == record['source']['task_id'] and
                item.get('coding_turn') == record['source']['turn'] and
                not item.get('pending_followup') and
                protocol.source_matches(record, report, inputs))

    def invalidate(self, number):
        item, record = self.record(number)
        changed = protocol.invalidate(record, item.get('pending_rpc'))
        if changed != record:
            item['review_transition'] = changed
            chief_review.invalidate(item)
            item.update(owner_state=None, owner_approved_at=None)
            self.save()

    def authorize(self, number, candidate):
        """Reserve and save one launch envelope, then return a detached command."""
        item, record = self.record(number)
        if not self.current(item, record, candidate['report'], candidate['inputs']):
            self.invalidate(number)
            return None
        binding, arguments = candidate['binding'], candidate['arguments']
        guard = arguments.get('review_binding')
        protocol.validate_launch_binding(guard)
        if (guard['source_task_id'] != record['source']['task_id'] or
                guard['source_turn'] != record['source']['turn'] or
                guard['source_generation'] != record['source']['runtime_generation'] or
                any(guard[key] != binding[key] for key in ('machine', 'repository', 'registry_digest', 'snapshot'))):
            raise protocol.InvalidTransition('Prepared review guard disagrees with its source')
        authorized, pending = protocol.authorize(record, arguments, binding)
        scheduler = Scheduler(self.state)
        key = record['request_id']
        existed = key in scheduler.jobs
        job = scheduler.enqueue(key, str(number), 'review', binding['machine'], binding['repository'])
        if ((job['owner'], job['kind'], job['machine'], job['repo']) !=
                (str(number), 'review', binding['machine'], binding['repository']) or
                job['status'] != 'QUEUED' or job.get('launch_pending') or job.get('task_id')):
            raise protocol.InvalidTransition('Review queue identity is inconsistent before authorization')
        try:
            scheduler.reserve(key)
        except QueueWait:
            if not existed:
                self.save()
            return None
        job.update(task_id=key[:24], launch_pending=True)
        item.update(review_transition=authorized, pending_rpc=pending)
        self.save()
        return {'kind': 'launch', 'request_id': key, 'arguments': deepcopy(pending['arguments'])}

    def inspection_command(self, number):
        item, record = self.record(number)
        if record['request_digest'] is None:
            raise protocol.InvalidTransition('Preparation has no external request to reconcile')
        return {'kind': 'inspect', 'request_id': record['request_id'],
                'arguments': {'request_id': record['request_id'], 'fence': record['invalidated']}}

    def apply_inspection(self, number, response):
        """Reconcile external facts, including responses to invalidated attempts."""
        item, record = self.record(number)
        if (type(response.get('schema_version')) is not int or response['schema_version'] != 1 or
                response.get('request_id') != record['request_id'] or
                response.get('task_id') != record['request_id'][:24]):
            raise protocol.InvalidTransition('Unbound worker request observation')
        status = response.get('state')
        if status in ('ABSENT', 'BUSY', 'UNKNOWN'):
            return status
        if status == 'FENCED':
            if response.get('launch_started') is not False or record['accepted_task'] is not None:
                raise protocol.InvalidTransition('Fence contradicts existing acceptance')
            self.settle_unlaunched(number, proof='FENCED')
            return 'FENCED'
        if status not in ('PENDING', 'STARTED'):
            raise protocol.InvalidTransition('Unknown worker request state')
        meta = response.get('metadata')
        pending = item['pending_rpc']
        if (not isinstance(meta, dict) or meta.get('id') != record['request_id'][:24] or
                meta.get('role') != 'review' or meta.get('launch_request_id') != record['request_id'] or
                meta.get('review_binding') != pending['arguments'].get('review_binding') or
                response.get('signature_version') != 3):
            raise protocol.InvalidTransition('Worker receipt is not bound to the saved review request')
        if meta.get('launch_prepared') or meta.get('resume_prepared'):
            return 'PREPARED'  # Never resume this automatically when invalidated.
        generation = meta.get('runtime_generation')
        if not isinstance(generation, str) or not generation:
            return 'UNKNOWN'
        accepted = protocol.accept(record, pending, dict(task_id=meta['id'],
            request_id=record['request_id'], runtime_generation=generation))
        if accepted != record:
            item['review_transition'] = accepted
            self.save()
        return 'BOUND'

    def replay_command(self, number):
        item, record = self.record(number)
        if record['phase'] != 'LAUNCH' or record['invalidated']:
            raise protocol.InvalidTransition('Only a current unresolved launch may be replayed')
        return {'kind': 'launch', 'request_id': record['request_id'],
                'arguments': deepcopy(item['pending_rpc']['arguments'])}

    def _worker_report(self, item, record, report):
        accepted = record['accepted_task']
        return bool(accepted and report.get('id') == accepted['task_id'] and
                    report.get('runtime_generation') == accepted['runtime_generation'] and
                    report.get('role') == 'review' and
                    report.get('launch_request_id') == record['request_id'] and
                    report.get('review_binding') == item['pending_rpc']['arguments'].get('review_binding'))

    def adopt(self, number, report, source_report, inputs, snapshot, effects):
        """Acceptance, gate state and logical effects become durable together."""
        item, record = self.record(number)
        if record['phase'] != 'VERIFY' or not self._worker_report(item, record, report):
            raise protocol.InvalidTransition('Cannot adopt an unverified reviewer')
        if not self.current(item, record, source_report, inputs) or snapshot != record['binding']['snapshot']:
            self.invalidate(number)
            return False
        if report.get('status') not in ('RUNNING', 'DONE', 'FAILED'):
            return False
        scheduler = Scheduler(self.state)
        scheduler.started(record['request_id'], report['id'], report['status'])
        scheduler.observe(report['id'], report['status'], report.get('cleanup_state'))
        chief_review.invalidate(item)
        item.update(review_task_id=report['id'], review_round=record['sequence'],
                    reviewer_agent=item['pending_rpc']['arguments']['agent'], review_status='RUNNING',
                    review_task_turn=report.get('turns', 1), review_coding_turn=record['source']['turn'],
                    review_start_tree=deepcopy(record['binding']['snapshot']), review_format_corrections=0,
                    owner_state=None, owner_approved_at=None)
        for index, effect in enumerate(effects):
            bridge_effects.enqueue(self.state, str(number), effect['kind'], deepcopy(effect['payload']),
                                   ['review-launch', record['request_id'], effect['kind'], index])
        item.pop('pending_rpc')
        item.pop('review_transition')
        item.pop('reconciliation_retry', None)
        item.get('consistency_diagnostics', {}).pop('reconciliation', None)
        self.save()
        return True

    def discard_preparation(self, number):
        item, record = self.record(number)
        if record['phase'] != 'PREPARE' or not record['invalidated']:
            raise protocol.InvalidTransition('Only invalidated unlaunched preparation may be discarded')
        job = self.state.get('work_queue', {}).get(record['request_id'])
        if job:
            unreserved = (job['status'] == 'QUEUED' or
                          (job['status'] == 'CANCELLED' and job.get('cancelled') == 'archived_issue'))
            if not unreserved or job.get('task_id') or job.get('launch_pending'):
                raise protocol.InvalidTransition('Preparation has uncertain queue ownership')
            job.update(status='CANCELLED', cancelled='review_preparation_invalidated')
        item.pop('review_transition')
        self.save()

    def settle_unlaunched(self, number, *, proof):
        item, record = self.record(number)
        if proof != 'FENCED' or record['accepted_task'] is not None:
            raise protocol.InvalidTransition('No conclusive unlaunched proof')
        job = self.state['work_queue'][record['request_id']]
        job.update(status='FAILED' if proof == 'REJECTED' else 'CANCELLED', launch_pending=False,
                   no_launch_proof=proof)
        item.pop('pending_rpc')
        item.pop('review_transition')
        item.get('consistency_diagnostics', {}).pop('review_request', None)
        self.settle_rejection(item)
        self.save()

    @staticmethod
    def settle_rejection(item):
        if item.pop('review_launch_rejection', None):
            item.update(review_status='FAILED', execution_failure={'role': 'review',
                'diagnostic': 'Reviewer execution policy rejected; original request ownership reconciled'})

    def drain(self, number, report):
        item, record = self.record(number)
        if record['phase'] != 'DRAIN' or not self._worker_report(item, record, report):
            raise protocol.InvalidTransition('Unbound stale-review drain observation')
        if (report.get('status') not in ('DONE', 'FAILED') or
                report.get('cleanup_state') not in ('ABSENT', 'CLEANED') or
                report.get('cleanup_generation') != record['accepted_task']['runtime_generation']):
            return False
        scheduler = Scheduler(self.state)
        scheduler.started(record['request_id'], report['id'], report['status'])
        scheduler.observe(report['id'], report['status'], report['cleanup_state'])
        item['stale_review_task_ids'] = [task for task in item.get('stale_review_task_ids', [])
                                       if task != report['id']]
        item.pop('pending_rpc')
        item.pop('review_transition')
        item.get('consistency_diagnostics', {}).pop('review_request', None)
        self.settle_rejection(item)
        self.save()
        return True

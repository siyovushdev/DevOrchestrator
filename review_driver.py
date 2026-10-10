"""Writer-side scheduling of persisted reviewer phases, without awaited I/O."""
import time

from execution_profiles import PreflightRejected, StaleReviewInput, check_preflight
from review_coordinator import ReviewCoordinator
import review_transitions as protocol
from transition_io import Command
from worker_runtime import RequestFenced


def inputs(issue, item):
    return dict(project=item['project'], coding_agent=item.get('coding_agent'),
                body=issue.get('body') or '')


class ReviewDriver:
    def __init__(self, state, persist, namespace, dispatcher):
        self.state = state
        self.coordinator = ReviewCoordinator(state, persist, namespace)
        self.dispatcher = dispatcher
        self.operations = {}
        self.next_attempt = {}

    def begin(self, issue, item, report):
        return self.coordinator.begin(str(issue['number']), report, inputs(issue, item))

    def schedule(self, number, issue):
        item, record = self.coordinator.record(number)
        key = 'review:' + record['request_id']
        if (key in self.operations or time.monotonic() < self.next_attempt.get(key, 0) or
                item.get('reconciliation_retry', {}).get('next_attempt', 0) > time.time()):
            return
        if record['invalidated'] and record['phase'] == 'PREPARE':
            self.coordinator.discard_preparation(number)
            return
        if record['phase'] == 'HELD':
            return
        if record['phase'] == 'PREPARE':
            operation = 'prepare'
            command = Command.create('prepare_review', dict(issue=issue, inputs=inputs(issue, item),
                task_id=record['source']['task_id'], request_id=record['request_id']))
        elif record['accepted_task'] is not None:
            operation = 'drain' if record['invalidated'] else 'verify'
            command = (Command.create('rpc', dict(name='get_task_report',
                arguments={'task_id': record['accepted_task']['task_id']})) if operation == 'drain' else
                Command.create('verify_review', dict(issue=issue, inputs=inputs(issue, item),
                    source_task_id=record['source']['task_id'], task_id=record['accepted_task']['task_id'])))
        else:
            operation = 'inspect'
            request = self.coordinator.inspection_command(number)
            command = Command.create('rpc', dict(name='inspect_start_request', arguments=request['arguments']))
        if self.dispatcher.submit(key, command):
            self.operations[key] = (number, operation)

    def submit_launch(self, number, request):
        from operational_state import exclusion_reason
        if exclusion_reason(self.state['issues'][number]):
            self.coordinator.invalidate(number)
            return
        key = 'review:' + request['request_id']
        command = Command.create('rpc', dict(name='start_task', arguments=request['arguments']))
        if self.dispatcher.submit(key, command):
            self.operations[key] = (number, 'launch')
        # If capacity is unavailable, durable LAUNCH is inspected next cycle.

    def complete(self, key, value, error, issue):
        import task_bridge as bridge
        number, operation = self.operations.pop(key)
        item, record = self.coordinator.record(number)
        bridge.archive_issue(issue, item, self.state)
        item, record = self.coordinator.record(number)
        if key != 'review:' + record['request_id']:
            raise protocol.InvalidTransition('Completion lost its durable review request')
        if error is not None:
            if operation == 'prepare' and isinstance(error, PreflightRejected):
                self.coordinator.invalidate(number)
                self.coordinator.discard_preparation(number)
                item.update(review_status='FAILED', execution_failure={'role': 'review',
                    'diagnostic': 'Reviewer execution policy rejected before launch'})
                self.coordinator.save()
                return
            # A local cancellation/network error provides no no-launch proof.
            bridge.defer_issue(item, self.state, number, error, stage='start_task' if operation == 'launch' else 'get_task_report')
            self.next_attempt[key] = time.monotonic() + 10
            return
        if operation == 'launch':
            try:
                if value.get('status') in ('REJECTED', 'UNAVAILABLE', 'STALE_INPUT', 'FENCED'):
                    check_preflight(value)
            except (PreflightRejected, StaleReviewInput, RequestFenced) as exc:
                # A no-launch answer covers this invocation only. An older
                # timed-out transmission may still reach worker.lock later.
                # Retain authority until a durable fence or bound runtime drain.
                if isinstance(exc, PreflightRejected):
                    item['review_launch_rejection'] = 'Reviewer execution policy rejected before launch'
                self.coordinator.invalidate(number)
                self.coordinator.save()
                return
            # Even a successful response is reconciled via the durable receipt.
            return
        if operation == 'drain':
            self.coordinator.drain(number, value)
            return
        if operation == 'prepare':
            if inputs(issue, item) != value['inputs']:
                self.coordinator.invalidate(number)
                return
            request = self.coordinator.authorize(number, value)
            if request:
                self.submit_launch(number, request)
            return
        if operation == 'inspect':
            status = self.coordinator.apply_inspection(number, value)
            if status in ('ABSENT', 'PREPARED') and not record['invalidated']:
                self.submit_launch(number, self.coordinator.replay_command(number))
            elif status in ('BUSY', 'UNKNOWN', 'PREPARED'):
                self.next_attempt[key] = time.monotonic() + 1
                if status == 'PREPARED' and record['invalidated']:
                    item.setdefault('consistency_diagnostics', {})['review_request'] = (
                        'Invalidated prepared reviewer request retained; inspect worker receipt. Automatic resume is prohibited.')
                    self.coordinator.save()
            return
        if operation == 'verify':
            # Fresh provider closure and fresh coding-turn observations are
            # applied by the writer before deciding whether adoption is allowed.
            bridge.archive_issue(value['issue'] or issue, item, self.state)
            bridge.archive_issue(issue, item, self.state)
            bridge.observe_coding_turn(item, self.state, value['source_report'])
            if inputs(issue, item) != inputs(value['issue'], item):
                self.coordinator.invalidate(number)
            item, record = self.coordinator.record(number)
            if record['invalidated']:
                self.coordinator.drain(number, value['report'])
                return
            effects = [dict(kind='comment', payload=bridge.format_report(value['source_report'])),
                       dict(kind='comment', payload='\n'.join([
                           '<!-- DEVORCHESTRATOR_REVIEW_START_V1 -->', '## Independent review started', '',
                           f"- Coding task: `{item['task_id']}`",
                           f"- Coding agent: `{item.get('coding_agent') or value['source_report'].get('agent')}`",
                           f"- Review task: `{record['accepted_task']['task_id']}`",
                           f"- Reviewer: `{item['pending_rpc']['arguments']['agent']}`",
                           f"- Review round: `{record['sequence']}`"]))]
            self.coordinator.adopt(number, value['report'], value['source_report'],
                inputs(value['issue'] or issue, item), value['snapshot'], effects)


async def prepare(command):
    """Detached reader returns copied evidence; never gets live state or callbacks."""
    import task_bridge as bridge
    from transition_executor import execute
    args = command.arguments()
    capabilities = await execute(Command.create('rpc', dict(name='get_transition_capabilities', arguments={})))
    if capabilities != dict(schema_version=1, review_binding=1, start_request_reconciliation=1):
        raise RuntimeError('Worker does not support resumable reviewer launch')
    report = await execute(Command.create('rpc', dict(name='get_task_report', arguments={'task_id': args['task_id']})))
    # Local config reads and remote repository snapshots execute in the work pool.
    candidate = await execute(Command.create('review_candidate', dict(args, report=report)))
    preflight = await execute(Command.create('rpc', dict(name='preflight_task',
        arguments={'operation': 'start_task', 'arguments': candidate['arguments']})))
    check_preflight(preflight)
    return candidate


def candidate(args):
    import devctl
    import execution_profiles
    import task_bridge as bridge
    report, values = args['report'], args['inputs']
    project = values['project']
    cfg = devctl.load_projects()[project]
    try:
        reviewer = bridge.reviewer_for(values['coding_agent'] or report.get('agent'), cfg)
    except ValueError as exc:
        raise PreflightRejected('Invalid registered independent reviewer policy') from exc
    machine, repository = bridge.project_identity(project)
    snapshot = bridge.Repository(project, report).snapshot()
    binding = dict(machine=machine, repository=repository, registry_digest=protocol.digest(cfg), snapshot=snapshot)
    guard = dict(binding, schema_version=1, source_task_id=report['id'], source_turn=report['turns'],
                 source_generation=report.get('runtime_generation'), catalog_digest=execution_profiles.load_catalog(cfg)[1]['sha256'])
    protocol.validate_launch_binding(guard)
    arguments = dict(project=project, role='review', agent=reviewer, review_binding=guard,
                     request_id=args['request_id'], prompt=bridge.build_review_prompt(args['issue'], report))
    return dict(report=report, inputs=values, binding=binding, arguments=arguments)


async def verify(command):
    from transition_executor import execute
    args = command.arguments()
    report = await execute(Command.create('rpc', dict(name='get_task_report', arguments={'task_id': args['task_id']})))
    source = await execute(Command.create('rpc', dict(name='get_task_report', arguments={'task_id': args['source_task_id']})))
    snapshot = await execute(Command.create('repository', dict(project=args['inputs']['project'],
                              report=source, method='snapshot')))
    issue = await execute(Command.create('issue', dict(number=args['issue']['number'])))
    if not issue:
        raise RuntimeError('Fresh issue observation unavailable')
    return dict(report=report, source_report=source, snapshot=snapshot, issue=issue)

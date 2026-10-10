"""Detached I/O implementation. No bridge lifecycle state is accepted here."""
import asyncio
from concurrent.futures import ThreadPoolExecutor


# Separate bounded pools prevent slow SSH/GitHub operations from occupying the
# threads needed by command polling. Dispatcher bounds submitted work as well.
POOLS = {name: ThreadPoolExecutor(max_workers=count, thread_name_prefix='bridge-' + name)
         for name, count in (('control', 2), ('work', 2), ('effect', 1), ('notification', 2), ('control_rpc', 1))}


def synchronous(name, args):
    import task_bridge as bridge
    if name == 'issue':
        return bridge.fetch_issue(args['number'])
    if name == 'comments':
        return bridge.issue_comments(args['number'])
    if name == 'control_read':
        return dict(issue=bridge.fetch_issue(args['number']), comments=bridge.issue_comments(args['number']))
    if name == 'labels':
        adapter = bridge.github_task_transport()
        return adapter.fetch_labels(adapter.issue_ref(args['number']))
    if name == 'post_comment':
        from external_effects import refuse
        refuse('github')
        return bridge.raw_add_comment(args['number'], args['body'])
    if name == 'transition':
        from external_effects import refuse
        refuse('github')
        return bridge.raw_transition_issue(args['issue'], args['target'])
    if name == 'telegram':
        from external_effects import refuse
        refuse('telegram')
        return bridge.send_telegram(args['text'])
    if name == 'github':
        from external_effects import guard_github
        guard_github(args['args'])
        return bridge.gh(*args['args'])
    if name == 'issues':
        return bridge.open_issues()
    if name == 'project_identity':
        identity = bridge.project_identity(args['project'])
        if args.get('binding'):
            import devctl
            from review_transitions import digest
            return dict(machine=identity[0], repository=identity[1], registry_digest=digest(devctl.load_projects()[args['project']]))
        return identity
    if name == 'repository':
        if args['method'] not in ('snapshot', 'review_evidence', 'git', 'run'):
            raise ValueError('Unknown repository operation')
        from external_effects import refuse
        refuse('publication')
        repo = bridge.Repository(args['project'], args.get('report'))
        if 'configuration' in args and repo.cfg != args['configuration']:
            from publication import PublicationError
            raise PublicationError('Registered repository changed during transition')
        return getattr(repo, args['method'])(*args.get('args', []))
    if name == 'review_candidate':
        from review_driver import candidate
        return candidate(args)
    if name == 'discovery_identities':
        import devctl
        registry, identities = devctl.load_projects(), {}
        for task in args['tasks']:
            if task.get('status') not in ('RUNNING', 'UNKNOWN', 'STARTING') and task.get('cleanup_state') != 'AMBIGUOUS':
                continue
            try:
                project = task.get('project')
                if not project or project == '-':
                    project = bridge.registered_project(task)
                if project not in identities:
                    identities[project] = (registry.get(project), bridge.project_identity(project))
            except Exception:
                continue
        return identities
    raise ValueError('Unknown detached I/O operation')


async def execute(command):
    args = command.arguments()
    if command.name == 'discovery':
        from control_loop import discover
        from external_effects import blocked
        latched = blocked()
        result = await discover()
        if latched or blocked():
            result['observation_confidence'] = 'READ_ONLY'
        return result
    if command.name == 'legacy_review_inspect':
        from transition_io import Command
        capability = await execute(Command.create('rpc', dict(name='get_transition_capabilities', arguments={})))
        if capability.get('start_request_reconciliation') != 1:
            raise RuntimeError('Worker request reconciliation capability unavailable')
        return await execute(Command.create('rpc', dict(name='inspect_start_request',
            arguments={'request_id': args['request_id'], 'fence': True})))
    if command.name in ('prepare_review', 'verify_review'):
        from review_driver import prepare, verify
        return await (prepare(command) if command.name == 'prepare_review' else verify(command))
    if command.name == 'rpc':
        import task_bridge as bridge
        from external_effects import guard_worker_rpc, blocked
        guard_worker_rpc(args['name'], args['arguments'])
        latched = blocked()
        result = await bridge.transport_call(args['name'], args['arguments'])
        if args['name'] in ('get_task_report', 'list_tasks') and (latched or blocked()):
            if isinstance(result, list):
                return [dict(row, observation_confidence='READ_ONLY') for row in result]
            return dict(result, observation_confidence='READ_ONLY')
        return result
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(POOLS[command.lane], synchronous, command.name, args)

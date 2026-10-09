"""Read-only lifecycle policy and operator view over the durable bridge state."""
from copy import deepcopy


def merge_convergence_complete(item):
    """Missing historical evidence is archival, never proof of completion."""
    result = item.get('post_merge_convergence') or {}
    return result.get('status') == 'DONE' or (
        result.get('status') == 'NOT_REQUIRED' and result.get('required') is False)


def post_merge_repository_hold(item):
    """Keep an attempted merge's workspace fenced even if its issue is archived."""
    result = item.get('post_merge_convergence') or {}
    if (result.get('status') == 'NOT_MERGED' and not item.get('merged') and
            item.get('owner_state') != 'MERGED' and not result.get('master_sha')):
        return False
    return item.get('post_merge_convergence') is not None and not merge_convergence_complete(item)


def exclusion_reason(item, issue=None):
    # Closing is a sticky archive fence. Reopening requires a NEW issue/task;
    # neither an old queue label nor an old owner command resumes this generation.
    if item.get('runtime_exclusion') or item.get('issue_closed'):
        return item.get('runtime_exclusion') or 'ISSUE_CLOSED'
    if issue and str(issue.get('state', 'OPEN')).upper() == 'CLOSED':
        return 'ISSUE_CLOSED'
    if (item.get('merged') or item.get('owner_state') == 'MERGED') and (
            item.get('post_merge_convergence') is None or merge_convergence_complete(item)):
        return 'PUBLICATION_TERMINAL'
    if (item.get('completed_at') or item.get('completed') is True or item.get('superseded') or
            item.get('desired_label') == 'dev:done' or
            any(str(item.get(field, '')).upper() in ('COMPLETE', 'COMPLETED', 'SUPERSEDED', 'CLOSED')
                for field in ('status', 'lifecycle_state', 'completion_status'))):
        return 'HISTORICAL_COMPLETE'
    return None


def chief_repository_hold(item):
    """Unpublished independent PASS needs Chief approval, including backfill.

    Match the existing Chief enrollment fence for archived or already published
    records. Approval uses the existing bundle/turn/tree validator; a PASS label
    or an older independent approval cannot finish repository work.
    """
    if (exclusion_reason(item) or
            item.get('owner_state') in ('COMMITTED', 'PUSHED', 'PR_CREATED', 'MERGED') or
            item.get('publication_journal')):
        return False
    if item.get('review_status') != 'PASS' and not item.get('chief_required'):
        return False
    from chief_review import approval_complete
    return not (item.get('chief_required') and approval_complete(item))


def lifecycle_state(item):
    if exclusion_reason(item):
        return 'ARCHIVED'
    if item.get('merged') and not merge_convergence_complete(item):
        return 'POST_MERGE_BLOCKED'
    result = item.get('post_merge_convergence') or {}
    if result.get('status') == 'NOT_MERGED':
        return 'MERGE_NOT_COMPLETED'
    if result.get('status') == 'PENDING':
        return 'MERGE_UNCERTAIN'
    if item.get('coding_status') in ('RUNNING', 'STARTING') or item.get('pending_rpc'):
        return 'RUNNING'
    if item.get('worker_uncertain') or item.get('review_uncertain'):
        return 'NEEDS_OWNER'
    if item.get('review_hold') or item.get('review_status') == 'FRESH_REVIEW_REQUIRED':
        return 'FRESH_REVIEW_REQUIRED'
    if item.get('owner_state'):
        return item['owner_state']
    if item.get('chief_status') in ('AWAITING_CHIEF_REVIEW', 'CONFLICT'):
        return item['chief_status']
    if item.get('review_status') == 'RUNNING':
        return 'INDEPENDENT_REVIEW'
    if item.get('review_status') in ('NEEDS_OWNER', 'FAILED'):
        return item['review_status']
    if item.get('desired_label'):
        return {'work:queued': 'QUEUED', 'dev:running': 'RUNNING', 'dev:failed': 'FAILED',
                'dev:needs-owner': 'NEEDS_OWNER', 'dev:ready-for-review': 'READY_FOR_REVIEW',
                'dev:awaiting-chief-review': 'AWAITING_CHIEF_REVIEW'}.get(item['desired_label'], 'UNKNOWN')
    return 'QUEUED' if item.get('queue_status') == 'QUEUED' else item.get('coding_status', 'UNKNOWN')


def next_action(item, report=None):
    state = lifecycle_state(item)
    if state == 'ARCHIVED':
        return 'NONE'
    if any(key.startswith('runtime_') for key in item.get('consistency_diagnostics', {})):
        return 'INSPECT_WORKER'
    if state == 'POST_MERGE_BLOCKED':
        return 'INSPECT_POST_MERGE_CONVERGENCE'
    if state in ('MERGE_NOT_COMPLETED', 'MERGE_UNCERTAIN'):
        return 'INSPECT_MERGE_RESULT'
    if state == 'READY_FOR_OWNER':
        from notifications import review_approval_complete, publication_delta_present
        if not review_approval_complete(item):
            return 'FOLLOWUP_FOR_FRESH_REVIEW'
        return 'NONE' if publication_delta_present(item, report or item.get('notification_report', {})) is False else 'COMMIT'
    if state == 'NEEDS_OWNER':
        return 'INSPECT_WORKER' if item.get('worker_uncertain') or item.get('review_uncertain') else 'ANSWER'
    return {'COMMITTED': 'PUSH', 'PUSHED': 'CREATE_PR', 'PR_CREATED': 'MERGE',
            'FRESH_REVIEW_REQUIRED': 'FOLLOWUP_FOR_FRESH_REVIEW', 'FAILED': 'FOLLOWUP_FOR_CORRECTION',
            'CONFLICT': 'FOLLOWUP_FOR_FRESH_REVIEW', 'AWAITING_CHIEF_REVIEW': 'CHIEF_VERDICT'}.get(state, 'NONE')


def notification_fingerprint(item):
    # Only semantic identity: changing heartbeat ages and projection repairs do
    # not make a new owner event. No full prompts or provider payloads.
    from copy import deepcopy
    return deepcopy({key: item.get(key) for key in ('task_id', 'coding_turn', 'coding_status', 'review_task_id',
        'review_round', 'review_status', 'chief_status', 'owner_state', 'owner_approved_at',
        'owner_blocker', 'worker_uncertain', 'review_uncertain', 'review_hold')})


def status_view(state, number, runtime=None):
    item = state.get('issues', {}).get(str(number))
    if not isinstance(item, dict):
        raise ValueError('Unknown tracked issue')
    owned = sorted((j for j in state.get('work_queue', {}).values() if str(j.get('owner')) == str(number)),
                   key=lambda j: j.get('sequence', 0))
    latest = {j.get('kind'): j for j in owned}
    jobs = [{key: job.get(key) for key in ('kind', 'status', 'task_id', 'launch_pending', 'runtime_cleanup_pending')}
            for job in owned if latest[job.get('kind')] is job or job.get('status') in ('RESERVED', 'RUNNING', 'UNKNOWN')]
    chief = item.get('chief_review') or {}
    return dict(issue=str(number), task=item.get('task_id'), project=item.get('project'),
        lifecycle=lifecycle_state(item), coding_agent=item.get('coding_agent'), model=item.get('model'),
        effort=item.get('effort'), session=(runtime or {}).get('session_id') or item.get('coding_session_id'),
        coding_turn=item.get('coding_turn'), reviewer=item.get('reviewer_agent'), review_task=item.get('review_task_id'),
        review_round=item.get('review_round'), chief_state=item.get('chief_status'),
        chief_bundle=(chief.get('identity') or {}).get('bundle_id'),
        runtime=runtime or item.get('runtime_health', {}).get('coding') or {'state': 'UNKNOWN'},
        review_runtime=item.get('runtime_health', {}).get('review') or {'state': 'UNKNOWN'},
        queue=jobs, locks=[{'repository': repo, 'owner': lock.get('owner')} for repo, lock in state.get('repo_locks', {}).items()
                           if str(lock.get('owner')) == str(number)], publication=item.get('owner_state'),
        next_action=next_action(item), projection=item.get('projection', {'health': 'UNKNOWN'}),
        diagnostics=item.get('consistency_diagnostics', {}),
        convergence=item.get('post_merge_convergence'),
        timings=deepcopy(state.get('timings', {'samples': []})),
        scheduler_diagnostics={repo: state.get('scheduler_diagnostics', {}).get(repo)
            for repo, lock in state.get('repo_locks', {}).items() if str(lock.get('owner')) == str(number)
            and repo in state.get('scheduler_diagnostics', {})})

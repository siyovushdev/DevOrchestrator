"""Deterministic owner publication; no publication commands go to an agent."""
import json
import re
import shlex

import devctl

# Runs on the registered machine. A temporary index leaves the user's index intact.
SNAPSHOT_SCRIPT = r'''
import hashlib, json, os, subprocess, sys, tempfile
workspace = sys.argv[1]
def git(*args, env=None, optional=False):
    p = subprocess.run(['git', '-C', workspace, *args], env=env,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.returncode and not optional:
        raise RuntimeError('Git snapshot failed')
    return p.stdout if not p.returncode else b''
head = git('rev-parse', '--verify', 'HEAD', optional=True).decode().strip()
branch = git('symbolic-ref', '--quiet', '--short', 'HEAD', optional=True).decode().strip()
origin = git('remote', 'get-url', 'origin', optional=True).decode().strip()
root = git('rev-parse', '--show-toplevel').decode().strip()
push_urls = git('remote', 'get-url', '--push', '--all', 'origin', optional=True).decode().splitlines()
status = git('status', '--porcelain=v1', '--untracked-files=all').decode()
index = git('ls-files', '--stage', '-z')
if b'160000 ' in index:
    raise RuntimeError('Automatic publication of submodules is unsupported')
fd, name = tempfile.mkstemp()
os.close(fd)
os.unlink(name)
env = dict(os.environ, GIT_INDEX_FILE=name)
try:
    if head:
        git('read-tree', head, env=env)
    git('add', '-A', env=env)
    tree = git('write-tree', env=env).decode().strip()
finally:
    if os.path.exists(name): os.unlink(name)
data = dict(head=head or None, branch=branch or None, origin=origin or None, root=root, push_urls=push_urls,
            tree=tree, status=status, index=hashlib.sha256(index).hexdigest())
data['fingerprint'] = hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
print(json.dumps(data))
'''


class PublicationError(RuntimeError):
    pass


class MergeRefused(PublicationError):
    """Only a captured HTTP refusal of the exact merge PUT, never raw CLI output."""
    def __init__(self, status):
        self.status = status
        super().__init__(f'GitHub refused the authorized merge (HTTP {status})')


class Repository:
    def __init__(self, project, report=None):
        registry = devctl.load_projects()
        if project not in registry:
            raise PublicationError('Unknown registered project')
        self.cfg = registry[project]
        self.machine = self.cfg['machine']
        self.workspace = self.cfg['workspace']
        if report and (report.get('machine') != self.machine or
                       report.get('workspace') != self.workspace):
            raise PublicationError('Task no longer matches registered workspace')

    def run(self, *args):
        merge_api = (args[:6] == ('gh', 'api', '--hostname', 'github.com', '--method', 'PUT') and
                     len(args) > 6 and re.fullmatch(
                         r'repos/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/pulls/[1-9][0-9]*/merge', args[6]))
        if merge_api:
            args = (*args, '--include')
        if args[0] == 'gh' and '--body' in args:
            index = args.index('--body')
            body = args[index + 1]
            args = args[:index] + args[index + 2:]
            script = "import subprocess,sys,tempfile; f=tempfile.NamedTemporaryFile(mode='w'); f.write(sys.argv[1]); f.flush(); p=subprocess.run(sys.argv[2:]+['--body-file',f.name]); sys.exit(p.returncode)"
            args = ('python3', '-c', script, body, *args)
        command = 'cd ' + shlex.quote(self.workspace) + ' && ' + shlex.join(args)
        result = devctl.capture(self.machine, command, separate_stderr=True)
        status, body = None, result.stdout
        if merge_api:
            # Parse only the response envelope. Body/error text may contain
            # credentials; neither participates in refusal classification.
            parts = re.split(r'\r?\n\r?\n', result.stdout, maxsplit=1)
            first = parts[0].splitlines()[0] if parts[0] else ''
            match = re.fullmatch(r'HTTP/[0-9]+(?:\.[0-9]+)? ([1-5][0-9]{2})(?: [^\r\n]*)?', first)
            if len(parts) == 2 and match:
                status, body = int(match[1]), parts[1]
        if result.returncode:
            if merge_api and result.returncode == 1 and status in (405, 409):
                raise MergeRefused(status)
            # Do not echo command output: remote URLs or auth errors may contain secrets.
            raise PublicationError(f'{args[0]} operation failed (exit {result.returncode})')
        if merge_api and (status is None or not 200 <= status < 300):
            raise PublicationError('Merge response status unavailable or unsuccessful')
        return body.strip()

    def git(self, *args):
        return self.run('git', *args)

    def snapshot(self):
        result = devctl.capture(self.machine, shlex.join([
            'python3', '-c', SNAPSHOT_SCRIPT, self.workspace]), separate_stderr=True)
        if result.returncode:
            raise PublicationError('Cannot fingerprint registered repository')
        return json.loads(result.stdout)

    def review_evidence(self, snapshot):
        """Local registered workspace and freshly verified object IDs only."""
        from chief_evidence import EVIDENCE_SCRIPT
        result = devctl.capture(self.machine, shlex.join([
            'python3', '-c', EVIDENCE_SCRIPT, self.workspace,
            snapshot['tree'], snapshot.get('head') or '']), separate_stderr=True)
        if result.returncode:
            raise PublicationError('Cannot safely extract bounded Chief evidence')
        return json.loads(result.stdout)


# The normal push protocol already uses the advertised old OID as a CAS. A
# private pre-push hook additionally binds that advertisement to our durable
# expected head; no force flag, user hook, or implicit ref can expand the push.
CAS_PUSH_SCRIPT = r'''
import os, subprocess, sys, tempfile
workspace, branch, expected, target = sys.argv[1:]
with tempfile.TemporaryDirectory() as hooks:
    hook = os.path.join(hooks, 'pre-push')
    with open(hook, 'w') as f:
        f.write('#!/usr/bin/env python3\nimport sys\n')
        f.write('rows = [line.split() for line in sys.stdin]\n')
        f.write('sys.exit(0 if len(rows) == 1 and len(rows[0]) == 4 and rows[0][1:] == ' +
                repr([target, 'refs/heads/' + branch, expected]) + ' else 1)\n')
    os.chmod(hook, 0o700)
    result = subprocess.run(['git', '-C', workspace, '-c', 'core.hooksPath=' + hooks,
        '-c', 'push.followTags=false', 'push', '--no-follow-tags', '--porcelain',
        'origin', target + ':refs/heads/' + branch], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    sys.exit(result.returncode)
'''


def converge_merge(repo, item, entry, persist):
    """Resume only our expected old/target refs; never reset, rebase or force."""
    branch, old, base = item['publication_branch'], item['commit_sha'], entry['base']
    result = item['post_merge_convergence']
    target = result['master_sha']
    try:
        current = repo.snapshot()
        if (current['branch'] != branch or current['head'] not in (old, target) or current['status'] or
                current['origin'] != item['publication_origin']):
            raise PublicationError('Registered development branch moved or worktree is dirty')
        def remote(name):
            refs = repo.git('ls-remote', 'origin', 'refs/heads/' + name).split()
            if len(refs) != 2 or refs[1] != 'refs/heads/' + name:
                raise PublicationError('Cannot prove remote branch identity')
            return refs[0]
        if remote(base) != target:
            raise PublicationError('Master moved after authorized merge')
        if remote(branch) not in (old, target):
            raise PublicationError('Remote development branch moved concurrently')
        repo.git('fetch', '--no-tags', 'origin', 'refs/heads/' + base + ':refs/remotes/origin/' + base)
        if repo.git('rev-parse', 'FETCH_HEAD') != target:
            raise PublicationError('Fetched master differs from authorized merge')
        # merge-base must succeed BEFORE either branch is updated.
        repo.git('merge-base', '--is-ancestor', old, target)
        if current['head'] == old:
            # CAS the checked-out branch, then safely update its index/worktree.
            # If read-tree fails, the next snapshot is dirty and recovery stops.
            repo.git('update-ref', 'refs/heads/' + branch, target, old)
            persist()
            observed = repo.snapshot()
            if observed['branch'] != branch or observed['head'] != target:
                raise PublicationError('Local branch changed after expected-head update')
            repo.git('read-tree', '-u', '-m', old, target)
        if remote(branch) == old:
            repo.run('python3', '-c', CAS_PUSH_SCRIPT, repo.workspace, branch, old, target)
        final = repo.snapshot()
        if (final['head'] != target or final['branch'] != branch or final['status'] or
                remote(branch) != target or remote(base) != target):
            raise PublicationError('Final development/master convergence could not be proven')
        result.update(status='DONE', diagnostic=None, local_head=target, remote_dev=target)
    except Exception:
        # Exceptions from Git/SSH may contain credentials. Store fixed diagnostics.
        result.update(status='BLOCKED', diagnostic='PR merged; post-merge fast-forward convergence could not be proven. Inspect registered local dev, remote dev and master; no force/reset/rebase was attempted.')
    persist()


def github_repo(origin):
    match = re.fullmatch(r'(?:git@github\.com:|https://github\.com/|ssh://git@github\.com/)'
                         r'([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+?)(?:\.git)?', origin or '')
    if not match:
        raise PublicationError('origin must be an uncredentialed GitHub repository URL')
    return match[1]


def bound_merge_head(pr, item, base):
    """A converged branch tip is valid only for the durably bound merged PR."""
    target = (item.get('post_merge_convergence') or {}).get('master_sha')
    if not target and pr.get('headRefOid') == item['commit_sha']:
        return True
    return bool(target and pr.get('headRefOid') in (item['commit_sha'], target) and pr.get('state') == 'MERGED' and
                pr.get('number') == item.get('pr_number') and pr.get('url') == item.get('pr_url') and
                pr.get('headRefName') == item['publication_branch'] and pr.get('baseRefName') == base)


def validate_baseline(item, report, snapshot):
    baseline = report.get('git_baseline')
    if not baseline or report.get('adopted'):
        raise PublicationError('Pre-task Git baseline is unknown; automatic COMMIT is blocked')
    if not baseline.get('is_repo') or not baseline.get('clean') or baseline.get('error'):
        raise PublicationError('Pre-task Git baseline was dirty or unavailable')
    if baseline.get('detached') or not snapshot.get('branch'):
        raise PublicationError('Detached HEAD is not publishable')
    identity = ('branch', 'origin', *(n for n in ('root', 'push_urls') if n in baseline))
    expected_head = baseline.get('head')
    for previous in item.get('publication_history') or []:
        # A correction may build on our own published commit only after a
        # proven non-merge. The original clean baseline and full commit chain
        # remain authoritative; arbitrary HEAD movement is never adopted.
        convergence = previous.get('post_merge_convergence') or {}
        journal = previous.get('journal') or {}
        commit = journal.get('COMMIT') or {}
        before = commit.get('before') or {}
        reviewed = previous.get('reviewed_tree') or {}
        turn = previous.get('coding_turn')
        if (previous.get('owner_state') == 'PR_CREATED' and previous.get('approved_at') and
                convergence.get('status') == 'NOT_MERGED' and not convergence.get('master_sha') and
                (journal.get('MERGE') or {}).get('outcome') == 'NOT_MERGED' and
                commit.get('done') is True and (commit.get('command') or {}).get('action') == 'COMMIT' and
                commit.get('candidate') == previous.get('commit_sha') == convergence.get('expected_head') and
                commit.get('candidate') and before.get('head') == expected_head and
                before.get('fingerprint') and reviewed.get('fingerprint') == before['fingerprint'] and
                all(before.get(name) == baseline.get(name) for name in identity) and
                type(turn) is int and turn > 0 and type(item.get('coding_turn')) is int and turn < item['coding_turn']):
            expected_head = commit['candidate']
    for name in ('head', *identity):
        if snapshot.get(name) != (expected_head if name == 'head' else baseline.get(name)):
            raise PublicationError(f'Unexpected {name} movement since task start')
    reviewed = item.get('reviewed_tree')
    if not reviewed or snapshot['fingerprint'] != reviewed['fingerprint']:
        raise PublicationError('Current repository does not match independently reviewed state')


def prove_non_merge(repo, item, persist, *, allow_open=False):
    """Resolve only a bound, definitive non-merge; unavailable reads retain the hold."""
    result = item.get('post_merge_convergence')
    if (item.get('merged') or item.get('owner_state') == 'MERGED' or
            (result and (result.get('master_sha') or result.get('status') in ('BLOCKED', 'DONE', 'NOT_REQUIRED')))):
        return False  # Never undo known merge/convergence evidence.
    entry = (item.get('publication_journal') or {}).get('MERGE') or {}
    try:
        remote = github_repo(item.get('publication_origin'))
        number = item['pr_number']
        sha = item['commit_sha']
        if (not isinstance(number, int) or number < 1 or not sha or
                entry.get('repo') != remote or entry.get('outcome') == 'MERGED' or
                (entry.get('command') or {}).get('action') != 'MERGE' or not entry.get('base') or
                not item.get('publication_branch') or
                repo.cfg.get('github_repo', remote) != remote or
                item.get('pr_url') != f'https://github.com/{remote}/pull/{number}' or
                (result is not None and result.get('expected_head') != sha)):
            return False
        view = json.loads(repo.run('gh', 'pr', 'view', str(number), '--repo', 'github.com/' + remote,
            '--json', 'number,url,state,headRefOid,isCrossRepository,mergedAt,mergeCommit'))
        if (view.get('number') != number or view.get('url') != item['pr_url'] or
                view.get('headRefOid') != sha or view.get('isCrossRepository') is not False or
                view.get('mergedAt') or (view.get('mergeCommit') or {}).get('oid') or
                view.get('state') not in (('OPEN', 'CLOSED') if allow_open else ('CLOSED',))):
            return False
    except Exception:
        return False  # Never store provider errors or infer a result from a failed read.
    if result is None:
        method = entry['command'].get('method', 'squash')
        result = item['post_merge_convergence'] = dict(expected_head=sha, method=method,
            branch=item['publication_branch'], base=entry['base'],
            required=method == 'merge' and item['publication_branch'] == repo.cfg.get('long_lived_branch'))
    diagnostic = ('PR closed without merging; repository merge hold released.' if view['state'] == 'CLOSED'
                  else 'GitHub refused the merge; PR remains unmerged and repository merge hold released.')
    result.update(status='NOT_MERGED', diagnostic=diagnostic)
    entry['outcome'] = 'NOT_MERGED'
    item['owner_blocker'] = diagnostic
    persist()
    return True


def execute(repo, item, report, command, persist):
    """Failed preconditions cannot erase an uncertain earlier external effect."""
    try:
        return _execute(repo, item, report, command, persist)
    except PublicationError:
        if command.get('action') == 'MERGE' and (
                item.get('post_merge_convergence') or {}).get('status') != 'NOT_MERGED':
            # Also inspect after early permanent errors (dirty tree, stale Chief,
            # moved refs). A CLOSED observation can settle a previous uncertainty
            # without authorizing a publication or changing any Git history.
            prove_non_merge(repo, item, persist)
        raise


def _execute(repo, item, report, command, persist):
    """Journal before mutations. Recover uncertain results by inspecting Git/GitHub."""
    from chief_review import approval_complete
    if not approval_complete(item):
        raise PublicationError('Current Chief PASS is required for publication')
    action = command['action']
    current = repo.snapshot()
    journal = item.setdefault('publication_journal', {})
    entry = journal.get(action)
    if entry and entry.get('command') != command:
        raise PublicationError('An interrupted action must use its original owner command')
    if action == 'COMMIT':
        if not entry:
            validate_baseline(item, report, current)
            if current['head'] and repo.git('rev-parse', current['head'] + '^{tree}') == current['tree']:
                raise PublicationError('Reviewed tree contains no commit changes')
            message = command.get('message') or f"DevOrchestrator: task {item['task_id']}"
            args = ['commit-tree', current['tree']]
            if current['head']:
                args += ['-p', current['head']]
            candidate = repo.git(*args, '-m', message)
            entry = {'command': command, 'before': current, 'candidate': candidate}
            journal[action] = entry
            persist()
        before, candidate = entry['before'], entry['candidate']
        if any(current.get(n) != before.get(n) for n in ('branch', 'origin', 'root', 'push_urls')):
            raise PublicationError('Branch/origin changed during COMMIT')
        if current['tree'] != before['tree']:
            raise PublicationError('Reviewed content changed during COMMIT')
        if current['head'] == before['head']:
            if current['fingerprint'] != before['fingerprint']:
                raise PublicationError('Reviewed index/state changed during COMMIT')
            repo.git('update-ref', 'refs/heads/' + before['branch'], candidate,
                     before['head'] or '0' * len(candidate))
        elif current['head'] != candidate:
            raise PublicationError('Unexpected HEAD during COMMIT recovery')
        elif current['index'] != before['index'] and current['status']:
            raise PublicationError('Index changed during interrupted COMMIT; preserving user index')
        # No working files are rewritten. This is also safe after an interrupted update-ref.
        repo.git('read-tree', candidate)
        after = repo.snapshot()
        if after['head'] != candidate or after['tree'] != before['tree'] or after['status']:
            raise PublicationError('Repository changed while completing COMMIT')
        item['commit_sha'] = candidate
        item['publication_branch'] = before['branch']
        item['publication_origin'] = before['origin']
    else:
        sha = item.get('commit_sha')
        branch = item.get('publication_branch')
        resume_target = (item.get('post_merge_convergence') or {}).get('master_sha') if action == 'MERGE' else None
        if not sha or current['head'] not in (sha, resume_target) or current['branch'] != branch or current['status']:
            raise PublicationError('Published branch/HEAD/worktree changed')
        if current['origin'] != item.get('publication_origin'):
            raise PublicationError('origin changed after COMMIT')
        remote = github_repo(current['origin'])
        push_urls = repo.git('remote', 'get-url', '--push', '--all', 'origin').splitlines()
        fetch_urls = repo.git('remote', 'get-url', '--all', 'origin').splitlines()
        if fetch_urls != [current['origin']] or len(push_urls) != 1 or github_repo(push_urls[0]) != remote:
            raise PublicationError('origin fetch/push URLs do not identify the same single GitHub repository')
        expected = repo.cfg.get('github_repo')
        if expected and remote != expected:
            raise PublicationError('origin does not match registered github_repo')
        gh_repo = 'github.com/' + remote
        default = json.loads(repo.run('gh', 'repo', 'view', gh_repo, '--json', 'defaultBranchRef,nameWithOwner,url'))
        if default.get('nameWithOwner', '').lower() != remote.lower() or default.get('url', '').lower() != ('https://github.com/' + remote).lower():
            raise PublicationError('GitHub repository identity changed or redirected')
        base = default['defaultBranchRef']['name']
        if branch == base:
            raise PublicationError('Publication requires a non-default branch')
        if not entry:
            entry = {'command': command, 'base': base, 'repo': remote}
            journal[action] = entry
            persist()
        if entry['repo'] != remote or entry['base'] != base:
            raise PublicationError('GitHub repository/base changed')
        if action == 'PUSH':
            refs = repo.git('ls-remote', 'origin', 'refs/heads/' + branch).split()
            if not refs or refs[0] != sha:
                # Local hooks and followTags config cannot expand owner authorization.
                repo.git('push', '--no-verify', '--no-follow-tags', 'origin', sha + ':refs/heads/' + branch)
            refs = repo.git('ls-remote', 'origin', 'refs/heads/' + branch).split()
            if not refs or refs[0] != sha:
                raise PublicationError('Remote branch does not match authorized commit')
            item['pushed_branch'] = branch
        else:
            refs = repo.git('ls-remote', 'origin', 'refs/heads/' + branch).split()
            if not refs or refs[0] not in (sha, resume_target):
                raise PublicationError('Remote branch changed after PUSH')
            prs = json.loads(repo.run('gh', 'pr', 'list', '--repo', gh_repo,
                             '--head', branch, '--base', base, '--state', 'all',
                             '--json', 'number,url,state,headRefOid,headRefName,baseRefName,headRepositoryOwner,isCrossRepository'))
            if any(pr.get('headRefOid') != sha or pr.get('isCrossRepository') for pr in prs if pr.get('state') == 'OPEN'):
                raise PublicationError('Existing open PR does not match authorized repository/commit')
            prs = [pr for pr in prs if not pr.get('isCrossRepository') and (
                bound_merge_head(pr, item, base) if action == 'MERGE' else pr.get('headRefOid') == sha)]
            if len(prs) > 1:
                raise PublicationError('Ambiguous PR for branch')
            if action == 'CREATE_PR':
                if prs and prs[0]['state'] != 'OPEN':
                    raise PublicationError('Existing PR is closed or merged')
                if not prs:
                    repo.run('gh', 'pr', 'create', '--repo', gh_repo, '--head', branch,
                             '--base', base, '--title', command.get('title') or f"Task {item['task_id']}",
                             '--body', command.get('body') or 'Independently reviewed DevOrchestrator task.')
                    prs = json.loads(repo.run('gh', 'pr', 'list', '--repo', gh_repo, '--head', branch,
                                     '--base', base, '--state', 'open', '--json', 'number,url,headRefOid,isCrossRepository'))
                    prs = [pr for pr in prs if pr.get('headRefOid') == sha and not pr.get('isCrossRepository')]
                if len(prs) != 1:
                    raise PublicationError('Cannot confirm created PR')
                item['pr_url'] = prs[0]['url']
                item['pr_number'] = prs[0]['number']
            elif action == 'MERGE':
                if not prs or prs[0]['number'] != item.get('pr_number') or prs[0]['url'] != item.get('pr_url'):
                    raise PublicationError('PR identity changed')
                pr = prs[0]
                if pr['state'] == 'MERGED':
                    item['merged'] = True
                    entry['outcome'] = 'MERGED'
                    persist()  # Latch a bound positive observation before further checks.
                convergence_required = command.get('method') == 'merge' and branch == repo.cfg.get('long_lived_branch')
                if item.get('merged') and pr['state'] != 'MERGED':
                    raise PublicationError('Merged PR observation changed')
                if pr['state'] == 'CLOSED':
                    raise PublicationError('PR is closed without merge')
                method = command.get('method', 'squash')
                if method not in ('squash', 'merge', 'rebase'):
                    raise PublicationError('Unsupported merge method')
                convergence = item.get('post_merge_convergence')
                if convergence is not None and (convergence.get('expected_head') != sha or
                        # Earlier convergence records existed only for required
                        # normal merges; registry changes cannot downgrade them.
                        convergence.get('required', True) != convergence_required or
                        any(convergence.get(key, value) != value for key, value in
                            (('branch', branch), ('base', base), ('method', method)))):
                    raise PublicationError('Post-merge convergence assessment changed')
                if pr['state'] != 'MERGED':
                    uncertain_retry = convergence is not None and convergence.get('status') == 'PENDING'
                    # Assess only a new publication, before the external mutation.
                    # A merged historical PR with no record must remain archival.
                    if convergence is None:
                        item['post_merge_convergence'] = dict(status='PENDING', expected_head=sha,
                            required=convergence_required, branch=branch, base=base, method=method)
                    else:
                        if item.get('owner_blocker') == convergence.get('diagnostic'):
                            item.pop('owner_blocker', None)
                        convergence.update(status='PENDING', diagnostic=None)
                        convergence.pop('refusal_http_status', None)
                    entry['outcome'] = 'PENDING'
                    persist()
                    try:
                        merged = json.loads(repo.run('gh', 'api', '--hostname', 'github.com', '--method', 'PUT',
                            f"repos/{remote}/pulls/{pr['number']}/merge",
                            '--raw-field', 'sha=' + sha, '--raw-field', 'merge_method=' + method))
                    except MergeRefused as exc:
                        item['post_merge_convergence']['refusal_http_status'] = exc.status
                        persist()
                        if not uncertain_retry:
                            prove_non_merge(repo, item, persist, allow_open=True)
                        raise
                    if merged.get('merged') is not True:
                        if merged.get('merged') is False and not uncertain_retry:
                            # An explicit rejection resolves this fresh attempt,
                            # but not a possibly in-flight older uncertain call.
                            prove_non_merge(repo, item, persist, allow_open=True)
                        raise PublicationError('GitHub refused the authorized merge')
                    item['merged'] = True
                    entry['outcome'] = 'MERGED'
                    persist()  # A later failed/stale read cannot undo a successful merge.
                convergence = item.get('post_merge_convergence')
                if convergence is not None and not convergence_required:
                    view = json.loads(repo.run('gh', 'pr', 'view', str(pr['number']), '--repo', gh_repo,
                                               '--json', 'state,headRefOid'))
                    if view['state'] != 'MERGED' or view['headRefOid'] != sha:
                        raise PublicationError('Merge has not completed')
                    convergence.update(status='NOT_REQUIRED', required=False, diagnostic=None)
                persist()  # GitHub merge is real even if convergence later fails.
                if convergence_required and convergence is not None:
                    try:
                        view = json.loads(repo.run('gh', 'pr', 'view', str(pr['number']), '--repo', gh_repo,
                            '--json', 'number,url,state,headRefOid,headRefName,baseRefName,mergeCommit,isCrossRepository'))
                    except Exception:
                        view = {}
                    target = (view.get('mergeCommit') or {}).get('oid')
                    if (view.get('state') != 'MERGED' or not bound_merge_head(view, item, base) or
                            view.get('headRefName') != branch or view.get('baseRefName') != base or
                            view.get('isCrossRepository') is not False or
                            not isinstance(target, str) or not re.fullmatch(r'[0-9a-f]{40,64}', target)):
                        item['post_merge_convergence'].update(status='BLOCKED',
                            diagnostic='PR merged; exact merge commit and branch identities unavailable')
                        persist()
                    else:
                        convergence = item['post_merge_convergence']
                        convergence.setdefault('master_sha', target)
                        if convergence.get('master_sha') != target or convergence.get('expected_head') != sha:
                            convergence.update(status='BLOCKED', diagnostic='PR merged; convergence identity changed')
                            persist()
                        else:
                            persist()
                            converge_merge(repo, item, entry, persist)
    entry['done'] = True

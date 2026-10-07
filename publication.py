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
        if args[0] == 'gh' and '--body' in args:
            index = args.index('--body')
            body = args[index + 1]
            args = args[:index] + args[index + 2:]
            script = "import subprocess,sys,tempfile; f=tempfile.NamedTemporaryFile(mode='w'); f.write(sys.argv[1]); f.flush(); p=subprocess.run(sys.argv[2:]+['--body-file',f.name]); sys.exit(p.returncode)"
            args = ('python3', '-c', script, body, *args)
        command = 'cd ' + shlex.quote(self.workspace) + ' && ' + shlex.join(args)
        result = devctl.capture(self.machine, command, separate_stderr=True)
        if result.returncode:
            # Do not echo command output: remote URLs or auth errors may contain secrets.
            raise PublicationError(f'{args[0]} operation failed (exit {result.returncode})')
        return result.stdout.strip()

    def git(self, *args):
        return self.run('git', *args)

    def snapshot(self):
        result = devctl.capture(self.machine, shlex.join([
            'python3', '-c', SNAPSHOT_SCRIPT, self.workspace]), separate_stderr=True)
        if result.returncode:
            raise PublicationError('Cannot fingerprint registered repository')
        return json.loads(result.stdout)


def github_repo(origin):
    match = re.fullmatch(r'(?:git@github\.com:|https://github\.com/|ssh://git@github\.com/)'
                         r'([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+?)(?:\.git)?', origin or '')
    if not match:
        raise PublicationError('origin must be an uncredentialed GitHub repository URL')
    return match[1]


def validate_baseline(item, report, snapshot):
    baseline = report.get('git_baseline')
    if not baseline or report.get('adopted'):
        raise PublicationError('Pre-task Git baseline is unknown; automatic COMMIT is blocked')
    if not baseline.get('is_repo') or not baseline.get('clean') or baseline.get('error'):
        raise PublicationError('Pre-task Git baseline was dirty or unavailable')
    if baseline.get('detached') or not snapshot.get('branch'):
        raise PublicationError('Detached HEAD is not publishable')
    for name in ('head', 'branch', 'origin', *(n for n in ('root', 'push_urls') if n in baseline)):
        if snapshot.get(name) != baseline.get(name):
            raise PublicationError(f'Unexpected {name} movement since task start')
    reviewed = item.get('reviewed_tree')
    if not reviewed or snapshot['fingerprint'] != reviewed['fingerprint']:
        raise PublicationError('Current repository does not match independently reviewed state')


def execute(repo, item, report, command, persist):
    """Journal before mutations. Recover uncertain results by inspecting Git/GitHub."""
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
        if not sha or current['head'] != sha or current['branch'] != branch or current['status']:
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
            if not refs or refs[0] != sha:
                raise PublicationError('Remote branch changed after PUSH')
            prs = json.loads(repo.run('gh', 'pr', 'list', '--repo', gh_repo,
                             '--head', branch, '--base', base, '--state', 'all',
                             '--json', 'number,url,state,headRefOid,headRepositoryOwner,isCrossRepository'))
            if any(pr.get('headRefOid') != sha or pr.get('isCrossRepository') for pr in prs if pr.get('state') == 'OPEN'):
                raise PublicationError('Existing open PR does not match authorized repository/commit')
            prs = [pr for pr in prs if pr.get('headRefOid') == sha and not pr.get('isCrossRepository')]
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
                if pr['state'] == 'CLOSED':
                    raise PublicationError('PR is closed without merge')
                if pr['state'] != 'MERGED':
                    method = command.get('method', 'squash')
                    if method not in ('squash', 'merge', 'rebase'):
                        raise PublicationError('Unsupported merge method')
                    merged = json.loads(repo.run('gh', 'api', '--hostname', 'github.com', '--method', 'PUT',
                        f"repos/{remote}/pulls/{pr['number']}/merge",
                        '--raw-field', 'sha=' + sha, '--raw-field', 'merge_method=' + method))
                    if not merged.get('merged'):
                        raise PublicationError('GitHub refused the authorized merge')
                    view = json.loads(repo.run('gh', 'pr', 'view', str(pr['number']), '--repo', gh_repo,
                                               '--json', 'state,headRefOid'))
                    if view['state'] != 'MERGED' or view['headRefOid'] != sha:
                        raise PublicationError('Merge has not completed')
                item['merged'] = True
    entry['done'] = True

"""Bounded read-only extraction from the snapshot's immutable Git objects.

The worker receives only a locally registered workspace and verified object IDs.
Paths come from that Git tree and are literal pathspecs, never issue commands.
"""
from repository_safety import GIT_PREAMBLE

EVIDENCE_SCRIPT = GIT_PREAMBLE + r'''
import hashlib, json, os, re, subprocess, sys
workspace, tree, head = sys.argv[1:]
if not re.fullmatch(r'[0-9a-f]{40,64}', tree) or (head and not re.fullmatch(r'[0-9a-f]{40,64}', head)):
    raise ValueError('Invalid local tree identity')
assert_git_config(workspace)
def git(*args, input=None):
    return safe_git(workspace,args,input=input)
base = head or git('mktree', input=b'').decode().strip()
raw = git('diff-tree', '--no-commit-id', '--raw', '-z', '-r', '--no-renames', base, tree).split(b'\0')
files, patches, total = [], [], 0
secret_path = re.compile(r'(^|/)(\.git|\.env[^/]*|\.ssh|\.aws|\.kube|credentials[^/]*|secrets?[^/]*|projects\.json)(/|$)|\.(pem|key|p12|pfx|keystore|env)$', re.I)
def clean(text):
    # Worker credentials may differ from the bridge's environment.
    for key, value in os.environ.items():
        if len(value) >= 8 and any(word in key.upper() for word in ('TOKEN', 'PASSWORD', 'SECRET', 'API_KEY')):
            text = text.replace(value, '[REDACTED]')
    text = re.sub(r'https?://[^\s/@]+@', 'https://[REDACTED]@', text)
    text = re.sub(r'\b(?:gh[pousr]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,}|sk-[A-Za-z0-9_-]{20,})\b', '[REDACTED]', text)
    text = re.sub(r'(api\.telegram\.org/bot)[^/\s]+', r'\1[REDACTED]', text)
    text = re.sub(r'-----BEGIN [^-\n]*PRIVATE KEY-----.*?-----END [^-\n]*PRIVATE KEY-----', '[REDACTED PRIVATE KEY]', text, flags=re.S)
    text = re.sub("(?im)^([ +\\-].*?\\b(?:[A-Za-z0-9_]*(?:password|passwd|secret|token|api_key)|authorization)\\b[\"']?\\s*[:=]\\s*).+$", r'\1[REDACTED]', text)
    return text
for i in range(0, len(raw)-1, 2):
    header, path = raw[i], raw[i+1]
    if len(files) >= 1000: raise ValueError('Too many changed files')
    oldmode, mode, old, new, status = header[1:].decode('ascii').split()
    name = path.decode('utf-8', errors='strict')
    if name.startswith('/') or '..' in name.split('/') or '.git' in name.split('/'):
        raise ValueError('Invalid repository path')
    entry = dict(path=clean(name), status=status, old_mode=oldmode, mode=mode, old_oid=old, oid=new)
    excluded = bool(secret_path.search(name))
    stat = git('diff', '--no-ext-diff', '--no-textconv', '--no-renames', '--numstat', base, tree, '--', name).decode('utf-8')
    binary = stat.startswith('-\t-\t')
    for prefix, oid in (('old_', old), ('', new)):
        if set(oid) == {'0'} or excluded: continue
        size = int(git('cat-file', '-s', oid))
        entry[prefix + 'size'] = size
        if size > 2*1024*1024:
            if binary: continue  # The Git object hash and size identify large binaries.
            raise ValueError('Textual blob exceeds safe evidence bound')
        content = git('cat-file', 'blob', oid)
        total += len(content)
        if total > 8*1024*1024: raise ValueError('Evidence exceeds safe extraction bound')
        entry[prefix + 'sha256'] = hashlib.sha256(content).hexdigest()
        try: content.decode('utf-8')
        except UnicodeDecodeError: binary = True
        if re.search(rb'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', content): binary = True
    entry.update(binary=binary, excluded=excluded, stat=clean(stat))
    files.append(entry)
    if not excluded and not binary:
        patch = git('diff', '--no-ext-diff', '--no-textconv', '--no-renames', '--full-index', '--no-color',
                    '--diff-algorithm=myers', '--unified=3', '--inter-hunk-context=0',
                    '--src-prefix=a/', '--dst-prefix=b/', base, tree, '--', name).decode('utf-8')
        patches.append(clean(patch))
        if sum(len(p) for p in patches) > 5*1024*1024:
            raise ValueError('Patch exceeds safe evidence bound')
print(json.dumps(dict(files=files, patch=''.join(patches)), ensure_ascii=True))
'''

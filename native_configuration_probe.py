"""Fixed offline config/help probe inside native Codex sandbox; never inference."""
import json
import os
from pathlib import Path
import sys

def main():
    spec=json.loads(sys.argv[1]);argv=spec['argv']
    if spec['mode']=='features':
        if argv[-2:]!=['features','list']:raise SystemExit('Unexpected offline command')
    elif spec['mode']=='argv-help':
        if argv[1]!='exec' or argv[-1]!='--help':raise SystemExit('Inference is not authorized')
    else:raise SystemExit('Unknown offline mode')
    names={name:os.readlink('/proc/self/ns/'+name) for name in ('user','pid','mnt','net')}
    if any(names[name]==spec['host_ns'][name] for name in names):raise SystemExit('Native namespace isolation missing')
    env=spec['offline_env']
    if set(env)!={'HOME','CODEX_HOME','TMPDIR','PATH','LANG'} or Path(env['CODEX_HOME']).name!='empty-codex':
        raise SystemExit('Invalid credential-free probe environment')
    if (Path(env['CODEX_HOME'])/'auth.json').exists():raise SystemExit('Probe credentials present')
    for path in ('/home/siovush/.codex','/home/siovush/DevOrchestrator','/home/siovush/DevOrchestrator-v1-rc-20261010'):
        try:fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY)
        except (FileNotFoundError,PermissionError):continue
        else:os.close(fd);raise SystemExit('Protected host directory accessible')
    row=dict(line.split(':',1) for line in Path('/proc/self/status').read_text().splitlines() if ':' in line)
    if int(row['CapEff'].strip(),16)!=0 or row['Seccomp'].strip()!='2' or row['NoNewPrivs'].strip()!='1':
        raise SystemExit('Native kernel safety flags missing')
    # No socket connect, bind, credentials or provider calls. The only permitted
    # exec targets are features/list or exec/--help on the same pinned binary.
    print('OFFLINE_NATIVE_CONFIG_ISOLATION_PASS '+json.dumps(names,sort_keys=True),flush=True)
    os.execve(argv[0],argv,env)

if __name__=='__main__':main()

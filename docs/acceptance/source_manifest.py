"""Hash the sources a controlled smoke checkout must match.

Read-only. A missing guard file fails the check. The printed JSON is the
manifest an accepted release commit has to reproduce before live activation.
"""
import argparse
import hashlib
import json
import sys
from pathlib import Path


FILES = (
    'external_effects.py',
    'bridge_effects.py',
    'control_loop.py',
    'devctl.py',
    'github_transport.py',
    'mcp_server.py',
    'mcp_smoke.py',
    'notifications.py',
    'operational_state.py',
    'publication.py',
    'remote_worker.py',
    'runtime_liveness.py',
    'scheduler.py',
    'task_bridge.py',
    'telegram_setup.py',
    'transition_executor.py',
    'worker_runtime.py',
)


def manifest(root):
    root = Path(root).resolve()
    rows = []
    for rel in FILES:
        path = (root / rel).resolve()
        if path.parent != root or not path.is_file():
            raise SystemExit('missing guard source: ' + rel)
        rows.append({'path': rel, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser(description='Print the smoke source manifest')
    parser.add_argument('--root', required=True)
    args = parser.parse_args(argv)
    json.dump(manifest(args.root), sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write('\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

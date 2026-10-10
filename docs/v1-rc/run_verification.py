"""Launch authorized synthetic regression only; no production paths are mounted."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import uuid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("group", choices=("targeted", "full", "idle", "deferred"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    run = root / ".rc-lab" / (args.group + "-" + uuid.uuid4().hex[:8])
    run.mkdir(mode=0o700)
    for name in ("home", "tmp", "evidence"):
        (run / name).mkdir(mode=0o700)
    argv = ["/usr/bin/bwrap", "--unshare-all", "--die-with-parent", "--new-session",
            "--cap-drop", "ALL", "--ro-bind", "/usr", "/usr",
            "--symlink", "usr/bin", "/bin", "--symlink", "usr/lib", "/lib",
            "--symlink", "usr/lib64", "/lib64", "--ro-bind", str(root / ".venv"), "/venv",
            "--ro-bind", str(Path("/usr/bin/awk").resolve()), "/etc/alternatives/awk",
            "--ro-bind", str(root), "/source", "--tmpfs", "/source/.git",
            "--tmpfs", "/source/.venv", "--tmpfs", "/source/.rc-lab",
            "--bind", str(run), "/lab", "--proc", "/proc", "--dev", "/dev",
            "--tmpfs", "/tmp", "--tmpfs", "/run", "--clearenv",
            "--setenv", "HOME", "/lab/home", "--setenv", "TMPDIR", "/lab/tmp",
            "--setenv", "PATH", "/venv/bin:/usr/bin:/bin",
            "--setenv", "PYTHONPATH", "/source/tests:/source",
            "--setenv", "PYTHONDONTWRITEBYTECODE", "1",
            "--setenv", "LANG", "C.UTF-8", "--setenv", "LC_ALL", "C.UTF-8",
            "--setenv", "RC_HOST_PID_NS", os.readlink("/proc/self/ns/pid"),
            "--setenv", "RC_HOST_NET_NS", os.readlink("/proc/self/ns/net"),
            "--chdir", "/source", "/venv/bin/python", "/source/docs/v1-rc/verify_suite.py",
            "--group", args.group]
    (run / "evidence/command.json").write_text(json.dumps(argv, indent=2) + "\n")
    started = datetime.now(timezone.utc).isoformat()
    with (run / "evidence/suite.log").open("w") as log:
        try:
            code = subprocess.run(argv, stdout=log, stderr=subprocess.STDOUT,
                                  timeout=900, start_new_session=True).returncode
        except subprocess.TimeoutExpired:
            code = 124
    (run / "evidence/execution.json").write_text(json.dumps({
        "group": args.group, "exit_code": code, "started_utc": started,
        "finished_utc": datetime.now(timezone.utc).isoformat(),
        "production_operations": False,
    }, indent=2) + "\n")
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
              for p in sorted((run / "evidence").iterdir()) if p.is_file()}
    (run / "evidence/SHA256.json").write_text(json.dumps(hashes, indent=2) + "\n")
    print(json.dumps({"exit_code": code, "evidence": str(run / "evidence")}))
    return code


if __name__ == "__main__":
    raise SystemExit(main())

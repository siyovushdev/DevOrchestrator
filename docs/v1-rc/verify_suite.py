"""Run unmodified regression tests after proving private release-lab isolation."""
import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import socket
import sys
import time
import unittest


def flatten(suite):
    for test in suite:
        if isinstance(test, unittest.TestSuite):
            yield from flatten(test)
        else:
            yield test


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", choices=("targeted", "full", "idle", "deferred"), required=True)
    args = parser.parse_args()
    assert Path.home() == Path("/lab/home")
    assert os.environ["TMPDIR"] == "/lab/tmp"
    assert os.readlink("/proc/self/ns/pid") != os.environ["RC_HOST_PID_NS"]
    assert os.readlink("/proc/self/ns/net") != os.environ["RC_HOST_NET_NS"]
    for path in ("/home/siovush", "/run/user", "/source/projects.json"):
        assert not Path(path).exists(), path
    assert not any(Path("/source/.git").iterdir())
    assert not any(Path("/lab/home").iterdir())
    assert not any(Path("/lab/tmp").iterdir())
    mount = next(line for line in Path("/proc/self/mountinfo").read_text().splitlines()
                 if line.split()[4] == "/source")
    assert "ro" in mount.split()[5].split(",")
    with socket.socket() as sock:
        sock.settimeout(1)
        assert sock.connect_ex(("127.0.0.1", 8787)) != 0
    # Runtime bytes remain the accepted source; the candidate manifest also pins
    # the documented fixture synchronization correction. Docs may evolve.
    source_rows = json.loads(Path("/source/docs/v1-rc/evidence/candidate-python-manifest.json").read_text())
    checked = [r for r in source_rows if r["path"].endswith(".py")]
    for row in checked:
        assert hashlib.sha256((Path("/source") / row["path"]).read_bytes()).hexdigest() == row["sha256"]
    evidence = Path("/lab/evidence")
    evidence.joinpath("isolation.json").write_text(json.dumps({
        "status": "PASS", "source_read_only": True, "production_visible": False,
        "host_mcp_reachable": False, "home_and_tmp_initially_empty": True,
        "private_pid_and_network_namespaces": True, "verified_python_sources": len(checked),
        "pid_namespace": os.readlink("/proc/self/ns/pid"),
        "network_namespace": os.readlink("/proc/self/ns/net"),
    }, indent=2) + "\n")
    if args.group in ("idle", "deferred"):
        import asyncio
        from measure_semantic_durability import measure
        result = asyncio.run(measure("fixed", 60, 10, args.group == "deferred"))
        evidence.joinpath("measurement.json").write_text(json.dumps(result, indent=2) + "\n")
        return 0
    loader = unittest.TestLoader()
    if args.group == "full":
        suite = loader.discover("tests")
    else:
        suite = loader.loadTestsFromNames([
            "test_safety_latch_final", "test_acceptance_report_latch",
            "test_acceptance_effect_latch", "test_acceptance_effect_gate",
            "test_acceptance_archived_merge", "test_chief_delivery_retry",
            "test_recovery", "test_request_protocol", "test_resumable_control",
            "test_resumable_semantic_durability", "test_publication_closure",
            "test_operational_stability.OwnedCleanupTests",
            "test_worker_integration.WorkerIntegrationTests",
        ])
    ids = [test.id() for test in flatten(suite)]
    evidence.joinpath("test-inventory.json").write_text(json.dumps(ids, indent=2) + "\n")
    started = time.monotonic()
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    seconds = time.monotonic() - started
    report = {
        "status": "PASS" if result.wasSuccessful() and not result.skipped else "FAIL",
        "group": args.group, "tests": result.testsRun, "discovered": len(ids),
        "unique_ids": len(set(ids)), "seconds": seconds,
        "failures": len(result.failures), "errors": len(result.errors),
        "skips": len(result.skipped), "python": sys.version,
        "mcp": importlib.metadata.version("mcp"),
        "failed_ids": [test.id() for test, _ in result.failures + result.errors],
        "skipped_ids": [test.id() for test, _ in result.skipped],
    }
    evidence.joinpath("result.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0 if report["status"] == "PASS" and result.testsRun == len(ids) else 1


if __name__ == "__main__":
    raise SystemExit(main())

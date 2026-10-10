"""Read-only check of the delivered candidate using an external manifest anchor."""
import argparse
import hashlib
import json
from pathlib import Path
import stat


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-manifest-sha256", required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    manifest = root / "docs/v1-rc/evidence/candidate-tree-manifest.json"
    assert hashlib.sha256(manifest.read_bytes()).hexdigest() == args.expected_manifest_sha256
    rows = json.loads(manifest.read_text())["files"]
    assert len({row["path"] for row in rows}) == len(rows)
    for row in rows:
        path = root / row["path"]
        assert path.resolve().is_relative_to(root) and not path.is_symlink(), row["path"]
        assert path.is_file(), row["path"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == row["sha256"], row["path"]
        assert path.stat().st_size == row["size"], row["path"]
        assert stat.S_IMODE(path.stat().st_mode) == row["mode"], row["path"]
    print(json.dumps({"status": "PASS", "verified_files": len(rows),
                      "candidate_manifest_sha256": args.expected_manifest_sha256}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

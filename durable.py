"""Atomic durable JSON and a single bridge writer per state directory."""
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import tempfile


def read_json(path, default=None, *, object_pairs_hook=None):
    path = Path(path)
    if not path.exists():
        return default
    # Corrupt state is a fail-closed error, never a fresh empty state.
    # Optional strict object decoding for new contracts; V1 keeps JSON defaults.
    return json.loads(path.read_text(), object_pairs_hook=object_pairs_hook)


def write_json_if_changed(path, data):
    """
    Durably replace a JSON file only when its serialized contents changed.

    Real state transitions keep the existing atomic write/fsync semantics.
    Repeated no-op save_state() calls do not rewrite the whole file.
    """
    path = Path(path)
    serialized = json.dumps(
        data,
        indent=2,
        ensure_ascii=False,
    )

    try:
        if path.read_text() == serialized:
            return False
    except FileNotFoundError:
        pass

    write_json(path, data)
    return True


def write_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(prefix=path.name + '.', dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(data, stream, indent=2, ensure_ascii=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


@contextmanager
def exclusive(path, blocking=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a') as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)

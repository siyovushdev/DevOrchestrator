import json


def coding_result(state='READY_FOR_REVIEW', blocker=None):
    return 'DEVORCHESTRATOR_RESULT: ' + json.dumps(dict(state=state, blocker=blocker,
        implementation='DONE', verification='PASS', self_review='PASS', summary='tested'))


def review_result(verdict='PASS', blocker=None, findings=None):
    return 'DEVORCHESTRATOR_REVIEW: ' + json.dumps(dict(verdict=verdict, blocker=blocker,
        summary='Reviewed', findings=findings or []))


def proc_fixture(root):
    """A zombie leader and a live child in the original session/process group."""
    proc = root / 'proc'
    boot = proc / 'sys/kernel/random/boot_id'
    boot.parent.mkdir(parents=True); boot.write_text('test-boot')
    for pid, state in ((123, 'Z'), (456, 'S')):
        directory = proc / str(pid); directory.mkdir()
        fields = [state, '1', '123', '123'] + ['0'] * 15 + ['10']
        (directory / 'stat').write_text(f'{pid} (fixture) ' + ' '.join(fields))
    return proc

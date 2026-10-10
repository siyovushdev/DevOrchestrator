"""Structural guards for the migration dry-run. No private state is loaded."""
import importlib.util
import unittest
from pathlib import Path


def load_probe():
    path = Path(__file__).resolve().parents[1] / 'docs' / 'acceptance' / 'migration_probe.py'
    spec = importlib.util.spec_from_file_location('acceptance_migration_probe', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class MigrationProbeGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.probe = load_probe()

    def test_allowed_removal_is_root_timings_or_known_health_leaves(self):
        allowed = self.probe.allowed_removal
        self.assertTrue(allowed('timings'))
        self.assertTrue(allowed('timings.samples'))
        for name in self.probe.VOLATILE:
            self.assertTrue(allowed(f'issues.47.runtime_health.coding.{name}'))
            self.assertTrue(allowed(f'issues.36.runtime_health.review.{name}'))
        self.assertFalse(allowed('issues.47.timings'))
        self.assertFalse(allowed('issues.47.observed_at'))
        self.assertFalse(allowed('observed_at'))
        self.assertFalse(allowed('issues.47.runtime_health.observed_at'))
        self.assertFalse(allowed('issues.47.runtime_health.coding.diagnostic'))
        self.assertFalse(allowed('issues.47.runtime_health.coding.extra.observed_at'))

    def test_output_paths_cannot_leave_staging_or_enter_production(self):
        guard = self.probe.assert_staging_target
        staging = Path('/home/siovush/DevOrchestrator-acceptance/docs/acceptance/evidence/summary.json')
        guard(staging, 'summary output')
        for path in (
            Path('/home/siovush/DevOrchestrator/task_bridge_state.json'),
            Path('/home/siovush/.local/share/devorchestrator/task_bridge_state.json'),
            Path('/home/siovush/.config/devorchestrator/network.env'),
            Path('/tmp/migration-summary.json'),
            Path('/home/siovush/DevOrchestrator-acceptance'),
        ):
            with self.assertRaises(SystemExit):
                guard(path, 'summary output')

import unittest
from unittest.mock import patch
import worker_runtime as w
class AnchorProof(unittest.TestCase):
 def test_live_wrapper_remains_anchor_when_members_receipt_empty(self):
  pid=1234567;identity=dict(boot='review-boot',start='100')
  receipt=dict(wrapper_pid=pid,wrapper_identity=identity,pgid=pid,sid=pid,members=[])
  wrapper=dict(pid=pid,identity=identity,pgid=pid,sid=pid)
  child=dict(pid=pid+1,identity=dict(boot='review-boot',start='101'),pgid=pid,sid=pid)
  with patch.object(w,'boot_id',return_value='review-boot'),patch.object(w,'process_details',return_value=wrapper),patch.object(w,'group_members',return_value=[wrapper,child]):
   self.assertEqual(w.group_ownership(receipt),'OWNED')
 def test_live_wrapper_identity_mismatch_rejects_even_recorded_child_anchor(self):
  pid=1234567;identity=dict(boot='review-boot',start='100')
  receipt=dict(wrapper_pid=pid,wrapper_identity=dict(identity,start='1'),pgid=pid,sid=pid,members=[dict(pid=pid+1,identity=dict(boot='review-boot',start='101'))])
  wrapper=dict(pid=pid,identity=identity,pgid=pid,sid=pid)
  child=dict(pid=pid+1,identity=dict(boot='review-boot',start='101'),pgid=pid,sid=pid)
  with patch.object(w,'boot_id',return_value='review-boot'),patch.object(w,'process_details',return_value=wrapper),patch.object(w,'group_members',return_value=[wrapper,child]):
   self.assertEqual(w.group_ownership(receipt),'AMBIGUOUS')
 def test_absent_wrapper_with_unrecorded_child_is_ambiguous(self):
  pid=1234567;identity=dict(boot='review-boot',start='100')
  receipt=dict(wrapper_pid=pid,wrapper_identity=identity,pgid=pid,sid=pid,members=[])
  child=dict(pid=pid+1,identity=dict(boot='review-boot',start='101'),pgid=pid,sid=pid)
  with patch.object(w,'boot_id',return_value='review-boot'),patch.object(w,'process_details',return_value=None),patch.object(w,'group_members',return_value=[child]):
   self.assertEqual(w.group_ownership(receipt),'AMBIGUOUS')
suite=unittest.TestSuite()
suite.addTests(unittest.defaultTestLoader.loadTestsFromName('test_operational_stability.OwnedCleanupTests'))
suite.addTests(unittest.defaultTestLoader.loadTestsFromName('test_worker_integration.WorkerIntegrationTests.test_git_baseline_fingerprint_detects_dirty_content_change'))
suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(AnchorProof))
result=unittest.TextTestRunner(verbosity=2).run(suite)
raise SystemExit(not result.wasSuccessful())

import unittest,sys
suite=unittest.TestSuite()
for pattern in ['test_chief*.py','test_acceptance_effect_latch.py','test_acceptance_report_latch.py','test_safety_latch_final.py']:
 suite.addTests(unittest.defaultTestLoader.discover('tests',pattern=pattern))
sys.path.insert(0,'/tmp/devorchestrator-independent-chief-a1q0hwc2')
import adversarial_review
suite.addTests(unittest.defaultTestLoader.loadTestsFromModule(adversarial_review))
result=unittest.TextTestRunner(verbosity=2).run(suite)
raise SystemExit(not result.wasSuccessful())

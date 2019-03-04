import os
from tempfile import mkstemp

from billiard.pool import Pool

from aodncore.pipeline import CheckResult, PipelineFile, PipelineFileCheckType, PipelineFileCollection
from aodncore.pipeline.exceptions import InvalidCheckTypeError, InvalidCheckSuiteError
from aodncore.pipeline.steps.check import (get_async_compliance_checker_broker, get_child_check_runner,
                                           run_compliance_checks, ComplianceCheckerCheckRunner, FormatCheckRunner,
                                           MultiprocessingAsyncComplianceCheckerBroker, NonEmptyCheckRunner)
from aodncore.testlib import BaseTestCase
from test_aodncore import TESTDATA_DIR

BAD_NC = os.path.join(TESTDATA_DIR, 'bad.nc')
EMPTY_NC = os.path.join(TESTDATA_DIR, 'empty.nc')
GOOD_NC = os.path.join(TESTDATA_DIR, 'good.nc')
WARNING_NC = os.path.join(TESTDATA_DIR, 'test_manifest.nc')


class TestPipelineStepsCheck(BaseTestCase):
    def test_get_check_runner(self):
        with self.assertRaises(ValueError):
            _ = get_child_check_runner(1, self.config, self.test_logger, None)
        with self.assertRaises(ValueError):
            _ = get_child_check_runner('str', self.config, self.test_logger, None)

        with self.assertRaises(InvalidCheckTypeError):
            _ = get_child_check_runner(PipelineFileCheckType.NO_ACTION, None, self.test_logger, None)

        cc_runner = get_child_check_runner(PipelineFileCheckType.NC_COMPLIANCE_CHECK, self.config, self.test_logger,
                                           {'checks': ['cf']})
        self.assertIsInstance(cc_runner, ComplianceCheckerCheckRunner)

        fc_runner = get_child_check_runner(PipelineFileCheckType.FORMAT_CHECK, self.config, self.test_logger, None)
        self.assertIsInstance(fc_runner, FormatCheckRunner)

        ne_runner = get_child_check_runner(PipelineFileCheckType.NONEMPTY_CHECK, self.config, self.test_logger, None)
        self.assertIsInstance(ne_runner, NonEmptyCheckRunner)

    def test_get_async_compliance_checker_broker(self):
        with self.assertRaises(ValueError):
            _ = get_async_compliance_checker_broker('invalid', self.config, self.test_logger)

        process_broker = get_async_compliance_checker_broker('pool', self.config, self.test_logger)
        self.assertIsInstance(process_broker, MultiprocessingAsyncComplianceCheckerBroker)
        self.assertIs(process_broker.pool_class, Pool)

        with self.assertRaises(NotImplementedError):
            _ = get_async_compliance_checker_broker('celery', self.config, self.test_logger)

    def test_run_compliance_checks_nochecks(self):
        with self.assertRaises(InvalidCheckSuiteError):
            _ = run_compliance_checks(GOOD_NC, [], 0)

    def test_run_compliance_checks_compliant(self):
        check_result = run_compliance_checks(GOOD_NC, ['cf'], 0)
        self.assertTrue(check_result.compliant)

    def test_run_compliance_checks_noncompliant(self):
        check_result = run_compliance_checks(BAD_NC, ['cf'], 0)
        self.assertFalse(check_result.compliant)

    def test_run_compliance_checks_invalid(self):
        _, temp_invalid_file = mkstemp(suffix='.nc', prefix=self.__class__.__name__, dir=self.temp_dir)
        check_result = run_compliance_checks(temp_invalid_file, ['cf'], 0)
        self.assertFalse(check_result.compliant)

    def test_run_compliance_checks_warning_skip(self):
        check_result = run_compliance_checks(WARNING_NC, ['cf'], 0, skip_checks=['check_convention_globals'])
        self.assertTrue(check_result.compliant)

    def test_run_compliance_checks_warning_noskip(self):
        check_result = run_compliance_checks(WARNING_NC, ['cf'], 0)
        self.assertFalse(check_result.compliant)


class TestComplianceCheckerRunner(BaseTestCase):
    def setUp(self):
        super(TestComplianceCheckerRunner, self).setUp()
        self.cc_runner = ComplianceCheckerCheckRunner(self.config, self.test_logger, {'checks': ['cf']})

    def test_compliant_file(self):
        collection = PipelineFileCollection([GOOD_NC])
        self.cc_runner.run(collection)

        check_result = collection[0].check_result

        self.assertIsInstance(check_result, CheckResult)
        self.assertTrue(check_result.compliant)
        self.assertFalse(check_result.errors)
        self.assertListEqual(check_result.log, [])

    def test_noncompliant_file(self):
        collection = PipelineFileCollection([BAD_NC])
        self.cc_runner.run(collection)

        check_result = collection[0].check_result

        self.assertIsInstance(check_result, CheckResult)
        self.assertFalse(check_result.compliant)
        self.assertFalse(check_result.errors)
        self.assertNotEqual(check_result.log, [])

    def test_invalid_file(self):
        _, temp_invalid_file = mkstemp(suffix='.txt', prefix=self.__class__.__name__, dir=self.temp_dir)

        collection = PipelineFileCollection([temp_invalid_file])
        self.cc_runner.run(collection)

        check_result = collection[0].check_result

        self.assertIsInstance(check_result, CheckResult)
        self.assertFalse(check_result.compliant)
        self.assertFalse(check_result.errors)
        self.assertNotEqual(check_result.log, [])

    def test_multiple_check_suite(self):
        collection = PipelineFileCollection([GOOD_NC])  # GOOD_NC complies with cf but NOT acdd:1.3
        self.cc_runner = ComplianceCheckerCheckRunner(self.config, self.test_logger, {'checks': ['cf', 'acdd:1.3']})
        self.cc_runner.run(collection)

        check_result = collection[0].check_result

        self.assertIsInstance(check_result, CheckResult)
        self.assertFalse(check_result.compliant)
        self.assertFalse(check_result.errors)
        self.assertNotEqual(check_result.log, [])

    def test_invalid_check_suite(self):
        with self.assertRaises(InvalidCheckSuiteError):
            _ = ComplianceCheckerCheckRunner(self.config, self.test_logger,
                                             {'checks': ['cf', 'no_such_thing']})

    def test_no_check_suite(self):
        with self.assertRaises(InvalidCheckSuiteError):
            _ = ComplianceCheckerCheckRunner(self.config, self.test_logger)

    def test_skip_checks(self):
        collection = PipelineFileCollection([WARNING_NC])
        self.cc_runner.run(collection)
        self.assertFalse(collection[0].check_result.compliant)  # WARNING_NC file fails with just one warning

        self.cc_runner = ComplianceCheckerCheckRunner(self.config,
                                                      self.test_logger,
                                                      {'checks': ['cf'], 'skip_checks': ['check_convention_globals']}
                                                      )
        self.cc_runner.run(collection)
        self.assertTrue(collection[0].check_result.compliant)  # now should pass


class TestFormatCheckRunner(BaseTestCase):
    def setUp(self):
        super(TestFormatCheckRunner, self).setUp()
        self.fc_runner = FormatCheckRunner(None, self.test_logger)

    def test_nc_file(self):
        nc_file = PipelineFile(GOOD_NC)
        nc_file.check_type = PipelineFileCheckType.FORMAT_CHECK
        collection = PipelineFileCollection(nc_file)
        self.fc_runner.run(collection)

        self.assertTrue(nc_file.is_checked)
        self.assertTrue(nc_file.check_passed)
        self.assertSequenceEqual([], nc_file.check_result.log)

    def test_nc_and_txt(self):
        _, temp_txt_file = mkstemp(suffix='.txt', prefix=self.__class__.__name__, dir=self.temp_dir)
        _, temp_invalid_nc_file = mkstemp(suffix='.nc', prefix=self.__class__.__name__, dir=self.temp_dir)

        txt = PipelineFile(temp_txt_file)
        txt.check_type = PipelineFileCheckType.FORMAT_CHECK
        nc = PipelineFile(temp_invalid_nc_file)
        nc.check_type = PipelineFileCheckType.FORMAT_CHECK

        collection = PipelineFileCollection([txt, nc])
        self.fc_runner.run(collection)

        self.assertFalse(txt.check_result.compliant)
        self.assertFalse(nc.check_result.compliant)


class TestNonEmptyCheckRunner(BaseTestCase):
    def setUp(self):
        super(TestNonEmptyCheckRunner, self).setUp()
        self.ne_runner = NonEmptyCheckRunner(None, self.test_logger)

    def test_nonempty_file(self):
        ne_file = PipelineFile(GOOD_NC)
        collection = PipelineFileCollection(ne_file)
        self.ne_runner.run(collection)

        check_result = ne_file.check_result

        self.assertTrue(check_result.compliant)
        self.assertFalse(check_result.errors)
        self.assertSequenceEqual(check_result.log, tuple())

    def test_empty_file(self):
        empty_file = PipelineFile(EMPTY_NC)
        collection = PipelineFileCollection(empty_file)
        self.ne_runner.run(collection)

        check_result = empty_file.check_result

        self.assertIsInstance(check_result, CheckResult)
        self.assertFalse(check_result.compliant)
        self.assertFalse(check_result.errors)
        self.assertNotEqual(check_result.log, [])

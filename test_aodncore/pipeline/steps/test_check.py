import os
from tempfile import mkstemp
from uuid import uuid4

from aodncore.pipeline import CheckResult, FileType, PipelineFileCheckType, PipelineFileCollection
from aodncore.pipeline.exceptions import InvalidCheckTypeError, InvalidCheckSuiteError
from aodncore.pipeline.steps.check import (get_child_check_runner, ComplianceCheckerCheckRunner, FormatCheckRunner,
                                           NetcdfFormatCheckRunner, NonEmptyCheckRunner)
from aodncore.testlib import BaseTestCase
from test_aodncore import TESTDATA_DIR

BAD_NC = os.path.join(TESTDATA_DIR, 'bad.nc')
EMPTY_NC = os.path.join(TESTDATA_DIR, 'empty.nc')
GOOD_NC = os.path.join(TESTDATA_DIR, 'good.nc')
WARNING_NC = os.path.join(TESTDATA_DIR, 'test_manifest.nc')


class TestPipelineStepsCheck(BaseTestCase):
    def test_get_check_runner(self):
        with self.assertRaises(ValueError):
            _ = get_child_check_runner(1, None, self.test_logger, None)
        with self.assertRaises(ValueError):
            _ = get_child_check_runner('str', None, self.test_logger, None)

        with self.assertRaises(InvalidCheckTypeError):
            _ = get_child_check_runner(PipelineFileCheckType.NO_ACTION, None, self.test_logger, None)

        cc_runner = get_child_check_runner(PipelineFileCheckType.NC_COMPLIANCE_CHECK, None, self.test_logger,
                                           {'checks': ['cf']})
        self.assertIsInstance(cc_runner, ComplianceCheckerCheckRunner)

        fc_runner = get_child_check_runner(PipelineFileCheckType.FORMAT_CHECK, None, self.test_logger, None)
        self.assertIsInstance(fc_runner, FormatCheckRunner)

        ne_runner = get_child_check_runner(PipelineFileCheckType.NONEMPTY_CHECK, None, self.test_logger, None)
        self.assertIsInstance(ne_runner, NonEmptyCheckRunner)


class TestComplianceCheckerRunner(BaseTestCase):
    def setUp(self):
        super(TestComplianceCheckerRunner, self).setUp()
        self.cc_runner = ComplianceCheckerCheckRunner(None, self.test_logger, {'checks': ['cf']})

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
        collection = PipelineFileCollection([GOOD_NC])  # GOOD_NC complies with cf but NOT imos:1.4
        self.cc_runner = ComplianceCheckerCheckRunner(None, self.test_logger, {'checks': ['cf', 'imos:1.4']})
        self.cc_runner.run(collection)

        check_result = collection[0].check_result

        self.assertIsInstance(check_result, CheckResult)
        self.assertFalse(check_result.compliant)
        self.assertFalse(check_result.errors)
        self.assertNotEqual(check_result.log, [])

    def test_invalid_check_suite(self):
        with self.assertRaises(InvalidCheckSuiteError):
            self.cc_runner = ComplianceCheckerCheckRunner(None, self.test_logger, {'checks': ['cf', 'no_such_thing']})

    def test_no_check_suite(self):
        with self.assertRaises(InvalidCheckSuiteError):
            self.cc_runner = ComplianceCheckerCheckRunner(None, self.test_logger)

    def test_skip_checks(self):
        collection = PipelineFileCollection([WARNING_NC])
        self.cc_runner.run(collection)
        self.assertFalse(collection[0].check_result.compliant)  # WARNING_NC file fails with just one warning

        self.cc_runner = ComplianceCheckerCheckRunner(None,
                                                      self.test_logger,
                                                      {'checks': ['cf'], 'skip_checks': ['check_convention_globals']}
                                                      )
        self.cc_runner.run(collection)
        self.assertTrue(collection[0].check_result.compliant)  # now should pass


class TestFormatCheckRunner(BaseTestCase):
    def setUp(self):
        super(TestFormatCheckRunner, self).setUp()
        self.fc_runner = FormatCheckRunner(None, self.test_logger)

    def test_get_format_check_runner(self):
        nc_runner = self.fc_runner.get_format_check_runner(FileType.NETCDF)
        self.assertIsInstance(nc_runner, NetcdfFormatCheckRunner)

        ne_runner = self.fc_runner.get_format_check_runner(str(uuid4()))
        self.assertIsInstance(ne_runner, NonEmptyCheckRunner)

    def test_nc_and_txt(self):
        _, temp_txt_file = mkstemp(suffix='.txt', prefix=self.__class__.__name__, dir=self.temp_dir)
        _, temp_invalid_nc_file = mkstemp(suffix='.nc', prefix=self.__class__.__name__, dir=self.temp_dir)
        collection = PipelineFileCollection([GOOD_NC, BAD_NC, temp_txt_file, temp_invalid_nc_file])
        self.fc_runner.run(collection)


class TestNetcdfFormatCheckRunner(BaseTestCase):
    pass


class TestNonEmptyCheckRunner(BaseTestCase):
    def setUp(self):
        super(TestNonEmptyCheckRunner, self).setUp()
        self.ne_runner = NonEmptyCheckRunner(None, self.test_logger)

    def test_nonempty_file(self):
        collection = PipelineFileCollection([GOOD_NC])
        self.ne_runner.run(collection)

        check_result = collection[0].check_result

        self.assertIsInstance(check_result, CheckResult)
        self.assertTrue(check_result.compliant)
        self.assertFalse(check_result.errors)
        self.assertEqual(check_result.log, [])

    def test_empty_file(self):
        collection = PipelineFileCollection([EMPTY_NC])
        self.ne_runner.run(collection)

        check_result = collection[0].check_result

        self.assertIsInstance(check_result, CheckResult)
        self.assertFalse(check_result.compliant)
        self.assertFalse(check_result.errors)
        self.assertNotEqual(check_result.log, [])

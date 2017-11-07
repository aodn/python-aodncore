import os
from tempfile import mkstemp
from uuid import uuid4

from aodncore.pipeline import CheckResult, PipelineFileCheckType, PipelineFileCollection
from aodncore.pipeline.exceptions import InvalidCheckTypeError, InvalidCheckSuiteError
from aodncore.pipeline.steps.check import (get_child_check_runner, ComplianceCheckerCheckRunner, FormatCheckRunner,
                                           NetcdfFormatCheckRunner, PermissiveCheckRunner)

from test_aodncore.testlib import BaseTestCase, MOCK_LOGGER

TESTDATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'testdata')
BAD_NC = os.path.join(TESTDATA_DIR, 'bad.nc')
GOOD_NC = os.path.join(TESTDATA_DIR, 'good.nc')


class TestPipelineStepsCheck(BaseTestCase):
    def test_get_check_runner(self):
        with self.assertRaises(ValueError):
            _ = get_child_check_runner(1, None, None, MOCK_LOGGER)
        with self.assertRaises(ValueError):
            _ = get_child_check_runner('str', None, None, MOCK_LOGGER)

        with self.assertRaises(InvalidCheckTypeError):
            _ = get_child_check_runner(PipelineFileCheckType.NO_ACTION, None, None, MOCK_LOGGER)

        cc_runner = get_child_check_runner(PipelineFileCheckType.NC_COMPLIANCE_CHECK, None, None, MOCK_LOGGER)
        self.assertIsInstance(cc_runner, ComplianceCheckerCheckRunner)

        fc_runner = get_child_check_runner(PipelineFileCheckType.FORMAT_CHECK, None, None, MOCK_LOGGER)
        self.assertIsInstance(fc_runner, FormatCheckRunner)


class TestComplianceCheckerRunner(BaseTestCase):
    def setUp(self):
        self.cc_runner = ComplianceCheckerCheckRunner(None, MOCK_LOGGER, {'checks': ['cf']})

    def test_compliant_file(self):
        collection = PipelineFileCollection([GOOD_NC])
        self.cc_runner.run(collection)

        check_result = collection[0].check_result

        self.assertIsInstance(check_result, CheckResult)
        self.assertTrue(check_result.compliant)
        self.assertListEqual(check_result.log, [])

    def test_noncompliant_file(self):
        collection = PipelineFileCollection([BAD_NC])
        self.cc_runner.run(collection)

        check_result = collection[0].check_result

        self.assertIsInstance(check_result, CheckResult)
        self.assertFalse(check_result.compliant)
        self.assertNotEqual(check_result.log, [])

    def test_invalid_file(self):
        _, temp_invalid_file = mkstemp(suffix='.txt', prefix=self.__class__.__name__, dir=self.temp_dir)

        collection = PipelineFileCollection([temp_invalid_file])
        self.cc_runner.run(collection)

        check_result = collection[0].check_result

        self.assertIsInstance(check_result, CheckResult)
        self.assertFalse(check_result.compliant)
        self.assertNotEqual(check_result.log, [])

    def test_multiple_check_suite(self):
        collection = PipelineFileCollection([GOOD_NC])  # GOOD_NC complies with cf but NOT imos:1.4
        self.cc_runner = ComplianceCheckerCheckRunner(None, MOCK_LOGGER, {'checks': ['cf', 'imos:1.4']})
        self.cc_runner.run(collection)

        check_result = collection[0].check_result

        self.assertIsInstance(check_result, CheckResult)
        self.assertFalse(check_result.compliant)
        self.assertNotEqual(check_result.log, [])

    def test_invalid_check_suite(self):
        with self.assertRaises(InvalidCheckSuiteError):
            self.cc_runner = ComplianceCheckerCheckRunner(None, MOCK_LOGGER, {'checks': ['cf', 'no_such_thing']})

    def test_no_check_suite(self):
        with self.assertRaises(InvalidCheckSuiteError):
            self.cc_runner = ComplianceCheckerCheckRunner(None, MOCK_LOGGER)


class TestFormatCheckRunner(BaseTestCase):
    def setUp(self):
        self.fc_runner = FormatCheckRunner(None, MOCK_LOGGER)

    def test_get_format_check_runner(self):
        nc_runner = self.fc_runner.get_format_check_runner('.nc')
        self.assertIsInstance(nc_runner, NetcdfFormatCheckRunner)

        permissive_runner = self.fc_runner.get_format_check_runner(str(uuid4()))
        self.assertIsInstance(permissive_runner, PermissiveCheckRunner)

    def test_nc_and_txt(self):
        _, temp_txt_file = mkstemp(suffix='.txt', prefix=self.__class__.__name__, dir=self.temp_dir)
        _, temp_invalid_nc_file = mkstemp(suffix='.nc', prefix=self.__class__.__name__, dir=self.temp_dir)
        collection = PipelineFileCollection([GOOD_NC, BAD_NC, temp_txt_file, temp_invalid_nc_file])
        self.fc_runner.run(collection)


class TestNetcdfFormatCheckRunner(BaseTestCase):
    pass


class TestPermissiveCheckRunner(BaseTestCase):
    pass

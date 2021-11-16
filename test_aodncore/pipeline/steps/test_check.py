import os
from tempfile import mkstemp

from aodncore.pipeline import CheckResult, PipelineFile, PipelineFileCheckType, PipelineFileCollection
from aodncore.pipeline.exceptions import InvalidCheckTypeError, InvalidCheckSuiteError
from aodncore.pipeline.steps.check import (get_child_check_runner, ComplianceCheckerCheckRunner, FormatCheckRunner,
                                           NonEmptyCheckRunner, TableSchemaCheckRunner)
from aodncore.testlib import BaseTestCase
from test_aodncore import TESTDATA_DIR

BAD_NC = os.path.join(TESTDATA_DIR, 'bad.nc')
EMPTY_NC = os.path.join(TESTDATA_DIR, 'empty.nc')
GOOD_NC = os.path.join(TESTDATA_DIR, 'good.nc')
WARNING_NC = os.path.join(TESTDATA_DIR, 'test_manifest.nc')

GOOD_CSV = os.path.join(TESTDATA_DIR, 'test_frictionless.csv')
BAD_CSV = os.path.join(TESTDATA_DIR, 'invalid.schemadata.csv')
UNMATCHED_CSV = os.path.join(TESTDATA_DIR, 'test_frictionless_no_resource.csv')


class TestPipelineStepsCheck(BaseTestCase):
    def test_get_check_runner(self):
        with self.assertRaises(TypeError):
            _ = get_child_check_runner(1, None, self.test_logger, None)
        with self.assertRaises(TypeError):
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

        ts_runner = get_child_check_runner(PipelineFileCheckType.TABLE_SCHEMA_CHECK, dummy_config(), self.test_logger,
                                           None)
        self.assertIsInstance(ts_runner, TableSchemaCheckRunner)


class TestComplianceCheckerRunner(BaseTestCase):
    def setUp(self):
        super().setUp()
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
        collection = PipelineFileCollection([GOOD_NC])  # GOOD_NC complies with cf but NOT acdd:1.3
        self.cc_runner = ComplianceCheckerCheckRunner(None, self.test_logger, {'checks': ['cf', 'acdd:1.3']})
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
        super().setUp()
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
        super().setUp()
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


class dummy_config(object):
    def __init__(self):
        self.pipeline_config = {
                'harvester': {
                    "config_dir": TESTDATA_DIR,
                    "schema_base_dir": TESTDATA_DIR
                }
            }


class TestTableSchemaCheckRunner(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.ts_runner = TableSchemaCheckRunner(dummy_config(), self.test_logger)

    def test_valid_file(self):
        ts_file = PipelineFile(GOOD_CSV)
        collection = PipelineFileCollection(ts_file)
        self.ts_runner.run(collection)

        check_result = ts_file.check_result

        self.assertTrue(check_result.compliant)
        self.assertFalse(check_result.errors)
        self.assertSequenceEqual(check_result.log, [])

    def test_invalid_file(self):
        ts_file = PipelineFile(BAD_CSV)
        collection = PipelineFileCollection(ts_file)
        self.ts_runner.run(collection)

        check_result = ts_file.check_result

        self.assertFalse(check_result.compliant)
        self.assertFalse(check_result.errors)
        self.assertNotEqual(check_result.log, [])

    def test_missing_schema_file(self):
        ts_file = PipelineFile(UNMATCHED_CSV)
        collection = PipelineFileCollection(ts_file)
        self.ts_runner.run(collection)

        check_result = ts_file.check_result

        self.assertFalse(check_result.compliant)
        self.assertFalse(check_result.errors)
        self.assertNotEqual(check_result.log, [])

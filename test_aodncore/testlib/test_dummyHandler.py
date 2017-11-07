import os
import sys
import unittest

from aodncore.pipeline import PipelineFilePublishType, HandlerResult
from aodncore.pipeline.exceptions import (ComplianceCheckFailedError, HandlerAlreadyRunError, InvalidCheckSuiteError,
                                          InvalidInputFileError, InvalidFileFormatError)
from aodncore.pipeline.steps import NotifyList
from test_aodncore.testlib import DummyHandler, HandlerTestCase, dest_path_testing, get_nonexistent_path, mock

TESTDATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'testdata')
BAD_NC = os.path.join(TESTDATA_DIR, 'bad.nc')
BAD_ZIP = os.path.join(TESTDATA_DIR, 'bad.zip')
GOOD_NC = os.path.join(TESTDATA_DIR, 'good.nc')
GOOD_ZIP = os.path.join(TESTDATA_DIR, 'good.zip')
INVALID_FILE = os.path.join(TESTDATA_DIR, 'invalid.png')
NOT_NETCDF_NC_FILE = os.path.join(TESTDATA_DIR, 'not_a_netcdf_file.nc')

MAP_MANIFEST = os.path.join(TESTDATA_DIR, 'test.map_manifest')
RSYNC_MANIFEST = os.path.join(TESTDATA_DIR, 'test.rsync_manifest')
SIMPLE_MANIFEST = os.path.join(TESTDATA_DIR, 'test.manifest')


class TestDummyHandler(HandlerTestCase):
    def setUp(self):
        self.handler_class = DummyHandler
        super(TestDummyHandler, self).setUp()

    def test_dest_path_from_handler(self):
        handler = self.run_handler(self.temp_nc_file)
        self.assertIs(handler._dest_path_function_ref, handler.dest_path)

    def test_dest_path_from_reference(self):
        handler = self.run_handler(self.temp_nc_file, dest_path_function=dest_path_testing)
        self.assertIs(handler._dest_path_function_ref, dest_path_testing)

    def test_dest_path_from_string(self):
        handler = self.run_handler(self.temp_nc_file, dest_path_function='dest_path_testing')
        self.assertIs(handler._dest_path_function_ref, dest_path_testing)

    def test_include(self):
        handler = self.run_handler(BAD_ZIP, include_regexes=['good\.nc'])
        eligible_filenames = [f.name for f in handler.file_collection if f.should_harvest]
        self.assertIn('good.nc', eligible_filenames)
        self.assertNotIn('bad.nc', eligible_filenames)

    def test_exclude(self):
        handler = self.run_handler(BAD_ZIP, include_regexes=['.*\.nc'], exclude_regexes=['bad\.nc'])
        eligible_filenames = [f.name for f in handler.file_collection if f.should_harvest]
        self.assertIn('good.nc', eligible_filenames)
        self.assertNotIn('bad.nc', eligible_filenames)

    def test_invalid_include_regex(self):
        self.run_handler_with_exception(ValueError, GOOD_NC, include_regexes=['['])

    def test_invalid_exclude_regex(self):
        self.run_handler_with_exception(ValueError, GOOD_NC, include_regexes=['.*'], exclude_regexes=['['])

    def test_nonexistent_file(self):
        nonexistent_file = get_nonexistent_path()
        self.run_handler_with_exception(InvalidInputFileError, nonexistent_file, dest_path_function=dest_path_testing)

    def test_run_handler_twice(self):
        handler = self.run_handler(self.temp_nc_file)
        with self.assertRaises(HandlerAlreadyRunError):
            handler.run()

    def test_handle_keyboard_interrupt(self):
        def raise_keyboardinterrupt():
            raise KeyboardInterrupt

        handler = self.handler_class(self.temp_nc_file)
        handler.preprocess = raise_keyboardinterrupt
        handler.run()
        self.assertIs(HandlerResult.ERROR, handler.result)

    def test_handle_sys_exit(self):
        handler = self.handler_class(self.temp_nc_file)
        handler.preprocess = sys.exit
        handler.run()
        self.assertIs(HandlerResult.ERROR, handler.result)

    def test_allowed_extensions(self):
        handler = self.run_handler_with_exception(InvalidFileFormatError, self.temp_nc_file,
                                                  dest_path_function=dest_path_testing,
                                                  allowed_extensions=('.pdf', '.zip'))
        self.assertRegexpMatches(handler._error_details, "input file extension '.nc' not in allowed_extensions list:.*")

    def test_archive_collection(self):
        handler = self.run_handler(self.temp_nc_file, archive_path_function=dest_path_testing, archive_input_file=True)
        self.assertTrue(handler.file_collection[0].is_archived)

    def test_archive_input_file(self):
        handler = self.run_handler(self.temp_nc_file, archive_path_function=dest_path_testing, archive_input_file=True)
        self.assertTrue(handler.is_archived)
        self.assertTrue(handler.file_collection[0].is_archived)

    def test_invalid_check_suite(self):
        self.run_handler_with_exception(InvalidCheckSuiteError, NOT_NETCDF_NC_FILE,
                                        check_params={'checks': ['invalid_check_suite_should_fail']},
                                        dest_path_function=dest_path_testing)

    def test_not_netcdf_nc(self):
        self.run_handler_with_exception(ComplianceCheckFailedError, NOT_NETCDF_NC_FILE,
                                        check_params={'checks': ['cf']}, dest_path_function=dest_path_testing)

    @mock.patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
    def test_notify_error(self, mock_smtp):
        mock_smtp.return_value.sendmail.return_value = {}

        handler = self.run_handler_with_exception(ComplianceCheckFailedError, NOT_NETCDF_NC_FILE,
                                                  notify_params={'error_notify_list': ['email:nobody1@example.com',
                                                                                       'email:nobody2@example.com']},
                                                  dest_path_function=dest_path_testing)
        self.assertIsInstance(handler.notify_list, NotifyList)
        self.assertEqual(len(handler.notify_list), 2)
        self.assertTrue(handler.notify_list[0].notification_succeeded)
        self.assertIsNone(handler.notify_list[0].error)

    @mock.patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
    def test_notify_fail(self, mock_smtp):
        mock_smtp.return_value.sendmail.return_value = {}

        handler = self.run_handler(self.temp_nc_file,
                                   notify_params={'success_notify_list': ['INVALID:nobody1@example.com',
                                                                          'email:nobody2@example.com']},
                                   dest_path_function=dest_path_testing)
        self.assertIsInstance(handler.notify_list, NotifyList)
        self.assertEqual(len(handler.notify_list), 2)
        self.assertFalse(handler.notify_list[0].notification_succeeded)
        self.assertTrue(handler.notify_list[1].notification_succeeded)
        self.assertIsNone(handler.notify_list[0].error)
        self.assertIsNone(handler.notify_list[1].error)

    @mock.patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
    def test_notify_success(self, mock_smtp):
        mock_smtp.return_value.sendmail.return_value = {}

        handler = self.run_handler(self.temp_nc_file,
                                   notify_params={'success_notify_list': ['email:nobody1@example.com',
                                                                          'email:nobody2@example.com',
                                                                          'email:nobody3@example.com',
                                                                          'email:nobody4@example.com']},
                                   dest_path_function=dest_path_testing)
        self.assertIsInstance(handler.notify_list, NotifyList)
        self.assertEqual(len(handler.notify_list), 4)
        self.assertTrue(all(r.notification_succeeded for r in handler.notify_list))
        self.assertIsNone(handler.notify_list[0].error)

    def test_property_default_addition_publish_type(self):
        handler = self.handler_class(self.temp_nc_file)
        handler.default_addition_publish_type = PipelineFilePublishType.NO_ACTION
        self.assertIs(handler.default_addition_publish_type, PipelineFilePublishType.NO_ACTION)

        with self.assertRaises(ValueError):
            handler.default_addition_publish_type = 'invalid'

    def test_property_default_deletion_publish_type(self):
        handler = self.handler_class(self.temp_nc_file)

        handler.default_deletion_publish_type = PipelineFilePublishType.NO_ACTION
        self.assertIs(handler.default_deletion_publish_type, PipelineFilePublishType.NO_ACTION)

        with self.assertRaises(ValueError):
            handler.default_deletion_publish_type = 'invalid'


if __name__ == '__main__':
    unittest.main()

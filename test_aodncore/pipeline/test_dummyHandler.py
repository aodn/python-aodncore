import os
import sys
import unittest

from jsonschema import ValidationError

from aodncore.pipeline import PipelineFilePublishType, HandlerResult
from aodncore.pipeline.exceptions import (ComplianceCheckFailedError, HandlerAlreadyRunError, InvalidCheckSuiteError,
                                          InvalidInputFileError, InvalidFileFormatError, InvalidRecipientError)
from aodncore.pipeline.statequery import StateQuery
from aodncore.pipeline.steps import NotifyList
from aodncore.testlib import DummyHandler, HandlerTestCase, dest_path_testing, get_nonexistent_path, mock
from aodncore.util import WriteOnceOrderedDict
from test_aodncore import TESTDATA_DIR

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

    def test_params_freeze(self):
        handler = self.run_handler(GOOD_NC,
                                   check_params={},
                                   custom_params={},
                                   harvest_params={},
                                   notify_params={},
                                   resolve_params={})

        self.assertIsInstance(handler.check_params, WriteOnceOrderedDict)
        self.assertIsInstance(handler.custom_params, WriteOnceOrderedDict)
        self.assertIsInstance(handler.harvest_params, WriteOnceOrderedDict)
        self.assertIsInstance(handler.notify_params, WriteOnceOrderedDict)
        self.assertIsInstance(handler.resolve_params, WriteOnceOrderedDict)

    def test_custom_params(self):
        custom_params = {
            'my_bool_param': False,
            'my_dict_param': {'key': 'value'},
            'my_int_param': 1,
            'my_list_param': [1],
            'my_string_param': 'str'

        }

        handler = self.run_handler(GOOD_NC, custom_params=custom_params)
        self.assertDictEqual(handler.custom_params, custom_params)

    def test_invalid_handler_params(self):
        with self.assertRaises(TypeError):
            _ = self.handler_class(GOOD_NC, invalid_unknown_keyword_argument=1)

    def test_invalid_include_regex(self):
        self.run_handler_with_exception(ValueError, GOOD_NC, include_regexes=['['])

    def test_invalid_exclude_regex(self):
        self.run_handler_with_exception(ValueError, GOOD_NC, include_regexes=['.*'], exclude_regexes=['['])

    def test_invalid_check_params(self):
        self.run_handler_with_exception(ValidationError, GOOD_NC, check_params={'invalid_param': 'value'})
        self.run_handler_with_exception(ValidationError, GOOD_NC, check_params={'checks': 'invalid_type'})

    def test_invalid_custom_params(self):
        self.run_handler_with_exception(ValidationError, GOOD_NC, custom_params='invalid_type')

    def test_invalid_harvest_params(self):
        self.run_handler_with_exception(ValidationError, GOOD_NC, harvest_params={'slice_size': 'twenty'})
        self.run_handler_with_exception(ValidationError, GOOD_NC, harvest_params={'invalid_param': 'value'})

    def test_invalid_notify_params(self):
        self.run_handler_with_exception(ValidationError, GOOD_NC, notify_params={'notify_owner_error': ['value']})
        self.run_handler_with_exception(ValidationError, GOOD_NC, notify_params={'invalid_param': 'value'})

    def test_invalid_resolve_params(self):
        self.run_handler_with_exception(ValidationError, GOOD_NC, resolve_params={'relative_path_root': 0})
        self.run_handler_with_exception(ValidationError, GOOD_NC, resolve_params={'invalid_param': 'value'})

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
                                                  allowed_extensions=['.pdf', '.zip'])
        self.assertRegexpMatches(handler._error_details, "input file extension '.nc' not in allowed_extensions list:.*")

        self.run_handler(self.temp_nc_file, dest_path_function=dest_path_testing, allowed_extensions=['.nc'])

    def test_allowed_regexes(self):
        handler = self.run_handler_with_exception(InvalidInputFileError, self.temp_nc_file,
                                                  dest_path_function=dest_path_testing,
                                                  allowed_regexes=['.*\.zip'])
        self.assertRegexpMatches(handler._error_details,
                                 "input file '.*' does not match any patterns in the allowed_regexes list:.*")

        self.run_handler(self.temp_nc_file, dest_path_function=dest_path_testing, allowed_regexes=['.*\.nc'])

    def test_allowed_extensions_and_allowed_regexes(self):
        self.run_handler_with_exception(InvalidInputFileError, GOOD_NC, dest_path_function=dest_path_testing,
                                        allowed_extensions=['.nc'], allowed_regexes=['bad\.nc'])

        self.run_handler(GOOD_NC, dest_path_function=dest_path_testing, allowed_extensions=['.nc'],
                         allowed_regexes=['good\.nc'])

    def test_archive_collection(self):
        handler = self.run_handler(self.temp_nc_file, archive_path_function=dest_path_testing, archive_input_file=True)
        self.assertTrue(handler.file_collection[0].is_archived)

    def test_archive_input_file(self):
        handler = self.run_handler(GOOD_ZIP, archive_path_function=dest_path_testing, archive_input_file=True)
        self.assertTrue(handler.input_file_object.is_archived)

    def test_archive_input_file_custom_path(self):
        expected_path = 'custom/relative/path'

        handler = self.handler_class(GOOD_ZIP, archive_path_function=dest_path_testing, archive_input_file=True)
        handler.input_file_archive_path = expected_path
        handler.run()

        self.assertTrue(handler.input_file_object.is_archived)
        self.assertEqual(handler.input_file_object.archive_path, expected_path)

    def test_input_file_archive_path(self):
        handler = self.handler_class(self.temp_nc_file)
        with self.assertRaises(ValueError):
            handler.input_file_archive_path = '/absolute/path/MYFACILITY/path/to/file.txt'

        try:
            handler.input_file_archive_path = 'relative/path/MYFACILITY/path/to/file.txt'
        except Exception as e:
            raise AssertionError(
                "unexpected exception raised. {cls} {msg}".format(cls=e.__class__.__name__, msg=e))

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
                                                  notify_params={'notify_owner_error': False,
                                                                 'owner_notify_list': ['email:owner1@example.com'],
                                                                 'success_notify_list': ['email:nobody1@example.com',
                                                                                         'email:nobody2@example.com'],
                                                                 'error_notify_list': ['email:nobody3@example.com',
                                                                                       'email:nobody4@example.com']},
                                                  dest_path_function=dest_path_testing)

        expected_recipients = ['email:nobody3@example.com', 'email:nobody4@example.com']

        self.assertIsInstance(handler.notification_results, NotifyList)
        self.assertItemsEqual(expected_recipients, [n.raw_string for n in handler.notification_results])
        self.assertTrue(all(r.notification_succeeded for r in handler.notification_results))
        self.assertTrue(all(r.error is None for r in handler.notification_results))

    @mock.patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
    def test_notify_error_unicode(self, mock_smtp):
        mock_smtp.return_value.sendmail.return_value = {}

        handler = self.run_handler_with_exception(ComplianceCheckFailedError, BAD_NC,
                                                  notify_params={'notify_owner_error': False,
                                                                 'owner_notify_list': ['email:owner1@example.com'],
                                                                 'success_notify_list': ['email:nobody1@example.com',
                                                                                         'email:nobody2@example.com'],
                                                                 'error_notify_list': ['email:nobody3@example.com',
                                                                                       'email:nobody4@example.com']},
                                                  dest_path_function=dest_path_testing,
                                                  check_params={'checks': ['cf']})

        expected_recipients = ['email:nobody3@example.com', 'email:nobody4@example.com']

        self.assertIsInstance(handler.notification_results, NotifyList)
        self.assertItemsEqual(expected_recipients, [n.raw_string for n in handler.notification_results])
        self.assertTrue(all(r.notification_succeeded for r in handler.notification_results))
        self.assertTrue(all(r.error is None for r in handler.notification_results))

    @mock.patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
    def test_notify_owner_error(self, mock_smtp):
        mock_smtp.return_value.sendmail.return_value = {}

        handler = self.run_handler_with_exception(ComplianceCheckFailedError, NOT_NETCDF_NC_FILE,
                                                  notify_params={'notify_owner_error': True,
                                                                 'owner_notify_list': ['email:owner1@example.com'],
                                                                 'success_notify_list': ['email:nobody1@example.com',
                                                                                         'email:nobody2@example.com'],
                                                                 'error_notify_list': ['email:nobody3@example.com',
                                                                                       'email:nobody4@example.com']},
                                                  dest_path_function=dest_path_testing)

        expected_recipients = ['email:owner1@example.com', 'email:nobody3@example.com', 'email:nobody4@example.com']

        self.assertIsInstance(handler.notification_results, NotifyList)
        self.assertItemsEqual(expected_recipients, [n.raw_string for n in handler.notification_results])
        self.assertTrue(all(r.notification_succeeded for r in handler.notification_results))
        self.assertTrue(all(r.error is None for r in handler.notification_results))

    @mock.patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
    def test_notify_system_error(self, mock_smtp):
        mock_smtp.return_value.sendmail.return_value = {}

        handler = self.run_handler_with_exception(InvalidInputFileError, get_nonexistent_path(),
                                                  notify_params={'notify_owner_error': False,
                                                                 'owner_notify_list': ['email:owner1@example.com'],
                                                                 'success_notify_list': ['email:nobody1@example.com',
                                                                                         'email:nobody2@example.com'],
                                                                 'error_notify_list': ['email:nobody3@example.com',
                                                                                       'email:nobody4@example.com']
                                                                 },
                                                  dest_path_function=dest_path_testing)

        # a PipelineSystemError should *always* be sent to owner, regardless of 'notify_owner_error' flag
        expected_recipients = ['email:owner1@example.com']

        self.assertIsInstance(handler.notification_results, NotifyList)
        self.assertItemsEqual(expected_recipients, [n.raw_string for n in handler.notification_results])
        self.assertTrue(all(r.notification_succeeded for r in handler.notification_results))
        self.assertTrue(all(r.error is None for r in handler.notification_results))

    @mock.patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
    def test_notify_fail(self, mock_smtp):
        mock_smtp.return_value.sendmail.return_value = {}

        handler = self.run_handler(self.temp_nc_file,
                                   notify_params={'notify_owner_error': False,
                                                  'owner_notify_list': ['email:owner1@example.com'],
                                                  'success_notify_list': ['email:nobody1@example.com',
                                                                          'INVALID:nobody2@example.com'],
                                                  'error_notify_list': ['email:nobody3@example.com',
                                                                        'email:nobody4@example.com']
                                                  },
                                   dest_path_function=dest_path_testing)

        expected_recipients = ['email:nobody1@example.com', 'INVALID:nobody2@example.com']

        self.assertIsInstance(handler.notification_results, NotifyList)
        self.assertItemsEqual(expected_recipients, [n.raw_string for n in handler.notification_results])
        self.assertTrue(handler.notification_results[0].notification_succeeded)
        self.assertFalse(handler.notification_results[1].notification_succeeded)
        self.assertIsNone(handler.notification_results[0].error)
        self.assertIsInstance(handler.notification_results[1].error, InvalidRecipientError)

    @mock.patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
    def test_notify_success(self, mock_smtp):
        mock_smtp.return_value.sendmail.return_value = {}

        handler = self.run_handler(self.temp_nc_file,
                                   notify_params={'owner_notify_list': ['email:owner1@example.com'],
                                                  'success_notify_list': ['email:nobody1@example.com',
                                                                          'email:nobody2@example.com'],
                                                  'error_notify_list': ['email:nobody3@example.com',
                                                                        'email:nobody4@example.com']},
                                   dest_path_function=dest_path_testing)

        expected_recipients = ['email:nobody1@example.com', 'email:nobody2@example.com']

        self.assertIsInstance(handler.notification_results, NotifyList)
        self.assertItemsEqual(expected_recipients, [n.raw_string for n in handler.notification_results])
        self.assertTrue(all(r.notification_succeeded for r in handler.notification_results))
        self.assertTrue(all(r.error is None for r in handler.notification_results))

    @mock.patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
    def test_notify_owner_success(self, mock_smtp):
        mock_smtp.return_value.sendmail.return_value = {}

        handler = self.run_handler(self.temp_nc_file,
                                   notify_params={'notify_owner_success': True,
                                                  'owner_notify_list': ['email:owner1@example.com'],
                                                  'success_notify_list': ['email:nobody1@example.com',
                                                                          'email:nobody2@example.com'],
                                                  'error_notify_list': ['email:nobody3@example.com',
                                                                        'email:nobody4@example.com']
                                                  },
                                   dest_path_function=dest_path_testing)

        expected_recipients = ['email:owner1@example.com', 'email:nobody1@example.com', 'email:nobody2@example.com']

        self.assertIsInstance(handler.notification_results, NotifyList)
        self.assertItemsEqual(expected_recipients, [n.raw_string for n in handler.notification_results])
        self.assertTrue(all(r.notification_succeeded for r in handler.notification_results))
        self.assertTrue(all(r.error is None for r in handler.notification_results))

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

    def test_state_query(self):
        handler = self.run_handler(self.temp_nc_file)
        self.assertIsInstance(handler.state_query, StateQuery)


if __name__ == '__main__':
    unittest.main()

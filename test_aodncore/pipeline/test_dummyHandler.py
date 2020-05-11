import os
import sys
from functools import partial
from unittest.mock import patch

from jsonschema import ValidationError

from aodncore.pipeline import PipelineFile, PipelineFileCheckType, PipelineFilePublishType, HandlerResult
from aodncore.pipeline.exceptions import (AttributeValidationError, ComplianceCheckFailedError, HandlerAlreadyRunError,
                                          InvalidCheckSuiteError, InvalidInputFileError, InvalidFileFormatError,
                                          InvalidRecipientError, UnmatchedFilesError)
from aodncore.pipeline.statequery import StateQuery
from aodncore.pipeline.steps import NotifyList
from aodncore.testlib import DummyHandler, HandlerTestCase, dest_path_testing, get_nonexistent_path
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
DELETE_MANIFEST = os.path.join(TESTDATA_DIR, 'test.delete_manifest')


class TestDummyHandler(HandlerTestCase):
    def setUp(self):
        self.handler_class = DummyHandler
        super().setUp()

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
        handler = self.run_handler_with_exception(UnmatchedFilesError, BAD_ZIP, include_regexes=[r'good\.nc'])
        eligible_filenames = handler.file_collection \
                                    .filter_by_attribute_id_not('publish_type', PipelineFilePublishType.UNSET) \
                                    .get_attribute_list('name')
        self.assertListEqual(['good.nc'], eligible_filenames)

    def test_exclude(self):
        handler = self.run_handler_with_exception(UnmatchedFilesError, BAD_ZIP, include_regexes=[r'.*\.nc'],
                                                  exclude_regexes=[r'bad.nc'])
        eligible_filenames = handler.file_collection \
                                    .filter_by_attribute_id_not('publish_type', PipelineFilePublishType.UNSET) \
                                    .get_attribute_list('name')

        self.assertListEqual(['good.nc'], eligible_filenames)

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
        with self.assertRaises(ValueError):
            _ = self.handler_class(GOOD_NC, include_regexes=[r'['])

    def test_invalid_exclude_regex(self):
        with self.assertRaises(ValueError):
            _ = self.handler_class(GOOD_NC, include_regexes=[r'.*'], exclude_regexes=[r'['])

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

    def test_allow_delete_manifests(self):
        self.run_handler_with_exception(InvalidFileFormatError, DELETE_MANIFEST)
        self.run_handler_with_exception(InvalidFileFormatError, DELETE_MANIFEST,
                                        resolve_params={'allow_delete_manifests': False})
        self.run_handler(DELETE_MANIFEST, resolve_params={'allow_delete_manifests': True})

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
        self.assertRegex(handler._error_details, r"input file extension '.nc' not in allowed_extensions list:.*")

        self.run_handler(self.temp_nc_file, dest_path_function=dest_path_testing, allowed_extensions=['.nc'])

    def test_allowed_regexes(self):
        handler = self.run_handler_with_exception(InvalidInputFileError, self.temp_nc_file,
                                                  dest_path_function=dest_path_testing,
                                                  allowed_regexes=[r'.*\.zip'])
        self.assertRegex(handler._error_details,
                         r"input file '.*' does not match any patterns in the allowed_regexes list:.*")

        self.run_handler(self.temp_nc_file, dest_path_function=dest_path_testing, allowed_regexes=[r'.*.nc'])

    def test_allowed_dest_path_regexes(self):
        self.run_handler_with_exception(AttributeValidationError, self.temp_nc_file,
                                        dest_path_function=dest_path_testing,
                                        allowed_dest_path_regexes=[r'DEFINITELY/NOT/A/MATCH'])

        self.run_handler(self.temp_nc_file, dest_path_function=dest_path_testing,
                         allowed_dest_path_regexes=[r'DUMMY.*'])

    def test_allowed_extensions_and_allowed_regexes(self):
        self.run_handler_with_exception(InvalidInputFileError, GOOD_NC, dest_path_function=dest_path_testing,
                                        allowed_extensions=[r'.nc'], allowed_regexes=[r'bad.nc'])

        self.run_handler(GOOD_NC, dest_path_function=dest_path_testing, allowed_extensions=['.nc'],
                         allowed_regexes=[r'good\.nc'])

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

    def test_archive_input_file_manual_publish_type(self):
        handler = self.handler_class(GOOD_ZIP, archive_path_function=dest_path_testing, archive_input_file=True)
        handler.input_file_object.publish_type = PipelineFilePublishType.HARVEST_ARCHIVE
        handler.run()

        self.assertIs(handler.input_file_object.publish_type, PipelineFilePublishType.HARVEST_ARCHIVE)

    def test_input_file_archive_path(self):
        handler = self.handler_class(self.temp_nc_file)
        with self.assertRaises(ValueError):
            handler.input_file_archive_path = '/absolute/path/MYFACILITY/path/to/file.txt'

        with self.assertNoException():
            handler.input_file_archive_path = 'relative/path/MYFACILITY/path/to/file.txt'

    def test_invalid_check_suite(self):
        self.run_handler_with_exception(InvalidCheckSuiteError, NOT_NETCDF_NC_FILE,
                                        check_params={'checks': ['invalid_check_suite_should_fail']},
                                        dest_path_function=dest_path_testing)

    def test_not_netcdf_nc(self):
        self.run_handler_with_exception(ComplianceCheckFailedError, NOT_NETCDF_NC_FILE,
                                        check_params={'checks': ['cf']}, dest_path_function=dest_path_testing)

    def test_manual_check_type_not_overwritten_by_default(self):
        handler = self.run_handler(BAD_ZIP)
        self.assertIs(handler.file_collection[0].check_type, PipelineFileCheckType.FORMAT_CHECK)
        self.assertIs(handler.file_collection[1].check_type, PipelineFileCheckType.NO_ACTION)

    def test_deletion_pipeline_files_not_checked(self):
        deletion = PipelineFile(self.temp_nc_file, is_deletion=True)

        def _preprocess(self_):
            self_.file_collection.add(deletion)

        handler = self.handler_class(GOOD_ZIP)
        handler.preprocess = partial(_preprocess, self_=handler)
        handler.run()

        self.assertIsNone(handler.error)
        self.assertFalse(deletion.is_checked)
        self.assertIs(deletion.check_type, PipelineFileCheckType.UNSET)

    @patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
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
        self.assertCountEqual(expected_recipients, [n.raw_string for n in handler.notification_results])
        self.assertTrue(all(r.notification_succeeded for r in handler.notification_results))
        self.assertTrue(all(r.error is None for r in handler.notification_results))

    @patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
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
        self.assertCountEqual(expected_recipients, [n.raw_string for n in handler.notification_results])
        self.assertTrue(all(r.notification_succeeded for r in handler.notification_results))
        self.assertTrue(all(r.error is None for r in handler.notification_results))

    @patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
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
        self.assertCountEqual(expected_recipients, [n.raw_string for n in handler.notification_results])
        self.assertTrue(all(r.notification_succeeded for r in handler.notification_results))
        self.assertTrue(all(r.error is None for r in handler.notification_results))

    @patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
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
        self.assertCountEqual(expected_recipients, [n.raw_string for n in handler.notification_results])
        self.assertTrue(all(r.notification_succeeded for r in handler.notification_results))
        self.assertTrue(all(r.error is None for r in handler.notification_results))

    @patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
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
        self.assertCountEqual(expected_recipients, [n.raw_string for n in handler.notification_results])
        self.assertTrue(handler.notification_results[0].notification_succeeded)
        self.assertFalse(handler.notification_results[1].notification_succeeded)
        self.assertIsNone(handler.notification_results[0].error)
        self.assertIsInstance(handler.notification_results[1].error, InvalidRecipientError)

    @patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
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
        self.assertCountEqual(expected_recipients, [n.raw_string for n in handler.notification_results])
        self.assertTrue(all(r.notification_succeeded for r in handler.notification_results))
        self.assertTrue(all(r.error is None for r in handler.notification_results))

    @patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
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
        self.assertCountEqual(expected_recipients, [n.raw_string for n in handler.notification_results])
        self.assertTrue(all(r.notification_succeeded for r in handler.notification_results))
        self.assertTrue(all(r.error is None for r in handler.notification_results))

    def test_property_default_addition_publish_type(self):
        handler = self.handler_class(self.temp_nc_file)
        handler.default_addition_publish_type = PipelineFilePublishType.NO_ACTION
        self.assertIs(handler.default_addition_publish_type, PipelineFilePublishType.NO_ACTION)

        with self.assertRaises(TypeError):
            handler.default_addition_publish_type = 'invalid'

    def test_property_default_deletion_publish_type(self):
        handler = self.handler_class(self.temp_nc_file)

        handler.default_deletion_publish_type = PipelineFilePublishType.NO_ACTION
        self.assertIs(handler.default_deletion_publish_type, PipelineFilePublishType.NO_ACTION)

        with self.assertRaises(TypeError):
            handler.default_deletion_publish_type = 'invalid'

    def test_opendap_root(self):
        handler = self.run_handler(self.temp_nc_file)
        self.assertEqual(handler.opendap_root, 'http://opendap.example.com')

    @patch('aodncore.util.wfs.WebFeatureService')
    def test_state_query(self, mock_webfeatureservice):
        handler = self.handler_class(self.temp_nc_file)
        self.assertIsInstance(handler.state_query, StateQuery)

    def test_add_pipelinefile(self):
        pf = PipelineFile(self.temp_nc_file)
        handler = self.handler_class(self.temp_nc_file)

        def _preprocess(self_):
            self_.add_pipelinefile(pf)

        handler.preprocess = partial(_preprocess, self_=handler)
        handler.run()

        self.assertEqual(handler._file_update_callback, pf.file_update_callback)
        self.assertIn(pf, handler.file_collection)

        with self.assertRaises(TypeError):
            handler.add_pipelinefile(1)

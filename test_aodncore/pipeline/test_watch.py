import logging
import os

from aodncore.pipeline import PipelineFile, PipelineFileCollection
from aodncore.pipeline.log import get_pipeline_logger
from aodncore.pipeline.watch import (delete_same_name_from_error_store_callback,
                                     delete_custom_regexes_from_error_store_callback,
                                     get_task_name, no_action_callback, CeleryConfig, ExitPolicy,
                                     IncomingFileStateManager)
from aodncore.testlib import mock, BaseTestCase
from aodncore.util import safe_copy_file
from test_aodncore import TESTDATA_DIR

GOOD_NC = os.path.join(TESTDATA_DIR, 'good.nc')
INVALID_PNG = os.path.join(TESTDATA_DIR, 'invalid.png')
TEST_ICO = os.path.join(TESTDATA_DIR, 'test.ico')
UNKNOWN_FILE_TYPE = os.path.join(TESTDATA_DIR, 'test.unknown_file_extension')

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
for lib in ('botocore', 'paramiko', 's3transfer', 'transitions'):
    logging.getLogger(lib).setLevel(logging.WARN)


class TestPipelineWatch(BaseTestCase):
    def setUp(self):
        self.logger = get_pipeline_logger('unittest')

        self.dummy_input_file = 'dummy.input_file'
        incoming_file_path = os.path.join(self.config.pipeline_config['watch']['incoming_dir'],
                                          os.path.basename(self.temp_nc_file))
        safe_copy_file(self.temp_nc_file, incoming_file_path)

        celery_request = type('DummyRequest', (object,), {'id': 'NO_REQUEST_ID'})()
        self.state_manager = IncomingFileStateManager(incoming_file_path, pipeline_name='UNITTEST', config=self.config,
                                                      logger=self.logger, celery_request=celery_request)
        self.state_manager.handler = mock.MagicMock(file_basename=self.dummy_input_file,
                                                    error_cleanup_regexes=['test.*'])

        previous_file_same_name = PipelineFile(self.temp_nc_file,
                                               dest_path='dummy.input_file.40c4ec0d-c9db-498d-84f9-01011330086e')
        nc = PipelineFile(GOOD_NC, dest_path=os.path.basename(GOOD_NC))
        png = PipelineFile(INVALID_PNG, dest_path=os.path.basename(INVALID_PNG))
        ico = PipelineFile(TEST_ICO, dest_path=os.path.basename(TEST_ICO))
        unknown = PipelineFile(UNKNOWN_FILE_TYPE, dest_path=os.path.basename(UNKNOWN_FILE_TYPE))
        existing_collection = PipelineFileCollection([previous_file_same_name, nc, png, ico, unknown])
        self.state_manager.error_broker.upload(existing_collection)

    def test_get_task_name(self):
        expected_name = 'tasks.UNITTEST'
        actual_name = get_task_name('tasks', 'UNITTEST')
        self.assertEqual(actual_name, expected_name)

    def test_no_action_callback(self):
        with mock.patch.object(self.state_manager, '_error_broker') as mock_error_broker:
            no_action_callback(self.state_manager.handler, self.state_manager)

        mock_error_broker.delete.assert_not_called()

    def test_delete_same_name_from_error_store_callback(self):
        actual_error_files_before_cleanup = self.state_manager.error_broker.query().keys()
        expected_error_files_before_cleanup = ['dummy.input_file.40c4ec0d-c9db-498d-84f9-01011330086e', 'good.nc',
                                               'test.unknown_file_extension', 'test.ico', 'invalid.png']
        self.assertItemsEqual(expected_error_files_before_cleanup, actual_error_files_before_cleanup)

        callback_log = delete_same_name_from_error_store_callback(self.state_manager.handler,
                                                                  self.state_manager)

        actual_error_files_after_cleanup = self.state_manager.error_broker.query().keys()
        expected_error_files_after_cleanup = ['good.nc', 'test.unknown_file_extension', 'test.ico', 'invalid.png']
        self.assertItemsEqual(expected_error_files_after_cleanup, actual_error_files_after_cleanup)

    def test_delete_custom_regexes_from_error_store_callback(self):
        actual_error_files_before_cleanup = self.state_manager.error_broker.query().keys()
        expected_error_files_before_cleanup = ['dummy.input_file.40c4ec0d-c9db-498d-84f9-01011330086e', 'good.nc',
                                               'test.unknown_file_extension', 'test.ico', 'invalid.png']
        self.assertItemsEqual(expected_error_files_before_cleanup, actual_error_files_before_cleanup)

        callback_log = delete_custom_regexes_from_error_store_callback(self.state_manager.handler,
                                                                       self.state_manager)

        actual_error_files_after_cleanup = self.state_manager.error_broker.query().keys()
        expected_error_files_after_cleanup = ['dummy.input_file.40c4ec0d-c9db-498d-84f9-01011330086e', 'good.nc',
                                              'invalid.png']
        self.assertItemsEqual(expected_error_files_after_cleanup, actual_error_files_after_cleanup)


class TestCeleryConfig(BaseTestCase):
    def test_init(self):
        celeryconfig = CeleryConfig()
        self.assertDictEqual(celeryconfig.CELERY_ROUTES, {})

    def test_init_routes(self):
        routes = {'/some/directory': 'UNITTEST'}
        celeryconfig = CeleryConfig(routes)
        self.assertIs(celeryconfig.CELERY_ROUTES, routes)


class TestCeleryManager(BaseTestCase):
    def setUp(self):
        self.celeryconfig = CeleryConfig()


class TestExitPolicy(BaseTestCase):
    def test_from_name(self):
        policy = ExitPolicy.from_name('DELETE_CUSTOM_REGEXES_FROM_ERROR_STORE')
        self.assertIs(policy, ExitPolicy.DELETE_CUSTOM_REGEXES_FROM_ERROR_STORE)

    def test_from_names(self):
        names = ['DELETE_SAME_NAME_FROM_ERROR_STORE', 'NO_ACTION']
        policies = ExitPolicy.from_names(names)
        expected_policies = [ExitPolicy.DELETE_SAME_NAME_FROM_ERROR_STORE, ExitPolicy.NO_ACTION]
        self.assertItemsEqual(expected_policies, policies)

    def test_callbacks(self):
        callbacks = [e.callback for e in ExitPolicy]
        self.assertTrue(all(callable(c) for c in callbacks))


class TestIncomingFileStateManager(BaseTestCase):
    def setUp(self):
        self.dummy_input_file = 'dummy.input_file'
        self.logger = get_pipeline_logger('unittest')
        incoming_file_path = os.path.join(self.config.pipeline_config['watch']['incoming_dir'],
                                          os.path.basename(self.temp_nc_file))
        safe_copy_file(self.temp_nc_file, incoming_file_path)
        celery_request = type('DummyRequest', (object,), {'id': 'NO_REQUEST_ID'})()
        self.state_manager = IncomingFileStateManager(incoming_file_path, pipeline_name='UNITTEST', config=self.config,
                                                      logger=self.logger, celery_request=celery_request)
        self.state_manager.handler = mock.MagicMock(file_basename=self.dummy_input_file,
                                                    error_cleanup_regexes=['test.*'])

    def test_error(self):
        self.assertTrue(os.path.exists(self.state_manager.input_file))
        self.assertFalse(os.path.exists(self.state_manager.processing_path))
        error_result = self.state_manager.error_broker.query(self.state_manager.error_name)
        self.assertNotIn(self.state_manager.error_name, error_result)

        self.state_manager.move_to_processing()

        self.assertFalse(os.path.exists(self.state_manager.input_file))
        self.assertTrue(os.path.exists(self.state_manager.processing_path))
        error_result = self.state_manager.error_broker.query(self.state_manager.error_name)
        self.assertNotIn(self.state_manager.error_name, error_result)

        self.state_manager.move_to_error()

        self.assertFalse(os.path.exists(self.state_manager.input_file))
        self.assertFalse(os.path.exists(self.state_manager.processing_path))
        error_result = self.state_manager.error_broker.query(self.state_manager.error_name)
        self.assertIn(self.state_manager.error_name, error_result)

    def test_success(self):
        self.assertTrue(os.path.exists(self.state_manager.input_file))
        self.assertFalse(os.path.exists(self.state_manager.processing_path))
        error_result = self.state_manager.error_broker.query(self.state_manager.error_name)
        self.assertNotIn(self.state_manager.error_name, error_result)

        self.state_manager.move_to_processing()

        self.assertFalse(os.path.exists(self.state_manager.input_file))
        self.assertTrue(os.path.exists(self.state_manager.processing_path))
        error_result = self.state_manager.error_broker.query(self.state_manager.error_name)
        self.assertNotIn(self.state_manager.error_name, error_result)

        self.state_manager.move_to_success()

        self.assertFalse(os.path.exists(self.state_manager.input_file))
        self.assertFalse(os.path.exists(self.state_manager.processing_path))
        error_result = self.state_manager.error_broker.query(self.state_manager.error_name)
        self.assertNotIn(self.state_manager.error_name, error_result)

    def test_cleanup(self):
        nc = PipelineFile(GOOD_NC, dest_path=os.path.basename(GOOD_NC))
        png = PipelineFile(INVALID_PNG, dest_path=os.path.basename(INVALID_PNG))
        ico = PipelineFile(TEST_ICO, dest_path=os.path.basename(TEST_ICO))
        unknown = PipelineFile(UNKNOWN_FILE_TYPE, dest_path=os.path.basename(UNKNOWN_FILE_TYPE))
        existing_collection = PipelineFileCollection([nc, png, ico, unknown])
        self.state_manager.error_broker.upload(existing_collection)

        self.state_manager.move_to_processing()

        actual_error_files_before_cleanup = self.state_manager.error_broker.query().keys()
        expected_error_files_before_cleanup = ['good.nc', 'test.unknown_file_extension', 'test.ico', 'invalid.png']
        self.assertItemsEqual(expected_error_files_before_cleanup, actual_error_files_before_cleanup)

        self.state_manager.success_exit_policies.append(ExitPolicy.DELETE_CUSTOM_REGEXES_FROM_ERROR_STORE)
        self.state_manager.run_success_exit_policies()

        actual_error_files_after_cleanup = self.state_manager.error_broker.query().keys()
        expected_error_files_after_cleanup = ['good.nc', 'invalid.png']
        self.assertItemsEqual(expected_error_files_after_cleanup, actual_error_files_after_cleanup)


class TestIncomingFileEventHandler(BaseTestCase):
    # TODO: write tests
    pass


class TestInotifyManager(BaseTestCase):
    # TODO: write tests
    pass

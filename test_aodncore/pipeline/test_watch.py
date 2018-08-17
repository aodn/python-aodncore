import os

from aodncore.pipeline.log import get_pipeline_logger
from aodncore.pipeline.watch import get_task_name, CeleryConfig, IncomingFileStateManager
from aodncore.testlib import BaseTestCase
from aodncore.util import safe_copy_file


class TestPipelineWatch(BaseTestCase):
    def test_get_task_name(self):
        expected_name = 'tasks.UNITTEST'
        actual_name = get_task_name('tasks', 'UNITTEST')
        self.assertEqual(actual_name, expected_name)


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


class TestIncomingFileStateManager(BaseTestCase):
    def setUp(self):
        self.logger = get_pipeline_logger('unittest')
        incoming_file_path = os.path.join(self.config.pipeline_config['watch']['incoming_dir'],
                                          os.path.basename(self.temp_nc_file))
        safe_copy_file(self.temp_nc_file, incoming_file_path)
        celery_request = type('DummyRequest', (object,), {'id': 'NO_REQUEST_ID'})()
        self.state_manager = IncomingFileStateManager(incoming_file_path, pipeline_name='UNITTEST', config=self.config,
                                                      logger=self.logger, celery_request=celery_request)

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


class TestIncomingFileEventHandler(BaseTestCase):
    # TODO: write tests
    pass


class TestInotifyManager(BaseTestCase):
    # TODO: write tests
    pass

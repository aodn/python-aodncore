from aodncore.pipeline.watch import (get_task_name, CeleryConfig)
from aodncore.testlib import BaseTestCase


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


class TestIncomingFileEventHandler(BaseTestCase):
    pass


class TestInotifyManager(BaseTestCase):
    pass

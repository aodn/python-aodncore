import os
import tempfile
import unittest

from aodncore.util import mkdir_p, rm_rf
from test_aodncore.testlib.testutil import GLOBAL_TEST_BASE, get_test_config, make_test_file


class BaseTestCase(unittest.TestCase):
    @property
    def config(self):
        if not hasattr(self, '_config'):
            self._config = get_test_config(self.temp_dir)
            for subdir in ('celery', 'harvest', 'process', 'watchservice'):
                mkdir_p(os.path.join(self.config.pipeline_config['logging']['log_root'], subdir))
        return self._config

    @property
    def temp_dir(self):
        if not hasattr(self, '_temp_dir'):
            self._temp_dir = tempfile.mkdtemp(prefix=self.__class__.__name__)
        return self._temp_dir

    @property
    def temp_nc_file(self):
        if not hasattr(self, '_temp_nc_file'):
            _, self._temp_nc_file = tempfile.mkstemp(suffix='.nc', prefix=self.__class__.__name__, dir=self.temp_dir)
            make_test_file(self._temp_nc_file)
        return self._temp_nc_file

    def setUp(self):
        self.maxDiff = 10000
        os.environ['PIPELINE_CONFIG_FILE'] = os.path.join(GLOBAL_TEST_BASE, 'pipeline', 'pipeline.conf')
        os.environ['PIPELINE_TRIGGER_CONFIG_FILE'] = os.path.join(GLOBAL_TEST_BASE, 'pipeline', 'trigger.conf')
        os.environ['PIPELINE_WATCH_CONFIG_FILE'] = os.path.join(GLOBAL_TEST_BASE, 'pipeline', 'watches.conf')

    def tearDown(self):
        if hasattr(self, '_temp_dir'):
            rm_rf(self._temp_dir)
        os.environ.pop('PIPELINE_CONFIG_FILE', None)
        os.environ.pop('PIPELINE_TRIGGER_CONFIG_FILE', None)
        os.environ.pop('PIPELINE_WATCH_CONFIG_FILE', None)

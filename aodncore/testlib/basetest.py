import logging.config
import os
import tempfile
import unittest

from aodncore.pipeline.log import SYSINFO, get_pipeline_logger
from .testutil import get_test_config, make_test_file
from ..util import mkdir_p, rm_rf

TEST_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
TEST_LOG_LEVEL = SYSINFO


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
        self.test_logger = get_pipeline_logger('unittest')
        logging.basicConfig(level=TEST_LOG_LEVEL, format=TEST_LOG_FORMAT)

    def tearDown(self):
        if hasattr(self, '_temp_dir'):
            rm_rf(self._temp_dir)

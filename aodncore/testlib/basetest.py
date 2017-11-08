import os
import tempfile
import unittest

from .testutil import get_test_config, make_test_file, mock
from ..util import mkdir_p, rm_rf


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
        self.mock_logger = mock.MagicMock()

    def tearDown(self):
        if hasattr(self, '_temp_dir'):
            rm_rf(self._temp_dir)

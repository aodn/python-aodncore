import logging.config
import os
import tempfile
import unittest

from aodncore.pipeline.log import SYSINFO, get_pipeline_logger
from .testutil import get_test_config, make_test_file
from ..util import mkdir_p, rm_rf

TEST_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
TEST_LOG_LEVEL = SYSINFO


class _AssertNoExceptionContext(object):  # pragma: no cover
    """A context manager used to implement BaseTestCase.assertNoException* method."""

    def __init__(self, test_case):
        self.failureException = test_case.failureException

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            raise self.failureException(
                "unexpected exception raised. {cls} {msg}".format(cls=exc_value.__class__.__name__, msg=exc_value))


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
            with tempfile.NamedTemporaryFile(suffix='.nc', prefix=self.__class__.__name__, dir=self.temp_dir) as f:
                pass
            self._temp_nc_file = f.name
            make_test_file(self._temp_nc_file)
        return self._temp_nc_file

    def setUp(self):
        self.maxDiff = 10000
        self.test_logger = get_pipeline_logger('unittest')
        logging.basicConfig(level=TEST_LOG_LEVEL, format=TEST_LOG_FORMAT)

    def tearDown(self):
        if hasattr(self, '_temp_dir'):
            rm_rf(self._temp_dir)

    def assertNoException(self, callableObj=None, *args, **kwargs):   # pragma: no cover
        """Fail if any exception is raised

        :return: None
        """
        context = _AssertNoExceptionContext(self)
        if callableObj is None:
            return context
        with context:
            callableObj(*args, **kwargs)

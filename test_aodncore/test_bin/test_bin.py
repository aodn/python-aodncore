import os

from aodncore.testlib import BaseTestCase, TESTLIB_CONF_DIR
from aodncore.bin import configvalidate

TEST_PIPELINE_CONFIG_FILE = os.path.join(TESTLIB_CONF_DIR, 'pipeline.conf')
TEST_WATCHES_CONFIG_FILE = os.path.join(TESTLIB_CONF_DIR, 'watches.conf')


class TestConfigValidate(BaseTestCase):
    def test_valid_pipeline_config(self):
        retval = configvalidate.validate_config_file(TEST_PIPELINE_CONFIG_FILE)
        self.assertEqual(retval, 0)

    def test_invalid_pipeline_config(self):
        retval = configvalidate.validate_config_file(TEST_WATCHES_CONFIG_FILE)
        self.assertEqual(retval, 1)

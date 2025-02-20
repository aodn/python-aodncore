import os
import unittest

from aodncore.testlib import BaseTestCase
from aodncore.util.logviewer import LogViewer

from .test_misc import get_nonexistent_path


TEST_ROOT = os.path.join(os.path.dirname(__file__))
LOG_FILE = os.path.join(TEST_ROOT, 'tasks.ANMN_SA.log')


class TestLogViewer(BaseTestCase):
    def test_init(self):
        lv = LogViewer(LOG_FILE)
        self.assertEqual(LOG_FILE, lv.logfile)
        self.assertRaises(ValueError, LogViewer, get_nonexistent_path())


if __name__ == '__main__':
    unittest.main()

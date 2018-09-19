from __future__ import absolute_import
import httpretty

from aodncore.pipeline.statequery import StateQuery
from aodncore.pipeline.storage import get_storage_broker
from aodncore.testlib import BaseTestCase
from test_aodncore.util.test_wfs import TEST_GETCAPABILITIES_RESPONSE


# noinspection PyUnusedLocal
class TestStateQuery(BaseTestCase):
    def setUp(self):
        self.storage_broker = get_storage_broker(self.config.pipeline_config['global']['error_uri'])

    @httpretty.activate
    def test_wfs(self):
        httpretty.register_uri(httpretty.GET, self.config.pipeline_config['global']['wfs_url'],
                               responses=[TEST_GETCAPABILITIES_RESPONSE])

        state_query = StateQuery(storage_broker=self.storage_broker,
                                 wfs_url=self.config.pipeline_config['global']['wfs_url'])

        # the inheritance style makes checking instance type difficult, so just check whether it quacks like a duck
        self.assertTrue(hasattr(state_query.wfs, 'get_schema'))
        self.assertTrue(hasattr(state_query.wfs, 'getfeature'))

    @httpretty.activate
    def test_no_wfs(self):
        httpretty.register_uri(httpretty.GET, self.config.pipeline_config['global']['wfs_url'],
                               responses=[TEST_GETCAPABILITIES_RESPONSE])

        state_query = StateQuery(storage_broker=self.storage_broker, wfs_url=None)

        with self.assertRaises(AttributeError):
            _ = state_query.wfs

        with self.assertRaises(AttributeError):
            _ = state_query.query_wfs_urls_for_layer('')

        with self.assertRaises(AttributeError):
            _ = state_query.query_wfs_url_exists('', '')

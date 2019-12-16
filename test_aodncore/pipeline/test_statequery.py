from aodncore.pipeline.statequery import StateQuery
from aodncore.pipeline.storage import get_storage_broker
from aodncore.testlib import BaseTestCase


class TestStateQuery(BaseTestCase):
    def setUp(self):
        self.storage_broker = get_storage_broker(self.config.pipeline_config['global']['error_uri'])

    def test_no_wfs(self):
        state_query = StateQuery(storage_broker=self.storage_broker, wfs_url=None)

        with self.assertRaises(AttributeError):
            _ = state_query.wfs

        with self.assertRaises(AttributeError):
            _ = state_query.query_wfs_urls_for_layer('')

        with self.assertRaises(AttributeError):
            _ = state_query.query_wfs_url_exists('', '')

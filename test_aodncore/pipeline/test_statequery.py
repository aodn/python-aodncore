import json
import os
from unittest.mock import patch

from aodncore.pipeline import RemotePipelineFileCollection, RemotePipelineFile, PipelineFile
from aodncore.pipeline.statequery import StateQuery
from aodncore.pipeline.storage import get_storage_broker
from aodncore.testlib import BaseTestCase
from aodncore.util.wfs import WfsBroker
from test_aodncore import TESTDATA_DIR

GOOD_NC = os.path.join(TESTDATA_DIR, 'good.nc')


class TestStateQuery(BaseTestCase):
    @patch('aodncore.util.wfs.WebFeatureService')
    def setUp(self, mock_webfeatureservice):
        self.storage_broker = get_storage_broker(self.config.pipeline_config['global']['upload_uri'])

        self.wfs_broker = WfsBroker(self.config.pipeline_config['global']['wfs_url'])

        with open(os.path.join(TESTDATA_DIR, 'wfs/GetFeature.json')) as f:
            self.wfs_broker.wfs.getfeature().getvalue.return_value = f.read()

        with open(os.path.join(TESTDATA_DIR, 'wfs/get_schema.json')) as f:
            self.wfs_broker.wfs.get_schema.return_value = json.load(f)

    def test_no_wfs(self):
        state_query = StateQuery(storage_broker=self.storage_broker, wfs_broker=None)

        with self.assertRaises(AttributeError):
            _ = state_query.wfs

        with self.assertRaises(AttributeError):
            _ = state_query.query_wfs_files('')

        with self.assertRaises(AttributeError):
            _ = state_query.query_wfs_file_exists('', '')

    def test_wfs(self):
        state_query = StateQuery(storage_broker=self.storage_broker, wfs_broker=self.wfs_broker)
        response = state_query.query_wfs_files('anmn_velocity_timeseries_map')

        expected = RemotePipelineFileCollection([
            RemotePipelineFile(
                'IMOS/ANMN/QLD/GBROTE/Velocity/IMOS_ANMN-QLD_AETVZ_20140408T102930Z_GBROTE_FV01_GBROTE-1404-AWAC-13_END-20141022T052930Z_C-20150215T063708Z.nc'),
            RemotePipelineFile(
                'IMOS/ANMN/NRS/NRSYON/Velocity/IMOS_ANMN-NRS_AETVZ_20110413T025900Z_NRSYON_FV01_NRSYON-1104-Workhorse-ADCP-27_END-20111014T222900Z_C-20150306T004801Z.nc')
        ])

        self.assertEqual(expected, response)

    def test_download_remotepipelinefilecollection(self):
        state_query = StateQuery(storage_broker=self.storage_broker, wfs_broker=self.wfs_broker)
        pipeline_file = PipelineFile(GOOD_NC, dest_path='dest/path/1.nc')
        self.storage_broker.upload(pipeline_file)

        remote_file = RemotePipelineFile.from_pipelinefile(pipeline_file)
        state_query.download(remote_file, local_path=self.temp_dir)

        self.assertTrue(os.path.exists(remote_file.local_path))

import json
import os
from unittest.mock import patch

from owslib.etree import etree
from owslib.fes import PropertyIsEqualTo

from aodncore.testlib import BaseTestCase
from aodncore.util import IndexedSet
from aodncore.util.wfs import WfsBroker, get_ogc_expression_for_file_url, ogc_filter_to_string
from test_aodncore import TESTDATA_DIR


class TestPipelineWfs(BaseTestCase):
    def test_get_filter_for_file_url(self):
        file_url = 'IMOS/test/file/url'
        ogc_expression = get_ogc_expression_for_file_url(file_url, property_name='file_url')
        xml_filter = ogc_filter_to_string(ogc_expression)

        root = etree.fromstring(xml_filter)
        property_name = root.findtext('ogc:PropertyName', namespaces=root.nsmap)
        literal = root.findtext('ogc:Literal', namespaces=root.nsmap)

        self.assertEqual(property_name, 'file_url')
        self.assertEqual(literal, file_url)


class TestWfsBroker(BaseTestCase):
    @patch('aodncore.util.wfs.WebFeatureService')
    def setUp(self, mock_webfeatureservice):
        self.broker = WfsBroker(self.config.pipeline_config['global']['wfs_url'])

        with open(os.path.join(TESTDATA_DIR, 'wfs/get_schema.json')) as f:
            self.broker.wfs.get_schema.return_value = json.load(f)

    def test_getfeature_dict(self):
        with open(os.path.join(TESTDATA_DIR, 'wfs/GetFeature.json')) as f:
            self.broker.wfs.getfeature().getvalue.return_value = f.read()

        response = self.broker.getfeature_dict(typename='anmn_velocity_timeseries_map', propertyname='file_url')

        self.assertEqual(len(response['features']), 2)
        self.assertEqual(response['features'][0]['properties']['file_url'],
                         'IMOS/ANMN/QLD/GBROTE/Velocity/IMOS_ANMN-QLD_AETVZ_20140408T102930Z_GBROTE_FV01_GBROTE-1404-AWAC-13_END-20141022T052930Z_C-20150215T063708Z.nc')
        self.assertEqual(response['features'][1]['properties']['file_url'],
                         'IMOS/ANMN/NRS/NRSYON/Velocity/IMOS_ANMN-NRS_AETVZ_20110413T025900Z_NRSYON_FV01_NRSYON-1104-Workhorse-ADCP-27_END-20111014T222900Z_C-20150306T004801Z.nc')

    def test_getfeature_dict_filter(self):
        with open(os.path.join(TESTDATA_DIR, 'wfs/GetFeature_filtered.json')) as f:
            self.broker.wfs.getfeature().getvalue.return_value = f.read()

        ogc_filter = PropertyIsEqualTo(propertyname='file_url',
                                       literal='IMOS/ANMN/QLD/GBROTE/Velocity/IMOS_ANMN-QLD_AETVZ_20140408T102930Z_GBROTE_FV01_GBROTE-1404-AWAC-13_END-20141022T052930Z_C-20150215T063708Z.nc')

        response = self.broker.getfeature_dict(typename='anmn_velocity_timeseries_map', propertyname='file_url',
                                               filter=ogc_filter)

        self.assertEqual(len(response['features']), 1)
        self.assertEqual(response['features'][0]['properties']['file_url'],
                         'IMOS/ANMN/QLD/GBROTE/Velocity/IMOS_ANMN-QLD_AETVZ_20140408T102930Z_GBROTE_FV01_GBROTE-1404-AWAC-13_END-20141022T052930Z_C-20150215T063708Z.nc')

    def test_get_url_property_name_for_layer(self):
        propertyname = self.broker.get_url_property_name('anmn_velocity_timeseries_map')
        self.assertEqual('file_url', propertyname)

    def test_get_url_property_name_for_layer_not_found(self):
        # patch the 'valid' candidates
        self.broker.url_propertyname_candidates = ('nonexistent_property', 'another_nonexistent_property')

        with self.assertRaises(RuntimeError):
            _ = self.broker.get_url_property_name('anmn_velocity_timeseries_map')

    def test_query_files_for_layer(self):
        with open(os.path.join(TESTDATA_DIR, 'wfs/GetFeature.json')) as f:
            self.broker.wfs.getfeature().getvalue.return_value = f.read()

        files_for_layer = self.broker.query_files('anmn_velocity_timeseries_map')
        self.assertIsInstance(files_for_layer, IndexedSet)

    def test_query_file_exists_for_layer_true(self):
        with open(os.path.join(TESTDATA_DIR, 'wfs/GetFeature.json')) as f:
            self.broker.wfs.getfeature().getvalue.return_value = f.read()

        file_to_check = 'IMOS/ANMN/QLD/GBROTE/Velocity/IMOS_ANMN-QLD_AETVZ_20140408T102930Z_GBROTE_FV01_GBROTE-1404-AWAC-13_END-20141022T052930Z_C-20150215T063708Z.nc'

        file_exists = self.broker.query_file_exists(layer='anmn_velocity_timeseries_map', name=file_to_check)
        self.assertTrue(file_exists)

    def test_query_file_exists_for_layer_false(self):
        with open(os.path.join(TESTDATA_DIR, 'wfs/GetFeature.json')) as f:
            self.broker.wfs.getfeature().getvalue.return_value = f.read()

        file_to_check = "IMOS/ANMN/QLD/GBROTE/Velocity/FILE_THAT_ISNT_IN_RESULTS.nc"

        file_exists = self.broker.query_file_exists(layer='anmn_velocity_timeseries_map', name=file_to_check)
        self.assertFalse(file_exists)

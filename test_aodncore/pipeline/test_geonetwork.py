import os
from io import StringIO
from unittest.mock import Mock, patch
import xml.etree.ElementTree as ET

import requests

from aodncore.pipeline.exceptions import GeonetworkRequestError, GeonetworkConnectionError
from aodncore.pipeline.geonetwork import (Geonetwork, GeonetworkMetadataHandler, dict_to_xml,
                                          geonetwork_exception_handler)
from aodncore.testlib import BaseTestCase
from test_aodncore import TESTDATA_DIR

GOOD_TEMPLATE = {
    'tag': ['path', 'to', 'elem'],
    'attr': {'id': '123456'},
    'elems': [
        {
            'display': False,
            'tag': ['do', 'not', 'display'],
            'value': 'you can\'t see me'
        },
        {
            'tag': 'not_a_list',
            'elems': [
                {
                    'tag': 'nested_one',
                    'value': 'now you see me'
                },
                {
                    'tag': 'nested_two',
                    'value': 'you also see me'
                }
            ]
        }
    ]
}

GOOD_XML = os.path.join(TESTDATA_DIR, 'test.metadata.xml')
BAD_URL = 'http://not/a/real/url'
TEST_BASE_URL = 'https://postman-echo.com'
USERNAME = 'postman'
PASSWORD = 'password'
METADATA = {
    'uuid': '123456',
    'spatial': {'table': 'not_a_table', 'column': 'NOT_A_COLUMN', 'resolution': 3},
    'temporal': {'table': 'not_a_table', 'column': 'NOT_A_COLUMN'},
    'vertical': {'table': 'not_a_table', 'column': 'NOT_A_COLUMN'}
}
NAMESPACES = {
    'xmlns:gex': 'http://standards.iso.org/iso/19115/-3/gex/1.0',
    'xmlns:mri': 'http://standards.iso.org/iso/19115/-3/mri/1.0',
    'xmlns:gco': 'http://standards.iso.org/iso/19115/-3/gco/1.0',
    'xmlns:gml': 'http://www.opengis.net/gml/3.2'
}

class TestHelperFunctions(BaseTestCase):
    def test_dict_to_xml(self):
        expect = "<path><to><elem id=\"123456\"><do><not><display>you can't see me</display></not></do><not_a_list>" \
                 "<nested_one>now you see me</nested_one><nested_two>you also see me</nested_two></not_a_list></elem>" \
                 "</to></path>"
        with self.assertNoException():
            actual = dict_to_xml(**GOOD_TEMPLATE)

        self.assertEqual(expect, actual)

    def test_geonetwork_exception_handler(self):
        session = requests.Session()

        with self.assertRaises(GeonetworkConnectionError):
            with geonetwork_exception_handler():
                session.get(BAD_URL)

        with self.assertRaises(GeonetworkRequestError):
            with geonetwork_exception_handler():
                response = session.get(os.path.join(TEST_BASE_URL, 'status', '500'))
                response.raise_for_status()


class TestGeonetwork(BaseTestCase):
    """Placeholder tests for Geonetwork enpoint handler

    To realistically test these endpoints we would need an instance of Geonetwork.  A possible future enhancement might
    be to run tests in a Docker container that contains a Geonetwork image
    """
    def _mock_response(
            self,
            status=200,
            content="CONTENT",
            json_data=None,
            raise_for_status=None):
        mock_resp = Mock()
        # mock raise_for_status call w/optional error
        mock_resp.raise_for_status = Mock()
        if raise_for_status:
            mock_resp.raise_for_status.side_effect = raise_for_status
        # set status code and content
        mock_resp.status_code = status
        mock_resp.content = content
        mock_resp.text = content
        # add json data if provided
        if json_data:
            mock_resp.json = Mock(
                return_value=json_data
            )
        return mock_resp

    def test_instantiate_geonetwork(self):
        with self.assertNoException():
            gn = Geonetwork(os.path.join(TEST_BASE_URL, 'post'), USERNAME, PASSWORD)

        self.assertEqual('https://postman-echo.com/post', gn.base_url)
        self.assertEqual((USERNAME, PASSWORD), gn.session.auth)

    @patch('aodncore.pipeline.geonetwork.requests.Session.get')
    def test_get_record(self, mock_get):
        mock_resp = self._mock_response(content="mock response")
        mock_get.return_value = mock_resp
        gn = Geonetwork(os.path.join(TEST_BASE_URL, 'get'), USERNAME, PASSWORD)
        with self.assertNoException():
            result = gn.get_record('123456')
        self.assertEqual(result, 'mock response')

    @patch('aodncore.pipeline.geonetwork.requests.Session.put')
    def test_update_record(self, mock_put):
        mock_resp = self._mock_response(content="mock response")
        mock_put.return_value = mock_resp
        gn = Geonetwork(os.path.join(TEST_BASE_URL, 'post'), USERNAME, PASSWORD)
        with self.assertNoException():
            gn.update_record(None, None)


class TestGeonetworkMetadataHandler(BaseTestCase):
    def test_metadata_handler(self):
        with self.assertNoException():
            handler = GeonetworkMetadataHandler(None, None, METADATA, None)
        self.assertIsNone(handler._logger)
        self.assertIsNone(handler._session)
        self.assertIsNone(handler._conn)
        self.assertEqual(METADATA['uuid'], handler.uuid)
        self.assertDictEqual(METADATA['spatial'], handler.spatial)
        self.assertDictEqual(METADATA['temporal'], handler.temporal)
        self.assertDictEqual(METADATA['vertical'], handler.vertical)

    def test_get_namespace_dict(self):
        handler = GeonetworkMetadataHandler(None, None, {}, None)
        with open(GOOD_XML, encoding='utf-8') as xml:
            handler.xml_text = xml.read()

        with self.assertNoException():
            ns = handler.get_namespace_dict()

        self.assertDictEqual(ns, NAMESPACES)

    def test_build_api_payload(self):
        handler = GeonetworkMetadataHandler(None, None, METADATA, None)
        with open(GOOD_XML, encoding='utf-8') as xml:
            handler.xml_text = xml.read()

        with self.assertNoException():
            payload = handler.build_api_payload()

        self.assertIsNotNone(payload)

    def test_run(self):
        mock_conn = Mock()
        mock_conn.get_spatial_extent.return_value = {'boundingpolygonasgml3': 'spatial data'}
        mock_conn.get_temporal_extent.return_value = {'min_value': '1900-01-01', 'max_value': '1900-01-02'}
        mock_conn.get_vertical_extent.return_value = {'min_value': 0, 'max_value': 1}
        mock_session = Mock()
        with open(GOOD_XML, encoding='utf-8') as xml:
            mock_session.get_record.return_value = xml.read()
        handler = GeonetworkMetadataHandler(mock_conn, mock_session, METADATA, self.test_logger)
        print(handler.xml_text)

        with self.assertNoException():
            handler.run()

        assert mock_conn.get_spatial_extent.called, 'Spatial extent method was not called but should have been'
        assert mock_conn.get_temporal_extent.called, 'Temporal extent method was not called but should have been'
        assert mock_conn.get_vertical_extent.called, 'Vertical extent method was not called but should have been'
        assert mock_session.update_record.called, 'Update record method was not called but should have been'

    def test_run_no_metadata(self):
        mock_conn = Mock()
        mock_session = Mock()
        handler = GeonetworkMetadataHandler(mock_conn, mock_session, {}, self.test_logger)
        with open(GOOD_XML, encoding='utf-8') as xml:
            mock_session.return_value.get_record = xml.read()

        with self.assertNoException():
            handler.run()
        assert not mock_conn.get_spatial_extent.called, 'Spatial extent method was called but should not have been'
        assert not mock_conn.get_temporal_extent.called, 'Temporal extent method was called but should not have been'
        assert not mock_conn.get_vertical_extent.called, 'Vertical extent method was called but should not have been'
        assert not mock_session.update_record.called, 'Update record method was called and should not have been'

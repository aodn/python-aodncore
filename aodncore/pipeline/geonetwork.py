"""
Geonetwork Library
"""
import contextlib
import json
import os
from io import StringIO
from xml.etree import ElementTree

# 'requests>=2.5' is a dependency of tableschema (and possibly other aodncore requirements), however should tableschema
# no longer be required, it may be necessary to explicitly install 'requests'
import requests
from requests.exceptions import ConnectionError, RequestException

from ..util import list_not_empty, generate_id
from .exceptions import GeonetworkRequestError, GeonetworkConnectionError


BASE_API = "srv/api/0.1"
ENDPOINT_RECORD_GET = 'records'
ENDPOINT_BATCH_UPDATE = 'records/batchediting'


def dict_to_xml(tag, value=None, attr=None, elems=None, display=True):
    """Convert a dictionary of XML nodes into a nested XML string

    :param tag: a string or list containing element tag(s).  If tag is a list then recursively build the parent elements
        and apply the rest of the logic to the last element.
    :param value: string representing text for an element
    :param attr: dict containing key:value pairs representing attributes of the element
    :param elems: list of dicts containing nested element definitions.  If this is present then the value will be
        overridden with recursive child elements
    :param display: boolen to identify whether the element (and child nodes) should be rendered to the XML string
    :return: an XML string
    """
    if isinstance(tag, list):
        nodes = {'tag': tag[-1], 'elems': elems, 'attr': attr, 'value': value}
        for t in reversed(tag[:-1]):
            nodes = {'tag': t, 'elems': [nodes]}
        return dict_to_xml(**nodes)
    if not display:
        return ''
    attr = [] if attr is None else [' {}="{}"'.format(k, v) for k, v in attr.items()]
    if elems:
        value = ''
        for elem in elems:
            value += dict_to_xml(**elem)
    return '<{tag}{attributes}>{value}</{tag}>'.format(tag=tag, attributes=' '.join(attr), value=value)


@contextlib.contextmanager
def geonetwork_exception_handler():
    try:
        yield
    except ConnectionError as e:
        raise GeonetworkConnectionError(e)
    except RequestException as e:
        raise GeonetworkRequestError(e)


class Geonetwork(object):
    """Geonetwork API session handler

    :param base_url: Geonetwork instance base url
    :param username: username for the Geonetwork API
    :param password: password for the Geonetwork API
    :param logger: an instance of the logger
    """
    def __init__(self, base_url, username, password):
        self.base_url = base_url

        self.session = requests.Session()
        self.session.verify = True
        self.session.auth = (username, password)

        # init cookies
        url = os.path.join(self.base_url, BASE_API)
        with geonetwork_exception_handler():
            self.session.post(url)

        for cookie in self.session.cookies:
            if cookie.name == "XSRF-TOKEN":
                self.session.headers.update({'X-XSRF-TOKEN': cookie.value})
        self.session.headers.update({'Accept': 'application/xml'})

    def _get(self, path):
        url = os.path.join(self.base_url, path)
        with geonetwork_exception_handler():
            response = self.session.get(url)
            response.raise_for_status()
        return response

    def _post(self, path, data=None, params=None):
        url = os.path.join(self.base_url, path)
        with geonetwork_exception_handler():
            response = self.session.post(url, data=json.dumps(data), params=params)
            response.raise_for_status()
        return response

    def _put(self, path, data=None, params=None, headers=None):
        url = os.path.join(self.base_url, path)
        with geonetwork_exception_handler():
            response = self.session.put(url, data=json.dumps(data), params=params, headers=headers)
            response.raise_for_status()
        return response

    def get_record(self, _uuid):
        """Retrieve a metadata record

        :param _uuid: Geonetwork record ID
        :return: xml of specified metadata record"""
        return self._get(os.path.join(BASE_API, ENDPOINT_RECORD_GET, _uuid)).text

    def update_record(self, _uuid, changes):
        """Update Geonetwork record

        :param _uuid: Geonetwork record ID
        :param changes: list of change dicts where each change contains a value and an xpath
        """
        params = {"uuids": _uuid}
        headers = {"accept": "application/json", "content-type": "application/json"}
        self._put(os.path.join(BASE_API, ENDPOINT_BATCH_UPDATE), data=changes, params=params, headers=headers)


class GeonetworkMetadataHandler(object):
    """Handle changes to Geonetwork metadata from Harvester

    Build the geonetwork payload and push changes
    :param conn: the database connection class (DatabaseInteractions)
    :param session: the geonetwork API session
    :param metadata: dict containing extents for a single metadata record
    :param logger: instance of the logger
    """

    def __init__(self, conn, session, metadata, logger):
        self._logger = logger
        self._session = session
        self._conn = conn

        self.uuid = metadata.get('uuid')
        self.spatial = metadata.get('spatial')
        self.temporal = metadata.get('temporal')
        self.vertical = metadata.get('vertical')
        self.spatial_data = {}
        self.vertical_data = {}
        self.temporal_data = {}
        self.xml_text = None

    def get_namespace_dict(self):
        """Scrape relevant namespaces from source metadata record"""
        ns_raw = dict([
            node for (_, node) in ElementTree.iterparse(StringIO(self.xml_text), events=['start-ns'])
        ])
        ns_keep = ['mri', 'gex', 'gml', 'gco']
        ns = {}
        for k, v in ns_raw.items():
            if k in ns_keep:
                ns['xmlns:{}'.format(k)] = v
        return ns

    def build_api_payload(self):
        """Build the batchedit API payload based on dict template

        The payload is a complete replacement of all extents for the provided metadata record
        """
        template = {
            'tag': ['gn_replace', 'gex:EX_Extent'],
            'attr': self.get_namespace_dict(),
            'elems': [
                {
                    # geographic extent
                    'display': bool(self.spatial_data),
                    'tag': ['gex:geographicElement', 'gex:EX_BoundingPolygon', 'gex:polygon'],
                    'value': self.spatial_data.get('boundingpolygonasgml3')
                },
                {
                    # vertical extent
                    'display': bool(self.vertical_data),
                    'tag': ['gex:verticalElement', 'gex:EX_VerticalExtent'],
                    'elems': [
                        {
                            'tag': ['gex:minimumValue', 'gco:Real'],
                            'value': self.vertical_data.get('min_value')
                        },
                        {
                            'tag': ['gex:maximumValue', 'gco:Real'],
                            'value': self.vertical_data.get('max_value')
                        }
                    ]
                },
                {
                    # temporal extent
                    'display': bool(self.temporal_data),
                    'tag': ['gex:temporalElement', 'gex:EX_TemporalExtent', 'gex:extent', 'gml:TimePeriod'],
                    'attr': {'gml:id': generate_id()},
                    'elems': [
                        {
                            'tag': 'gml:beginPosition',
                            'value': self.temporal_data.get('min_value')
                        },
                        {
                            'tag': 'gml:endPosition',
                            'value': self.temporal_data.get('max_value')
                        }
                    ]
                }
            ]
        }
        return [
            {'value': dict_to_xml(**template),
             'xpath': './/mri:MD_DataIdentification/mri:extent'}
        ]

    def run(self):
        if list_not_empty([self.spatial, self.temporal, self.vertical]):
            self._logger.info('Collecting extent data for {}'.format(self.uuid))
            if self.spatial:
                self.spatial_data = self._conn.get_spatial_extent(**self.spatial)
            if self.temporal:
                self.temporal_data = self._conn.get_temporal_extent(**self.temporal)
            if self.vertical:
                self.vertical_data = self._conn.get_vertical_extent(**self.vertical)

            self.xml_text = self._session.get_record(self.uuid)
            payload = self.build_api_payload()
            self._logger.info('Updating extent data for {}'.format(self.uuid))
            self._session.update_record(_uuid=self.uuid, changes=payload)

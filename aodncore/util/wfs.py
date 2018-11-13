import json
from collections import OrderedDict

from owslib.etree import etree
from owslib.fes import PropertyIsEqualTo
from owslib.wfs import WebFeatureService

from ..util import IndexedSet

__all__ = [
    'WfsBroker',
    'get_filter_for_file_url',
    'ogc_filter_to_string'
]


def ogc_filter_to_string(ogc_filter):
    """Convert an OGC filter object into it's XML string representation

    :param ogc_filter: OGC filter object
    :return: XML string
    """
    return etree.tostring(ogc_filter.toXML()).decode('utf-8')


def get_filter_for_file_url(file_url, property_name='url'):
    """Return OGC filter XML to query for a single file_url

    :param file_url: URL string
    :param property_name: URL property name to filter on
    :return: OGC XML filter string
    """
    file_url_filter = PropertyIsEqualTo(propertyname=property_name, literal=file_url)
    return ogc_filter_to_string(file_url_filter)


class WfsBroker(object):
    """Simple higher level interface to a WebFeatureService instance, to provide common helper methods and standardise
    response handling around JSON
    """

    # The *first* matching property name found by the WebFeatureService.get_schema method will be considered to be the
    # "url" property for a given layer. Accordingly, this should be ordered with highest priority name first.
    url_propertyname_candidates = ('file_url', 'url')

    def __init__(self, wfs_url, version='1.0.0'):
        self._wfs = WebFeatureService(wfs_url, version=version)

    @property
    def wfs(self):
        """Read-only property to access the instantiated WebFeatureService object directly

        :return: WebFeatureService instance
        """
        return self._wfs

    def getfeature_dict(self, **kwargs):
        """Make a GetFeature request, and return the response in a native dict

        :param kwargs: keyword arguments passed to the underlying WebFeatureService.getfeature method
        :return: dict containing the parsed GetFeature response
        """
        kwargs.pop('outputFormat', None)
        response = self.wfs.getfeature(outputFormat='json', **kwargs)
        response_body = response.getvalue()
        try:
            return json.loads(response_body, object_pairs_hook=OrderedDict)
        finally:
            response.close()

    def get_url_property_name_for_layer(self, layer):
        """Get the URL property name for a given layer

        :param layer: schema dict as returned by WebFeatureService.get_schema
        :return: string containing the URL property name
        """
        schema = self.wfs.get_schema(layer)
        for candidate in self.url_propertyname_candidates:
            if candidate in schema['properties']:
                return candidate
        else:
            raise RuntimeError('unable to determine URL property name!')

    def query_urls_for_layer(self, layer, ogc_filter=None, url_property_name=None):
        """Return an IndexedSet of files for a given layer

        :param layer: layer name supplied to GetFeature typename parameter
        :param ogc_filter: XML string represenation of an OGC filter expression. If omitted, all URLs are returned.
        :param url_property_name: property name for file URL. If omitted, property name is determined from layer schema
        :return: list of files for the layer
        """

        if url_property_name is None:
            url_property_name = self.get_url_property_name_for_layer(layer)

        getfeature_kwargs = {
            'typename': [layer],
            'propertyname': url_property_name
        }

        if ogc_filter:
            getfeature_kwargs['filter'] = ogc_filter

        parsed_response = self.getfeature_dict(**getfeature_kwargs)
        file_urls = IndexedSet(f['properties'][url_property_name] for f in parsed_response['features'])
        return file_urls

    def query_url_exists_for_layer(self, layer, name):
        """Returns a bool representing whether a given 'file_url' is present in a layer

        :param layer: layer name supplied to GetFeature typename parameter
        :param name: 'file_url' inserted into OGC filter, and supplied to GetFeature filter parameter
        :return: list of files for the layer
        """
        url_property_name = self.get_url_property_name_for_layer(layer)
        ogc_filter = get_filter_for_file_url(name, property_name=url_property_name)
        file_urls = self.query_urls_for_layer(layer, ogc_filter=ogc_filter, url_property_name=url_property_name)
        return name in file_urls

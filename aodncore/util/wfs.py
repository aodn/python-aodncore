import json
import warnings
from collections import OrderedDict

from owslib.etree import etree
from owslib.fes import PropertyIsEqualTo
from owslib.wfs import WebFeatureService

from ..util import IndexedSet, lazyproperty

__all__ = [
    'DEFAULT_WFS_VERSION',
    'WfsBroker',
    'get_ogc_expression_for_file_url',
    'ogc_filter_to_string'
]

DEFAULT_WFS_VERSION = '1.1.0'


def ogc_filter_to_string(ogc_expression):
    """Convert an OGCExpression object into it's XML string representation. If parameter is a `str` object, it is
        returned unchanged

    :param ogc_expression: OGCExpression object
    :return: XML string representation of the input object
    """
    if isinstance(ogc_expression, str):
        return ogc_expression
    return etree.tostring(ogc_expression.toXML(), encoding='unicode')


def get_ogc_expression_for_file_url(file_url, property_name='url'):
    """Return OGCExpression to query for a single file_url

    :param file_url: URL string
    :param property_name: URL property name to filter on
    :return: OGCExpression which may be used to query the given URL value
    """
    return PropertyIsEqualTo(propertyname=property_name, literal=file_url)


class WfsBroker(object):
    """Simple higher level interface to a WebFeatureService instance, to provide common helper methods and standardise
    response handling around JSON
    """

    # The *first* matching property name found by the WebFeatureService.get_schema method will be considered to be the
    # "url" property for a given layer. Accordingly, this should be ordered with highest priority name first.
    url_propertyname_candidates = ('file_url', 'url')

    def __init__(self, wfs_url, version=DEFAULT_WFS_VERSION):
        self._wfs_url = wfs_url
        self._wfs_version = version

    @lazyproperty
    def wfs(self):
        """Read-only property to access the instantiated WebFeatureService object directly

        Note: lazily initialised because instantiating a WebFeatureService causes HTTP traffic, which is only
        desirable if subsequent WFS requests are actually going to be made (which isn't always the case when
        instantiating this broker class)

        :return: WebFeatureService instance
        """
        return WebFeatureService(self._wfs_url, version=self._wfs_version)

    def getfeature_dict(self, layer, ogc_expression=None, **kwargs):
        """Make a GetFeature request, and return the response in a native dict.

        :param layer: layer name supplied to GetFeature typename parameter
        :param ogc_expression: OgcExpression used to filter the returned features. If omitted, returns all features.
        :param kwargs: keyword arguments passed to the underlying WebFeatureService.getfeature method
        :return: dict containing the parsed GetFeature response
        """
        getfeature_kwargs = kwargs.copy()

        # convert expression to XML string representation as required by underlying WFS API
        if ogc_expression:
            getfeature_kwargs['filter'] = ogc_filter_to_string(ogc_expression)

        # force the output format to JSON, as other formats don't make sense in the context of parsing into a dict
        getfeature_kwargs['outputFormat'] = 'json'

        getfeature_kwargs['typename'] = layer

        response = self.wfs.getfeature(**getfeature_kwargs)
        response_body = response.getvalue()
        try:
            return json.loads(response_body, object_pairs_hook=OrderedDict)
        finally:
            response.close()

    def get_url_property_name(self, layer):
        """Get the URL property name for a given layer

        :param layer: schema dict as returned by WebFeatureService.get_schema
        :return: string containing the URL property name
        """
        schema = self.wfs.get_schema(layer)
        for candidate in self.url_propertyname_candidates:
            if candidate in schema['properties']:
                return candidate
        else:  # pragma: no cover
            raise RuntimeError('unable to determine URL property name!')

    def query_files(self, layer, ogc_expression=None, url_property_name=None):
        """Return an IndexedSet of files for a given layer

        :param layer: layer name supplied to GetFeature typename parameter
        :param ogc_expression: OgcExpression used to filter the returned features. If omitted, all URLs are returned.
        :param url_property_name: property name for file URL. If omitted, property name is determined from layer schema
        :return: list of files for the layer
        """

        if url_property_name is None:
            url_property_name = self.get_url_property_name(layer)

        getfeature_kwargs = {
            'propertyname': url_property_name
        }

        if ogc_expression:
            getfeature_kwargs['ogc_expression'] = ogc_expression

        parsed_response = self.getfeature_dict(layer, **getfeature_kwargs)
        file_urls = IndexedSet(f['properties'][url_property_name] for f in parsed_response['features'])
        return file_urls

    def query_urls_for_layer(self, layer, ogc_expression=None, url_property_name=None):
        warnings.warn("This method will be removed in a future version. Please update code to use "
                      "`query_urls` instead.", DeprecationWarning)

        return self.query_files(layer, ogc_expression=ogc_expression, url_property_name=url_property_name)

    def query_file_exists(self, layer, name):
        """Returns a bool representing whether a given 'file_url' is present in a layer

        :param layer: layer name supplied to GetFeature typename parameter
        :param name: 'file_url' inserted into OGC filter, and supplied to GetFeature filter parameter
        :return: whether the given file is present in the layer
        """
        url_property_name = self.get_url_property_name(layer)
        ogc_expression = get_ogc_expression_for_file_url(name, property_name=url_property_name)
        file_urls = self.query_files(layer, ogc_expression=ogc_expression, url_property_name=url_property_name)
        return name in file_urls

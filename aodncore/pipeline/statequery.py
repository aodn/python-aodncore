from ..util.wfs import WfsBroker

__all__ = [
    'StateQuery'
]


class StateQuery(object):
    """Simple state query interface, to provide user friendly access for querying existing Pipeline state
    """

    def __init__(self, storage_broker, wfs_url, wfs_version='1.0.0'):
        self._storage_broker = storage_broker
        self._wfs_url = wfs_url
        self._wfs_version = wfs_version

        self._wfs_broker_object = None

    @property
    def _wfs_broker(self):
        if not self._wfs_url:
            raise AttributeError('WFS querying unavailable: no wfs_url configured?')

        # lazy instantiation of broker to avoid any WFS activity unless a handler explicitly calls it
        if self._wfs_broker_object is None:
            self._wfs_broker_object = WfsBroker(self._wfs_url, version=self._wfs_version)
        return self._wfs_broker_object

    @property
    def wfs(self):
        """Read-only property to access the instantiated WebFeatureService object

        :return: WebFeatureService instance
        """
        return self._wfs_broker.wfs

    def query_storage(self, query):  # pragma: no cover
        """Query the storage backend and return existing files matching the given query

        :param query: S3-style prefix for filtering query results
        :return: dict containing the query results
        """
        return self._storage_broker.query(query)

    def query_wfs_urls_for_layer(self, layer):  # pragma: no cover
        """Return an IndexedSet of files for a given layer

        :param layer: layer name supplied to GetFeature typename parameter
        :return: list of files for the layer
        """
        return self._wfs_broker.query_urls_for_layer(layer)

    def query_wfs_url_exists(self, layer, name):  # pragma: no cover
        """Returns a bool representing whether a given 'file_url' is present in a layer

        :param layer: layer name supplied to GetFeature typename parameter
        :param name: 'file_url' inserted into OGC filter, and supplied to GetFeature filter parameter
        :return: list of files for the layer
        """
        return self._wfs_broker.query_url_exists_for_layer(layer, name)

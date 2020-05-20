import warnings

from .files import RemotePipelineFileCollection

__all__ = [
    'StateQuery'
]


class StateQuery(object):
    """Simple state query interface, to provide user friendly access for querying existing Pipeline state
    """

    def __init__(self, storage_broker, wfs_broker):
        self._storage_broker = storage_broker
        self._wfs_broker = wfs_broker

    # WFS methods
    @property
    def wfs(self):
        """Read-only property to access the instantiated WebFeatureService object

        :return: WebFeatureService instance
        """
        return self._wfs_broker.wfs

    def query_wfs_getfeature_dict(self, **kwargs):
        """Make a GetFeature request, and return the response in a native dict

        :param kwargs: keyword arguments passed to the underlying WebFeatureService.getfeature method
        :return: dict containing the parsed GetFeature response
        """
        return self._wfs_broker.getfeature_dict(**kwargs)

    def query_wfs_urls(self, layer, **kwargs):  # pragma: no cover
        """Return a RemotePipelineFileCollection containing all files for a given layer

        :param layer: layer name supplied to GetFeature typename parameter
        :param kwargs: keyword arguments passed to underlying broker method
        :return: RemotePipelineFileCollection containing list of files for the layer
        """
        return RemotePipelineFileCollection(self._wfs_broker.query_urls(layer, **kwargs))

    def query_wfs_urls_for_layer(self, layer, **kwargs):  # pragma: no cover
        warnings.warn("This method will be removed in a future version. Please update code to use "
                      "`query_wfs_urls` instead.", DeprecationWarning)
        return self._wfs_broker.query_urls(layer, **kwargs)

    def query_wfs_url_exists(self, layer, name):  # pragma: no cover
        """Returns a bool representing whether a given 'file_url' is present in a layer

        :param layer: layer name supplied to GetFeature typename parameter
        :param name: 'file_url' inserted into OGC filter, and supplied to GetFeature filter parameter
        :return: list of files for the layer
        """
        return self._wfs_broker.query_url_exists(layer, name)

    # Storage methods
    def download(self, remotepipelinefilecollection, local_path):
        """Helper method to download a RemotePipelineFileCollection or RemotePipelineFile

        :param remotepipelinefilecollection: RemotePipelineFileCollection to download
        :param local_path: local path where files will be downloaded. Defaults to the handler's :attr:`temp_dir` value.
        :return: None
        """
        self._storage_broker.download(remotepipelinefilecollection, local_path)

    def query_storage(self, query):  # pragma: no cover
        """Query the storage backend and return existing files matching the given query

        :param query: S3-style prefix for filtering query results
        :return: dict containing the query results
        """
        return self._storage_broker.query(query)

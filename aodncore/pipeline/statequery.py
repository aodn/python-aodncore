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

    def query_wfs_getfeature_dict(self, layer=None, **kwargs):
        """Make a GetFeature request, and return the response in a native dict

        :param layer: layer name supplied to GetFeature typename parameter
        :param kwargs: keyword arguments passed to the underlying WebFeatureService.getfeature method
        :return: dict containing the parsed GetFeature response
        """
        # TODO: once aodndata code uses the layer parameter, this deprecation can be removed, and the parameter
        #       converted into a mandatory positional argument
        if not layer:
            warnings.warn("This method signature will be updated to require a layer positional argument in a future"
                          "version. Please update code to use `query_wfs_getfeature_dict(layer, **kwargs)` signature"
                          "instead.", DeprecationWarning)
            layer = kwargs.pop('typename')
        return self._wfs_broker.getfeature_dict(layer, **kwargs)

    def query_wfs_files(self, layer, **kwargs):  # pragma: no cover
        """Return a RemotePipelineFileCollection containing all files for a given layer, 
        or files matching the filter specified in the kwarg `ogc_expression` (of type OgcExpression)

        :param layer: layer name supplied to GetFeature typename parameter
        :param kwargs: keyword arguments passed to underlying broker method
        :return: RemotePipelineFileCollection containing list of files for the layer
        """
        return RemotePipelineFileCollection(self._wfs_broker.query_files(layer, **kwargs))

    def query_wfs_urls_for_layer(self, layer, **kwargs):  # pragma: no cover
        warnings.warn("This method will be removed in a future version. Please update code to use "
                      "`query_wfs_urls` instead.", DeprecationWarning)
        return self._wfs_broker.query_files(layer, **kwargs)

    def query_wfs_file_exists(self, layer, name):  # pragma: no cover
        """Returns a bool representing whether a given 'file_url' is present in a layer

        :param layer: layer name supplied to GetFeature typename parameter
        :param name: 'file_url' inserted into OGC filter, and supplied to GetFeature filter parameter
        :return: whether the given file is present in the layer
        """
        return self._wfs_broker.query_file_exists(layer, name)

    # Storage methods
    def download(self, remotepipelinefilecollection, local_path):
        """Helper method to download a RemotePipelineFileCollection or RemotePipelineFile

        :param remotepipelinefilecollection: RemotePipelineFileCollection to download
        :param local_path: local path where files will be downloaded.
        :return: None
        """
        self._storage_broker.download(remotepipelinefilecollection, local_path)

    def query_storage(self, query):  # pragma: no cover
        """Query the storage backend and return existing files matching the given query

        :param query: S3-style prefix for filtering query results
        :return: RemotePipelineFileCollection of files matching the prefix
        """
        return self._storage_broker.query(query)

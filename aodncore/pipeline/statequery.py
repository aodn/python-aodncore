class StateQuery(object):
    """Simple state query interface, to provide user friendly access for querying existing Pipeline state
    """

    def __init__(self, broker):
        self._broker = broker

    def query_storage(self, query):
        """Query the storage backend and return existing files matching the given query

        :param query: S3-style prefix for filtering query results
        :return: dict containing the query results
        """
        return self._broker.query(query)

    def query_wfs(self, query):
        raise NotImplementedError

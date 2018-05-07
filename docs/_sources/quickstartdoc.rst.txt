Query Storage
=============

Query the existing pipeline storage for files matching a string::

    from aodncore.pipeline import HandlerBase
    from aodncore.pipeline.config import CONFIG

    class MyHandler(HandlerBase):
        def preprocess(self):
            # query the storage for all objects with a given prefix
            prefix = 'Department_of_Defence/DSTG/slocum_glider/Perth'

            results = self.state_query.query_storage(prefix)

            for filename, metadata in results.iteritems():
                # test the results somehow, usually  to see if there is a "hit" for a particular file
                print(filename)
                print(metadata)

            Department_of_Defence/DSTG/slocum_glider/PerthCanyonB20140213/PerthCanyonB20140213.kml
            {'last_modified': datetime.datetime(2016, 4, 27, 2, 30, 8, tzinfo=tzutc()), 'size': 21574}
            Department_of_Defence/DSTG/slocum_glider/PerthCanyonB20140213/PerthCanyonB20140213_TEMP.jpg
            {'last_modified': datetime.datetime(2016, 4, 27, 2, 30, 8, tzinfo=tzutc()), 'size': 132122}

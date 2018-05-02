Query Storage
=============

Query the existing pipeline storage for files matching a string::

    from aodncore.pipeline import HandlerBase
    from aodncore.pipeline.config import CONFIG

    class MyHandler(HandlerBase):
        pass

    h = MyHandler('/path/to/input/file.nc', config=CONFIG, upload_path='/original/incoming/path/file.nc')
    h.run()
    sq = handler.state_query
    results = sq.query_storage('Department_of_Defence/DSTG/slocum_glider/Perth')

    for filename, metadata in results.iteritems():
        #do something
        print(filename)
        print(metadata)

    Department_of_Defence/DSTG/slocum_glider/PerthCanyonB20140213/PerthCanyonB20140213.kml
    {'last_modified': datetime.datetime(2016, 4, 27, 2, 30, 8, tzinfo=tzutc()), 'size': 21574}
    Department_of_Defence/DSTG/slocum_glider/PerthCanyonB20140213/PerthCanyonB20140213_TEMP.jpg
    {'last_modified': datetime.datetime(2016, 4, 27, 2, 30, 8, tzinfo=tzutc()), 'size': 132122}

Query Storage
=============

Query the existing pipeline storage for files matching a string::

    from aodncore.pipeline import HandlerBase
    from aodncore.pipeline.config import CONFIG

    class MyHandler(HandlerBase):
        def preprocess(self):
            results = self.state_query.query_storage('Department_of_Defence/DSTG/slocum_glider/Perth')
            # test the results somehow, usually  to see if there is a "hit" for a particular file

    h = MyHandler('/path/to/input/file.nc', config=CONFIG, upload_path='/original/incoming/path/file.nc')
    h.run()


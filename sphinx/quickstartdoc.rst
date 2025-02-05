Writing a :meth:`dest_path` function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The 'dest_path' (destination path) function is responsible for generating the path 'key' at
which a file will be published. This key is used by both the harvesting process and the upload
process, and is one of the most important elements of a handler.

* Writing a :meth:`dest_path` function with an unmodified filename::

    import os

    class MyHandler(HandlerBase):
        def dest_path(self, file_path):
            basename = os.path.basename(file_path)
            dest_filename = "IMOS_filename_01_XX_{basename}".format(basename=basename)
            return os.path.join('IMOS/MYFACILITY', dest_filename)

* Writing a :meth:`dest_path` function based on contents of a NetCDF file::

    import os
    from netCDF4 import Dataset
    from aodncore.pipeline import HandlerBase

    class MyHandler(HandlerBase):
        def dest_path(self, file_path):
            with Dataset(file_path, mode='r') as d:
                site_code = d.site_code

            dest_filename = "IMOS_filename_00_{site_code}.nc".format(site_code=site_code)
            return os.path.join('IMOS/MYFACILITY', dest_filename)

* Writing a :meth:`dest_path` function which is *external* to the handler class::

    import os

    def dest_path_external(file_path):
        return os.path.join("IMOS/DUMMY/{basename}".format(basename=os.path.basename(file_path))


    class MyHandler(HandlerBase):
        pass

    handler = MyHandler('/path/to/input/file', dest_path_function=dest_path_external)

.. note:: Decoupling the :meth:`dest_path` function from the handler means the same handler class
    can be used for multiple pipelines and act as a generic handler where calculating the destination
    path is the only point of difference between them, to save duplicating code.

Overriding default file actions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Set all '.txt' files to UPLOAD_ONLY publish type in the :meth:`preprocess` step::

    from aodncore.pipeline import HandlerBase, PipelineFilePublishType

    class MyHandler(HandlerBase):
        def preprocess(self):
            # use of filter_* and set_* methods reduces excessive nesting of 'if' and 'for'
            # statements
            txt_files = self.file_collection.filter_by_attribute_value('extension', '.txt')
            txt_files.set_publish_type(PipelineFilePublishType.UPLOAD_ONLY)

        def preprocess(self):
            # functionally equivalent to the above example, but with unnecessary indentation
            # and explicit looping
            for pf in txt_files:
                if pf.extension == '.txt':
                    pf.publish_type = PipelineFilePublishType.UPLOAD_ONLY


* Do not perform any checks on PDF (.pdf) files::

    from aodncore.pipeline import FileType, HandlerBase, PipelineFilePublishType

    class MyHandler(HandlerBase):
        def preprocess(self):
            # 'known' file types may be filtered by their type rather than by their extension
            # string attribute
            pdf_files = self.file_collection.filter_by_attribute_id('file_type', FileType.PDF)
            pdf_files.set_check_types(PipelineFileCheckType.NO_ACTION)

Creating products during the handler lifetime
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Create a simple product during the :meth:`preprocess` step and add to the file collection::

    import os
    from aodncore.pipeline import HandlerBase, PipelineFile, PipelineFilePublishType

    class MyHandler(HandlerBase):
        def preprocess(self):
            # create the product
            product_path = os.path.join(self.products_dir, 'product.txt')
            with open(product_path, 'w') as f:
                f.write('some file contents' + os.linesep)

            # create a PipelineFile to represent the product file, set it's 'publish type'
            # attribute and add it to the handler's file collection
            product = PipelineFile(product_path)
            product.publish_type = PipelineFilePublishType.UPLOAD_ONLY
            self.collection.add(product)

Query Storage
~~~~~~~~~~~~~

Query the existing pipeline storage for files matching a string::

    from aodncore.pipeline import HandlerBase
    from aodncore.pipeline.config import CONFIG

    class MyHandler(HandlerBase):
        def preprocess(self):
            prefix = 'Department_of_Defence/DSTG/slocum_glider/Perth'
            file_to_check = 'Department_of_Defence/DSTG/slocum_glider/PerthCanyonB20140213/PerthCanyonB20140213.kml'

            # query the storage for all objects with a given prefix
            results = self.state_query.query_storage(prefix)

            # test for a given dest_path to conditional run some code (e.g. replace/delete a
            # previous version which had a different dest_path)
            if file_to_check in results:
                pass

            # iterate over the results
            for filename, metadata in results.iteritems():
                print(filename)
                print(metadata)

    Department_of_Defence/DSTG/slocum_glider/PerthCanyonB20140213/PerthCanyonB20140213.kml
    {'last_modified': datetime.datetime(2016, 4, 27, 2, 30, 8, tzinfo=tzutc()), 'size': 21574}
    Department_of_Defence/DSTG/slocum_glider/PerthCanyonB20140213/PerthCanyonB20140213_TEMP.jpg
    {'last_modified': datetime.datetime(2016, 4, 27, 2, 30, 8, tzinfo=tzutc()), 'size': 132122}

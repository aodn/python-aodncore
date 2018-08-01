from aodncore.pipeline import HandlerBase, PipelineFilePublishType


class DummyHandler(HandlerBase):
    """This is an example handler, used for testing and also to demonstrate some of the kinds of things that each step
        might perform.
        
        Bear in mind that this inherits everything from the HandlerBase, but only some of the things are suitable to be
        overridden in this child class.
      
        Inherited instance members that will nearly always be relevant:
        
        * self.file_collection - the main collection of files, which is a PipelineFileCollection object whose elements
                                are all PipelineFile instances (see handlerlib.files to see what these are)
        
        * self.logger - log things!

        Inherited
        Things that you would typically implement here:
        
        * dest_path static method to 
        * preprocess 
        * process
        * postprocess methods
    
    """

    def __init__(self, *args, **kwargs):
        super(DummyHandler, self).__init__(*args, **kwargs)
        self.default_addition_publish_type = PipelineFilePublishType.HARVEST_ARCHIVE_UPLOAD
        self.opendap_root = self.config.pipeline_config['global'].get('opendap_root')

    @staticmethod
    def dest_path(filename):
        """This method is optional, but if it is defined, it will be used to determine the destination path for a given
        input path. This could be based purely on the filename, or you could import the netcdf library and open the file
        and work it out based on the contents, or of course any other way you want to determine it
        :param filename: individual input file
        :return: destination path for the input file
        """
        dest_path = "DUMMY/path/renamed-from-handler-method-{name}".format(name=filename)
        return dest_path

    def preprocess(self):
        """Here you can run code that needs to run before the compliance checker step. This might be where you specify
        which files in the "eligible_files" list are "UPLOAD_ONLY", or not published at all 
        
        :return: None
        """
        self.logger.info("Running preprocess from child class")

    def process(self):
        """Here you can run code that needs to run *after* the compliance checker step but *before* the publishing step.

        :return: None
        """
        self.logger.info("Running process from child class")

    def postprocess(self):
        """Here you can run code that needs to run *after* the publishing step but *before* the notify step

        :return: None
        """
        self.logger.info("Running postprocess from child class")

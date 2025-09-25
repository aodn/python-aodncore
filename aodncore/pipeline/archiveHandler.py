from aodncore.pipeline import HandlerBase, PipelineFileCheckType, PipelineFileCollection, PipelineFilePublishType


class ArchiveHandler(HandlerBase):
    """This handler does nothing other than archiving the incoming file.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_addition_publish_type = PipelineFilePublishType.NO_ACTION
        self.archive_input_file = True

    # def preprocess(self):
    #     """Here you can run code that needs to run before the compliance checker step. This might be where you specify
    #     which files in the "eligible_files" list are "UPLOAD_ONLY", or not published at all
    #
    #     :return: None
    #     """
    #     self.logger.info("Running preprocess from child class")
    #     (
    #         PipelineFileCollection(f for f in self.file_collection if f.publish_type.is_addition_type)[1:]
    #             .filter_by_attribute_id('check_type', PipelineFileCheckType.UNSET)
    #             .set_check_types(PipelineFileCheckType.NO_ACTION)
    #     )

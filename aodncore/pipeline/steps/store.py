"""This module provides the step runner class for the "store" step, which is a sub-step of the :ref:`publish` step.

The step runner delegates the low level storage operations to an internal :py:class:`BaseStorageBroker` instance, and so
it's primary purpose is to abstract the storage operations from the :py:class:`HandlerBase` by providing an interface
similar to the other handler steps.
"""

from .basestep import BaseStepRunner
from ..files import validate_pipelinefilecollection
from ..storage import get_storage_broker

__all__ = [
    'get_store_runner',
    'StoreRunner'

]


def get_store_runner(store_base_url, config, logger, archive_mode=False):
    """Factory function to return store runner class, with it's storage broker based on URL scheme

    :param store_base_url: URL base for storage location
    :param config: LazyConfigManager instance
    :param logger: Logger instance
    :param archive_mode: flag to indicate archive
    :return: StoreRunner instance
    """
    broker = get_storage_broker(store_base_url)
    return StoreRunner(broker, config, logger, archive_mode)


class StoreRunner(BaseStepRunner):
    def __init__(self, broker, config, logger, archive_mode=False):
        super(StoreRunner, self).__init__(config, logger)
        self.broker = broker
        self.archive_mode = archive_mode

    @property
    def is_stored_attr(self):
        """PipelineFile attribute to flag completion of upload operation

        :return: bool
        """
        return 'is_archived' if self.archive_mode else 'is_stored'

    @property
    def pending_addition_attr(self):
        return 'pending_archive' if self.archive_mode else 'pending_store_addition'

    @property
    def dest_path_attr(self):
        """PipelineFile attribute containing the destination path

        :return: bool
        """
        return 'archive_path' if self.archive_mode else 'dest_path'

    def set_is_overwrite(self, pipeline_files):
        """Set the "is_overwrite" attribute for each file in the given collection

        :param pipeline_files: collection to
        :return: None
        """
        validate_pipelinefilecollection(pipeline_files)

        self.broker.set_is_overwrite(pipeline_files=pipeline_files, dest_path_attr=self.dest_path_attr)

    def run(self, pipeline_files):
        """Execute the pending storage operation(s) for each file in the given collection

        :param pipeline_files: PipelineFileCollection instance
        :return: None
        """
        validate_pipelinefilecollection(pipeline_files)

        additions = pipeline_files.filter_by_bool_attribute(self.pending_addition_attr)
        if additions:
            self.broker.upload(pipeline_files=additions, is_stored_attr=self.is_stored_attr,
                               dest_path_attr=self.dest_path_attr)

        deletions = pipeline_files.filter_by_bool_attribute('pending_store_deletion')
        if deletions:
            self.broker.delete(pipeline_files=deletions, is_stored_attr='is_stored', dest_path_attr=self.dest_path_attr)

        undo_deletions = pipeline_files.filter_by_bool_attribute('pending_undo')
        if undo_deletions:
            self.broker.delete(pipeline_files=undo_deletions, is_stored_attr='is_upload_undone',
                               dest_path_attr=self.dest_path_attr)

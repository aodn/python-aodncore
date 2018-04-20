"""This module provides the step runner classes for the "upload" step, which is a sub-step of the :ref:`publish` step.

    Uploading/deleting is performed by an :py:class:`StoreRunner` class.

    The classes perform the role of a general storage "broker", by abstracting the
    details of interacting with a given storage protocol, while exposing a generic interface to the handler class. The
    handler class itself then only needs to understand the concept of "uploading" and "deleting" without needing to
    understand the differences between interacting with a local directory or with S3, for example.

    The most common use of this step is to upload files to S3 after they have been checked and harvested.
"""

from .basestep import BaseStepRunner
from ..storage import get_storage_broker

__all__ = [
    'get_upload_runner',
    'UploadRunner'

]


def get_upload_runner(upload_base_url, config, logger, archive_mode=False):
    """Factory function to return uploader class, with it's storage broker based on URL scheme

    :param upload_base_url: URL base
    :param config: :py:class:`LazyConfigManager` instance
    :param logger: :py:class:`Logger` instance
    :param archive_mode: :py:class:`bool` flag to indicate archive
    :return: :py:class:`BaseUploadRunner` class
    """
    broker = get_storage_broker(upload_base_url, config, logger, archive_mode)
    return UploadRunner(broker, config, logger)


class UploadRunner(BaseStepRunner):
    def __init__(self, broker, config, logger):
        super(UploadRunner, self).__init__(config, logger)
        self.broker = broker

    def set_is_overwrite(self, pipeline_files):
        self.broker.set_is_overwrite(pipeline_files)

    def run(self, pipeline_files):
        self.broker.run(pipeline_files)

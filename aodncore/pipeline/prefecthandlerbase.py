import logging
import os

from aodncore.pipeline import HandlerBase, FileType
from aodncore.pipeline.exceptions import InvalidInputFileError
from aodncore.pipeline.handlerbase import FALLBACK_LOG_LEVEL, FALLBACK_LOG_FORMAT
from aodncore.pipeline.log import get_pipeline_logger


class PrefectHandlerBase(HandlerBase):

    def _set_input_file_attributes(self):
        """ Override HandlerBase"""

        try:
            self._file_checksum = self.etag
        except (IOError, OSError) as e:
            self.logger.exception(e)
            raise InvalidInputFileError(e)
        self.logger.sysinfo("get_file_checksum -> '{self.file_checksum}'".format(self=self))

        self._file_basename = os.path.basename(self.input_file)
        self.logger.sysinfo("file_basename -> '{self._file_basename}'".format(self=self))
        _, self._file_extension = os.path.splitext(self.input_file)
        self.logger.sysinfo("file_extension -> '{self._file_extension}'".format(self=self))
        self._file_type = FileType.get_type_from_extension(self.file_extension)
        self.logger.sysinfo("file_type -> {self._file_type}".format(self=self))

    def init_logger(self, logger_function):
        self._init_logger(logger_function)

    def _init_logger(self, logger_function):

        logger = get_pipeline_logger(None, logger_function=logger_function)

        # turn down logging for noisy libraries to WARN, unless overridden in pipeline config 'liblevel' key
        liblevel = getattr(self.config, 'pipeline_config', {}).get('logging', {}).get('liblevel', 'WARN')
        for lib in ('botocore', 'paramiko', 's3transfer', 'transitions'):
            logging.getLogger(lib).setLevel(liblevel)

        self._logger = logger
        self._celery_task_id = None
        self._celery_task_name = 'NO_TASK'

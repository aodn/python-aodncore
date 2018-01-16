from ..common.exceptions import AodnBaseError

__all__ = [
    'PipelineProcessingError',
    'PipelineSystemError',
    'ComplianceCheckFailedError',
    'DuplicatePipelineFileError',
    'FileDeleteFailedError',
    'FileUploadFailedError',
    'HandlerAlreadyRunError',
    'InvalidCheckSuiteError',
    'InvalidFileContentError',
    'InvalidFileFormatError',
    'InvalidFileNameError',
    'InvalidCheckTypeError',
    'InvalidConfigError',
    'InvalidPathFunctionError',
    'InvalidHandlerError',
    'InvalidHarvesterError',
    'InvalidInputFileError',
    'InvalidRecipientError',
    'InvalidUploadUrlError',
    'MissingConfigParameterError',
    'MissingFileError',
    'NotificationFailedError'
]


class PipelineProcessingError(AodnBaseError):
    """Base class for all exceptions which indicate that there was a problem processing the file as opposed to an
    internal configuration or environmental error. Handler classes should typically raise exceptions based on this
    exception to signal non-compliance of the file or some other *user correctable* problem.
    """
    pass


class PipelineSystemError(AodnBaseError):
    """Base class for all exceptions *not* related to file processing and which would typically *not* be suitable to
    return to an end user
    """
    pass


# Processing errors

class ComplianceCheckFailedError(PipelineProcessingError):
    pass


class InvalidFileNameError(PipelineProcessingError):
    pass


class InvalidFileContentError(PipelineProcessingError):
    pass


class InvalidFileFormatError(PipelineProcessingError):
    pass


# System errors

class DuplicatePipelineFileError(PipelineSystemError):
    pass


class FileDeleteFailedError(PipelineSystemError):
    pass


class FileUploadFailedError(PipelineSystemError):
    pass


class HandlerAlreadyRunError(PipelineSystemError):
    pass


class InvalidCheckSuiteError(PipelineSystemError):
    pass


class InvalidCheckTypeError(PipelineSystemError):
    pass


class InvalidConfigError(PipelineSystemError):
    pass


class InvalidHandlerError(PipelineSystemError):
    pass


class InvalidHarvesterError(PipelineSystemError):
    pass


class InvalidInputFileError(PipelineSystemError):
    pass


class InvalidPathFunctionError(PipelineSystemError):
    pass


class InvalidRecipientError(PipelineSystemError):
    pass


class InvalidUploadUrlError(PipelineSystemError):
    pass


class MissingConfigParameterError(PipelineSystemError):
    pass


class MissingFileError(PipelineSystemError):
    pass


class NotificationFailedError(PipelineSystemError):
    pass

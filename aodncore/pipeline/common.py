"""This module contains common "types" (typically enums or simple data classes) used by more than one module in the
    project.
"""

import os

from enum import Enum

from ..util import classproperty, is_valid_email_address, iter_public_attributes, validate_membership, validate_type

__all__ = [
    'CheckResult',
    'FileType',
    'HandlerResult',
    'NotificationRecipientType',
    'PipelineFileCheckType',
    'PipelineFilePublishType',
    'validate_addition_publishtype',
    'validate_deletion_publishtype',
    'validate_checkresult',
    'validate_checktype',
    'validate_publishtype',
    'validate_recipienttype',
    'validate_settable_checktype'
]


class CheckResult(object):
    """Simple container class to hold a check result
    """

    def __init__(self, compliant, log, errors=False):
        self._compliant = compliant
        self._log = log
        self._errors = errors

    def __iter__(self):
        return iter_public_attributes(self)

    @property
    def compliant(self):
        return self._compliant

    @property
    def log(self):
        return self._log

    @property
    def errors(self):
        return self._errors


class HandlerResult(Enum):
    UNKNOWN = 0
    SUCCESS = 1
    ERROR = 2


class NotificationRecipientType(Enum):
    """Notification recipient type
    """
    INVALID = ('invalid', lambda p: False, 'invalid protocol')
    EMAIL = ('email', is_valid_email_address, 'invalid email address')
    SNS = ('sns', lambda p: True, 'invalid SNS topic')

    def __init__(self, protocol, address_validation_function, error_string):
        """

        :param protocol: protocol string used to map string representation of a protocol to an Enum element
        :param address_validation_function: function used to validate the address component (must accept the address
                as a parameter and return a bool representing whether it is valid for the given protocol)
        :param error_string: descriptive error string used when the validation function fails (returns False)
        """
        self._protocol = protocol
        self._address_validation_function = address_validation_function
        self._error_string = error_string

    @property
    def protocol(self):
        return self._protocol

    @property
    def address_validation_function(self):
        return self._address_validation_function

    @property
    def error_string(self):
        return self._error_string

    @classmethod
    def get_type_from_protocol(cls, protocol):
        return next((t for t in cls if t.protocol == protocol), cls.INVALID)


class FileType(Enum):
    """Represents each known file type, including extension and mime type

    Each enum member may have it's attributes accessed by name when required for comparisons and filtering, e.g.

    - lookup the extension for PNG file types in general::

        FileType.PNG.extension
        '.png'

    - assign a type attribute to an object, and query the type-specific values directly from the object::

        class Object(object):
            pass

        o = Object()
        o.file_type = FileType.ZIP
        o.file_type.extension
        '.zip'
        o.file_type.mime_type
        'application/zip'
    """
    __slots__ = ('extension', 'mime_type')

    UNKNOWN = ()

    CSV = ('.csv', 'text/csv')
    GZIP = ('.gz', 'application/gzip')
    JPEG = ('.jpg', 'image/jpeg')
    PDF = ('.pdf', 'application/pdf')
    PNG = ('.png', 'image/png')
    ZIP = ('.zip', 'application/zip')

    NETCDF = ('.nc', 'application/octet-stream')
    DIR_MANIFEST = ('.dir_manifest', 'text/plain')
    MAP_MANIFEST = ('.map_manifest', 'text/plain')
    RSYNC_MANIFEST = ('.rsync_manifest', 'text/plain')
    SIMPLE_MANIFEST = ('.manifest', 'text/plain')

    def __init__(self, extension=None, mime_type=None):
        self.extension = extension
        self.mime_type = mime_type

    # noinspection PyTypeChecker
    @classmethod
    def get_type_from_extension(cls, extension):
        return next((t for t in cls if t.extension == extension), cls.UNKNOWN)

    @classmethod
    def get_type_from_name(cls, name):
        _, extension = os.path.splitext(name)
        return cls.get_type_from_extension(extension)


class PipelineFileCheckType(Enum):
    """Each :py:class:`PipelineFile` may individually specify which checks are performed against it
    """
    UNSET = 0
    NO_ACTION = 1
    NONEMPTY_CHECK = 2
    FORMAT_CHECK = 3
    NC_COMPLIANCE_CHECK = 4

    # noinspection PyMethodParameters,PyTypeChecker
    @classproperty
    def all_settable_types(cls):
        return {t for t in cls if t != cls.UNSET}

    # noinspection PyMethodParameters,PyTypeChecker
    @classproperty
    def all_checkable_types(cls):
        return {t for t in cls if t not in (cls.UNSET, cls.NO_ACTION)}


class PipelineFilePublishType(Enum):
    """Each :py:class:`PipelineFile` may individually specify which combination of archive/upload/harvest actions must
    occur before it is considered "published"

    Enum member values are a tuple containing boolean flags used for querying/validating types, which are provided
    to the :py:meth:`__init__` for each member (since confusingly, each :py:class:`Enum` member is an *instance* of this
    class).

    Each member's valid is therefore a tuple of :py:class:`bool` values representing the following flags::

        (is_addition_type, is_deletion_type, is_archive_type, is_store_type, is_harvest_type)

    """
    __slots__ = ('_is_addition_type', '_is_deletion_type', '_is_archive_type', '_is_store_type', '_is_harvest_type')

    # initial type, used as a sentinel value to denote files which haven't been assigned a type
    UNSET = (None, None, None, None, None)

    # valid for both addition and deletion types
    NO_ACTION = (True, True, False, False, False)

    # valid addition types
    ARCHIVE_ONLY = (True, False, True, False, False)
    UPLOAD_ONLY = (True, False, False, True, False)
    HARVEST_ONLY = (True, False, False, False, True)
    HARVEST_ARCHIVE = (True, False, True, False, True)
    HARVEST_UPLOAD = (True, False, False, True, True)
    HARVEST_ARCHIVE_UPLOAD = (True, False, True, True, True)

    # valid deletion types
    UNHARVEST_ONLY = (False, True, False, False, True)
    DELETE_ONLY = (False, True, False, True, False)
    DELETE_UNHARVEST = (False, True, False, True, True)

    def __init__(self, is_addition_type, is_deletion_type, is_archive_type, is_store_type, is_harvest_type):
        self._is_addition_type = is_addition_type
        self._is_deletion_type = is_deletion_type
        self._is_archive_type = is_archive_type
        self._is_store_type = is_store_type
        self._is_harvest_type = is_harvest_type

    @property
    def is_addition_type(self):
        return self._is_addition_type

    @property
    def is_deletion_type(self):
        return self._is_deletion_type

    @property
    def is_archive_type(self):
        return self._is_archive_type

    @property
    def is_store_type(self):
        return self._is_store_type

    @property
    def is_harvest_type(self):
        return self._is_harvest_type

    # noinspection PyMethodParameters,PyTypeChecker
    @classproperty
    def all_addition_types(cls):
        return {t for t in cls if t.is_addition_type}

    # noinspection PyMethodParameters,PyTypeChecker
    @classproperty
    def all_deletion_types(cls):
        return {t for t in cls if t.is_deletion_type}


validate_addition_publishtype = validate_membership(PipelineFilePublishType.all_addition_types)
validate_checkable_checktype = validate_membership(PipelineFileCheckType.all_checkable_types)
validate_checkresult = validate_type(CheckResult)
validate_checktype = validate_membership(PipelineFileCheckType)
validate_deletion_publishtype = validate_membership(PipelineFilePublishType.all_deletion_types)
validate_recipienttype = validate_membership(NotificationRecipientType)
validate_publishtype = validate_membership(PipelineFilePublishType)
validate_settable_checktype = validate_membership(PipelineFileCheckType.all_settable_types)

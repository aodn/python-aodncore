import abc
import mimetypes
import os
import warnings
from collections import Counter, MutableSet, OrderedDict

from .common import (FileType, PipelineFilePublishType, PipelineFileCheckType, validate_addition_publishtype,
                     validate_checkresult, validate_deletion_publishtype, validate_publishtype,
                     validate_settable_checktype)
from .exceptions import AttributeValidationError, DuplicatePipelineFileError, MissingFileError
from .schema import validate_check_params
from ..util import (IndexedSet, ensure_regex_list, format_exception, get_file_checksum, iter_public_attributes,
                    matches_regexes, rm_f, slice_sequence, validate_bool, validate_callable, validate_int,
                    validate_mapping, validate_nonstring_iterable, validate_regexes, validate_relative_path_attr,
                    validate_string, validate_type)

__all__ = [
    'PipelineFileCollection',
    'PipelineFile',
    'RemotePipelineFile',
    'RemotePipelineFileCollection',
    'ensure_pipelinefilecollection',
    'ensure_remotepipelinefilecollection',
    'validate_pipelinefilecollection',
    'validate_pipelinefile_or_pipelinefilecollection',
    'validate_pipelinefile_or_string'
]


def ensure_pipelinefilecollection(o):
    """Function to accept either a single PipelineFile OR a PipelineFileCollection and ensure that a
    PipelineFileCollection object is returned in either case

    :param o: PipelineFile or PipelineFileCollection object
    :return: PipelineFileCollection object
    """
    validate_pipelinefile_or_pipelinefilecollection(o)
    return o if isinstance(o, PipelineFileCollection) else PipelineFileCollection(o)


def ensure_remotepipelinefilecollection(o):
    """Function to accept either a single RemotePipelineFile OR a RemotePipelineFileCollection and ensure that a
    RemotePipelineFileCollection object is returned in either case

    :param o: PipelineFile or PipelineFileCollection object
    :return: PipelineFileCollection object
    """
    validate_remotepipelinefile_or_remotepipelinefilecollection(o)
    return o if isinstance(o, RemotePipelineFileCollection) else RemotePipelineFileCollection(o)


class PipelineFileBase(object, metaclass=abc.ABCMeta):
    """A base class to represent a "pipeline file", which consists of a local path and a remote "destination path"
    """
    __slots__ = ['_file_checksum', '_dest_path', '_local_path', '_extension', '_name', 'file_type']

    def __init__(self, local_path, dest_path=None):
        self._local_path = local_path
        self._dest_path = dest_path

        self._name = None
        self._file_checksum = None

        self._set_local_file_attributes()

    def _set_local_file_attributes(self):
        if self.local_path:
            _, self._extension = os.path.splitext(self.local_path)
            self.file_type = FileType.get_type_from_extension(self._extension)
        else:
            self._extension = None
            self.file_type = FileType.UNKNOWN

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self._key() == other._key()
        return False

    def __hash__(self):
        return hash(self._key())

    @abc.abstractmethod
    def _key(self):
        raise NotImplementedError

    def __iter__(self):
        return iter_public_attributes(self)

    def __repr__(self):  # pragma: no cover
        return "{name}({repr})".format(name=self.__class__.__name__, repr=repr(dict(self)))

    def __str__(self):
        return "{name}({str})".format(name=self.__class__.__name__, str=dict(self))

    #
    # Static properties (read-only, should never change during the lifecycle of the object)
    #
    @property
    def extension(self):
        return self._extension

    @property
    def file_checksum(self):
        if self._file_checksum is None:
            try:
                self._file_checksum = get_file_checksum(self._local_path)
            except (IOError, OSError) as e:
                raise MissingFileError(
                    "failed to determine checksum for RemoteFile '{local_path}'. {e}".format(
                        local_path=self._local_path,
                        e=format_exception(e)))
        return self._file_checksum

    @property
    def name(self):
        return self._name

    @property
    def local_path(self):
        return self._local_path

    @property
    def dest_path(self):
        return self._dest_path


class RemotePipelineFile(PipelineFileBase):
    """Implementation of PipelineFileBase to represents a single *remote* file. This is used to provide a common
        interface for a remote file, to facilitate querying and downloading operations where the *current* state
        of the storage is relevant information
    """
    __slots__ = ['_last_modified', '_size']

    def __init__(self, dest_path, local_path=None, name=None, last_modified=None, size=None):
        super().__init__(local_path, dest_path)
        self._name = name if name is not None else os.path.basename(dest_path)
        self._last_modified = last_modified
        self._size = size

    def __getattr__(self, item):
        # backwards compatibility for code expecting items to be a key/value pair like "(dest_path, metadata_dict)"
        return getattr(self.dest_path, item)

    def __getitem__(self, item):
        # backwards compatibility for code expecting items to be a key/value pair like "(dest_path, metadata_dict)"
        return self.dest_path.__getitem__(item)

    @classmethod
    def from_pipelinefile(cls, pipeline_file):
        """Construct a RemotePipelineFile instance from an existing PipelineFile instance

        Note: the local_path is deliberately *not* set in the new instance, primarily to avoid accidental overwriting of
        local files in case of conversion to a remote file and having the remote file downloaded. The local_path
        attribute may still be set, but it is an explicit operation.

        :param pipeline_file: PipelineFile instance used to instantiate a RemotePipelineFile instance
        :return: RemotePipelineFile instance
        """
        return cls(dest_path=pipeline_file.dest_path, local_path=None, name=pipeline_file.name)

    def _key(self):
        return self.name, self.dest_path

    @property
    def file_checksum(self):
        # override superclass property to not attempt to checksum the file if it has no local path
        # (i.e. has not been downloaded)
        if self.local_path is None:
            return None
        return super().file_checksum

    @property
    def last_modified(self):
        return self._last_modified

    @property
    def size(self):
        return self._size

    @PipelineFileBase.local_path.setter
    def local_path(self, local_path):
        self._local_path = local_path
        # reset file_checksum to None, so that it will be re-evaluated lazily if required
        self._file_checksum = None
        self._set_local_file_attributes()

    def remove_local(self):
        _local_path = self.local_path
        self.local_path = None
        rm_f(_local_path)


class PipelineFile(PipelineFileBase):
    """Represents a single file in order to store state information relating to the intended actions to be performed
    on the file, and the actions that *were* performed on the file

    :param local_path: absolute source path to the file being represented
    :type local_path: :py:class:`str`
    :param name: arbitrary name (defaults to the output of :py:func:`os.path.basename` on local_path)
    :type name: :py:class:`str`
    :param archive_path: relative path used when archiving the file
    :type archive_path: :py:class:`str`
    :param dest_path: relative path used when publishing the file
    :type dest_path: :py:class:`str`
    :param is_deletion: flag designating whether this is a deletion
    :type is_deletion: :py:class:`bool`
    :param late_deletion: flag to indicate that this file should be deleted *after* additions are performed (note: ignored if `is_deletion=False`)
    :type late_deletion: :py:class:`bool`
    :param file_update_callback: optional callback to call when a file property is updated
    :type file_update_callback: :py:class:`callable`
    """
    __slots__ = ['_archive_path', '_file_update_callback', '_check_type', '_is_deletion', '_late_deletion',
                 '_publish_type', '_should_archive', '_should_harvest', '_should_store', '_should_undo', '_is_checked',
                 '_is_archived', '_is_harvested', '_is_overwrite', '_is_stored', '_is_harvest_undone',
                 '_is_upload_undone', '_check_result', '_mime_type']

    def __init__(self, local_path, name=None, archive_path=None, dest_path=None, is_deletion=False, late_deletion=False,
                 file_update_callback=None):
        super().__init__(local_path, dest_path)

        self._name = name if name is not None else os.path.basename(local_path)

        self._archive_path = archive_path

        self._file_update_callback = None
        if file_update_callback is not None:
            self.file_update_callback = file_update_callback

        # processing flags - these express the *intended actions* for the file
        self._check_type = PipelineFileCheckType.UNSET
        self._is_deletion = is_deletion
        self._late_deletion = late_deletion
        self._publish_type = PipelineFilePublishType.UNSET
        self._should_archive = False
        self._should_harvest = False
        self._should_store = False
        self._should_undo = False

        # status flags - these express the *current state* of the file
        self._is_checked = False
        self._is_archived = False
        self._is_harvested = False
        self._is_overwrite = None
        self._is_stored = False
        self._is_harvest_undone = False
        self._is_upload_undone = False

        self._check_result = None
        self._mime_type = None

    @classmethod
    def from_remotepipelinefile(cls, remotepipelinefile, is_deletion=False):
        """Construct a PipelineFile instance from an existing RemotePipelineFile instance

        :param remotepipelinefile: RemotePipelineFile instance used to instantiate a PipelineFile instance
        :param is_deletion: is_deletion flag passed directly to __init__
        :return: PipelineFile instance
        """
        return cls(local_path=remotepipelinefile.local_path, dest_path=remotepipelinefile.dest_path,
                   name=remotepipelinefile.name, is_deletion=is_deletion)

    def _key(self):
        return self.name, self.local_path, self.file_checksum

    @property
    def src_path(self):
        return self._local_path

    @property
    def file_checksum(self):
        # override superclass property to handle deletions (which have no local_path and therefore can't be summed)
        if self.is_deletion:
            return None
        return super().file_checksum

    #
    # State properties (may change during the lifecycle of the object to reflect the current state)
    #

    @property
    def archive_path(self):
        return self._archive_path

    @archive_path.setter
    def archive_path(self, archive_path):
        validate_relative_path_attr(archive_path, 'archive_path')
        self._archive_path = archive_path
        self._post_property_update({'archive_path': archive_path})

    @property
    def check_log(self):
        return '' if self._check_result is None else os.linesep.join(self._check_result.log)

    @property
    def check_passed(self):
        return 'N/A' if self._check_result is None else str(self._check_result.compliant)

    @property
    def check_result(self):
        return self._check_result

    @check_result.setter
    def check_result(self, check_result):
        validate_checkresult(check_result)

        self._is_checked = True
        self._check_result = check_result
        self._post_property_update({'is_checked': True})

    @property
    def check_type(self):
        return self._check_type

    @check_type.setter
    def check_type(self, check_type):
        if self.is_deletion:
            raise ValueError('deletions cannot be assigned a check_type')
        validate_settable_checktype(check_type)

        self._check_type = check_type
        self._post_property_update({'check_type': check_type.name})

    @property
    def dest_path(self):
        return self._dest_path

    @dest_path.setter
    def dest_path(self, dest_path):
        validate_relative_path_attr(dest_path, 'dest_path')
        self._dest_path = dest_path
        self._post_property_update({'dest_path': dest_path})

    @property
    def file_update_callback(self):
        return self._file_update_callback

    @file_update_callback.setter
    def file_update_callback(self, callback):
        validate_callable(callback)

        self._file_update_callback = callback

    @property
    def is_harvested(self):
        return self._is_harvested

    @is_harvested.setter
    def is_harvested(self, is_harvested):
        validate_bool(is_harvested)
        self._is_harvested = is_harvested
        self._post_property_update({'is_harvested': is_harvested})

    @property
    def is_archived(self):
        return self._is_archived

    @is_archived.setter
    def is_archived(self, is_archived):
        validate_bool(is_archived)

        self._is_archived = is_archived
        self._post_property_update({'is_archived': is_archived})

    @property
    def is_checked(self):
        return self._is_checked

    @property
    def is_deletion(self):
        return self._is_deletion

    @property
    def late_deletion(self):
        return self._late_deletion

    @property
    def is_deleted(self):
        return self.is_deletion and self.is_stored

    @property
    def is_overwrite(self):
        return self._is_overwrite

    @is_overwrite.setter
    def is_overwrite(self, is_overwrite):
        validate_bool(is_overwrite)

        self._is_overwrite = is_overwrite
        self._post_property_update({'is_overwrite': is_overwrite})

    @property
    def is_stored(self):
        return self._is_stored

    @property
    def is_harvest_undone(self):
        return self._is_harvest_undone

    @is_harvest_undone.setter
    def is_harvest_undone(self, is_harvest_undone):
        validate_bool(is_harvest_undone)

        self._is_harvest_undone = is_harvest_undone
        self._post_property_update({'is_harvest_undone': is_harvest_undone})

    @property
    def is_upload_undone(self):
        return self._is_upload_undone

    @is_upload_undone.setter
    def is_upload_undone(self, is_upload_undone):
        validate_bool(is_upload_undone)

        self._is_upload_undone = is_upload_undone
        self._post_property_update({'is_upload_undone': is_upload_undone})

    @is_stored.setter
    def is_stored(self, is_stored):
        validate_bool(is_stored)

        self._is_stored = is_stored
        self._post_property_update({'is_stored': is_stored})

    @property
    def is_uploaded(self):
        return not self.is_deletion and self.is_stored

    @property
    def mime_type(self):
        if not self._mime_type:
            self._mime_type = self.file_type.mime_type or mimetypes.types_map.get(self.extension,
                                                                                  'application/octet-stream')
        return self._mime_type

    @mime_type.setter
    def mime_type(self, mime_type):
        validate_string(mime_type)

        self._mime_type = mime_type
        self._post_property_update({'mime_type': mime_type})

    @property
    def published(self):
        stored = self.is_stored and not self.is_upload_undone
        harvested = self.is_harvested and not self.is_harvest_undone
        if self.should_store and self.should_harvest:
            published = stored and harvested
        else:
            published = stored or harvested
        return 'Yes' if published else 'No'

    @property
    def pending_archive(self):
        return self.should_archive and not self.is_archived

    @property
    def pending_harvest(self):
        return self.should_harvest and not self.is_harvested and not self.should_undo

    @property
    def pending_harvest_addition(self):
        return self.pending_harvest and not self.is_deletion

    @property
    def pending_harvest_deletion(self):
        return self.pending_harvest and self.is_deletion

    @property
    def pending_harvest_early_deletion(self):
        return self.pending_harvest and self.is_deletion and not self.late_deletion

    @property
    def pending_harvest_late_deletion(self):
        return self.pending_harvest and self.is_deletion and self.late_deletion

    @property
    def pending_harvest_undo(self):
        return self.should_undo and self.should_harvest and not self.is_harvest_undone

    @property
    def pending_store(self):
        return self.should_store and not self.is_stored and not self.should_undo

    @property
    def pending_store_addition(self):
        return self.pending_store and not self.is_deletion

    @property
    def pending_store_deletion(self):
        return self.pending_store and self.is_deletion

    @property
    def pending_store_undo(self):
        return self.should_undo and self.should_store and not self.is_upload_undone

    @property
    def pending_undo(self):
        return self.pending_harvest_undo or self.pending_store_undo

    @property
    def publish_type(self):
        return self._publish_type

    @publish_type.setter
    def publish_type(self, publish_type):
        """Publish type is a special property which allows handler sub-classes to specify in a single line what
        "publishing" actions should be performed on a given file. For this reason, the boolean flags are read-only
        properties and only intended to be changed via this property.
        
        :param publish_type: an element of the PipelineFilePublishType enum
        :return: None
        """
        validate_publishtype(publish_type)

        validate_value_func = validate_deletion_publishtype if self.is_deletion else validate_addition_publishtype
        validate_value_func(publish_type)

        self._should_archive = publish_type.is_archive_type
        self._should_harvest = publish_type.is_harvest_type
        self._should_store = publish_type.is_store_type

        self._publish_type = publish_type
        self._post_property_update({'publish_type': publish_type.name})

    @property
    def should_archive(self):
        return self._should_archive

    @property
    def should_store(self):
        return self._should_store

    @property
    def should_harvest(self):
        return self._should_harvest

    @property
    def should_undo(self):
        return self._should_undo

    @should_undo.setter
    def should_undo(self, should_undo):
        validate_bool(should_undo)

        if self.is_deletion:
            raise ValueError('undo is not possible for deletions')

        self._should_undo = should_undo
        self._post_property_update({'should_undo': should_undo})

    def _post_property_update(self, properties, include_values=True):
        """Method run after a property is updated in order to perform optional actions such as updating ORM (if enabled)
            and running the update callback (if set)

        :param properties: dict containing the updated properties and their new values (used to update ORM)
        :return: None
        """
        validate_mapping(properties)

        if self.file_update_callback:
            log_output = properties if include_values else list(properties.keys())
            self.file_update_callback(name=self.name, is_deletion=self.is_deletion,
                                      message="{properties}".format(properties=log_output))


class PipelineFileCollectionBase(MutableSet):
    """A collection base class which implements the MutableSet abstract base class to allow clean set operations, but
    limited to containing only :py:class:`PipelineFile` or :py:class:`RemotePipelineFile`elements and providing specific
    functionality for handling a collection of them (e.g. filtering, generating tabular data, etc.)

    :param data: data to add during initialisation of the collection, either a single :py:class:`PipelineFile` or file
        path, or an :py:class:`Iterable` whose elements are :py:class:`PipelineFile` instances or file paths
    :param validate_unique: :py:class:`bool` passed to the `add` method
    :type data: :py:class:`PipelineFile`, :py:class:`RemotePipelineFile`, :py:class:`str`, :py:class:`Iterable`
    """
    __slots__ = ['_s', 'member_class', 'member_validator', 'member_from_string_method', 'unique_attributes']

    def __init__(self, data=None, validate_unique=True, member_class=PipelineFile, member_validator=None,
                 member_from_string_method=None, unique_attributes=()):
        super().__init__()

        self._s = IndexedSet()

        self.member_class = member_class
        self.member_validator = member_validator or validate_pipelinefile_or_string
        self.member_from_string_method = getattr(self, member_from_string_method) or self.get_pipelinefile_from_src_path
        self.unique_attributes = unique_attributes

        if data is not None:
            if isinstance(data, (self.member_class, str)):
                data = [data]
            for f in data:
                self.add(f, validate_unique=validate_unique)

    def __bool__(self):
        return bool(self._s)

    def __contains__(self, v):
        return v in self._s

    def __getitem__(self, index):
        result = self._s[index]
        return self.__class__(result) if isinstance(result, IndexedSet) else result

    def __iter__(self):
        return iter(self._s)

    def __len__(self):
        return len(self._s)

    def __repr__(self):  # pragma: no cover
        return "{name}({repr})".format(name=self.__class__.__name__, repr=repr(list(self._s)))

    def add(self, pipeline_file, overwrite=False, validate_unique=True, **kwargs):
        """Add a file to the collection

        :param pipeline_file: :py:class:`PipelineFile` or file path
        :param kwargs: :py:class:`dict` additional keywords passed to to PipelineFileBase __init__ method
        :param overwrite: :py:class:`bool` which, if True, will overwrite an existing matching file in the collection
        :param validate_unique: :py:class:`bool` which, if True, will validate unique attributes when adding the file
        :return: :py:class:`bool` which indicates whether the file was successfully added
        """
        self.member_validator(pipeline_file)
        validate_bool(overwrite)

        if isinstance(pipeline_file, self.member_class):
            fileobj = pipeline_file
        else:
            fileobj = self.member_class(pipeline_file, **kwargs)

        result = fileobj not in self._s
        if not result and not overwrite:
            raise DuplicatePipelineFileError("{f.name} already in collection".format(f=fileobj))

        if overwrite:
            self._s.discard(fileobj)
            result = True

        if validate_unique:
            for attribute in self.unique_attributes:
                value = getattr(fileobj, attribute)
                if value is not None:
                    self.validate_unique_attribute_value(attribute, value)

        self._s.add(fileobj)
        return result

    # alias append to the add method
    append = add

    def discard(self, pipeline_file):
        """Remove an element from the collection. Do not raise an exception if absent.

        :param pipeline_file: :py:class:`PipelineFile` or file path
        :return: :py:class:`bool` which indicates whether the file was in the collection AND was successfully discarded
        """
        self.member_validator(pipeline_file)
        if isinstance(pipeline_file, self.member_class):
            fileobj = pipeline_file
        else:
            fileobj = self.member_from_string_method(pipeline_file)

        result = fileobj in self._s

        self._s.discard(fileobj)
        return result

    def difference(self, sequence):
        return self.__class__(self._s.difference(sequence))

    def issubset(self, sequence):
        return self._s.issubset(sequence)

    def issuperset(self, sequence):
        return self._s.issuperset(sequence)

    def union(self, sequence):
        if not all(isinstance(f, self.member_class) for f in sequence):
            raise TypeError('invalid sequence, all elements must be PipelineFile objects')
        return self.__class__(self._s.union(sequence))

    def update(self, sequence, overwrite=False, validate_unique=True):
        """Add the elements of an existing :py:class:`Sequence` to this collection

        :param sequence: :py:class:`Sequence` containing :py:class:`PipelineFile` or file path elements to be added to
            the collection
        :param overwrite: :param overwrite: :py:class:`bool` which, if True, will overwrite any existing matching files
            in the collection
        :param validate_unique: :py:class:`bool` which, if True, will validate unique attributes when adding the files
        :return: :py:class:`bool` which indicates whether any files were successfully added
        """
        validate_nonstring_iterable(sequence)

        results = []
        for item in sequence:
            results.append(self.add(item, overwrite=overwrite, validate_unique=validate_unique))
        return any(results)

    def get_pipelinefile_from_dest_path(self, dest_path):
        """Get PipelineFile for a given src_path

        :param dest_path: destination path string for which to retrieve corresponding :py:class:`RemotePipelineFile`
        instance
        :return: matching :py:class:`RemotePipelineFile` instance or :py:const:`None` if it is not in the collection
        """
        pipeline_file = next((f for f in self._s if f.dest_path == dest_path), None)
        return pipeline_file

    def get_pipelinefile_from_src_path(self, src_path):
        """Get PipelineFile for a given src_path

        :param src_path: source path string for which to retrieve corresponding :py:class:`PipelineFile` instances
        :return: matching :py:class:`PipelineFile` instance or :py:const:`None` if it is not in the collection
        """
        pipeline_file = next((f for f in self._s if f.local_path == src_path), None)
        return pipeline_file

    def get_slices(self, slice_size):
        """Slice this collection into a list of :py:class:`PipelineFileCollections` with maximum length of slice_size

        :param slice_size: maximum length of each slice
        :return: list containing the current object sliced into new :py:class:`PipelineFileCollection` instances of max
            length slice_size
        """
        validate_int(slice_size)
        return slice_sequence(self, slice_size)

    def filter_by_attribute_id(self, attribute, value):
        """Return a new :py:class:`PipelineFileCollection` containing only elements where the id of the given attribute
        *is* the given id (i.e. refers to the same object)
        
        :param attribute: attribute by which to filter :py:class:`PipelineFile` instances
        :param value: attribute id to filter on
        :return: :py:class:`PipelineFileCollection` containing only :py:class:`PipelineFile` instances with the given
            attribute matching the given value
        """
        collection = self.__class__((f for f in self._s if getattr(f, attribute) is value), validate_unique=False)
        return collection

    def filter_by_attribute_id_not(self, attribute, value):
        """Return a new :py:class:`PipelineFileCollection` containing only elements where the id of the given attribute
        is *not* the given id (i.e. refers to the same object)

        :param attribute: attribute by which to filter :py:class:`PipelineFile` instances
        :param value: attribute id to filter on
        :return: :py:class:`PipelineFileCollection` containing only :py:class:`PipelineFile` instances with the given
            attribute not matching the given value
        """
        collection = self.__class__((f for f in self._s if getattr(f, attribute) is not value), validate_unique=False)
        return collection

    def filter_by_attribute_value(self, attribute, value):
        """Return a new :py:class:`PipelineFileCollection` containing only elements where the value of the given
        attribute is equal to the given value

        :param attribute: attribute by which to filter :py:class:`PipelineFile` instances
        :param value: attribute value to filter on
        :return: :py:class:`PipelineFileCollection` containing only :py:class:`PipelineFile`instances with the given
            attribute matching the given value
        """
        collection = self.__class__((f for f in self._s if getattr(f, attribute) == value), validate_unique=False)
        return collection

    def filter_by_attribute_regexes(self, attribute, regexes):
        """Return a new :py:class:`PipelineFileCollection` containing only elements where the value of the named
        attribute matches a given regex pattern

        :param attribute: attribute to filter on
        :param regexes: regex pattern(s) by which to filter PipelineFiles
        :return: :py:class:`PipelineFileCollection` containing only :py:class:`PipelineFile` instances with the
            attribute matching the given pattern
        """
        regexes = ensure_regex_list(regexes)
        collection = self.__class__(
            (f for f in self._s if matches_regexes(getattr(f, attribute), include_regexes=regexes)),
            validate_unique=False
        )
        return collection

    # add method alias for backwards compatibility
    filter_by_attribute_regex = filter_by_attribute_regexes

    def filter_by_bool_attribute(self, attribute):
        """Return a new :py:class:`PipelineFileCollection` containing only elements where the named attribute resolves
        to True

        :param attribute: attribute by which to filter :py:class:`PipelineFile` instances
        :return: :py:class:`PipelineFileCollection` containing only :py:class:`PipelineFile` instances with a True value
            for the given attribute
        """
        collection = self.__class__((f for f in self._s if getattr(f, attribute)), validate_unique=False)
        return collection

    def filter_by_bool_attribute_not(self, attribute):
        """Return a new :py:class:`PipelineFileCollection` containing only elements where the named attribute resolves
        to False

        :param attribute: attribute by which to filter :py:class:`PipelineFile` instances
        :return: :py:class:`PipelineFileCollection` containing only :py:class:`PipelineFile` instances with a False
            value for the given attribute
        """
        collection = self.__class__((f for f in self._s if not getattr(f, attribute)), validate_unique=False)
        return collection

    def filter_by_bool_attributes_and(self, *attributes):
        """Return a new :py:class:`PipelineFileCollection` containing only elements where *all* of the named attributes
        resolve to True

        :param attributes: attributes by which to filter :py:class:`PipelineFile` instances
        :return: :py:class:`PipelineFileCollection` containing only :py:class:`PipelineFile` instances with a True value
            for all of the given attributes
        """
        attributes_set = set(attributes)

        def all_attributes_true(pf):
            return all(getattr(pf, a) for a in attributes_set)

        collection = self.__class__((f for f in self._s if all_attributes_true(f)), validate_unique=False)
        return collection

    def filter_by_bool_attributes_and_not(self, true_attributes, false_attributes):
        """Return a new :py:class:`PipelineFileCollection` containing only elements where *all* of the named
        true_attributes have a value of True and all of the false_attributes have a value of False

        :param true_attributes: attributes which *must* be True
        :param false_attributes: attributes which *must* be False
        :return: :py:class:`PipelineFileCollection` containing only :py:class:`PipelineFile` instances with a True value
            for all attributes named in true_attributes and a False value for all attributes named in false_attributes
        """
        if isinstance(true_attributes, str):
            true_attributes = [true_attributes]
        if isinstance(false_attributes, str):
            false_attributes = [false_attributes]

        true_attributes_set = set(true_attributes)
        false_attributes_set = set(false_attributes)

        def check_true_attributes(pf):
            return all(getattr(pf, a) for a in true_attributes_set)

        def check_false_attributes(pf):
            return not any(getattr(pf, a) for a in false_attributes_set)

        collection = self.__class__(
            (f for f in self._s if check_true_attributes(f) and check_false_attributes(f)),
            validate_unique=False
        )
        return collection

    def filter_by_bool_attributes_not(self, *attributes):
        """Return a new :py:class:`PipelineFileCollection` containing only elements where *all* of the named attributes
        resolve to False

        :param attributes: attributes by which to filter :py:class:`PipelineFile` instances
        :return: :py:class:`PipelineFileCollection` containing only :py:class:`PipelineFile` instances with a False
            value for all of the given attributes
        """
        attributes_set = set(attributes)

        def no_attributes_true(pf):
            return not any(getattr(pf, a) for a in attributes_set)

        collection = self.__class__((f for f in self._s if no_attributes_true(f)), validate_unique=False)
        return collection

    def filter_by_bool_attributes_or(self, *attributes):
        """Return a new :py:class:`PipelineFileCollection` containing only elements where *any* of the named attributes
        resolve to True

        :param attributes: attributes by which to filter :py:class:`PipelineFile` instances
        :return: :py:class:`PipelineFileCollection` containing only :py:class:`PipelineFile` instances with a True value
            for any of the given attributes
        """
        attributes_set = set(attributes)

        def any_attributes_true(pf):
            return any(getattr(pf, a) for a in attributes_set)

        collection = self.__class__((f for f in self._s if any_attributes_true(f)), validate_unique=False)
        return collection

    def get_attribute_list(self, attribute):
        """Return a :py:class:`list` containing the given attribute from each PipelineFile in the collection

        :param attribute: the attribute name to retrieve from the objects
        :return: :py:class:`list` containing the value of the given attribute for each file in the collection
        """
        return [getattr(f, attribute) for f in self._s]

    def get_table_data(self):
        """Return :py:class:`PipelineFile` members in a simple tabular data format suitable for rendering into formatted
        tables

        :return: a :py:class:`tuple` with the first element being a list of columns, and the second being a 2D list of
            the data
        """
        data = [OrderedDict(e) for e in self._s]
        try:
            columns = list(data[0].keys())
        except IndexError:
            columns = []
        return columns, data

    def validate_unique_attribute_value(self, attribute, value):
        """Check that a given value is not already in the collection for the given :py:class:`PipelineFile` attribute,
        and raise an exception if it is

        This is intended for the use case of when an *intended* value is known for a particular attribute, and it is
        desirable to check uniqueness before setting it (e.g. when adding new files to the collection).

        :param attribute: the attribute to check
        :param value: the value being tested for uniqueness for the given attribute
        :return: None
        """
        duplicates = [f for f in self._s if getattr(f, attribute) == value]
        if duplicates:
            raise AttributeValidationError(
                "{attribute} value '{value}' already set for file(s) '{duplicates}'".format(attribute=attribute,
                                                                                            value=value,
                                                                                            duplicates=duplicates))

    def validate_attribute_value_matches_regexes(self, attribute, include_regexes):
        """Check that the given :py:class:`PipelineFile` attribute matches at least one of the given regexes for each
        file in the collection and raise an exception if any have a non-matched value

        :param attribute: the attribute to compare
        :param include_regexes: list of regexes of which the attribute must match at least one
        :return: None
        """
        validate_regexes(include_regexes)
        unmatched = {f.name: getattr(f, attribute)
                     for f in self._s
                     if not matches_regexes(getattr(f, attribute), include_regexes=include_regexes)}
        if unmatched:
            raise AttributeValidationError(
                "invalid '{attribute}' values found for files: {unmatched}. Must match one of: {regexes}".format(
                    attribute=attribute, unmatched=unmatched, regexes=include_regexes))

    def validate_attribute_uniqueness(self, attribute):
        """Check that the given :py:class:`PipelineFile` attribute is unique amongst all :py:class:`PipelineFile`
        instances currently in the collection, and raise an exception if any duplicates are found

        This is intended for the use case of a final sanity check of the collection before using it (e.g. before
        progressing to the :ref:`publish` step).

        :param attribute: the attribute to compare
        :return: None
        """
        counter = Counter(getattr(f, attribute) for f in self._s if getattr(f, attribute) is not None)
        duplicate_values = [k for k, v in counter.items() if v > 1]
        if duplicate_values:
            duplicates = []
            for value in duplicate_values:
                duplicates.extend(f for f in self._s if getattr(f, attribute) == value)
            raise AttributeValidationError(
                "duplicate attribute '{attribute}' found for files '{duplicates}'".format(attribute=attribute,
                                                                                          duplicates=duplicates))


class RemotePipelineFileCollection(PipelineFileCollectionBase):
    """A PipelineFileCollectionBase subclass to hold a set of RemotePipelineFile instances
    """

    def __init__(self, *args, **kwargs):
        kwargs['member_class'] = RemotePipelineFile
        kwargs['member_validator'] = validate_remotepipelinefile_or_string
        kwargs['member_from_string_method'] = 'get_pipelinefile_from_dest_path'
        kwargs['unique_attributes'] = {'local_path', 'dest_path'}
        super().__init__(*args, **kwargs)

    def __contains__(self, v):
        element = v if isinstance(v, self.member_class) else self.get_pipelinefile_from_dest_path(v)
        return element in self._s

    @classmethod
    def from_pipelinefilecollection(cls, pipelinefilecollection):
        return cls(RemotePipelineFile.from_pipelinefile(f) for f in pipelinefilecollection)

    def download(self, broker, local_path):
        """Helper method to download the current collection from a given broker to a given local path

        :param broker: BaseStorageBroker subclass to download from
        :param local_path: local path into which files are downloaded
        :return: None
        """
        warnings.warn("This method will be removed in a future version. From a pipeline handler, you should use "
                      "`self.state_query.download` instead.", DeprecationWarning)
        broker.download(self, local_path)

    def keys(self):
        # backwards compatibility for code expecting broker query method to return a dict with keys being "dest_path"
        return self.get_attribute_list('dest_path')


class PipelineFileCollection(PipelineFileCollectionBase):
    """A PipelineFileCollectionBase subclass to hold a set of PipelineFile instances
    """

    def __init__(self, *args, **kwargs):
        kwargs['member_class'] = PipelineFile
        kwargs['member_validator'] = validate_pipelinefile_or_string
        kwargs['member_from_string_method'] = 'get_pipelinefile_from_src_path'
        kwargs['unique_attributes'] = {'archive_path', 'dest_path'}
        super().__init__(*args, **kwargs)

    def __contains__(self, v):
        element = v if isinstance(v, self.member_class) else self.get_pipelinefile_from_src_path(v)
        return element in self._s

    @classmethod
    def from_remotepipelinefilecollection(cls, remotepipelinefilecollection, are_deletions=False):
        return cls(PipelineFile.from_remotepipelinefile(f, is_deletion=are_deletions)
                   for f in remotepipelinefilecollection)

    def add(self, pipeline_file, deletion=False, overwrite=False, validate_unique=True):
        self.member_validator(pipeline_file)
        validate_bool(deletion)

        if not isinstance(pipeline_file, self.member_class) and not deletion and not os.path.isfile(pipeline_file):
            raise MissingFileError("file '{src}' doesn't exist".format(src=pipeline_file))

        return super().add(pipeline_file, overwrite=overwrite, validate_unique=validate_unique, is_deletion=deletion)

    def _set_attribute(self, attribute, value):
        for f in self._s:
            setattr(f, attribute, value)

    def set_archive_paths(self, archive_path_function):
        """Set archive_path attributes for each file in the collection

        :param archive_path_function: function used to determine archive destination path
        :return: None
        """
        validate_callable(archive_path_function)

        for f in self._s:
            if f.archive_path is None and f.should_archive:
                candidate_path = archive_path_function(f.src_path)
                self.validate_unique_attribute_value('archive_path', candidate_path)
                f.archive_path = candidate_path

    def set_check_types(self, check_type):
        """Set check_type attributes for each file in the collection

        :param check_type: :py:class:`PipefileFileCheckType` enum member
        :return: None
        """
        validate_settable_checktype(check_type)
        additions = self.__class__(f for f in self._s if not f.is_deletion)
        additions._set_attribute('check_type', check_type)

    def set_dest_paths(self, dest_path_function):
        """Set dest_path attributes for each file in the collection

        :param dest_path_function: function used to determine publishing destination path
        :return: None
        """
        validate_callable(dest_path_function)

        for f in self._s:
            if f.dest_path is None and any((f.should_store, f.should_harvest)):
                candidate_path = dest_path_function(f.src_path)
                self.validate_unique_attribute_value('dest_path', candidate_path)
                f.dest_path = candidate_path

    def set_bool_attribute(self, attribute, value):
        """Set a :py:class:`bool` attribute for each file in the collection

        :param attribute: attribute to set
        :param value: value to set the attribute
        :return: None
        """
        validate_bool(value)
        self._set_attribute(attribute, value)

    def set_publish_types(self, publish_type):
        """Set publish_type attributes for each file in the collection

        :param publish_type: :py:class:`PipefileFilePublishType` enum member
        :return: None
        """
        validate_publishtype(publish_type)
        self._set_attribute('publish_type', publish_type)

    def set_string_attribute(self, attribute, value):
        """Set a string attribute for each file in the collection

        :param attribute: attribute to set
        :param value: value to set the attribute
        :return: None
        """
        validate_string(value)
        self._set_attribute(attribute, value)

    def set_file_update_callback(self, file_update_callback):
        """Set a callback function in each :py:class:`PipelineFile` in this collection

        :param file_update_callback: callback (function)
        :return: None
        """
        for f in self._s:
            f.file_update_callback = file_update_callback

    def set_default_check_types(self, check_params=None):
        """Set check_type attribute for each file in the collection to the default value, based on the file type and
        presence of compliance checker checks in the check parameters

        :param check_params: :py:class:`dict` or None
        :return: None
        """
        if check_params is None:
            check_params = {}
        else:
            validate_check_params(check_params)

        checks = check_params.get('checks', ())

        all_additions = self.__class__(f for f in self._s if not f.is_deletion)
        netcdf_additions = self.__class__(f for f in all_additions if f.file_type is FileType.NETCDF)
        non_netcdf_additions = all_additions.difference(netcdf_additions)

        netcdf_check_type = PipelineFileCheckType.NC_COMPLIANCE_CHECK if checks else PipelineFileCheckType.FORMAT_CHECK

        netcdf_additions.set_check_types(netcdf_check_type)
        non_netcdf_additions.set_check_types(PipelineFileCheckType.FORMAT_CHECK)

    def set_publish_types_from_regexes(self, include_regexes, exclude_regexes, addition_type, deletion_type):
        """Set publish_type attribute for each file in the collection depending on whether it is considered "included"
        according to the regex parameters

        :param include_regexes: regex(es) for which a file must match one or more to be included
        :param exclude_regexes: regex(es) which will exclude an already included file
        :param addition_type: :py:class:`PipefileFilePublishType` enum member set for included addition files
        :param deletion_type: :py:class:`PipefileFilePublishType` enum member set for included deletion files
        :return: None
        """
        validate_regexes(include_regexes)
        if exclude_regexes:
            validate_regexes(exclude_regexes)

        for f in self._s:
            if matches_regexes(f.name, include_regexes, exclude_regexes):
                f.publish_type = deletion_type if f.is_deletion else addition_type


validate_pipelinefilecollection = validate_type(PipelineFileCollection)
validate_pipelinefile_or_pipelinefilecollection = validate_type((PipelineFile, PipelineFileCollection))
validate_pipelinefile_or_string = validate_type((PipelineFile, str))

validate_remotepipelinefilecollection = validate_type(RemotePipelineFileCollection)
validate_remotepipelinefile_or_remotepipelinefilecollection = validate_type((RemotePipelineFile,
                                                                             RemotePipelineFileCollection))
validate_remotepipelinefile_or_string = validate_type((RemotePipelineFile, str))

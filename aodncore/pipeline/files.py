import os
import re
from collections import MutableSet, OrderedDict

import six

from .common import (EXT_NETCDF, PipelineFilePublishType, PipelineFileCheckType, validate_addition_publishtype,
                     validate_checkresult, validate_checktype, validate_deletion_publishtype, validate_publishtype)
from .exceptions import MissingFileError
from ..util import (IndexedSet, format_exception, get_file_checksum, matches_regexes, slice_sequence,
                    validate_bool, validate_callable, validate_dict, validate_mapping, validate_nonstring_iterable,
                    validate_regex, validate_type)

__all__ = [
    'PipelineFileCollection',
    'PipelineFile',
    'validate_pipelinefilecollection',
    'validate_pipelinefile_or_string'
]


class PipelineFile(object):
    """Represents a single file in order to store state information relating to the intended actions to be performed
        on the file, and the actions that *were*
    """

    def __init__(self, src_path, name=None, archive_path=None, dest_path=None, is_deletion=False,
                 file_update_callback=None):
        """
        :param src_path: absolute source path to the file being represented
        :param name: arbitrary name (defaults to os.path.basename)
        :param archive_path: relative path used when archiving the file
        :param dest_path: relative path used when publishing the file
        :param is_deletion: boolean designating whether this is a deletion
        :param file_update_callback: optional callback to call when a file property is updated
        """
        try:
            self.file_checksum = None if is_deletion else get_file_checksum(src_path)
        except (IOError, OSError) as e:
            raise MissingFileError(
                "failed to create PipelineFile addition for '{src_path}'. {e}".format(src_path=src_path,
                                                                                      e=format_exception(e)))

        self.name = name if name is not None else os.path.basename(src_path)
        self.src_path = src_path
        self._archive_path = archive_path
        self._dest_path = dest_path

        _, self.extension = os.path.splitext(src_path)

        self.file_update_callback = file_update_callback

        # processing flags - these express the *intended actions* for the file
        self._check_type = PipelineFileCheckType.NO_ACTION
        self._is_deletion = is_deletion
        self._publish_type = PipelineFilePublishType.NO_ACTION
        self._should_archive = False
        self._should_harvest = False
        self._should_store = False

        # status flags - these express the *current state* of the file
        self._is_checked = False
        self._is_archived = False
        self._is_harvested = False
        self._is_stored = False

        self._check_result = None

    def __iter__(self):
        yield 'archive_path', self.archive_path
        yield 'check_log', self.check_log
        yield 'check_passed', self.check_passed
        yield 'check_type', self.check_type.name
        yield 'dest_path', self.dest_path
        yield 'file_checksum', self.file_checksum
        yield 'is_checked', str(self._is_checked)
        yield 'is_deletion', str(self._is_deletion)
        yield 'is_archived', str(self._is_archived)
        yield 'is_harvested', str(self._is_harvested)
        yield 'is_stored', str(self._is_stored)
        yield 'name', self.name
        yield 'published', self.published
        yield 'pending_archive', str(self.pending_archive)
        yield 'pending_harvest_addition', str(self.pending_harvest_addition)
        yield 'pending_harvest_deletion', str(self.pending_harvest_deletion)
        yield 'pending_store_addition', str(self.pending_store_addition)
        yield 'pending_store_deletion', str(self.pending_store_deletion)
        yield 'publish_type', self.publish_type.name
        yield 'should_archive', str(self._should_archive)
        yield 'should_harvest', str(self._should_harvest)
        yield 'should_store', str(self._should_store)
        yield 'src_path', self.src_path

    def __repr__(self):  # pragma: no cover
        return "PipelineFile({repr})".format(repr=repr(dict(self)))

    @property
    def archive_path(self):
        return self._archive_path

    @archive_path.setter
    def archive_path(self, archive_path):
        if os.path.isabs(archive_path):
            raise ValueError('archive_path must be a relative path')
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
        """

        :param check_result: 
        :return: 
        """
        validate_checkresult(check_result)

        self._is_checked = True
        self._check_result = check_result
        self._post_property_update({'is_checked': True})

    @property
    def check_type(self):
        return self._check_type

    @check_type.setter
    def check_type(self, check_type):
        validate_checktype(check_type)

        self._check_type = check_type
        self._post_property_update({'check_type': check_type.name})

    @property
    def dest_path(self):
        return self._dest_path

    @dest_path.setter
    def dest_path(self, dest_path):
        if os.path.isabs(dest_path):
            raise ValueError('dest_path must be a relative path')
        self._dest_path = dest_path
        self._post_property_update({'dest_path': dest_path})

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
    def is_deleted(self):
        return self.is_deletion and self.is_stored

    @property
    def is_stored(self):
        return self._is_stored

    @is_stored.setter
    def is_stored(self, is_stored):
        validate_bool(is_stored)

        self._is_stored = is_stored
        self._post_property_update({'is_stored': is_stored})

    @property
    def is_uploaded(self):
        return not self.is_deletion and self.is_stored

    @property
    def published(self):
        should_publish = self.should_store or self.should_harvest
        was_published = self.is_stored or self.is_harvested
        return 'Yes' if should_publish and was_published else 'No'

    @property
    def pending_archive(self):
        return self.should_archive and not self.is_archived

    @property
    def pending_harvest(self):
        return self.should_harvest and not self.is_harvested

    @property
    def pending_harvest_addition(self):
        return self.pending_harvest and not self.is_deletion

    @property
    def pending_harvest_deletion(self):
        return self.pending_harvest and self.is_deletion

    @property
    def pending_store(self):
        return self.should_store and not self.is_stored

    @property
    def pending_store_addition(self):
        return self.pending_store and not self.is_deletion

    @property
    def pending_store_deletion(self):
        return self.pending_store and self.is_deletion

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

    def _post_property_update(self, properties, include_values=True):
        """Method run after a property is updated in order to perform optional actions such as updating ORM (if enabled)
            and running the update callback (if set)

        :param properties: dict containing the updated properties and their new values (used to update ORM)
        :return: None
        """
        validate_mapping(properties)

        if self.file_update_callback:
            log_output = properties if include_values else properties.keys()
            self.file_update_callback(name=self.name,
                                      message="updated: {properties}".format(properties=log_output))


class PipelineFileCollection(MutableSet):
    """A collection which implements the MutableSet abstract base class, with pipeline file specific functionality
    """
    __slots__ = ['__s']

    def __init__(self, data=None):
        super(PipelineFileCollection, self).__init__()

        self.__s = IndexedSet()

        if data is not None:
            if isinstance(data, (PipelineFile, six.string_types)):
                data = [data]
            for f in data:
                self.add(f)

    def __contains__(self, v):
        if isinstance(v, PipelineFile):
            element = v
        else:
            element = self.get_pipelinefile_from_src_path(v)
        return element in self.__s

    def __getitem__(self, index):
        result = self.__s[index]
        return PipelineFileCollection(result) if isinstance(result, IndexedSet) else result

    def __iter__(self):
        return iter(self.__s)

    def __len__(self):
        return len(self.__s)

    def __repr__(self):  # pragma: no cover
        return "PipelineFileCollection({repr})".format(repr=repr(list(self.__s)))

    def add(self, src_path, deletion=False):
        validate_pipelinefile_or_string(src_path)

        if isinstance(src_path, PipelineFile):
            fileobj = src_path
        else:
            if not deletion and not os.path.isfile(src_path):
                raise MissingFileError("file '{src}' doesn't exist".format(src=src_path))

            fileobj = PipelineFile(src_path, is_deletion=deletion)
        result = fileobj not in self.__s
        self.__s.add(fileobj)
        return result

    # alias append to the add method
    append = add

    def discard(self, pipelinefile):
        if isinstance(pipelinefile, PipelineFile):
            fileobj = pipelinefile
        else:
            fileobj = self.get_pipelinefile_from_src_path(pipelinefile)

        result = fileobj in self.__s
        self.__s.discard(fileobj)
        return result

    def difference(self, sequence):
        return self.__s.difference(sequence)

    def issubset(self, sequence):
        return self.__s.issubset(sequence)

    def issuperset(self, sequence):
        return self.__s.issuperset(sequence)

    def union(self, sequence):
        if not all(isinstance(f, PipelineFile) for f in sequence):
            raise TypeError('invalid sequence, all elements must be PipelineFile objects')
        return PipelineFileCollection(self.__s.union(sequence))

    def update(self, sequence):
        validate_nonstring_iterable(sequence)

        result = None
        for item in sequence:
            result = self.add(item)
        return result

    def get_pipelinefile_from_src_path(self, src_path):
        """Get PipelineFile for a given src_path

        :param src_path: source path string for which to retrieve corresponding PipelineFile
        :return: matching PipelineFile or None if it is not in the collection
        """
        pipeline_file = next((f for f in self.__s if f.src_path == src_path), None)
        return pipeline_file

    def get_slices(self, slice_size):
        """Slice this collection into a list of PipelineFileCollections with maximum length of slice_size

        :param slice_size: maximum length of each slice
        :return: list containing the current object sliced into new PipelineFileCollections of max length slice_size
        """
        return slice_sequence(self, slice_size)

    def filter_by_attribute_id(self, attribute, value):
        """Return a new PipelineFileCollection containing only elements where the id of the given attribute *is* the
        given id (i.e. refers to the same object)
        
        :param attribute: attribute by which to filter PipelineFiles
        :param value: attribute id to filter on
        :return: PipelineFileCollection containing only PipelineFiles with the given attribute matching the given value
        """
        collection = PipelineFileCollection(f for f in self.__s if getattr(f, attribute) is value)
        return collection

    def filter_by_attribute_value(self, attribute, value):
        """Return a new PipelineFileCollection containing only elements where the value of the given attribute is equal
            to the given value

        :param attribute: attribute by which to filter PipelineFiles
        :param value: attribute value to filter on
        :return: PipelineFileCollection containing only PipelineFiles with the given attribute matching the given value
        """
        collection = PipelineFileCollection(f for f in self.__s if getattr(f, attribute) == value)
        return collection

    def filter_by_attribute_regex(self, attribute, pattern):
        """Return a new PipelineFileCollection containing only elements where the value of the named attribute matches a
            given regex pattern

        :param attribute: attribute to filter on
        :param pattern: regex pattern by which to filter PipelineFiles
        :return: PipelineFileCollection containing only PipelineFiles with the attribute matching the given pattern
        """
        validate_regex(pattern)
        collection = PipelineFileCollection(
            f for f in self.__s if getattr(f, attribute) and re.match(pattern, getattr(f, attribute)))
        return collection

    def filter_by_bool_attribute(self, attribute):
        """Return a new PipelineFileCollection containing only elements where the named attribute resolves to True

        :param attribute: attribute by which to filter PipelineFiles
        :return: PipelineFileCollection containing only PipelineFiles with a True value for the given attribute
        """
        collection = PipelineFileCollection(f for f in self.__s if getattr(f, attribute))
        return collection

    def filter_by_bool_attribute_not(self, attribute):
        """Return a new PipelineFileCollection containing only elements where the named attribute resolves to False

        :param attribute: attribute by which to filter PipelineFiles
        :return: PipelineFileCollection containing only PipelineFiles with a False value for the given attribute
        """
        collection = PipelineFileCollection(f for f in self.__s if not getattr(f, attribute))
        return collection

    def filter_by_bool_attributes_and(self, *attributes):
        """Return a new PipelineFileCollection containing only elements where *all* of the named attributes resolve to
            True

        :param attributes: attributes by which to filter PipelineFiles
        :return: PipelineFileCollection containing only PipelineFiles with a True value for all of the given attributes
        """
        attributes_set = set(attributes)

        def all_attributes_true(pf):
            return all(getattr(pf, a) for a in attributes_set)

        collection = PipelineFileCollection(f for f in self.__s if all_attributes_true(f))
        return collection

    def filter_by_bool_attributes_and_not(self, true_attributes, false_attributes):
        """Return a new PipelineFileCollection containing only elements where *all* of the named true_attributes have a
            value of True and all of the false_attributes have a value of False

        :param true_attributes: attributes which *must* be True
        :param false_attributes: attributes which *must* be False
        :return: PipelineFileCollection containing only PipelineFiles with a True value for all attributes named in
        true_attributes and a False value for all attributes named in false_attributes
        """
        if isinstance(true_attributes, six.string_types):
            true_attributes = [true_attributes]
        if isinstance(false_attributes, six.string_types):
            false_attributes = [false_attributes]

        true_attributes_set = set(true_attributes)
        false_attributes_set = set(false_attributes)

        def check_true_attributes(pf):
            return all(getattr(pf, a) for a in true_attributes_set)

        def check_false_attributes(pf):
            return not any(getattr(pf, a) for a in false_attributes_set)

        collection = PipelineFileCollection(
            f for f in self.__s if check_true_attributes(f) and check_false_attributes(f))
        return collection

    def filter_by_bool_attributes_not(self, *attributes):
        """Return a new PipelineFileCollection containing only elements where *all* of the named attributes resolve to
            False

        :param attributes: attributes by which to filter PipelineFiles
        :return: PipelineFileCollection containing only PipelineFiles with a False value for all of the given attributes
        """
        attributes_set = set(attributes)

        def no_attributes_true(pf):
            return not any(getattr(pf, a) for a in attributes_set)

        collection = PipelineFileCollection(f for f in self.__s if no_attributes_true(f))
        return collection

    def filter_by_bool_attributes_or(self, *attributes):
        """Return a new PipelineFileCollection containing only elements where *any* of the named attributes resolve to
            True

        :param attributes: attributes by which to filter PipelineFiles
        :return: PipelineFileCollection containing only PipelineFiles with a True value for any of the given attributes
        """
        attributes_set = set(attributes)

        def any_attributes_true(pf):
            return any(getattr(pf, a) for a in attributes_set)

        collection = PipelineFileCollection(f for f in self.__s if any_attributes_true(f))
        return collection

    def get_table_data(self):
        """Return PipelineFile members in a simple tabular data format suitable for rendering into formatted tables

        :return: a tuple with the first element being a list of columns, and the second being a 2D list of the data
        """
        data = [OrderedDict(e) for e in self.__s]
        try:
            columns = data[0].keys()
        except IndexError:
            columns = []
        return columns, data

    def set_check_types(self, check_params):
        if check_params is None:
            check_params = {}
        validate_dict(check_params)
        checks = check_params.get('checks', ())
        for f in self.__s:
            if f.is_deletion:
                f.check_type = PipelineFileCheckType.NO_ACTION
            elif checks and f.extension == EXT_NETCDF:
                f.check_type = PipelineFileCheckType.NC_COMPLIANCE_CHECK
            else:
                f.check_type = PipelineFileCheckType.FORMAT_CHECK

    def set_archive_paths(self, archive_path_function):
        """Set archive_path attributes for each file in the collection

        :param archive_path_function: function used to determine archive destination path
        :return: None
        """
        validate_callable(archive_path_function)

        for f in self.__s:
            if f.archive_path is None and f.should_archive:
                f.archive_path = archive_path_function(f.src_path)

    def set_dest_paths(self, dest_path_function):
        """Set dest_path attributes for each file in the collection

        :param dest_path_function: function used to determine publishing destination path
        :return: None
        """
        validate_callable(dest_path_function)

        for f in self.__s:
            if f.dest_path is None and any((f.should_store, f.should_harvest)):
                f.dest_path = dest_path_function(f.src_path)

    def set_file_update_callback(self, file_update_callback):
        """Set a callback function in each PipelineFile in this collection

        :param file_update_callback: callback (function)
        :return:
        """
        for f in self.__s:
            f.file_update_callback = file_update_callback

    def set_default_publish_types(self, include_regexes, exclude_regexes, addition_type, deletion_type):
        """

        :param include_regexes:
        :param exclude_regexes:
        :param addition_type:
        :param deletion_type:
        :return:
        """
        for f in self.__s:
            if matches_regexes(f.name, include_regexes, exclude_regexes):
                f.publish_type = deletion_type if f.is_deletion else addition_type


validate_pipelinefilecollection = validate_type(PipelineFileCollection)
validate_pipelinefile_or_string = validate_type((PipelineFile, six.string_types))

"""This module provides the step runner classes for the :ref:`resolve` step.

Resolution is performed by a :py:class:`BaseResolveRunner` class, which is responsible for resolving an input file into
a :py:class:`PipelineFileCollection`. The input file may resolve into more than one file, for example in the case of a
ZIP file (which may contain multiple files to be processed) or a manifest file (which may refer to multiple file paths).

The :py:meth:`__init__` method is supplied with the input file and the output_dir. Based on the file extension, the
appropriate ResolveRunner class is first determined, before the abstract :py:meth:`run` method causes the file to be
extracted into the actual files to be processed. If this step is successful, the following will have occurred:

- in the case of a non-manifest input file, the directory defined by the :py:attr:`output_dir` parameter will contain
  all of the files being handled
- in the case of a manifest file, the existence of the files defined in the manifest will be confirmed
- the run method returns a PipelineFileCollection instance populated with all of these files

This means the rest of the handler code has no further need to be aware of the source of the files, and the file
collection may then be processed in a generic way.
"""

import abc
import json
import os
import re
import warnings
from collections import namedtuple
from enum import Enum
from io import open

import tableschema
from jsonschema.exceptions import ValidationError

from .basestep import BaseStepRunner
from ..common import FileType, PipelineFilePublishType
from ..exceptions import InvalidFileFormatError
from ..files import PipelineFile, PipelineFileCollection, RemotePipelineFile
from ..schema import validate_json_manifest
from ...util import extract_gzip, extract_zip, list_regular_files, is_gzip_file, is_zip_file, safe_copy_file

__all__ = [
    'get_resolve_runner',
    'DeleteManifestResolveRunner',
    'DirManifestResolveRunner',
    'GzipFileResolveRunner',
    'JsonManifestResolveRunner',
    'MapManifestResolveRunner',
    'RsyncManifestResolveRunner',
    'SimpleManifestResolveRunner',
    'SingleFileResolveRunner',
    'ZipFileResolveRunner'
]


def get_resolve_runner(input_file, output_dir, config, logger, resolve_params=None):
    """Factory function to return appropriate resolver class based on the file extension

    :param input_file: path to the input file
    :param output_dir: directory where the resolved files will be extracted/copied
    :param config: :py:class:`LazyConfigManager` instance
    :param logger: :py:class:`Logger` instance
    :param resolve_params: dict of parameters to pass to :py:class:`BaseResolveRunner` class for runtime configuration
    :return: :py:class:`BaseResolveRunner` class
    """
    file_type = FileType.get_type_from_name(input_file)

    if file_type is FileType.ZIP:
        return ZipFileResolveRunner(input_file, output_dir, config, logger)
    elif file_type is FileType.SIMPLE_MANIFEST:
        return SimpleManifestResolveRunner(input_file, output_dir, config, logger, resolve_params)
    elif file_type is FileType.DELETE_MANIFEST:
        delete_manifests_allowed = resolve_params.get('allow_delete_manifests', False) if resolve_params else False
        if delete_manifests_allowed:
            return DeleteManifestResolveRunner(input_file, output_dir, config, logger, resolve_params)
        raise InvalidFileFormatError('delete_manifests are not enabled for this pipeline')
    elif file_type is FileType.JSON_MANIFEST:
        return JsonManifestResolveRunner(input_file, output_dir, config, logger, resolve_params)
    elif file_type is FileType.MAP_MANIFEST:
        return MapManifestResolveRunner(input_file, output_dir, config, logger, resolve_params)
    elif file_type is FileType.RSYNC_MANIFEST:
        return RsyncManifestResolveRunner(input_file, output_dir, config, logger, resolve_params)
    elif file_type is FileType.DIR_MANIFEST:
        return DirManifestResolveRunner(input_file, output_dir, config, logger, resolve_params)
    elif file_type is FileType.GZIP:
        return GzipFileResolveRunner(input_file, output_dir, config, logger)
    else:
        return SingleFileResolveRunner(input_file, output_dir, config, logger)


class BaseResolveRunner(BaseStepRunner, metaclass=abc.ABCMeta):
    def __init__(self, input_file, output_dir, config, logger):
        super().__init__(config, logger)
        self.input_file = input_file
        self.output_dir = output_dir
        self._collection = PipelineFileCollection()

    @abc.abstractmethod
    def run(self):
        pass


class SingleFileResolveRunner(BaseResolveRunner):
    def run(self):
        name = os.path.basename(self.input_file)
        temp_location = os.path.join(self.output_dir, name)
        safe_copy_file(self.input_file, temp_location)

        self._collection.add(temp_location)
        return self._collection


class GzipFileResolveRunner(BaseResolveRunner):
    def run(self):
        if not is_gzip_file(self.input_file):
            raise InvalidFileFormatError("input_file must be a valid GZ file")

        extract_gzip(self.input_file, self.output_dir)

        for f in list_regular_files(self.output_dir):
            self._collection.add(f)
        return self._collection


class ZipFileResolveRunner(BaseResolveRunner):
    def run(self):
        if not is_zip_file(self.input_file):
            raise InvalidFileFormatError("input_file must be a valid ZIP file")

        extract_zip(self.input_file, self.output_dir)

        for f in list_regular_files(self.output_dir, recursive=True):
            self._collection.add(f)
        return self._collection


# noinspection PyAbstractClass
class BaseManifestResolveRunner(BaseResolveRunner):
    def __init__(self, input_file, output_dir, config, logger, resolve_params=None):
        super().__init__(input_file, output_dir, config, logger)

        if resolve_params is None:
            resolve_params = {}

        relative_path_root = resolve_params.get('relative_path_root', self._config.pipeline_config['global']['wip_dir'])
        self.relative_path_root = relative_path_root

    def get_abs_path(self, path):
        return path if os.path.isabs(path) else os.path.join(self.relative_path_root, path)


class JsonManifestResolveRunner(BaseManifestResolveRunner):
    """Handles a JSON manifest file, *optionally* with a pre-determined destination path. Unlike other resolve runners,
    this creates :py:class:`PipelineFile` objects to add to the collection rather than allowing the collection to
    generate the objects.

    If a "files" attribute is present, the files will be added to the collection. The elements of the "files"
    attribute may be one of the following types::

        1. an object, in which the 'local_path' attribute represents the source path of the file
            1. optionally, if a the 'dest_path' attribute is provided, this will be used as a predetermined destination
                path, (similar to the MapManifestResolveRunner)
        e.g.

        {
            "files": [
                {
                    "local_path": "/path/to/source/file1"
                },
                {
                    "local_path": "/path/to/source/file2",
                    "dest_path": "destination/path/for/upload2"
                }
            ]
        }

    """

    def run(self):
        try:
            with open(self.input_file) as f:
                contents = json.load(f)
            validate_json_manifest(contents)
        except ValueError:
            raise InvalidFileFormatError("input_file must be a valid JSON file")
        except ValidationError:
            raise InvalidFileFormatError("input_file failed to validate against the JSON manifest schema")

        for entry in contents.get('files', []):
            abs_path = self.get_abs_path(entry['local_path'])
            fileobj = PipelineFile(abs_path, dest_path=entry.get('dest_path'))
            self._collection.add(fileobj)

        return self._collection


# define a namedtuple for storing a more human readable structure for the tableschema exception handler
TableRowErrorDetails = namedtuple('TableRowErrorDetails', ('exc', 'row_number', 'row_data', 'error_data'))


# noinspection PyAbstractClass
class BaseCsvManifestResolveRunner(BaseManifestResolveRunner):
    """Base class for handling a CSV manifest file. Unlike other resolve runners, this creates :py:class:`PipelineFile`
    objects to add to the collection rather than allowing the collection to generate theobjects.

    Subclasses must implement a 'schema' property which describes and validates the CSV fields, and a `_row_handler`
    method which creates a PipelineFile object from the parsed row (which is implementation specific)

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.errors = []

    @property
    @abc.abstractmethod
    def schema(self):
        """Defines the tableschema schema describing the input file
        :returns: tableschema.Schema instance
        """
        pass

    @abc.abstractmethod
    def _row_handler(self, row):
        """Translates a parsed row into a PipelineFile object to add to the collection
        :returns: PipelineFile instance
        """
        pass

    def _exc_handler(self, exc, row_number=None, row_data=None, error_data=None):
        self.errors.append(TableRowErrorDetails(exc, row_number, row_data, error_data))

    @staticmethod
    def _format_error(error):
        return "{args}: {errors}".format(args=','.join(str(e) for e in error.exc.args),
                                         errors=','.join(str(e) for e in error.exc.errors))

    def _handle_errors(self):
        error_details = [self._format_error(e) for e in self.errors]
        raise InvalidFileFormatError(error_details)

    def _table_iterator(self):
        table = tableschema.Table(self.input_file, headers=self.schema.field_names, schema=self.schema, format='csv')
        return (r for r in table.iter(exc_handler=self._exc_handler))

    def run(self):
        for row in self._table_iterator():
            if self.errors:
                # continue iterating to detect *all* errors in the file, but don't waste resources building the
                # PipelineFileCollection
                continue
            else:
                pipeline_file = self._row_handler(row)
                self._collection.add(pipeline_file)

        if self.errors:
            self._handle_errors()

        return self._collection


class MapManifestResolveRunner(BaseCsvManifestResolveRunner):
    """Handles a manifest file with a pre-determined destination path. Unlike other resolve runners, this creates
    :py:class:`PipelineFile` objects to add to the collection rather than allowing the collection to generate the
    objects.
    
    File format must be as follows::
    
        /path/to/source/file1,destination/path/for/upload1
        /path/to/source/file2,destination/path/for/upload2

    """

    @property
    def schema(self):
        return tableschema.Schema({
            'fields': [
                {
                    'name': 'local_path',
                    'type': 'string',
                    'constraints': {
                        'required': True,
                        'unique': True
                    }
                },
                {
                    'name': 'dest_path',
                    'type': 'string',
                    'constraints': {
                        'required': True,
                        'unique': True
                    }
                }
            ]},
            strict=True
        )

    def _row_handler(self, row):
        local_path, dest_path = row
        abs_path = self.get_abs_path(local_path)
        pipeline_file = PipelineFile(abs_path, dest_path=dest_path)
        return pipeline_file


class DeleteManifestResolveRunner(BaseCsvManifestResolveRunner):
    """Handles a delete manifest file which only contains a list of source files, and optionally a valid
        "deletion publish type" string, as defined by the PipelineFilePublishType enum. If delete_publish_type is
        omitted, the value will remain UNSET, and the handler will assume responsibility for setting the appropriate
        type

    File format must be as follows::

        destination/path/for/delete1,DELETE_PUBLISH_TYPE
        destination/path/for/delete2,DELETE_PUBLISH_TYPE

    """

    @property
    def schema(self):
        return tableschema.Schema({
            'fields': [
                {
                    'name': 'dest_path',
                    'type': 'string',
                    'constraints': {
                        'required': True,
                        'unique': True
                    }
                }
            ]},
            strict=True
        )

    def _row_handler(self, row):
        dest_path, = row
        remote_file = RemotePipelineFile(dest_path)
        pipeline_file = PipelineFile.from_remotepipelinefile(remote_file, is_deletion=True)
        return pipeline_file


class RsyncLineType(Enum):
    INVALID = 0
    HEADER = 1
    FILE_ADD = 2
    FILE_DELETE = 3
    DIRECTORY_ADD = 4
    DIRECTORY_DELETE = 5


class RsyncManifestLine(object):
    def __init__(self, path, type_):
        self.path = path
        self.type = type_


class RsyncManifestResolveRunner(BaseManifestResolveRunner):
    """Handles a manifest file as output by an rsync process

    The manifest is generated by capturing the output of an rsync process run with the "-i, --itemize-changes" argument.
    See the RSYNC man page, https://download.samba.org/pub/rsync/rsync.html, for a detailed description of this format.

    File format is expected to have invalid lines(header, whitespace,summary lines), so valid lines are extracted using
    regular expressions to determine the intended action. A file will *typically* look as follows. The lines should be
    classified as follows (text in square brackets not in actual files)::
    
        receiving incremental file list                                     [HEADER LINE, IGNORED]
        *deleting   aoml/1900709/profiles/                                  [DIRECTORY DELETION, IGNORED]
        .d..t...... aoml/1900709/                                           [DIRECTORY ADDITION, IGNORED]
        >f.st...... handlers/dummy/test_manifest.nc                         [FILE ADDITION]
        *deleting   handlers/dummy/aoml/1900728/1900728_Rtraj.nc            [FILE DELETION]
                                                                            [NON-MATCHING LINE, IGNORED]
                                                                            [NON-MATCHING LINE, IGNORED]
        sent 65477852 bytes  received 407818360 bytes  115508.53 bytes/sec  [NON-MATCHING LINE, IGNORED]
        total size is 169778564604  speedup is 358.72                       [NON-MATCHING LINE, IGNORED]

    """
    HEADER_LINE = 'receiving incremental file list'
    RECORD_PATTERN = re.compile(r"""^
                                (?P<operation>\*deleting|[>.][df].{9}) # file operation type
                                \s{1,3} # space(s) separating operation from path
                                (?P<path>.*) # file path
                                $
                                """, re.VERBOSE)

    FILE_ADD_PATTERN = re.compile(r'^>f.{9}')
    DIR_ADD_PATTERN = re.compile(r'^\.d.{9}')
    DELETE_PATTERN = re.compile(r'^\*deleting')

    @classmethod
    def classify_line(cls, line):

        match = cls.RECORD_PATTERN.match(line)
        try:
            matchdict = match.groupdict()
            operation = matchdict['operation']
            path = matchdict['path']
        except (AttributeError, KeyError):
            return RsyncManifestLine(None, RsyncLineType.INVALID)

        if cls.FILE_ADD_PATTERN.match(operation):
            return RsyncManifestLine(path, RsyncLineType.FILE_ADD)
        elif cls.DIR_ADD_PATTERN.match(operation):
            return RsyncManifestLine(path, RsyncLineType.DIRECTORY_ADD)
        elif cls.DELETE_PATTERN.match(operation):
            delete_type = RsyncLineType.DIRECTORY_DELETE if path.endswith('/') else RsyncLineType.FILE_DELETE
            return RsyncManifestLine(path, delete_type)
        elif line == cls.HEADER_LINE:
            return RsyncManifestLine(None, RsyncLineType.HEADER)
        else:
            return RsyncManifestLine(None, RsyncLineType.INVALID)  # pragma: no cover

    def run(self):
        with open(self.input_file, 'r') as f:
            for line_newline in f:
                line = line_newline.rstrip(os.linesep)
                record = self.classify_line(line)

                if record.type not in {RsyncLineType.FILE_ADD, RsyncLineType.FILE_DELETE}:
                    continue

                abs_path = self.get_abs_path(record.path)
                if record.type is RsyncLineType.FILE_ADD:
                    self._collection.add(abs_path)
                elif record.type is RsyncLineType.FILE_DELETE:
                    self._collection.add(abs_path, is_deletion=True)

        return self._collection


class SimpleManifestResolveRunner(BaseManifestResolveRunner):
    """Handles a simple manifest file which only contains a list of source files

    File format must be as follows::

        /path/to/source/file1
        /path/to/source/file2

    """

    def run(self):
        with open(self.input_file, 'r') as f:
            for line_newline in f:
                line = line_newline.rstrip(os.linesep)
                abs_path = self.get_abs_path(line)
                self._collection.add(abs_path)

        return self._collection


class DirManifestResolveRunner(BaseManifestResolveRunner):
    """Handles a simple manifest file which only contains a list of source files or directories, which will have all
    files recursively added to the collection

    File format must be as follows::

        /path/to/source/file1
        /path/to/source/dir1

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        warnings.warn("This FileType ('.dir_manifest') will be removed in a future version. Please update code to use "
                      "a pre-generated simple manifest ('.manifest') instead.", DeprecationWarning)

    def run(self):
        with open(self.input_file, 'r') as f:
            for line_newline in f:
                line = line_newline.rstrip(os.linesep)
                abs_path = self.get_abs_path(line)
                if os.path.isdir(abs_path):
                    for f_ in list_regular_files(abs_path, recursive=True):
                        self._collection.add(f_)
                else:
                    self._collection.add(abs_path)

        return self._collection

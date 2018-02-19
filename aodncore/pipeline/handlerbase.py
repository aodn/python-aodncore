"""
Handler user guide
==================

Overview
--------

The ``aodncore.pipeline`` package provides the base class for each pipeline handler at the
``aodncore.pipeline.HandlerBase`` namespace. This is the starting point for any new handler development, as it contains
all of the core functionality of each handler, which is then available to the child class via class inheritance.


State machine / handler steps
-------------------------------

In order to provide consistency and structure to the handler, the pipeline is broken into a series of ordered "steps",
with each performing a distinct function in the processing of the input file. The "machine" defines a series of states,
and also controls/enforces the transitions between states.

For example, since it makes no sense to check a collection of files before the collection exists, the state machine
enforces that the ``check`` step may *only* be entered into from the ``resolve`` step.

Similarly, the ``publish`` step cannot ever be entered into other than from the ``process`` step, which means that the
step can safely make several assumptions about the overall state of the handler when it does get executed. For example,
it can automatically assume with a 100% guarantee that the ``initalise``, ``resolve``, ``preprocess``, ``check`` and
``process`` step have all been run in that order with no errors, allowing it to focus purely on the core concern of the
step; publishing files, and nothing more.

The ordered steps are as follows:

initialise
~~~~~~~~~~

Responsible for general setup of handler class and performing initial sanity checking of the input file and parameters

#. validation of parameters
#. validation of input file (e.g. the file exists, is accessible, is of an allowed type etc.)
#. setup temporary directories

resolve
~~~~~~~

Responsible for preparing the central file collection of the handler instance, including handling input files which
represent multiple files (e.g. ZIP and manifest files). The file collection is used to hold the processing state of all
"known" files for the duration of the handler. After this step, there is no need to consider the original source format
of the input file, as this step "resolves" the file into a generic collection for further processing.

#. prepare the "file collection" used by all subsequent steps by placing files into a temporary directory and
   creating an entry in the handlers "file_collection" attribute, which is a special type of set
   (PipelineFileCollection object) optimised for dealing with pipeline files (PipelineFile objects)

    #. if single file, copy to temporary directory and add to file collection
    #. if ZIP file, extract files into temporary directory and add them to the file collection
    #. if manifest file, add files "in place" to the file collection

#. update files to be included/excluded from processing based on regex filter (if defined in parameter)

preprocess
~~~~~~~~~~

Special override method (see below for details)

check
~~~~~

Responsible for checking the validity and/or compliance of files in the collection.

#.  determine the type of check to be performed based on the handler parameters and file type

    #. if NetCDF and compliance checks defined in parameters, check against listed check suites
    #. if NetCDF and no compliance checks defined, validate NetCDF format
    #. if known file type, validate file format (e.g. if .pdf extension, validate PDF format)  # TODO
    #. if unknown file type, check that the file is not empty

process
~~~~~~~

Special override method (see below for details)

publish
~~~~~~~

Responsible for publishing the file to external repositories. This is a composite step, and will perform the following
actions only on files in the collection which have been flagged for that action (as determined by the publish_type
attribute of the files).

#. determine files flagged as needing to be archived, and upload to 'archive' location
#. determine files flagged as needing to be harvested, match and execute Talend harvester(s) for files
#. determine files flagged as needing to be uploaded, and upload to 'upload'

postprocess
~~~~~~~~~~~

Special override method (see below for details)

notify
~~~~~~

Responsible for notifying the uploader and/or the pipeline 'owner' of the result of the handler attempt.

#. determine the recipients, based on notification parameters and handler result
#. send notifications

Customising handler behaviour
-----------------------------

Methods
~~~~~~~

The methods in the HandlerBase (and therefore any subclasses inheriting from it) can be separated into two categories:

*Internal / non-public methods*

These methods must *not* be overridden by child handlers, or the handler behaviour will be compromised. In following the
Python convention, these methods begin with a single underscore (_) character. Note that this is a convention, and
therefore it is possible to manipulate or even override them, however it is mandatory that the conventions are followed
to maintain the integrity of the handler execution.

In addition to any methods starting with one or more underscores, the ``run`` method is also a special case, which must
*not* be overridden or extended, as this is the entry point for handler execution. This is implemented and run
separately from the class initialiser (```__init__```) such that the handler instance can be created, and have it's
contents inspected (e.g. by unit tests) before and after actually executing the file processing code of the handler.

*Public methods*

There are three special methods defined which are *intended* to be overridden by subclasses in order to provide a
handler author with the ability to call code in order to modify the behaviour of the handler during it's execution.

The special methods are: ``preprocess``, ``process`` and ``postprocess``

These methods are deliberately left empty (i.e. they are there but don't do anything) in the base class, so it is purely
optional whether the subclass implements these.

The only difference between these methods is *when* they are called by the handler state machine. Refer to the above
section for further details about where they appear in the steps order.

Attributes
----------

A handler instance contains a number of attributes which control or modify the behaviour of the handler. The attributes
are typically set from the **params** key of the watch configuration, or as initialisation parameters to ``__init__``
method of a handler subclass (e.g. when writing tests).

Class parameters
~~~~~~~~~~~~~~~~

The class parameters are also assigned to instance attributes of the same name, as a convenience.

A handler instantiated with any of these class parameters may also access them from the class instance::

    from aodncore.pipeline import HandlerBase
    from aodncore.pipeline.config import CONFIG


    class MyHandler(HandlerBase):
        def print_upload_path(self):
            # Note: when accessing attributes from within the class itself, the
            # usual Python 'self.attr' convention applies to access the *current* instance
            print(self.upload_path)


    h = MyHandler('/path/to/input/file.nc', config=CONFIG,
                  upload_path='/original/incoming/path/file.nc')

    # 'input_file' parameter is now available as the 'input_file' attribute
    h.input_file
    '/path/to/input/file.nc'

    # 'upload_path' parameter is now available as the 'upload_path' attribute
    h.upload_path
    '/original/incoming/path/file.nc'

    # 'config' parameter is now available as the 'config' attribute
    h.config
    <aodncore.pipeline.configlib.LazyConfigManager object at 0x7f22230c5990>

    h.print_upload_path()
    /original/incoming/path/file.nc

Examples
--------

Writing a :meth:`dest_path` function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Writing a :meth:`dest_path` function with an unmodified filename::

    import os

    class MyHandler(HandlerBase):
        def dest_path(self, file_path):
            basename = os.path.basename(file_path)
            dest_filename = "IMOS_filename_01_XX_{basename}".format(basename=basename)
            return os.path.join('IMOS/MYFACILITY', dest_filename)

* Writing a :meth:`dest_path` function based on contents of a NetCDF file::

    import os
    from netCDF4 import Dataset

    class MyHandler(HandlerBase):
        def dest_path(self, file_path):
            with Dataset(file_path, mode='r') as d:
                site_code = d.site_code

            dest_filename = "IMOS_filename_00_{site_code}.nc".format(site_code=site_code)
            return os.path.join('IMOS/MYFACILITY', dest_filename)

Creating products during the handler lifetime
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Create a simple product during the :meth:`preprocess` step and add to the file collection::

    import os
    from aodncore.pipeline import PipelineFile

    class MyHandler(HandlerBase):
        def preprocess(self):
            # create the product
            product_path = os.path.join(self.products_dir, 'product.txt')
            with open(product_path, 'w') as f:
                f.write('some file contents' + os.linesep)

            # create a PipelineFile to represent the product file,
            # set it's 'publish type' attribute
            # and add it to the handler's file collection
            product = PipelineFile(product_path)
            product.publish_type = PipelineFilePublishType.UPLOAD_ONLY
            self.collection.add(product)

Overriding default file actions
~~~~~~~~~~~~~~~~

* Set all '.txt' files to UPLOAD_ONLY publish type in the :meth:`preprocess` step::

    class MyHandler(HandlerBase):
        def preprocess(self):
            # use of filter methods can reduce excessive nesting of 'if' and 'for' statements
            txt_files = self.file_collection.filter_by_attribute_value('extension', '.txt')
            for pf in txt_files:
                pf.publish_type = PipelineFilePublishType.UPLOAD_ONLY

        def preprocess(self):
            # functionally equivalent to the above example
            for pf in txt_files:
                if pf.extension == '.txt':
                    pf.publish_type = PipelineFilePublishType.UPLOAD_ONLY


* Do not perform any checks on PDF (.pdf) files::

    class MyHandler(HandlerBase):
        def preprocess(self):
            pdf_files = self.file_collection.filter_by_attribute_value('extension', '.pdf')
            for pf in pdf_files:
                pf.check_type = PipelineFileCheckType.NO_ACTION

"""

import logging
import logging.config
import os
from datetime import datetime
from tempfile import gettempdir

from transitions import Machine

from .common import FileType, HandlerResult, PipelineFilePublishType, PipelineFileCheckType, validate_publishtype
from .configlib import validate_lazyconfigmanager
from .destpath import get_path_function
from .exceptions import (PipelineProcessingError, HandlerAlreadyRunError, InvalidConfigError, InvalidInputFileError,
                         InvalidFileFormatError, MissingConfigParameterError)
from .files import PipelineFile, PipelineFileCollection
from .log import SYSINFO, get_pipeline_logger
from .schema import validate_check_params, validate_harvest_params, validate_notify_params, validate_resolve_params
from .steps import (get_cc_module_versions, get_check_runner, get_harvester_runner, get_notify_runner,
                    get_resolve_runner, get_upload_runner)
from ..util import (format_exception, get_file_checksum, iter_public_attributes, merge_dicts, validate_bool,
                    TemporaryDirectory)

__all__ = [
    'HandlerBase'
]

FALLBACK_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
FALLBACK_LOG_LEVEL = SYSINFO


class HandlerBase(object):
    """Base class for pipeline handler sub-classes.

    Implements common handler methods and defines state machine states and transitions.

    :param input_file: Path to the file being handled. A non-existent file will cause the handler to exit with an error
        during the initialise step.

        .. note:: :attr:`input_file` is the only positional argument. Other arguments may be provided in any order.
    :type input_file: str

    :param allowed_extensions: List of allowed extensions for :attr:`input_file`. Non-matching input files with cause
        the handler to exit with an error during the initialise step.
    :type allowed_extensions: list

    :param archive_input_file: Flags whether the original input file should be uploaded to the archive, the location of
        which is configured by the environment configuration. The file will be archived at
        ARCHIVE_URI/PIPELINE_NAME/BASENAME.
    :type archive_input_file: bool

    :param archive_path_function: See :attr:`dest_path_function`. This operates identically, except that it is used to
        calculate the :attr:`PipelineFile.archive_path` attribute and that the path is relative to the ARCHIVE_URI.
    :type archive_path_function: str, function

    :param celery_task: A Celery task object, in order for the handler instance to derive runtime information such as
        the current task name and UUID.

        .. note:: If absent (e.g. when unit testing), the handler will revert to having no task information available,
            and will log output to standard output.
    :type celery_task: :class:`celery.Task`

    :param check_params: A dict containing parameters passed directly to the check step (e.g. compliance checker
        suites). The structure of the dict is defined by the :const:`CHECK_PARAMS_SCHEMA` object in the
        :mod:`aodncore.pipeline.schema` module.
    :type check_params: dict

    :param config: A configuration object which the handler uses to retrieve configuration from it's environment. If
        absent, the handler will exit with an error during the :meth:`__init__` method (i.e. will not
        instantiate).

        .. note:: While this attribute is mandatory, it is not generally required to supply it directly in normal use
            cases, unless instantiating the handler class manually.

            When deployed, the parameter is supplied by the worker service configuration.

            When testing, unit tests inheriting from :class:`HandlerTestCase` contain a pre-prepared config object
            available as :attr:`self.config`. The :meth:`HandlerTestCase.run_handler` and
            :meth:`HandlerTestCase.run_handler_with_exception` helper methods automatically assign the test config to
            the handler being tested.
    :type config: :class:`aodncore.pipeline.config.LazyConfigManager`

    :param dest_path_function: The function used to determine the :attr:`PipelineFile.dest_path` attribute, relative to
        the UPLOAD_URI configuration item. If absent, the handler will attempt to use the :meth:`dest_path` method in
        the handler itself. If a function is not found by either mechanism, the handler will exit with an error during
        the initialise step.

        .. note:: When the value is a string, it is assumed that it refers to the name of a function advertised in the
            *pipeline.handlers* entry point group.
    :type dest_path_function: str, function

    :param exclude_regexes: See :attr:`include_regexes`.
    :type exclude_regexes: list

    :param harvest_params: A dict containing parameters passed directly to the harvest step (e.g. slice size,
        undo behaviour). The structure of the dict is defined by the :const:`HARVEST_PARAMS_SCHEMA` object in the
        :mod:`aodncore.pipeline.schema` module.
    :type harvest_params: dict

    :param harvest_type: String to inform the :mod:`aodncore.pipeline.steps.harvest` step factory function which
        HarvesterRunner implementation to use during the publish step.

        .. note:: Currently the only valid value is 'talend', which is the default.
    :type harvest_type: str

    :param include_regexes: A list of regexes which, when combined with :attr:`exclude_regexes`, determines which files
        in the collection are assigned with the :attr:`default_addition_publish_type` or
        :attr:`default_deletion_publish_type` types (depending on whether the file is an addition or a deletion). If
        set, to be considered included, file paths must match one of the regexes in :attr:`include_regexes` but *not*
        any of the regexes in :attr:`exclude_regexes`.

        Files not matching the inclusion criteria will remain with a :attr:`publish_type` attribute of
        :const:`PipelineFilePublishType.NO_ACTION`, meaning they will be ignored by the publish step.

        .. note:: If omitted, the default is to select *all* files in :attr:`file_collection` for publication.

        .. note:: This relates only to the files in :attr:`file_collection`, and has no relation to the
            :attr:`input_file` path, unless the input file is itself in the collection (e.g. when handling a single
            file).

            For example, a single '.nc' file could feasibly match the :attr:`allowed_extensions` for the handler, but
            still be excluded by this mechanism once it is added to :attr:`file_collection` during the
            :mod:`aodncore.pipeline.steps.resolve` step.

    :type include_regexes: list

    :param notify_params: A dict containing parameters passed directly to the :mod:`aodncore.pipeline.steps.notify` step
        (e.g. owner/success/failure notify lists). The structure of the dict is defined by the
        :const:`NOTIFY_PARAMS_SCHEMA` object in the :mod:`aodncore.pipeline.schema` module.
    :type notify_params: dict

    :param upload_path: A string attribute to hold the original upload path of the :attr:`input_file`.

        .. note:: This is intended for information purposes only (e.g. to appear in notification templates), since there
            is a distinction between the original path, and the :attr:`input_file` as provided to the handler, which
            represents where the file was moved to for processing.
    :type upload_path: str

    :param resolve_params: A dict containing parameters passed directly to the resolve step (e.g. the root path
        prepended to relative paths in manifest files). The structure of the dict is defined by the
        :const:`RESOLVE_PARAMS_SCHEMA` object in the :mod:`aodncore.pipeline.schema` module.
    :type resolve_params: dict

    :param kwargs: Any additional keyword arguments passed to the handler are ignored by the :class:`HandlerBase`` base
        class. This is to leave open the ability for handler specific params to be passed from the watch configuration
        to control some arbitrary handler behaviour, without interfering with the core state machine operation.

    """

    ordered_states = [
        'HANDLER_INITIAL',
        'HANDLER_INITIALISED',
        'HANDLER_RESOLVED',
        'HANDLER_PREPROCESSED',
        'HANDLER_CHECKED',
        'HANDLER_PROCESSED',
        'HANDLER_PUBLISHED',
        'HANDLER_POSTPROCESSED'
    ]

    other_states = [
        'HANDLER_NOTIFIED_SUCCESS',
        'HANDLER_NOTIFIED_ERROR',
        'HANDLER_COMPLETED_SUCCESS',
        'HANDLER_COMPLETED_ERROR'
    ]

    all_states = ordered_states[:]
    all_states.extend(other_states)

    ordered_transitions = [
        {
            'trigger': '_trigger_initialise',
            'source': 'HANDLER_INITIAL',
            'dest': 'HANDLER_INITIALISED',
            'before': '_initialise'
        },
        {
            'trigger': '_trigger_resolve',
            'source': 'HANDLER_INITIALISED',
            'dest': 'HANDLER_RESOLVED',
            'before': '_resolve'
        },
        {
            'trigger': '_trigger_preprocess',
            'source': 'HANDLER_RESOLVED',
            'dest': 'HANDLER_PREPROCESSED',
            'before': 'preprocess'
        },
        {
            'trigger': '_trigger_check',
            'source': 'HANDLER_PREPROCESSED',
            'dest': 'HANDLER_CHECKED',
            'before': '_check'
        },
        {
            'trigger': '_trigger_process',
            'source': 'HANDLER_CHECKED',
            'dest': 'HANDLER_PROCESSED',
            'before': 'process'
        },
        {
            'trigger': '_trigger_publish',
            'source': 'HANDLER_PROCESSED',
            'dest': 'HANDLER_PUBLISHED',
            'before': '_publish'
        },
        {
            'trigger': '_trigger_postprocess',
            'source': 'HANDLER_PUBLISHED',
            'dest': 'HANDLER_POSTPROCESSED',
            'before': 'postprocess'
        }
    ]

    other_transitions = [
        {
            'trigger': '_trigger_notify_success',
            'source': 'HANDLER_POSTPROCESSED',
            'dest': 'HANDLER_NOTIFIED_SUCCESS',
            'before': '_notify_success'
        },
        {
            'trigger': '_trigger_notify_error',
            'source': ordered_states,  # note: reference to ordered_states list, not a string
            'dest': 'HANDLER_NOTIFIED_ERROR',
            'before': '_notify_error'
        },
        {
            'trigger': '_trigger_complete_success',
            'source': 'HANDLER_NOTIFIED_SUCCESS',
            'dest': 'HANDLER_COMPLETED_SUCCESS',
            'before': '_complete_success'
        },
        {
            'trigger': '_trigger_complete_with_errors',
            'source': 'HANDLER_NOTIFIED_ERROR',
            'dest': 'HANDLER_COMPLETED_ERROR',
            'before': '_complete_with_errors'
        }
    ]

    all_transitions = ordered_transitions[:]
    all_transitions.extend(other_transitions)

    def __init__(self, input_file,
                 allowed_extensions=None,
                 archive_input_file=False,
                 archive_path_function=None,
                 celery_task=None,
                 check_params=None,
                 config=None,
                 dest_path_function=None,
                 exclude_regexes=None,
                 harvest_params=None,
                 harvest_type='talend',
                 include_regexes=None,
                 notify_params=None,
                 upload_path=None,
                 resolve_params=None,
                 **kwargs):

        # property backing variables
        self._cc_versions = None
        self._config = None
        self._default_addition_publish_type = PipelineFilePublishType.HARVEST_UPLOAD
        self._default_deletion_publish_type = PipelineFilePublishType.DELETE_UNHARVEST
        self._error = None
        self._file_checksum = None
        _, self._file_extension = os.path.splitext(input_file)
        self._file_type = FileType.get_type_from_extension(self.file_extension)
        self._is_archived = False
        self._result = HandlerResult.UNKNOWN
        self._start_time = datetime.now()

        # public attributes
        self.input_file = input_file
        self.allowed_extensions = allowed_extensions
        self.archive_input_file = archive_input_file
        self.archive_path_function = archive_path_function
        self.celery_task = celery_task
        self.check_params = check_params
        self.config = config
        self.dest_path_function = dest_path_function
        self.exclude_regexes = exclude_regexes
        self.harvest_params = harvest_params
        self.harvest_type = harvest_type
        self.include_regexes = include_regexes
        self.notify_params = notify_params
        self.upload_path = upload_path
        self.resolve_params = resolve_params

        self.file_collection = PipelineFileCollection()

        self._archive_path_function_ref = None
        self._archive_path_function_name = None
        self._dest_path_function_ref = None
        self._dest_path_function_name = None
        self._error_details = None
        self._handler_run = False
        self._instance_working_directory = None
        self._notification_results = None
        self._should_notify = None

        self._machine = Machine(model=self, states=HandlerBase.all_states, initial='HANDLER_INITIAL',
                                auto_transitions=False, transitions=HandlerBase.all_transitions,
                                after_state_change='_after_state_change')

    def __iter__(self):
        ignored_attributes = {'celery_task', 'config', 'default_addition_publish_type', 'default_deletion_publish_type',
                              'logger', 'state', 'trigger'}
        ignored_attributes.update("is_{state}".format(state=s) for s in self.all_states)

        return iter_public_attributes(self, ignored_attributes)

    def __str__(self):
        return "{cls}({attrs})".format(cls=self.__class__.__name__, attrs=dict(self))

    #
    # properties
    #

    @property
    def celery_task_id(self):
        """Read-only property to access Celery task ID

        :return: Celery task ID (if applicable)
        :rtype: :class:`str`, :class:`None`
        """
        return self._celery_task_id

    @property
    def celery_task_name(self):
        """Read-only property to access Celery task name

        :return: Celery task name (if applicable)
        :rtype: :class:`str`, :class:`None`
        """
        return self._celery_task_name

    @property
    def cc_versions(self):
        """Read-only property to access compliance checker module versions

        :return: compliance checker version strings for core and plugin modules
        :rtype: :class:`dict`
        """
        return self._cc_versions

    @property
    def config(self):
        """Read-only property to access the :attr:`config` attribute

        :return: configuration object
        :rtype: :class:`aodncore.pipeline.config.LazyConfigManager`
        """
        return self._config

    @config.setter
    def config(self, config):
        validate_lazyconfigmanager(config)
        self._config = config

    @property
    def error(self):
        """Read-only property to access Exception object from handler instance

        :return: the exception object which caused the handler to fail (if applicable)
        :rtype: :class:`Exception`, :class:`None`
        """
        return self._error

    @property
    def error_details(self):
        """Read-only property to retrieve string description of error from handler instance

        :return: error description string (if applicable)
        :rtype: :class:`str`, :class:`None`
        """
        return self._error_details

    @property
    def file_checksum(self):
        """Read-only property to access the input_file checksum

        :return: :attr:`input_file` checksum string
        :rtype: :class:`str`
        """
        return self._file_checksum

    @property
    def file_extension(self):
        """Read-only property to access the input_file extension

        :return: :attr:`input_file` extension string
        :rtype: :class:`str`
        """
        return self._file_extension

    @property
    def file_type(self):
        """Read-only property to access the input_file type

        :return: :attr:`input_file` type
        :rtype: :class:`FileType`
        """
        return self._file_type

    @property
    def instance_working_directory(self):
        """Read-only property to retrieve the instance working directory

        :return: string containing path to top level working directory for this instance
        :rtype: :class:`str`, :class:`None`
        """
        return self._instance_working_directory

    @property
    def notification_results(self):
        """Read-only property to retrieve the notification results, including the sent status of each recipient

        :return: list of :class:`aodncore.pipeline.steps.notify.NotifyRecipient` instances
        :rtype: :class:`aodncore.pipeline.steps.notify.NotifyList`
        """
        return self._notification_results

    @property
    def result(self):
        """Read-only property to retrieve the overall end result of the handler instance

        :return: handler result
        :rtype: :class:`aodncore.pipeline.common.HandlerResult`
        """
        return self._result

    @property
    def should_notify(self):
        """Read-only property to retrieve the list of intended recipients *after* being assembled based on notify_params

        :return: list of intended recipients
        :rtype: :class:`list`
        """
        return self._should_notify

    @property
    def start_time(self):
        """Read-only property containing the timestamp of when this instance was created

        :return: timestamp of handler starting time
        :rtype: :class:`datetime.datetime`
        """
        return self._start_time

    @property
    def default_addition_publish_type(self):
        """Property to manage attribute which determines the default publish type assigned to 'addition' PipelineFiles

        :return: default addition publish type
        :rtype: :class:`aodncore.pipeline.common.PipelinePublishType`
        """
        return self._default_addition_publish_type

    @default_addition_publish_type.setter
    def default_addition_publish_type(self, publish_type):
        validate_publishtype(publish_type)
        self._default_addition_publish_type = publish_type

    @property
    def default_deletion_publish_type(self):
        """Property to manage attribute which determines the default publish type assigned to 'deletion' PipelineFiles

        :return: default deletion publish type
        :rtype: :class:`aodncore.pipeline.common.PipelinePublishType`
        """
        return self._default_deletion_publish_type

    @default_deletion_publish_type.setter
    def default_deletion_publish_type(self, publish_type):
        validate_publishtype(publish_type)
        self._default_deletion_publish_type = publish_type

    @property
    def is_archived(self):
        """Boolean property indicating whether the input_file has been archived

        :return: whether the :attr:`input_file` has been archived or not
        :rtype: :class:`bool`
        """
        return self._is_archived

    @is_archived.setter
    def is_archived(self, is_archived):
        validate_bool(is_archived)
        self._is_archived = is_archived

    @property
    def collection_dir(self):
        """Temporary subdirectory where collection will be unpacked

        .. note:: physical directory should not be manipulated directly by handler, rather the files should be managed
            via their corresponding :class:`PipelineFile
        :return: collection subdirectory of instance working directory (as populated by
            :mod:`aodncore.pipeline.steps.resolve` step)
        :rtype: :class:`str`, :class:`None`
        """
        if self._instance_working_directory:
            return os.path.join(self._instance_working_directory, 'collection')

    @property
    def products_dir(self):
        """Temporary subdirectory in which products may be created

        :return: products subdirectory of instance working directory
        :rtype: :class:`str`, :class:`None`
        """
        if self._instance_working_directory:
            return os.path.join(self._instance_working_directory, 'products')

    @property
    def temp_dir(self):
        """Temporary subdirectory where any other arbitrary temporary files may be created by handler sub-classes

        :return: temporary subdirectory of instance working directory
        :rtype: :class:`str`, :class:`None`
        """
        if self._instance_working_directory:
            return os.path.join(self._instance_working_directory, 'temp')

    #
    # 'before' methods for ordered state machine transitions
    #

    def _initialise(self):
        """Perform basic initialisation tasks that must occur *before* any file handling commences.
        
        ORM is initialised in a finally in order to record failed executions of the handler (e.g. non-existent input
        files)

        :return: None
        """
        self._init_logging()
        self._validate_params()
        self._set_checksum()
        self._check_extension()
        self._set_cc_versions()
        self._set_path_functions()
        self._init_working_directory()

    def _resolve(self):
        """Determine the list of file candidates by expanding the input file (if applicable) and filter the candidates
            according to the supplied regular expressions

        :return: None
        """
        resolve_runner = get_resolve_runner(self.input_file, self.collection_dir, self.config, self.logger,
                                            self.resolve_params)
        self.logger.sysinfo("get_resolve_runner -> '{runner}'".format(runner=resolve_runner.__class__.__name__))
        resolved_files = resolve_runner.run()

        resolved_files.set_file_update_callback(self._file_update_callback)
        resolved_files.set_default_publish_types(self.include_regexes, self.exclude_regexes,
                                                 self.default_addition_publish_type,
                                                 self.default_deletion_publish_type)

        self.file_collection.update(resolved_files)

    def _check(self):
        check_runner = get_check_runner(self.config, self.logger, self.check_params)
        self.logger.sysinfo("get_check_runner -> '{runner}'".format(runner=check_runner.__class__.__name__))
        self.file_collection.set_check_types(self.check_params)
        files_to_check = PipelineFileCollection(
            f for f in self.file_collection if f.check_type is not PipelineFileCheckType.NO_ACTION)

        if files_to_check:
            check_runner.run(files_to_check)

    def _archive(self, upload_runner):
        files_to_archive = self.file_collection.filter_by_bool_attribute('pending_archive')

        if self.archive_input_file:
            input_file_obj = PipelineFile(self.input_file, archive_path=os.path.join(self._pipeline_name,
                                                                                     os.path.basename(self.input_file)))
            input_file_obj.publish_type = PipelineFilePublishType.ARCHIVE_ONLY
            infile_collection = PipelineFileCollection(input_file_obj)
            upload_runner.run(infile_collection)
            self.is_archived = input_file_obj.is_archived

        if files_to_archive:
            self.file_collection.set_archive_paths(self._archive_path_function_ref)
            upload_runner.run(files_to_archive)

    def _harvest(self, upload_runner):
        harvest_runner = get_harvester_runner(self.harvest_type, upload_runner, self.harvest_params, self.temp_dir,
                                              self.config, self.logger)
        self.logger.sysinfo("get_harvester_runner -> '{runner}'".format(runner=harvest_runner.__class__.__name__))
        files_to_harvest = self.file_collection.filter_by_bool_attribute('pending_harvest')

        if files_to_harvest:
            harvest_runner.run(files_to_harvest)

    def _store_unharvested(self, upload_runner):
        files_to_store = self.file_collection.filter_by_bool_attribute('pending_store')

        if files_to_store:
            upload_runner.run(files_to_store)

    def _publish(self):
        archive_runner = get_upload_runner(self._config.pipeline_config['global']['archive_uri'], self._config,
                                           self.logger, archive_mode=True)
        self.logger.sysinfo(
            "get_upload_runner (archive) -> '{runner}'".format(runner=archive_runner.__class__.__name__))

        self._archive(archive_runner)

        upload_runner = get_upload_runner(self._config.pipeline_config['global']['upload_uri'], self._config,
                                          self.logger)
        self.logger.sysinfo("get_upload_runner -> '{runner}'".format(runner=upload_runner.__class__.__name__))

        self.file_collection.set_dest_paths(self._dest_path_function_ref)

        upload_runner.set_is_overwrite(self.file_collection)

        self._harvest(upload_runner)
        self._store_unharvested(upload_runner)

    #
    # 'before' methods for non-ordered state machine transitions
    #

    def _notify_common(self):
        collection_headers, collection_data = self.file_collection.get_table_data()
        checks = () if self.check_params is None else self.check_params.get('checks', ())

        class_dict = dict(self)
        extra = {
            'input_file': os.path.basename(self.input_file),
            'processing_result': self.result.name,
            'handler_start_time': self.start_time.strftime("%Y-%m-%d %H:%M"),
            'checks': ','.join(checks) or 'None',
            'collection_headers': collection_headers,
            'collection_data': collection_data,
            'error_details': self.error_details or False,
            'upload_dir': os.path.dirname(self.upload_path) if self.upload_path else None
        }
        notification_data = merge_dicts(class_dict, extra)

        notify_runner = get_notify_runner(notification_data, self.config, self.logger, self.notify_params)
        self.logger.sysinfo("get_notify_runner -> '{runner}'".format(runner=notify_runner.__class__.__name__))

        if self._should_notify:
            self._notification_results = notify_runner.run(self._should_notify)

    def _notify_success(self):
        self._notify_common()

    def _notify_error(self):
        self._notify_common()

    def _complete_common(self):
        self.logger.info(
            "handler result for input_file '{name}': {result}".format(name=self.input_file, result=self._result.name))

    def _complete_success(self):
        self._complete_common()

    def _complete_with_errors(self):
        self._complete_common()

    #
    # callbacks
    #

    def _after_state_change(self):
        """Method run after each successful state transition

        :return: None
        """
        self.logger.sysinfo(
            "{name} transitioned to state: {state}".format(name=self.__class__.__name__, state=self.state))
        if self.celery_task is not None:
            self.celery_task.update_state(state=self.state)

    def _file_update_callback(self, name, message=None):
        """Called by steps to notify of a file state update

        :return: None
        """
        self.logger.info("file: '{name}' {message}".format(name=name, message=message))

    #
    # "internal" helper methods
    #

    def _check_extension(self):
        """Check that the input file has a valid extension (if allowed_extensions defined)

        :return: None
        """
        if self.allowed_extensions and self.file_extension not in self.allowed_extensions:
            raise InvalidFileFormatError(
                "input file extension '{extension}' not in allowed_extensions list: {allowed}".format(
                    extension=self.file_extension, allowed=self.allowed_extensions))

    def _init_logging(self):
        """Initialise logging, including Celery integration

        :return: None
        """
        try:
            celery_task_id = self.celery_task.request.id
            celery_task_name = self.celery_task.name
            pipeline_name = self.celery_task.pipeline_name
            self.logger = self.celery_task.logger
        except AttributeError as e:
            # the absence of a celery task indicates we're in a unittest or IDE, so fall-back to basic logging config
            celery_task_id = None
            celery_task_name = 'NO_TASK'
            pipeline_name = 'NO_PIPELINE'
            logging.basicConfig(level=FALLBACK_LOG_LEVEL, format=FALLBACK_LOG_FORMAT)

            logging_extra = {
                'celery_task_id': celery_task_id,
                'celery_task_name': celery_task_name,
                'pipeline_name': pipeline_name
            }
            logger = get_pipeline_logger('', logging_extra)

            # turn down logging for noisy libraries to WARN, unless overridden in pipeline config 'liblevel' key
            liblevel = getattr(self.config, 'pipeline_config', {}).get('logging', {}).get('liblevel', 'WARN')
            for lib in ('botocore', 'paramiko', 's3transfer', 'transitions'):
                logging.getLogger(lib).setLevel(liblevel)

            logger.warning('no logger parameter or celery task found, falling back to root logger')
            logger.debug('_init_logging exception: {e}'.format(e=e))
            self.logger = logger

        self._celery_task_id = celery_task_id
        self._celery_task_name = celery_task_name
        self._pipeline_name = pipeline_name

        self.logger.info("running handler -> '{str}'".format(str=self))

    def _init_working_directory(self):
        """Initialise the working directory
        
        :return: None
        """
        for subdirectory in ('collection', 'products', 'temp'):
            os.mkdir(os.path.join(self._instance_working_directory, subdirectory))

    def _handle_error(self, exception, system_error=False):
        """Update error details with exception details
        
        :param exception: exception instance being handled 
        :return: None
        """
        self._error = exception
        self._result = HandlerResult.ERROR

        should_notify = []
        notify_params_dict = self.notify_params or {}

        try:
            if system_error:
                self.logger.exception(format_exception(exception))

                import traceback
                self._error_details = traceback.format_exc()

                # invalid configuration means notification is not possible
                if not isinstance(exception, (InvalidConfigError, MissingConfigParameterError)):
                    should_notify.extend(notify_params_dict.get('owner_notify_list', []))

            else:
                self.logger.error(format_exception(exception))
                self._error_details = str(exception)
                should_notify.extend(notify_params_dict.get('error_notify_list', []))

                if notify_params_dict.get('notify_owner_error', False):
                    should_notify.extend(notify_params_dict.get('owner_notify_list', []))

            self._should_notify = should_notify

            self._trigger_notify_error()
            self._trigger_complete_with_errors()
        except Exception as e:
            self.logger.exception('error during _handle_error method: {e}'.format(e=format_exception(e)))

    def _handle_success(self):
        self._result = HandlerResult.SUCCESS

        notify_params_dict = self.notify_params or {}
        should_notify = notify_params_dict.get('success_notify_list', [])

        if notify_params_dict.get('notify_owner_success', False):
            should_notify.extend(notify_params_dict.get('owner_notify_list', []))

        self._should_notify = should_notify

        try:
            self._trigger_notify_success()
            self._trigger_complete_success()
        except Exception as e:
            self.logger.exception('error during _handle_success method: {e}'.format(e=format_exception(e)))

    def _set_cc_versions(self):
        self._cc_versions = get_cc_module_versions()
        self.logger.sysinfo("get_cc_module_versions -> {versions}".format(versions=self._cc_versions))

    def _set_checksum(self):
        try:
            self._file_checksum = get_file_checksum(self.input_file)
        except (IOError, OSError) as e:
            self.logger.exception(e)
            raise InvalidInputFileError(e)
        self.logger.sysinfo("get_file_checksum -> '{checksum}'".format(checksum=self.file_checksum))

    def _set_path_functions(self):
        """Determine functions to use for publishing destination and archive path resolution

        :return: None
        """
        dest_path_function_ref, dest_path_function_name = get_path_function(self, self.config.pipeline_config[
            'pluggable']['path_function_group'])
        self._dest_path_function_ref = dest_path_function_ref
        self._dest_path_function_name = dest_path_function_name
        self.logger.sysinfo(
            "get_path_function -> '{function}'".format(function=self._dest_path_function_name))

        archive_path_function_ref, archive_path_function_name = get_path_function(self, self.config.pipeline_config[
            'pluggable']['path_function_group'], archive_mode=True)
        self._archive_path_function_ref = archive_path_function_ref
        self._archive_path_function_name = archive_path_function_name
        self.logger.sysinfo(
            "get_path_function (archive) -> '{function}'".format(function=self._archive_path_function_name))

    def _validate_params(self):
        """Validate *_params dicts against their respective schemas

        :return: None
        """
        if self.check_params:
            validate_check_params(self.check_params)
        if self.harvest_params:
            validate_harvest_params(self.harvest_params)
        if self.notify_params:
            validate_notify_params(self.notify_params)
        if self.resolve_params:
            validate_resolve_params(self.resolve_params)

    #
    # process methods - to be overridden by child class as required
    #

    def preprocess(self):  # pragma: no cover
        """Method designed to be overridden by child handlers in order to execute code between resolve and check steps

        :return: None
        """
        self.logger.sysinfo("`preprocess` not overridden by child class, skipping step")

    def process(self):  # pragma: no cover
        """Method designed to be overridden by child handlers in order to execute code between check and publish steps

        :return: None
        """
        self.logger.sysinfo("`process` not overridden by child class, skipping step")

    def postprocess(self):  # pragma: no cover
        """Method designed to be overridden by child handlers in order to execute code between publish and notify steps

        :return: None
        """
        self.logger.sysinfo("`postprocess` not overridden by child class, skipping step")

    #
    # "public" methods
    #

    def run(self):
        """The entry point to the handler instance. Executes the automatic state machine transitions, and populates the
            :attr:`result` attribute to signal success or failure of the handler instance.
        """
        if self._handler_run:
            raise HandlerAlreadyRunError('handler instance has already been run')
        self._handler_run = True

        base_temp_directory = self.config.pipeline_config['global'].get('tmp_dir', gettempdir())
        with TemporaryDirectory(prefix=self.__class__.__name__, dir=base_temp_directory) as instance_working_directory:
            self._instance_working_directory = instance_working_directory
            try:
                for transition in HandlerBase.ordered_transitions:
                    self.trigger(transition['trigger'])
            except PipelineProcessingError as e:
                self._handle_error(e)
            except (Exception, KeyboardInterrupt, SystemExit) as e:
                self._handle_error(e, system_error=True)
            else:
                self._handle_success()

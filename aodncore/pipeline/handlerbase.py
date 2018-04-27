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
from .statequery import StateQuery
from .steps import (get_cc_module_versions, get_check_runner, get_harvester_runner, get_notify_runner,
                    get_resolve_runner, get_store_runner)
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

        .. note:: :py:attr:`input_file` is the only positional argument. Other arguments may be provided in any order.

    :type input_file: str

    :param allowed_extensions: List of allowed extensions for :py:attr:`input_file`. Non-matching input files with cause
        the handler to exit with an error during the initialise step.
    :type allowed_extensions: list

    :param archive_input_file: Flags whether the original input file should be uploaded to the archive, the location of
        which is configured by the environment configuration. The file will be archived at
        ARCHIVE_URI/PIPELINE_NAME/BASENAME.
    :type archive_input_file: bool

    :param archive_path_function: See :py:attr:`dest_path_function`. This operates identically, except that it is used
        to calculate the :py:attr:`PipelineFile.archive_path` attribute and that the path is relative to the
        ARCHIVE_URI.
    :type archive_path_function: str, function

    :param celery_task: A Celery task object, in order for the handler instance to derive runtime information such as
        the current task name and UUID.

        .. note:: If absent (e.g. when unit testing), the handler will revert to having no task information available,
            and will log output to standard output.
    :type celery_task: :py:class:`celery.Task`

    :param check_params: A dict containing parameters passed directly to the check step (e.g. compliance checker
        suites). The structure of the dict is defined by the :const:`CHECK_PARAMS_SCHEMA` object in the
        :py:mod:`aodncore.pipeline.schema` module.
    :type check_params: :py:class:`dict`

    :param config: A configuration object which the handler uses to retrieve configuration from it's environment. If
        absent, the handler will exit with an error during the :py:meth:`__init__` method (i.e. will not
        instantiate).

        .. note:: While this attribute is mandatory, it is not generally required to supply it directly in normal use
            cases, unless instantiating the handler class manually.

            When deployed, the parameter is automatically included by the worker service configuration.

            When testing, unit tests inheriting from :py:class:`HandlerTestCase` contain a pre-prepared config object
            available as :attr:`self.config`. The :py:meth:`HandlerTestCase.run_handler` and
            :py:meth:`HandlerTestCase.run_handler_with_exception` helper methods automatically assign the test config to
            the handler being tested.
    :type config: :py:class:`aodncore.pipeline.config.LazyConfigManager`

    :param dest_path_function: The function used to determine the :py:attr:`PipelineFile.dest_path` attribute, relative
        to the UPLOAD_URI configuration item. If absent, the handler will attempt to use the :py:meth:`dest_path` method
        in the handler itself. If a function is not found by either mechanism, the handler will exit with an error
        during the initialise step.

        .. note:: When the value is a string, it is assumed that it refers to the name of a function advertised in the
            *pipeline.handlers* entry point group.
    :type dest_path_function: :py:class:`str`, :py:class:`callable`

    :param exclude_regexes: See :py:attr:`include_regexes`.
    :type exclude_regexes: :py:class:`list`

    :param harvest_params: A dict containing parameters passed directly to the harvest step (e.g. slice size,
        undo behaviour). The structure of the dict is defined by the :py:const:`HARVEST_PARAMS_SCHEMA` object in the
        :py:mod:`aodncore.pipeline.schema` module.
    :type harvest_params: :py:class:`dict`

    :param harvest_type: String to inform the :py:mod:`aodncore.pipeline.steps.harvest` step factory function which
        HarvesterRunner implementation to use during the publish step.

        .. note:: Currently the only valid value is 'talend', which is the default.
    :type harvest_type: :py:class:`str`

    :param include_regexes: A list of regexes which, when combined with :py:attr:`exclude_regexes`, determines which
        files in the collection are assigned with the :py:attr:`default_addition_publish_type` or
        :py:attr:`default_deletion_publish_type` types (depending on whether the file is an addition or a deletion). If
        set, to be considered included, file paths must match one of the regexes in :attr:`include_regexes` but *not*
        any of the regexes in :py:attr:`exclude_regexes`.

        Files not matching the inclusion criteria will remain with a :attr:`publish_type` attribute of
        :py:attr:`PipelineFilePublishType.NO_ACTION`, meaning they will be ignored by the publish step.

        .. note:: If omitted, the default is to select *all* files in :py:attr:`file_collection` for publication.

        .. note:: This relates only to the files in :py:attr:`file_collection`, and has no relation to the
            :py:attr:`input_file` path, unless the input file is itself in the collection (e.g. when handling a single
            file).

            For example, a single '.nc' file could feasibly match the :py:attr:`allowed_extensions` for the handler, but
            still be excluded by this mechanism once it is added to :py:attr:`file_collection` during the
            :py:mod:`aodncore.pipeline.steps.resolve` step.

    :type include_regexes: list

    :param notify_params: A dict containing parameters passed directly to the :py:mod:`aodncore.pipeline.steps.notify`
        step (e.g. owner/success/failure notify lists). The structure of the dict is defined by the
        :py:const:`NOTIFY_PARAMS_SCHEMA` object in the :py:mod:`aodncore.pipeline.schema` module.
    :type notify_params: :py:class:`dict`

    :param upload_path: A string attribute to hold the original upload path of the :py:attr:`input_file`.

        .. note:: This is intended for information purposes only (e.g. to appear in notification templates), since there
            is a distinction between the original path, and the :py:attr:`input_file` as provided to the handler, which
            represents where the file was moved to for processing.

    :type upload_path: :py:class:`str`

    :param resolve_params: A dict containing parameters passed directly to the resolve step (e.g. the root path
        prepended to relative paths in manifest files). The structure of the dict is defined by the
        :py:const:`RESOLVE_PARAMS_SCHEMA` object in the :py:mod:`aodncore.pipeline.schema` module.
    :type resolve_params: :py:class:`dict`

    :param kwargs: Any additional keyword arguments passed to the handler are ignored by the :py:class:`HandlerBase`
        base class. This is to leave open the ability for handler specific params to be passed from the watch
        configuration to control some arbitrary handler behaviour, without interfering with the core state machine
        operation.

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
        self._state_query = None
        self._upload_runner = None
        self._upload_runner_archive = None

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
        """Read-only property to access :py:class:`Exception` object from handler instance

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
        """Read-only property to access the :py:attr:`input_file` checksum

        :return: :attr:`input_file` checksum string
        :rtype: :class:`str`
        """
        return self._file_checksum

    @property
    def file_extension(self):
        """Read-only property to access the :py:attr:`input_file` extension

        :return: :attr:`input_file` extension string
        :rtype: :class:`str`
        """
        return self._file_extension

    @property
    def file_type(self):
        """Read-only property to access the :py:attr:`input_file` type

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
        """Read-only property to retrieve the list of intended recipients *after* being assembled based on
        :py:attr:`notify_params`

        :return: list of intended recipients
        :rtype: :py:class:`list`
        """
        return self._should_notify

    @property
    def start_time(self):
        """Read-only property containing the timestamp of when this instance was created

        :return: timestamp of handler starting time
        :rtype: :py:class:`datetime.datetime`
        """
        return self._start_time

    @property
    def state_query(self):
        """Read-only property containing an initialised StateQuery instance, for querying existing pipeline state

        :return: StateQuery instance
        :rtype: :py:class:`StateQuery`
        """
        return self._state_query

    @property
    def default_addition_publish_type(self):
        """Property to manage attribute which determines the default publish type assigned to 'addition'
        :py:class:`PipelineFile` instances

        :return: default addition publish type
        :rtype: :py:class:`aodncore.pipeline.common.PipelinePublishType`
        """
        return self._default_addition_publish_type

    @default_addition_publish_type.setter
    def default_addition_publish_type(self, publish_type):
        validate_publishtype(publish_type)
        self._default_addition_publish_type = publish_type

    @property
    def default_deletion_publish_type(self):
        """Property to manage attribute which determines the default publish type assigned to 'deletion'
        :py:class:`PipelineFile` instances

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
        """Boolean property indicating whether the :py:attr:`input_file` has been archived

        :return: whether the :py:attr:`input_file` has been archived or not
        :rtype: :class:`bool`
        """
        return self._is_archived

    @is_archived.setter
    def is_archived(self, is_archived):
        validate_bool(is_archived)
        self._is_archived = is_archived

    @property
    def collection_dir(self):
        """Temporary subdirectory where the *initial* input file collection will be unpacked

        .. warning:: Any new files created during the handler execution (i.e. were not in the original input file)
            should be created in :py:attr:`self.products_dir` rather than here.

        :return: collection subdirectory of instance working directory (as populated by
            :py:mod:`aodncore.pipeline.steps.resolve` step)
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
        self._init_logging()
        self._validate_params()
        self._set_checksum()
        self._check_extension()
        self._set_cc_versions()
        self._set_path_functions()
        self._init_working_directory()
        self._init_upload_runners()

    def _resolve(self):
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

    def _archive(self, store_runner):
        files_to_archive = self.file_collection.filter_by_bool_attribute('pending_archive')

        if self.archive_input_file:
            input_file_obj = PipelineFile(self.input_file, archive_path=os.path.join(self._pipeline_name,
                                                                                     os.path.basename(self.input_file)))
            input_file_obj.publish_type = PipelineFilePublishType.ARCHIVE_ONLY
            infile_collection = PipelineFileCollection(input_file_obj)
            store_runner.run(infile_collection)
            self.is_archived = input_file_obj.is_archived

        if files_to_archive:
            store_runner.run(files_to_archive)

    def _harvest(self):
        harvest_runner = get_harvester_runner(self.harvest_type, self._upload_runner.broker, self.harvest_params,
                                              self.temp_dir, self.config, self.logger)
        self.logger.sysinfo("get_harvester_runner -> '{runner}'".format(runner=harvest_runner.__class__.__name__))
        files_to_harvest = self.file_collection.filter_by_bool_attribute('pending_harvest')

        if files_to_harvest:
            harvest_runner.run(files_to_harvest)

    def _store_unharvested(self):
        files_to_store = self.file_collection.filter_by_bool_attribute('pending_store')

        if files_to_store:
            self._upload_runner.run(files_to_store)

    def _publish(self):
        self.file_collection.set_archive_paths(self._archive_path_function_ref)
        self.file_collection.validate_attribute_uniqueness('archive_path')
        self._archive(self._upload_runner_archive)

        self.file_collection.set_dest_paths(self._dest_path_function_ref)
        self.file_collection.validate_attribute_uniqueness('dest_path')

        self._upload_runner.set_is_overwrite(self.file_collection)

        self._harvest()
        self._store_unharvested()

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
        self.logger.sysinfo(
            "{name} transitioned to state: {state}".format(name=self.__class__.__name__, state=self.state))
        if self.celery_task is not None:
            self.celery_task.update_state(state=self.state)

    def _file_update_callback(self, name, message=None):
        self.logger.info("file: '{name}' {message}".format(name=name, message=message))

    #
    # "internal" helper methods
    #

    def _check_extension(self):
        if self.allowed_extensions and self.file_extension not in self.allowed_extensions:
            raise InvalidFileFormatError(
                "input file extension '{extension}' not in allowed_extensions list: {allowed}".format(
                    extension=self.file_extension, allowed=self.allowed_extensions))

    def _init_logging(self):
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

    def _init_upload_runners(self):
        self._upload_runner = get_store_runner(self._config.pipeline_config['global']['upload_uri'], self._config,
                                               self.logger)
        self.logger.sysinfo("get_store_runner -> '{runner}'".format(runner=self._upload_runner.__class__.__name__))

        self._upload_runner_archive = get_store_runner(self._config.pipeline_config['global']['archive_uri'],
                                                       self._config, self.logger, archive_mode=True)
        self.logger.sysinfo(
            "get_store_runner (archive) -> '{runner}'".format(runner=self._upload_runner_archive.__class__.__name__))

        self._state_query = StateQuery(self._upload_runner.broker)

    def _init_working_directory(self):
        for subdirectory in ('collection', 'products', 'temp'):
            os.mkdir(os.path.join(self._instance_working_directory, subdirectory))

    def _handle_error(self, exception, system_error=False):
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

        should_notify = []
        notify_params_dict = self.notify_params or {}

        should_notify.extend(notify_params_dict.get('success_notify_list', []))

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

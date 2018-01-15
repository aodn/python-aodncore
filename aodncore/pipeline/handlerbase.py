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
from .steps import (get_cc_module_versions, get_check_runner, get_harvester_runner, get_notify_runner,
                    get_resolve_runner, get_upload_runner)
from ..util import format_exception, get_file_checksum, merge_dicts, validate_bool, TemporaryDirectory

__all__ = [
    'HandlerBase'
]

FALLBACK_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
FALLBACK_LOG_LEVEL = SYSINFO


class HandlerBase(object):
    """Base class for pipeline handler sub-classes.

    Implements common handler methods and defines state machine states and transitions.
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
                 resolve_params=None,
                 **kwargs):
        """

        Note: input_file *must* remain the only positional parameter. Any additional arguments *must* be added as
        keyword arguments in order to support assumptions about how the class is called by the task handling code.

        :param input_file: input file being handled
        :param allowed_extensions: list of allowed extensions for the input file
        :param archive_input_file: flag to determine whether the original input file is archived
        :param archive_path_function: function reference or entry point used to determine archive_path for a file
        :param celery_task: reference to the Celery task instance which instantiated the handler instance
        :param check_params: list of parameters to passed through to the compliance checker library
        :param config: LazyConfigManager instance
        :param dest_path_function: function reference or entry point used to determine dest_path for a file
        :param exclude_regexes: list of regexes that files matching include_regexes must *not* match to be 'eligible'
        :param harvest_params: keyword parameters passed to the publish step to control harvest runner parameters
        :param harvest_type: determine which harvest type will be used (supported types in harvest module)
        :param include_regexes: list of regexes that files must match to be 'eligible'
        :param notify_params: keyword parameters passed to the notify step to control notification behaviour
        :param resolve_params: keyword parameters passed to the publish step to control harvest runner parameters
        :param kwargs: allow additional keyword arguments to allow potential for child handler to use custom arguments
        """

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
        self.resolve_params = resolve_params

        self.file_collection = PipelineFileCollection()

        self._archive_path_function_ref = None
        self._archive_path_function_name = None
        self._dest_path_function_ref = None
        self._dest_path_function_name = None
        self._error_details = None
        self._handler_run = False
        self._instance_working_directory = None
        self._notify_list = None
        self._machine = Machine(model=self, states=HandlerBase.all_states, initial='HANDLER_INITIAL',
                                auto_transitions=False, transitions=HandlerBase.all_transitions,
                                after_state_change='_after_state_change')

    def __iter__(self):
        ignored_attributes = {'celery_task', 'config', 'default_addition_publish_type', 'default_deletion_publish_type',
                              'logger', 'state', 'trigger'}
        ignored_attributes.update("is_{state}".format(state=s) for s in self.all_states)

        def includeattr(attr):
            if attr.startswith('_') or attr in ignored_attributes:
                return False
            return True

        property_names = {p for p in dir(HandlerBase) if isinstance(getattr(HandlerBase, p), property)}
        properties = {p: str(getattr(self, p)) for p in property_names if includeattr(p)}
        public_attrs = {k: str(v) for k, v in self.__dict__.items() if includeattr(k)}
        public_attrs.update(properties)

        for item in public_attrs.items():
            yield item

    def __str__(self):
        return "{cls}({attrs})".format(cls=self.__class__.__name__, attrs=dict(self))

    #
    # properties
    #

    @property
    def cc_versions(self):
        """Read-only property to retrieve compliance checker module versions

        :return: dict containing compliance checker version strings for core and plugin modules
        """
        return self._cc_versions

    @property
    def config(self):
        """Property to manage config attribute

        :return: LazyConfigManager instance
        """
        return self._config

    @config.setter
    def config(self, config):
        validate_lazyconfigmanager(config)
        self._config = config

    @property
    def error(self):
        """Read-only property to retrieve Exception object from handler instance

        :return: Exception object or None
        """
        return self._error

    @property
    def error_details(self):
        """Read-only property to retrieve string description of error (if applicable) from handler instance

        :return: error description or 'None'
        """
        return self._error_details

    @property
    def file_checksum(self):
        """Read-only property to retrieve the input_file checksum

        :return: checksum string or None
        """
        return self._file_checksum

    @property
    def file_extension(self):
        """Read-only property to retrieve the input_file extension

        :return: extension string
        """
        return self._file_extension

    @property
    def file_type(self):
        """Read-only property to retrieve the input_file type

        :return: FileType member
        """
        return self._file_type

    @property
    def instance_working_directory(self):
        """Read-only property to retrieve the instance working directory

        :return: string containing path to top level working directory for this instance
        """
        return self._instance_working_directory

    @property
    def notify_list(self):
        """Read-only property to retrieve the notification list and sent status of each recipient

        :return: NotifyList instance
        """
        return self._notify_list

    @property
    def result(self):
        """Read-only property to retrieve the overall end result of the handler instance

        :return: HandlerResult member
        """
        return self._result

    @property
    def start_time(self):
        """Read-only property containing the timestamp of when this instance was created

        :return: datetime instance
        """
        return self._start_time

    @property
    def default_addition_publish_type(self):
        """Property to manage attribute which determines the default publish type assigned to 'addition' PipelineFiles

        :return: PipelinePublishType member
        """
        return self._default_addition_publish_type

    @default_addition_publish_type.setter
    def default_addition_publish_type(self, publish_type):
        validate_publishtype(publish_type)
        self._default_addition_publish_type = publish_type

    @property
    def default_deletion_publish_type(self):
        """Property to manage attribute which determines the default publish type assigned to 'deletion' PipelineFiles

        :return: PipelinePublishType member
        """
        return self._default_deletion_publish_type

    @default_deletion_publish_type.setter
    def default_deletion_publish_type(self, publish_type):
        validate_publishtype(publish_type)
        self._default_deletion_publish_type = publish_type

    @property
    def is_archived(self):
        """Boolean property indicating whether the input_file has been archived

        :return: bool
        """
        return self._is_archived

    @is_archived.setter
    def is_archived(self, is_archived):
        validate_bool(is_archived)
        self._is_archived = is_archived

    @property
    def collection_dir(self):
        """Temporary subdirectory where collection will be unpacked

        :return: None
        """
        if self._instance_working_directory:
            return os.path.join(self._instance_working_directory, 'collection')

    @property
    def products_dir(self):
        """Temporary subdirectory in which products will be created

        :return: None
        """
        if self._instance_working_directory:
            return os.path.join(self._instance_working_directory, 'products')

    @property
    def temp_dir(self):
        """Temporary subdirectory where any other temporary files may be created by handler sub-classes

        :return: None
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
        self._harvest(upload_runner)
        self._store_unharvested(upload_runner)

    #
    # 'before' methods for non-ordered state machine transitions
    #

    def _notify_common(self, notify_list_param):
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
        }
        notification_data = merge_dicts(class_dict, extra)
        
        notify_runner = get_notify_runner(notification_data, self.config, self.logger, self.notify_params)
        self.logger.sysinfo("get_notify_runner -> '{runner}'".format(runner=notify_runner.__class__.__name__))

        notify_params_dict = self.notify_params or {}
        notify_list = notify_params_dict.get(notify_list_param)

        if notify_list:
            self._notify_list = notify_runner.run(notify_list)

    def _notify_success(self):
        self._notify_common('success_notify_list')

    def _notify_error(self):
        self._notify_common('error_notify_list')

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
        """Method run after each successful state transition to update state in the DB and log

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

    def _handle_error(self, exception, full_traceback=False):
        """Update error details with exception details
        
        :param exception: exception instance being handled 
        :return: None
        """
        self._error = exception
        self._result = HandlerResult.ERROR

        try:
            if full_traceback:
                self.logger.exception(format_exception(exception))

                import traceback
                self._error_details = traceback.format_exc()

                # invalid configuration means notification is not possible
                if isinstance(exception, (InvalidConfigError, MissingConfigParameterError)):
                    self.notify_on_error = self.notify_on_success = False
                    self.notify_params = {'error_notify_list': []}
                else:
                    self.notify_on_error = True
                    self.notify_params = {
                        'error_notify_list': self.config.pipeline_config['global']['admin_recipients']}
            else:
                self.logger.error(format_exception(exception))
                self._error_details = str(exception)

            self._trigger_notify_error()
            self._trigger_complete_with_errors()
        except Exception as e:
            self.logger.exception('error during _handle_error method: {e}'.format(e=format_exception(e)))

    def _handle_success(self):
        self._result = HandlerResult.SUCCESS

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
        """The entry point to the handler instance
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
                self._handle_error(e, full_traceback=True)
            else:
                self._handle_success()

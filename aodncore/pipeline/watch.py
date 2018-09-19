"""This module provides the code to implement the "watchservice" component of the pipeline.

This includes setting up directory watches, handling incoming inotify events, defining the Celery tasks and
routing/queuing events.

The watchservice itself is designed as an `executable module <https://wiki.python.org/moin/ExecutableModules>`_, with
the entry point being the :py:mod:`aodncore.pipeline.watchservice` module.

This means that once :py:mod:`aodncore` is installed, running the
watchservice is simply a matter of running the following::

    python -m aodncore.pipeline.watchservice

This is typically run as an operating system service by something like supervisord, but can be run from the command-line
for debugging.
"""

from __future__ import absolute_import
import logging.config
import os
import re
import stat
import warnings
from uuid import uuid4

from enum import Enum
from six import PY2
from transitions import Machine

from .files import PipelineFile
from .log import get_pipeline_logger
from .storage import get_storage_broker
from ..util import (ensure_regex_list, format_exception, lazyproperty, mkdir_p, rm_f, rm_r, validate_dir_writable,
                    validate_file_writable, validate_membership)

# OS X test compatibility, due to absence of pyinotify (which is specific to the Linux kernel)
try:
    import pyinotify
except ImportError:
    class pyinotify(object):
        IN_MOVED_TO = 0
        IN_CLOSE_WRITE = 0

        def __init__(self):
            raise NotImplementedError('pyinotify package is not installed')

        class ProcessEvent(object):
            def __init__(self):
                raise NotImplementedError('pyinotify package is not installed')

from celery import Task
from celery.utils.log import get_task_logger
from six import iteritems

from .exceptions import InvalidHandlerError
from ..util import list_regular_files, safe_move_file

# Filter noisy and useless numpy warnings
# Reference: https://github.com/numpy/numpy/pull/432
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

__all__ = [
    'get_task_name',
    'CeleryConfig',
    'CeleryContext',
    'IncomingFileEventHandler',
    'IncomingFileStateManager',
    'WatchServiceContext',
    'WatchServiceManager'
]


def get_task_name(namespace, function_name):
    """Convenience function for :py:meth:`CeleryManager.get_task_name`

    :param namespace: task namespace
    :param function_name: name of function
    :return: string containing qualified task name
    """
    task_name = "{namespace}.{function_name}".format(namespace=namespace, function_name=function_name)
    return task_name


class CeleryConfig(object):
    # TODO: remove this hardcoding and get values from pipeline_config
    BROKER_URL = 'amqp://'
    BROKER_TRANSPORT = 'amqp'
    CELERY_ACCEPT_CONTENT = ['json']
    CELERY_TASK_SERIALIZER = 'json'
    CELERY_RESULT_SERIALIZER = 'json'

    CELERY_ROUTES = {}

    def __init__(self, routes=None):
        self.CELERY_ROUTES = routes or {}


def delete_same_name_from_error_store_callback(handler, file_state_manager):
    """Delete files from the error store if they match the regular expression "INPUT_FILE.UUID"

    :param handler: Handler instance
    :param file_state_manager: IncomingFileStateManager instance
    :return: None
    """
    escaped_basename = re.escape(handler.file_basename)
    cleanup_regexes = ensure_regex_list(r"^{escaped_basename}\.[0-9a-f\-]{{36}}$".format(
        escaped_basename=escaped_basename))
    deleted_files = file_state_manager.error_broker.delete_regexes(cleanup_regexes)
    log = "delete_same_name_from_error_store_callback deleted -> {}".format(deleted_files.get_attribute_list('name'))
    return log


def delete_custom_regexes_from_error_store_callback(handler, file_state_manager):
    """Delete files from the error store if they match one of regular expressions in the error_cleanup_regexes attribute
    of the handler

    :param handler: Handler instance
    :param file_state_manager: IncomingFileStateManager instance
    :return: None
    """
    cleanup_regexes = ensure_regex_list(handler.error_cleanup_regexes)
    deleted_files = file_state_manager.error_broker.delete_regexes(cleanup_regexes)
    log = "delete_custom_regexes_from_error_store_callback deleted -> {}".format(
        deleted_files.get_attribute_list('name'))
    return log


class ExitPolicy(Enum):
    """Policies defining callback actions performed on completion of a handler

    Callbacks are called with two parameters: the BaseHandler instance and the IncomingFileStateManager instance from
    the given task. These two objects encapsulate the entire state of a task execution, which allows a callback to
    perform essentially any action relating to the handling of a file, e.g. accessing the storage brokers, the handler
    file collection, the handler status etc.

    While they are given access to the entire task, it is not intended that exit callbacks interfere with the incoming
    file itself or any functionality which is the responsibility of the handler instance itself, e.g. harvesting,
    managing upload/archive storage.

    NO_ACTION: do nothing (default policy)
    DELETE_SAME_NAME_FROM_ERROR_STORE: remove all files with exactly the same name as the input file (accounting for a
        trailing UUID)
    DELETE_CUSTOM_REGEXES_FROM_ERROR_STORE: remove all files matching one of a list of regexes defined by the handler
        instance
    """
    NO_ACTION = {'callback': lambda handler, file_state_manager: None}
    DELETE_SAME_NAME_FROM_ERROR_STORE = {'callback': delete_same_name_from_error_store_callback}
    DELETE_CUSTOM_REGEXES_FROM_ERROR_STORE = {'callback': delete_custom_regexes_from_error_store_callback}

    @classmethod
    def from_name(cls, name):
        return getattr(ExitPolicy, name, cls.NO_ACTION)

    @classmethod
    def from_names(cls, names):
        return tuple(cls.from_name(n) for n in names)

    @property
    def callback(self):
        return self.value['callback']


def build_task(config, pipeline_name, handler_class, success_exit_policies, error_exit_policies, kwargs):
    """Closure function to return a Celery Task instance which has been prepared for a specific pipeline.
        The this allows the task to accept a single input_file parameter, while dynamically instantiating the
        handler class passed in via the 'handler_class' parameter, with the per-pipeline 'kwargs' pre-applied to the
        class through the use of partial.

    :param config: :py:class:`LazyConfigManager` instance
    :param pipeline_name: explicit task name for handling by celery Workers
    :param handler_class: :py:class:`HandlerBase` instance which the task will instantiate
    :param success_exit_policies: list of :py:class:`ExitPolicy` members
    :param error_exit_policies: list of :py:class:`ExitPolicy` members
    :param kwargs: dictionary containing the keyword arguments for use by the handler class
    :return: reference to a Celery task function which runs the given handler with the given keywords
    """
    task_name = get_task_name(config.pipeline_config['watch']['task_namespace'], pipeline_name)

    class PipelineTask(Task):
        ignore_result = True
        name = task_name

        def __init__(self):
            self.logger = None
            self.pipeline_name = pipeline_name

        def run(self, incoming_file):
            try:
                logging.config.dictConfig(config.worker_logging_config)
                logging_extra = {
                    'celery_task_id': self.request.id,
                    'celery_task_name': task_name
                }
                self.logger = get_pipeline_logger(task_name, extra=logging_extra, logger_function=get_task_logger)

                self.logger.sysinfo(
                    "{self.__class__.__name__}.success_exit_policies -> "
                    "{policies}".format(self=self, policies=[p.name for p in success_exit_policies]))
                self.logger.sysinfo(
                    "{self.__class__.__name__}.error_exit_policies -> "
                    "{policies}".format(self=self, policies=[p.name for p in error_exit_policies]))

                file_state_manager = IncomingFileStateManager(input_file=incoming_file,
                                                              pipeline_name=pipeline_name,
                                                              config=config,
                                                              logger=self.logger,
                                                              celery_request=self.request,
                                                              error_exit_policies=error_exit_policies,
                                                              success_exit_policies=success_exit_policies)

                file_state_manager.move_to_processing()

                try:
                    handler = handler_class(file_state_manager.processing_path, celery_task=self, config=config,
                                            upload_path=file_state_manager.relative_path, **kwargs)
                except Exception as e:
                    file_state_manager.move_to_error()
                    self.logger.error("failed to instantiate handler class: {e}".format(e=format_exception(e)))

                handler.run()

                file_state_manager.handler = handler

                if handler.error:
                    file_state_manager.move_to_error()
                else:
                    file_state_manager.move_to_success()
            except Exception:
                if self.logger:
                    self.logger.exception('unhandled exception in PipelineTask')
                raise

    return PipelineTask()


class CeleryContext(object):
    def __init__(self, application, config, celeryconfig):
        self._application = application
        self._config = config
        self._celeryconfig = celeryconfig

        self._application_configured = False

    @property
    def application(self):
        """Return the configured Celery application instance

        :return: Celery application instance with config applied and tasks registered
        """
        if not self._application_configured:
            self._configure_application()
        return self._application

    def _configure_application(self):
        self._application.config_from_object(self._celeryconfig)
        self._register_tasks()
        self._application_configured = True

    def _register_tasks(self):
        available_handler_names = set(self._config.discovered_handlers.keys())
        configured_handler_names = {h['handler'] for h in self._config.watch_config.values()}

        if not configured_handler_names.issubset(available_handler_names):
            invalid_handlers = configured_handler_names.difference(available_handler_names)
            warnings.warn("one or more handlers not found in discovered handlers. "
                          "{invalid} not in {discovered}".format(invalid=list(invalid_handlers),
                                                                 discovered=list(available_handler_names)))

        for pipeline_name, items in iteritems(self._config.watch_config):
            try:
                handler_class = self._config.discovered_handlers[items['handler']]
            except KeyError:
                raise InvalidHandlerError(
                    "handler not found in discovered handlers. "
                    "{items[handler]} not in {available_handler_names}".format(items=items,
                                                                               available_handler_names=available_handler_names))

            params = items.get('params', {})
            success_exit_policies = ExitPolicy.from_names(items.get('success_exit_policies', []))
            error_exit_policies = ExitPolicy.from_names(items.get('error_exit_policies', []))

            try:
                _ = handler_class('', config=self._config, **params)
            except TypeError as e:
                warnings.warn("invalid parameters for pipeline '{pipeline}', handler '{items[handler]}': {e}".format(
                    pipeline=pipeline_name, items=items, e=format_exception(e)))
            else:
                task_object = build_task(self._config, pipeline_name, handler_class, success_exit_policies,
                                         error_exit_policies, params)
                self._application.register_task(task_object)


def should_ignore_event(pathname):
    """Determine whether an inotify event should be ignored

    :param pathname: path to the file which triggered an event
    :return: True if the event should be ignored
    """

    # ignore non-regular files
    try:
        mode = os.stat(pathname).st_mode
    except OSError:
        return True
    if not stat.S_ISREG(mode):
        return True

    # ignore dotfiles
    basename = os.path.basename(pathname)
    if basename.startswith('.'):
        return True

    return False


class IncomingFileEventHandler(pyinotify.ProcessEvent):
    def __init__(self, config):
        super(IncomingFileEventHandler, self).__init__()
        self._config = config
        self._logger = get_pipeline_logger(config.pipeline_config['watch']['logger_name'])

    def process_default(self, event):
        # event_id is distinct from task_id, and exists in order to correlate log messages before *and* after a task
        # is queued for a given event
        event_id = uuid4()
        self._logger.info("inotify event: event_id='{event_id}' maskname='{event.maskname}'".format(event_id=event_id,
                                                                                                    event=event))
        self.queue_task(event.path, event.pathname, event_id)

    def queue_task(self, directory, pathname, event_id=None):
        """Add a task to the queue corresponding with the given directory, handling the given file

        :param directory: the watched directory
        :param pathname: the fully qualified path to the file which triggered the event
        :param event_id: UUID to identify this event in log files (will be generated if not present)
        :return: None
        """
        if should_ignore_event(pathname):
            self._logger.info("ignored event for '{pathname}'".format(pathname=pathname))
            return

        queue = self._config.watch_directory_map[directory]
        task_name = get_task_name(self._config.pipeline_config['watch']['task_namespace'], queue)

        task_data = {
            'event_id': event_id or uuid4(),
            'pathname': pathname,
            'queue': queue,
            'task_name': task_name
        }

        self._logger.info(
            "task data: event_id='{event_id}' queue='{queue}' task_name='{task_name}' pathname='{pathname}'".format(
                **task_data))

        result = self._config.celery_application.send_task(task_name, args=[pathname])
        task_data['task_id'] = result.id

        # pathname is deliberately duplicated here to enable cross-referencing from pipeline specific logs in order to
        # correlate a filename to the associated task_id
        self._logger.info(
            "task sent: task_id='{task_id}' task_name='{task_name}' event_id='{event_id}' pathname='{pathname}'".format(
                **task_data))
        self._logger.debug("full task_data: {task_data}".format(task_data=task_data))


class IncomingFileStateManager(object):
    processing_mode = stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH
    error_mode = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH

    states = [
        'FILE_IN_INCOMING',
        'FILE_IN_PROCESSING',
        'FILE_IN_ERROR',
        'FILE_SUCCESS'
    ]

    transitions = [
        {
            'trigger': 'move_to_processing',
            'source': 'FILE_IN_INCOMING',
            'dest': 'FILE_IN_PROCESSING',
            'before': ['_pre_processing_checks', '_move_to_processing']
        },
        {
            'trigger': 'move_to_error',
            'source': 'FILE_IN_PROCESSING',
            'dest': 'FILE_IN_ERROR',
            'before': ['_run_error_callbacks', '_move_to_error']
        },
        {
            'trigger': 'move_to_success',
            'source': 'FILE_IN_PROCESSING',
            'dest': 'FILE_SUCCESS',
            'before': '_run_success_callbacks',
            'after': '_remove_processing_file'
        }
    ]

    def __init__(self, input_file, pipeline_name, config, logger, celery_request, error_exit_policies=None,
                 success_exit_policies=None, error_broker=None):
        self.input_file = input_file
        self.pipeline_name = pipeline_name
        self.config = config
        self.logger = logger
        self.celery_request = celery_request
        self.error_exit_policies = error_exit_policies or []
        self.success_exit_policies = success_exit_policies or []
        self._error_broker = error_broker

        self._machine = Machine(model=self, states=self.states, initial='FILE_IN_INCOMING', auto_transitions=False,
                                transitions=self.transitions, after_state_change='_after_state_change')
        self._log_state()

        self.handler = None

    def _log_state(self):
        self.logger.sysinfo(
            "{self.__class__.__name__}.state -> '{self.state}'".format(self=self))

    @lazyproperty
    def error_broker(self):
        error_broker = get_storage_broker(self.error_uri)
        error_broker.mode = self.error_mode
        self.logger.info("{self.__class__.__name__}.error_broker -> {error_broker}".format(self=self,
                                                                                           error_broker=error_broker))
        return error_broker

    @property
    def basename(self):
        return os.path.basename(self.input_file)

    @property
    def incoming_dir(self):
        return os.path.dirname(self.input_file)

    @property
    def processing_dir(self):
        return os.path.join(self.config.pipeline_config['global']['processing_dir'], self.pipeline_name,
                            self.celery_request.id)

    @property
    def processing_path(self):
        return os.path.join(self.processing_dir, self.basename)

    @property
    def relative_path(self):
        return os.path.relpath(self.input_file, self.config.pipeline_config['watch']['incoming_dir'])

    @property
    def error_name(self):
        return "{name}.{id}".format(name=self.basename, id=self.celery_request.id)

    @property
    def error_uri(self):
        return os.path.join(self.config.pipeline_config['global']['error_uri'], self.pipeline_name)

    def _after_state_change(self):
        self._log_state()

    def _pre_processing_checks(self):
        try:
            validate_file_writable(self.input_file)

            mkdir_p(self.processing_dir)
            validate_dir_writable(self.processing_dir)

            # TODO: better validation of broker usability?
            _ = self.error_broker.query()
        except Exception:  # pragma: no cover
            self.logger.exception('exception occurred initialising IncomingFileStateManager')
            raise

    def _move_to_processing(self):
        self.logger.info("{self.__class__.__name__}.move_to_processing -> '{self.processing_path}'".format(self=self))
        safe_move_file(self.input_file, self.processing_path)
        os.chmod(self.processing_path, self.processing_mode)

    def _move_to_error(self):
        full_error_path = os.path.join(self.error_broker.prefix, self.error_name)
        self.logger.info("{self.__class__.__name__}.move_to_error -> '{path}'".format(self=self, path=full_error_path))
        error_file = PipelineFile(self.processing_path, dest_path=self.error_name)
        self.error_broker.upload(error_file)
        rm_f(self.processing_path)

    def _run_error_callbacks(self):
        try:
            callbacks = [p.callback for p in self.error_exit_policies]
            for callback in callbacks:
                callback_log = callback(self.handler, self)
                self.logger.info(callback_log)
        except Exception as e:
            self.logger.exception(
                "error running error callbacks: '{policies}'. {e}".format(policies=self.error_exit_policies, e=e))

    def _run_success_callbacks(self):
        try:
            callbacks = [p.callback for p in self.success_exit_policies]
            for callback in callbacks:
                callback_log = callback(self.handler, self)
                self.logger.info(callback_log)
        except Exception as e:
            self.logger.exception(
                "error running success callbacks: '{policies}'. {e}".format(policies=self.success_exit_policies, e=e))

    def _remove_processing_file(self):
        rm_f(self.processing_path)
        rm_r(self.processing_dir)


class WatchServiceContext(object):
    """Class to create instances required for WatchServiceManager

    """

    def __init__(self, config):
        self.event_handler = IncomingFileEventHandler(config)
        self.watch_manager = pyinotify.WatchManager()
        self.notifier = pyinotify.Notifier(self.watch_manager, self.event_handler)


class WatchServiceManager(object):
    EVENT_MASK = pyinotify.IN_MOVED_TO | pyinotify.IN_CLOSE_WRITE

    def __init__(self, config, event_handler, watch_manager, notifier):
        # noinspection PyProtectedMember
        if watch_manager is not notifier._watch_manager:
            raise ValueError("notifier must be instantiated with the same watch_manager instance as __init__ param")

        self._watch_manager = watch_manager
        self.notifier = notifier

        self._config = config
        self._event_handler = event_handler

        self._logger = get_pipeline_logger(config.pipeline_config['watch']['logger_name'])

    @property
    def watches(self):
        return [w.path for w in self._watch_manager.watches.values()]

    # noinspection PyUnusedLocal
    def handle_signal(self, signo=None, stackframe=None):
        self.stop("received signal '{signo}'".format(signo=signo))

    def stop(self, reason='unknown'):
        self._logger.info("stopping Notifier event loop. Reason: {reason}".format(reason=reason))
        try:
            self.notifier.stop()
        except AttributeError:
            # already stopped
            pass

    def __enter__(self):
        self._queue_and_watch_directories()
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        self.stop('context manager exiting')

    def _queue_and_watch_directories(self):
        """Configure the given WatchManager with the watches defined in the configuration, queuing any existing files

        :return: None
        """
        for directory, queue in iteritems(self._config.watch_directory_map):
            # Python 2 cannot handle the unicode string due to using str.* methods for sorting
            str_directory = str(directory) if PY2 else directory

            for existing_file in list_regular_files(str_directory):
                self._logger.info(
                    "queuing existing file: existing_file='{existing_file}'".format(
                        existing_file=existing_file))
                self._event_handler.queue_task(directory, existing_file)

            self._logger.info("adding watch for '{directory}'".format(directory=directory))
            self._watch_manager.add_watch(directory, self.EVENT_MASK)


validate_exitpolicy = validate_membership(ExitPolicy)

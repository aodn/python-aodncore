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

import logging.config
import os
import stat
import warnings
from uuid import uuid4

from six import PY2
from transitions import Machine

from .files import PipelineFile
from .log import get_pipeline_logger
from .storage import get_storage_broker
from ..util import format_exception, mkdir_p, rm_f, rm_r, validate_dir_writable, validate_file_writable

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

    def _build_task(self, pipeline_name, handler_class, kwargs):
        """Closure method to return a Celery Task instance which has been prepared for a specific pipeline.
            The this allows the task to accept a single input_file parameter, while dynamically instantiating the
            handler class passed in via the 'handler_class' parameter, with the per-pipeline 'kwargs' pre-applied to the
            class through the use of partial.

        :param pipeline_name: explicit task name for handling by celery Workers
        :param handler_class: :py:class:`HandlerBase` instance which the task will instantiate
        :param kwargs: dictionary containing the keyword arguments for use by the handler class
        :return: reference to a Celery task function which runs the given handler with the given keywords
        """
        task_name = get_task_name(self._config.pipeline_config['watch']['task_namespace'], pipeline_name)
        config = self._config

        class PipelineTask(Task):
            ignore_result = True
            name = task_name

            def __init__(self):
                self.file_state_manager = None
                self.handler = None
                self.input_file = None
                self.logger = None
                self.pipeline_name = pipeline_name

            def _configure_logger(self):
                logging.config.dictConfig(config.worker_logging_config)
                logging_extra = {
                    'celery_task_id': self.request.id,
                    'celery_task_name': task_name
                }
                self.logger = get_pipeline_logger(task_name, extra=logging_extra, logger_function=get_task_logger)

            def run(self, incoming_file):
                try:
                    self._configure_logger()

                    self.file_state_manager = IncomingFileStateManager(input_file=incoming_file,
                                                                       pipeline_name=pipeline_name,
                                                                       config=config,
                                                                       logger=self.logger,
                                                                       celery_request=self.request)

                    self.file_state_manager.move_to_processing()

                    try:
                        self.handler = handler_class(self.file_state_manager.processing_path, celery_task=self,
                                                     config=config, upload_path=self.file_state_manager.relative_path,
                                                     **kwargs)
                    except Exception as e:
                        self.file_state_manager.move_to_error()
                        self.logger.error("failed to instantiate handler class: {e}".format(e=format_exception(e)))

                    self.handler.run()

                    if self.handler.error:
                        self.file_state_manager.move_to_error()
                    else:
                        self.file_state_manager.move_to_success()
                except Exception:
                    self.logger.exception('unhandled exception in PipelineTask')
                    raise

        return PipelineTask()

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
                    "{name} not in {handlers}".format(name=items['handler'], handlers=available_handler_names))

            params = items.get('params', {})

            try:
                _ = handler_class('', config=self._config, **params)
            except TypeError as e:
                warnings.warn(
                    "invalid parameters for pipeline '{pipeline}', handler '{name}': {e}".format(pipeline=pipeline_name,
                                                                                                 name=items['handler'],
                                                                                                 e=format_exception(e)))
            else:
                task_object = self._build_task(pipeline_name, handler_class, params)
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

    states = ['FILE_IN_INCOMING', 'FILE_IN_PROCESSING', 'FILE_IN_ERROR', 'FILE_SUCCESS']
    transitions = [
        {
            'trigger': 'move_to_processing',
            'source': 'FILE_IN_INCOMING',
            'dest': 'FILE_IN_PROCESSING',
            'before': '_move_to_processing'
        },
        {
            'trigger': 'move_to_error',
            'source': 'FILE_IN_PROCESSING',
            'dest': 'FILE_IN_ERROR',
            'before': '_move_to_error'
        },
        {
            'trigger': 'move_to_success',
            'source': 'FILE_IN_PROCESSING',
            'dest': 'FILE_SUCCESS',
            'after': '_cleanup_success'
        }
    ]

    def __init__(self, input_file, pipeline_name, config, logger, celery_request, error_broker=None):
        self.input_file = input_file
        self.pipeline_name = pipeline_name
        self.config = config
        self.logger = logger
        self.celery_request = celery_request

        self._machine = Machine(model=self, states=self.states, initial='FILE_IN_INCOMING', auto_transitions=False,
                                transitions=self.transitions, after_state_change='_after_state_change')
        self.logger.sysinfo(
            "{name} initialised in state: {state}".format(name=self.__class__.__name__, state=self.state))

        self._error_broker = error_broker

        self._pre_check()

    @property
    def error_broker(self):
        if self._error_broker is None:
            self._error_broker = get_storage_broker(self.error_uri)
        return self._error_broker

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
        self.logger.sysinfo(
            "{name} transitioned to state: {state}".format(name=self.__class__.__name__, state=self.state))

    def _pre_check(self):
        try:
            validate_file_writable(self.input_file)

            mkdir_p(self.processing_dir)
            validate_dir_writable(self.processing_dir)

            # TODO: better validation of broker usability?
            _ = self.error_broker.query('')
        except Exception:  # pragma: no cover
            self.logger.exception('exception occurred initialising IncomingFileStateManager')
            raise

    def _move_to_processing(self):
        self.logger.info("{name}.move_to_processing -> '{path}'".format(name=self.__class__.__name__,
                                                                        path=self.processing_path))
        safe_move_file(self.input_file, self.processing_path)
        os.chmod(self.processing_path, self.processing_mode)

    def _move_to_error(self):
        full_error_path = os.path.join(self.error_broker.prefix, self.error_name)
        self.logger.info("{name}.move_to_error -> '{path}'".format(name=self.__class__.__name__,
                                                                   path=full_error_path))
        error_file = PipelineFile(self.processing_path, dest_path=self.error_name)
        self.error_broker.upload(error_file)
        rm_f(self.processing_path)

    def _cleanup_success(self):
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

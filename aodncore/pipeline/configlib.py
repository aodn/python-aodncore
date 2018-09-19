"""This module provides code to support loading and accessing a central configuration object. Other modules should
typically access the configuration by importing :py:const:`CONFIG` from :py:mod:`aodncore.pipeline.config` rather than
manually creating a new :py:class:`LazyConfigManager` instance.
"""

from __future__ import absolute_import
import json
import os

from celery import Celery
from six import iteritems

from .exceptions import InvalidConfigError
from .log import WorkerLoggingConfigBuilder, get_watchservice_logging_config
from .schema import validate_logging_config, validate_pipeline_config
from .watch import get_task_name, CeleryConfig, CeleryContext
from ..util import discover_entry_points, format_exception, lazyproperty, validate_type, WriteOnceOrderedDict

__all__ = [
    'LazyConfigManager',
    'load_json_file',
    'load_pipeline_config',
    'load_trigger_config',
    'load_watch_config',
    'validate_lazyconfigmanager'
]

DEFAULT_CONFIG_FILE = '/mnt/ebs/aodn-pipeline/etc/pipeline.conf'
DEFAULT_CONFIG_ENVVAR = 'PIPELINE_CONFIG_FILE'
DEFAULT_WATCH_CONFIG = '/mnt/ebs/aodn-pipeline/etc/watches.conf'
DEFAULT_WATCH_CONFIG_ENVVAR = 'PIPELINE_WATCH_CONFIG_FILE'
DEFAULT_TRIGGER_CONFIG = '/usr/local/talend/etc/trigger.conf'
DEFAULT_TRIGGER_CONFIG_ENVVAR = 'PIPELINE_TRIGGER_CONFIG_FILE'


class LazyConfigManager(object):
    """Configuration object to consolidate configuration values.

    Different configuration sources are represented as lazy properties so that it is still efficient to pass an instance
    of this class to several modules, even if they only require one particular type of configuration to operate.
    """

    @lazyproperty
    def celery_application(self):
        application = Celery(self.pipeline_config['watch']['task_namespace'])
        celeryconfig = CeleryConfig(self.celery_routes)
        celerycontext = CeleryContext(application, self, celeryconfig)
        return celerycontext.application

    @lazyproperty
    def celery_routes(self):
        routes = {}
        for name in self.watch_config.keys():
            task_name = get_task_name(self.pipeline_config['watch']['task_namespace'], name)
            queue_dict = {'queue': name, 'routing_key': name}
            routes[task_name] = queue_dict
        return routes

    @lazyproperty
    def discovered_dest_path_functions(self):
        discovered_dest_path_functions = discover_entry_points(
            self.pipeline_config['pluggable']['path_function_group'])
        return discovered_dest_path_functions

    @lazyproperty
    def discovered_handlers(self):
        discovered_handlers = discover_entry_points(self.pipeline_config['pluggable']['handlers_group'])
        return discovered_handlers

    @lazyproperty
    def discovered_module_versions(self):
        discovered_module_versions = discover_entry_points(self.pipeline_config['pluggable']['module_versions_group'])
        return discovered_module_versions

    @lazyproperty
    def watchservice_logging_config(self):
        watchservice_logging_config = get_watchservice_logging_config(self.pipeline_config)
        validate_logging_config(watchservice_logging_config)
        return watchservice_logging_config

    @lazyproperty
    def worker_logging_config(self):
        config_builder = WorkerLoggingConfigBuilder(self.pipeline_config)

        for name in self.watch_config.keys():
            task_name = get_task_name(self.pipeline_config['watch']['task_namespace'], name)
            config_builder.add_watch_config(task_name)

        worker_logging_config = config_builder.get_config()

        validate_logging_config(worker_logging_config)
        return worker_logging_config

    @lazyproperty
    def pipeline_config(self):
        pipeline_config = load_pipeline_config(DEFAULT_CONFIG_FILE, envvar=DEFAULT_CONFIG_ENVVAR)
        validate_pipeline_config(pipeline_config)
        return pipeline_config

    @lazyproperty
    def watch_config(self):
        watch_config = load_watch_config(DEFAULT_WATCH_CONFIG)
        return watch_config

    @lazyproperty
    def trigger_config(self):
        trigger_config = load_trigger_config(DEFAULT_TRIGGER_CONFIG)
        return trigger_config

    @lazyproperty
    def watch_directory_map(self):
        directories = {}
        # noinspection PyTypeChecker
        for name, items in iteritems(self.watch_config):
            for rel_path in items['path']:
                path = os.path.join(self.pipeline_config['watch']['incoming_dir'], rel_path)
                directories[path] = name
        return directories


def load_pipeline_config(default_config_file=DEFAULT_WATCH_CONFIG, envvar=DEFAULT_CONFIG_ENVVAR):
    config_file = os.environ.get(envvar, default_config_file)
    config = load_json_file(config_file, envvar=envvar)
    return config


def load_watch_config(default_config_file=DEFAULT_WATCH_CONFIG):
    """

    :param default_config_file: default path to return if not found set in environment variable
    :return: dict representation of watch config parsed from json file
    """
    config = load_json_file(default_config_file, envvar=DEFAULT_WATCH_CONFIG_ENVVAR)
    return config


def load_trigger_config(default_config_file=DEFAULT_TRIGGER_CONFIG):
    """

    :param default_config_file: default path to return if not found set in environment variable
    :return: dict representation of watch config parsed from json file
    """
    config = load_json_file(default_config_file, envvar=DEFAULT_TRIGGER_CONFIG_ENVVAR)
    return config


def load_json_file(default_config_file, envvar=None, object_pairs_hook=WriteOnceOrderedDict):
    """Load a JSON file into a :py:class:`dict`

    :param default_config_file:
    :param envvar: environment variable containing path to load
    :param object_pairs_hook: class used for json.load 'object_pairs_hook' parameter
    :return: object containing loaded JSON config
    """
    config_file = os.environ.get(envvar, default_config_file)
    try:
        with open(config_file) as f:
            config = json.load(f, object_pairs_hook=object_pairs_hook)
    except (IOError, OSError) as e:
        raise InvalidConfigError(format_exception(e))

    return config


validate_lazyconfigmanager = validate_type(LazyConfigManager)

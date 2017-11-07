import json
import os

import jsonschema
from celery import Celery
from six import iteritems
from six.moves import configparser

from .exceptions import InvalidConfigError
from .schema import LOGGING_CONFIG_SCHEMA, PIPELINE_CONFIG_SCHEMA
from .watch import get_task_name, CeleryConfig, CeleryContext
from ..util import discover_entry_points, format_exception, merge_dicts, str_to_list, validate_type

__all__ = [
    'CustomParser',
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


# noinspection PyClassicStyleClass
class CustomParser(configparser.SafeConfigParser):
    """Sub-class of SafeConfigParser to implement an "as_dict" method
    """

    def as_dict(self):
        """Return dict representation of the SafeConfigParser object

        :return: dict
        """
        parser_as_dict = {s: dict(self.items(s)) for s in self.sections()}
        return parser_as_dict

    def getlist(self, section, option, **kwargs):
        """Return a comma-separated string as native list

        :param section: ConfigParser section
        :param option: ConfigParser option
        :param kwargs: additional kwargs passed to str_to_list
        :return: list representation of the given config option
        """
        value = self.get(section, option)
        value_as_list = str_to_list(value, **kwargs)
        return value_as_list


class LazyConfigManager(object):
    def __init__(self):
        self._celery_application = None
        self._celery_routes = None
        self._discovered_dest_path_functions = None
        self._discovered_handlers = None
        self._logging_config = None
        self._pipeline_config = None
        self._trigger_config = None
        self._watch_config = None
        self._watch_directory_map = None

    @property
    def celery_application(self):
        if self._celery_application is None:
            application = Celery(self.pipeline_config['watch']['task_namespace'])
            celeryconfig = CeleryConfig(self.celery_routes)
            celerycontext = CeleryContext(application, self, celeryconfig)
            self._celery_application = celerycontext.application

        return self._celery_application

    @property
    def celery_routes(self):
        if self._celery_routes is None:
            routes = {}
            for name in self.watch_config.keys():
                task_name = get_task_name(self.pipeline_config['watch']['task_namespace'], name)
                queue_dict = {'queue': name, 'routing_key': name}
                routes[task_name] = queue_dict
                self._celery_routes = routes

        return self._celery_routes

    @property
    def discovered_dest_path_functions(self):
        if self._discovered_dest_path_functions is None:
            discovered_dest_path_functions = discover_entry_points(
                self.pipeline_config['pluggable']['path_function_group'])
            self._discovered_dest_path_functions = discovered_dest_path_functions
        return self._discovered_dest_path_functions

    @property
    def discovered_handlers(self):
        if self._discovered_handlers is None:
            discovered_handlers = discover_entry_points(self.pipeline_config['pluggable']['handlers_group'])
            self._discovered_handlers = discovered_handlers
        return self._discovered_handlers

    @property
    def logging_config(self):
        if self._logging_config is None:
            logging_config = get_base_logging_config(self.pipeline_config)
            for name in self.watch_config.keys():
                task_name = get_task_name(self.pipeline_config['watch']['task_namespace'], name)
                watch_logging_config = get_logging_config_for_watch(task_name,
                                                                    self.pipeline_config['logging']['log_root'],
                                                                    self.pipeline_config['logging']['level'])
                logging_config = merge_dicts(logging_config, watch_logging_config)
            validate_logging_config(logging_config)
            self._logging_config = logging_config

        return self._logging_config

    @property
    def pipeline_config(self):
        if self._pipeline_config is None:
            pipeline_config = load_pipeline_config(DEFAULT_CONFIG_FILE, envvar=DEFAULT_CONFIG_ENVVAR)
            validate_pipeline_config(pipeline_config)
            self._pipeline_config = pipeline_config

        return self._pipeline_config

    @property
    def watch_config(self):
        if self._watch_config is None:
            watch_config = load_watch_config(DEFAULT_WATCH_CONFIG)
            self._watch_config = watch_config

        return self._watch_config

    @property
    def trigger_config(self):
        if self._trigger_config is None:
            trigger_config = load_trigger_config(DEFAULT_TRIGGER_CONFIG)
            self._trigger_config = trigger_config

        return self._trigger_config

    @property
    def watch_directory_map(self):
        if self._watch_directory_map is None:
            directories = {}
            # noinspection PyTypeChecker
            for name, items in iteritems(self.watch_config):
                for rel_path in items['path']:
                    path = os.path.join(self.pipeline_config['watch']['incoming_dir'], rel_path)
                    directories[path] = name
            self._watch_directory_map = directories

        return self._watch_directory_map

    def purge_lazy_properties(self):
        self._celery_application = None
        self._celery_routes = None
        self._logging_config = None
        self._pipeline_config = None
        self._watch_config = None
        self._watch_directory_map = None


def get_base_logging_config(pipeline_config):
    base_logging_config = {
        'version': 1,
        'formatters': {
            'pipeline_formatter': {
                'format': pipeline_config['logging']['pipeline_format']
            },
            'watchservice_formatter': {
                'format': pipeline_config['logging']['watchservice_format']
            }
        },
        'filters': {},
        'handlers': {
            'watchservice_handler': {
                'level': pipeline_config['logging']['level'],
                'class': 'logging.StreamHandler',
                'formatter': 'watchservice_formatter'
            }
        },
        'loggers': {
            'watchservice': {
                'handlers': ['watchservice_handler'],
                'level': pipeline_config['logging']['level'],
                'propagate': False
            }
        }
    }

    # decrease log level for noisy library loggers, unless explicitly increased for debugging
    for lib in ('botocore', 'paramiko', 's3transfer', 'transitions'):
        base_logging_config['loggers'][lib] = {'level': pipeline_config['logging'].get('liblevel', 'WARN')}

    return base_logging_config


def get_logging_config_for_watch(task_name, log_root, level='INFO'):
    handler_name = "{name}_handler".format(name=task_name)
    watch_logging_config = {
        'handlers': {
            handler_name: {
                'level': level,
                'class': 'logging.FileHandler',
                'formatter': 'pipeline_formatter',
                'filename': os.path.join(log_root, 'process', "{task_name}.log".format(task_name=task_name))
            }
        },
        'loggers': {
            task_name: {
                'handlers': [handler_name],
                'level': level,
                'propagate': False
            }
        }
    }
    return watch_logging_config


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


def load_json_file(default_config_file, envvar=None):
    """Load a JSON file into a dict, using either a

    :param default_config_file:
    :param envvar:
    :return:
    """
    config_file = os.environ.get(envvar, default_config_file)
    try:
        with open(config_file) as f:
            config = json.load(f)
    except (IOError, OSError) as e:
        raise InvalidConfigError(format_exception(e))

    return config


def validate_logging_config(logging_config):
    jsonschema.validate(logging_config, LOGGING_CONFIG_SCHEMA)


def validate_pipeline_config(pipeline_config):
    jsonschema.validate(pipeline_config, PIPELINE_CONFIG_SCHEMA)


validate_lazyconfigmanager = validate_type(LazyConfigManager)

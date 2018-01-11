import logging
import os

import jsonschema

from .schema import LOGGING_CONFIG_SCHEMA

# custom SYSINFO logging level
SYSINFO = 15
logging.addLevelName(SYSINFO, 'SYSINFO')

__all__ = [
    'SYSINFO',
    'get_base_worker_logging_config',
    'get_logging_config_for_watch',
    'get_watchservice_logging_config',
    'get_pipeline_logger',
    'validate_logging_config'
]


def get_base_worker_logging_config(pipeline_config):
    """Get the *base* logging config for pipeline worker processes, suitable for use by logging.config.dictConfig

    :param pipeline_config: LazyConfigManager.pipeline_config dict
    :return: dict containing base worker logging config
    """
    base_logging_config = {
        'version': 1,
        'formatters': {
            'pipeline_formatter': {
                'format': pipeline_config['logging']['pipeline_format']
            }
        },
        'filters': {},
        'handlers': {},
        'loggers': {}
    }

    # decrease log level for noisy library loggers, unless explicitly increased for debugging
    for lib in ('botocore', 'paramiko', 's3transfer', 'transitions'):
        base_logging_config['loggers'][lib] = {'level': pipeline_config['logging'].get('liblevel', 'WARN')}

    return base_logging_config


def get_logging_config_for_watch(task_name, log_root, level=SYSINFO):
    """Get logging configuration for a single pipeline watch, intended to be merged onto the output of
        `get_base_worker_logging_config`

    :param task_name: name of the pipeline for which the config is being generated
    :param log_root: logging root directory
    :param level: logging level to
    :return: dict containing handlers/loggers for a single watch
    """
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


def get_watchservice_logging_config(pipeline_config):
    """Generate logging configuration for the 'watchservice' service, suitable for use by logging.config.dictConfig

    :param pipeline_config: LazyConfigManager.pipeline_config dict
    :return: rendered watchservice logging config
    """
    watchservice_logging_config = {
        'version': 1,
        'formatters': {
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
    return watchservice_logging_config


def get_pipeline_logger(name, extra=None, logger_function=logging.getLogger):
    """Return a logger adapter prepared with given extra metadata and SYSINFO logging level

    :param name: logger name
    :param extra: extra dict to pass to LoggerAdapter
    :param logger_function: function which accepts logger name and returns a Logger instance
    :return: Logger instance
    """
    if extra is None:
        extra = {}
    logger = logger_function(name)
    logger_adapter = logging.LoggerAdapter(logger, extra)
    setattr(logger_adapter, 'sysinfo', lambda *args: logger_adapter.log(SYSINFO, *args))
    return logger_adapter


def validate_logging_config(logging_config):
    jsonschema.validate(logging_config, LOGGING_CONFIG_SCHEMA)

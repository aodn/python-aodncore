import logging
import os

from ..util import validate_nonstring_iterable

# custom SYSINFO logging level
SYSINFO = 15
logging.addLevelName(SYSINFO, 'SYSINFO')

__all__ = [
    'SYSINFO',
    'WorkerLoggingConfigBuilder',
    'get_watchservice_logging_config',
    'get_pipeline_logger'
]


class WorkerLoggingConfigBuilder(object):
    def __init__(self, pipeline_config):
        self.pipeline_config = pipeline_config

        liblevel = self.pipeline_config['logging'].get('liblevel', 'WARN')

        self._dict_config = {
            'version': 1,
            'formatters': {
                'pipeline_formatter': {
                    'format': self.pipeline_config['logging']['pipeline_format']
                }
            },
            'filters': {},
            'handlers': {},
            'loggers': {
                'botocore': {
                    'level': liblevel
                },
                'paramiko': {
                    'level': liblevel
                },
                's3transfer': {
                    'level': liblevel
                },
                'transitions': {
                    'level': liblevel
                }
            }
        }

    def add_watch_config(self, name, formatter='pipeline_formatter', level=None):
        """Add logging configuration for a single pipeline watch

        :param name: name of the pipeline for which the config is being generated
        :param formatter: name of the formatter to use for handler
        :param level: logging level
        :return: dict containing handlers/loggers for a single watch
        """
        if level is None:
            level = self.pipeline_config['logging']['level']

        handler_name = "{name}_handler".format(name=name)

        self._dict_config['handlers'][handler_name] = {
            'level': level,
            'class': 'logging.FileHandler',
            'formatter': formatter,
            'filename': os.path.join(self.pipeline_config['logging']['log_root'], 'process',
                                     "{task_name}.log".format(task_name=name))
        }
        self._dict_config['loggers'][name] = {
            'handlers': [handler_name],
            'level': level,
            'propagate': False
        }

    def add_watches(self, watches):
        validate_nonstring_iterable(watches)

        for watch in watches:
            self.add_watch_config(watch)

    def get_config(self):
        return self._dict_config


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



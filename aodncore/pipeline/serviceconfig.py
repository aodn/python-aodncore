"""This module acts as an entry-point for Celery worker processes and the inotify service, in order for them to access
the configured Celery application and other runtime configuration (as required)

"""

from .config import CONFIG

__all__ = [
    'APPLICATION',
    'CONFIG'
]

# Celery application must be immediately evaluated and aliased as a module level global in order to support Celery
# workers loading the configuration from the command-line
APPLICATION = CONFIG.celery_application

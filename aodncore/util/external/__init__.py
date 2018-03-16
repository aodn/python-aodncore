"""This sub-module contains freely available *external* code which has been copied into this project to avoid
    unnecessary dependencies.

Code taken from:
    * awsretry :: https://github.com/linuxdynasty/awsretry (MIT License)
    * boltons :: https://boltons.readthedocs.io/en/latest/index.html (BSD License)

"""
from .awsretry import AWSRetry
from .boltons import IndexedSet, classproperty

__all__ = [
    'AWSRetry',
    'IndexedSet',
    'classproperty'
]

"""This sub-module contains freely available *external* code which has been copied into this project to avoid
    unnecessary dependencies.

Code taken from:
    * retry :: https://github.com/invl/retry (Apache 2.0 License)
    * boltons :: https://boltons.readthedocs.io/en/latest/index.html (BSD License)

"""
from .retry import retry as retry_decorator
from .boltons import IndexedSet, classproperty

__all__ = [
    'IndexedSet',
    'classproperty',
    'retry_decorator'
]

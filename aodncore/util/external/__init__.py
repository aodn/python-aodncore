"""This sub-module contains freely available *external* code which has been copied into this project to avoid
    unnecessary dependencies.

Code taken from:

    * boltons :: https://boltons.readthedocs.io/en/latest/index.html (BSD License)

"""
from .boltons import IndexedSet, classproperty

__all__ = [
    'IndexedSet',
    'classproperty'
]

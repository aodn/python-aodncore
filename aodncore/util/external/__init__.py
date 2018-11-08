"""This sub-module contains freely available *external* code which has been copied into this project to avoid
    unnecessary dependencies.

Code taken from:
    * astropy :: http://docs.astropy.org/en/stable/utils/index.html (BSD Licence)
    * boltons :: https://boltons.readthedocs.io/en/latest/index.html (BSD License)
    * retry :: https://github.com/invl/retry (Apache 2.0 License)

"""
from .astropy.decorators import classproperty, lazyproperty
from .boltons.setutils import IndexedSet
from .retry import retry as retry_decorator

__all__ = [
    'IndexedSet',
    'classproperty',
    'lazyproperty',
    'retry_decorator'
]

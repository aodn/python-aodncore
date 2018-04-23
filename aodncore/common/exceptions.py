"""This module contains base exceptions for the :py:mod:`aodncore` package.
"""

__all__ = [
    'AodnBaseError',
    'SystemCommandFailedError'
]


class AodnBaseError(Exception):
    """Base class for *all* exceptions
    """
    pass


class SystemCommandFailedError(AodnBaseError):
    pass

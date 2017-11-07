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

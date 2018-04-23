"""This module is separate from the :py:mod:`aodncore.pipeline.configlib` definitions in order for it to act as the
    shared configuration "singleton" for the package.
"""

from .configlib import LazyConfigManager

__all__ = [
    'CONFIG'
]

CONFIG = LazyConfigManager()

"""This module provides a common base class for step runner classes in for all steps (which in turn have a common base
class for each step, inheriting from this one)
"""

__all__ = [
    'BaseStepRunner'
]


class BaseStepRunner(object):
    """Common parent class of all "step runner" child classes
    """

    def __init__(self, config, logger):
        self._config = config
        self._logger = logger

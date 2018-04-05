"""This module provides a common base class for step runner classes in for all steps (which in turn have a common base
class for each step, inheriting from this one)
"""

import abc

__all__ = [
    'BaseStepRunner',
    'AbstractCollectionStepRunner',
    'AbstractNotifyRunner',
    'AbstractResolveRunner'
]


class BaseStepRunner(object):
    """Common parent class of all "step runner" child classes
    """

    def __init__(self, config, logger):
        self._config = config
        self._logger = logger


# TODO: these Abstract classes are an unnecessary complications, and should be removed/consolidated with the step runner
# parent classes
class AbstractCollectionStepRunner(BaseStepRunner):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod  # pragma: no cover
    def run(self, pipeline_files):
        pass


class AbstractNotifyRunner(BaseStepRunner):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod  # pragma: no cover
    def run(self, notify_list):
        pass


class AbstractResolveRunner(BaseStepRunner):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod  # pragma: no cover
    def run(self):
        pass

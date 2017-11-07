import abc
import inspect

from aodncore.pipeline.steps.basestep import AbstractCollectionStepRunner, AbstractNotifyRunner, AbstractResolveRunner
from test_aodncore.testlib import BaseTestCase


class TestAbstractCollectionStepRunner(BaseTestCase):
    def test_abc(self):
        self.assertIsInstance(AbstractCollectionStepRunner, abc.ABCMeta)

    def test_run_method(self):
        self.assertIn('run', AbstractCollectionStepRunner.__abstractmethods__)
        run_arguments = inspect.getargspec(AbstractCollectionStepRunner.run)
        self.assertListEqual(run_arguments.args, ['self', 'pipeline_files'])


class TestAbstractNotifyRunner(BaseTestCase):
    def test_abc(self):
        self.assertIsInstance(AbstractNotifyRunner, abc.ABCMeta)

    def test_run_method(self):
        self.assertIn('run', AbstractNotifyRunner.__abstractmethods__)
        run_arguments = inspect.getargspec(AbstractNotifyRunner.run)
        self.assertListEqual(run_arguments.args, ['self', 'notify_list'])


class TestAbstractResolveRunner(BaseTestCase):
    def test_abc(self):
        self.assertIsInstance(AbstractResolveRunner, abc.ABCMeta)

    def test_run_method(self):
        self.assertIn('run', AbstractResolveRunner.__abstractmethods__)
        run_arguments = inspect.getargspec(AbstractResolveRunner.run)
        self.assertListEqual(run_arguments.args, ['self'])

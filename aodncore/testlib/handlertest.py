import os
from functools import partial

from .basetest import BaseTestCase
from ..pipeline import HandlerBase

__all__ = [
    'HandlerTestCase'
]

TEST_ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


class HandlerTestCase(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        cls._metadata_regenerated = set()

    def setUp(self):
        super(HandlerTestCase, self).setUp()
        handler_class = getattr(self, 'handler_class', HandlerBase)

        self.handler_class = partial(handler_class, config=self.config)

    def test_base(self):
        handler = self.handler_class(self.temp_nc_file)
        self.assertIsInstance(handler, HandlerBase)

    def run_handler(self, *args, **kwargs):
        """Instantiate and run the handler class defined in self.handler_class.
            Raise an AssertionError if the handler completes with any error

        :param args: args passed directly to handler instance
        :param kwargs: kwargs passed directly to handler instance
        :return: the handler instance
        """
        handler = self.handler_class(*args, **kwargs)
        handler.run()
        self.assertIsNone(handler.error)
        return handler

    def run_handler_with_exception(self, expected_error, *args, **kwargs):
        """Instantiate and run the handler class defined in self.handler_class
            Raise an AssertionError if the handler completes *without* the error attribute being the expected Exception
            class

        :param expected_error: expected Exception class
        :param args: args passed directly to handler instance
        :param kwargs: kwargs passed directly to handler instance
        :return: the handler instance
        """
        handler = self.handler_class(*args, **kwargs)
        handler.run()
        self.assertIsInstance(handler.error, expected_error)
        return handler

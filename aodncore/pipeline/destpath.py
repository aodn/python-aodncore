"""This module provides code to determine which function/method will be used in order for the handler to generate the
destination path (dest_path) and/or archive path (archive_path) for each file being processed. Since the path function
may be in the handler class itself, or provided as a runtime parameter, this code is to ensure that the discovery
mechanism is consistent.
"""

import six

from .exceptions import InvalidPathFunctionError
from ..util import validate_callable

__all__ = [
    'get_path_function'
]


def get_path_function(handler_instance, entry_point_group, archive_mode=False):
    """Return a tuple containing a reference to a path function, and the function's fully qualified, printable name

    :param handler_instance: :py:class:`HandlerBase` instance
    :param entry_point_group: name of the entry point group used discover path functions
    :param archive_mode: :py:class:`bool` flag to modify which attributes are used
    :return: result of :py:meth:`PathFunctionResolver.resolve` method
    """
    resolver = PathFunctionResolver(handler_instance, entry_point_group, archive_mode)
    return resolver.resolve()


class PathFunctionResolver(object):
    def __init__(self, handler_instance, entry_point_group, archive_mode=False):
        self.handler_instance = handler_instance
        self.entry_point_group = entry_point_group
        self.archive_mode = archive_mode

    @property
    def attribute_name(self):
        return 'archive_path' if self.archive_mode else 'dest_path'

    @property
    def function_param(self):
        return getattr(self.handler_instance, self.param_name)

    @property
    def param_name(self):
        return 'archive_path_function' if self.archive_mode else 'dest_path_function'

    def resolve(self):
        try:
            return self._resolve()
        except InvalidPathFunctionError:
            if self.archive_mode:
                self.archive_mode = False
                return self._resolve()
            raise

    def _resolve(self):
        if self.function_param is None:
            function_ref, function_parent = self._resolve_from_handler()
        else:
            function_ref, function_parent = self._resolve_from_param()

        function_qualified_name = "{function_parent}.{function}".format(function_parent=function_parent,
                                                                        function=function_ref.__name__)
        return function_ref, function_qualified_name

    def _resolve_from_handler(self):
        # attempt to find the dest path function (method) in the handler class itself
        try:
            function_ref = getattr(self.handler_instance, self.attribute_name)
        except AttributeError:
            raise InvalidPathFunctionError(
                "missing path function. Must be set in '{param_name}' parameter or a method named "
                "'{attribute_name}' in the handler class".format(param_name=self.param_name,
                                                                 attribute_name=self.attribute_name))

        try:
            validate_callable(function_ref)
        except TypeError:
            raise InvalidPathFunctionError("incorrect type for '{attr_name}' attribute in the handler class. "
                                           "Expected a method, found '{type}'".format(attr_name=self.attribute_name,
                                                                                      type=type(function_ref)))

        function_parent = self.handler_instance.__class__.__name__
        return function_ref, function_parent

    def _resolve_from_param(self):
        if isinstance(self.function_param, six.string_types):
            # a string parameter is assumed to be referring to an advertised entry point of the same name
            try:
                function_ref = self.handler_instance.config.discovered_dest_path_functions[self.function_param]
            except KeyError:
                message = "{attribute_name} function '{function}' not found in '{functions}'".format(
                    attribute_name=self.attribute_name,
                    function=self.function_param,
                    functions=self.handler_instance.config.discovered_dest_path_functions)
                raise InvalidPathFunctionError(message)
        elif callable(self.function_param):
            # dest_path_function parameter is already a Callable object, use it
            function_ref = self.function_param
        else:
            raise InvalidPathFunctionError(
                "invalid {param_name} parameter. Must be a function reference or "
                "the name of an entry point in group '{group}'".format(param_name=self.param_name,
                                                                       group=self.entry_point_group))
        function_parent = function_ref.__module__
        return function_ref, function_parent

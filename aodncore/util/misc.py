import logging
import re
import sys
import types
from collections import Iterable, OrderedDict, Mapping

import jinja2
import pkg_resources
import six
from six.moves import range

StringIO = six.StringIO

__all__ = [
    'discover_entry_points',
    'format_exception',
    'is_nonstring_iterable',
    'is_function',
    'is_valid_email_address',
    'matches_regexes',
    'merge_dicts',
    'slice_sequence',
    'str_to_list',
    'validate_bool',
    'validate_callable',
    'validate_dict',
    'validate_int',
    'validate_mapping',
    'validate_mandatory_elements',
    'validate_membership',
    'validate_nonstring_iterable',
    'validate_regex',
    'validate_string',
    'validate_type',
    'CaptureStdIO',
    'LoggingContext',
    'TemplateRenderer'
]


def discover_entry_points(entry_point_group, working_set=pkg_resources.working_set):
    """Discover entry points registered under the given entry point group name in the given working_set

    :param entry_point_group: entry point group name used to find entry points in working set
    :param working_set: working set object
    :return: dict containing each discovered entry point, with keys being the entry point name and values being a
    reference to the object referenced by the entry point
    """
    entry_points = {}
    for entry_point in working_set.iter_entry_points(entry_point_group):
        entry_point_object = entry_point.load()
        entry_points[entry_point.name] = entry_point_object
    return entry_points


def format_exception(exception):
    """Return a pretty string representation of an Exception object containing the Exception name and message

    :param exception: Exception object
    :return: string
    """
    return "{cls}: {message}".format(cls=exception.__class__.__name__, message=exception)


def is_function(o):
    """Check whether a given object is a function

    :param o: object to check
    :return: True if object is a function, otherwise False
    """
    return isinstance(o, types.FunctionType)


def is_nonstring_iterable(sequence):
    """Check whether an object is a non-string Iterable
    
    :param sequence: object to check 
    :return: True if object is a non-string sub class of Iterable
    """
    return isinstance(sequence, Iterable) and not isinstance(sequence, (six.string_types, bytes, Mapping))


def is_valid_email_address(address):
    """Simple email address validation

    :param address: address to validate
    :return: True if address matches the regex, otherwise False
    """
    pattern = re.compile(r"^[A-Z0-9_.+-]+@(localhost|[A-Z0-9-]+\.[A-Z0-9-.]+)$", re.IGNORECASE)
    return re.match(pattern, address)


def matches_regexes(input_string, include_regexes=None, exclude_regexes=None):
    """Function to filter a string (e.g. file path) according to regular expression inclusions minus exclusions

    :param input_string: string for comparison to the regular expressions
    :param include_regexes: list of inclusions
    :param exclude_regexes: list of exclusions to *subtract* from the list produced by inclusions
    :return: True if the of the string matches one of the 'include_regexes' but *not* one of the 'exclude_regexes'
    """
    if isinstance(include_regexes, six.string_types):
        include_regexes = [include_regexes]
    if not include_regexes:
        include_regexes = ['.*']
    if exclude_regexes and isinstance(exclude_regexes, six.string_types):
        exclude_regexes = [exclude_regexes]

    matches_includes = False
    for r in include_regexes:
        validate_regex(r)
        if re.match(r, input_string):
            matches_includes = True
            break

    matches_excludes = False
    if matches_includes and exclude_regexes is not None:
        for r in exclude_regexes:
            validate_regex(r)
            if re.match(r, input_string):
                matches_excludes = True
                break

    if matches_includes and not matches_excludes:
        return True
    return False


def merge_dicts(*args):
    """Recursive dict merge.

    Dict-like objects are merged sequentially from left to right into a new dict

    Based on: https://gist.github.com/angstwad/bf22d1822c38a92ec0a9

    :return: None
    """
    master = {}

    for current_dict in args:
        for k, v in six.iteritems(current_dict):
            if k in master and isinstance(master[k], (dict, OrderedDict)) and isinstance(current_dict[k], Mapping):
                master[k] = merge_dicts(master[k], current_dict[k])
            elif k in master and isinstance(master[k], list) and isinstance(current_dict[k], Iterable):
                if current_dict[k] not in master[k]:
                    master[k].extend(current_dict[k])
            else:
                master[k] = current_dict[k]

    return master


def slice_sequence(sequence, slice_size):
    """Return a list containing the input Sequence sliced into Sequences with a length equal to or less than slice_size

    :param sequence: input sequence
    :param slice_size: size of each sub-Sequence
    :return: list of Sequences
    """
    return [sequence[x:x + slice_size] for x in range(0, len(sequence), slice_size)]


def str_to_list(string_, delimiter=',', strip_method='strip', include_empty=False):
    """Return a comma-separated string as native list, with whitespace stripped and empty strings excluded

    :param string_: input string
    :param delimiter: character(s) used to split the string
    :param strip_method: which strip method to use for each element (invalid method names
    :param include_empty: boolean to control whether empty strings are included in returned list
    :return: list representation of the given config option
    """
    if isinstance(string_, list):
        return string_

    valid_methods = {'lstrip', 'rstrip', 'strip'}
    method = strip_method if strip_method in valid_methods else '__str__'

    def _process(str_):
        for raw in str_.split(delimiter):
            stripped = getattr(raw, method)()
            if include_empty or stripped:
                yield stripped

    return [e for e in _process(string_)]


def validate_membership(c):
    def validate_membership_func(o):
        if o not in c:
            raise ValueError("value '{o}' must be a member of '{c}'".format(o=o, c=c))

    return validate_membership_func


def validate_type(t):
    """Closure to generate type validation functions

    :param t: type
    :return: function reference to a function which validates a given input is an instance of type `t`
    """

    def validate_type_func(o):
        if not isinstance(o, t):
            raise TypeError("object '{o}' must be of type '{t}'".format(o=o, t=t))

    return validate_type_func


validate_bool = validate_type(bool)
validate_dict = validate_type(dict)
validate_int = validate_type(int)
validate_mapping = validate_type(Mapping)
validate_string = validate_type(six.string_types)


def validate_callable(o):
    if not callable(o):
        raise TypeError('value must be a Callable object')


def validate_nonstring_iterable(o):
    if not is_nonstring_iterable(o):
        raise TypeError('value must be a non-string Iterable')


def validate_regex(o):
    try:
        re.compile(o)
    except re.error as e:
        raise ValueError("invalid regex '{o}'. {e}".format(o=o, e=format_exception(e)))


def validate_mandatory_elements(mandatory, actual, name='item'):
    """Ensure that a collection contains all the elements in a 'mandatory' collection of elements

    :param mandatory: collection of mandatory elements
    :param actual: collection to compare against mandatory collection
    :param name: name of object being validated for exception message (e.g. 'item' or 'section') 
    :return: None
    """
    mandatory = mandatory if isinstance(mandatory, set) else set(mandatory)
    actual = actual if isinstance(actual, set) else set(actual)

    if not mandatory.issubset(actual):
        missing = list(mandatory.difference(actual))
        raise ValueError("missing mandatory {name}(s): {missing}".format(name=name, missing=missing))


class CaptureStdIO(object):
    """Context manager to capture stdout and stderr emitted from the block into a list. 
        Optionally merge stdout and stderr streams into stdout.
    """

    def __init__(self, merge_streams=False):
        self._merge_streams = merge_streams
        self.__stdout_lines = []
        self.__stderr_lines = []

    def __enter__(self):
        self.old_stdout, self.old_stderr = sys.stdout, sys.stderr

        sys.stdout = self._stdout_stringio = StringIO()
        self._stderr_stringio = self._stdout_stringio if self._merge_streams else StringIO()
        sys.stderr = self._stderr_stringio

        return self.__stdout_lines, self.__stderr_lines

    def __exit__(self, *args):
        self.__stdout_lines.extend(self._stdout_stringio.getvalue().splitlines())
        if not self._merge_streams:
            self.__stderr_lines.extend(self._stderr_stringio.getvalue().splitlines())
        del self._stdout_stringio, self._stderr_stringio
        sys.stdout, sys.stderr = self.old_stdout, self.old_stderr


class LoggingContext(object):
    """Context manager to allow temporary changes to logging configuration within the context of the block
    """

    def __init__(self, logger, level=None, format_=None, handler=None, close=True):
        self.logger = logger.logger if isinstance(logger, logging.LoggerAdapter) else logger
        self.level = level
        self.format = format_
        self.handler = handler
        self.close = close

    def __enter__(self):
        if self.level is not None:
            self.old_level = self.logger.level
            self.logger.setLevel(self.level)
        if self.format is not None:
            self.old_formatters = []
            temp_formatter = logging.Formatter(self.format)
            for h in self.logger.handlers:
                self.old_formatters.append(h.formatter)
                h.setFormatter(temp_formatter)
        if self.handler:
            self.logger.addHandler(self.handler)

    def __exit__(self, et, ev, tb):
        if self.level is not None:
            self.logger.setLevel(self.old_level)
        if self.format is not None:
            for i, h in enumerate(self.logger.handlers):
                h.setFormatter(self.old_formatters[i])
        if self.handler:
            self.logger.removeHandler(self.handler)
        if self.handler and self.close:
            self.handler.close()


class TemplateRenderer(object):
    """Simple template renderer
    """

    def __init__(self, package='aodncore.pipeline', package_path='templates'):
        super(TemplateRenderer, self).__init__()
        self._package = package
        self._loader = jinja2.PackageLoader(package, package_path)
        self._env = jinja2.Environment(loader=self._loader)

    def render(self, name, values):
        validate_mapping(values)
        template = self._env.get_template(name)
        rendered = template.render(values)
        return rendered

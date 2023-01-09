"""This module provides miscellaneous utility functions *not* related to filesystem or subprocess operations.

These are typically functions which query, manipulate or transform Python objects.
"""

import logging
import os
import re
import sys
import types

try:
    # Import of collections stop working after 3.10, use collections.abc
    from collections.abc import Iterable, OrderedDict, Mapping
except ImportError:
    # Python 2, Python < 3.2
    from collections import Iterable, OrderedDict, Mapping

from enum import Enum, EnumMeta
from io import StringIO
import uuid
import random
import string

import jinja2
import pkg_resources

Pattern = type(re.compile(''))

__all__ = [
    'discover_entry_points',
    'ensure_regex',
    'ensure_regex_list',
    'ensure_writeonceordereddict',
    'format_exception',
    'generate_id',
    'get_pattern_subgroups_from_string',
    'get_regex_subgroups_from_string',
    'is_nonstring_iterable',
    'is_function',
    'is_valid_email_address',
    'iter_public_attributes',
    'matches_regexes',
    'merge_dicts',
    'slice_sequence',
    'str_to_list',
    'list_not_empty',
    'validate_bool',
    'validate_callable',
    'validate_dict',
    'validate_int',
    'validate_mapping',
    'validate_mandatory_elements',
    'validate_membership',
    'validate_nonstring_iterable',
    'validate_regex',
    'validate_regexes',
    'validate_relative_path',
    'validate_relative_path_attr',
    'validate_string',
    'validate_type',
    'CaptureStdIO',
    'LoggingContext',
    'Pattern',
    'TemplateRenderer',
    'WriteOnceOrderedDict'
]


def discover_entry_points(entry_point_group, working_set=pkg_resources.working_set):
    """Discover entry points registered under the given entry point group name in the given
    :py:class:`pkg_resources.WorkingSet` instance

    :param entry_point_group: entry point group name used to find entry points in working set
    :param working_set: :py:class:`pkg_resources.WorkingSet` instance
    :return: :py:class:`tuple` containing two elements, with the first being a :py:class:`dict` containing each
        discovered and *successfully loaded* entry point (with keys being the entry point name and values being a
        reference to the object referenced by the entry point), and the second tuple element being a :py:class:`list` of
        objects which failed to be loaded
    """
    entry_points = {}
    failed = []
    for entry_point in working_set.iter_entry_points(entry_point_group):
        try:
            entry_point_object = entry_point.load()
            entry_points[entry_point.name] = entry_point_object
        except ImportError:
            failed.append(entry_point.name)
    return entry_points, failed


def ensure_regex(o):
    """Ensure that the returned value is a compiled regular expression (Pattern) from a given input, or raise if the
    object is not a valid regular expression

    :param o: input object, a single regex (string or pre-compiled)
    :return: :py:class:`Pattern` instance
    """
    validate_regex(o)
    if isinstance(o, Pattern):
        return o
    return re.compile(o)


def ensure_regex_list(o):
    """Ensure that the returned value is a list of compiled regular expressions (Pattern) from a given input, or raise
    if the object is not a list of valid regular expression

    :param o: input object, either a single regex or a sequence of regexes (string or pre-compiled)
    :return: :py:class:`list` of :py:class:`Pattern` instances
    """
    if o is None:
        return []

    # if parameter is a single valid pattern, return it wrapped in a list
    try:
        return [ensure_regex(o)]
    except TypeError:
        pass

    validate_nonstring_iterable(o)
    return [ensure_regex(p) for p in o]


def ensure_writeonceordereddict(o, empty_on_fail=True):
    """Function to accept and object and return the WriteOnceOrderedDict representation of the object. An object which
    can *not* be handled by the WriteOnceOrderedDict __init__ method will either result in an empty, or if
    'empty_on_fail' is set to False, will result in an exception.

    :param o: input object
    :param empty_on_fail: boolean flag to determine whether an invalid object will result in a new empty
        WriteOnceOrderedDict being returned or the exception re-raised.
    :return: :py:class:`WriteOnceOrderedDict` instance
    """
    if isinstance(o, WriteOnceOrderedDict):
        return o

    try:
        return WriteOnceOrderedDict(o)
    except (TypeError, ValueError):
        if empty_on_fail:
            return WriteOnceOrderedDict()
        raise


def format_exception(exception):
    """Return a pretty string representation of an Exception object containing the Exception name and message

    :param exception: :py:class:`Exception` object
    :return: string
    """
    return "{cls}: {message}".format(cls=exception.__class__.__name__, message=exception)


def generate_id():
    """Generate a unique id starting with non-numeric character

    :return: unique id
    """
    return random.choice(string.ascii_lowercase) + str(uuid.uuid4().hex)


def get_regex_subgroups_from_string(string, regex):
    """Function to retrieve parts of a string given a compiled pattern (re.compile(pattern))
    the pattern needs to match the beginning of the string
    (see https://docs.python.org/2/library/re.html#re.RegexObject.match)

    * No need to start the pattern with "^"; and
    * To match anywhere in the string, start the pattern with ".*".

    :return: dictionary of fields matching a given pattern
    """
    compiled_regex = ensure_regex(regex)
    m = compiled_regex.match(string)
    return {} if m is None else m.groupdict()


get_pattern_subgroups_from_string = get_regex_subgroups_from_string


def is_function(o):
    """Check whether a given object is a function

    :param o: object to check
    :return: True if object is a function, otherwise False
    """
    return isinstance(o, (types.FunctionType, types.MethodType))


def is_nonstring_iterable(sequence):
    """Check whether an object is a non-string :py:class:`Iterable`
    
    :param sequence: object to check 
    :return: True if object is a non-string sub class of :py:class:`Iterable`
    """
    return isinstance(sequence, Iterable) and not isinstance(sequence, (str, bytes, Mapping))


def is_valid_email_address(address):
    """Simple email address validation

    :param address: address to validate
    :return: True if address matches the regex, otherwise False
    """
    regex = re.compile(r"^[A-Z0-9_.+-]+@(localhost|[A-Z0-9-]+\.[A-Z0-9-.]+)$", re.IGNORECASE)
    return regex.match(address)


def iter_public_attributes(instance, ignored_attributes=None):
    """Get an iterator over an instance's public attributes, *including* properties

    :param instance: object instance
    :param ignored_attributes: set of attribute names to exclude
    :return: iterator over the instances public attributes
    """
    ignored_attributes = {} if ignored_attributes is None else set(ignored_attributes)

    def includeattr(attr):
        if attr.startswith('_') or attr in ignored_attributes:
            return False
        return True

    attribute_names = set(getattr(instance, '__slots__', getattr(instance, '__dict__', {})))
    property_names = {p for p in dir(instance.__class__) if isinstance(getattr(instance.__class__, p), property)}
    all_names = attribute_names.union(property_names)

    public_attrs = {a: getattr(instance, a) for a in all_names if includeattr(a)}

    return iter(public_attrs.items())


def matches_regexes(input_string, include_regexes, exclude_regexes=None):
    """Function to filter a string (e.g. file path) according to regular expression inclusions minus exclusions

    :param input_string: string for comparison to the regular expressions
    :param include_regexes: list of inclusions
    :param exclude_regexes: list of exclusions to *subtract* from the list produced by inclusions
    :return: True if the of the string matches one of the 'include_regexes' but *not* one of the 'exclude_regexes'
    """
    includes = ensure_regex_list(include_regexes)
    excludes = ensure_regex_list(exclude_regexes)

    matches_includes = any(re.match(r, input_string) for r in includes)
    matches_excludes = any(re.match(r, input_string) for r in excludes)

    if matches_includes and not matches_excludes:
        return True
    return False


def merge_dicts(*args):
    """Recursive :py:class:`dict` merge

    Dict-like objects are merged sequentially from left to right into a new :py:class:`dict`

    Based on: https://gist.github.com/angstwad/bf22d1822c38a92ec0a9

    :return: None
    """
    if args and isinstance(args[0], OrderedDict):
        master = OrderedDict()
    else:
        master = {}

    for current_dict in args:
        for k, v in current_dict.items():
            if k in master and isinstance(master[k], (dict, OrderedDict)) and isinstance(current_dict[k], Mapping):
                master[k] = merge_dicts(master[k], current_dict[k])
            elif k in master and isinstance(master[k], list) and isinstance(current_dict[k], Iterable):
                if current_dict[k] not in master[k]:
                    master[k].extend(current_dict[k])
            else:
                master[k] = current_dict[k]

    return master


def slice_sequence(sequence, slice_size):
    """Return a :py:class:`list` containing the input :py:class:`Sequence` sliced into :py:class:`Sequence` instances
    with a length equal to or less than :py:attr:`slice_size`

    .. note:: The type of the elements should be the same type as the original sequence based on the usual Python
        slicing behaviour, but the outer sequence will always be a :py:class:`list` type.

    :param sequence: input sequence
    :param slice_size: size of each sub-Sequence
    :return: :py:class:`list` of :py:class:`Sequence` instances
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


def list_not_empty(_list):
    """Flag a list containing not None values
    :return: boolean - True if list contains any non-None values, otherwise False
    """
    if len(_list) == 0:
        return False
    return any(item is not None for item in _list)


def validate_membership(c):
    def validate_membership_func(o):
        # Compatibility fix for Python <3.8.
        # Python 3.8 raises a TypeError when testing for non-Enum objects, so this causes this function to also raise
        # a TypeError in earlier Python 3 versions. This can be removed when Python 3.8 becomes the minimum required
        # version.
        if isinstance(c, (EnumMeta, Enum)) and not isinstance(o, (EnumMeta, Enum)):
            raise TypeError(
                "unsupported operand type(s) for 'in': '%s' and '%s'" % (
                    type(o).__qualname__, c.__class__.__qualname__))

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
validate_string = validate_type(str)


def validate_callable(o):
    if not callable(o):
        raise TypeError('value must be a Callable object')


def validate_nonstring_iterable(o):
    if not is_nonstring_iterable(o):
        raise TypeError('value must be a non-string Iterable')


def validate_regex(o):
    if isinstance(o, Pattern):
        return
    try:
        re.compile(o)
    except re.error as e:
        raise ValueError("invalid regex '{o}'. {e}".format(o=o, e=format_exception(e)))
    except TypeError as e:
        raise TypeError("invalid regex '{o}'. {e}".format(o=o, e=format_exception(e)))


def validate_regexes(o):
    validate_nonstring_iterable(o)
    for regex in o:
        validate_regex(regex)


def validate_relative_path(o):
    if os.path.isabs(o):
        raise ValueError("path '{o}' must be a relative path".format(o=o))


def validate_relative_path_attr(path, path_attr):
    """Validate a path, raising an exception containing the name of the attribute which failed

    :param path: string containing the path to test
    :param path_attr: attribute name to include in the exceptions message if validation fails
    :return: None
    """
    try:
        validate_relative_path(path)
    except ValueError as e:
        raise ValueError("error validating '{attr}': {e}".format(attr=path_attr, e=e))


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


class WriteOnceOrderedDict(OrderedDict):
    """Sub-class of OrderedDict which prevents overwriting/deleting of keys once set
    """

    def __readonly__(self, *args, **kwargs):
        raise RuntimeError('updates or deletions not permitted on WriteOnceOrderedDict')

    def __setitem__(self, key, value):
        if key in self:
            raise RuntimeError("key '{}' has already been set".format(key))
        super().__setitem__(key, value)

    __delitem__ = __readonly__
    clear = __readonly__
    pop = __readonly__
    popitem = __readonly__
    setdefault = __readonly__
    update = __readonly__


class TemplateRenderer(object):
    """Simple template renderer
    """

    def __init__(self, package='aodncore.pipeline', package_path='templates'):
        super().__init__()
        self._package = package
        self._loader = jinja2.PackageLoader(package, package_path)
        self._env = jinja2.Environment(loader=self._loader)

    def render(self, name, values):
        """Render a template with the given values and return as a :py:class:`str`

        :param name: name of the template to find in the :py:class:`jinja2.Environment`
        :param values: :py:class:`dict` containing values to render into the template
        :return: :py:class:`str` containing the rendered template
        """
        validate_mapping(values)
        template = self._env.get_template(name)
        rendered = template.render(values)
        return rendered

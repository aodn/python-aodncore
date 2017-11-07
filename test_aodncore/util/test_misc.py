import os
import sys
import uuid
from unittest import skipIf

import six

from aodncore.util import (discover_entry_points, format_exception, is_function, is_nonstring_iterable, matches_regexes,
                           merge_dicts, slice_sequence, str_to_list, validate_callable, validate_mandatory_elements,
                           validate_membership, validate_nonstring_iterable, validate_type, CaptureStdIO)
from test_aodncore.testlib import BaseTestCase, DummyHandler, get_test_working_set

StringIO = six.StringIO

TEST_ROOT = os.path.join(os.path.dirname(__file__))


def get_nonexistent_path():
    return os.path.join('/nonexistent/path/with/a/{uuid}/in/the/middle'.format(uuid=uuid.uuid4()))


class TestCaptureStdIO(BaseTestCase):
    def test_capture_stdio(self):
        test_output = ['a', 'series', 'of', 'test', 'values', 'for', 'comparison']
        test_output_newlines = ["{line}{sep}".format(line=l, sep=os.linesep) for l in test_output]

        with CaptureStdIO() as (output, error):
            sys.stderr.writelines(test_output_newlines)
        self.assertListEqual(test_output, error)
        self.assertListEqual([], output)

        with CaptureStdIO() as (output, error):
            sys.stdout.writelines(test_output_newlines)
        self.assertListEqual(test_output, output)
        self.assertListEqual([], error)

        with CaptureStdIO(merge_streams=True) as (output, error):
            sys.stdout.writelines("a{sep}".format(sep=os.linesep))
            sys.stderr.writelines("b{sep}".format(sep=os.linesep))
            sys.stdout.writelines("c{sep}".format(sep=os.linesep))
            sys.stderr.writelines("d{sep}".format(sep=os.linesep))
        self.assertListEqual(['a', 'b', 'c', 'd'], output)
        self.assertListEqual([], error)


class TestLoggingContext(BaseTestCase):
    # TODO: tests
    pass


class TestUtilMisc(BaseTestCase):
    @skipIf(sys.platform == 'darwin', 'pyinotify not available on darwin platform')
    def test_discover_entry_points(self):
        search_path = os.path.dirname(os.path.dirname(TEST_ROOT))
        ws = get_test_working_set(search_path)

        discovered_entry_points = discover_entry_points('unittest.handlers', ws)
        self.assertDictEqual(discovered_entry_points, {'DummyHandler': DummyHandler})

    def test_is_function(self):
        class DummyClass(object):
            def dummy_method(self):
                pass

            @staticmethod
            def dummy_staticmethod():
                pass

        def dummy_func():
            pass

        self.assertTrue(is_function(dummy_func))
        self.assertTrue(is_function(lambda p: p))
        self.assertTrue(is_function(DummyClass.dummy_staticmethod))
        self.assertFalse(is_function(DummyClass))
        self.assertFalse(is_function(DummyClass.dummy_method))
        self.assertFalse(is_function(1))
        self.assertFalse(is_function({1: 1}))
        self.assertFalse(is_function({1}))
        self.assertFalse(is_function([1]))

    def test_validate_membership(self):
        validate_in_collection = validate_membership([1, 2, 3])
        try:
            validate_in_collection(1)
        except Exception as e:
            raise AssertionError("unexpected exception raised. {e}".format(e=format_exception(e)))
        with self.assertRaises(ValueError):
            validate_in_collection(4)

    def test_validate_type(self):
        f = validate_type(int)
        self.assertTrue(is_function(f))

        try:
            f(1)
        except Exception as e:
            raise AssertionError("unexpected exception raised. {e}".format(e=format_exception(e)))

        with self.assertRaises(TypeError):
            f('s')

    def test_validate_callable(self):
        def dummy_function():
            return

        class DummyClass(object):
            pass

        try:
            validate_callable(dummy_function)
            validate_callable(DummyClass)
        except Exception as e:
            raise AssertionError("unexpected exception raised. {e}".format(e=format_exception(e)))

        with self.assertRaises(TypeError):
            validate_callable(1)

        with self.assertRaises(TypeError):
            validate_callable('s')

        with self.assertRaises(TypeError):
            validate_callable({1: 1})

        with self.assertRaises(TypeError):
            validate_callable([1])

    def test_validate_nonstring_iterable(self):
        try:
            validate_nonstring_iterable([1])
            validate_nonstring_iterable({1})
            validate_nonstring_iterable((1,))
        except Exception as e:
            raise AssertionError("unexpected exception raised. {e}".format(e=format_exception(e)))

        with self.assertRaises(TypeError):
            validate_nonstring_iterable({1: 1})

        with self.assertRaises(TypeError):
            validate_nonstring_iterable('s')

    def test_format_exception(self):
        try:
            raise EnvironmentError('dummy exception for format test')
        except EnvironmentError as e:
            formatted = format_exception(e)

        self.assertEqual(formatted, 'EnvironmentError: dummy exception for format test')

    def test_is_nonstring_iterable(self):
        self.assertTrue(is_nonstring_iterable(('foo', 'bar')))
        self.assertTrue(is_nonstring_iterable(['foo', 'bar']))
        self.assertTrue(is_nonstring_iterable({'foo', 'bar'}))
        self.assertFalse(is_nonstring_iterable({'foo': 'bar'}))
        self.assertFalse(is_nonstring_iterable('foobar'))
        self.assertFalse(is_nonstring_iterable(u'foobar'))
        self.assertFalse(is_nonstring_iterable(r'foobar'))
        self.assertFalse(is_nonstring_iterable(b'foobar'))
        self.assertFalse(is_nonstring_iterable(1))

    def test_matches_regexes(self):
        # Testing inclusion from list of regexes
        self.assertTrue(matches_regexes('example-filename.nc', ['.*\.nc']))

        # Testing inclusion from string regex
        self.assertTrue(matches_regexes('example-filename.nc', 'example-.*\.nc'))

        # Testing exclusion from list of regexes
        self.assertFalse(matches_regexes('another-example.zip', ['.*\.zip'], ['another-example.zip']))

        # Testing exclusion from string regex
        self.assertFalse(matches_regexes('another-example.zip', '.*\.zip', 'another-example.zip'))

        # Testing invalid arguments
        with self.assertRaises(TypeError):
            matches_regexes('example-filename.nc', 1)
        with self.assertRaises(TypeError):
            matches_regexes('example-filename.nc', [], 1)

    def test_merge_dicts(self):
        reference_dict = {
            'key1': 'value1_override',
            'key2': 'value2',
            'listkey1': ['listvalue1', 'listvalue2', 'listvalue3', 'listvalue4'],
            'listkey2': ['listvalue5', 'listvalue6'],
            'key3': 'value3',
            'key4': {
                'subkey1': 'subvalue1',
                'subkey2': 'subvalue2_override'
            },
            'key5': 'value5',
            'key6': {
                'subkey3': {
                    'subsubkey1': 'subsubvalue1_override',
                    'subsubkey2': 'subsubvalue2'
                }
            }
        }

        dict1 = {
            'key1': 'value1',
            'key2': 'value2',
            'listkey1': ['listvalue1', 'listvalue2']
        }
        dict2 = {
            'key1': 'value1_override',
            'listkey1': ['listvalue3', 'listvalue4'],
            'listkey2': ['listvalue5', 'listvalue6']
        }
        dict3 = {
            'key3': 'value3'
        }
        dict4 = {
            'key4': {
                'subkey1': 'subvalue1',
                'subkey2': 'subvalue2'
            },
            'key5': 'value5'
        }
        dict5 = {
            'key4': {
                'subkey2': 'subvalue2_override'
            }
        }
        dict6 = {
            'key6': {
                'subkey3': {
                    'subsubkey1': 'subsubvalue1',
                    'subsubkey2': 'subsubvalue2'
                }
            }
        }
        dict7 = {
            'key6': {
                'subkey3': {
                    'subsubkey1': 'subsubvalue1_override'
                }
            }
        }

        merged_dict = merge_dicts(dict1, dict2, dict3, dict4, dict5, dict6, dict7)
        self.assertDictEqual(merged_dict, reference_dict)

    def test_slice_sequence(self):
        test_sequence = ('a', 'b', 'c')

        slices_of_one = slice_sequence(test_sequence, 1)
        self.assertIsInstance(slices_of_one, list)
        self.assertListEqual(slices_of_one, [('a',), ('b',), ('c',)])

        slices_of_two = slice_sequence(test_sequence, 2)
        self.assertIsInstance(slices_of_one, list)
        self.assertListEqual(slices_of_two, [('a', 'b'), ('c',)])

        slices_of_three = slice_sequence(test_sequence, 3)
        self.assertIsInstance(slices_of_one, list)
        self.assertListEqual(slices_of_three, [('a', 'b', 'c')])

        slices_of_four = slice_sequence(test_sequence, 4)
        self.assertIsInstance(slices_of_one, list)
        self.assertListEqual(slices_of_four, [('a', 'b', 'c')])

    def test_str_to_list(self):
        input_string1 = 'str1,str2,str3, str4,str5 ,,str6, ,'
        input_string2 = ' str1 str2 str3  str4'

        list1_default = str_to_list(input_string1)
        self.assertListEqual(list1_default, ['str1', 'str2', 'str3', 'str4', 'str5', 'str6'])

        list1_includeempty = str_to_list(input_string1, include_empty=True)
        self.assertListEqual(list1_includeempty, ['str1', 'str2', 'str3', 'str4', 'str5', '', 'str6', '', ''])

        list1_lstrip = str_to_list(input_string1, strip_method='lstrip')
        self.assertListEqual(list1_lstrip, ['str1', 'str2', 'str3', 'str4', 'str5 ', 'str6'])

        list1_rstrip = str_to_list(input_string1, strip_method='rstrip')
        self.assertListEqual(list1_rstrip, ['str1', 'str2', 'str3', ' str4', 'str5', 'str6'])

        list1_nostrip = str_to_list(input_string1, strip_method=None)
        self.assertListEqual(list1_nostrip, ['str1', 'str2', 'str3', ' str4', 'str5 ', 'str6', ' '])

        list2_spaces_default = str_to_list(input_string2, delimiter=' ')
        self.assertListEqual(list2_spaces_default, ['str1', 'str2', 'str3', 'str4'])

        list2_spaces_include_empty = str_to_list(input_string2, delimiter=' ', include_empty=True)
        self.assertListEqual(list2_spaces_include_empty, ['', 'str1', 'str2', 'str3', '', 'str4'])

    def test_validate_mandatory_elements(self):
        superset = {'a', 'b', 'c'}
        subset = {'a', 'b'}

        with self.assertRaises(ValueError):
            validate_mandatory_elements(superset, subset)

        try:
            validate_mandatory_elements(subset, superset)
        except Exception as e:
            raise AssertionError("unexpected exception: {e}".format(e=format_exception(e)))

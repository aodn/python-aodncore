import os
import re
import sys
import uuid
from collections import OrderedDict

from aodncore.testlib import BaseTestCase
from aodncore.util import (ensure_regex, ensure_regex_list, ensure_writeonceordereddict, format_exception,
                           get_pattern_subgroups_from_string, is_function, is_nonstring_iterable, matches_regexes,
                           merge_dicts, slice_sequence, str_to_list, validate_callable, validate_mandatory_elements,
                           validate_membership, validate_nonstring_iterable, validate_regex, validate_regexes,
                           validate_relative_path, validate_relative_path_attr, validate_type, CaptureStdIO, Pattern,
                           WriteOnceOrderedDict)

TEST_ROOT = os.path.join(os.path.dirname(__file__))

VALID_PATTERN = r'.*'
INVALID_PATTERN = r'^(?P<incomplete[A-Z]{3,4})'
COMPILED_PATTERN = re.compile(VALID_PATTERN)


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


class TestWriteOnceOrderedDict(BaseTestCase):
    def setUp(self):
        self.write_once_ordered_dict = WriteOnceOrderedDict({'key': 'value'})

    def test_new_key(self):
        with self.assertNoException():
            self.write_once_ordered_dict['key2'] = 'value'

    def test_no_overwrite(self):
        with self.assertRaises(RuntimeError):
            self.write_once_ordered_dict['key'] = 'value2'

        with self.assertRaises(RuntimeError):
            self.write_once_ordered_dict.update({'key': 'value2'})

    def test_no_delete(self):
        with self.assertRaises(RuntimeError):
            self.write_once_ordered_dict.pop('key')

        with self.assertRaises(RuntimeError):
            self.write_once_ordered_dict.popitem(('key', 'value'))

        with self.assertRaises(RuntimeError):
            del self.write_once_ordered_dict['key']

        with self.assertRaises(RuntimeError):
            self.write_once_ordered_dict.clear()


class TestUtilMisc(BaseTestCase):
    def test_ensure_regex(self):
        ensured_pattern = ensure_regex(VALID_PATTERN)
        self.assertIsInstance(ensured_pattern, Pattern)

        ensured_compiled_pattern = ensure_regex(COMPILED_PATTERN)
        self.assertIs(ensured_compiled_pattern, COMPILED_PATTERN)

        with self.assertRaises(ValueError):
            _ = ensure_regex(INVALID_PATTERN)

        with self.assertRaises(TypeError):
            _ = ensure_regex(1)

    def test_ensure_regex_list(self):
        ensured_pattern_list = ensure_regex_list([VALID_PATTERN])
        self.assertIsInstance(ensured_pattern_list, list)
        self.assertTrue(all(isinstance(p, Pattern) for p in ensured_pattern_list))

        with self.assertNoException():
            _ = ensure_regex_list([VALID_PATTERN, COMPILED_PATTERN])

        with self.assertRaises(ValueError):
            _ = ensure_regex_list([INVALID_PATTERN])

        with self.assertRaises(TypeError):
            _ = ensure_regex_list(1)

        list_from_none = ensure_regex_list(None)
        self.assertListEqual(list_from_none, [])

        list_from_empty_list = ensure_regex_list([])
        self.assertListEqual(list_from_empty_list, [])

        list_from_pattern = ensure_regex_list(VALID_PATTERN)
        self.assertIsInstance(list_from_pattern, list)
        self.assertTrue(all(isinstance(p, Pattern) for p in list_from_pattern))

        list_from_compiled = ensure_regex_list(COMPILED_PATTERN)
        self.assertIsInstance(list_from_compiled, list)
        self.assertTrue(all(isinstance(p, Pattern) for p in list_from_compiled))

    def test_ensure_writeonceordereddict(self):
        test_wood = WriteOnceOrderedDict({'key': 'value'})
        ensured_wood = ensure_writeonceordereddict(test_wood)
        self.assertIsInstance(ensured_wood, WriteOnceOrderedDict)
        self.assertIs(ensured_wood, test_wood)

        test_dict = {'key': 'value'}
        ensured_dict = ensure_writeonceordereddict(test_dict)
        self.assertIsInstance(ensured_dict, WriteOnceOrderedDict)
        self.assertDictEqual(ensured_dict, test_dict)

        test_invalid = 'str'
        ensured_invalid = ensure_writeonceordereddict(test_invalid)
        self.assertIsInstance(ensured_invalid, WriteOnceOrderedDict)
        self.assertEqual(len(ensured_invalid), 0)

        test_none = None
        ensured_none = ensure_writeonceordereddict(test_none)
        self.assertIsInstance(ensured_none, WriteOnceOrderedDict)
        self.assertEqual(len(ensured_none), 0)

        test_none_fail = None
        with self.assertRaises(TypeError):
            _ = ensure_writeonceordereddict(test_none_fail, empty_on_fail=False)

        test_string_fail = 'str'
        with self.assertRaises(ValueError):
            _ = ensure_writeonceordereddict(test_string_fail, empty_on_fail=False)

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
        self.assertTrue(is_function(DummyClass.dummy_method))
        self.assertFalse(is_function(DummyClass))
        self.assertFalse(is_function(1))
        self.assertFalse(is_function({1: 1}))
        self.assertFalse(is_function({1}))
        self.assertFalse(is_function([1]))

    def test_validate_membership(self):
        validate_in_collection = validate_membership([1, 2, 3])
        with self.assertNoException():
            validate_in_collection(1)
        with self.assertRaises(ValueError):
            validate_in_collection(4)

    def test_validate_type(self):
        f = validate_type(int)
        self.assertTrue(is_function(f))

        with self.assertNoException():
            f(1)

        with self.assertRaises(TypeError):
            f('s')

    def test_validate_callable(self):
        def dummy_function():
            return

        class DummyClass(object):
            pass

        with self.assertNoException():
            validate_callable(dummy_function)
            validate_callable(DummyClass)

        with self.assertRaises(TypeError):
            validate_callable(1)

        with self.assertRaises(TypeError):
            validate_callable('s')

        with self.assertRaises(TypeError):
            validate_callable({1: 1})

        with self.assertRaises(TypeError):
            validate_callable([1])

    def test_validate_nonstring_iterable(self):
        with self.assertNoException():
            validate_nonstring_iterable([1])
            validate_nonstring_iterable({1})
            validate_nonstring_iterable((1,))

        with self.assertRaises(TypeError):
            validate_nonstring_iterable({1: 1})

        with self.assertRaises(TypeError):
            validate_nonstring_iterable('s')

    def test_validate_regex(self):
        with self.assertNoException():
            validate_regex(VALID_PATTERN)

        with self.assertNoException():
            validate_regex(COMPILED_PATTERN)

        with self.assertRaises(ValueError):
            validate_regex(INVALID_PATTERN)

        with self.assertRaises(TypeError):
            validate_regex(1)

        with self.assertRaises(TypeError):
            validate_regex([VALID_PATTERN])  # valid pattern, but in a list

    def test_validate_regexes(self):
        with self.assertNoException():
            validate_regexes([VALID_PATTERN, COMPILED_PATTERN])

        with self.assertRaises(ValueError):
            validate_regexes([VALID_PATTERN, INVALID_PATTERN])

        with self.assertRaises(TypeError):
            validate_regexes([1, {}])

        with self.assertRaises(TypeError):
            validate_regexes(VALID_PATTERN)  # valid pattern, but not in a list

    def test_validate_relative_path(self):
        with self.assertRaises(ValueError):
            validate_relative_path('/absolute/path')

        with self.assertNoException():
            validate_relative_path('relative/path')

    def test_validate_relative_path_attr(self):
        with self.assertRaisesRegex(ValueError, r'.*dest_path.*'):
            validate_relative_path_attr('/absolute/path', 'dest_path')

        with self.assertNoException():
            validate_relative_path_attr('relative/path', 'dest_path')

    def test_get_pattern_subgroups_from_string(self):
        good_pattern = re.compile(r"""
                                      ^.*FILE_SUFFIX_
                                       (?P<product_code>[A-Z]{3,4})_C-
                                       (?P<creation_date>[0-9]{8}T[0-9]{6}Z)\.
                                       (?P<extension>nc|txt|csv)$
                                       """, re.VERBOSE)
        good_file = 'FILE_SUFFIX_ABC_C-20180101T000000Z.txt'
        bad_file = 'BAD_ABC_C-20180101T000000Z.txt'

        # test on filename only
        fields = get_pattern_subgroups_from_string(good_file, good_pattern)
        self.assertEqual(fields['product_code'], 'ABC')

        # test on filepath
        fields = get_pattern_subgroups_from_string(os.path.join('/not/a/real/path', good_file),
                                                   good_pattern)
        self.assertEqual(fields['product_code'], 'ABC')

        with self.assertRaises(TypeError):
            get_pattern_subgroups_from_string(bad_file, 12)

        fields = get_pattern_subgroups_from_string(bad_file, '')
        self.assertDictEqual(fields, {})

        with self.assertRaises(ValueError):
            get_pattern_subgroups_from_string(bad_file, r'^FILE_SUFFIX_(?P<product_code[A-Z]{3,4})')

    def test_format_exception(self):
        try:
            raise OSError('dummy exception for format test')
        except OSError as e:
            formatted = format_exception(e)

        self.assertEqual(formatted, 'OSError: dummy exception for format test')

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
        self.assertTrue(matches_regexes('example-filename.nc', [VALID_PATTERN]))

        # Testing inclusion from string regex
        self.assertTrue(matches_regexes('example-filename.nc', r'example-.*\.nc'))

        # Testing exclusion from list of regexes
        self.assertFalse(matches_regexes('another-example.zip', [r'.*\.zip'], [r'another-example\.zip']))

        # Testing exclusion from string regex
        self.assertFalse(matches_regexes('another-example.zip', r'.*\.zip', r'another-example\.zip'))

        # Testing invalid arguments
        with self.assertRaises(TypeError):
            matches_regexes('example-filename.nc', 1)
        with self.assertRaises(TypeError):
            matches_regexes('example-filename.nc', r'.*\.zip', 1)
        with self.assertRaises(ValueError):
            matches_regexes('example-filename.nc', INVALID_PATTERN)

        # Empty string is a valid regex which matches everything
        self.assertTrue(matches_regexes('example-filename.nc', ''))

        self.assertFalse(matches_regexes('example-filename.nc', None))
        self.assertFalse(matches_regexes('example-filename.nc', []))

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

        reference_ordered_dict = OrderedDict(sorted(reference_dict.items()))
        reference_ordered_dict['key7'] = {
            'subkey4': {
                'subsubkey2': 'subsubvalue3'
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
        dict8 = {
            'key7': {
                'subkey4': {
                    'subsubkey2': 'subsubvalue3'
                }
            }
        }

        merged_dict = merge_dicts(dict1, dict2, dict3, dict4, dict5, dict6, dict7)
        self.assertDictEqual(merged_dict, reference_dict)

        ordered_merged_dict = merge_dicts(reference_ordered_dict, dict8)
        self.assertIsInstance(ordered_merged_dict, OrderedDict)
        self.assertDictEqual(ordered_merged_dict, reference_ordered_dict)

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

        already_a_list = ['already', 'a', 'list']
        self.assertIs(already_a_list, str_to_list(already_a_list))

    def test_validate_mandatory_elements(self):
        superset = {'a', 'b', 'c'}
        subset = {'a', 'b'}

        with self.assertRaises(ValueError):
            validate_mandatory_elements(superset, subset)

        with self.assertNoException():
            validate_mandatory_elements(subset, superset)

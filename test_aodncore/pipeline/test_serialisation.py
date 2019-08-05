import json
from datetime import datetime

from aodncore.pipeline import FileType
from aodncore.pipeline.serialisation import (PipelineJSONDecoder, PipelineJSONEncoder, datetime_to_string,
                                             string_to_datetime)
from aodncore.testlib import BaseTestCase


class TestPipelineSerialisation(BaseTestCase):
    def test_datetime_to_string(self):
        original = "2019-07-02T17:25:54.948323"
        converted = string_to_datetime(original)
        expected = datetime(2019, 7, 2, 17, 25, 54, 948323)
        self.assertEqual(converted, expected)

    def test_string_to_datetime(self):
        original = datetime(2019, 7, 2, 17, 25, 54, 948323)
        converted = datetime_to_string(original)
        expected = "2019-07-02T17:25:54.948323"
        self.assertEqual(converted, expected)


class TestPipelineJSONEncoder(BaseTestCase):
    def test_encode_datetime(self):
        original = datetime(2019, 7, 2, 17, 25, 54, 948323)
        encoded = json.dumps(original, cls=PipelineJSONEncoder)
        decoded = json.loads(encoded)
        expected = {"__pipeline_datetime__": "2019-07-02T17:25:54.948323"}
        self.assertEqual(decoded, expected)

    def test_encode_enum(self):
        original = FileType.CSV
        encoded = json.dumps(original, cls=PipelineJSONEncoder)
        decoded = json.loads(encoded)
        expected = {
            "__module__": "aodncore.pipeline.common",
            "__pipeline_enum__": "FileType.CSV"
        }
        self.assertEqual(decoded, expected)

    def test_encode_unknown_type(self):
        class Dummy(object):
            dummy_attribute = ''

        original = Dummy()
        encoded = json.dumps(original, cls=PipelineJSONEncoder)
        decoded = json.loads(encoded, cls=PipelineJSONDecoder)
        self.assertTrue(decoded.startswith('UNSERIALISABLE('))


class TestPipelineJSONDecoder(BaseTestCase):
    def test_decode_datetime(self):
        original = '{"__pipeline_datetime__": "2019-07-02T17:25:54.948323"}'
        decoded = json.loads(original, cls=PipelineJSONDecoder)
        expected = datetime(2019, 7, 2, 17, 25, 54, 948323)
        self.assertEqual(expected, decoded)

    def test_decode_valid_enum(self):
        original = '{"__module__": "aodncore.pipeline.common", "__pipeline_enum__": "FileType.NETCDF"}'
        decoded = json.loads(original, cls=PipelineJSONDecoder)
        self.assertIs(FileType.NETCDF, decoded)

    def test_decode_invalid_enum_member(self):
        original = ('{"__module__": "aodncore.pipeline.common", '
                    '"__pipeline_enum__": "FileType.INVALID_NONEXISTENT_TYPE"}')
        with self.assertRaises(AttributeError):
            _ = json.loads(original, cls=PipelineJSONDecoder)

    def test_decode_invalid_enum(self):
        original = ('{"__module__": "aodncore.pipeline.common", '
                    '"__pipeline_enum__": "InvalidNonExistentEnum.INVALID_NONEXISTENT_TYPE"}')
        with self.assertRaises(AttributeError):
            _ = json.loads(original, cls=PipelineJSONDecoder)

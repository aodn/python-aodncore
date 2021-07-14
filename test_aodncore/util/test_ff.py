import os
import yaml
from tableschema import Schema

from aodncore.testlib import BaseTestCase
from aodncore.util import (get_field_type, get_tableschema_descriptor)
from aodncore.pipeline.exceptions import InvalidSchemaError


from test_aodncore import TESTDATA_DIR

GOOD_SCHEMA_FILE = os.path.join(TESTDATA_DIR, 'test.frictionless.schema.yaml')
GOOD_RESOURCE_FILE = os.path.join(TESTDATA_DIR, 'test.frictionless.resource.yaml')
BAD_SCHEMA_FILE = os.path.join(TESTDATA_DIR, 'invalid.frictionless.schema')
BAD_RESOURCE_FILE = os.path.join(TESTDATA_DIR, 'invalid.frictionless.resource')


class TestUtilFrictionlessFramework(BaseTestCase):
    def test_get_tableschema_schema(self):
        with open(GOOD_SCHEMA_FILE) as stream:
            result = get_tableschema_descriptor(yaml.safe_load(stream), 'schema')
            schema = Schema(result)

        self.assertIsInstance(result, dict)
        self.assertIsInstance(schema, Schema)
        self.assertTrue(schema.valid)

    def test_get_tableschema_resource(self):
        with open(GOOD_RESOURCE_FILE) as stream, open(GOOD_SCHEMA_FILE) as compare:
            result = get_tableschema_descriptor(yaml.safe_load(stream), 'schema')
            schema = Schema(result)
            match = Schema(yaml.safe_load(compare))

        self.assertIsInstance(result, dict)
        self.assertIsInstance(schema, Schema)
        self.assertTrue(schema.valid)
        self.assertEqual(schema.descriptor, match.descriptor)

    def test_get_tableschema_schema_invalid(self):
        with open(BAD_SCHEMA_FILE) as stream, self.assertRaises(InvalidSchemaError):
            get_tableschema_descriptor(yaml.safe_load(stream), 'schema')

    def test_get_tableschema_resource_invalid(self):
        with open(BAD_RESOURCE_FILE) as stream, self.assertRaises(InvalidSchemaError):
            get_tableschema_descriptor(yaml.safe_load(stream), 'schema')

    def test_get_field_type(self):
        field = 'datetime'
        translation = 'timestamp'
        self.assertEqual(get_field_type(field), translation)

    def test_get_field_type_no_translation(self):
        # tableschema / frictionless already validate field against jsonschema field types so we only need to know
        # that a field type missing from the translations returns the original field type
        field = 'array'
        self.assertEqual(get_field_type(field), field)

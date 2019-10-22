import json
import os

from jsonschema.exceptions import ValidationError

from aodncore.pipeline.schema import validate_json_manifest
from aodncore.testlib import BaseTestCase
from test_aodncore import TESTDATA_DIR

GOOD_MANIFEST = os.path.join(TESTDATA_DIR, 'test.json_manifest')
BAD_MANIFEST = os.path.join(TESTDATA_DIR, 'invalid.json_manifest')


class TestPipelineSchema(BaseTestCase):
    def test_validate_json_manifest_valid(self):
        with open(GOOD_MANIFEST) as f:
            content = json.load(f)

        with self.assertNoException():
            validate_json_manifest(content)

    def test_validate_json_manifest_invalid(self):
        with open(BAD_MANIFEST) as f:
            content = json.load(f)

        with self.assertRaises(ValidationError):
            validate_json_manifest(content)

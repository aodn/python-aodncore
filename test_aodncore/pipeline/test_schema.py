import json
import os

from jsonschema.exceptions import ValidationError

from aodncore.pipeline.schema import validate_json_manifest, validate_harvest_params
from aodncore.testlib import BaseTestCase
from test_aodncore import TESTDATA_DIR

GOOD_MANIFEST = os.path.join(TESTDATA_DIR, 'test.json_manifest')
BAD_MANIFEST = os.path.join(TESTDATA_DIR, 'invalid.json_manifest')
GOOD_HARVEST = os.path.join(TESTDATA_DIR, 'test.harvest_params')
BAD_HARVEST = os.path.join(TESTDATA_DIR, 'invalid.harvest_params')

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

    def test_validate_harvest_params_valid(self):
        with open(GOOD_HARVEST) as f:
            content = json.load(f)

        with self.assertNoException():
            validate_harvest_params(content)

    def test_validate_harvest_params_invalid(self):
        with open(BAD_HARVEST) as f:
            content = json.load(f)

        with self.assertRaises(ValidationError):
            validate_harvest_params(content)

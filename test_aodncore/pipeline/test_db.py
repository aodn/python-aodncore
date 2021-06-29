import yaml
import os
import pathlib as pl
import unittest
import psycopg2
from psycopg2.extensions import parse_dsn
from testcontainers.postgres import PostgresContainer  # pip install testcontainers[postgresql]
from aodncore.pipeline.db import get_tableschema_descriptor, get_recursive_filenames, DatabaseInteractions
from aodncore.testlib import BaseTestCase
from test_aodncore import TESTDATA_DIR

GOOD_SCHEMA = os.path.join(TESTDATA_DIR, 'test.frictionless.schema.yaml')
GOOD_RESOURCE = os.path.join(TESTDATA_DIR, 'test.frictionless.resource.yaml')
BAD_SCHEMA = os.path.join(TESTDATA_DIR, 'invalid.frictionless.schema')
BAD_RESOURCE = os.path.join(TESTDATA_DIR, 'invalid.frictionless.resource')
GOOD_DIR = TESTDATA_DIR
BAD_DIR = 'not/a/real/directory'
db_config = {"dbname": "harvest", "user": "test", "password": "test"}


class TestDbHelperFunctions(BaseTestCase):
    def assertIsFile(self, path):
        if not pl.Path(path).resolve().is_file():
            raise AssertionError("File does not exist: %s" % str(path))

    def test_get_schema_valid(self):
        with open(GOOD_SCHEMA) as f:
            content = yaml.safe_load(f)

        with self.assertNoException():
            get_tableschema_descriptor(content, 'schema')

    def test_get_schema_invalid(self):
        with open(BAD_SCHEMA) as f:
            content = yaml.safe_load(f)

        with self.assertRaises(Exception):
            get_tableschema_descriptor(content, 'schema')

    def test_get_resource_valid(self):
        with open(GOOD_RESOURCE) as f:
            content = yaml.safe_load(f)

        with self.assertNoException():
            get_tableschema_descriptor(content, 'schema')

    def test_get_resource_invalid(self):
        with open(BAD_RESOURCE) as f:
            content = yaml.safe_load(f)

        with self.assertRaises(Exception):
            get_tableschema_descriptor(content, 'schema')

    def test_get_recursive_filenames_valid(self):
        files = list(get_recursive_filenames(GOOD_DIR))
        self.assertIsNotNone(files)
        self.assertGreater(len([f for f in files if 'wfs' in f]), 0)
        for file in files:
            self.assertIsFile(file)

    def test_get_recursive_filenames_invalid(self):
        files = list(get_recursive_filenames(BAD_DIR))
        self.assertListEqual(files, [])


class TestDatabaseInteractions(BaseTestCase):
    def setUp(self):
        # Start the postgresql container and create the schema
        self.pg = PostgresContainer('postgres:9.5', **db_config)
        self.pg.start()
        self.params = parse_dsn(self.pg.get_connection_url().replace('+psycopg2', ''))
        with psycopg2.connect(**self.params) as conn:
            cur = conn.cursor()
            cur.execute(f"CREATE SCHEMA {db_config['user']} AUTHORIZATION {db_config['user']}")
        self.params['options'] = f"-c search_path={db_config['user']}"

    def tearDown(self):
        # Stop the postgresql container
        self.pg.stop()

    def test_connect_to_schema(self):
        # This doesn't actually test the code, but if this fails none of the other tests will pass
        with psycopg2.connect(**self.params) as conn:
            cursor = conn.cursor()
            cursor.execute('SHOW search_path')
            schema = cursor.fetchone()
            self.assertIn('test', schema)

import os.path

import psycopg2
from psycopg2.extensions import parse_dsn
from testcontainers.postgres import PostgresContainer
from aodncore.pipeline.db import DatabaseInteractions
from aodncore.testlib import BaseTestCase
from test_aodncore import TESTDATA_DIR
from aodncore.pipeline.exceptions import InvalidSQLConnectionError, InvalidSQLTransactionError, MissingFileError

db_config = {"dbname": "harvest", "user": "test", "password": "test"}
GOOD_TABLE_DEFN = {"name": "frictionless", "type": "table"}
GOOD_VIEW_DEFN = {"name": "frictionless_mv", "type": "materialized view"}
BAD_SQL = {"name": "invalid", "type": "table"}
SAMPLE_DATA = os.path.join(TESTDATA_DIR, "test.sample_data.csv")
GOOD_CSV = {"name": "sample_data", "type": "table", "local_path": SAMPLE_DATA}
NO_DATA = {"name": "no_data", "type": "table", "local_path": 'not/a/real/file'}


class TestDatabaseInteractions(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        # Start the postgresql container and create the schema
        cls.pg = PostgresContainer('postgres:9.5', **db_config)
        cls.pg.start()
        cls.params = parse_dsn(cls.pg.get_connection_url().replace('+psycopg2', ''))

        # Independent session used for additional actions
        cls.conn = psycopg2.connect(**cls.params)
        cls.cursor = cls.conn.cursor()
        cls.cursor.execute("CREATE SCHEMA {user} AUTHORIZATION {user}".format(**db_config))
        cls.conn.commit()

        # Additional
        cls.params['options'] = "-c search_path={user}".format(**db_config)
        cls.bad_params = cls.params.copy()
        cls.bad_params['password'] = 'not_a_real_password'


    @classmethod
    def tearDownClass(cls):
        # Cleanup and stop the postgresql container
        cls.cursor.close()
        cls.conn.close()
        cls.pg.stop()

    def create_sample_table(self, table_name, with_data=True):
        with open(SAMPLE_DATA) as fn:
            self.cursor.execute('DROP TABLE IF EXISTS {}'.format(table_name))
            self.cursor.execute('CREATE TABLE {} (id int, value varchar)'.format(table_name))
            if with_data:
                self.cursor.copy_expert('COPY {} FROM STDIN WITH HEADER CSV'.format(table_name), fn)
            self.conn.commit()

    def create_materialized_view(self, base_name):
        self.create_sample_table(base_name)
        self.cursor.execute('CREATE MATERIALIZED VIEW {}_mv AS (SELECT * FROM {})'.format(base_name, base_name))
        self.conn.commit()

    def drop_table(self, table_name):
        self.cursor.execute('DROP TABLE IF EXISTS {} CASCADE'.format(table_name))
        self.conn.commit()

    def get_table_count(self, table_name, conditions=None):
        stmt = "select count(*) from {} ".format(table_name)
        if conditions:
            c = ["{}='{}'".format(k, v) for k, v in conditions.items()]
            stmt += 'where {}'.format(' and '.join(c))
        self.cursor.execute(stmt)
        return self.cursor.fetchone()[0]

    def test_db_connect(self):
        with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
            self.assertIsNotNone(db._conn)
            self.assertIsNotNone(db._cur)

    def test_db_connect_invalid(self):
        with self.assertRaises(InvalidSQLConnectionError):
            with DatabaseInteractions(config=self.bad_params, schema_base_path=TESTDATA_DIR, logger=self.test_logger):
                pass

    def test_roll_back(self):
        self.drop_table(GOOD_TABLE_DEFN['name'])
        with self.assertRaises(InvalidSQLTransactionError):
            with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
                db.create_table_from_yaml_file(GOOD_TABLE_DEFN)
                db.execute_sql_file(BAD_SQL)

        self.assertEqual('rolled_back', db.status)
        cond = {'table_schema': self.params['user'], 'table_name': GOOD_TABLE_DEFN['name']}
        count = self.get_table_count('information_schema.tables', cond)
        self.assertEqual(0, count)

    def test_commit(self):
        with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
            db.create_table_from_yaml_file(GOOD_TABLE_DEFN)

        self.assertEqual('committed', db.status)
        cond = {'table_schema': self.params['user'], 'table_name': GOOD_TABLE_DEFN['name']}
        count = self.get_table_count('information_schema.tables', cond)
        self.assertEqual(1, count)

    def test_compare_schemas(self):
        # TODO: placeholder until function is implemented
        with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
            self.assertTrue(db.compare_schemas())

    def test_truncate_table(self):
        self.create_sample_table(GOOD_TABLE_DEFN['name'])
        with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
            db.truncate_table(GOOD_TABLE_DEFN)

        count = self.get_table_count(GOOD_TABLE_DEFN['name'])
        self.assertEqual(0, count)

    def test_truncate_table_not_exists(self):
        self.drop_table(GOOD_TABLE_DEFN['name'])
        with self.assertRaises(InvalidSQLTransactionError):
            with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
                db.truncate_table(GOOD_TABLE_DEFN)

    def test_refresh_materialized_view(self):
        self.create_materialized_view(GOOD_TABLE_DEFN['name'])
        with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
            # Truncate underlying table then refresh materialized view
            db.truncate_table(GOOD_TABLE_DEFN)
            db.refresh_materialized_view(GOOD_VIEW_DEFN)

        count = self.get_table_count(GOOD_VIEW_DEFN['name'])
        self.assertEqual(0, count)

    def test_refresh_materialized_view_not(self):
        with self.assertNoException():
            with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
                # Pass in object with type != materialized view
                db.refresh_materialized_view(GOOD_TABLE_DEFN)

    def test_drop_object_table(self):
        self.create_sample_table(GOOD_TABLE_DEFN['name'])
        with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
            db.drop_object(GOOD_TABLE_DEFN)

        cond = {'table_schema': self.params['user'], 'table_name': GOOD_TABLE_DEFN['name']}
        count = self.get_table_count('information_schema.tables', cond)
        self.assertEqual(0, count)

    def test_drop_object_view(self):
        self.create_materialized_view(GOOD_TABLE_DEFN['name'])
        with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
            db.drop_object(GOOD_VIEW_DEFN)

        cond = {'relname': GOOD_VIEW_DEFN['name']}
        count = self.get_table_count('pg_catalog.pg_class', cond)
        self.assertEqual(0, count)

    def test_drop_object_not_exists(self):
        with self.assertNoException():
            with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
                db.drop_object(BAD_SQL)

    def test_drop_object_cascade(self):
        self.create_materialized_view(GOOD_TABLE_DEFN['name'])
        with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
            # Drop underlying table
            db.drop_object(GOOD_TABLE_DEFN)

        cond = {'relname': GOOD_VIEW_DEFN['name']}
        count = self.get_table_count('pg_catalog.pg_class', cond)
        self.assertEqual(0, count)

    def test_load_data_from_csv(self):
        self.create_sample_table(GOOD_CSV['name'], with_data=False)
        with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
            db.load_data_from_csv(GOOD_CSV)

        count = self.get_table_count(GOOD_CSV['name'])
        self.assertGreater(count, 0)

    def test_load_data_from_csv_no_local_path(self):
        with self.assertNoException():
            with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
                db.load_data_from_csv(GOOD_TABLE_DEFN)

    def test_load_data_from_csv_no_file(self):
        with self.assertRaises(MissingFileError):
            with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
                db.load_data_from_csv(NO_DATA)

    def test_load_data_from_csv_no_table(self):
        self.drop_table(GOOD_CSV['name'])
        with self.assertRaises(InvalidSQLTransactionError):
            with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
                db.load_data_from_csv(GOOD_CSV)

    def test_execute_sql_file(self):
        self.drop_table(GOOD_TABLE_DEFN['name'])
        with self.assertNoException():
            with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
                db.execute_sql_file(GOOD_TABLE_DEFN)

        # Check table exists
        cond = {'table_schema': self.params['user'], 'table_name': GOOD_TABLE_DEFN['name']}
        count = self.get_table_count('information_schema.tables', cond)
        self.assertEqual(1, count)

        # Check table is populated
        recs = self.get_table_count(GOOD_TABLE_DEFN['name'])
        self.assertGreater(recs, 0)

    def test_execute_sql_file_no_file(self):
        # If no file exists we just want the function to exit
        with self.assertNoException():
            with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
                db.execute_sql_file(NO_DATA)

    def test_execute_sql_file_invalid(self):
        with self.assertRaises(InvalidSQLTransactionError):
            with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
                db.create_table_from_yaml_file(GOOD_TABLE_DEFN)
                db.execute_sql_file(BAD_SQL)

    def test_create_table_from_yaml_file(self):
        self.drop_table(GOOD_TABLE_DEFN['name'])
        with self.assertNoException():
            with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
                db.create_table_from_yaml_file(GOOD_TABLE_DEFN)

        # Check table exists
        cond = {'table_schema': self.params['user'], 'table_name': GOOD_TABLE_DEFN['name']}
        count = self.get_table_count('information_schema.tables', cond)
        self.assertEqual(1, count)

        # Check table is not populated
        recs = self.get_table_count(GOOD_TABLE_DEFN['name'])
        self.assertEqual(recs, 0)

    def test_create_table_from_yaml_file_no_file(self):
        # If no file exists we just want the function to exit
        with self.assertNoException():
            with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
                db.create_table_from_yaml_file(NO_DATA)

    def test_create_table_from_yaml_file_not_table(self):
        self.drop_table(GOOD_TABLE_DEFN['name'])
        # If no file exists we just want the function to exit
        with self.assertNoException():
            with DatabaseInteractions(config=self.params, schema_base_path=TESTDATA_DIR, logger=self.test_logger) as db:
                db.create_table_from_yaml_file(GOOD_VIEW_DEFN)

        # Check table does not exist
        cond = {'table_schema': self.params['user'], 'table_name': GOOD_TABLE_DEFN['name']}
        count = self.get_table_count('information_schema.tables', cond)
        self.assertEqual(0, count)
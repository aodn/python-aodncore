import os
import re
import csv
import yaml
import psycopg2
from psycopg2 import sql

from .exceptions import InvalidSQLConnectionError, InvalidSQLTransactionError, InvalidConfigError

db_field_translate = {
    'integer': 'int',
    'string': 'varchar',
    'any': 'varchar',
    'number': 'numeric',
    'datetime': 'timestamp',
    'date': 'date'
}


class DatabaseInteractions(object):

    # private methods

    def __init__(self, config, schema_base_path, logger):
        self._conn = None
        self._cur = None
        self.config = config
        self._logger = logger
        self.schema_base_path = schema_base_path

    def __enter__(self):
        self._conn = self.__connect()
        self._cur = self._conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._logger.info("Rolling back changes")
            self._conn.rollback()
        else:
            self._logger.info("Committing changes")
            self._conn.commit()
        self._cur.close()
        self._conn.close()

    def __connect(self):
        """ Connect to the PostgreSQL database server """
        params = self.config
        try:
            return psycopg2.connect(**params)
        except Exception as error:
            raise InvalidSQLConnectionError(error)

    def __exec(self, statement):
        try:
            self._cur.execute(sql.SQL(statement))
        except psycopg2.DatabaseError as error:
            raise InvalidSQLTransactionError(error)

    def __exec_copy(self, statement, file):
        try:
            self._cur.copy_expert(sql.SQL(statement), file)
        except psycopg2.DatabaseError as error:
            raise InvalidSQLTransactionError(error)

    def __find_file(self, regex):
        sdir = os.scandir(self.schema_base_path)
        p = re.compile(regex, re.IGNORECASE)
        for f in sdir:
            m = p.match(f.path)
            if m:
                return m.group()
        return None

    # public methods

    def compare_schemas(self):
        """Placeholder for possible future implementation of schema version checking

        :return: boolean - True if schemas match, else False
        """
        return True

    def truncate_table(self, step):
        if step['type'] == 'table':
            self.__exec("TRUNCATE TABLE {}".format(step['name']))

    def refresh_materialized_view(self, step):
        if step['type'] == 'materialized_view':
            self.__exec("REFRESH MATERIALIZED_VIEW {}".format(step['name']))

    def drop_object(self, step):
        self._logger.info("Dropping {type} {name}".format(**step))
        stmt = "DROP {type} IF EXISTS {name} CASCADE".format(**step)
        self.__exec(stmt)

    def load_data_from_csv(self, step):
        fn = step.get('local_path', '')
        if fn:
            with open(fn) as f:
                headers = next(csv.reader(f))
                stmt = "COPY {} ({}) FROM STDIN WITH HEADER CSV".format(step['name'], ", ".join(headers))
                self.__exec_copy(stmt, f)

    def execute_sql_file(self, step):
        fn = self.__find_file('(.*){}(.*).sql'.format(step['name']))
        if fn:
            self._logger.info("Executing additional sql from {}".format(fn))
            with open(fn) as stream:
                self.__exec(stream.read())

    def create_table_from_yaml_file(self, step):
        fn = self.__find_file('(.*){}(.*).yaml'.format(step['name']))
        if fn:
            self._logger.info("Creating {type} {name}".format(**step))
            with open(fn) as stream:
                try:
                    schema = yaml.safe_load(stream)
                    # need to add details for primary key (and other constraints?)
                    columns = ", ".join(('{}  {}'.format(col['name'], db_field_translate[col['type']] or col['type'])
                                         for col in schema['schema']['fields']))
                    self.__exec('CREATE TABLE {} ({})'.format(schema['name'], columns))
                except yaml.YAMLError as exc:
                    raise InvalidConfigError(exc)


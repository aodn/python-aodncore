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


def get_nested_object(obj, name):
    """Convenience function to return a nested object by name if it exists

    """
    # TODO: this could live in common and also be made available to the check step
    return obj.get(name, obj)


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
        # TODO: this is a bit clunky - could be made a bit more pythonic with the help of a generator
        src = []
        for path, current_directory, files in os.walk(self.schema_base_path):
            for f in files:
                src.append(os.path.join(path, f))
        p = re.compile(regex, re.IGNORECASE)
        for f in src:
            m = p.match(f)
            if m:
                return m.group()
        return None

    # public methods

    def compare_schemas(self):
        """Placeholder for possible future implementation of schema version checking

        :return: boolean - True if schemas match, else False
        """
        self._logger.info("Compare schema not yet implemented...")
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
            with open(fn, encoding="utf-8") as f:
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
                    schema = get_nested_object(yaml.safe_load(stream), 'schema')
                    columns = []
                    for f in schema['fields']:
                        f['pk'] = 'PRIMARY KEY' if f['name'] in schema.get('primaryKey', []) else ''
                        f['type'] = db_field_translate[f['type']] or f['type']
                        columns.append('{name} {type} {pk}'.format(**f))
                    self.__exec('CREATE TABLE {} ({})'.format(step['name'], ','.join(columns)))
                except yaml.YAMLError as exc:
                    raise InvalidConfigError(exc)


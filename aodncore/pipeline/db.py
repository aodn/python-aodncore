import csv
import yaml
import psycopg2
from psycopg2 import sql

from .exceptions import InvalidSQLConnectionError, InvalidSQLTransactionError, InvalidConfigError, MissingFileError
from ..util import find_file, get_field_type, get_tableschema_descriptor

__all__ = [
    'DatabaseInteractions'
]


class DatabaseInteractions(object):
    """Database connection object.

    This class should be instantiated via the 'with DatabaseInteractions() as...' method, so the __enter__ and __exit__
    functions will be correctly implemented.
    """

    # private methods

    def __init__(self, config, schema_base_path, logger):
        self._conn = None
        self._cur = None
        self.config = config
        self._logger = logger
        self.schema_base_path = schema_base_path
        self.status = 'initiated'

    def __enter__(self):
        # Call database connection method and then create a cursor
        self._conn = self.__connect()
        self._cur = self._conn.cursor()
        self.status = 'connected'
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Check for errors and roll back changes if they exist, otherwise commit changes
        # Finally close the cursor and the connection
        if exc_type:
            self._logger.info("Rolling back changes")
            self._conn.rollback()
            self.status = 'rolled_back'
        else:
            self._logger.info("Committing changes")
            self._conn.commit()
            self.status = 'committed'
        self._cur.close()
        self._conn.close()

    def __connect(self):
        """Connect to the PostgreSQL database server.

        :return: The database connection.
        """
        params = self.config
        try:
            return psycopg2.connect(**params)
        except Exception as error:
            raise InvalidSQLConnectionError(error)

    def __exec(self, statement):
        """Execute an SQL statement using the instance cursor.

        :param statement: A string containing an SQL statement or multiple statements separated by semi-colons.
        :return: None - currently this method does not return the result set, so it can't be used for querying the
        database.  It may be best to use a separate query method for that use case as the result set can be managed
        prior to returning
        """
        try:
            self._cur.execute(sql.SQL(statement))
        except Exception as error:
            raise InvalidSQLTransactionError(error)

    def __exec_copy(self, statement, file):
        """Execute a COPY FROM statement using the instance cursor.

        :param statement: A string containing a COPY statement.
        :param file: A readable file-like object.
        :return: None
        """
        try:
            self._cur.copy_expert(sql.SQL(statement), file)
        except Exception as error:
            raise InvalidSQLTransactionError(error)

    # public methods

    def compare_schemas(self):
        """Placeholder for possible future implementation of schema version checking

        :return: boolean - True if schemas match, else False
        """
        self._logger.info("Compare schema not yet implemented...")
        return True

    def truncate_table(self, step):
        """Truncate the specified table.

        :param step: A dict containing 'name' and 'type' (at least) keys
        - step.name is the name of the database object
        - step.type is the type of database object - the database transaction will only be performed
            if type = 'table'
        """
        if step['type'] == 'table':
            self.__exec("TRUNCATE TABLE {}".format(step['name']))

    def refresh_materialized_view(self, step):
        """Refresh the specified materialized view.

        :param step: A dict containing 'name' and 'type' (at least) keys
        - step.name is the name of the database object
        - step.type is the type of database object - the database transaction will only be performed
            if type = 'materialized view'
        """
        if step['type'] == 'materialized view':
            self.__exec("REFRESH MATERIALIZED VIEW {}".format(step['name']))

    def drop_object(self, step):
        """Drop the specified database object.

        The database transaction uses the IF EXISTS parameter, so will not error if the database object does not exist;
        and also the CASCADE parameter meaning that a previous call to this method may have already cascaded to the
        current database object.

        :param step: A dict containing 'name' and 'type' (at least) keys
        - step.name is the name of the database object
        - step.type is the type of database object
        """
        self._logger.info("Dropping {type} {name}".format(**step))
        stmt = "DROP {type} IF EXISTS {name} CASCADE".format(**step)
        self.__exec(stmt)

    def load_data_from_csv(self, step):
        """Function to read a csv file prior to loading into the specified table.

        Currently uses the utf-8 encoding to read the csv file, and reads the headings into the COPY FROM statement -
        the latter may not be necessary as it is assumed that the file has been validated in a previous handler step.
        :param step: A dict containing 'name' and 'local_path' (at least) keys
        - step.name is the name of the target table
        - step.local_path is the full path to the source file (csv)
        """
        fn = step.get('local_path', '')
        if fn:
            try:
                with open(fn, encoding="utf-8") as f:
                    headers = next(csv.reader(f))
                    self._logger.info("Loding data from {}".format(fn))
                    stmt = "COPY {} ({}) FROM STDIN WITH HEADER CSV".format(step['name'], ", ".join(headers))
                    self.__exec_copy(stmt, f)
            except FileNotFoundError as e:
                raise MissingFileError(e)

    def execute_sql_file(self, step):
        """Function to read an SQL file prior to executing against the database.

        :param step: A dict containing 'name' (at least) key
        - step.name is the name used as part of the match regular expression
        """
        fn = find_file(self.schema_base_path, '(.*){}(.*).sql'.format(step['name']))
        if fn:
            self._logger.info("Executing additional sql from {}".format(fn))
            with open(fn) as stream:
                self.__exec(stream.read())

    def create_table_from_yaml_file(self, step):
        """Function to read an yaml file and use it to build a CREATE TABLE script for execution against the database.

        :param step: A dict containing 'name' and 'type' (at least) keys
        - step.name is the name used as part of the match regular expression
        - step.type is the type of database object. Type should always be table in this context
        """
        fn = find_file(self.schema_base_path, '(.*){}(.*).yaml'.format(step['name']))
        if fn and step['type'] == 'table':
            self._logger.info("Creating {type} {name}".format(**step))
            try:
                with open(fn) as stream:
                    try:
                        schema = get_tableschema_descriptor(yaml.safe_load(stream), 'schema')
                        columns = []
                        for f in schema['fields']:
                            # f['pk'] = 'PRIMARY KEY' if f['name'] in schema.get('primaryKey', []) else ''
                            f['type'] = get_field_type(f['type'])
                            # columns.append('{name} {type} {pk}'.format(**f))
                            columns.append('{name} {type}'.format(**f))
                        if schema.get('primaryKey'):
                            columns.append("PRIMARY KEY ({})".format(','.join(schema.get('primaryKey'))))
                        self.__exec('CREATE TABLE {} ({})'.format(step['name'], ','.join(columns)))
                    except yaml.YAMLError as exc:
                        raise InvalidConfigError(exc)
            except FileNotFoundError as e:
                raise MissingFileError(e)


from __future__ import absolute_import

import datetime
import decimal

import re
from sqlalchemy import exc
from sqlalchemy import processors
from sqlalchemy import types
from sqlalchemy import util
from sqlalchemy.engine.base import Engine
# TODO shouldn't use mysql type
from sqlalchemy.databases import mysql
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy.sql.compiler import SQLCompiler
from pyhive.common import UniversalSet

from dateutil.parser import parse
from decimal import Decimal


class TDWHiveStringTypeBase(types.TypeDecorator):
    """Translates strings returned by Thrift into something else"""
    impl = types.String

    def process_bind_param(self, value, dialect):
        raise NotImplementedError("Writing to Hive not supported")


class TDWHiveDate(TDWHiveStringTypeBase):
    """Translates date strings to date objects"""
    impl = types.DATE

    def process_result_value(self, value, dialect):
        return processors.str_to_date(value)

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, datetime.datetime):
                return value.date()
            elif isinstance(value, datetime.date):
                return value
            elif value is not None:
                return parse(value).date()
            else:
                return None

        return process

    def adapt(self, impltype, **kwargs):
        return self.impl


class TDWHiveTimestamp(TDWHiveStringTypeBase):
    """Translates timestamp strings to datetime objects"""
    impl = types.TIMESTAMP

    def process_result_value(self, value, dialect):
        return processors.str_to_datetime(value)

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, datetime.datetime):
                return value
            elif value is not None:
                return parse(value)
            else:
                return None

        return process

    def adapt(self, impltype, **kwargs):
        return self.impl


class TDWHiveDecimal(TDWHiveStringTypeBase):
    """Translates strings to decimals"""
    impl = types.DECIMAL

    def process_result_value(self, value, dialect):
        if value is not None:
            return decimal.Decimal(value)
        else:
            return None

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, Decimal):
                return value
            elif value is not None:
                return Decimal(value)
            else:
                return None

        return process

    def adapt(self, impltype, **kwargs):
        return self.impl


class TDWHiveIdentifierPreparer(compiler.IdentifierPreparer):
    # Just quote everything to make things simpler / easier to upgrade
    reserved_words = UniversalSet()

    def __init__(self, dialect):
        super(TDWHiveIdentifierPreparer, self).__init__(
            dialect,
            initial_quote='`',
        )


class TDWHiveCompiler(SQLCompiler):
    def visit_concat_op_binary(self, binary, operator, **kw):
        return "concat(%s, %s)" % (self.process(binary.left), self.process(binary.right))

    def visit_insert(self, *args, **kwargs):
        result = super(TDWHiveCompiler, self).visit_insert(*args, **kwargs)
        # Massage the result into Hive's format
        #   INSERT INTO `pyhive_test_database`.`test_table` (`a`) SELECT ...
        #   =>
        #   INSERT INTO TABLE `pyhive_test_database`.`test_table` SELECT ...
        regex = r'^(INSERT INTO) ([^\s]+) \([^\)]*\)'
        assert re.search(regex, result), "Unexpected visit_insert result: {}".format(result)
        return re.sub(regex, r'\1 TABLE \2', result)

    def visit_column(self, *args, **kwargs):
        result = super(TDWHiveCompiler, self).visit_column(*args, **kwargs)
        dot_count = result.count('.')
        assert dot_count in (0, 1, 2), "Unexpected visit_column result {}".format(result)
        if dot_count == 2:
            # we have something of the form schema.table.column
            # hive doesn't like the schema in front, so chop it out
            result = result[result.index('.') + 1:]
        return result

    def visit_char_length_func(self, fn, **kw):
        return 'length{}'.format(self.function_argspec(fn, **kw))


class TDWHiveTypeCompiler(compiler.GenericTypeCompiler):
    def visit_INTEGER(self, type_):
        return 'INT'

    def visit_NUMERIC(self, type_):
        return 'DECIMAL'

    def visit_CHAR(self, type_):
        return 'STRING'

    def visit_VARCHAR(self, type_):
        return 'STRING'

    def visit_NCHAR(self, type_):
        return 'STRING'

    def visit_TEXT(self, type_):
        return 'STRING'

    def visit_CLOB(self, type_):
        return 'STRING'

    def visit_BLOB(self, type_):
        return 'BINARY'

    def visit_TIME(self, type_):
        return 'TIMESTAMP'

    def visit_DATE(self, type_):
        return 'TIMESTAMP'

    def visit_DATETIME(self, type_):
        return 'TIMESTAMP'


_type_map = {
    'boolean': types.Boolean,
    'tinyint': mysql.MSTinyInteger,
    'smallint': types.SmallInteger,
    'int': types.Integer,
    'bigint': types.BigInteger,
    'float': types.Float,
    'double': types.Float,
    'string': types.String,
    'varchar': types.String,
    'date': TDWHiveDate,
    'timestamp': TDWHiveTimestamp,
    'binary': types.String,
    'array': types.String,
    'map': types.String,
    'struct': types.String,
    'uniontype': types.String,
    'decimal': TDWHiveDecimal,
}


class PyCalciteDialect(default.DefaultDialect):
    name = 'pycalcite'
    driver = 'pycalcite'

    preparer = TDWHiveIdentifierPreparer
    statement_compiler = TDWHiveCompiler
    supports_views = True
    supports_alter = True
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_native_decimal = True
    supports_native_boolean = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_multivalues_insert = True
    type_compiler = TDWHiveTypeCompiler

    default_paramstyle = 'pyformat'

    @classmethod
    def dbapi(cls):
        return __import__('pycalcite')

    def do_close(self, dbapi_connection):
        # TODO
        dbapi_connection.close()

    def create_connect_args(self, url):
        kwargs = {
            'host': url.host,
            'port': url.port or 8080,
            'username': url.username,
            'password': url.password,
            'database': url.database or 'default',
        }
        kwargs.update(url.query)
        return [], kwargs

    def get_schema_names(self, connection, **kw):
        # Equivalent to SHOW DATABASES
        # return [row[0] for row in connection.execute('SHOW SCHEMAS')]
        # calcite_bridge = connection.connect().connection.connection.conn
        connection_object = connection.raw_connection().connection
        if connection_object.connection_closed():
            connection_object.reconnect()
        calcite_bridge = connection_object.conn
        schemas_list = calcite_bridge.getSchemaMetaInfo(None, None)
        connection_object.close()
        return [str(schema.getSchemaName()) for schema in schemas_list]

    def get_view_names(self, connection, schema=None, **kw):
        # Hive does not provide functionality to query tableType
        # This allows reflection to not crash at the cost of being inaccurate
        # return self.get_table_names(connection, schema, **kw)
        calcite_bridge = connection.connection.connection.conn
        views_list = calcite_bridge.getTablesMetaInfo(None, schema, None, ['VIEW'])
        # return [str(view.getTableName()) for view in views_list]
        return []

    def _get_table_columns(self, connection, table_name, schema):
        full_table = table_name
        if schema:
            full_table = schema + '.' + table_name
        # TODO using TGetColumnsReq hangs after sending TFetchResultsReq.
        # Using DESCRIBE works but is uglier.
        try:
            # This needs the table name to be unescaped (no backticks).
            rows = connection.execute('DESCRIBE {}'.format(full_table)).fetchall()
        except exc.OperationalError as e:
            # Does the table exist?
            regex_fmt = r'TExecuteStatementResp.*SemanticException.*Table not found {}'
            regex = regex_fmt.format(re.escape(full_table))
            if re.search(regex, e.args[0]):
                raise exc.NoSuchTableError(full_table)
            else:
                raise
        else:
            # Hive is stupid: this is what I get from DESCRIBE some_schema.does_not_exist
            regex = r'Table .* does not exist'
            if len(rows) == 1 and re.match(regex, rows[0].col_name):
                raise exc.NoSuchTableError(full_table)
            return rows

    def has_table(self, connection, table_name, schema=None):
        try:
            calcite_bridge = connection.connection.connection.conn
            tables_list = calcite_bridge.getTablesMetaInfo(None, None, table_name, ['TABLE'])
            # rows = connection.execute('DESCRIBE {}'.format(table_name)).fetchall()
            for table_info in tables_list:
                if table_info.getTableName().toLowerCase() == table_name.lower():
                    return True
            return False
        except exc.NoSuchTableError:
            return False

    def get_columns(self, connection, table_name, schema=None, **kw):
        if isinstance(connection, Engine):
            connection_object = connection.raw_connection().connection
            if connection_object.connection_closed():
                connection_object.reconnect()
            calcite_bridge = connection_object.conn
        else:
            connection_object = None
            calcite_bridge = connection.connection.connection.conn
        columns_list = calcite_bridge.getTableColumnsMetaInfo(None, schema, table_name, None)
        if connection_object:
            connection_object.close()
        result = [{
            'name': str(column.getColumnName()),
            'type': _type_map.get(column.getColumnType().toLowerCase(), types.NullType),
            'nullable': column.getNullable() != 0,
            'default': None
        } for column in columns_list]
        return result

    def get_columns_old(self, connection, table_name, schema=None, **kw):
        rows = self._get_table_columns(connection, table_name, schema)
        # Strip whitespace
        rows = [[col.strip() if col else None for col in row] for row in rows]
        # Filter out empty rows and comment
        rows = [row for row in rows if row[0] and row[0] != '# col_name']
        result = []
        for (col_name, col_type, _comment) in rows:
            if col_name == '# Partition Information':
                break
            # Take out the more detailed type information
            # e.g. 'map<int,int>' -> 'map'
            #      'decimal(10,1)' -> decimal
            col_type = re.search(r'^\w+', col_type).group(0)
            try:
                coltype = _type_map[col_type]
            except KeyError:
                util.warn("Did not recognize type '%s' of column '%s'" % (col_type, col_name))
                coltype = types.NullType

            result.append({
                'name': col_name,
                'type': coltype,
                'nullable': True,
                'default': None,
            })
        return result

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # Hive has no support for foreign keys.
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # Hive has no support for primary keys.
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_table_names(self, connection, schema=None, **kw):
        # original code
        """
        query = 'SHOW TABLES'
        if schema:
            query += ' IN ' + self.identifier_preparer.quote_identifier(schema)
        return [row[0] for row in connection.execute(query)]
        """
        if isinstance(connection, Engine):
            connection_object = connection.raw_connection().connection
            if connection_object.connection_closed():
                connection_object.reconnect()
            calcite_bridge = connection_object.conn
        else:
            connection_object = None
            calcite_bridge = connection.connection.connection.conn
        tables_list = calcite_bridge.getTablesMetaInfo(None, schema, None, ['TABLE'])
        if connection_object:
            connection_object.close()
        return [str(table.getTableName()) for table in tables_list]

    def do_rollback(self, dbapi_connection):
        # No transactions for Hive
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        # We decode everything as UTF-8
        return True

    def _check_unicode_description(self, connection):
        # We decode everything as UTF-8
        return True


dialect = PyCalciteDialect
# -*- coding: utf-8 -*-

__version__ = '0.0.2'
__license__ = 'MIT'
__author__ = 'xihaoxie'

import os
import datetime
import threading

import jpype
from .log import logger
import gc

apilevel = 2
threadsafety = 1
paramstyle = 'qmark'


# DB-API 2.0 Module Interface Exceptions
class Error(Exception): pass


class Warning(Exception): pass


class InterfaceError(Error): pass


class DatabaseError(Error): pass


class InternalError(DatabaseError): pass


class OperationalError(DatabaseError): pass


class ProgrammingError(DatabaseError): pass


class IntegrityError(DatabaseError): pass


class DataError(DatabaseError): pass


class NotSupportedError(DatabaseError): pass


def _get_system_classpath():
    classpath = os.environ.get('CLASSPATH', None)
    if not classpath:
        logger.debug('CLASSPATH is empty')
        return []
    logger.debug('_get_system_classpath=%s' % classpath)
    return classpath.split(';')


def startup_JVM(jar_paths=None):
    """Only starup once before caller first time to use JDBC4py"""
    if not jar_paths or not isinstance(jar_paths, list):
        jar_paths = []

    if not jpype.isJVMStarted():
        jar_paths.extend(_get_system_classpath())
        uniq_jar = list(set(jar_paths))
        class_path = '-Djava.class.path=' + ';'.join(uniq_jar)  # not ; but :
        jvm_args = [class_path, '-Dfile.encoding=UTF-8']
        jvm_path = jpype.getDefaultJVMPath()
        logger.debug('jpype.startJVM() with jvm_args=%s, jvm_path=%s' % (jvm_args, jvm_path))
        jpype.startJVM(jvm_path, *jvm_args)

    if not jpype.isThreadAttachedToJVM():
        logger.debug('need to attachThreadToJVM()')
        jpype.attachThreadToJVM()


def stop_JVM():
    if jpype.isThreadAttachedToJVM():
        jpype.detachThreadFromJVM()
    if jpype.isJVMStarted():
        jpype.shutdownJVM()
    gc.collect()


class Calcite4py(object):
    """thread safe JDBC connection"""
    _lock = threading.RLock()

    def __init__(self, jar_paths=None, json_str='', lex='MYSQL'):
        self._json_str = json_str
        self._jar_paths = jar_paths
        self._lex = lex

    def connect(self):
        with Calcite4py._lock:
            startup_JVM(self._jar_paths)
        try:
            calcite_bridge = jpype.JClass('com.fawvw.ms.bp.core.CalciteBridge')
            conn = calcite_bridge(self._json_str, self._lex)
            return conn
        except jpype.JException(jpype.java.lang.RuntimeException) as ex:
            msg = 'RuntimeException -> Can not find Calcite driver: %s' % ex.message()
            logger.error(msg)
            raise InterfaceError(msg)
        except jpype.JException(jpype.java.sql.SQLException) as ex:
            msg = 'SQLException -> Can not connect database: %s' % ex.message()
            logger.error(msg)
            raise DatabaseError(msg)


class Cursor(object):
    arraysize = 1
    rowcount = -1
    _stmt = None
    _rs = None
    _rs_meta = None

    def __init__(self, conn):
        self._conn = conn
        self._closed = False
        self._description = None

    def __del__(self):
        self.close()

    @property
    def description(self):
        # [column, None, None, 0, None, 0, True]
        if not self._description:
            self._description = zip(self.columnnames(), self.columntypenames())
        return self._description

    def close(self):
        if self._closed:
            return
        if self._stmt:
            self._stmt.close()
        if self._rs:
            self._rs.close()
        self._stmt = None
        self._rs = None
        self._rs_meta = None
        self._description = None
        self._conn = None
        self._closed = True
        logger.debug('%s closed successfully' % self)

    def _format_stmt_paras(self, operation, parameters):
        if not parameters or parameters == (): return operation
        formated = operation.format(*parameters)
        return formated

    def execute(self, operation, parameters=None):
        if self._conn._closed:
            raise DatabaseError('Connection has been closed')
        if not parameters:
            parameters = ()
        operation = self._format_stmt_paras(operation, parameters)
        self._stmt = self._conn.conn.createStatement()
        # self._stmt.setQueryTimeout(30)
        try:
            logger.debug('begin execute')
            # self._rs = self._stmt.executeQuery(operation)
            flag = self._stmt.execute(operation)
            update_count = self._stmt.getUpdateCount()
            self._rs = self._stmt.getResultSet()
            if self._rs:
                self._rs_meta = self._rs.getMetaData()
            logger.debug('flag=%s, updatecount=%s', flag, update_count)
            return update_count
        except jpype.JException(jpype.java.sql.SQLException) as ex:
            msg = 'SQLException -> execute() error: %s' % ex.message()
            logger.error(msg)
            raise DatabaseError(msg)
        except Exception as ex:
            logger.error(ex)
            raise ex

    def executemany(self, operation, seq_of_parameters):
        self._close_last()
        self._stmt = self._conn.conn.createStatement()
        # self._stmt.setQueryTimeout(60)
        try:
            for para in seq_of_parameters:
                operation = self._format_stmt_paras(para)
                self._stmt.addBatch(operation)
            update_count = self._stmt.executeBatch()
            self.rowcount = update_count
            self._rs = self._stmt.getResultSet()
            self._rs_meta = self._rs.getMetaData()
        except jpype.JException(jpype.java.sql.SQLException) as ex:
            msg = 'SQLException -> executemany() error: %s' % ex.message()
            logger.error(msg)
            raise DatabaseError(msg)
        except Exception as ex:
            logger.error(ex)
            raise ex

    def fetchone(self):
        if not self._rs:
            # raise DataError('Not result set')
            return None
        if not self._rs.next(): return None
        row = []
        for i in range(1, self._rs_meta.getColumnCount() + 1):
            # convert SQL types to Python types
            # see https://docs.oracle.com/javase/8/docs/api/java/sql/Types.html
            # and https://docs.oracle.com/javase/8/docs/api/constant-values.html#java.sql.Types.ARRAY
            # and http://www.cnblogs.com/shishm/archive/2012/01/30/2332142.html
            col = None
            typename = self._rs_meta.getColumnTypeName(i)
            col_type = self._rs_meta.getColumnType(i)
            obj_val = self._rs.getObject(i)
            if obj_val is None:
                row.append(None)
            else:
                if col_type in (-6, 5, 4):  # ('TINYINT', 'SMALLINT', 'INTEGER')
                    val = self._rs.getInt(i)
                    col = int(val) if obj_val is not None else None

                elif col_type == -5:  # 'BIGINT'
                    val = self._rs.getLong(i)
                    col = int(val) if obj_val is not None else None

                elif col_type in (6, 8, 3, 7, 2):  # ('FLOAT', 'DOUBLE', 'DECIMAL', 'REAL', 'NUMERIC')
                    val = self._rs.getFloat(i)
                    col = float(val) if obj_val is not None else None

                elif col_type in (-7, 16):  # ('BIT', 'BOOLEAN')
                    col = bool(self._rs.getBoolean(i))

                elif col_type in (1, 12):  # ('CHAR', 'VARCHAR')
                    col = str(self._rs.getString(i)) if obj_val is not None else None

                elif col_type in (93, 2014):  # ('TIMESTAMP', 'TIMESTAMP_WITH_TIMEZONE')
                    val = self._rs.getTimestamp(i)
                    if val:
                        d = datetime.datetime.strptime(str(val)[:19], '%Y-%m-%d %H:%M:%S')
                        col = d.strftime('%Y-%m-%d %H:%M:%S')

                elif col_type in (92, 2013):  # ('TIME', 'TIME_WITH_TIMEZONE')
                    val = self._rs.getTime(i)
                    col = str(val) if val else ''

                elif col_type == 91:  # 'DATE'
                    val = self._rs.getDate(i)
                    if val:
                        d = datetime.datetime.strptime(str(val)[:10], '%Y-%m-%d')
                        col = d.strftime('%Y-%m-%d')

                elif col_type in (-2, -3):  # ('BINARY', 'VARBINARY')
                    col = self._rs.getBytes(i)

                elif col_type == 0:  # 'NULL'
                    col = 'NULL'
                else:
                    col = self._rs.getString(i)
                row.append(col)
        return tuple(row)

    def fetchmany(self, size=None):
        if not self._rs:
            # raise DataError('Not result set')
            return None
        if not size:
            size = self.arraysize
        self._rs.setFetchSize(size)
        rows = []
        row = None
        for i in range(size):
            row = self.fetchone()
            if not row:
                break
            else:
                rows.append(row)
        return rows

    def fetchall(self):
        rows = []
        while True:
            row = self.fetchone()
            if not row:
                break
            else:
                rows.append(row)
        return rows

    def columnnames(self):
        if not self._rs_meta:
            return []
        names = []
        column_count = self._rs_meta.getColumnCount()
        for i in range(1, column_count + 1):
            names.append(str(self._rs_meta.getColumnName(i)))
        return names

    def columntypenames(self):
        if not self._rs_meta:
            return []
        names = []
        column_count = self._rs_meta.getColumnCount()
        for i in range(1, column_count + 1):
            names.append(str(self._rs_meta.getColumnTypeName(i)))
        return names

    def columntype(self):
        if not self._rs_meta:
            return []
        names = []
        column_count = self._rs_meta.getColumnCount()
        for i in range(1, column_count + 1):
            names.append(self._rs_meta.getColumnType(i))
        return names

    def nextset(self):
        raise NotSupportedError('nextset() not supported')

    def setinputsizes(self, sizes):
        raise NotSupportedError('setinputsizes(sizes) not supported')

    def setoutputsize(self, size, column=None):
        raise NotSupportedError('setoutputsize(size[, column ]) not supported')

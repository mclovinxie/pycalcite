from __future__ import absolute_import

import os
import gc
import json

# from .cursor import Cursor
from .log import logger
from .calcite4py import Calcite4py, Cursor, stop_JVM

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HIVE_DRIVER_NAME = 'org.apache.hadoop.hive.jdbc.HiveDriver'


# def _build_connection(host, port, db, username=None, password=None, **kwargs):
#     conn_url = 'jdbc:%s://%s:%s/%s' % ('hive', host, port, db)
#     logger.debug('conn_url=%s, dbuser=%s, dbpwd=%s' % (conn_url, username, password))
#     jar_paths = [os.path.join(BASE_DIR, 'jar', 'hive_jdbc.jar'), os.path.join(BASE_DIR, 'jar', 'JDBCBridge.jar')]
#     jdbc = JDBC4py(HIVE_DRIVER_NAME, conn_url, username, password, jar_paths=jar_paths)
#     conn = jdbc.connect()
#     return conn


def _build_connection(json_str, lex='MYSQL'):
    logger.debug('json_str=%s' % json_str)
    # jar_paths = [os.path.join(BASE_DIR, 'jar', 'dialect_calcite.jar'), os.path.join(BASE_DIR, 'jar', 'ojdbc8-19.7.0.0.jar')]
    jar_paths = [os.path.join(BASE_DIR, 'jar', 'dialect_calcite.jar')]
    jdbc = Calcite4py(jar_paths=jar_paths, json_str=json_str, lex=lex)
    return jdbc.connect()


class Connection(object):
    def __init__(self, username=None, password=None, host=None, port=None, database=None, **kwargs):
        self.host = host
        self.database = database
        self.port = port or 8080
        self.username = username
        self.password = password
        self.limit = kwargs['limit'] if 'limit' in kwargs else 5000000
        self.con_json_dct = kwargs.get('con_json_dict', {})
        self.lex_type = self.con_json_dct.get('lex', 'MYSQL')
        # self.conn = _build_connection(self.host, self.port, self.database, self.username, self.password)
        del self.con_json_dct['lex']
        self.conn = _build_connection(json.dumps(self.con_json_dct), self.lex_type)
        self._closed = False
        self._cursor = None

    def __enter__(self):
        """Transport should already be opened by __init__"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Call close"""
        if not self._closed:
            self.close()

    def close(self):
        if not self._closed:
            self.conn.close()
        self._cursor.close()
        # stop_JVM()
        gc.collect()
        self._closed = True

    def commit(self):
        try:
            self.conn.commit()
        except:
            logger.warn("transaction may be not supported")

    def rollback(self):
        logger.warn('Transactional rollback is not supported')

    def cursor(self):
        self._cursor = Cursor(self)
        return self._cursor

    def reconnect(self):
        if self._closed:
            self.conn = _build_connection(json.dumps(self.con_json_dct), self.lex_type)
            self._closed = False

    def connection_closed(self):
        return self._closed


def connect(username=None, password=None, host=None, port=None, database=None, **kwargs):
    return Connection(username, password, host, port, database, **kwargs)

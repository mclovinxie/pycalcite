# !/usr/bin python
# -*- coding: utf-8 -*-
# Copyright 2020.
# Author: mclovinxie <mclovin.xxh@gmail.com>
# Created on 2020/7/28

from pycalcite.error import Error

VERSION = (0, 0, 1, None)
threadsafety = 1
apilevel = "2.0"
paramstyle = "pyformat"


class DBAPISet(frozenset):
    def __ne__(self, other):
        if isinstance(other, set):
            return frozenset.__ne__(self, other)
        else:
            return other not in self

    def __eq__(self, other):
        if isinstance(other, frozenset):
            return frozenset.__eq__(self, other)
        else:
            return other in self

    def __hash__(self):
        return frozenset.__hash__(self)


def Connect(*args, **kwargs):
    """
    Connect to the database; see connections.Connection.__init__() for
    more information.
    """
    from .connection import Connection
    return Connection(*args, **kwargs)


def get_client_info():
    return '.'.join(map(str, VERSION))


connect = Connection = Connect

# we include a doctored version_info here for MySQLdb compatibility
version_info = (1, 2, 6, "final", 0)

NULL = "NULL"

__version__ = get_client_info()


def thread_safe():
    return True  # match MySQLdb.thread_safe()


__all__ = [
    'BINARY', 'Error', 'Connect', 'Connection',

    'DBAPISet', 'connect',

    'paramstyle', 'threadsafety', 'version_info',
    "NULL", "__version__",
]

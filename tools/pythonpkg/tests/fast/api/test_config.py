# simple DB API testcase

import duckdb
import numpy
import pandas

class TestDBConfig(object):
    def test_default_order(self, duckdb_cursor):
        df = pandas.DataFrame({'a': [1,2,3]})
        con = duckdb.connect(':memory:', config={'default_order': 'desc'})
        result = con.execute('select * from df order by a').fetchall()
        assert result == [(3,), (2,), (1,)]

    def test_null_order(self, duckdb_cursor):
        df = pandas.DataFrame({'a': [1,2,3,None]})
        con = duckdb.connect(':memory:', config={'default_null_order': 'nulls_last'})
        result = con.execute('select * from df order by a').fetchall()
        assert result == [(1,), (2,), (3,), (None,)]

    def test_multiple_options(self, duckdb_cursor):
        df = pandas.DataFrame({'a': [1,2,3,None]})
        con = duckdb.connect(':memory:', config={'default_null_order': 'nulls_last', 'default_order': 'desc'})
        result = con.execute('select * from df order by a').fetchall()
        assert result == [(3,), (2,), (1,), (None,)]

    def test_external_access(self, duckdb_cursor):
        df = pandas.DataFrame({'a': [1,2,3]})
        # this works (replacement scan)
        con_regular = duckdb.connect(':memory:', config={})
        con_regular.execute('select * from df')
        # disable external access: this also disables pandas replacement scans
        con = duckdb.connect(':memory:', config={'enable_external_access': False})
        # this should fail
        query_failed = False
        try:
            con.execute('select * from df').fetchall()
        except:
            query_failed = True
        assert query_failed == True

    def test_unrecognized_option(self, duckdb_cursor):
        success = True
        try:
            con_regular = duckdb.connect(':memory:', config={'thisoptionisprobablynotthere': '42'})
        except:
            success = False
        assert success==False

    def test_incorrect_parameter(self, duckdb_cursor):
        success = True
        try:
            con_regular = duckdb.connect(':memory:', config={'default_null_order': '42'})
        except:
            success = False
        assert success==False



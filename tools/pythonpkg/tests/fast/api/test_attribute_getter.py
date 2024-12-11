import duckdb
import tempfile
import os
import pandas as pd
import tempfile
import pandas._testing as tm
import datetime
import csv
import pytest


class TestGetAttribute(object):
    def test_basic_getattr(self, duckdb_cursor):
        rel = duckdb_cursor.sql('select i as a, (i + 5) % 10 as b, (i + 2) % 3 as c from range(100) tbl(i)')
        assert rel.a.fetchmany(5) == [(0,), (1,), (2,), (3,), (4,)]
        assert rel.b.fetchmany(5) == [(5,), (6,), (7,), (8,), (9,)]
        assert rel.c.fetchmany(5) == [(2,), (0,), (1,), (2,), (0,)]

    def test_basic_getitem(self, duckdb_cursor):
        rel = duckdb_cursor.sql('select i as a, (i + 5) % 10 as b, (i + 2) % 3 as c from range(100) tbl(i)')
        assert rel['a'].fetchmany(5) == [(0,), (1,), (2,), (3,), (4,)]
        assert rel['b'].fetchmany(5) == [(5,), (6,), (7,), (8,), (9,)]
        assert rel['c'].fetchmany(5) == [(2,), (0,), (1,), (2,), (0,)]

    def test_getitem_nonexistant(self, duckdb_cursor):
        rel = duckdb_cursor.sql('select i as a, (i + 5) % 10 as b, (i + 2) % 3 as c from range(100) tbl(i)')
        with pytest.raises(AttributeError):
            rel['d']

    def test_getattr_nonexistant(self, duckdb_cursor):
        rel = duckdb_cursor.sql('select i as a, (i + 5) % 10 as b, (i + 2) % 3 as c from range(100) tbl(i)')
        with pytest.raises(AttributeError):
            rel.d

    def test_getattr_collision(self, duckdb_cursor):
        rel = duckdb_cursor.sql('select i as df from range(100) tbl(i)')

        # 'df' also exists as a method on DuckDBPyRelation
        assert rel.df.__class__ != duckdb.DuckDBPyRelation

    def test_getitem_collision(self, duckdb_cursor):
        rel = duckdb_cursor.sql('select i as df from range(100) tbl(i)')

        # this case is not an issue on __getitem__
        assert rel['df'].__class__ == duckdb.DuckDBPyRelation

    def test_getitem_struct(self, duckdb_cursor):
        rel = duckdb_cursor.sql("select {'a':5, 'b':6} as a, 5 as b")
        assert rel['a']['a'].fetchall()[0][0] == 5
        assert rel['a']['b'].fetchall()[0][0] == 6

    def test_getattr_struct(self, duckdb_cursor):
        rel = duckdb_cursor.sql("select {'a':5, 'b':6} as a, 5 as b")
        assert rel.a.a.fetchall()[0][0] == 5
        assert rel.a.b.fetchall()[0][0] == 6

    def test_getattr_spaces(self, duckdb_cursor):
        rel = duckdb_cursor.sql('select 42 as "hello world"')
        assert rel['hello world'].fetchall()[0][0] == 42

    def test_getattr_doublequotes(self, duckdb_cursor):
        rel = duckdb_cursor.sql('select 1 as "tricky"", ""quotes", 2 as tricky, 3 as quotes')
        assert rel[rel.columns[0]].fetchone() == (1,)

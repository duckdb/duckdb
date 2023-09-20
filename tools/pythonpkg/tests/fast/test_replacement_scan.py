import duckdb
import os
import pytest


class TestReplacementScan(object):
    def test_csv_replacement(self):
        con = duckdb.connect()
        filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'integers.csv')
        res = con.execute("select count(*) from '%s'" % (filename))
        assert res.fetchone()[0] == 2

    def test_parquet_replacement(self):
        con = duckdb.connect()
        filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'binary_string.parquet')
        res = con.execute("select count(*) from '%s'" % (filename))
        assert res.fetchone()[0] == 3

    def test_replacement_scan_relapi(self):
        con = duckdb.connect()
        pyrel1 = con.query('from (values (42), (84), (120)) t(i)')
        assert isinstance(pyrel1, duckdb.DuckDBPyRelation)
        assert pyrel1.fetchall() == [(42,), (84,), (120,)]

        pyrel2 = con.query('from pyrel1 limit 2')
        assert isinstance(pyrel2, duckdb.DuckDBPyRelation)
        assert pyrel2.fetchall() == [(42,), (84,)]

        pyrel3 = con.query('select i + 100 from pyrel2')
        assert type(pyrel3) == duckdb.DuckDBPyRelation
        assert pyrel3.fetchall() == [(142,), (184,)]

    def test_replacement_scan_alias(self):
        con = duckdb.connect()
        pyrel1 = con.query('from (values (1, 2)) t(i, j)')
        pyrel2 = con.query('from (values (1, 10)) t(i, k)')
        pyrel3 = con.query('from pyrel1 join pyrel2 using(i)')
        assert type(pyrel3) == duckdb.DuckDBPyRelation
        assert pyrel3.fetchall() == [(1, 2, 10)]

    def test_replacement_scan_pandas_alias(self):
        con = duckdb.connect()
        df1 = con.query('from (values (1, 2)) t(i, j)').df()
        df2 = con.query('from (values (1, 10)) t(i, k)').df()
        df3 = con.query('from df1 join df2 using(i)')
        assert df3.fetchall() == [(1, 2, 10)]

    def test_replacement_scan_fail(self):
        random_object = "I love salmiak rondos"
        con = duckdb.connect()
        with pytest.raises(
            duckdb.InvalidInputException,
            match=r'Python Object "random_object" of type "str" found on line .* not suitable for replacement scans.',
        ):
            con.execute("select count(*) from random_object").fetchone()

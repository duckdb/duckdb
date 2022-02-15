import duckdb
import pytest
import os

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','binary_string.parquet')

class TestParquet(object):

    def test_scan_binary(self, duckdb_cursor):
        conn = duckdb.connect()
        res = conn.execute("SELECT typeof(#1) FROM parquet_scan('"+filename+"') limit 1").fetchall()
        assert res[0] == ('BLOB',)

        res = conn.execute("SELECT * FROM parquet_scan('"+filename+"')").fetchall()
        assert res[0] == (b'foo',)

    def test_from_parquet_binary(self, duckdb_cursor):
        rel = duckdb.from_parquet(filename)
        assert rel.types == ['BLOB']

        res = rel.execute().fetchall()
        assert res[0] == (b'foo',)

    def test_scan_binary_as_string(self, duckdb_cursor):
        conn = duckdb.connect()
        res = conn.execute("SELECT typeof(#1) FROM parquet_scan('"+filename+"',binary_as_string=True) limit 1").fetchall()
        assert res[0] == ('VARCHAR',)

        res = conn.execute("SELECT * FROM parquet_scan('"+filename+"',binary_as_string=True)").fetchall()
        assert res[0] == ('foo',)

    def test_from_parquet_binary_as_string(self, duckdb_cursor):
        rel = duckdb.from_parquet(filename,True)
        assert rel.types == ['VARCHAR']

        res = rel.execute().fetchall()
        assert res[0] == ('foo',)

    def test_parquet_binary_as_string_pragma(self, duckdb_cursor):
        conn = duckdb.connect()
        res = conn.execute("SELECT typeof(#1) FROM parquet_scan('"+filename+"') limit 1").fetchall()
        assert res[0] == ('BLOB',)

        res = conn.execute("SELECT * FROM parquet_scan('"+filename+"')").fetchall()
        assert res[0] == (b'foo',)

        conn.execute("PRAGMA binary_as_string=1")

        res = conn.execute("SELECT typeof(#1) FROM parquet_scan('"+filename+"') limit 1").fetchall()
        assert res[0] == ('VARCHAR',)

        res = conn.execute("SELECT * FROM parquet_scan('"+filename+"')").fetchall()
        assert res[0] == ('foo',)

        res = conn.execute("SELECT typeof(#1) FROM parquet_scan('"+filename+"',binary_as_string=False) limit 1").fetchall()
        assert res[0] == ('BLOB',)

        res = conn.execute("SELECT * FROM parquet_scan('"+filename+"',binary_as_string=False)").fetchall()
        assert res[0] == (b'foo',)

        conn.execute("PRAGMA binary_as_string=0")

        res = conn.execute("SELECT typeof(#1) FROM parquet_scan('"+filename+"') limit 1").fetchall()
        assert res[0] == ('BLOB',)

        res = conn.execute("SELECT * FROM parquet_scan('"+filename+"')").fetchall()
        assert res[0] == (b'foo',)

    def test_from_parquet_binary_as_string_default_conn(self,duckdb_cursor):
        duckdb.default_connection.execute("PRAGMA binary_as_string=1")
        
        rel = duckdb.from_parquet(filename,True)
        assert rel.types == ['VARCHAR']

        res = rel.execute().fetchall()
        assert res[0] == ('foo',)




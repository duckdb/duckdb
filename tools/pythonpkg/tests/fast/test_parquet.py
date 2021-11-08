import duckdb
import pytest


class TestParquet(object):

    def test_scan_binary(self, duckdb_cursor):
        conn = duckdb.connect()
        res = conn.execute("SELECT typeof(#1) FROM parquet_scan('data/binary_string.parquet') limit 1").fetchall()
        assert res[0] == ('BLOB',)

        res = conn.execute("SELECT * FROM parquet_scan('data/binary_string.parquet',binary_as_string=True)").fetchall()
        assert res[0] == ('foo',)

    def test_from_parquet_binary(self, duckdb_cursor):
        rel = duckdb.from_parquet("data/binary_string.parquet")
        assert rel.types == ['BLOB'] 

        res = rel.execute().fetchall()
        assert res[0] == (b'foo',) 
    
    def test_scan_binary_as_string(self, duckdb_cursor):
        conn = duckdb.connect()
        res = conn.execute("SELECT typeof(#1) FROM parquet_scan('data/binary_string.parquet',binary_as_string=True) limit 1").fetchall()
        assert res[0] == ('VARCHAR',)

        res = conn.execute("SELECT * FROM parquet_scan('data/binary_string.parquet',binary_as_string=True)").fetchall()
        assert res[0] == ('foo',)

    def test_from_parquet_binary_as_string(self, duckdb_cursor):
        rel = duckdb.from_parquet("data/binary_string.parquet",True)
        assert rel.types == ['VARCHAR'] 

        res = rel.execute().fetchall()
        assert res[0] == ('foo',) 
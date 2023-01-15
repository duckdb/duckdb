import duckdb
import pytest
import os
import tempfile
import pandas as pd

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

    def test_from_parquet_file_row_number(self, duckdb_cursor):
        rel = duckdb.from_parquet(filename,binary_as_string=True,file_row_number=True)
        assert rel.types == ['VARCHAR', 'BIGINT']

        res = rel.execute().fetchall()
        assert res[0] == ('foo',0,)

    def test_from_parquet_filename(self, duckdb_cursor):
        rel = duckdb.from_parquet(filename,binary_as_string=True,filename=True)
        assert rel.types == ['VARCHAR', 'VARCHAR']

        res = rel.execute().fetchall()
        assert res[0] == ('foo',filename,)

    def test_from_parquet_list_binary_as_string(self, duckdb_cursor):
        rel = duckdb.from_parquet([filename],binary_as_string=True)
        assert rel.types == ['VARCHAR']

        res = rel.execute().fetchall()
        assert res[0] == ('foo',)

    def test_from_parquet_list_file_row_number(self, duckdb_cursor):
        rel = duckdb.from_parquet([filename],binary_as_string=True,file_row_number=True)
        assert rel.types == ['VARCHAR', 'BIGINT']

        res = rel.execute().fetchall()
        assert res[0] == ('foo',0,)

    def test_from_parquet_list_filename(self, duckdb_cursor):
        rel = duckdb.from_parquet([filename],binary_as_string=True,filename=True)
        assert rel.types == ['VARCHAR', 'VARCHAR']

        res = rel.execute().fetchall()
        assert res[0] == ('foo',filename,)

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

    def test_from_parquet_binary_as_string_default_conn(self, duckdb_cursor):
        duckdb.default_connection.execute("PRAGMA binary_as_string=1")

        rel = duckdb.from_parquet(filename,True)
        assert rel.types == ['VARCHAR']

        res = rel.execute().fetchall()
        assert res[0] == ('foo',)

    def test_from_parquet_union_by_name(self, duckdb_cursor):
        conn = duckdb.connect()
        temp_dir = tempfile.mkdtemp()

        temp_file_name_1 = os.path.join(temp_dir, next(tempfile._get_candidate_names()))
        conn.execute("copy (from (values (1::bigint), (2::bigint), (9223372036854775807::bigint)) t(a)) to '"+temp_file_name_1+"' (format 'parquet');")

        temp_file_name_2 = os.path.join(temp_dir, next(tempfile._get_candidate_names()))
        conn.execute("copy (from (values (3::integer, 4::integer), (5::integer, 6::integer)) t(a, b)) to '"+temp_file_name_2+"' (format 'parquet');")

        temp_file_name_3 = os.path.join(temp_dir, next(tempfile._get_candidate_names()))
        conn.execute("copy (from (values (100::integer, 101::integer), (102::integer, 103::integer)) t(a, c)) to '"+temp_file_name_3+"' (format 'parquet');")

        rel = duckdb.from_parquet([temp_file_name_1, temp_file_name_2, temp_file_name_3], union_by_name = True).order('a')
        assert rel.execute().fetchall() == [
                (1, None, None,),
                (2, None, None,),
                (3, 4, None,),
                (5, 6, None,),
                (100, None, 101,),
                (102, None, 103,),
                (9223372036854775807, None, None,),
        ]




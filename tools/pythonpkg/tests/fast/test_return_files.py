import duckdb
import os
import pytest


class TestReturnFiles(object):
    def test_copy_to_file(self, duckdb_cursor):
        con = duckdb.connect()
        con.execute("CREATE TABLE integers AS SELECT range i FROM range(200000);")

        filepath = os.path.join(os.getcwd(), 'test_copy_to_file.parquet')

        con.execute("SET preserve_insertion_order=false;")
        res = con.execute(f"COPY integers TO '{filepath}' (RETURN_FILES);").fetchall()[0]
        assert res[0] == 200000
        assert res[1][0] == filepath

    def test_batch_copy_to_file(self, duckdb_cursor):
        con = duckdb.connect()
        con.execute("CREATE TABLE integers AS SELECT range i FROM range(200000);")

        filepath = os.path.join(os.getcwd(), 'test_batch_copy_to_file.parquet')

        con.execute("SET preserve_insertion_order=true;")
        res = con.execute(f"COPY integers TO '{filepath}' (RETURN_FILES TRUE);").fetchall()[0]
        assert res[0] == 200000
        assert res[1][0] == filepath

    def test_per_thread_output(self, duckdb_cursor):
        con = duckdb.connect()
        con.execute("CREATE TABLE integers AS SELECT range i FROM range(200000);")

        filedir = os.path.join(os.getcwd(), 'test_per_thread_output')

        res = con.execute(f"COPY integers TO '{filedir}' (RETURN_FILES TRUE, PER_THREAD_OUTPUT TRUE);").fetchall()[0]
        assert res[0] == 200000
        assert res[1] == [os.path.join(filedir, 'data_0.csv'), os.path.join(filedir, 'data_1.csv')]

    def test_partition_by(self, duckdb_cursor):
        con = duckdb.connect()
        con.execute("CREATE TABLE integers AS SELECT range i, range % 4 j FROM range(200000);")

        filedir = os.path.join(os.getcwd(), 'test_partition_by')

        res = con.execute(f"COPY integers TO '{filedir}' (RETURN_FILES TRUE, PARTITION_BY j);").fetchall()[0]
        assert sorted(res[1]) == [
            os.path.join(filedir, 'j=0', 'data_0.csv'),
            os.path.join(filedir, 'j=1', 'data_0.csv'),
            os.path.join(filedir, 'j=2', 'data_0.csv'),
            os.path.join(filedir, 'j=3', 'data_0.csv'),
        ]

import duckdb
import tempfile
import os
import pytest
from conftest import NumpyPandas, ArrowPandas


class TestInsert(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_insert(self, pandas):
        test_df = pandas.DataFrame({"i": [1, 2, 3], "j": ["one", "two", "three"]})
        # connect to an in-memory temporary database
        conn = duckdb.connect()
        # get a cursor
        cursor = conn.cursor()
        conn.execute("CREATE TABLE test (i INTEGER, j STRING)")
        rel = conn.table("test")
        rel.insert([1, 'one'])
        rel.insert([2, 'two'])
        rel.insert([3, 'three'])
        rel_a3 = cursor.table('test').project('CAST(i as BIGINT)i, j').to_df()
        pandas.testing.assert_frame_equal(rel_a3, test_df)

    def test_insert_with_schema(self, duckdb_cursor):
        duckdb_cursor.sql("create schema not_main")
        duckdb_cursor.sql("create table not_main.tbl as select * from range(10)")

        res = duckdb_cursor.table('not_main.tbl').fetchall()
        assert len(res) == 10

        # FIXME: This is not currently supported
        with pytest.raises(duckdb.CatalogException, match='Table with name tbl does not exist'):
            duckdb_cursor.table('not_main.tbl').insert([42, 21, 1337])

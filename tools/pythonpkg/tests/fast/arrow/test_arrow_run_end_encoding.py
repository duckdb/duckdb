import duckdb
import pytest
import pandas as pd
import duckdb

pa = pytest.importorskip("pyarrow", '14.0.0', reason="Needs pyarrow >= 14")
pc = pytest.importorskip("pyarrow.compute")


class TestArrowREE(object):
    @pytest.mark.parametrize(
        'query',
        [
            """
            select
                CASE WHEN (i % 2 == 0) THEN NULL ELSE (i // {})::{} END as ree
            from range({}) t(i);
        """,
            """
            select
                (i // {})::{} as ree
            from range({}) t(i);
        """,
        ],
    )
    @pytest.mark.parametrize('run_length', [4, 1, 10, 1000, 2048, 3000])
    @pytest.mark.parametrize('size', [100, 10000])
    @pytest.mark.parametrize(
        'value_type',
        ['UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT', 'TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT'],
    )
    def test_arrow_run_end_encoding(self, duckdb_cursor, query, run_length, size, value_type):
        if value_type == 'UTINYINT':
            if size > 255:
                size = 255
        if value_type == 'TINYINT':
            if size > 127:
                size = 127
        query = query.format(run_length, value_type, size)
        rel = duckdb_cursor.sql(query)
        array = rel.arrow()['ree']
        expected = rel.fetchall()

        encoded_array = pc.run_end_encode(array)

        schema = pa.schema([("ree", encoded_array.type)])
        tbl = pa.Table.from_arrays([encoded_array], schema=schema)
        res = duckdb.sql("select * from tbl").fetchall()
        assert res == expected

    def test_arrow_ree_empty_table(self, duckdb_cursor):
        duckdb_cursor.query("create table tbl (ree integer)")
        rel = duckdb_cursor.table('tbl')
        array = rel.arrow()['ree']
        expected = rel.fetchall()

        encoded_array = pc.run_end_encode(array)

        schema = pa.schema([("ree", encoded_array.type)])
        pa_res = pa.Table.from_arrays([encoded_array], schema=schema)
        res = duckdb.sql("select * from pa_res").fetchall()
        assert res == expected


# TODO: add tests with a WHERE clause
# TODO: add tests with projections
# TODO: add tests with lists
# TODO: add tests with structs
# TODO: add tests with maps
# TODO: add tests with ENUMs

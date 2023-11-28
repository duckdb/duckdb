import duckdb
import pytest
import pandas as pd
import duckdb

pa = pytest.importorskip("pyarrow", '14.0.0.dev', reason="Needs pyarrow >= 14")
import pyarrow.compute as pc


class TestArrowREE(object):
    @pytest.mark.parametrize(
        'query',
        [
            """
            select
                CASE WHEN (i % 2 == 0) THEN NULL ELSE (i // {}) END as ree
            from range({}) t(i);
        """,
            """
            select
                (i // {}) as ree
            from range({}) t(i);
        """,
        ],
    )
    @pytest.mark.parametrize('run_length', [4, 1, 10, 1000, 2048, 3000])
    @pytest.mark.parametrize('size', [100, 10000])
    def test_arrow_run_end_encoding(self, duckdb_cursor, query, run_length, size):
        query = query.format(run_length, size)
        rel = duckdb_cursor.sql(query)
        array = rel.arrow()['ree']
        expected = rel.fetchall()

        encoded_array = pc.run_end_encode(array)

        schema = pa.schema([("ree", encoded_array.type)])
        tbl = pa.Table.from_arrays([encoded_array], schema=schema)
        res = duckdb.sql("select * from tbl").fetchall()
        assert res == expected

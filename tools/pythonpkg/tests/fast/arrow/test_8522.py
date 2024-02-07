import duckdb
import pytest
import string
import datetime as dt

pa = pytest.importorskip("pyarrow")


# Reconstruct filters when pushing down into arrow scan
# arrow supports timestamp_tz with different units than US, we only support US
# so we have to convert ConstantValues back to their native unit when pushing the filter expression containing them down to pyarrow
class Test8522(object):
    def test_8522(self, duckdb_cursor):
        t_us = pa.Table.from_arrays(
            arrays=[pa.array([dt.datetime(2022, 1, 1)])],
            schema=pa.schema([pa.field("time", pa.timestamp("us", tz="UTC"))]),
        )

        t_ms = pa.Table.from_arrays(
            arrays=[pa.array([dt.datetime(2022, 1, 1)])],
            schema=pa.schema([pa.field("time", pa.timestamp("ms", tz="UTC"))]),
        )

        expected = duckdb_cursor.sql("FROM t_us").filter("time>='2022-01-01'").fetchall()
        assert len(expected) == 1

        actual = duckdb_cursor.sql("FROM t_ms").filter("time>='2022-01-01'").fetchall()
        assert actual == expected

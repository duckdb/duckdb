from re import S
import duckdb
import os
import pytest
import tempfile
from conftest import pandas_supports_arrow_backend

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
ds = pytest.importorskip("pyarrow.dataset")
np = pytest.importorskip("numpy")


class TestArrowLargeOffsets(object):
    @pytest.mark.skip(reason="CI does not have enough memory to validate this")
    def test_large_lists(self, duckdb_cursor):
        ary = pa.array([np.arange(start=0, stop=3000, dtype=np.uint8) for i in range(1_000_000)])
        tbl = pa.Table.from_pydict(dict(col=ary))
        with pytest.raises(
            duckdb.InvalidInputException,
            match='Arrow Appender: The maximum combined list offset for regular list buffers is 2147483647 but the offset of 2147481000 exceeds this.',
        ):
            res = duckdb_cursor.sql("SELECT col FROM tbl").arrow()

        tbl2 = pa.Table.from_pydict(dict(col=ary.cast(pa.large_list(pa.uint8()))))
        duckdb_cursor.sql("set arrow_large_buffer_size = true")
        res2 = duckdb_cursor.sql("SELECT col FROM tbl2").arrow()
        res2.validate()

    @pytest.mark.skip(reason="CI does not have enough memory to validate this")
    def test_large_maps(self, duckdb_cursor):
        ary = pa.array([np.arange(start=3000 * j, stop=3000 * (j + 1), dtype=np.uint64) for j in range(1_000_000)])
        tbl = pa.Table.from_pydict(dict(col=ary))

        with pytest.raises(
            duckdb.InvalidInputException,
            match='Arrow Appender: The maximum combined list offset for regular list buffers is 2147483647 but the offset of 2147481000 exceeds this.',
        ):
            arrow_map = duckdb_cursor.sql("select map(col, col) from tbl").arrow()

        duckdb_cursor.sql("set arrow_large_buffer_size = true")
        arrow_map_large = duckdb_cursor.sql("select map(col, col) from tbl").arrow()
        arrow_map_large.validate()

import duckdb
import os
import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")


class TestArrowView(object):
    def test_arrow_view(self, duckdb_cursor):
        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'userdata1.parquet')
        userdata_parquet_table = pa.parquet.read_table(parquet_filename)
        userdata_parquet_table.validate(full=True)
        duckdb_cursor.from_arrow(userdata_parquet_table).create_view('arrow_view')
        assert duckdb_cursor.execute("PRAGMA show_tables").fetchone() == ('arrow_view',)
        assert duckdb_cursor.execute("select avg(salary)::INT from arrow_view").fetchone()[0] == 149005

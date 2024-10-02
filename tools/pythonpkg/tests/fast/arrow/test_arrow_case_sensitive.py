import duckdb
import pytest

pa = pytest.importorskip("pyarrow")


class TestArrowCaseSensitive(object):
    def test_arrow_case_sensitive(self, duckdb_cursor):
        data = (pa.array([1], type=pa.int32()), pa.array([1000], type=pa.int32()))
        arrow_table = pa.Table.from_arrays([data[0], data[1]], ['A1', 'a1'])

        duckdb_cursor.register('arrow_tbl', arrow_table)
        assert duckdb_cursor.table("arrow_tbl").columns == ['A1', 'a1_1']
        assert duckdb_cursor.execute("select A1 from arrow_tbl;").fetchall() == [(1,)]
        assert duckdb_cursor.execute("select a1_1 from arrow_tbl;").fetchall() == [(1000,)]
        assert arrow_table.column_names == ['A1', 'a1']

    def test_arrow_case_sensitive_repeated(self, duckdb_cursor):
        data = (pa.array([1], type=pa.int32()), pa.array([1000], type=pa.int32()))
        arrow_table = pa.Table.from_arrays([data[0], data[1], data[1]], ['A1', 'a1_1', 'a1'])

        duckdb_cursor.register('arrow_tbl', arrow_table)
        assert duckdb_cursor.table("arrow_tbl").columns == ['A1', 'a1_1', 'a1_2']
        assert arrow_table.column_names == ['A1', 'a1_1', 'a1']

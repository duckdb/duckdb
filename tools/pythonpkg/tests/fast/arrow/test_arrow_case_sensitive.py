import duckdb
import pytest
try:
    import pyarrow as pa
    can_run = True
except:
    can_run = False

class TestArrowCaseSensitive(object):
    def test_arrow_case_sensitive(self, duckdb_cursor):
        if not can_run:
            return
        data = (pa.array([1], type=pa.int32()),pa.array([1000], type=pa.int32()))
        arrow_table = pa.Table.from_arrays([data[0],data[1]],['A1','a1'])

        con = duckdb.connect()
        con.register('arrow_tbl', arrow_table)

        assert con.execute("DESCRIBE arrow_tbl;").fetchall() == [('a1', 'INTEGER', 'YES', None, None, None), ('a1_1', 'INTEGER', 'YES', None, None, None)]
        assert con.execute("select a1 from arrow_tbl;").fetchall() == [(1,)]
        assert con.execute("select a1_1 from arrow_tbl;").fetchall() == [(1000,)]
        assert arrow_table.column_names == ['A1', 'a1']
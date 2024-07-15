import duckdb
import pytest


class TestGetTableNames(object):
    def test_table_success(self, duckdb_cursor):
        conn = duckdb.connect()
        table_names = conn.get_table_names("SELECT * FROM my_table1, my_table2, my_table3")
        assert table_names == {'my_table2', 'my_table3', 'my_table1'}

    def test_table_fail(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.close()
        with pytest.raises(duckdb.ConnectionException, match="Connection already closed"):
            table_names = conn.get_table_names("SELECT * FROM my_table1, my_table2, my_table3")

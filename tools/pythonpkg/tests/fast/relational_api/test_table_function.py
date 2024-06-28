import duckdb
import pytest
import os

script_path = os.path.dirname(__file__)


class TestTableFunction(object):
    def test_table_function(self, duckdb_cursor):
        path = os.path.join(script_path, '..', 'data/integers.csv')
        rel = duckdb_cursor.table_function('read_csv', [path])
        res = rel.fetchall()
        assert res == [(1, 10, 0), (2, 50, 30)]

        # Provide only a string as argument, should error, needs a list
        with pytest.raises(duckdb.InvalidInputException, match=r"'params' has to be a list of parameters"):
            rel = duckdb_cursor.table_function('read_csv', path)

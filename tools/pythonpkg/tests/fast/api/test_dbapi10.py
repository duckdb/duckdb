# cursor description
from datetime import datetime, date
from pytest import mark


class TestCursorDescription(object):
    @mark.parametrize(
        "query,column_name,string_type,real_type",
        [
            ["SELECT * FROM integers", "i", "NUMBER", int],
            ["SELECT * FROM timestamps", "t", "DATETIME", datetime],
            ["SELECT DATE '1992-09-20' AS date_col;", "date_col", "Date", date],
            ["SELECT '\\xAA'::BLOB AS blob_col;", "blob_col", "BINARY", bytes],
            ["SELECT {'x': 1, 'y': 2, 'z': 3} AS struct_col", "struct_col", "dict", dict],
            ["SELECT [1, 2, 3] AS list_col", "list_col", "list", list],
            ["SELECT 'Frank' AS str_col", "str_col", "STRING", str],
            ["SELECT [1, 2, 3]::JSON AS json_col", "json_col", "STRING", str],
            ["SELECT union_value(tag := 1) AS union_col", "union_col", "UNION(tag INTEGER)", int],
        ],
    )
    def test_description(self, query, column_name, string_type, real_type, duckdb_cursor):
        duckdb_cursor.execute(query)
        assert duckdb_cursor.description == [(column_name, string_type, None, None, None, None, None)]
        assert isinstance(duckdb_cursor.fetchone()[0], real_type)

    def test_none_description(self, duckdb_empty_cursor):
        assert duckdb_empty_cursor.description is None

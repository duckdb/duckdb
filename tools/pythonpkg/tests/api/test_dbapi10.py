# cursor description

class TestCursorDescription(object):
    def test_description(self, duckdb_cursor):
        description = duckdb_cursor.execute("SELECT * FROM integers").description
        assert description == [('i', None, None, None, None, None, None)]

    def test_none_description(self, duckdb_empty_cursor):
        assert duckdb_empty_cursor.description is None

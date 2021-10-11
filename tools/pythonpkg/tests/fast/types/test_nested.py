import duckdb


class TestNested(object):

    def test_lists(self, duckdb_cursor):
        duckdb_conn = duckdb.connect()
        result = duckdb_conn.execute("SELECT LIST_VALUE(1, 2, 3, 4) ").fetchall()
        assert result == [([1, 2, 3, 4],)]

        result = duckdb_conn.execute("SELECT LIST_VALUE() ").fetchall()
        assert result == [([],)]

        result = duckdb_conn.execute("SELECT LIST_VALUE(1, 2, 3, NULL) ").fetchall()
        assert result == [([1, 2, 3, None],)]


    def test_nested_lists(self, duckdb_cursor):
        duckdb_conn = duckdb.connect()
        result = duckdb_conn.execute("SELECT LIST_VALUE(LIST_VALUE(1, 2, 3, 4), LIST_VALUE(1, 2, 3, 4)) ").fetchall()
        assert result == [([[1, 2, 3, 4], [1, 2, 3, 4]],)]

        result = duckdb_conn.execute("SELECT LIST_VALUE(LIST_VALUE(1, 2, 3, 4), LIST_VALUE(1, 2, 3, NULL)) ").fetchall()
        assert result == [([[1, 2, 3, 4], [1, 2, 3, None]],)]

    def test_struct(self, duckdb_cursor):
        duckdb_conn = duckdb.connect()
        result = duckdb_conn.execute("SELECT STRUCT_PACK(a := 42, b := 43)").fetchall()
        assert result == [({'a': 42, 'b': 43},)]

        result = duckdb_conn.execute("SELECT STRUCT_PACK(a := 42, b := NULL)").fetchall()
        assert result == [({'a': 42, 'b': None},)]
       

    def test_nested_struct(self, duckdb_cursor):
        duckdb_conn = duckdb.connect()
        result = duckdb_conn.execute("SELECT STRUCT_PACK(a := 42, b := LIST_VALUE(10, 9, 8, 7))").fetchall()
        assert result == [({'a': 42, 'b': [10,9,8,7]},)]

        result = duckdb_conn.execute("SELECT STRUCT_PACK(a := 42, b := LIST_VALUE(10, 9, 8, NULL))").fetchall()
        assert result == [({'a': 42, 'b': [10,9,8,None]},)]

    def test_map(self, duckdb_cursor):
        duckdb_conn = duckdb.connect()
        result = duckdb_conn.execute("select MAP(LIST_VALUE(1, 2, 3, 4),LIST_VALUE(10, 9, 8, 7))").fetchall()
        assert result == [({'key': [1,2,3,4], 'value': [10,9,8,7]},)]

        result = duckdb_conn.execute("select MAP(LIST_VALUE(1, 2, 3, 4),LIST_VALUE(10, 9, 8, NULL))").fetchall()
        assert result == [({'key': [1,2,3,4], 'value': [10,9,8,None]},)]

        result = duckdb_conn.execute("SELECT MAP() ").fetchall()
        assert result == [({'key': [], 'value': []},)]
        
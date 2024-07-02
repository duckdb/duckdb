import duckdb


class TestNested(object):
    def test_lists(self, duckdb_cursor):
        result = duckdb_cursor.execute("SELECT LIST_VALUE(1, 2, 3, 4) ").fetchall()
        assert result == [([1, 2, 3, 4],)]

        result = duckdb_cursor.execute("SELECT LIST_VALUE() ").fetchall()
        assert result == [([],)]

        result = duckdb_cursor.execute("SELECT LIST_VALUE(1, 2, 3, NULL) ").fetchall()
        assert result == [([1, 2, 3, None],)]

    def test_nested_lists(self, duckdb_cursor):
        result = duckdb_cursor.execute("SELECT LIST_VALUE(LIST_VALUE(1, 2, 3, 4), LIST_VALUE(1, 2, 3, 4)) ").fetchall()
        assert result == [([[1, 2, 3, 4], [1, 2, 3, 4]],)]

        result = duckdb_cursor.execute(
            "SELECT LIST_VALUE(LIST_VALUE(1, 2, 3, 4), LIST_VALUE(1, 2, 3, NULL)) "
        ).fetchall()
        assert result == [([[1, 2, 3, 4], [1, 2, 3, None]],)]

    def test_struct(self, duckdb_cursor):
        result = duckdb_cursor.execute("SELECT STRUCT_PACK(a := 42, b := 43)").fetchall()
        assert result == [({'a': 42, 'b': 43},)]

        result = duckdb_cursor.execute("SELECT STRUCT_PACK(a := 42, b := NULL)").fetchall()
        assert result == [({'a': 42, 'b': None},)]

    def test_unnamed_struct(self, duckdb_cursor):
        result = duckdb_cursor.execute("SELECT row('aa','bb') AS x").fetchall()
        assert result == [(('aa', 'bb'),)]

        result = duckdb_cursor.execute("SELECT row('aa',NULL) AS x").fetchall()
        assert result == [(('aa', None),)]

    def test_nested_struct(self, duckdb_cursor):
        result = duckdb_cursor.execute("SELECT STRUCT_PACK(a := 42, b := LIST_VALUE(10, 9, 8, 7))").fetchall()
        assert result == [({'a': 42, 'b': [10, 9, 8, 7]},)]

        result = duckdb_cursor.execute("SELECT STRUCT_PACK(a := 42, b := LIST_VALUE(10, 9, 8, NULL))").fetchall()
        assert result == [({'a': 42, 'b': [10, 9, 8, None]},)]

    def test_map(self, duckdb_cursor):
        result = duckdb_cursor.execute("select MAP(LIST_VALUE(1, 2, 3, 4),LIST_VALUE(10, 9, 8, 7))").fetchall()
        assert result == [({1: 10, 2: 9, 3: 8, 4: 7},)]

        result = duckdb_cursor.execute("select MAP(LIST_VALUE(1, 2, 3, 4),LIST_VALUE(10, 9, 8, NULL))").fetchall()
        assert result == [({1: 10, 2: 9, 3: 8, 4: None},)]

        result = duckdb_cursor.execute("SELECT MAP() ").fetchall()
        assert result == [({},)]

#simple DB API testcase


class TestSimpleDBAPI(object):
    def test_regular_selection(self, duckdb_cursor):
        duckdb_cursor.execute('SELECT * FROM integers')
        result = duckdb_cursor.fetchall()
        assert result == [(0,), (1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (None,)], "Incorrect result returned"

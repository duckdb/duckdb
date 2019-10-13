#simple DB API testcase


class TestSimpleDBAPI(object):
    def test_prepare_bytes(self, duckdb_cursor):
        result = duckdb_cursor.execute('SELECT CAST(? AS INTEGER)', b'42').fetchall()
        assert result == [[42]], "Incorrect result returned"

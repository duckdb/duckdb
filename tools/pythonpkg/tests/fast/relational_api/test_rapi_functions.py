import duckdb

class TestRAPIFunctions(object):
    def test_rapi_str_print(self):
        res = duckdb.query('select 42::INT AS a, 84::BIGINT AS b')
        assert str(res) is not None
        res.show()

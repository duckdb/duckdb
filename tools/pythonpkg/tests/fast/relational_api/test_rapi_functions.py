import duckdb


class TestRAPIFunctions(object):
    def test_rapi_str_print(self, duckdb_cursor):
        res = duckdb_cursor.query('select 42::INT AS a, 84::BIGINT AS b')
        assert str(res) is not None
        res.show()

    def test_rapi_relation_sql_query(self):
        res = duckdb.table_function('range', [10])
        assert res.sql_query() == 'SELECT * FROM range(10)'

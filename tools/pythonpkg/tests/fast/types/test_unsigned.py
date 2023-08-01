class TestUnsigned(object):
    def test_unsigned(self, duckdb_cursor):
        duckdb_cursor.execute('create table unsigned (a utinyint, b usmallint, c uinteger, d ubigint)')
        duckdb_cursor.execute('insert into unsigned values (1,1,1,1), (null,null,null,null)')
        duckdb_cursor.execute('select * from unsigned order by a nulls first')
        result = duckdb_cursor.fetchall()
        assert result == [(None, None, None, None), (1, 1, 1, 1)]
